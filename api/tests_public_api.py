from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from crm.models import Cliente, PedidoCliente
from integraciones.models import PublicApiAccessLog, PublicApiClient
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct
from recetas.models import LineaReceta, Receta, RecetaCodigoPointAlias


class PublicApiTests(APITestCase):
    def setUp(self):
        self.unidad_kg = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=1000,
        )
        self.insumo = Insumo.objects.create(nombre="Azucar refinada", unidad_base=self.unidad_kg, activo=True)
        CostoInsumo.objects.create(
            insumo=self.insumo,
            costo_unitario=Decimal("23.500000"),
            source_hash=f"test-{uuid4()}",
        )
        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            stock_actual=Decimal("12.000"),
            punto_reorden=Decimal("8.000"),
        )

        self.receta = Receta.objects.create(
            nombre="Betun Vainilla API",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("3.000000"),
            rendimiento_unidad=self.unidad_kg,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Azucar refinada",
            cantidad=Decimal("0.500000"),
            unidad=self.unidad_kg,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("23.500000"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        # La subseccion no debe sumar al conteo de lineas en el endpoint.
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=2,
            tipo_linea=LineaReceta.TIPO_SUBSECCION,
            insumo_texto="Decorado",
            cantidad=Decimal("0.050000"),
            unidad=self.unidad_kg,
            unidad_texto="kg",
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_SUBSECTION,
        )

        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.point_branch = PointBranch.objects.create(
            external_id="1",
            name="Matriz",
            erp_branch=self.sucursal,
            status=PointBranch.STATUS_ACTIVE,
        )
        self.pickup_receta = Receta.objects.create(
            nombre="Pastel Selva Negra",
            codigo_point="01PSV",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-pickup-{uuid4()}",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=self.pickup_receta,
            codigo_point="PASTEL-SELVA-NEGRA",
            nombre_point="Pastel Selva Negra",
        )
        self.point_product = PointProduct.objects.create(
            external_id="1001",
            sku="01PSV",
            name="Pastel Selva Negra",
            category="Pasteles",
        )
        self.snapshot_job_id = 1
        self._ensure_snapshot_job()
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=self.point_product,
            stock=Decimal("5"),
            min_stock=Decimal("0"),
            max_stock=Decimal("0"),
            captured_at=timezone.now(),
            sync_job_id=self.snapshot_job_id,
        )

        self.public_client, self.raw_api_key = PublicApiClient.create_with_generated_key(
            nombre="Integrador QA",
            descripcion="Cliente de pruebas API publica",
        )

    def _auth_headers(self):
        return {"HTTP_X_API_KEY": self.raw_api_key}

    def _ensure_snapshot_job(self):
        from pos_bridge.models import PointSyncJob

        PointSyncJob.objects.get_or_create(
            id=self.snapshot_job_id,
            defaults={
                "job_type": PointSyncJob.JOB_TYPE_INVENTORY,
                "status": PointSyncJob.STATUS_SUCCESS,
            },
        )

    def test_health_public_without_key(self):
        url = reverse("api_public_health")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "ok")

    def test_insumos_requires_key(self):
        url = reverse("api_public_insumos")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertIn("X-API-Key", response.data["detail"])

    def test_insumos_with_valid_key_returns_payload_and_logs(self):
        url = reverse("api_public_insumos")
        response = self.client.get(url, **self._auth_headers())
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], Insumo.objects.filter(activo=True).count())
        self.assertEqual(response.data["results"][0]["nombre"], "Azucar refinada")
        self.assertEqual(response.data["results"][0]["costo_unitario"], "23.500000")
        self.assertTrue(PublicApiAccessLog.objects.filter(client=self.public_client, endpoint=url).exists())

    def test_insumos_with_invalid_key_returns_401(self):
        url = reverse("api_public_insumos")
        response = self.client.get(url, HTTP_X_API_KEY="pk_invalid_key")
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    @override_settings(PUBLIC_API_RATE_LIMIT_PER_MINUTE=1)
    def test_insumos_rate_limit_returns_429(self):
        url = reverse("api_public_insumos")
        first = self.client.get(url, **self._auth_headers())
        self.assertEqual(first.status_code, status.HTTP_200_OK)

        second = self.client.get(url, **self._auth_headers())
        self.assertEqual(second.status_code, status.HTTP_429_TOO_MANY_REQUESTS)
        self.assertIn("Límite", second.data["detail"])

    def test_recetas_returns_tipo_and_line_count(self):
        url = reverse("api_public_recetas")
        response = self.client.get(url, **self._auth_headers())
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], Receta.objects.count())
        row = response.data["results"][0]
        self.assertEqual(row["tipo_producto"], Receta.TIPO_PREPARACION)
        self.assertEqual(row["lineas"], 1)

    def test_resumen_returns_global_counts(self):
        url = reverse("api_public_resumen")
        response = self.client.get(url, **self._auth_headers())
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["insumos_activos"], Insumo.objects.filter(activo=True).count())
        self.assertEqual(response.data["recetas_activas"], Receta.objects.count())

    def test_pedidos_create_creates_cliente_and_pedido(self):
        url = reverse("api_public_pedidos_create")
        payload = {
            "cliente_nombre": "Sucursal Centro",
            "descripcion": "Pedido publico de prueba",
            "sucursal": "Centro",
            "prioridad": PedidoCliente.PRIORIDAD_ALTA,
            "monto_estimado": "1250.50",
            "fecha_compromiso": "2026-02-25",
        }
        response = self.client.post(url, payload, format="json", **self._auth_headers())
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertTrue(Cliente.objects.filter(nombre="Sucursal Centro").exists())
        pedido = PedidoCliente.objects.get(id=response.data["id"])
        self.assertEqual(pedido.prioridad, PedidoCliente.PRIORIDAD_ALTA)
        self.assertEqual(str(pedido.monto_estimado), "1250.50")
        self.assertIsNotNone(pedido.fecha_compromiso)

    def test_pedidos_create_invalid_fecha_compromiso_returns_400(self):
        url = reverse("api_public_pedidos_create")
        payload = {
            "cliente_nombre": "Sucursal Norte",
            "descripcion": "Pedido invalido",
            "fecha_compromiso": "25/02/2026",
        }
        response = self.client.post(url, payload, format="json", **self._auth_headers())
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("fecha_compromiso", response.data["detail"])

    @override_settings(
        PICKUP_AVAILABILITY_FRESHNESS_MINUTES=20,
        PICKUP_STOCK_BUFFER_DEFAULT="1",
        PICKUP_LOW_STOCK_THRESHOLD="2",
        PICKUP_RESERVATION_TTL_MINUTES=15,
    )
    def test_pickup_availability_returns_available_for_branch(self):
        url = reverse("api_public_pickup_availability")
        response = self.client.get(
            url,
            {"product_code": "01PSV", "branch_code": "MATRIZ", "quantity": "1"},
            **self._auth_headers(),
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "AVAILABLE")
        self.assertEqual(response.data["available_to_promise"], "4.000")
        self.assertTrue(response.data["available"])

    @override_settings(
        PICKUP_AVAILABILITY_FRESHNESS_MINUTES=20,
        PICKUP_STOCK_BUFFER_DEFAULT="1",
        PICKUP_LOW_STOCK_THRESHOLD="2",
        PICKUP_RESERVATION_TTL_MINUTES=15,
    )
    def test_pickup_reservation_create_and_confirm_creates_order(self):
        reserve_url = reverse("api_public_pickup_reservations")
        reserve_response = self.client.post(
            reserve_url,
            {
                "product_code": "01PSV",
                "branch_code": "MATRIZ",
                "quantity": "2",
                "cliente_nombre": "Cliente Pickup",
                "external_reference": "WEB-ORDER-1001",
            },
            format="json",
            **self._auth_headers(),
        )
        self.assertEqual(reserve_response.status_code, status.HTTP_201_CREATED)
        token = reserve_response.data["reservation_token"]

        confirm_url = reverse("api_public_pickup_reservations_confirm", kwargs={"token": token})
        confirm_response = self.client.post(
            confirm_url,
            {
                "cliente_nombre": "Cliente Pickup",
                "descripcion": "Pedido pickup web",
                "monto_estimado": "899.00",
                "fecha_compromiso": "2026-03-18",
            },
            format="json",
            **self._auth_headers(),
        )
        self.assertEqual(confirm_response.status_code, status.HTTP_201_CREATED)
        pedido = PedidoCliente.objects.get(id=confirm_response.data["pedido_id"])
        self.assertEqual(pedido.sucursal_ref_id, self.sucursal.id)
        self.assertEqual(pedido.pickup_reservation.token, token)
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_CONFIRMADO)

    @override_settings(
        PICKUP_AVAILABILITY_FRESHNESS_MINUTES=20,
        PICKUP_STOCK_BUFFER_DEFAULT="1",
        PICKUP_LOW_STOCK_THRESHOLD="2",
        PICKUP_RESERVATION_TTL_MINUTES=15,
    )
    def test_pickup_reservation_release_returns_refund_required_for_confirmed(self):
        reserve_url = reverse("api_public_pickup_reservations")
        reserve_response = self.client.post(
            reserve_url,
            {
                "product_code": "01PSV",
                "branch_code": "MATRIZ",
                "quantity": "1",
                "cliente_nombre": "Cliente Pickup",
                "external_reference": "WEB-ORDER-1002",
            },
            format="json",
            **self._auth_headers(),
        )
        token = reserve_response.data["reservation_token"]
        confirm_url = reverse("api_public_pickup_reservations_confirm", kwargs={"token": token})
        self.client.post(
            confirm_url,
            {
                "cliente_nombre": "Cliente Pickup",
                "descripcion": "Pedido pickup web",
            },
            format="json",
            **self._auth_headers(),
        )

        release_url = reverse("api_public_pickup_reservations_release", kwargs={"token": token})
        release_response = self.client.post(
            release_url,
            {"reason": "Sin stock real en sucursal"},
            format="json",
            **self._auth_headers(),
        )
        self.assertEqual(release_response.status_code, status.HTTP_200_OK)
        self.assertTrue(release_response.data["refund_required"])

    @override_settings(
        PICKUP_AVAILABILITY_FRESHNESS_MINUTES=1,
        PICKUP_STOCK_BUFFER_DEFAULT="1",
        PICKUP_LOW_STOCK_THRESHOLD="2",
        PICKUP_RESERVATION_TTL_MINUTES=15,
    )
    def test_pickup_availability_returns_unknown_when_snapshot_is_stale(self):
        PointInventorySnapshot.objects.all().update(captured_at=timezone.now() - timedelta(minutes=5))
        url = reverse("api_public_pickup_availability")
        response = self.client.get(
            url,
            {"product_code": "01PSV", "branch_code": "MATRIZ", "quantity": "1"},
            **self._auth_headers(),
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "UNKNOWN")
        self.assertFalse(response.data["available"])
