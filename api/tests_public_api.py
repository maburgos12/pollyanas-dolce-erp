from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from crm.models import Cliente, PedidoCliente
from integraciones.models import PublicApiAccessLog, PublicApiClient
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from recetas.models import LineaReceta, Receta


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

        self.public_client, self.raw_api_key = PublicApiClient.create_with_generated_key(
            nombre="Integrador QA",
            descripcion="Cliente de pruebas API publica",
        )

    def _auth_headers(self):
        return {"HTTP_X_API_KEY": self.raw_api_key}

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
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["nombre"], "Azucar refinada")
        self.assertEqual(response.data["results"][0]["costo_unitario"], "23.500000")
        self.assertTrue(PublicApiAccessLog.objects.filter(client=self.public_client, endpoint=url).exists())

    def test_insumos_with_invalid_key_returns_401(self):
        url = reverse("api_public_insumos")
        response = self.client.get(url, HTTP_X_API_KEY="pk_invalid_key")
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_recetas_returns_tipo_and_line_count(self):
        url = reverse("api_public_recetas")
        response = self.client.get(url, **self._auth_headers())
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        row = response.data["results"][0]
        self.assertEqual(row["tipo_producto"], Receta.TIPO_PREPARACION)
        self.assertEqual(row["lineas"], 1)

    def test_resumen_returns_global_counts(self):
        url = reverse("api_public_resumen")
        response = self.client.get(url, **self._auth_headers())
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["insumos_activos"], 1)
        self.assertEqual(response.data["recetas_activas"], 1)

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
