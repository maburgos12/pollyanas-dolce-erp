from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.db import OperationalError
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal
from compras.models import OrdenCompra, PresupuestoCompraPeriodo, RecepcionCompra, SolicitudCompra
from inventario.models import AjusteInventario, ExistenciaInsumo, MovimientoInventario
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from recetas.models import (
    LineaReceta,
    PlanProduccion,
    PlanProduccionItem,
    PronosticoVenta,
    Receta,
    SolicitudVenta,
    VentaHistorica,
)
from recetas.utils.costeo_versionado import asegurar_version_costeo


class RecetasCosteoApiTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_api_costeo",
            email="admin_api_costeo@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(nombre="Azucar API", unidad_base=self.unidad, activo=True)
        self.receta = Receta.objects.create(
            nombre="Receta API",
            sheet_name="Insumos API",
            hash_contenido="hash-api-001",
            rendimiento_cantidad=Decimal("5"),
            rendimiento_unidad=self.unidad,
        )
        self.linea = LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Azucar API",
            cantidad=Decimal("2"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("3"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )
        asegurar_version_costeo(self.receta, fuente="TEST_API")

    def test_endpoint_versiones(self):
        url = reverse("api_receta_versiones", args=[self.receta.id])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["receta_id"], self.receta.id)
        self.assertGreaterEqual(payload["total"], 1)
        self.assertIn("items", payload)
        self.assertIn("costo_total", payload["items"][0])
        self.assertFalse(payload["data_unavailable"])

    def test_endpoint_versiones_handles_missing_table_gracefully(self):
        url = reverse("api_receta_versiones", args=[self.receta.id])
        with patch("api.views._load_versiones_costeo", side_effect=OperationalError("missing table")):
            resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["data_unavailable"])
        self.assertEqual(payload["total"], 0)
        self.assertEqual(payload["items"], [])
        self.assertGreaterEqual(len(payload["warnings"]), 1)

    def test_endpoint_versiones_limit_invalido_no_rompe(self):
        url = reverse("api_receta_versiones", args=[self.receta.id])
        resp = self.client.get(url, {"limit": "abc"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertIn("items", payload)
        self.assertGreaterEqual(payload["total"], 1)

    def test_endpoint_costo_historico_con_comparativo(self):
        self.linea.costo_unitario_snapshot = Decimal("4")
        self.linea.save(update_fields=["costo_unitario_snapshot"])
        asegurar_version_costeo(self.receta, fuente="TEST_API")

        url = reverse("api_receta_costo_historico", args=[self.receta.id])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["receta_id"], self.receta.id)
        self.assertGreaterEqual(len(payload["puntos"]), 2)
        self.assertIn("comparativo", payload)
        self.assertIn("delta_total", payload["comparativo"])
        self.assertFalse(payload["data_unavailable"])

    def test_endpoint_costo_historico_handles_missing_table_gracefully(self):
        url = reverse("api_receta_costo_historico", args=[self.receta.id])
        with patch("api.views._load_versiones_costeo", side_effect=OperationalError("missing table")):
            resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["data_unavailable"])
        self.assertEqual(payload["puntos"], [])
        self.assertNotIn("comparativo", payload)
        self.assertGreaterEqual(len(payload["warnings"]), 1)

    def test_endpoint_costo_historico_limit_invalido_no_rompe(self):
        url = reverse("api_receta_costo_historico", args=[self.receta.id])
        resp = self.client.get(url, {"limit": "xyz"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertIn("puntos", payload)
        self.assertGreaterEqual(len(payload["puntos"]), 1)

    def test_endpoint_costo_historico_comparativo_seleccionado(self):
        self.linea.costo_unitario_snapshot = Decimal("4")
        self.linea.save(update_fields=["costo_unitario_snapshot"])
        asegurar_version_costeo(self.receta, fuente="TEST_API")

        url = reverse("api_receta_costo_historico", args=[self.receta.id])
        resp = self.client.get(url, {"base": 1, "target": 2})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertIn("comparativo_seleccionado", payload)
        selected = payload["comparativo_seleccionado"]
        self.assertEqual(selected["base"], 1)
        self.assertEqual(selected["target"], 2)
        self.assertIn("delta_total", selected)

    def test_endpoint_mrp_calcular_requerimientos_por_plan(self):
        plan = PlanProduccion.objects.create(nombre="Plan API", fecha_produccion=date(2026, 2, 21))
        PlanProduccionItem.objects.create(plan=plan, receta=self.receta, cantidad=Decimal("3"))
        ExistenciaInsumo.objects.create(insumo=self.insumo, stock_actual=Decimal("2"))

        url = reverse("api_mrp_calcular_requerimientos")
        resp = self.client.post(url, {"plan_id": plan.id})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["source"], "plan")
        self.assertEqual(payload["plan_id"], plan.id)
        self.assertGreaterEqual(payload["totales"]["insumos"], 1)
        self.assertGreaterEqual(payload["totales"]["alertas_capacidad"], 1)
        self.assertEqual(payload["items"][0]["insumo"], self.insumo.nombre)

    def test_endpoint_mrp_calcular_requerimientos_por_periodo_mes(self):
        plan_1 = PlanProduccion.objects.create(nombre="Plan API mes 1", fecha_produccion=date(2026, 2, 10))
        plan_2 = PlanProduccion.objects.create(nombre="Plan API mes 2", fecha_produccion=date(2026, 2, 20))
        PlanProduccionItem.objects.create(plan=plan_1, receta=self.receta, cantidad=Decimal("2"))
        PlanProduccionItem.objects.create(plan=plan_2, receta=self.receta, cantidad=Decimal("1"))

        url = reverse("api_mrp_calcular_requerimientos")
        resp = self.client.post(
            url,
            {"periodo": "2026-02", "periodo_tipo": "mes"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["source"], "periodo")
        self.assertEqual(payload["periodo"], "2026-02")
        self.assertEqual(payload["periodo_tipo"], "mes")
        self.assertEqual(payload["planes_count"], 2)
        self.assertEqual(Decimal(payload["items"][0]["cantidad_requerida"]), Decimal("6"))

    def test_endpoint_mrp_calcular_requerimientos_por_periodo_quincena(self):
        plan_q1 = PlanProduccion.objects.create(nombre="Plan API q1", fecha_produccion=date(2026, 2, 12))
        plan_q2 = PlanProduccion.objects.create(nombre="Plan API q2", fecha_produccion=date(2026, 2, 22))
        PlanProduccionItem.objects.create(plan=plan_q1, receta=self.receta, cantidad=Decimal("1"))
        PlanProduccionItem.objects.create(plan=plan_q2, receta=self.receta, cantidad=Decimal("3"))

        url = reverse("api_mrp_calcular_requerimientos")
        resp_q1 = self.client.post(
            url,
            {"periodo": "2026-02", "periodo_tipo": "q1"},
            content_type="application/json",
        )
        self.assertEqual(resp_q1.status_code, 200)
        payload_q1 = resp_q1.json()
        self.assertEqual(payload_q1["planes_count"], 1)
        self.assertEqual(Decimal(payload_q1["items"][0]["cantidad_requerida"]), Decimal("2"))

        resp_q2 = self.client.post(
            url,
            {"periodo": "2026-02", "periodo_tipo": "q2"},
            content_type="application/json",
        )
        self.assertEqual(resp_q2.status_code, 200)
        payload_q2 = resp_q2.json()
        self.assertEqual(payload_q2["planes_count"], 1)
        self.assertEqual(Decimal(payload_q2["items"][0]["cantidad_requerida"]), Decimal("6"))

    def test_endpoint_mrp_calcular_requerimientos_rechaza_fuentes_combinadas(self):
        plan = PlanProduccion.objects.create(nombre="Plan API combinado", fecha_produccion=date(2026, 2, 21))
        PlanProduccionItem.objects.create(plan=plan, receta=self.receta, cantidad=Decimal("1"))
        url = reverse("api_mrp_calcular_requerimientos")
        resp = self.client.post(
            url,
            {"plan_id": plan.id, "periodo": "2026-02"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertIn("non_field_errors", payload)

    def test_endpoint_inventario_sugerencias_compra_por_plan(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor API", lead_time_dias=3, activo=True)
        self.insumo.proveedor_principal = proveedor
        self.insumo.save(update_fields=["proveedor_principal"])
        self.linea.cantidad = Decimal("6")
        self.linea.save(update_fields=["cantidad"])

        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            stock_actual=Decimal("4"),
            punto_reorden=Decimal("5"),
            stock_minimo=Decimal("2"),
            dias_llegada_pedido=2,
            consumo_diario_promedio=Decimal("1.5"),
        )
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=proveedor,
            costo_unitario=Decimal("10"),
            source_hash="api-sug-1",
        )
        plan = PlanProduccion.objects.create(nombre="Plan sugerencias", fecha_produccion=date(2026, 2, 20))
        PlanProduccionItem.objects.create(plan=plan, receta=self.receta, cantidad=Decimal("1"))

        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="API",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("1"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            estatus=OrdenCompra.STATUS_ENVIADA,
        )

        url = reverse("api_inventario_sugerencias_compra")
        resp = self.client.get(url, {"plan_id": plan.id})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["scope"]["plan_id"], plan.id)
        self.assertGreaterEqual(payload["totales"]["insumos"], 1)
        self.assertEqual(len(payload["items"]), 1)

        row = payload["items"][0]
        self.assertEqual(row["insumo_id"], self.insumo.id)
        self.assertEqual(Decimal(row["compra_sugerida"]), Decimal("3"))
        self.assertEqual(Decimal(row["costo_compra_sugerida"]), Decimal("30"))
        self.assertEqual(row["estatus"], "BAJO_REORDEN")

    def test_endpoint_inventario_sugerencias_compra_include_all(self):
        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            stock_actual=Decimal("20"),
            punto_reorden=Decimal("5"),
            stock_minimo=Decimal("1"),
            dias_llegada_pedido=1,
            consumo_diario_promedio=Decimal("1"),
        )
        url = reverse("api_inventario_sugerencias_compra")
        resp_default = self.client.get(url)
        self.assertEqual(resp_default.status_code, 200)
        self.assertEqual(resp_default.json()["totales"]["insumos"], 0)

        resp_all = self.client.get(url, {"include_all": 1})
        self.assertEqual(resp_all.status_code, 200)
        self.assertEqual(resp_all.json()["totales"]["insumos"], 1)

    def test_endpoint_compras_solicitud_crea(self):
        url = reverse("api_compras_solicitud")
        resp = self.client.post(
            url,
            {
                "area": "Produccion",
                "solicitante": "coordinador",
                "insumo_id": self.insumo.id,
                "cantidad": "3.500",
                "fecha_requerida": "2026-02-25",
            },
        )
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertEqual(payload["area"], "Produccion")
        self.assertEqual(payload["solicitante"], "coordinador")
        self.assertEqual(payload["insumo_id"], self.insumo.id)
        self.assertEqual(payload["cantidad"], "3.500")
        self.assertTrue(payload["folio"].startswith("SOL-"))

    def test_endpoint_compras_solicitud_auto_crea_orden(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor auto OC", activo=True)
        self.insumo.proveedor_principal = proveedor
        self.insumo.save(update_fields=["proveedor_principal"])
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=proveedor,
            costo_unitario=Decimal("12.5"),
            source_hash="api-oc-auto-1",
        )
        url = reverse("api_compras_solicitud")
        resp = self.client.post(
            url,
            {
                "area": "Compras",
                "solicitante": "api-auto",
                "insumo_id": self.insumo.id,
                "cantidad": "4",
                "estatus": SolicitudCompra.STATUS_APROBADA,
                "auto_crear_orden": True,
                "orden_estatus": OrdenCompra.STATUS_ENVIADA,
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertTrue(payload["auto_crear_orden"])
        self.assertIsNotNone(payload["orden_id"])
        orden = OrdenCompra.objects.get(pk=payload["orden_id"])
        self.assertEqual(orden.solicitud_id, payload["id"])
        self.assertEqual(orden.estatus, OrdenCompra.STATUS_ENVIADA)
        self.assertEqual(orden.monto_estimado, Decimal("50.00"))

    def test_endpoint_compras_solicitud_auto_crear_orden_requiere_aprobada(self):
        url = reverse("api_compras_solicitud")
        resp = self.client.post(
            url,
            {
                "area": "Compras",
                "insumo_id": self.insumo.id,
                "cantidad": "1",
                "estatus": SolicitudCompra.STATUS_BORRADOR,
                "auto_crear_orden": True,
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("auto_crear_orden", resp.json())

    def test_endpoint_compras_solicitud_requiere_rol(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="lector_api",
            email="lector_api@example.com",
            password="test12345",
        )
        self.client.force_login(user)

        url = reverse("api_compras_solicitud")
        resp = self.client.post(
            url,
            {
                "area": "Compras",
                "insumo_id": self.insumo.id,
                "cantidad": "1.000",
            },
        )
        self.assertEqual(resp.status_code, 403)

    def test_endpoint_compras_solicitud_estatus_update(self):
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            cantidad=Decimal("2"),
            estatus=SolicitudCompra.STATUS_BORRADOR,
        )
        url = reverse("api_compras_solicitud_estatus", args=[solicitud.id])
        resp = self.client.post(url, {"estatus": SolicitudCompra.STATUS_EN_REVISION}, content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["updated"])
        self.assertEqual(payload["from"], SolicitudCompra.STATUS_BORRADOR)
        self.assertEqual(payload["to"], SolicitudCompra.STATUS_EN_REVISION)
        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estatus, SolicitudCompra.STATUS_EN_REVISION)

    def test_endpoint_compras_solicitud_estatus_transition_invalida(self):
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            cantidad=Decimal("2"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        url = reverse("api_compras_solicitud_estatus", args=[solicitud.id])
        resp = self.client.post(url, {"estatus": SolicitudCompra.STATUS_RECHAZADA}, content_type="application/json")
        self.assertEqual(resp.status_code, 400)
        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estatus, SolicitudCompra.STATUS_APROBADA)

    def test_endpoint_compras_solicitud_crear_orden(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor flujo API", activo=True)
        self.insumo.proveedor_principal = proveedor
        self.insumo.save(update_fields=["proveedor_principal"])
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=proveedor,
            costo_unitario=Decimal("14.00"),
            source_hash="api-crear-orden-1",
        )
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("3"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )

        url = reverse("api_compras_solicitud_crear_orden", args=[solicitud.id])
        resp = self.client.post(url, {"estatus": OrdenCompra.STATUS_ENVIADA}, content_type="application/json")
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertTrue(payload["created"])
        self.assertEqual(payload["estatus"], OrdenCompra.STATUS_ENVIADA)
        self.assertEqual(Decimal(payload["monto_estimado"]), Decimal("42.00"))
        orden = OrdenCompra.objects.get(pk=payload["id"])
        self.assertEqual(orden.solicitud_id, solicitud.id)

        # Segundo intento: debe responder idempotente con la misma orden activa.
        resp_existing = self.client.post(url, {}, content_type="application/json")
        self.assertEqual(resp_existing.status_code, 200)
        payload_existing = resp_existing.json()
        self.assertFalse(payload_existing["created"])
        self.assertEqual(payload_existing["id"], orden.id)

    def test_endpoint_compras_solicitud_crear_orden_requires_perm(self):
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            cantidad=Decimal("1"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="no_perm_crear_orden",
            email="no_perm_crear_orden@example.com",
            password="test12345",
        )
        self.client.force_login(user)
        url = reverse("api_compras_solicitud_crear_orden", args=[solicitud.id])
        resp = self.client.post(url, {}, content_type="application/json")
        self.assertEqual(resp.status_code, 403)

    def test_endpoint_compras_orden_estatus_update(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor orden estatus", activo=True)
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("2"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        orden = OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            estatus=OrdenCompra.STATUS_BORRADOR,
            monto_estimado=Decimal("25.00"),
        )

        url = reverse("api_compras_orden_estatus", args=[orden.id])
        resp = self.client.post(url, {"estatus": OrdenCompra.STATUS_ENVIADA}, content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["updated"])
        self.assertEqual(payload["from"], OrdenCompra.STATUS_BORRADOR)
        self.assertEqual(payload["to"], OrdenCompra.STATUS_ENVIADA)
        orden.refresh_from_db()
        self.assertEqual(orden.estatus, OrdenCompra.STATUS_ENVIADA)

    def test_endpoint_compras_orden_estatus_no_cierra_sin_recepcion(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor no cerrar", activo=True)
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("2"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        orden = OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            estatus=OrdenCompra.STATUS_CONFIRMADA,
            monto_estimado=Decimal("25.00"),
        )
        url = reverse("api_compras_orden_estatus", args=[orden.id])
        resp = self.client.post(url, {"estatus": OrdenCompra.STATUS_CERRADA}, content_type="application/json")
        self.assertEqual(resp.status_code, 400)
        orden.refresh_from_db()
        self.assertEqual(orden.estatus, OrdenCompra.STATUS_CONFIRMADA)

    def test_endpoint_compras_orden_create_recepcion_cerrada_cierra_orden(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor recepcion close", activo=True)
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("2"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        orden = OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            estatus=OrdenCompra.STATUS_ENVIADA,
            monto_estimado=Decimal("25.00"),
        )
        url = reverse("api_compras_orden_recepciones", args=[orden.id])
        resp = self.client.post(
            url,
            {"estatus": RecepcionCompra.STATUS_CERRADA, "conformidad_pct": "98.50"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertEqual(payload["estatus"], RecepcionCompra.STATUS_CERRADA)
        existencia = ExistenciaInsumo.objects.get(insumo=self.insumo)
        self.assertEqual(existencia.stock_actual, Decimal("2"))
        self.assertEqual(
            MovimientoInventario.objects.filter(
                source_hash=f"recepcion:{payload['id']}:entrada",
                tipo=MovimientoInventario.TIPO_ENTRADA,
            ).count(),
            1,
        )
        orden.refresh_from_db()
        self.assertEqual(orden.estatus, OrdenCompra.STATUS_CERRADA)

    def test_endpoint_compras_recepcion_estatus_update_cierra_orden(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor recepcion update", activo=True)
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("2"),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        orden = OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            estatus=OrdenCompra.STATUS_PARCIAL,
            monto_estimado=Decimal("25.00"),
        )
        recepcion = RecepcionCompra.objects.create(
            orden=orden,
            estatus=RecepcionCompra.STATUS_PENDIENTE,
            conformidad_pct=Decimal("90"),
        )
        url = reverse("api_compras_recepcion_estatus", args=[recepcion.id])
        resp = self.client.post(url, {"estatus": RecepcionCompra.STATUS_CERRADA}, content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["to"], RecepcionCompra.STATUS_CERRADA)
        recepcion.refresh_from_db()
        self.assertEqual(recepcion.estatus, RecepcionCompra.STATUS_CERRADA)
        existencia = ExistenciaInsumo.objects.get(insumo=self.insumo)
        self.assertEqual(existencia.stock_actual, Decimal("2"))
        self.assertEqual(
            MovimientoInventario.objects.filter(source_hash=f"recepcion:{recepcion.id}:entrada").count(),
            1,
        )

        # Repetir cierre no debe duplicar movimiento ni stock.
        resp_retry = self.client.post(url, {"estatus": RecepcionCompra.STATUS_CERRADA}, content_type="application/json")
        self.assertEqual(resp_retry.status_code, 200)
        existencia.refresh_from_db()
        self.assertEqual(existencia.stock_actual, Decimal("2"))
        self.assertEqual(
            MovimientoInventario.objects.filter(source_hash=f"recepcion:{recepcion.id}:entrada").count(),
            1,
        )
        orden.refresh_from_db()
        self.assertEqual(orden.estatus, OrdenCompra.STATUS_CERRADA)

    def test_endpoint_compras_solicitudes_list_filters_and_totals(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor lista solicitudes", activo=True)
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=proveedor,
            costo_unitario=Decimal("11.25"),
            source_hash="api-list-sol-1",
        )
        SolicitudCompra.objects.create(
            area="Compras",
            solicitante="planeacion",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("4"),
            fecha_requerida=date(2026, 2, 18),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        SolicitudCompra.objects.create(
            area="Produccion",
            solicitante="jefe",
            insumo=self.insumo,
            cantidad=Decimal("2"),
            fecha_requerida=date(2026, 3, 2),
            estatus=SolicitudCompra.STATUS_BORRADOR,
        )

        url = reverse("api_compras_solicitudes")
        resp = self.client.get(url, {"mes": "2026-02", "q": "planea"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["filters"]["periodo"], "2026-02")
        self.assertEqual(payload["totales"]["rows"], 1)
        self.assertEqual(Decimal(payload["totales"]["presupuesto_estimado_total"]), Decimal("45.00"))
        self.assertEqual(payload["totales"]["by_status"][SolicitudCompra.STATUS_APROBADA], 1)
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["solicitante"], "planeacion")

    def test_endpoint_compras_ordenes_list_filters_and_totals(self):
        proveedor_a = Proveedor.objects.create(nombre="Proveedor orden A", activo=True)
        proveedor_b = Proveedor.objects.create(nombre="Proveedor orden B", activo=True)
        solicitud_a = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor_a,
            cantidad=Decimal("3"),
            fecha_requerida=date(2026, 2, 10),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        solicitud_b = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor_b,
            cantidad=Decimal("2"),
            fecha_requerida=date(2026, 2, 12),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        OrdenCompra.objects.create(
            solicitud=solicitud_a,
            proveedor=proveedor_a,
            referencia="OC-API-A",
            fecha_emision=date(2026, 2, 14),
            monto_estimado=Decimal("120.50"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )
        OrdenCompra.objects.create(
            solicitud=solicitud_b,
            proveedor=proveedor_b,
            referencia="OC-API-B",
            fecha_emision=date(2026, 2, 15),
            monto_estimado=Decimal("90.00"),
            estatus=OrdenCompra.STATUS_BORRADOR,
        )

        url = reverse("api_compras_ordenes")
        resp = self.client.get(
            url,
            {
                "mes": "2026-02",
                "estatus": OrdenCompra.STATUS_ENVIADA,
                "proveedor_id": proveedor_a.id,
                "q": "API-A",
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["totales"]["rows"], 1)
        self.assertEqual(Decimal(payload["totales"]["monto_estimado_total"]), Decimal("120.50"))
        self.assertEqual(payload["totales"]["by_status"][OrdenCompra.STATUS_ENVIADA], 1)
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["proveedor_id"], proveedor_a.id)

    def test_endpoint_compras_recepciones_list_filters(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor recepciones list", activo=True)
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="api",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("2"),
            fecha_requerida=date(2026, 2, 10),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        orden = OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            referencia="REC-LIST",
            fecha_emision=date(2026, 2, 11),
            monto_estimado=Decimal("55"),
            estatus=OrdenCompra.STATUS_PARCIAL,
        )
        RecepcionCompra.objects.create(
            orden=orden,
            fecha_recepcion=date(2026, 2, 20),
            estatus=RecepcionCompra.STATUS_CERRADA,
            observaciones="Entrada completa",
        )
        RecepcionCompra.objects.create(
            orden=orden,
            fecha_recepcion=date(2026, 3, 1),
            estatus=RecepcionCompra.STATUS_PENDIENTE,
            observaciones="Pendiente validacion",
        )

        url = reverse("api_compras_recepciones")
        resp = self.client.get(url, {"mes": "2026-02", "estatus": RecepcionCompra.STATUS_CERRADA, "q": "completa"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["totales"]["rows"], 1)
        self.assertEqual(payload["totales"]["by_status"][RecepcionCompra.STATUS_CERRADA], 1)
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["estatus"], RecepcionCompra.STATUS_CERRADA)

    def test_endpoint_compras_listados_requiere_permiso_view(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="sin_perm_listados_compras",
            email="sin_perm_listados_compras@example.com",
            password="test12345",
        )
        self.client.force_login(user)

        for name in ("api_compras_solicitudes", "api_compras_ordenes", "api_compras_recepciones"):
            resp = self.client.get(reverse(name))
            self.assertEqual(resp.status_code, 403)

    def test_endpoint_presupuestos_consolidado_periodo(self):
        proveedor = Proveedor.objects.create(nombre="Proveedor Consolidado", lead_time_dias=4, activo=True)
        self.insumo.proveedor_principal = proveedor
        self.insumo.categoria = "Lacteos"
        self.insumo.save(update_fields=["proveedor_principal", "categoria"])

        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=proveedor,
            costo_unitario=Decimal("50"),
            source_hash="api-presupuesto-1",
        )
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="planeacion",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=Decimal("2"),
            fecha_requerida=date(2026, 2, 12),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            fecha_emision=date(2026, 2, 13),
            monto_estimado=Decimal("80"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )
        PresupuestoCompraPeriodo.objects.create(
            periodo_tipo=PresupuestoCompraPeriodo.TIPO_MES,
            periodo_mes="2026-02",
            monto_objetivo=Decimal("120"),
        )

        url = reverse("api_presupuestos_consolidado", args=["2026-02"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["periodo"]["mes"], "2026-02")
        self.assertEqual(payload["periodo"]["tipo"], "mes")
        self.assertEqual(payload["totals"]["solicitudes_count"], 1)
        self.assertAlmostEqual(payload["totals"]["presupuesto_estimado_total"], 100.0, places=2)
        self.assertAlmostEqual(payload["totals"]["presupuesto_ejecutado_total"], 80.0, places=2)
        self.assertAlmostEqual(payload["totals"]["presupuesto_objetivo"], 120.0, places=2)
        self.assertIn("consumo_vs_plan", payload)

    def test_endpoint_presupuestos_consolidado_periodo_invalido(self):
        url = reverse("api_presupuestos_consolidado", args=["2026-99"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 400)

    def test_endpoint_mrp_generar_plan_pronostico(self):
        receta_final = Receta.objects.create(
            nombre="Producto API forecast",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-api-forecast-final",
        )
        receta_prep = Receta.objects.create(
            nombre="Preparacion API forecast",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="hash-api-forecast-prep",
        )
        PronosticoVenta.objects.create(receta=receta_final, periodo="2026-03", cantidad=Decimal("10"))
        PronosticoVenta.objects.create(receta=receta_prep, periodo="2026-03", cantidad=Decimal("5"))

        url = reverse("api_mrp_generar_plan_pronostico")
        resp = self.client.post(
            url,
            {"periodo": "2026-03", "fecha_produccion": "2026-03-12"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertEqual(payload["periodo"], "2026-03")
        self.assertEqual(payload["renglones_creados"], 1)
        plan = PlanProduccion.objects.get(pk=payload["plan_id"])
        self.assertEqual(plan.items.count(), 1)
        self.assertEqual(plan.items.first().receta_id, receta_final.id)

    def test_endpoint_mrp_generar_plan_pronostico_requires_perm(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="api_forecast_sin_perm",
            email="api_forecast_sin_perm@example.com",
            password="test12345",
        )
        self.client.force_login(user)
        url = reverse("api_mrp_generar_plan_pronostico")
        resp = self.client.post(
            url,
            {"periodo": "2026-03"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 403)

    def test_endpoint_ventas_pronostico_estadistico(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        for week in range(1, 7):
            week_start = date(2026, 3, 20) - timedelta(days=(7 * week))
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=sucursal,
                fecha=week_start,
                cantidad=Decimal("10"),
                fuente="API_TEST",
            )
        SolicitudVenta.objects.create(
            receta=self.receta,
            sucursal=sucursal,
            alcance=SolicitudVenta.ALCANCE_SEMANA,
            periodo="2026-03",
            fecha_inicio=date(2026, 3, 16),
            fecha_fin=date(2026, 3, 22),
            cantidad=Decimal("8"),
            fuente="API_TEST",
        )

        url = reverse("api_ventas_pronostico_estadistico")
        resp = self.client.post(
            url,
            {
                "alcance": "semana",
                "fecha_base": "2026-03-20",
                "sucursal_id": sucursal.id,
                "incluir_preparaciones": True,
                "include_solicitud_compare": True,
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["scope"]["alcance"], "semana")
        self.assertEqual(payload["scope"]["sucursal_id"], sucursal.id)
        self.assertGreaterEqual(payload["totals"]["recetas_count"], 1)
        self.assertIn("compare_solicitud", payload)
        self.assertGreaterEqual(len(payload["compare_solicitud"]["rows"]), 1)

    def test_endpoint_ventas_pronostico_backtest(self):
        sucursal = Sucursal.objects.create(codigo="BT", nombre="Backtest", activa=True)
        monthly_data = [
            (2025, 9, "20"),
            (2025, 10, "24"),
            (2025, 11, "28"),
            (2025, 12, "30"),
            (2026, 1, "33"),
            (2026, 2, "36"),
        ]
        for year, month, qty in monthly_data:
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=sucursal,
                fecha=date(year, month, 15),
                cantidad=Decimal(qty),
                fuente="API_TEST",
            )

        url = reverse("api_ventas_pronostico_backtest")
        resp = self.client.post(
            url,
            {
                "alcance": "mes",
                "fecha_base": "2026-03-15",
                "periods": 3,
                "sucursal_id": sucursal.id,
                "incluir_preparaciones": True,
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["scope"]["alcance"], "mes")
        self.assertEqual(payload["scope"]["sucursal_id"], sucursal.id)
        self.assertGreaterEqual(payload["totals"]["windows_evaluated"], 1)
        self.assertIn("windows", payload)
        self.assertGreaterEqual(len(payload["windows"]), 1)
        self.assertIn("mape_promedio", payload["totals"])

    def test_endpoint_ventas_historial_list_filters_and_totals(self):
        sucursal = Sucursal.objects.create(codigo="MAT", nombre="Matriz API", activa=True)
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=sucursal,
            fecha=date(2026, 2, 10),
            cantidad=Decimal("10"),
            tickets=5,
            monto_total=Decimal("320"),
            fuente="API_TEST",
        )
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=sucursal,
            fecha=date(2026, 3, 10),
            cantidad=Decimal("4"),
            tickets=2,
            monto_total=Decimal("120"),
            fuente="API_TEST",
        )

        url = reverse("api_ventas_historial")
        resp = self.client.get(url, {"periodo": "2026-02", "sucursal_id": sucursal.id})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["totales"]["rows"], 1)
        self.assertEqual(Decimal(payload["totales"]["cantidad_total"]), Decimal("10"))
        self.assertEqual(payload["totales"]["tickets_total"], 5)
        self.assertEqual(Decimal(payload["totales"]["monto_total"]), Decimal("320"))
        self.assertEqual(len(payload["items"]), 1)

    def test_endpoint_ventas_pronostico_list_filters_and_totals(self):
        PronosticoVenta.objects.create(
            receta=self.receta,
            periodo="2026-03",
            cantidad=Decimal("18"),
            fuente="API_TEST",
        )
        PronosticoVenta.objects.create(
            receta=self.receta,
            periodo="2026-04",
            cantidad=Decimal("22"),
            fuente="API_TEST",
        )

        url = reverse("api_ventas_pronostico")
        resp = self.client.get(url, {"periodo_desde": "2026-03", "periodo_hasta": "2026-03"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["totales"]["rows"], 1)
        self.assertEqual(Decimal(payload["totales"]["cantidad_total"]), Decimal("18"))
        self.assertEqual(payload["totales"]["periodos_count"], 1)
        self.assertEqual(len(payload["items"]), 1)

    def test_endpoint_ventas_solicitudes_list_filters_and_totals(self):
        sucursal = Sucursal.objects.create(codigo="SUR", nombre="Sucursal Sur API", activa=True)
        SolicitudVenta.objects.create(
            receta=self.receta,
            sucursal=sucursal,
            alcance=SolicitudVenta.ALCANCE_MES,
            periodo="2026-03",
            fecha_inicio=date(2026, 3, 1),
            fecha_fin=date(2026, 3, 31),
            cantidad=Decimal("30"),
            fuente="API_TEST",
        )
        SolicitudVenta.objects.create(
            receta=self.receta,
            sucursal=sucursal,
            alcance=SolicitudVenta.ALCANCE_SEMANA,
            periodo="2026-03",
            fecha_inicio=date(2026, 3, 2),
            fecha_fin=date(2026, 3, 8),
            cantidad=Decimal("8"),
            fuente="API_TEST",
        )

        url = reverse("api_ventas_solicitudes")
        resp = self.client.get(url, {"periodo": "2026-03", "alcance": "MES"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["totales"]["rows"], 1)
        self.assertEqual(Decimal(payload["totales"]["cantidad_total"]), Decimal("30"))
        self.assertEqual(payload["totales"]["by_alcance"][SolicitudVenta.ALCANCE_MES], 1)
        self.assertEqual(len(payload["items"]), 1)

    def test_endpoint_ventas_listados_requires_perm(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="sin_perm_listados_ventas",
            email="sin_perm_listados_ventas@example.com",
            password="test12345",
        )
        self.client.force_login(user)

        for name in ("api_ventas_historial", "api_ventas_pronostico", "api_ventas_solicitudes"):
            resp = self.client.get(reverse(name))
            self.assertEqual(resp.status_code, 403)

    def test_endpoint_ventas_pronostico_bulk_dry_run_y_apply(self):
        url = reverse("api_ventas_pronostico_bulk")

        payload_dry = {
            "dry_run": True,
            "rows": [
                {
                    "receta_id": self.receta.id,
                    "periodo": "2026-03",
                    "cantidad": "18",
                },
                {
                    "receta_id": 999999,
                    "periodo": "2026-03",
                    "cantidad": "10",
                },
            ],
        }
        resp_dry = self.client.post(url, payload_dry, content_type="application/json")
        self.assertEqual(resp_dry.status_code, 200)
        data_dry = resp_dry.json()
        self.assertTrue(data_dry["dry_run"])
        self.assertEqual(data_dry["summary"]["created"], 1)
        self.assertEqual(data_dry["summary"]["skipped"], 1)
        self.assertEqual(PronosticoVenta.objects.count(), 0)

        resp_apply = self.client.post(
            url,
            {
                "dry_run": False,
                "rows": [
                    {"receta_id": self.receta.id, "periodo": "2026-03", "cantidad": "18"},
                ],
            },
            content_type="application/json",
        )
        self.assertEqual(resp_apply.status_code, 200)
        data_apply = resp_apply.json()
        self.assertEqual(data_apply["summary"]["created"], 1)
        self.assertEqual(PronosticoVenta.objects.count(), 1)

        resp_acc = self.client.post(
            url,
            {
                "dry_run": False,
                "modo": "accumulate",
                "rows": [
                    {"receta_id": self.receta.id, "periodo": "2026-03", "cantidad": "2"},
                ],
            },
            content_type="application/json",
        )
        self.assertEqual(resp_acc.status_code, 200)
        data_acc = resp_acc.json()
        self.assertEqual(data_acc["summary"]["updated"], 1)
        pron = PronosticoVenta.objects.get(receta=self.receta, periodo="2026-03")
        self.assertEqual(pron.cantidad, Decimal("20"))

    def test_endpoint_ventas_solicitud_upsert(self):
        sucursal = Sucursal.objects.create(codigo="NORTE", nombre="Sucursal Norte", activa=True)
        url = reverse("api_ventas_solicitud")
        resp = self.client.post(
            url,
            {
                "receta_id": self.receta.id,
                "sucursal_id": sucursal.id,
                "alcance": "mes",
                "periodo": "2026-04",
                "cantidad": "40",
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertTrue(payload["created"])
        self.assertEqual(Decimal(payload["cantidad"]), Decimal("40"))

        resp = self.client.post(
            url,
            {
                "receta_id": self.receta.id,
                "sucursal_id": sucursal.id,
                "alcance": "mes",
                "periodo": "2026-04",
                "cantidad": "52",
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertFalse(payload["created"])
        self.assertEqual(Decimal(payload["cantidad"]), Decimal("52"))

    def test_endpoint_ventas_solicitud_aplicar_forecast(self):
        sucursal = Sucursal.objects.create(codigo="SUR", nombre="Sucursal Sur", activa=True)
        for month_idx, qty in [(10, "30"), (11, "36"), (12, "40"), (1, "44"), (2, "48")]:
            year = 2025 if month_idx >= 10 else 2026
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=sucursal,
                fecha=date(year, month_idx, 15),
                cantidad=Decimal(qty),
                fuente="API_TEST",
            )
        SolicitudVenta.objects.create(
            receta=self.receta,
            sucursal=sucursal,
            alcance=SolicitudVenta.ALCANCE_MES,
            periodo="2026-03",
            fecha_inicio=date(2026, 3, 1),
            fecha_fin=date(2026, 3, 31),
            cantidad=Decimal("100"),
            fuente="API_TEST",
        )

        url = reverse("api_ventas_solicitud_aplicar_forecast")
        resp = self.client.post(
            url,
            {
                "alcance": "mes",
                "periodo": "2026-03",
                "sucursal_id": sucursal.id,
                "incluir_preparaciones": True,
                "modo": "receta",
                "receta_id": self.receta.id,
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["updated"]["created"], 0)
        self.assertGreaterEqual(payload["updated"]["updated"], 1)
        self.assertGreaterEqual(len(payload["adjusted_rows"]), 1)
        updated = SolicitudVenta.objects.get(
            receta=self.receta,
            sucursal=sucursal,
            alcance=SolicitudVenta.ALCANCE_MES,
            fecha_inicio=date(2026, 3, 1),
            fecha_fin=date(2026, 3, 31),
        )
        self.assertNotEqual(updated.cantidad, Decimal("100"))

    def test_endpoint_ventas_historial_bulk_dry_run_y_apply(self):
        sucursal = Sucursal.objects.create(codigo="CENTRO", nombre="Sucursal Centro", activa=True)
        url = reverse("api_ventas_historial_bulk")

        payload_dry = {
            "dry_run": True,
            "fuente": "API_TEST_BULK",
            "rows": [
                {
                    "receta_id": self.receta.id,
                    "fecha": "2026-03-10",
                    "cantidad": "12",
                    "sucursal_id": sucursal.id,
                    "tickets": 5,
                },
                {
                    "receta_id": self.receta.id,
                    "fecha": "2026-03-11",
                    "cantidad": "9",
                },
                {
                    "receta_id": 999999,
                    "fecha": "2026-03-12",
                    "cantidad": "2",
                },
            ],
        }
        resp_dry = self.client.post(url, payload_dry, content_type="application/json")
        self.assertEqual(resp_dry.status_code, 200)
        data_dry = resp_dry.json()
        self.assertTrue(data_dry["dry_run"])
        self.assertEqual(data_dry["summary"]["created"], 2)
        self.assertEqual(data_dry["summary"]["skipped"], 1)
        self.assertEqual(VentaHistorica.objects.count(), 0)

        payload_apply = {
            "dry_run": False,
            "modo": "replace",
            "fuente": "API_TEST_BULK",
            "rows": [
                {
                    "receta_id": self.receta.id,
                    "fecha": "2026-03-10",
                    "cantidad": "12",
                    "sucursal_id": sucursal.id,
                    "tickets": 5,
                },
                {
                    "receta_id": self.receta.id,
                    "fecha": "2026-03-11",
                    "cantidad": "9",
                },
            ],
        }
        resp_apply = self.client.post(url, payload_apply, content_type="application/json")
        self.assertEqual(resp_apply.status_code, 200)
        data_apply = resp_apply.json()
        self.assertFalse(data_apply["dry_run"])
        self.assertEqual(data_apply["summary"]["created"], 2)
        self.assertEqual(VentaHistorica.objects.count(), 2)

        resp_acc = self.client.post(
            url,
            {
                "dry_run": False,
                "modo": "accumulate",
                "rows": [
                    {
                        "receta_id": self.receta.id,
                        "fecha": "2026-03-10",
                        "cantidad": "3",
                        "sucursal_id": sucursal.id,
                        "tickets": 2,
                    }
                ],
            },
            content_type="application/json",
        )
        self.assertEqual(resp_acc.status_code, 200)
        data_acc = resp_acc.json()
        self.assertEqual(data_acc["summary"]["updated"], 1)
        venta = VentaHistorica.objects.get(receta=self.receta, sucursal=sucursal, fecha=date(2026, 3, 10))
        self.assertEqual(venta.cantidad, Decimal("15"))
        self.assertEqual(venta.tickets, 7)

    def test_endpoint_ventas_solicitud_bulk_dry_run_y_apply(self):
        sucursal = Sucursal.objects.create(codigo="PONIENTE", nombre="Sucursal Poniente", activa=True)
        url = reverse("api_ventas_solicitud_bulk")

        payload_dry = {
            "dry_run": True,
            "rows": [
                {
                    "receta_id": self.receta.id,
                    "sucursal_id": sucursal.id,
                    "alcance": "mes",
                    "periodo": "2026-03",
                    "cantidad": "40",
                },
                {
                    "receta_id": self.receta.id,
                    "sucursal_id": sucursal.id,
                    "alcance": "semana",
                    "fecha_base": "2026-03-20",
                    "cantidad": "12",
                },
            ],
        }
        resp_dry = self.client.post(url, payload_dry, content_type="application/json")
        self.assertEqual(resp_dry.status_code, 200)
        data_dry = resp_dry.json()
        self.assertTrue(data_dry["dry_run"])
        self.assertEqual(data_dry["summary"]["created"], 2)
        self.assertEqual(SolicitudVenta.objects.count(), 0)

        payload_apply = dict(payload_dry)
        payload_apply["dry_run"] = False
        resp_apply = self.client.post(url, payload_apply, content_type="application/json")
        self.assertEqual(resp_apply.status_code, 200)
        data_apply = resp_apply.json()
        self.assertEqual(data_apply["summary"]["created"], 2)
        self.assertEqual(SolicitudVenta.objects.count(), 2)

        resp_acc = self.client.post(
            url,
            {
                "dry_run": False,
                "modo": "accumulate",
                "rows": [
                    {
                        "receta_id": self.receta.id,
                        "sucursal_id": sucursal.id,
                        "alcance": "mes",
                        "periodo": "2026-03",
                        "cantidad": "5",
                    }
                ],
            },
            content_type="application/json",
        )
        self.assertEqual(resp_acc.status_code, 200)
        data_acc = resp_acc.json()
        self.assertEqual(data_acc["summary"]["updated"], 1)
        solicitud_mes = SolicitudVenta.objects.get(
            receta=self.receta,
            sucursal=sucursal,
            alcance=SolicitudVenta.ALCANCE_MES,
            fecha_inicio=date(2026, 3, 1),
            fecha_fin=date(2026, 3, 31),
        )
        self.assertEqual(solicitud_mes.cantidad, Decimal("45"))


class InventarioAjustesApiTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser(
            username="admin_api_ajustes",
            email="admin_api_ajustes@example.com",
            password="test12345",
        )
        self.almacen = user_model.objects.create_user(
            username="almacen_api_ajustes",
            email="almacen_api_ajustes@example.com",
            password="test12345",
        )
        group_almacen, _ = Group.objects.get_or_create(name="ALMACEN")
        self.almacen.groups.add(group_almacen)
        self.lector = user_model.objects.create_user(
            username="lector_api_ajustes",
            email="lector_api_ajustes@example.com",
            password="test12345",
        )

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo API Ajuste",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(
            nombre="Harina API Ajustes",
            unidad_base=self.unidad,
            activo=True,
        )
        self.existencia = ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            stock_actual=Decimal("10"),
        )

    def test_list_ajustes_requires_view_perm(self):
        self.client.force_login(self.lector)
        resp = self.client.get(reverse("api_inventario_ajustes"))
        self.assertEqual(resp.status_code, 403)

    def test_almacen_can_create_pending_ajuste(self):
        self.client.force_login(self.almacen)
        resp = self.client.post(
            reverse("api_inventario_ajustes"),
            {
                "insumo_id": self.insumo.id,
                "cantidad_sistema": "10",
                "cantidad_fisica": "8",
                "motivo": "Conteo semanal",
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertEqual(payload["estatus"], AjusteInventario.STATUS_PENDIENTE)
        self.assertEqual(payload["solicitado_por"], self.almacen.username)
        self.existencia.refresh_from_db()
        self.assertEqual(self.existencia.stock_actual, Decimal("10"))

    def test_almacen_cannot_apply_inmediato(self):
        self.client.force_login(self.almacen)
        resp = self.client.post(
            reverse("api_inventario_ajustes"),
            {
                "insumo_id": self.insumo.id,
                "cantidad_sistema": "10",
                "cantidad_fisica": "7",
                "motivo": "Conteo semanal",
                "aplicar_inmediato": True,
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(AjusteInventario.objects.count(), 0)

    def test_admin_create_apply_inmediato_updates_stock(self):
        self.client.force_login(self.admin)
        resp = self.client.post(
            reverse("api_inventario_ajustes"),
            {
                "insumo_id": self.insumo.id,
                "cantidad_sistema": "10",
                "cantidad_fisica": "13",
                "motivo": "Correccion entrada",
                "aplicar_inmediato": True,
                "comentario_revision": "ok admin",
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 201)
        payload = resp.json()
        self.assertTrue(payload["aplicado"])
        self.assertEqual(payload["estatus"], AjusteInventario.STATUS_APLICADO)
        self.existencia.refresh_from_db()
        self.assertEqual(self.existencia.stock_actual, Decimal("13"))
        mov = MovimientoInventario.objects.get(referencia=payload["folio"])
        self.assertEqual(mov.tipo, MovimientoInventario.TIPO_ENTRADA)
        self.assertEqual(mov.cantidad, Decimal("3"))

    def test_admin_can_decide_approve(self):
        ajuste = AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema=Decimal("10"),
            cantidad_fisica=Decimal("7"),
            motivo="Merma",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.almacen,
        )
        self.client.force_login(self.admin)
        resp = self.client.post(
            reverse("api_inventario_ajuste_decision", args=[ajuste.id]),
            {"action": "approve", "comentario_revision": "aprobado api"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["estatus"], AjusteInventario.STATUS_APLICADO)
        ajuste.refresh_from_db()
        self.assertEqual(ajuste.estatus, AjusteInventario.STATUS_APLICADO)
        self.existencia.refresh_from_db()
        self.assertEqual(self.existencia.stock_actual, Decimal("7"))
        mov = MovimientoInventario.objects.get(referencia=ajuste.folio)
        self.assertEqual(mov.tipo, MovimientoInventario.TIPO_SALIDA)
        self.assertEqual(mov.cantidad, Decimal("3"))

    def test_admin_can_decide_reject(self):
        ajuste = AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema=Decimal("10"),
            cantidad_fisica=Decimal("6"),
            motivo="Revision",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.almacen,
        )
        self.client.force_login(self.admin)
        resp = self.client.post(
            reverse("api_inventario_ajuste_decision", args=[ajuste.id]),
            {"action": "reject", "comentario_revision": "no procede"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["estatus"], AjusteInventario.STATUS_RECHAZADO)
        ajuste.refresh_from_db()
        self.assertEqual(ajuste.estatus, AjusteInventario.STATUS_RECHAZADO)
        self.existencia.refresh_from_db()
        self.assertEqual(self.existencia.stock_actual, Decimal("10"))
        self.assertEqual(MovimientoInventario.objects.count(), 0)
