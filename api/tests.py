from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import OperationalError
from django.test import TestCase
from django.urls import reverse

from compras.models import OrdenCompra, PresupuestoCompraPeriodo, SolicitudCompra
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, PronosticoVenta, Receta
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
