from datetime import date, timedelta
from decimal import Decimal
import os
import tempfile
from io import BytesIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import OperationalError
from django.test import TestCase
from django.urls import reverse
from openpyxl import Workbook, load_workbook

from compras.models import OrdenCompra, SolicitudCompra
from core.models import Sucursal
from maestros.models import Insumo, Proveedor, UnidadMedida
from recetas.models import (
    CostoDriver,
    LineaReceta,
    PlanProduccion,
    PlanProduccionItem,
    PronosticoVenta,
    Receta,
    RecetaCostoVersion,
    SolicitudVenta,
    VentaHistorica,
)
from recetas.utils.costeo_versionado import asegurar_version_costeo, calcular_costeo_receta
from recetas.utils.importador import ImportadorCosteo


class MatchingPendientesAutocompleteTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_match",
            email="admin_match@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo_1 = Insumo.objects.create(nombre="Harina Pastelera", unidad_base=unidad, activo=True)
        self.insumo_2 = Insumo.objects.create(nombre="Harina Integral", unidad_base=unidad, activo=True)
        Insumo.objects.create(nombre="Mantequilla", unidad_base=unidad, activo=True)

        receta = Receta.objects.create(nombre="Receta Test Match", hash_contenido="hash-match-test-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Harina",
            cantidad=Decimal("1"),
            unidad=unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("0"),
            match_status=LineaReceta.STATUS_NEEDS_REVIEW,
            match_score=85,
            match_method="FUZZY",
        )

    def test_matching_pendientes_view_loads(self):
        response = self.client.get(reverse("recetas:matching_pendientes"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Matching pendientes")

    def test_matching_pendientes_export_csv(self):
        response = self.client.get(reverse("recetas:matching_pendientes"), {"export": "csv", "q": "Harina"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        body = response.content.decode("utf-8")
        self.assertIn("receta,posicion,ingrediente,metodo,score,insumo_ligado", body)
        self.assertIn("Receta Test Match", body)

    def test_matching_pendientes_export_xlsx(self):
        response = self.client.get(reverse("recetas:matching_pendientes"), {"export": "xlsx", "q": "Harina"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response["Content-Type"],
        )
        wb = load_workbook(BytesIO(response.content), data_only=True)
        ws = wb.active
        headers = [ws.cell(row=1, column=i).value for i in range(1, 7)]
        self.assertEqual(headers, ["receta", "posicion", "ingrediente", "metodo", "score", "insumo_ligado"])
        self.assertEqual(ws.cell(row=2, column=1).value, "Receta Test Match")
        self.assertEqual(ws.cell(row=2, column=3).value, "Harina")

    def test_matching_pendientes_context_stats(self):
        response = self.client.get(reverse("recetas:matching_pendientes"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["stats"]["total"], 1)
        self.assertEqual(response.context["stats"]["recetas"], 1)
        self.assertEqual(response.context["stats"]["fuzzy"], 1)
        self.assertEqual(response.context["stats"]["no_match"], 0)

    def test_matching_insumos_search_filters_by_query(self):
        response = self.client.get(
            reverse("recetas:matching_insumos_search"),
            {"q": "harina", "limit": "10"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        names = [x["nombre"] for x in payload["results"]]
        self.assertIn(self.insumo_1.nombre, names)
        self.assertIn(self.insumo_2.nombre, names)
        self.assertNotIn("Mantequilla", names)

    def test_matching_insumos_search_enforces_limit(self):
        response = self.client.get(
            reverse("recetas:matching_insumos_search"),
            {"q": "harina", "limit": "1"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(len(payload["results"]), 1)


class RecetasAuthRedirectTests(TestCase):
    def test_drivers_legacy_redirects_to_login_when_anonymous(self):
        response = self.client.get(reverse("recetas:drivers_costeo_legacy"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response["Location"])
        self.assertIn("next=", response["Location"])

    def test_pronosticos_legacy_redirects_to_login_when_anonymous(self):
        response = self.client.get(reverse("recetas:pronosticos_legacy"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response["Location"])
        self.assertIn("next=", response["Location"])

    def test_matching_pendientes_redirects_to_login_when_anonymous(self):
        response = self.client.get(reverse("recetas:matching_pendientes"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response["Location"])
        self.assertIn("next=", response["Location"])

    def test_plan_produccion_redirects_to_login_when_anonymous(self):
        response = self.client.get(reverse("recetas:plan_produccion"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response["Location"])
        self.assertIn("next=", response["Location"])


class PlanProduccionRobustnessTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_plan_robust",
            email="admin_plan_robust@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(nombre="Insumo robust", unidad_base=self.unidad, activo=True)
        self.receta = Receta.objects.create(nombre="Receta robust", hash_contenido="hash-robust-001")
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Insumo robust",
            cantidad=Decimal("1"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("10"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )
        self.plan = PlanProduccion.objects.create(
            nombre="Plan robust",
            fecha_produccion=date(2026, 2, 20),
        )
        PlanProduccionItem.objects.create(
            plan=self.plan,
            receta=self.receta,
            cantidad=Decimal("2"),
        )

    def test_plan_produccion_graceful_when_pronostico_table_unavailable(self):
        with patch("recetas.views.PronosticoVenta.objects.filter", side_effect=OperationalError("missing table")):
            response = self.client.get(reverse("recetas:plan_produccion"), {"plan_id": self.plan.id})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["pronosticos_unavailable"])
        self.assertTrue(response.context["plan_vs_pronostico"]["pronosticos_unavailable"])


class PlanProduccionAdminComparativoTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_plan_admin",
            email="admin_plan_admin@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        insumo = Insumo.objects.create(nombre="Insumo admin", unidad_base=unidad, activo=True)
        receta = Receta.objects.create(nombre="Receta admin", hash_contenido="hash-admin-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=insumo,
            insumo_texto="Insumo admin",
            cantidad=Decimal("1"),
            unidad=unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("5"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )
        self.plan = PlanProduccion.objects.create(nombre="Plan admin", fecha_produccion=date(2026, 2, 10))
        PlanProduccionItem.objects.create(plan=self.plan, receta=receta, cantidad=Decimal("3"))
        PronosticoVenta.objects.create(receta=receta, periodo="2026-02", cantidad=Decimal("2"))

    def test_admin_comparativo_view_loads(self):
        response = self.client.get(
            reverse("admin:recetas_planproduccion_comparativo_pronostico", args=[self.plan.id])
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Comparativo Plan vs Pronóstico")
        self.assertContains(response, "Plan admin")
        self.assertContains(response, "Receta admin")

    def test_admin_changelist_shows_comparativo_link(self):
        response = self.client.get(reverse("admin:recetas_planproduccion_changelist"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ver plan vs pronóstico")


class PlanProduccionPeriodoMrpTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_plan_periodo",
            email="admin_plan_periodo@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(nombre="Insumo periodo", unidad_base=unidad, activo=True)
        self.receta = Receta.objects.create(nombre="Receta periodo", hash_contenido="hash-periodo-001")
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Insumo periodo",
            cantidad=Decimal("2"),
            unidad=unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("10"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )

        self.plan_q1 = PlanProduccion.objects.create(
            nombre="Plan q1",
            fecha_produccion=date(2026, 2, 10),
        )
        self.plan_q2 = PlanProduccion.objects.create(
            nombre="Plan q2",
            fecha_produccion=date(2026, 2, 22),
        )
        PlanProduccionItem.objects.create(plan=self.plan_q1, receta=self.receta, cantidad=Decimal("1"))
        PlanProduccionItem.objects.create(plan=self.plan_q2, receta=self.receta, cantidad=Decimal("3"))

    def test_plan_produccion_contexto_mrp_periodo_mes(self):
        response = self.client.get(
            reverse("recetas:plan_produccion"),
            {"mrp_periodo": "2026-02", "mrp_periodo_tipo": "mes"},
        )
        self.assertEqual(response.status_code, 200)
        resumen = response.context["mrp_periodo_resumen"]
        self.assertEqual(resumen["planes_count"], 2)
        self.assertEqual(resumen["insumos_count"], 1)
        self.assertEqual(resumen["periodo"], "2026-02")
        self.assertEqual(resumen["periodo_tipo"], "mes")
        self.assertEqual(resumen["insumos"][0]["cantidad"], Decimal("8"))
        self.assertEqual(resumen["costo_total"], Decimal("80"))

    def test_plan_produccion_contexto_mrp_periodo_q1_filtra_planes(self):
        response = self.client.get(
            reverse("recetas:plan_produccion"),
            {"mrp_periodo": "2026-02", "mrp_periodo_tipo": "q1"},
        )
        self.assertEqual(response.status_code, 200)
        resumen = response.context["mrp_periodo_resumen"]
        self.assertEqual(resumen["planes_count"], 1)
        self.assertEqual(len(resumen["planes"]), 1)
        self.assertEqual(resumen["planes"][0]["id"], self.plan_q1.id)
        self.assertEqual(resumen["insumos"][0]["cantidad"], Decimal("2"))

    def test_plan_produccion_periodo_export_csv(self):
        response = self.client.get(
            reverse("recetas:plan_produccion_periodo_export"),
            {"mrp_periodo": "2026-02", "mrp_periodo_tipo": "mes", "format": "csv"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        content = response.content.decode("utf-8")
        self.assertIn("MRP CONSOLIDADO POR PERIODO", content)
        self.assertIn("Insumo periodo", content)

    def test_plan_produccion_periodo_export_xlsx(self):
        response = self.client.get(
            reverse("recetas:plan_produccion_periodo_export"),
            {"mrp_periodo": "2026-02", "mrp_periodo_tipo": "q2", "format": "xlsx"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("spreadsheetml", response["Content-Type"])
        wb = load_workbook(BytesIO(response.content), data_only=True)
        self.assertIn("Resumen", wb.sheetnames)
        self.assertIn("Planes", wb.sheetnames)
        self.assertIn("Insumos", wb.sheetnames)


class PlanProduccionSolicitudesModeTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_plan",
            email="admin_plan@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.proveedor = Proveedor.objects.create(nombre="Proveedor Plan", activo=True)
        self.insumo = Insumo.objects.create(
            nombre="Harina Plan",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.receta = Receta.objects.create(nombre="Receta Plan", hash_contenido="hash-plan-001")
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Harina Plan",
            cantidad=Decimal("2"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("5"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )
        self.plan = PlanProduccion.objects.create(nombre="Plan Test", fecha_produccion=date(2026, 2, 20))
        PlanProduccionItem.objects.create(plan=self.plan, receta=self.receta, cantidad=Decimal("1"))

    def test_generar_solicitudes_accumulate_mode_updates_existing(self):
        url = reverse("recetas:plan_produccion_generar_solicitudes", args=[self.plan.id])

        response_1 = self.client.post(
            url,
            {"next_view": "plan", "replace_prev": "1", "auto_create_oc": "1"},
        )
        self.assertEqual(response_1.status_code, 302)

        response_2 = self.client.post(
            url,
            {"next_view": "plan", "replace_prev": "0", "auto_create_oc": "1"},
        )
        self.assertEqual(response_2.status_code, 302)

        solicitudes = SolicitudCompra.objects.filter(
            area=f"PLAN_PRODUCCION:{self.plan.id}",
            estatus=SolicitudCompra.STATUS_BORRADOR,
            insumo=self.insumo,
        )
        self.assertEqual(solicitudes.count(), 1)
        self.assertEqual(solicitudes.first().cantidad, Decimal("4"))

        ocs = OrdenCompra.objects.filter(
            referencia=f"PLAN_PRODUCCION:{self.plan.id}",
            estatus=OrdenCompra.STATUS_BORRADOR,
            solicitud__isnull=True,
            proveedor=self.proveedor,
        )
        self.assertEqual(ocs.count(), 1)
        self.assertEqual(ocs.first().monto_estimado, Decimal("20"))


class RecetaCosteoVersionadoTests(TestCase):
    def setUp(self):
        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(nombre="Harina Base", unidad_base=self.unidad, activo=True)
        self.receta = Receta.objects.create(
            nombre="Batida Vainilla",
            sheet_name="Insumos 1",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("10"),
            rendimiento_unidad=self.unidad,
            hash_contenido="hash-versionado-001",
        )
        self.linea = LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Harina Base",
            cantidad=Decimal("2"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("4"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )

    def test_aplica_driver_producto_y_versiona_por_cambio(self):
        CostoDriver.objects.create(
            nombre="Driver Producto Batida",
            scope=CostoDriver.SCOPE_PRODUCTO,
            receta=self.receta,
            mo_pct=Decimal("10"),
            indirecto_pct=Decimal("5"),
            mo_fijo=Decimal("1"),
            indirecto_fijo=Decimal("2"),
            prioridad=10,
        )

        v1, created_1 = asegurar_version_costeo(self.receta, fuente="TEST")
        self.assertTrue(created_1)
        self.assertEqual(v1.version_num, 1)
        self.assertEqual(v1.costo_mp, Decimal("8.000000"))
        self.assertEqual(v1.costo_mo, Decimal("1.800000"))
        self.assertEqual(v1.costo_indirecto, Decimal("2.400000"))
        self.assertEqual(v1.costo_total, Decimal("12.200000"))
        self.assertEqual(v1.costo_por_unidad_rendimiento, Decimal("1.220000"))

        self.linea.costo_unitario_snapshot = Decimal("5")
        self.linea.save(update_fields=["costo_unitario_snapshot"])
        v2, created_2 = asegurar_version_costeo(self.receta, fuente="TEST")
        self.assertTrue(created_2)
        self.assertEqual(v2.version_num, 2)
        self.assertEqual(v2.costo_mp, Decimal("10.000000"))
        self.assertEqual(v2.costo_total, Decimal("14.500000"))

        self.assertEqual(RecetaCostoVersion.objects.filter(receta=self.receta).count(), 2)

    def test_calculo_no_driver_solo_mp(self):
        breakdown = calcular_costeo_receta(self.receta)
        self.assertEqual(breakdown.costo_mp, Decimal("8.000000"))
        self.assertEqual(breakdown.costo_mo, Decimal("0.000000"))
        self.assertEqual(breakdown.costo_indirecto, Decimal("0.000000"))
        self.assertEqual(breakdown.costo_total, Decimal("8.000000"))


class PronosticoImportViewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_pronostico",
            email="admin_pronostico@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)
        self.receta = Receta.objects.create(
            nombre="Pastel Fresas Con Crema - Chico",
            codigo_point="PFC-CHICO",
            hash_contenido="hash-pronostico-001",
        )

    def test_import_replace_crea_pronosticos(self):
        csv_data = (
            "receta,codigo_point,periodo,cantidad\n"
            "Pastel Fresas Con Crema - Chico,PFC-CHICO,2026-02,120\n"
        ).encode("utf-8")
        upload = SimpleUploadedFile("pronostico.csv", csv_data, content_type="text/csv")
        response = self.client.post(
            reverse("recetas:pronosticos_importar"),
            {
                "modo": "replace",
                "periodo_default": "2026-02",
                "fuente": "TEST_UI",
                "archivo": upload,
            },
        )
        self.assertEqual(response.status_code, 302)
        record = PronosticoVenta.objects.get(receta=self.receta, periodo="2026-02")
        self.assertEqual(record.cantidad, Decimal("120"))
        self.assertEqual(record.fuente, "TEST_UI")

    def test_import_accumulate_acumula_cantidad(self):
        PronosticoVenta.objects.create(
            receta=self.receta,
            periodo="2026-02",
            cantidad=Decimal("100"),
            fuente="MANUAL",
        )
        csv_data = (
            "codigo_point,periodo,cantidad\n"
            "PFC-CHICO,2026-02,20\n"
        ).encode("utf-8")
        upload = SimpleUploadedFile("pronostico.csv", csv_data, content_type="text/csv")
        response = self.client.post(
            reverse("recetas:pronosticos_importar"),
            {
                "modo": "accumulate",
                "periodo_default": "2026-02",
                "fuente": "TEST_UI",
                "archivo": upload,
            },
        )
        self.assertEqual(response.status_code, 302)
        record = PronosticoVenta.objects.get(receta=self.receta, periodo="2026-02")
        self.assertEqual(record.cantidad, Decimal("120"))


class PlanGeneradoDesdePronosticoTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_plan_gen",
            email="admin_plan_gen@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.receta_final = Receta.objects.create(
            nombre="Producto Final Test",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-plan-gen-final",
        )
        self.receta_prep = Receta.objects.create(
            nombre="Preparacion Test",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="hash-plan-gen-prep",
        )
        PronosticoVenta.objects.create(receta=self.receta_final, periodo="2026-03", cantidad=Decimal("12"))
        PronosticoVenta.objects.create(receta=self.receta_prep, periodo="2026-03", cantidad=Decimal("8"))

    def test_genera_plan_desde_pronostico_solo_producto_final_por_default(self):
        response = self.client.post(
            reverse("recetas:plan_produccion_generar_desde_pronostico"),
            {
                "periodo": "2026-03",
                "fecha_produccion": "2026-03-10",
                "nombre": "Plan desde pronostico test",
            },
        )
        self.assertEqual(response.status_code, 302)
        plan = PlanProduccion.objects.get(nombre="Plan desde pronostico test")
        items = list(plan.items.select_related("receta").all())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].receta_id, self.receta_final.id)
        self.assertEqual(items[0].cantidad, Decimal("12"))

    def test_genera_plan_desde_pronostico_incluyendo_preparaciones(self):
        response = self.client.post(
            reverse("recetas:plan_produccion_generar_desde_pronostico"),
            {
                "periodo": "2026-03",
                "fecha_produccion": "2026-03-12",
                "nombre": "Plan pronostico con preparaciones",
                "incluir_preparaciones": "1",
            },
        )
        self.assertEqual(response.status_code, 302)
        plan = PlanProduccion.objects.get(nombre="Plan pronostico con preparaciones")
        self.assertEqual(plan.items.count(), 2)


class PronosticoEstadisticoDesdeHistorialTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_hist_forecast",
            email="admin_hist_forecast@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.receta = Receta.objects.create(
            nombre="Pastel Forecast",
            codigo_point="P-FC-01",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-forecast-hist-001",
        )

    def test_import_ventas_historicas_crea_registros(self):
        csv_data = (
            "receta,codigo_point,sucursal_codigo,fecha,cantidad,tickets,monto_total\n"
            "Pastel Forecast,P-FC-01,MATRIZ,2026-01-10,12,8,1200\n"
            "Pastel Forecast,P-FC-01,MATRIZ,2026-01-11,9,6,900\n"
        ).encode("utf-8")
        upload = SimpleUploadedFile("ventas.csv", csv_data, content_type="text/csv")
        response = self.client.post(
            reverse("recetas:ventas_historicas_importar"),
            {"archivo": upload, "modo": "replace", "fuente": "TEST_VENTAS"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(VentaHistorica.objects.count(), 2)
        row = VentaHistorica.objects.order_by("fecha").first()
        self.assertEqual(row.sucursal_id, self.sucursal.id)
        self.assertEqual(row.receta_id, self.receta.id)
        self.assertEqual(row.fuente, "TEST_VENTAS")

    def test_pronostico_estadistico_crea_plan(self):
        base = date(2026, 3, 12)  # jueves
        # 8 semanas de historial con patrón estable por día.
        for w in range(1, 9):
            week_start = base - timedelta(days=base.weekday()) - timedelta(days=(7 * w))
            for d in range(7):
                VentaHistorica.objects.create(
                    receta=self.receta,
                    sucursal=self.sucursal,
                    fecha=week_start + timedelta(days=d),
                    cantidad=Decimal("5"),
                    fuente="TEST_SEMANA",
                )

        response = self.client.post(
            reverse("recetas:pronostico_estadistico_desde_historial"),
            {
                "alcance": "semana",
                "fecha_base": "2026-03-12",
                "periodo": "2026-03",
                "sucursal_id": str(self.sucursal.id),
                "run_mode": "crear_plan",
                "safety_pct": "0",
            },
        )
        self.assertEqual(response.status_code, 302)
        plan = PlanProduccion.objects.order_by("-id").first()
        self.assertIsNotNone(plan)
        self.assertIn("pronóstico estadístico", plan.notas.lower())
        self.assertEqual(plan.items.count(), 1)
        self.assertGreater(plan.items.first().cantidad, Decimal("0"))

        preview = self.client.session.get("pronostico_estadistico_preview")
        self.assertIsNotNone(preview)
        self.assertEqual(preview["sucursal_nombre"], "MATRIZ - Matriz")
        self.assertGreaterEqual(preview["totals"]["recetas_count"], 1)

    def test_pronostico_estadistico_aplica_pronostico_mensual(self):
        for month_idx, qty in [(10, "40"), (11, "50"), (12, "60"), (1, "65"), (2, "70")]:
            year = 2025 if month_idx >= 10 else 2026
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=self.sucursal,
                fecha=date(year, month_idx, 15),
                cantidad=Decimal(qty),
                fuente="TEST_MENSUAL",
            )

        response = self.client.post(
            reverse("recetas:pronostico_estadistico_desde_historial"),
            {
                "alcance": "mes",
                "periodo": "2026-03",
                "sucursal_id": str(self.sucursal.id),
                "run_mode": "apply_pronostico",
                "safety_pct": "0",
            },
        )
        self.assertEqual(response.status_code, 302)
        pron = PronosticoVenta.objects.filter(receta=self.receta, periodo="2026-03").first()
        self.assertIsNotNone(pron)
        self.assertGreater(pron.cantidad, Decimal("0"))

    def test_pronostico_estadistico_backtest_genera_contexto(self):
        for month_idx, qty in [(9, "30"), (10, "35"), (11, "40"), (12, "46"), (1, "50"), (2, "55"), (3, "60")]:
            year = 2025 if month_idx >= 9 else 2026
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=self.sucursal,
                fecha=date(year, month_idx, 15),
                cantidad=Decimal(qty),
                fuente="TEST_BACKTEST",
            )

        response = self.client.post(
            reverse("recetas:pronostico_estadistico_desde_historial"),
            {
                "alcance": "mes",
                "periodo": "2026-04",
                "fecha_base": "2026-04-15",
                "sucursal_id": str(self.sucursal.id),
                "run_mode": "backtest",
                "backtest_periods": "4",
                "backtest_top": "5",
                "safety_pct": "0",
            },
        )
        self.assertEqual(response.status_code, 302)
        preview = self.client.session.get("pronostico_backtest_preview")
        self.assertIsNotNone(preview)
        self.assertGreaterEqual(preview["totals"]["windows_evaluated"], 1)
        self.assertIn("mape_promedio", preview["totals"])
        self.assertGreaterEqual(len(preview["windows"]), 1)

        response = self.client.get(reverse("recetas:plan_produccion"), {"periodo": "2026-04"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("forecast_backtest", response.context)
        self.assertIsNotNone(response.context["forecast_backtest"])


class SolicitudVentasForecastTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_sol_ventas",
            email="admin_sol_ventas@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.receta = Receta.objects.create(
            nombre="Pastel Solicitud",
            codigo_point="P-SOL-01",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-sol-ventas-001",
        )

    def test_guardar_solicitud_venta_reemplaza_por_rango(self):
        payload = {
            "receta_id": str(self.receta.id),
            "sucursal_id": str(self.sucursal.id),
            "alcance": "mes",
            "periodo": "2026-04",
            "cantidad": "120",
            "fuente": "TEST_SOL",
        }
        response = self.client.post(reverse("recetas:solicitud_ventas_guardar"), payload)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(SolicitudVenta.objects.count(), 1)
        row = SolicitudVenta.objects.first()
        self.assertEqual(row.periodo, "2026-04")
        self.assertEqual(row.cantidad, Decimal("120"))

        payload["cantidad"] = "145"
        response = self.client.post(reverse("recetas:solicitud_ventas_guardar"), payload)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(SolicitudVenta.objects.count(), 1)
        row.refresh_from_db()
        self.assertEqual(row.cantidad, Decimal("145"))

    def test_import_solicitud_ventas_csv_crea_registros(self):
        csv_data = (
            "codigo_point,sucursal_codigo,alcance,periodo,cantidad\n"
            "P-SOL-01,MATRIZ,MES,2026-04,88\n"
            "P-SOL-01,MATRIZ,SEMANA,2026-04,24\n"
        ).encode("utf-8")
        upload = SimpleUploadedFile("solicitudes_ventas.csv", csv_data, content_type="text/csv")
        response = self.client.post(
            reverse("recetas:solicitud_ventas_importar"),
            {
                "archivo": upload,
                "modo": "replace",
                "periodo_default": "2026-04",
                "fecha_base_default": "2026-04-10",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(SolicitudVenta.objects.count(), 2)

    def test_contexto_muestra_comparativo_pronostico_vs_solicitud(self):
        for month_idx, qty in [(11, "60"), (12, "72"), (1, "81"), (2, "78"), (3, "90")]:
            year = 2025 if month_idx >= 11 else 2026
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=self.sucursal,
                fecha=date(year, month_idx, 15),
                cantidad=Decimal(qty),
                fuente="TEST_HIST_SOL",
            )

        SolicitudVenta.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            alcance=SolicitudVenta.ALCANCE_MES,
            periodo="2026-04",
            fecha_inicio=date(2026, 4, 1),
            fecha_fin=date(2026, 4, 30),
            cantidad=Decimal("110"),
            fuente="TEST_SOL",
        )

        response = self.client.post(
            reverse("recetas:pronostico_estadistico_desde_historial"),
            {
                "alcance": "mes",
                "periodo": "2026-04",
                "sucursal_id": str(self.sucursal.id),
                "run_mode": "preview",
                "safety_pct": "0",
            },
        )
        self.assertEqual(response.status_code, 302)

        response = self.client.get(reverse("recetas:plan_produccion"), {"periodo": "2026-04"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("forecast_vs_solicitud", response.context)
        compare = response.context["forecast_vs_solicitud"]
        self.assertIsNotNone(compare)
        self.assertGreater(len(compare["rows"]), 0)
        row = next((r for r in compare["rows"] if r["receta_id"] == self.receta.id), None)
        self.assertIsNotNone(row)
        self.assertEqual(row["solicitud_qty"], Decimal("110"))

    def test_aplicar_ajuste_desde_forecast_actualiza_solicitud(self):
        for month_idx, qty in [(11, "60"), (12, "72"), (1, "81"), (2, "78"), (3, "90")]:
            year = 2025 if month_idx >= 11 else 2026
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=self.sucursal,
                fecha=date(year, month_idx, 15),
                cantidad=Decimal(qty),
                fuente="TEST_HIST_SOL",
            )

        SolicitudVenta.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            alcance=SolicitudVenta.ALCANCE_MES,
            periodo="2026-04",
            fecha_inicio=date(2026, 4, 1),
            fecha_fin=date(2026, 4, 30),
            cantidad=Decimal("110"),
            fuente="TEST_SOL",
        )

        response = self.client.post(
            reverse("recetas:pronostico_estadistico_desde_historial"),
            {
                "alcance": "mes",
                "periodo": "2026-04",
                "sucursal_id": str(self.sucursal.id),
                "run_mode": "preview",
                "safety_pct": "0",
            },
        )
        self.assertEqual(response.status_code, 302)

        response = self.client.get(reverse("recetas:plan_produccion"), {"periodo": "2026-04"})
        self.assertEqual(response.status_code, 200)
        compare = response.context["forecast_vs_solicitud"]
        row = next((r for r in compare["rows"] if r["receta_id"] == self.receta.id), None)
        self.assertIsNotNone(row)
        expected_qty = Decimal(str(row["forecast_qty"]))

        response = self.client.post(
            reverse("recetas:solicitud_ventas_aplicar_desde_forecast"),
            {
                "modo": "receta",
                "receta_id": str(self.receta.id),
            },
        )
        self.assertEqual(response.status_code, 302)

        solicitud = SolicitudVenta.objects.get(
            receta=self.receta,
            sucursal=self.sucursal,
            alcance=SolicitudVenta.ALCANCE_MES,
            fecha_inicio=date(2026, 4, 1),
            fecha_fin=date(2026, 4, 30),
        )
        self.assertEqual(solicitud.cantidad, expected_qty)


class RecetaPhase2ViewsTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_phase2",
            email="admin_phase2@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(nombre="Insumo Driver", unidad_base=self.unidad, activo=True)
        self.receta = Receta.objects.create(
            nombre="Receta Driver",
            sheet_name="Insumos 1",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("8"),
            rendimiento_unidad=self.unidad,
            hash_contenido="hash-phase2-views-001",
        )
        self.linea = LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Insumo Driver",
            cantidad=Decimal("2"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("4"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )
        asegurar_version_costeo(self.receta, fuente="TEST_PHASE2")
        self.linea.costo_unitario_snapshot = Decimal("5")
        self.linea.save(update_fields=["costo_unitario_snapshot"])
        asegurar_version_costeo(self.receta, fuente="TEST_PHASE2")

    def test_receta_detail_renderiza_comparador_y_export(self):
        resp = self.client.get(reverse("recetas:receta_detail", args=[self.receta.id]))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Comparar versiones")
        self.assertContains(resp, "Exportar versiones CSV")
        self.assertContains(resp, "Drivers costeo")

    def test_receta_versiones_export_csv(self):
        resp = self.client.get(
            reverse("recetas:receta_versiones_export", args=[self.receta.id]),
            {"format": "csv"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/csv", resp["Content-Type"])
        self.assertIn("version,fecha,fuente", resp.content.decode("utf-8"))

    def test_receta_versiones_export_xlsx(self):
        resp = self.client.get(
            reverse("recetas:receta_versiones_export", args=[self.receta.id]),
            {"format": "xlsx"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("spreadsheetml", resp["Content-Type"])

    def test_receta_detail_handles_missing_version_table_gracefully(self):
        with patch("recetas.views._load_versiones_costeo", side_effect=OperationalError("missing table")):
            resp = self.client.get(reverse("recetas:receta_detail", args=[self.receta.id]))
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.context["versiones_unavailable"])
        self.assertContains(resp, "no está disponible en este entorno")

    def test_receta_detail_handles_missing_driver_table_gracefully(self):
        with patch("recetas.views.calcular_costeo_receta", side_effect=OperationalError("missing table")):
            resp = self.client.get(reverse("recetas:receta_detail", args=[self.receta.id]))
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.context["costeo_unavailable"])
        self.assertContains(resp, "costeo avanzado por drivers no está disponible")

    def test_receta_versiones_export_handles_missing_version_table_gracefully(self):
        with patch("recetas.views._load_versiones_costeo", side_effect=OperationalError("missing table")):
            resp = self.client.get(
                reverse("recetas:receta_versiones_export", args=[self.receta.id]),
                {"format": "csv"},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/csv", resp["Content-Type"])
        self.assertIn("version,fecha,fuente", resp.content.decode("utf-8"))

    def test_drivers_costeo_create_and_list(self):
        payload = {
            "scope": CostoDriver.SCOPE_PRODUCTO,
            "nombre": "Driver test producto",
            "receta_id": str(self.receta.id),
            "familia": "",
            "lote_desde": "",
            "lote_hasta": "",
            "mo_pct": "7",
            "indirecto_pct": "3",
            "mo_fijo": "0",
            "indirecto_fijo": "0",
            "prioridad": "15",
            "activo": "1",
        }
        post_resp = self.client.post(reverse("recetas:drivers_costeo"), payload)
        self.assertEqual(post_resp.status_code, 302)
        self.assertEqual(CostoDriver.objects.count(), 1)
        get_resp = self.client.get(reverse("recetas:drivers_costeo"))
        self.assertEqual(get_resp.status_code, 200)
        self.assertContains(get_resp, "Driver test producto")

    def test_drivers_costeo_plantilla_csv(self):
        resp = self.client.get(reverse("recetas:drivers_costeo_plantilla"), {"format": "csv"})
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/csv", resp["Content-Type"])
        body = resp.content.decode("utf-8")
        self.assertIn("scope,nombre,receta", body)

    def test_drivers_costeo_handles_missing_table_gracefully(self):
        with patch("recetas.views.CostoDriver.objects.select_related", side_effect=OperationalError("missing table")):
            resp = self.client.get(reverse("recetas:drivers_costeo"))
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.context["drivers_unavailable"])
        self.assertContains(resp, "no disponibles en este entorno")


class ImportCosteoDriverExcelTests(TestCase):
    def setUp(self):
        self.tempfile = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        wb = Workbook()

        ws_cost = wb.active
        ws_cost.title = "Costo Materia Prima"
        ws_cost.append(["Proveedor", "Producto", "Descripcion", "Cantidad", "Unidad", "Costo", "Fecha"])
        ws_cost.append(["Proveedor Test", "Harina Driver", "", 1, "kg", 10, "2026-02-20"])

        ws_receta = wb.create_sheet("Insumos 1")
        ws_receta.append(["Batida Driver"])
        ws_receta.append(["Ingrediente", "Cantidad", "Unidad", "Costo"])
        ws_receta.append(["Harina Driver", 1, "kg", 10])

        ws_drivers = wb.create_sheet("Drivers Costeo")
        ws_drivers.append(
            [
                "scope",
                "nombre",
                "receta",
                "mo_pct",
                "indirecto_pct",
                "mo_fijo",
                "indirecto_fijo",
                "prioridad",
                "activo",
            ]
        )
        ws_drivers.append(
            [
                "PRODUCTO",
                "Driver Batida Import",
                "Batida Driver",
                10,
                5,
                0,
                0,
                10,
                1,
            ]
        )

        wb.save(self.tempfile.name)
        wb.close()

    def tearDown(self):
        try:
            os.unlink(self.tempfile.name)
        except OSError:
            pass

    def test_import_costeo_detecta_drivers_y_versiona_costeo(self):
        resultado = ImportadorCosteo(self.tempfile.name).procesar_completo()
        self.assertEqual(resultado.drivers_hojas_detectadas, 1)
        self.assertEqual(resultado.drivers_creados, 1)

        receta = Receta.objects.get(nombre_normalizado="batida driver")
        driver = CostoDriver.objects.get(nombre="Driver Batida Import")
        self.assertEqual(driver.scope, CostoDriver.SCOPE_PRODUCTO)
        self.assertEqual(driver.receta_id, receta.id)

        latest = receta.versiones_costo.order_by("-version_num").first()
        self.assertIsNotNone(latest)
        self.assertGreater(latest.costo_mo, Decimal("0"))
        self.assertGreater(latest.costo_indirecto, Decimal("0"))
