import os
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from django.db import OperationalError
from django.test import TestCase
from django.test.client import RequestFactory
from django.urls import reverse
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.utils import timezone
from unittest.mock import MagicMock, patch

from compras.models import PresupuestoCompraPeriodo, SolicitudCompra
from control.models import MermaPOS
from core.access import ROLE_ADMIN, ROLE_COMPRAS, can_view_compras
from core.middleware import CanonicalLocalHostMiddleware
from core.models import Departamento, Sucursal, UserProfile
from core.views import _compute_budget_semaforo, _compute_plan_forecast_semaforo
from core.management.commands.ejecutar_rutina_diaria_erp import _prefer_public_database_url_if_needed
from inventario.models import AlmacenSyncRun, ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, PointPendingMatch, UnidadMedida
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta, VentaHistorica


class DashboardForecastRobustnessTests(TestCase):
    def test_compute_plan_forecast_handles_missing_pronostico_table(self):
        with patch("core.views.PronosticoVenta.objects.filter", side_effect=OperationalError("missing table")):
            result = _compute_plan_forecast_semaforo("2026-02")

        self.assertEqual(result["periodo_mes"], "2026-02")
        self.assertEqual(result["recetas_total"], 0)
        self.assertEqual(result["recetas_con_desviacion"], 0)
        self.assertTrue(result["data_unavailable"])


class CanonicalLocalHostMiddlewareTests(TestCase):
    def test_localhost_redirects_to_canonical_local_host(self):
        factory = RequestFactory()
        request = factory.get("/login/?next=/dashboard/", HTTP_HOST="localhost:8002")
        middleware = CanonicalLocalHostMiddleware(lambda req: MagicMock(status_code=200))
        response = middleware(request)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "http://127.0.0.1:8002/login/?next=/dashboard/")


class DashboardHomologacionContextTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_dashboard",
            email="admin_dashboard@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

    def test_dashboard_context_includes_homologacion_counts(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-100",
            point_nombre="Insumo pendiente",
            fuzzy_sugerencia="",
            fuzzy_score=0,
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_PRODUCTO,
            point_codigo="PT-PROD-100",
            point_nombre="Producto pendiente",
            fuzzy_sugerencia="",
            fuzzy_score=0,
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_PROVEEDOR,
            point_codigo="PT-PROV-100",
            point_nombre="Proveedor pendiente",
            fuzzy_sugerencia="",
            fuzzy_score=0,
        )

        receta = Receta.objects.create(nombre="Receta test", hash_contenido="hash-dashboard-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo_texto="Insumo por revisar",
            match_status=LineaReceta.STATUS_NEEDS_REVIEW,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=2,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo_texto="Insumo rechazado",
            match_status=LineaReceta.STATUS_REJECTED,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=3,
            tipo_linea=LineaReceta.TIPO_SUBSECCION,
            insumo_texto="Subsección no cuenta",
            match_status=LineaReceta.STATUS_NEEDS_REVIEW,
        )

        AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            unmatched=5,
            matched=20,
        )

        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)

        self.assertEqual(response.context["point_pending_total"], 3)
        self.assertEqual(response.context["point_pending_insumos"], 1)
        self.assertEqual(response.context["point_pending_productos"], 1)
        self.assertEqual(response.context["point_pending_proveedores"], 1)
        self.assertEqual(response.context["recetas_pending_matching_count"], 2)
        self.assertEqual(response.context["inventario_last_unmatched_count"], 5)
        self.assertEqual(response.context["homologacion_total_pending"], 10)

    def test_dashboard_excludes_hidden_point_pending_from_totals(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-HIST-DASH",
            point_nombre="Historico oculto",
            method="POINT_BRIDGE_MOVEMENTS",
            clasificacion_operativa=PointPendingMatch.CLASIFICACION_OPERATIVA_HISTORICO,
            visible_en_operacion=False,
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["point_pending_total"], 0)
        self.assertEqual(response.context["point_pending_insumos"], 0)

    def test_dashboard_inventory_kpis_use_canonical_catalog(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-dash-can",
            nombre="Kilogramo Dashboard Canon",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        canonical = Insumo.objects.create(
            nombre="Azucar Canonica Dashboard",
            unidad_base=unidad,
            activo=True,
            codigo_point="DASH-CAN-001",
        )
        variant = Insumo.objects.create(
            nombre="AZUCAR CANONICA DASHBOARD",
            unidad_base=unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=canonical,
            stock_actual=Decimal("2"),
            stock_minimo=Decimal("3"),
            punto_reorden=Decimal("4"),
            consumo_diario_promedio=Decimal("1"),
            dias_llegada_pedido=1,
        )
        ExistenciaInsumo.objects.create(
            insumo=variant,
            stock_actual=Decimal("1"),
            stock_minimo=Decimal("1"),
            punto_reorden=Decimal("2"),
            consumo_diario_promedio=Decimal("1"),
            dias_llegada_pedido=1,
        )

        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["insumos_count"], 1)
        self.assertEqual(response.context["inventario_total_count"], 1)
        self.assertEqual(response.context["alertas_count"], 1)
        self.assertEqual(response.context["bajo_reorden_count"], 1)

    def test_dashboard_shows_enterprise_governance_cards(self):
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("users_governance_summary", response.context)
        self.assertIn("users_coverage_summary", response.context)
        self.assertIn("activos_governance_summary", response.context)
        self.assertIn("master_governance_summary", response.context)
        self.assertIn("recipe_governance_summary", response.context)
        self.assertGreaterEqual(len(response.context["users_governance_summary"]), 1)
        self.assertGreaterEqual(len(response.context["users_coverage_summary"]), 1)
        self.assertGreaterEqual(len(response.context["activos_governance_summary"]), 1)
        self.assertGreaterEqual(len(response.context["master_governance_summary"]), 1)
        self.assertGreaterEqual(len(response.context["recipe_governance_summary"]), 1)
        self.assertTrue(any(item["label"] == "Sucursales con gap" for item in response.context["users_coverage_summary"]))
        self.assertGreater(response.context["homologacion_total_pending"], -1)

    def test_dashboard_shows_master_and_recipe_blockers(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-dash-gov",
            nombre="Kilogramo Dashboard Gov",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        interno = Insumo.objects.create(
            nombre="Interno Dashboard Sin Codigo",
            unidad_base=unidad,
            tipo_item=Insumo.TIPO_INTERNO,
            categoria="Batidas",
            activo=True,
        )
        receta_final = Receta.objects.create(
            nombre="Pastel Dashboard Sin Empaque",
            hash_contenido="hash-dashboard-sin-empaque-001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
        )
        LineaReceta.objects.create(
            receta=receta_final,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo=interno,
            insumo_texto=interno.nombre,
            match_status=LineaReceta.STATUS_AUTO,
        )

        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertTrue(any(item["label"] == "Artículos incompletos" for item in response.context["master_governance_summary"]))
        self.assertTrue(any(item["label"] == "Productos sin empaque" for item in response.context["recipe_governance_summary"]))
        self.assertIn("master_demand_priority_rows", response.context)

    def test_dashboard_shows_master_demand_priority(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-dash-priority",
            nombre="Kilogramo Dashboard Priority",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        empaque = Insumo.objects.create(
            nombre="Empaque Critico Dashboard",
            tipo_item=Insumo.TIPO_EMPAQUE,
            activo=True,
        )
        receta_final = Receta.objects.create(
            nombre="Pastel Dashboard Prioridad",
            hash_contenido="hash-dashboard-priority-001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
        )
        LineaReceta.objects.create(
            receta=receta_final,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo=empaque,
            insumo_texto=empaque.nombre,
            unidad=unidad,
            unidad_texto="kg",
            cantidad=Decimal("1"),
            match_status=LineaReceta.STATUS_AUTO,
        )
        sucursal = Sucursal.objects.create(codigo="SUC-DASH-PRIO", nombre="Sucursal Dashboard Prioridad")
        VentaHistorica.objects.create(
            receta=receta_final,
            sucursal=sucursal,
            fecha=timezone.localdate() - timedelta(days=2),
            cantidad=Decimal("95"),
            monto_total=Decimal("1800"),
            fuente="POINT_PRIORITY_TEST",
        )

        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Bloqueos del maestro con impacto comercial")
        self.assertContains(response, "Bloqueos que frenan la operación")
        self.assertContains(response, "Qué falta corregir")
        self.assertContains(response, "Empaque Critico Dashboard")
        self.assertContains(response, "Demanda crítica bloqueada")
        self.assertIn("master_demand_priority_rows", response.context)
        self.assertTrue(response.context["master_demand_priority_rows"])
        self.assertIn("master_demand_critical_queue", response.context)
        self.assertTrue(response.context["master_demand_critical_queue"])
        self.assertIn("master_demand_critical_focus", response.context)
        self.assertEqual(response.context["master_demand_critical_focus"]["tone"], "danger")

    def test_dashboard_shows_enterprise_cockpit_summary(self):
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Centro de mando ERP")
        self.assertNotContains(response, "Ruta crítica ERP")
        self.assertIn("erp_command_center", response.context)
        self.assertIn("erp_cockpit_summary", response.context)
        self.assertIn("erp_operating_chain", response.context)
        self.assertIn("erp_extended_module_cards", response.context)
        self.assertIn("erp_extended_governance_rows", response.context)
        self.assertIn("erp_extended_release_rows", response.context)
        self.assertIn("erp_extended_handoff_rows", response.context)
        self.assertIn("erp_module_map", response.context)
        self.assertIn("erp_handoff_map", response.context)
        self.assertIn("erp_maturity_summary", response.context)
        self.assertIn("erp_stage_progress_rows", response.context)
        self.assertIn("erp_workflow_module_rows", response.context)
        self.assertIn("erp_governance_rows", response.context)
        self.assertIn("erp_executive_radar_rows", response.context)
        self.assertIn("erp_trunk_chain_rows", response.context)
        self.assertIn("erp_trunk_closure_cards", response.context)
        self.assertIn("erp_critical_path_rows", response.context)
        cockpit = {item["title"]: item for item in response.context["erp_cockpit_summary"]}
        extended = {item["module"]: item for item in response.context["erp_extended_module_cards"]}
        self.assertIn("Maestro ERP", cockpit)
        self.assertIn("Recetas y BOM", cockpit)
        self.assertIn("Compras documentales", cockpit)
        self.assertIn("Inventario y conciliación", cockpit)
        self.assertEqual(cockpit["Maestro ERP"]["cta"], "Abrir maestro")
        self.assertEqual(cockpit["Recetas y BOM"]["cta"], "Abrir recetas")
        self.assertEqual(cockpit["Compras documentales"]["cta"], "Abrir compras")
        self.assertEqual(cockpit["Inventario y conciliación"]["cta"], "Abrir inventario")
        chain = {item["title"]: item for item in response.context["erp_operating_chain"]}
        self.assertIn("Maestro de artículos", chain)
        self.assertIn("Recetas y BOM", chain)
        self.assertIn("Compras documentales", chain)
        trunk = {item["title"]: item for item in response.context["erp_trunk_chain_rows"]}
        self.assertIn("Maestro de artículos", trunk)
        self.assertIn("BOM y producto final", trunk)
        self.assertIn("Compras documentales", trunk)
        self.assertIn("Inventario y conciliación", trunk)
        self.assertIn("dependency_status", trunk["BOM y producto final"])
        self.assertIn("upstream_blocking", trunk["Inventario y conciliación"])
        self.assertNotContains(response, "Cadena Troncal ERP")
        self.assertNotContains(response, "Cierre troncal ERP consolidado")
        self.assertIn("Inventario y conciliación", chain)
        self.assertIn("Usuarios y Accesos", extended)
        self.assertIn("Activos", extended)
        self.assertIn("Integración comercial", extended)
        self.assertIn("Reportes ejecutivos", extended)
        self.assertIn("RRHH", extended)
        self.assertIn("CRM", extended)
        self.assertIn("Logística", extended)
        self.assertIn("Control", extended)
        extended_handoff = {item["module"]: item for item in response.context["erp_extended_handoff_rows"]}
        self.assertIn("Logística", extended_handoff)
        self.assertIn("depends_on", extended_handoff["Logística"])
        self.assertIn("exit_criteria", extended_handoff["Logística"])
        module_map = {item["module"]: item for item in response.context["erp_module_map"]}
        self.assertIn("Maestro", module_map)
        self.assertIn("Recetas", module_map)
        self.assertIn("Compras", module_map)
        self.assertIn("Inventario", module_map)
        handoff_map = {(item["from"], item["to"]): item for item in response.context["erp_handoff_map"]}
        self.assertIn(("Maestro", "Recetas"), handoff_map)
        self.assertIn(("Recetas", "Compras"), handoff_map)
        self.assertIn(("Compras", "Inventario"), handoff_map)
        self.assertIn(("Inventario", "Reabasto"), handoff_map)
        maestro_handoff = handoff_map[("Maestro", "Recetas")]
        self.assertIn("owner", maestro_handoff)
        self.assertIn("depends_on", maestro_handoff)
        self.assertIn("exit_criteria", maestro_handoff)
        self.assertIn("next_step", maestro_handoff)
        self.assertIn("completion", maestro_handoff)
        maturity = response.context["erp_maturity_summary"]
        self.assertIn("controlled_modules", maturity)
        self.assertIn("pending_modules", maturity)
        self.assertIn("coverage_pct", maturity)
        self.assertIn("weighted_progress_pct", maturity)
        self.assertIn("next_priority_module", maturity)
        self.assertEqual(len(response.context["erp_stage_progress_rows"]), 4)
        self.assertEqual(len(response.context["erp_workflow_module_rows"]), 4)
        self.assertEqual(len(response.context["erp_executive_radar_rows"]), 4)
        maestro_row = next(item for item in response.context["erp_workflow_module_rows"] if item["module"] == "Maestro")
        self.assertEqual(maestro_row["owner"], "Maestros / DG")
        self.assertTrue(maestro_row["next_step"])
        maestro_radar = next(item for item in response.context["erp_executive_radar_rows"] if item["module"] == "Maestro")
        self.assertIn("depends_on", maestro_radar)
        self.assertIn("dominant_blocker", maestro_radar)
        self.assertIn("erp_release_gate_rows", response.context)
        self.assertIn("erp_release_gate_completion", response.context)
        critical_path = response.context["erp_critical_path_rows"]
        self.assertGreaterEqual(len(critical_path), 1)

    def test_dashboard_shows_sales_history_summary(self):
        sucursal_a = Sucursal.objects.create(codigo="SUC-A-DASH", nombre="Sucursal A")
        sucursal_b = Sucursal.objects.create(codigo="SUC-B-DASH", nombre="Sucursal B")
        receta_a = Receta.objects.create(nombre="Pastel Chocolate Dashboard", hash_contenido="hash-venta-dashboard-a")
        receta_b = Receta.objects.create(nombre="Pay Queso Dashboard", hash_contenido="hash-venta-dashboard-b")

        VentaHistorica.objects.create(
            receta=receta_a,
            sucursal=sucursal_a,
            fecha="2026-01-01",
            cantidad=Decimal("12"),
            monto_total=Decimal("1200"),
            fuente="POINT_HIST_2026_Q1",
        )
        VentaHistorica.objects.create(
            receta=receta_b,
            sucursal=sucursal_b,
            fecha="2026-01-02",
            cantidad=Decimal("8"),
            monto_total=Decimal("800"),
            fuente="POINT_HIST_2026_Q1",
        )
        VentaHistorica.objects.create(
            receta=receta_a,
            sucursal=sucursal_a,
            fecha="2026-01-03",
            cantidad=Decimal("10"),
            monto_total=Decimal("1000"),
            fuente="POINT_HIST_2026_Q1",
        )

        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Base histórica de ventas")
        self.assertContains(response, "Cobertura cerrada")
        self.assertContains(response, "Sucursales líderes")
        self.assertContains(response, "Productos líderes")
        summary = response.context["sales_history_summary"]
        self.assertTrue(summary["available"])
        self.assertEqual(summary["total_rows"], 3)
        self.assertEqual(summary["branch_count"], 2)
        self.assertEqual(summary["recipe_count"], 2)
        self.assertEqual(summary["active_days"], 3)
        self.assertEqual(summary["expected_days"], 3)
        self.assertEqual(summary["missing_days"], 0)
        self.assertEqual(summary["latest_source"], "POINT_HIST_2026_Q1")
        self.assertEqual(summary["top_branches"][0]["sucursal__codigo"], "SUC-A-DASH")
        critical_path = response.context["erp_critical_path_rows"]
        self.assertGreaterEqual(len(critical_path), 1)
        self.assertIn("rank", critical_path[0])
        self.assertIn("depends_on", critical_path[0])
        self.assertIn("owner", critical_path[0])
        self.assertNotContains(response, "Madurez ERP Ejecutiva")
        self.assertNotContains(response, "Avance ERP por etapa")
        self.assertNotContains(response, "Workflow ERP por módulo")
        self.assertNotContains(response, "Mesa de gobierno ERP")
        self.assertNotContains(response, "Radar ejecutivo ERP")
        self.assertNotContains(response, "Módulos complementarios ERP")
        self.assertNotContains(response, "Gobierno de módulos complementarios ERP")

    def test_dashboard_prioritizes_operational_daily_sections(self):
        sucursal = Sucursal.objects.create(codigo="SUC-DASH-DAY", nombre="Sucursal Diario")
        receta = Receta.objects.create(nombre="Pastel Diario Dashboard", hash_contenido="hash-dashboard-diario")
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=timezone.localdate() - timedelta(days=1),
            cantidad=Decimal("44"),
            tickets=12,
            monto_total=Decimal("880"),
            fuente="DASH_DAILY_TEST",
        )
        insumo = Insumo.objects.create(nombre="Insumo Daily Dashboard", activo=True)
        SolicitudCompra.objects.create(
            area="CEDIS",
            solicitante="DG",
            insumo=insumo,
            cantidad=Decimal("5"),
            fecha_requerida=timezone.localdate() - timedelta(days=1),
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Pulso operativo del día")
        self.assertContains(response, "Resumen operativo")
        self.assertContains(response, "Ticket promedio")
        self.assertContains(response, "Venta acumulada del mes")
        self.assertContains(response, "Ventas recientes")
        self.assertContains(response, "Ranking comercial del corte")
        self.assertContains(response, "Producción CEDIS y cobertura")
        self.assertContains(response, "Forecast de venta y producción")
        self.assertContains(response, "Tendencia mensual de ventas")
        self.assertContains(response, "Abasto e inventario")
        self.assertContains(response, "Compras y producción")
        self.assertNotContains(response, "Ver control de implantación ERP")
        self.assertIn("daily_sales_snapshot", response.context)
        self.assertIn("purchase_snapshot", response.context)
        self.assertIn("production_snapshot", response.context)
        self.assertIn("production_summary", response.context)
        self.assertIn("forecast_summary", response.context)

    def test_dashboard_shows_recent_waste_snapshot(self):
        sucursal = Sucursal.objects.create(codigo="SUC-WASTE", nombre="Sucursal Merma")
        receta = Receta.objects.create(nombre="Pastel Merma Dashboard", hash_contenido="hash-dashboard-waste")
        MermaPOS.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=timezone.localdate() - timedelta(days=1),
            cantidad=Decimal("9"),
            motivo="Dañado",
            fuente="MERMA_TEST",
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Merma sucursal y CEDIS")
        self.assertContains(response, "Sucursales con más merma")
        self.assertIn("waste_executive_summary", response.context)
        self.assertTrue(response.context["waste_executive_summary"])

    def test_dashboard_shows_branch_daily_exceptions(self):
        sucursal_a = Sucursal.objects.create(codigo="SUC-EXC-A", nombre="Sucursal Excepcion A")
        sucursal_b = Sucursal.objects.create(codigo="SUC-EXC-B", nombre="Sucursal Excepcion B")
        receta = Receta.objects.create(nombre="Pastel Excepcion Dashboard", hash_contenido="hash-dashboard-exc")
        base_date = timezone.localdate() - timedelta(days=2)
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_a,
            fecha=base_date,
            cantidad=Decimal("40"),
            tickets=10,
            monto_total=Decimal("1000"),
            fuente="EXC_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_b,
            fecha=base_date,
            cantidad=Decimal("20"),
            tickets=5,
            monto_total=Decimal("500"),
            fuente="EXC_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_a,
            fecha=base_date + timedelta(days=1),
            cantidad=Decimal("20"),
            tickets=6,
            monto_total=Decimal("450"),
            fuente="EXC_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_b,
            fecha=base_date + timedelta(days=1),
            cantidad=Decimal("35"),
            tickets=8,
            monto_total=Decimal("900"),
            fuente="EXC_TEST",
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Excepciones por sucursal")
        self.assertContains(response, "Sucursal Excepcion A")
        self.assertContains(response, "Sucursal Excepcion B")
        self.assertIn("branch_daily_exception_rows", response.context)
        self.assertTrue(response.context["branch_daily_exception_rows"])
        self.assertContains(response, "Ranking comercial del corte")
        self.assertNotContains(response, "Cierre de módulos complementarios ERP")
        self.assertNotContains(response, "Entrega de módulos complementarios a downstream")
        self.assertNotContains(response, "Avance de implantación")
        self.assertNotContains(response, "Criterios de cierre ERP")
        self.assertNotContains(response, "Cadena de Cierre ERP")

    def test_dashboard_shows_product_daily_exceptions(self):
        sucursal = Sucursal.objects.create(codigo="SUC-PROD-EXC", nombre="Sucursal Producto Excepcion")
        receta_a = Receta.objects.create(nombre="Pastel Producto A", hash_contenido="hash-dashboard-prod-a")
        receta_b = Receta.objects.create(nombre="Pastel Producto B", hash_contenido="hash-dashboard-prod-b")
        base_date = timezone.localdate() - timedelta(days=2)
        VentaHistorica.objects.create(
            receta=receta_a,
            sucursal=sucursal,
            fecha=base_date,
            cantidad=Decimal("50"),
            tickets=11,
            monto_total=Decimal("1200"),
            fuente="PROD_EXC_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta_b,
            sucursal=sucursal,
            fecha=base_date,
            cantidad=Decimal("15"),
            tickets=4,
            monto_total=Decimal("300"),
            fuente="PROD_EXC_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta_a,
            sucursal=sucursal,
            fecha=base_date + timedelta(days=1),
            cantidad=Decimal("18"),
            tickets=5,
            monto_total=Decimal("400"),
            fuente="PROD_EXC_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta_b,
            sucursal=sucursal,
            fecha=base_date + timedelta(days=1),
            cantidad=Decimal("35"),
            tickets=9,
            monto_total=Decimal("840"),
            fuente="PROD_EXC_TEST",
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Productos con variación fuerte")
        self.assertContains(response, "Pastel Producto A")
        self.assertContains(response, "Pastel Producto B")
        self.assertIn("product_daily_exception_rows", response.context)
        self.assertTrue(response.context["product_daily_exception_rows"])

    def test_dashboard_shows_branch_weekday_comparison(self):
        sucursal_a = Sucursal.objects.create(codigo="SUC-WEEK-A", nombre="Sucursal Comparable A")
        sucursal_b = Sucursal.objects.create(codigo="SUC-WEEK-B", nombre="Sucursal Comparable B")
        receta = Receta.objects.create(nombre="Pastel Comparable Semana", hash_contenido="hash-dashboard-weekday")
        latest_date = timezone.localdate() - timedelta(days=1)
        while latest_date.weekday() != 2:
            latest_date -= timedelta(days=1)
        comparable_date = latest_date - timedelta(days=7)

        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_a,
            fecha=comparable_date,
            cantidad=Decimal("40"),
            tickets=10,
            monto_total=Decimal("1000"),
            fuente="WEEKDAY_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_b,
            fecha=comparable_date,
            cantidad=Decimal("30"),
            tickets=8,
            monto_total=Decimal("750"),
            fuente="WEEKDAY_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_a,
            fecha=latest_date,
            cantidad=Decimal("22"),
            tickets=6,
            monto_total=Decimal("520"),
            fuente="WEEKDAY_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal_b,
            fecha=latest_date,
            cantidad=Decimal("44"),
            tickets=11,
            monto_total=Decimal("1120"),
            fuente="WEEKDAY_TEST",
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Sucursales vs mismo día de semana")
        self.assertContains(response, "Sucursal Comparable A")
        self.assertContains(response, "Sucursal Comparable B")
        self.assertIn("branch_weekday_comparison_rows", response.context)
        self.assertTrue(response.context["branch_weekday_comparison_rows"])

    def test_dashboard_shows_product_weekday_comparison(self):
        sucursal = Sucursal.objects.create(codigo="SUC-WEEK-PROD", nombre="Sucursal Comparable Producto")
        receta_a = Receta.objects.create(nombre="Pastel Comparable A", hash_contenido="hash-dashboard-weekly-prod-a")
        receta_b = Receta.objects.create(nombre="Pastel Comparable B", hash_contenido="hash-dashboard-weekly-prod-b")
        latest_date = timezone.localdate() - timedelta(days=1)
        while latest_date.weekday() != 3:
            latest_date -= timedelta(days=1)
        comparable_date = latest_date - timedelta(days=7)

        VentaHistorica.objects.create(
            receta=receta_a,
            sucursal=sucursal,
            fecha=comparable_date,
            cantidad=Decimal("50"),
            tickets=11,
            monto_total=Decimal("1250"),
            fuente="WEEKLY_PROD_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta_b,
            sucursal=sucursal,
            fecha=comparable_date,
            cantidad=Decimal("18"),
            tickets=5,
            monto_total=Decimal("420"),
            fuente="WEEKLY_PROD_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta_a,
            sucursal=sucursal,
            fecha=latest_date,
            cantidad=Decimal("24"),
            tickets=6,
            monto_total=Decimal("560"),
            fuente="WEEKLY_PROD_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta_b,
            sucursal=sucursal,
            fecha=latest_date,
            cantidad=Decimal("34"),
            tickets=9,
            monto_total=Decimal("900"),
            fuente="WEEKLY_PROD_TEST",
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Productos vs mismo día de semana")
        self.assertContains(response, "Pastel Comparable A")
        self.assertContains(response, "Pastel Comparable B")
        self.assertIn("product_weekday_comparison_rows", response.context)
        self.assertTrue(response.context["product_weekday_comparison_rows"])

    def test_dashboard_shows_top_daily_decisions(self):
        sucursal = Sucursal.objects.create(codigo="SUC-DECISION", nombre="Sucursal Decision")
        receta = Receta.objects.create(nombre="Pastel Decision Diario", hash_contenido="hash-dashboard-decision")
        base_date = timezone.localdate() - timedelta(days=2)
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=base_date,
            cantidad=Decimal("60"),
            tickets=12,
            monto_total=Decimal("1500"),
            fuente="DECISION_TEST",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=base_date + timedelta(days=1),
            cantidad=Decimal("20"),
            tickets=5,
            monto_total=Decimal("400"),
            fuente="DECISION_TEST",
        )
        insumo = Insumo.objects.create(nombre="Insumo Decision Diario", activo=True)
        SolicitudCompra.objects.create(
            area="CEDIS",
            solicitante="DG",
            insumo=insumo,
            cantidad=Decimal("8"),
            fecha_requerida=timezone.localdate() - timedelta(days=1),
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Top decisiones del día")
        self.assertContains(response, "Liberar solicitudes vencidas")
        self.assertContains(response, "Revisar producto Pastel Decision Diario")
        self.assertIn("daily_decision_rows", response.context)
        self.assertTrue(response.context["daily_decision_rows"])

    def test_dashboard_shows_plan_supply_watchlist(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-dash-supply",
            nombre="Kilogramo Dashboard Supply",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        sucursal = Sucursal.objects.create(codigo="SUC-DASH-SUP", nombre="Sucursal Dashboard Supply")
        insumo = Insumo.objects.create(
            nombre="Chocolate Dashboard Supply",
            unidad_base=unidad,
            tipo_item=Insumo.TIPO_MATERIA_PRIMA,
            activo=True,
        )
        receta = Receta.objects.create(
            nombre="Pastel Dashboard Supply",
            hash_contenido="hash-dashboard-supply",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo=insumo,
            insumo_texto=insumo.nombre,
            unidad=unidad,
            unidad_texto="kg",
            cantidad=Decimal("2"),
            match_status=LineaReceta.STATUS_AUTO,
        )
        plan = PlanProduccion.objects.create(
            nombre="Plan Dashboard Supply",
            fecha_produccion=timezone.localdate(),
            creado_por=self.user,
        )
        PlanProduccionItem.objects.create(plan=plan, receta=receta, cantidad=Decimal("5"))
        ExistenciaInsumo.objects.create(insumo=insumo, stock_actual=Decimal("1"), punto_reorden=Decimal("3"))
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=timezone.localdate() - timedelta(days=2),
            cantidad=Decimal("40"),
            monto_total=Decimal("800"),
            fuente="DASH_SUPPLY_TEST",
        )

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Insumos críticos del plan")
        self.assertContains(response, "Plan Dashboard Supply")
        self.assertContains(response, "Chocolate Dashboard Supply")
        self.assertIn("supply_watchlist", response.context)
        self.assertTrue(response.context["supply_watchlist"])

    def test_budget_semaforo_usa_costo_canonico_para_variantes_en_solicitudes(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-dash-budget",
            nombre="Kilogramo Dashboard Budget",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        canonical = Insumo.objects.create(
            nombre="Mantequilla Canonica Dashboard",
            unidad_base=unidad,
            activo=True,
            codigo_point="DASH-BUDGET-001",
        )
        variant = Insumo.objects.create(
            nombre="MANTEQUILLA CANONICA DASHBOARD",
            unidad_base=unidad,
            activo=True,
        )
        CostoInsumo.objects.create(insumo=canonical, costo_unitario=Decimal("10.00"))
        SolicitudCompra.objects.create(
            area="Compras",
            solicitante="DG",
            insumo=variant,
            cantidad=Decimal("3.000"),
            fecha_requerida="2026-03-10",
        )
        PresupuestoCompraPeriodo.objects.create(
            periodo_tipo=PresupuestoCompraPeriodo.TIPO_MES,
            periodo_mes="2026-03",
            monto_objetivo=Decimal("1000.00"),
        )

        result = _compute_budget_semaforo(PresupuestoCompraPeriodo.TIPO_MES, "2026-03")
        self.assertEqual(result["estimado"], Decimal("30.00"))


class UsersAccessTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.departamento = Departamento.objects.create(codigo="ADM-USR", nombre="Administración Usuarios")
        self.admin = user_model.objects.create_user(
            username="admin_users",
            email="admin_users@example.com",
            password="test12345",
        )
        admin_group, _ = Group.objects.get_or_create(name=ROLE_ADMIN)
        self.admin.groups.add(admin_group)
        admin_profile, _ = UserProfile.objects.get_or_create(user=self.admin)
        admin_profile.departamento = self.departamento
        admin_profile.save(update_fields=["departamento"])

        self.compras = user_model.objects.create_user(
            username="compras_user",
            email="compras_user@example.com",
            password="test12345",
        )
        compras_group, _ = Group.objects.get_or_create(name=ROLE_COMPRAS)
        self.compras.groups.add(compras_group)

    def test_admin_can_open_users_access_page(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Crear Usuario")
        self.assertContains(response, "Centro de mando ERP")
        self.assertContains(response, "Cockpit de habilitación ERP")
        self.assertContains(response, "Cadena de habilitación ERP")
        self.assertContains(response, "Cadena troncal de habilitación")
        self.assertContains(response, "Cierre troncal ERP consolidado")
        self.assertContains(response, "Ruta crítica ERP")
        self.assertContains(response, "Radar ejecutivo ERP")
        self.assertContains(response, "Depende de")
        self.assertContains(response, "Cierre")
        self.assertContains(response, "Dependencia")
        self.assertContains(response, "Madurez de accesos ERP")
        self.assertContains(response, "Criterios de cierre ERP")
        self.assertContains(response, "Cierre global")
        self.assertContains(response, "Cadena de control RBAC")
        self.assertContains(response, "Entrega de accesos a downstream")
        self.assertContains(response, "Mesa de gobierno ERP")
        self.assertContains(response, "Salud operativa ERP")
        self.assertContains(response, "Control RBAC")
        self.assertContains(response, "Matriz de Roles")
        self.assertContains(response, "Operativo")
        self.assertContains(response, "Gobierno de accesos")
        self.assertContains(response, "Cobertura Operativa")
        self.assertContains(response, "Sucursales con gap")
        self.assertContains(response, "Listos ERP")
        self.assertContains(response, "Requisitos operativos por rol")
        self.assertIn("users_operational_health_cards", response.context)
        self.assertIn("users_focus_cards", response.context)
        self.assertIn("users_stage_rows", response.context)
        self.assertIn("users_erp_governance_rows", response.context)
        self.assertIn("users_release_gate_rows", response.context)
        self.assertIn("users_release_gate_completion", response.context)
        self.assertIn("users_maturity_summary", response.context)
        self.assertIn("users_handoff_map", response.context)
        self.assertIn("owner", response.context["users_handoff_map"][0])
        self.assertIn("depends_on", response.context["users_handoff_map"][0])
        self.assertIn("exit_criteria", response.context["users_handoff_map"][0])
        self.assertIn("next_step", response.context["users_handoff_map"][0])
        self.assertIn("completion", response.context["users_handoff_map"][0])
        self.assertIn("users_trunk_chain_rows", response.context)
        self.assertIn("users_trunk_closure_cards", response.context)
        self.assertIn("users_critical_path_rows", response.context)
        self.assertIn("users_executive_radar_rows", response.context)
        self.assertIn("erp_command_center", response.context)
        self.assertIn("completion", response.context["users_trunk_chain_rows"][0])

    def test_non_admin_cannot_open_users_access_page(self):
        self.client.force_login(self.compras)
        response = self.client.get(reverse("users_access"))
        self.assertEqual(response.status_code, 403)

    def test_lock_compras_blocks_access_even_with_compras_role(self):
        self.assertTrue(can_view_compras(self.compras))
        profile, _ = UserProfile.objects.get_or_create(user=self.compras)
        profile.lock_compras = True
        profile.save(update_fields=["lock_compras"])
        self.assertFalse(can_view_compras(self.compras))

    def test_users_access_lists_blocked_modules_and_scope(self):
        profile, _ = UserProfile.objects.get_or_create(user=self.compras)
        profile.lock_compras = True
        profile.modo_captura_sucursal = True
        profile.save(update_fields=["lock_compras", "modo_captura_sucursal"])
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Captura sucursal")
        self.assertContains(response, "Compras")

    def test_users_access_can_filter_by_role(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"), {"role": ROLE_COMPRAS})
        self.assertEqual(response.status_code, 200)
        users = response.context["users"]
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]["username"], self.compras.username)

    def test_users_access_detects_enterprise_blocker_without_departamento(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"))
        self.assertEqual(response.status_code, 200)
        compras_row = next(row for row in response.context["users"] if row["username"] == self.compras.username)
        self.assertIn("SIN_DEPARTAMENTO", compras_row["blocker_codes"])
        self.assertEqual(compras_row["status_label"], "Bloqueado")

    def test_users_access_can_filter_by_enterprise_gap(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"), {"enterprise_gap": "SIN_DEPARTAMENTO"})
        self.assertEqual(response.status_code, 200)
        users = response.context["users"]
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]["username"], self.compras.username)
        self.assertContains(response, "Sin departamento")
        self.assertIsNotNone(response.context["users_focus_summary"])
        self.assertContains(response, "Quitar foco")

    def test_users_access_can_filter_by_sucursal_coverage_gap(self):
        sucursal = Sucursal.objects.create(nombre="Matriz", codigo="MAT", activa=True)
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"), {"coverage": "sucursal", "scope_id": sucursal.id})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cobertura Operativa")
        self.assertContains(response, "Gap operativo")

    def test_users_access_edit_view_shows_profile_diagnosis(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"), {"edit": self.compras.id})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Editar Usuario")
        self.assertContains(response, "Sin departamento")
        self.assertNotContains(response, "Sin bloqueos críticos")


class RutinaDiariaDatabaseFallbackTests(TestCase):
    def test_fallback_returns_none_without_public_url(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://u:p@postgres.railway.internal:5432/railway",
            },
            clear=False,
        ):
            os.environ.pop("DATABASE_PUBLIC_URL", None)
            result = _prefer_public_database_url_if_needed()
        self.assertIsNone(result)

    @patch("core.management.commands.ejecutar_rutina_diaria_erp.dj_database_url.parse")
    @patch("core.management.commands.ejecutar_rutina_diaria_erp.socket.getaddrinfo", side_effect=OSError("dns fail"))
    def test_fallback_switches_to_public_url_when_internal_dns_fails(self, _dns_mock, parse_mock):
        parse_mock.return_value = {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "railway",
            "USER": "postgres",
            "PASSWORD": "secret",
            "HOST": "shinkansen.proxy.rlwy.net",
            "PORT": "29018",
        }
        mock_conn = MagicMock()
        fake_connections = {"default": mock_conn}
        fake_settings = SimpleNamespace(DATABASES={"default": {"ENGINE": "django.db.backends.postgresql"}})

        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://postgres:secret@postgres.railway.internal:5432/railway",
                "DATABASE_PUBLIC_URL": "postgresql://postgres:secret@shinkansen.proxy.rlwy.net:29018/railway?sslmode=require",
            },
            clear=False,
        ):
            with (
                patch("core.management.commands.ejecutar_rutina_diaria_erp.connections", fake_connections),
                patch("core.management.commands.ejecutar_rutina_diaria_erp.settings", fake_settings),
            ):
                result = _prefer_public_database_url_if_needed()
                effective_db_url = os.environ.get("DATABASE_URL")

        self.assertIn("DATABASE_URL fallback aplicado", result or "")
        self.assertIn("DATABASE_PUBLIC_URL", result or "")
        self.assertEqual(
            effective_db_url,
            "postgresql://postgres:secret@shinkansen.proxy.rlwy.net:29018/railway?sslmode=require",
        )
        parse_mock.assert_called_once()
        mock_conn.close.assert_called_once()
