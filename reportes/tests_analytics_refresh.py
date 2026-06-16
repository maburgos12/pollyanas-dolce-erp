from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.contrib.auth.models import Group, User
from django.core.management import call_command
from django.core.cache import cache
from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ROLE_DG
from core.models import AuditLog, Sucursal
from control.models import MermaPOS
from inventario.models import ExistenciaInsumo, MovimientoInventario
from inventario.stock_trace import TRACE_RECONSTRUCTED_MOVEMENT
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointDailyBranchIndicator, PointDailySale, PointProduct, PointProductionLine, PointSalesDailyProductFact, PointWasteLine
from reportes.analytics_service import full_rebuild, rebuild_sales_facts, mark_analytics_dirty
from reportes.analytics_service import refresh_dashboard_daily_ops_materialized_view, refresh_dashboard_full_materialized_view
from reportes.models import (
    AnalyticRefreshWindow,
    FactInventarioDiario,
    FactProduccionDiaria,
    FactVentaDiaria,
    ProductoCostoOperativoMensual,
)
from reportes.sales_dashboard_freshness import ensure_sales_dashboard_freshness
from reportes.views import _sales_refresh_status
from recetas.models import Receta
from ventas.models import VentaAutoritativaPoint


class AnalyticsSalesFactSourceSelectionTests(TestCase):
    def test_rebuild_sales_facts_resolves_authoritative_recipe_by_point_code_and_name(self):
        sale_date = date(2026, 1, 8)
        matriz = Sucursal.objects.create(codigo="AUTH-MAP", nombre="Matriz Auth", activa=True)
        receta = Receta.objects.create(
            nombre="Pastel de 3 Pecados R",
            codigo_point="0110",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Rebanada",
            hash_contenido="auth-recipe-resolution-test",
        )
        VentaAutoritativaPoint.objects.create(
            branch=matriz,
            sale_date=sale_date,
            product_code="0110",
            point_name="Pastel de 3 Pecados R",
            category="Rebanada",
            product=None,
            quantity=Decimal("4"),
            gross_amount=Decimal("40.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("40.00"),
            tax_amount=Decimal("0.00"),
            net_amount=Decimal("40.00"),
        )

        inserted = rebuild_sales_facts(start_date=sale_date, end_date=sale_date)

        self.assertEqual(inserted, 1)
        fact = FactVentaDiaria.objects.get(fecha=sale_date)
        self.assertEqual(fact.source_kind, FactVentaDiaria.SOURCE_AUTHORITATIVE)
        self.assertEqual(fact.receta_id, receta.id)
        self.assertEqual(fact.cantidad, Decimal("4"))

    def test_rebuild_sales_facts_selects_source_per_branch_day(self):
        sale_date = date(2026, 4, 5)
        matriz = Sucursal.objects.create(codigo="MATRIZ-T", nombre="Matriz Test", activa=True)
        crucero = Sucursal.objects.create(codigo="CRUCERO-T", nombre="Crucero Test", activa=True)
        crucero_branch = PointBranch.objects.create(external_id="2", name="Crucero Test", erp_branch=crucero)
        crucero_product = PointProduct.objects.create(external_id="POINT-2", sku="PAN02", name="Pan Crucero", active=True)

        VentaAutoritativaPoint.objects.create(
            branch=matriz,
            sale_date=sale_date,
            product_code="PASTEL01",
            point_name="Pastel Matriz",
            category="Pasteles",
            quantity=Decimal("10"),
            gross_amount=Decimal("100.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("100.00"),
            tax_amount=Decimal("0.00"),
            net_amount=Decimal("100.00"),
        )
        PointSalesDailyProductFact.objects.create(
            branch=crucero_branch,
            sale_date=sale_date,
            sucursal_nombre="Crucero Test",
            categoria="Pan",
            producto_nombre_historico="Pan Crucero",
            point_product=crucero_product,
            total_cantidad=Decimal("5"),
            total_descuento=Decimal("0.00"),
            total_venta=Decimal("200.00"),
            total_impuestos=Decimal("0.00"),
            total_venta_neta=Decimal("200.00"),
        )

        inserted = rebuild_sales_facts(start_date=sale_date, end_date=sale_date)

        self.assertEqual(inserted, 2)
        fact_rows = list(FactVentaDiaria.objects.filter(fecha=sale_date).order_by("sucursal__codigo"))
        self.assertEqual(len(fact_rows), 2)
        self.assertEqual({row.sucursal.codigo for row in fact_rows}, {"CRUCERO-T", "MATRIZ-T"})
        totals = {row.sucursal.codigo: row.venta_total for row in fact_rows}
        self.assertEqual(totals["MATRIZ-T"], Decimal("100.00"))
        self.assertEqual(totals["CRUCERO-T"], Decimal("200.00"))
        sources = {row.sucursal.codigo: row.source_kind for row in fact_rows}
        self.assertEqual(sources["MATRIZ-T"], FactVentaDiaria.SOURCE_AUTHORITATIVE)
        self.assertEqual(sources["CRUCERO-T"], FactVentaDiaria.SOURCE_V2)

    def test_rebuild_sales_facts_uses_month_aligned_cost_not_future_latest_cost(self):
        sale_date = date(2026, 1, 8)
        branch = Sucursal.objects.create(codigo="COST-MONTH", nombre="Costo Mes", activa=True)
        receta = Receta.objects.create(
            nombre="Pastel Costo Mes",
            codigo_point="COST01",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pastel Grande",
            hash_contenido="month-aligned-cost-test",
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=date(2026, 1, 1),
            receta=receta,
            costo_fabricacion_unit=Decimal("2.00"),
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=date(2026, 4, 1),
            receta=receta,
            costo_fabricacion_unit=Decimal("500.00"),
        )
        VentaAutoritativaPoint.objects.create(
            branch=branch,
            sale_date=sale_date,
            product=receta,
            product_code="COST01",
            point_name="Pastel Costo Mes",
            category="Pastel Grande",
            quantity=Decimal("4"),
            gross_amount=Decimal("100.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("100.00"),
            tax_amount=Decimal("0.00"),
            net_amount=Decimal("100.00"),
        )

        rebuild_sales_facts(start_date=sale_date, end_date=sale_date)

        fact = FactVentaDiaria.objects.get(fecha=sale_date)
        self.assertEqual(fact.costo_estimado, Decimal("8.00"))
        self.assertEqual(fact.margen, Decimal("92.00"))
        self.assertEqual(fact.metadata["costing"]["source"], "producto_costo_operativo_mensual")
        self.assertEqual(fact.metadata["costing"]["period"], "2026-01-01")
        self.assertEqual(fact.metadata["costing"]["unit_cost"], "2.000000")

    def test_rebuild_sales_facts_does_not_use_future_cost_when_monthly_cost_is_missing(self):
        sale_date = date(2026, 1, 8)
        branch = Sucursal.objects.create(codigo="COST-MISS", nombre="Costo Faltante", activa=True)
        receta = Receta.objects.create(
            nombre="Pastel Costo Faltante",
            codigo_point="COST02",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pastel Grande",
            hash_contenido="missing-monthly-cost-test",
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=date(2026, 4, 1),
            receta=receta,
            costo_fabricacion_unit=Decimal("500.00"),
        )
        VentaAutoritativaPoint.objects.create(
            branch=branch,
            sale_date=sale_date,
            product=receta,
            product_code="COST02",
            point_name="Pastel Costo Faltante",
            category="Pastel Grande",
            quantity=Decimal("4"),
            gross_amount=Decimal("100.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("100.00"),
            tax_amount=Decimal("0.00"),
            net_amount=Decimal("100.00"),
        )

        rebuild_sales_facts(start_date=sale_date, end_date=sale_date)

        fact = FactVentaDiaria.objects.get(fecha=sale_date)
        self.assertEqual(fact.costo_estimado, Decimal("0.00"))
        self.assertEqual(fact.margen, Decimal("100.00"))
        self.assertEqual(fact.metadata["costing"]["source"], "missing_monthly_cost")
        self.assertIsNone(fact.metadata["costing"]["period"])


class BIForceRefreshEndpointTests(TestCase):
    def setUp(self):
        cache.delete("reportes:bi-force-refresh-lock")
        self.user = User.objects.create_user(username="dg_refresh", password="secret")
        group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(group)
        self.client.force_login(self.user)

    @patch("reportes.views.task_operations_automation_cycle.delay")
    def test_force_refresh_queues_operations_cycle_and_logs_request(self, mock_delay):
        mock_delay.return_value = SimpleNamespace(id="task-123")
        response = self.client.post(
            reverse("reportes:bi_force_refresh"),
            {
                "reference_date": "2026-04-05",
                "lookback_days": "7",
                "next": reverse("reportes:bi"),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        mock_delay.assert_called_once_with(
            reference_date_iso="2026-04-05",
            lookback_days=7,
            sucursal_id=None,
            skip_refresh=False,
            triggered_by_id=self.user.id,
        )
        requested = AuditLog.objects.filter(action="REPORTES_BI_FORCE_REFRESH_REQUESTED").latest("timestamp")
        self.assertEqual(requested.user_id, self.user.id)
        self.assertEqual(requested.payload["reference_date"], "2026-04-05")
        self.assertEqual(requested.payload["lookback_days"], 7)
        self.assertFalse(AuditLog.objects.filter(action="INTEGRATIONS_OPERATIONAL_REFRESH_COMPLETED").exists())
        self.assertContains(response, "Actualización del corte en proceso")

    @patch("reportes.views.task_visible_cut_refresh_cycle.delay")
    def test_force_refresh_queues_visible_cut_refresh_for_dashboard_scope(self, mock_delay):
        mock_delay.return_value = SimpleNamespace(id="task-cut-123")

        response = self.client.post(
            reverse("reportes:bi_force_refresh"),
            {
                "reference_date": "2026-04-21",
                "lookback_days": "3",
                "refresh_scope": "cutoff",
                "next": reverse("reportes:bi"),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        mock_delay.assert_called_once_with(
            reference_date_iso="2026-04-21",
            triggered_by_id=self.user.id,
        )
        requested = AuditLog.objects.filter(action="REPORTES_BI_FORCE_REFRESH_REQUESTED").latest("timestamp")
        self.assertEqual(requested.payload["reference_date"], "2026-04-21")
        self.assertEqual(requested.payload["lookback_days"], 1)
        self.assertEqual(requested.payload["scope"], "visible_cut")
        self.assertContains(response, "Actualización del corte en proceso")

    @patch("reportes.views.task_operations_automation_cycle.delay", side_effect=RuntimeError("fallo refresh"))
    def test_force_refresh_logs_failure_and_releases_lock_when_queueing_fails(self, mock_delay):
        response = self.client.post(
            reverse("reportes:bi_force_refresh"),
            {
                "reference_date": "2026-04-05",
                "lookback_days": "7",
                "next": reverse("reportes:bi"),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        mock_delay.assert_called_once()
        failed = AuditLog.objects.filter(action="INTEGRATIONS_OPERATIONAL_REFRESH_FAILED").latest("timestamp")
        self.assertEqual(failed.payload["reference_date"], "2026-04-05")
        self.assertIn("fallo refresh", failed.payload["error"])
        self.assertContains(response, "La actualización operativa no se pudo completar")
        self.assertIsNone(cache.get("reportes:bi-force-refresh-lock"))

    def test_force_refresh_requires_management_role(self):
        self.client.logout()
        lectura = User.objects.create_user(username="lectura_refresh", password="secret")
        group, _ = Group.objects.get_or_create(name="LECTURA")
        lectura.groups.add(group)
        self.client.force_login(lectura)

        response = self.client.post(reverse("reportes:bi_force_refresh"), {"reference_date": "2026-04-05"})

        self.assertEqual(response.status_code, 403)


class VentasRefreshUiTests(TestCase):
    def setUp(self):
        cache.delete("reportes:bi-force-refresh-lock")
        AuditLog.objects.filter(model="reportes.AnalyticRefreshWindow").delete()

    def test_ventas_view_shows_refresh_button_and_scheduler_for_management_role(self):
        from django_celery_beat.models import PeriodicTask

        user = User.objects.create_user(username="dg_ventas_ui", password="secret")
        group, _ = Group.objects.get_or_create(name=ROLE_DG)
        user.groups.add(group)
        self.client.force_login(user)
        call_command("setup_celery_schedules")
        AuditLog.objects.create(
            user=user,
            action="INTEGRATIONS_OPERATIONAL_REFRESH_COMPLETED",
            model="reportes.AnalyticRefreshWindow",
            object_id="2026-04-07",
            payload={"reference_date": "2026-04-07", "lookback_days": 7},
        )

        response = self.client.get(reverse("reportes:ventas"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Actualizar ventas")
        self.assertContains(response, "Automatización habilitada")
        self.assertContains(response, "Último evento")
        self.assertTrue(PeriodicTask.objects.filter(name="reportes: refresh analytics operativo", enabled=True).exists())

    def test_ventas_view_hides_refresh_button_for_read_only_role(self):
        user = User.objects.create_user(username="lectura_ventas_ui", password="secret")
        group, _ = Group.objects.get_or_create(name="LECTURA")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get(reverse("reportes:ventas"))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Actualizar ventas")
        self.assertContains(response, "actualización manual queda visible para Dirección General y Administración")

    def test_ventas_view_marks_refresh_as_pending_and_disables_button_when_requested(self):
        user = User.objects.create_user(username="dg_ventas_pending", password="secret")
        group, _ = Group.objects.get_or_create(name=ROLE_DG)
        user.groups.add(group)
        self.client.force_login(user)
        AuditLog.objects.create(
            user=user,
            action="REPORTES_BI_FORCE_REFRESH_REQUESTED",
            model="reportes.AnalyticRefreshWindow",
            object_id="2026-04-07",
            payload={"reference_date": "2026-04-07", "lookback_days": 7},
        )
        cache.set("reportes:bi-force-refresh-lock", "2026-04-07", 60)

        response = self.client.get(reverse("reportes:ventas"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Actualización solicitada")
        self.assertContains(response, "Pendiente")
        self.assertContains(response, "Actualización en curso")
        self.assertContains(response, "sales-refresh-button")
        self.assertContains(response, "disabled")

    def test_sales_refresh_status_detects_delayed_cut_against_expected_operational_date(self):
        user = User.objects.create_user(username="dg_ventas_lag", password="secret")
        call_command("setup_celery_schedules")
        AuditLog.objects.create(
            user=user,
            action="REPORTES_BI_FORCE_REFRESH_REQUESTED",
            model="reportes.AnalyticRefreshWindow",
            object_id="2026-04-06",
            payload={"reference_date": "2026-04-06", "lookback_days": 7},
        )
        cache.set("reportes:bi-force-refresh-lock", "2026-04-06", 60)
        expected_meta = {
            "expected_cut_date": date(2026, 4, 6),
            "schedule_hour": 3,
            "schedule_minute": 35,
            "timezone_label": "America/Mazatlan",
            "schedule_time_label": "03:35",
        }

        with patch("reportes.views._expected_sales_cut_date", return_value=expected_meta):
            status = _sales_refresh_status(visible_cut_date=date(2026, 4, 5))

        self.assertEqual(status["expected_cut_date_iso"], "2026-04-06")
        self.assertEqual(status["target_refresh_date_iso"], "2026-04-06")
        self.assertEqual(status["cut_lag_days"], 1)
        self.assertTrue(status["is_cut_delayed"])
        self.assertEqual(status["last_status"], "PENDIENTE_REZAGO")

    def test_sales_refresh_status_treats_current_visible_cut_as_ok_after_failed_job(self):
        user = User.objects.create_user(username="dg_ventas_current", password="secret")
        call_command("setup_celery_schedules")
        AuditLog.objects.create(
            user=user,
            action="INTEGRATIONS_OPERATIONAL_REFRESH_FAILED",
            model="reportes.AnalyticRefreshWindow",
            object_id="2026-04-06",
            payload={"reference_date": "2026-04-06"},
        )
        expected_meta = {
            "expected_cut_date": date(2026, 4, 6),
            "schedule_hour": 3,
            "schedule_minute": 35,
            "timezone_label": "America/Mazatlan",
            "schedule_time_label": "03:35",
        }

        with patch("reportes.views._expected_sales_cut_date", return_value=expected_meta):
            status = _sales_refresh_status(visible_cut_date=date(2026, 4, 6))

        self.assertEqual(status["expected_cut_date_iso"], "2026-04-06")
        self.assertEqual(status["last_status"], "OK")
        self.assertFalse(status["is_cut_delayed"])


class SalesDashboardFreshnessTests(SimpleTestCase):
    def test_ensure_sales_dashboard_freshness_retries_when_fact_layer_lags_point_sales(self):
        refresh_summary = SimpleNamespace(
            sales_rows=100,
            inventory_rows=20,
            production_rows=30,
            forecast_rows=40,
            calibration_rows=5,
        )
        with (
            patch("reportes.sales_dashboard_freshness.PointDailySale.objects") as point_manager,
            patch("reportes.sales_dashboard_freshness.FactVentaDiaria.objects") as fact_manager,
            patch("reportes.sales_dashboard_freshness._visible_cut_for", side_effect=[date(2026, 4, 7), date(2026, 4, 10)]),
            patch("reportes.sales_dashboard_freshness.refresh_incremental", return_value=refresh_summary) as refresh_mock,
            patch("reportes.sales_dashboard_freshness.log_event") as log_mock,
        ):
            point_manager.filter.return_value.order_by.return_value.values_list.return_value.first.return_value = date(2026, 4, 10)
            fact_manager.filter.return_value.order_by.return_value.values_list.return_value.first.side_effect = [
                date(2026, 4, 7),
                date(2026, 4, 10),
            ]

            result = ensure_sales_dashboard_freshness(
                reference_date=date(2026, 4, 10),
                lookback_days=2,
                triggered_by=None,
                trigger="point_daily_sales_sync",
            )

        self.assertTrue(result.catchup_attempted)
        self.assertTrue(result.catchup_succeeded)
        self.assertEqual(result.lag_days_before, 3)
        self.assertEqual(result.lag_days_after, 0)
        refresh_mock.assert_called_once_with(reference_date=date(2026, 4, 10), lookback_days=4)
        log_mock.assert_called_once()

    def test_ensure_sales_dashboard_freshness_skips_retry_when_visible_cut_is_current(self):
        with (
            patch("reportes.sales_dashboard_freshness.PointDailySale.objects") as point_manager,
            patch("reportes.sales_dashboard_freshness.FactVentaDiaria.objects") as fact_manager,
            patch("reportes.sales_dashboard_freshness._visible_cut_for", return_value=date(2026, 4, 10)),
            patch("reportes.sales_dashboard_freshness.refresh_incremental") as refresh_mock,
            patch("reportes.sales_dashboard_freshness.log_event") as log_mock,
        ):
            point_manager.filter.return_value.order_by.return_value.values_list.return_value.first.return_value = date(2026, 4, 10)
            fact_manager.filter.return_value.order_by.return_value.values_list.return_value.first.return_value = date(2026, 4, 10)

            result = ensure_sales_dashboard_freshness(
                reference_date=date(2026, 4, 10),
                lookback_days=2,
                triggered_by=None,
                trigger="point_daily_sales_sync",
            )

        self.assertFalse(result.catchup_attempted)
        self.assertTrue(result.catchup_succeeded)
        refresh_mock.assert_not_called()
        log_mock.assert_not_called()


class AnalyticsDashboardCacheInvalidationTests(SimpleTestCase):
    @patch("reportes.analytics_service._bump_sales_dashboard_cache_scopes")
    @patch("reportes.analytics_service.connection")
    def test_daily_ops_refresh_invalidates_sales_and_dashboard_scopes(self, connection_mock, bump_mock):
        cursor_cm = MagicMock()
        connection_mock.cursor.return_value = cursor_cm
        connection_mock.in_atomic_block = False

        refresh_dashboard_daily_ops_materialized_view(concurrently=False)

        bump_mock.assert_called_once_with()
        cursor_cm.__enter__.return_value.execute.assert_called_once_with(
            "REFRESH MATERIALIZED VIEW mv_dashboard_daily_ops"
        )

    @patch("reportes.analytics_service.build_dashboard_full_payload")
    @patch("reportes.analytics_service.DashboardFullSnapshot.objects.bulk_create")
    @patch("reportes.analytics_service._bump_sales_dashboard_cache_scopes")
    @patch("reportes.analytics_service.connection")
    def test_full_refresh_invalidates_sales_and_dashboard_scopes(
        self,
        connection_mock,
        bump_mock,
        bulk_create_mock,
        build_payload_mock,
    ):
        cursor_cm = MagicMock()
        connection_mock.cursor.return_value = cursor_cm
        connection_mock.in_atomic_block = False
        events = []
        bump_mock.side_effect = lambda: events.append("bump")
        build_payload_mock.side_effect = lambda **_: events.append("build") or {
            "executive_panels": {"latest_cutoff_date": "2026-04-10"}
        }

        refresh_dashboard_full_materialized_view(months_windows=(6,), concurrently=False)

        build_payload_mock.assert_called_once_with(months_window=6)
        bulk_create_mock.assert_called_once()
        self.assertEqual(events, ["bump", "build", "bump"])
        self.assertEqual(bump_mock.call_count, 2)
        cursor_cm.__enter__.return_value.execute.assert_called_once_with(
            "REFRESH MATERIALIZED VIEW mv_dashboard_full"
        )


class AnalyticsRefreshWindowLifecycleTests(TestCase):
    def test_full_rebuild_marks_pending_windows_as_done(self):
        sale_date = date(2026, 4, 5)
        branch = Sucursal.objects.create(codigo="DONE-T", nombre="Done Test", activa=True)
        VentaAutoritativaPoint.objects.create(
            branch=branch,
            sale_date=sale_date,
            product_code="SKU-DONE",
            point_name="Producto Done",
            quantity=Decimal("1"),
            gross_amount=Decimal("10.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("10.00"),
            tax_amount=Decimal("0.00"),
            net_amount=Decimal("10.00"),
        )
        window = mark_analytics_dirty(
            dataset=AnalyticRefreshWindow.DATASET_SALES,
            date_from=sale_date,
            date_to=sale_date,
            reason="test full rebuild",
        )

        full_rebuild(start_date=sale_date, end_date=sale_date)

        window.refresh_from_db()
        self.assertEqual(window.status, AnalyticRefreshWindow.STATUS_DONE)


class DailyOperationalClosureViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="dg_closure", password="secret")
        group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(group)
        self.client.force_login(self.user)

        self.target_date = date(2026, 4, 7)
        self.branch = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.point_branch = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=self.branch)
        self.point_product = PointProduct.objects.create(external_id="P-1", sku="SKU-1", name="Pastel")
        self.kg = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(
            nombre="Harina",
            tipo_item=Insumo.TIPO_MATERIA_PRIMA,
            unidad_base=self.kg,
        )
        self.existencia = ExistenciaInsumo.objects.create(insumo=self.insumo, stock_actual=Decimal("12.000"))

        PointDailySale.objects.create(
            branch=self.point_branch,
            product=self.point_product,
            sale_date=self.target_date,
            quantity=Decimal("8"),
            tickets=5,
            gross_amount=Decimal("100"),
            total_amount=Decimal("100"),
            net_amount=Decimal("100"),
        )
        FactVentaDiaria.objects.create(
            fecha=self.target_date,
            sucursal=self.branch,
            producto_clave="SKU-1",
            producto_nombre="Pastel",
            cantidad=Decimal("8"),
            tickets=5,
            venta_total=Decimal("100"),
            venta_neta=Decimal("100"),
            source_kind=FactVentaDiaria.SOURCE_LEGACY,
        )
        PointProductionLine.objects.create(
            branch=self.point_branch,
            erp_branch=self.branch,
            production_external_id="PROD-1",
            detail_external_id="DET-1",
            source_hash="prod-hash-1",
            production_date=self.target_date,
            item_name="Pastel",
            produced_quantity=Decimal("10"),
        )
        FactProduccionDiaria.objects.create(
            fecha=self.target_date,
            sucursal=self.branch,
            producido=Decimal("10"),
            vendido=Decimal("8"),
            merma=Decimal("1"),
            transferido=Decimal("0"),
        )
        PointWasteLine.objects.create(
            branch=self.point_branch,
            erp_branch=self.branch,
            movement_external_id="WASTE-1",
            source_hash="waste-hash-1",
            movement_at=timezone.make_aware(datetime(2026, 4, 7, 8, 0)),
            item_name="Pastel dañado",
            quantity=Decimal("1"),
            total_cost=Decimal("12"),
        )
        MermaPOS.objects.create(
            sucursal=self.branch,
            fecha=self.target_date,
            producto_texto="Pastel dañado",
            cantidad=Decimal("1"),
        )
        MovimientoInventario.objects.create(
            fecha=timezone.make_aware(datetime(2026, 4, 7, 9, 0)),
            tipo=MovimientoInventario.TIPO_CONSUMO,
            insumo=self.insumo,
            cantidad=Decimal("2"),
            referencia="MERMA|TEST",
        )
        FactInventarioDiario.objects.create(
            fecha=self.target_date,
            insumo=self.insumo,
            stock_inicial=Decimal("15"),
            entradas=Decimal("0"),
            salidas=Decimal("3"),
            stock_final=Decimal("12"),
            costo=Decimal("120"),
        )
        call_command(
            "reconcile_inventory_traceability",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-04-08",
            "--execute",
        )
        self.existencia.refresh_from_db()

    def test_cierre_operativo_view_renders_reconciled_domains(self):
        response = self.client.get(reverse("reportes:cierre_operativo"), {"fecha": self.target_date.isoformat()})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cierre operativo diario")
        self.assertContains(response, self.target_date.isoformat())
        self.assertContains(response, "Ventas del día")
        self.assertContains(response, "Producción Point del día")
        self.assertContains(response, "Merma visible del día")
        self.assertContains(response, "Desviaciones de stock")
        self.assertContains(response, "PlanProduccion")
        self.assertContains(response, "PointDailySale")
        self.assertContains(response, "MATRIZ")
        self.assertContains(response, "kg")
        self.assertContains(response, "Cada renglón se expresa en la unidad base del insumo.")
        self.assertContains(response, "Stock esperado al cierre")
        self.assertContains(response, "Stock visible actual")
        self.assertContains(response, "Movimiento del día e interpretación")
        self.assertContains(response, "Cierre esperado del día vs stock vivo actual")
        self.assertContains(response, "Origen stock visible")
        self.assertContains(response, "Insumos sin traza")
        self.assertContains(response, "Movimiento reconstruido")
        self.assertEqual(self.existencia.trazabilidad_stock.get("source"), TRACE_RECONSTRUCTED_MOVEMENT)

    def test_cierre_operativo_uses_branch_indicator_tickets_when_sales_facts_have_zero(self):
        FactVentaDiaria.objects.filter(fecha=self.target_date).update(tickets=0)
        PointDailyBranchIndicator.objects.create(
            branch=self.point_branch,
            indicator_date=self.target_date,
            total_tickets=5,
            total_amount=Decimal("100"),
        )

        response = self.client.get(reverse("reportes:cierre_operativo"), {"fecha": self.target_date.isoformat()})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["closure"]["sales"]["fact_tickets"], 5)
        self.assertContains(response, "8 piezas · 5 tickets")
