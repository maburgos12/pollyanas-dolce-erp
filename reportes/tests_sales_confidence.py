from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, SimpleTestCase

from core.models import Sucursal
from reportes.models import FactVentaDiaria
from rentabilidad.views_rentabilidad import _build_productos_panel


class SalesConfidenceTests(TestCase):
    def setUp(self):
        self.branch = Sucursal.objects.create(codigo="CRUCERO-TEST", nombre="Crucero", activa=False)

    def fact(self, key="1", source="AUTHORITATIVE", **kwargs):
        return FactVentaDiaria.objects.create(
            fecha=date(2025, 9, 10), sucursal=self.branch, producto_clave=key,
            source_kind=source, venta_neta=100, venta_total=100, cantidad=1, **kwargs,
        )

    def confidence(self):
        from reportes.sales_confidence import build_sales_confidence
        return build_sales_confidence(start_date=date(2025, 9, 1), end_date=date(2025, 9, 30))

    def test_historical_branch_and_product_keys_are_preserved_and_sources_do_not_double_count(self):
        self.fact()
        self.fact("0001")
        self.fact("receta:65", source="V2_FACT")
        result = self.confidence()
        self.assertEqual(result["net_sales"], Decimal("200"))
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["branches"], 1)
        self.assertEqual(result["missing_cost_sales"], Decimal("200"))
        self.assertIsNone(result["margin"])

    def test_prior_cost_is_estimate_and_future_or_undocumented_cost_is_unavailable(self):
        self.fact(costo_estimado=40, metadata={"costing": {"source": "producto_costo_operativo_mensual", "period": "2025-08-01", "unit_cost": "40"}})
        self.fact("2", costo_estimado=50, metadata={"costing": {"source": "producto_costo_operativo_mensual", "period": "2025-10-01", "unit_cost": "50"}})
        self.fact("3", costo_estimado=60)
        result = self.confidence()
        self.assertEqual(result["prior_cost_sales"], Decimal("100"))
        self.assertEqual(result["missing_cost_sales"], Decimal("200"))
        self.assertIsNone(result["margin"])

    def test_preopening_identity_is_pending_without_reassigning_rows(self):
        self.branch.fecha_apertura = date(2026, 7, 14)
        self.branch.save()
        original = self.fact(source="V2_FACT")
        result = self.confidence()
        self.assertEqual(result["identity_pending_rows"], 1)
        original.refresh_from_db()
        self.assertEqual(original.sucursal_id, self.branch.id)

    def test_empty_period_has_no_coverage_or_margin_not_zero_percent(self):
        result = self.confidence()
        self.assertIsNone(result["cost_coverage_pct"])
        self.assertIsNone(result["margin"])
        self.assertEqual(result["days_observed"], 0)
        self.assertIsNone(result["net_sales"])


class ReconciliationTests(SimpleTestCase):
    def test_offsetting_branch_errors_never_claim_reconciled(self):
        from reportes.sales_confidence import reconcile_branch_sales
        result = reconcile_branch_sales(
            source_rows=[{"branch_id": 1, "name": "Crucero", "total": Decimal("100")}, {"branch_id": 2, "name": "Bamoa", "total": Decimal("100")}],
            snapshot_rows=[{"branch_id": 1, "name": "Crucero", "total": Decimal("110")}, {"branch_id": 2, "name": "Bamoa", "total": Decimal("90")}],
        )
        self.assertEqual(result["difference"], 0)
        self.assertFalse(result["reconciled"])
        self.assertEqual(len(result["discrepancies"]), 2)

    def test_missing_snapshot_is_pending_even_for_zero_sale(self):
        from reportes.sales_confidence import reconcile_branch_sales
        result = reconcile_branch_sales(source_rows=[{"branch_id": 1, "name": "Matriz", "total": Decimal("0")}], snapshot_rows=[])
        self.assertFalse(result["reconciled"])

    @patch("rentabilidad.views_rentabilidad._costos_reventa_para_periodo", return_value={})
    @patch("rentabilidad.views_rentabilidad._costos_recetas_para_periodo", return_value={})
    @patch("rentabilidad.views_rentabilidad.get_point_sales_product_panel_rows")
    def test_missing_cost_and_advance_are_not_profit_leaders(self, sales, recipe_costs, resale_costs):
        sales.return_value = [dict(receta_id=None, product_id=1, cantidad=1, venta=1000, product__name="Pastel sin costo", product__category="Pastel", sucursales=1)]
        panel = _build_productos_panel(date(2026, 9, 1), date(2026, 9, 1), date(2026, 9, 30))
        self.assertEqual(panel["top_utilidad"], [])
        self.assertIsNone(panel["riesgo_margen"][0]["margen"])
        self.assertIsNone(panel["riesgo_margen"][0]["utilidad"])

class SnapshotConfidenceTests(SimpleTestCase):
    @patch('reportes.dashboard_full_dataset.build_closed_yoy_panel', return_value={})
    @patch('reportes.dashboard_full_dataset.get_dashboard_sales_dataset', return_value={'latest_date': date(2026, 9, 30), 'daily_sales_snapshot': {}})
    def test_cached_cost_panel_is_refreshed_with_the_current_sales_cutoff(self, sales, yoy):
        from reportes.dashboard_full_dataset import _hydrate_dashboard_full_payload
        with patch('reportes.executive_panels.build_profitability_panel', return_value={'basis_note': 'fresh'}) as profitability:
            result = _hydrate_dashboard_full_payload({'months_window': 6, 'profitability_panel': {'basis_note': 'old'}})
        self.assertEqual(result['profitability_panel']['basis_note'], 'fresh')
        profitability.assert_called_once_with(latest_date=date(2026, 9, 30))

class RentabilidadSummaryConfidenceTests(TestCase):
    def test_snapshot_without_cost_evidence_cannot_present_a_consolidated_margin(self):
        from django.contrib.auth.models import User
        from django.urls import reverse
        from rentabilidad.models_rentabilidad import SucursalRentabilidad
        user = User.objects.create_superuser('confidence_summary', '', 'local-test')
        branch = Sucursal.objects.create(codigo='CONF-SUM', nombre='Sucursal sin evidencia')
        SucursalRentabilidad.objects.create(sucursal=branch, periodo=date(2026,9,1), ventas_brutas=Decimal('100'))
        self.client.force_login(user)
        response = self.client.get(reverse('rentabilidad_dashboard'), {'periodo':'2026-09'})
        self.assertIsNone(response.context['totales']['pct_margen_bruto'])
        self.assertEqual(response.context['diagnostico']['ranking_margen'], [])
        self.assertContains(response, 'N/D')


    def test_sales_cost_evidence_does_not_certify_a_snapshot_with_zero_variable_cost(self):
        from django.contrib.auth.models import User
        from django.urls import reverse
        from pos_bridge.models import PointBranch, PointDailySale, PointProduct
        from rentabilidad.models_rentabilidad import SucursalRentabilidad
        user = User.objects.create_superuser('confidence_zero', '', 'local-test')
        branch = Sucursal.objects.create(codigo='CONF-ZERO', nombre='Cálculo sin costo')
        point_branch = PointBranch.objects.create(external_id='CONF-ZERO', name=branch.nombre, erp_branch=branch)
        product = PointProduct.objects.create(external_id='CONF-ZERO', name='Producto costeado')
        PointDailySale.objects.create(branch=point_branch, product=product, sale_date=date(2026,9,10), quantity=1, gross_amount=100, total_amount=100, net_amount=100)
        FactVentaDiaria.objects.create(fecha=date(2026,9,10), sucursal=branch, producto_clave='ZERO', source_kind='AUTHORITATIVE', cantidad=1, venta_neta=100, costo_estimado=40, metadata={'costing':{'source':'producto_costo_operativo_mensual','period':'2026-09-01','unit_cost':'40'}})
        SucursalRentabilidad.objects.create(sucursal=branch, periodo=date(2026,9,1), ventas_brutas=Decimal('100'))
        self.client.force_login(user)
        response = self.client.get(reverse('rentabilidad_dashboard'), {'periodo':'2026-09'})
        self.assertTrue(response.context['fuente_estado']['cuadra'])
        self.assertIsNone(response.context['totales']['pct_margen_bruto'])


class CostEvidenceTests(TestCase):
    def test_partial_recipe_cost_is_unavailable_even_with_a_positive_amount(self):
        from recetas.models import Receta
        from reportes.models import RecetaCostoHistoricoMensual
        from rentabilidad.views_rentabilidad import _costos_recetas_para_periodo
        recipe = Receta.objects.create(nombre='Costo parcial prueba', hash_contenido='confidence-partial-cost')
        RecetaCostoHistoricoMensual.objects.create(receta=recipe, periodo=date(2026,9,1), costo_total=50, coverage_pct=50)
        costs = _costos_recetas_para_periodo([recipe.pk], date(2026,9,1), date(2026,9,30))
        self.assertEqual(costs[recipe.pk], 0)

    def test_weekly_profitability_never_selects_a_cost_after_sales_cutoff(self):
        from datetime import timedelta
        from recetas.models import Receta
        from recetas.models import RecetaCostoSemanal
        from reportes.executive_panels import build_profitability_panel
        recipe = Receta.objects.create(nombre='Costo futuro prueba', hash_contenido='confidence-future-cost')
        for start in (date(2026,8,31), date(2026,10,5)):
            RecetaCostoSemanal.objects.create(scope_type=RecetaCostoSemanal.SCOPE_RECIPE, identity_key=f'recipe:{recipe.pk}', label=recipe.nombre, receta=recipe, week_start=start, week_end=start+timedelta(days=6), costo_total=50)
        panel = build_profitability_panel(latest_date=date(2026,9,30))
        self.assertEqual(panel['latest_week'], date(2026,8,31))

    def test_complete_month_evidence_reports_estimate_not_unavailable(self):
        branch = Sucursal.objects.create(codigo='CONF-FULL', nombre='Costo mensual completo')
        FactVentaDiaria.objects.create(fecha=date(2026,9,10), sucursal=branch, producto_clave='FULL', source_kind='AUTHORITATIVE', cantidad=1, venta_neta=100, costo_estimado=40, metadata={'costing':{'source':'producto_costo_operativo_mensual','period':'2026-09-01','unit_cost':'40'}})
        from reportes.sales_confidence import build_sales_confidence
        result = build_sales_confidence(start_date=date(2026,9,1), end_date=date(2026,9,30))
        self.assertEqual(result['margin'], Decimal('60'))
        self.assertEqual(result['cost_coverage_pct'], Decimal('100'))
        self.assertEqual(result['status'], 'Estimado con costo del mes')
