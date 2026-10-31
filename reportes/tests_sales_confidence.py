from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, SimpleTestCase

from core.models import Sucursal
from reportes.models import FactVentaDiaria
from reportes.sales_confidence import _source_priority, selected_sales_facts
from django.db.models import Exists, OuterRef, Value
from django.db.models.functions import Coalesce
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

    def resale(self, external_id="169", name="Pirotecnia", cost=20, day=date(2025, 9, 1)):
        from pos_bridge.models import PointProduct
        from reportes.models import ProductoReventaCosto
        product = PointProduct.objects.create(external_id=external_id, name=name)
        source = ProductoReventaCosto.objects.create(producto_point=product, costo_unitario=cost,
            fecha_vigencia=day, source_hash=external_id, metadata={"point_name": name})
        return product, source

    def test_authorized_zero_is_distinct_from_unknown_and_keeps_service_provisional(self):
        from reportes.models import ProductoReventaCosto
        product, source = self.resale(cost=0)
        original = self.fact(key=product.external_id, producto_nombre=product.name)
        self.assertIsNone(self.confidence()["margin"])
        source.fuente = ProductoReventaCosto.FUENTE_MANUAL
        source.metadata.update(cost_treatment="UNALLOCATED_SERVICE", authorization="Mauricio",
                               point_external_id=product.external_id)
        source.save()
        result = self.confidence()
        self.assertEqual(result["margin"], 100)
        self.assertEqual(result["service_unallocated_sales"], 100)
        self.assertEqual(result["cost_coverage_pct"], 100)
        self.assertEqual(result["status"], "Provisional")
        source.metadata["point_external_id"] = "different"
        source.save()
        self.assertIsNone(self.confidence()["margin"])
        original.refresh_from_db()
        self.assertEqual(original.costo_estimado, 0)

    def test_authorized_purchase_corrects_bad_recipe_without_rewriting_sales(self):
        from recetas.models import Receta
        from reportes.models import ProductoReventaCosto
        wrong = Receta.objects.create(nombre="Bollo", hash_contenido="wrong-candle")
        product, source = self.resale(cost=3)
        original = self.fact(key=product.external_id, producto_nombre=product.name, receta=wrong,
            costo_estimado=80, metadata={"costing": {"source": "producto_costo_operativo_mensual", "period": "2025-09-01", "unit_cost": "80"}})
        source.fuente = ProductoReventaCosto.FUENTE_MANUAL
        source.metadata.update(cost_treatment="PURCHASE_REFERENCE", authorization="Mauricio",
                               point_external_id=product.external_id)
        source.save()
        self.assertEqual(self.confidence()["margin"], 97)
        original.refresh_from_db()
        self.assertEqual(original.receta_id, wrong.pk)
        self.assertEqual(original.costo_estimado, 80)

    def test_recipe_and_free_addon_use_existing_costs_without_rewriting_identities(self):
        from recetas.models import Receta, RecetaAgrupacionAddon
        from pos_bridge.models import PointProduct
        from reportes.models import RecetaCostoHistoricoMensual
        base = Receta.objects.create(nombre="Pastel", nombre_normalizado="pastel", hash_contenido="confidence-base")
        addon = Receta.objects.create(nombre="Topping", nombre_normalizado="topping", hash_contenido="confidence-addon")
        for recipe, amount in [(base, 30), (addon, 5)]:
            RecetaCostoHistoricoMensual.objects.create(receta=recipe, periodo=date(2025,9,1), costo_total=amount, coverage_pct=100)
        self.fact(receta=base)
        product = PointProduct.objects.create(external_id="gift", sku="addon", name="Topping")
        RecetaAgrupacionAddon.objects.create(base_receta=base, addon_receta=addon, addon_codigo_point="addon",
            addon_nombre_point="Topping", status="APPROVED", activo=True)
        gift = self.fact(key="gift", producto_nombre="Topping")
        FactVentaDiaria.objects.filter(pk=gift.pk).update(venta_neta=0, venta_total=0, cantidad=2)
        self.assertEqual(self.confidence()["margin"], 60)
        from reportes.sales_confidence import build_sales_confidence
        early = build_sales_confidence(start_date=date(2025,9,1), end_date=date(2025,9,15))
        self.assertIsNone(early["margin"])
        gift.refresh_from_db()
        self.assertIsNone(gift.receta_id)
        self.assertIsNone(gift.point_product_id)
        self.assertEqual(gift.costo_estimado, 0)
        FactVentaDiaria.objects.filter(pk=gift.pk).update(producto_nombre="Otro topping")
        self.assertIsNone(self.confidence()["margin"])
        FactVentaDiaria.objects.filter(pk=gift.pk).update(producto_nombre="Topping")
        RecetaAgrupacionAddon.objects.filter(addon_receta=addon).update(addon_codigo_point="")
        product.sku = ""
        product.save()
        self.assertIsNone(self.confidence()["margin"])
        RecetaAgrupacionAddon.objects.filter(addon_receta=addon).update(addon_codigo_point="addon")
        product.sku = "addon"
        product.save()
        RecetaCostoHistoricoMensual.objects.filter(receta=addon).update(coverage_pct=90)
        self.assertIsNone(self.confidence()["margin"])

    def test_existing_resale_recipe_uses_acquisition_cost_without_changing_its_mode(self):
        from recetas.models import Receta
        recipe = Receta.objects.create(nombre="Pirotecnia", nombre_normalizado="pirotecnia", hash_contenido="resale-recipe")
        original_mode = recipe.modo_costeo
        recipe.modo_costeo = Receta.MODO_COSTEO_REVENTA
        recipe.save()
        product, source = self.resale()
        fact = self.fact(key=product.external_id, producto_nombre=product.name, receta=recipe)
        self.assertEqual(self.confidence()["margin"], 80)
        fact.refresh_from_db()
        recipe.refresh_from_db()
        self.assertEqual(fact.receta_id, recipe.pk)
        self.assertEqual(recipe.modo_costeo, Receta.MODO_COSTEO_REVENTA)
        recipe.modo_costeo = original_mode
        recipe.save()
        self.assertIsNone(self.confidence()["margin"])
        from reportes.models import RecetaCostoHistoricoMensual
        recipe.modo_costeo = Receta.MODO_COSTEO_REVENTA
        recipe.save()
        source.delete()
        RecetaCostoHistoricoMensual.objects.create(receta=recipe, periodo=date(2025,9,1), costo_total=30, coverage_pct=100)
        self.assertEqual(self.confidence()["margin"], 70)

    def test_recipe_version_keeps_origin_date_and_excludes_labor_and_overhead(self):
        from datetime import datetime, timezone
        from recetas.models import Receta, RecetaCostoVersion
        recipe = Receta.objects.create(nombre="Vaso", nombre_normalizado="vaso", hash_contenido="confidence-version")
        version = RecetaCostoVersion.objects.create(receta=recipe, version_num=1, costo_total=50, costo_mo=10,
            costo_indirecto=10, rendimiento_cantidad=2)
        RecetaCostoVersion.objects.filter(pk=version.pk).update(creado_en=datetime(2025,8,20,tzinfo=timezone.utc))
        self.fact(receta=recipe)
        result = self.confidence()
        self.assertEqual(result["margin"], 85)
        self.assertEqual(result["prior_cost_sales"], 100)
        self.assertEqual(result["cost_periods"], [date(2025,8,1)])
        RecetaCostoVersion.objects.filter(pk=version.pk).update(creado_en=datetime(2025,10,1,tzinfo=timezone.utc))
        self.assertEqual(self.confidence()["cost_periods"], [date(2025,9,1)])
        RecetaCostoVersion.objects.filter(pk=version.pk).update(creado_en=datetime(2025,10,2,tzinfo=timezone.utc))
        self.assertIsNone(self.confidence()["margin"])

    def test_resale_without_recipe_is_backed_without_rewriting_sales(self):
        product, source = self.resale()
        fact = self.fact(point_product=product, metadata={"costing": {"source": "missing_recipe"}})
        FactVentaDiaria.objects.filter(pk=fact.pk).update(cantidad=2)
        result = self.confidence()
        self.assertEqual(result["margin"], Decimal("60"))
        self.assertEqual(result["cost_coverage_pct"], 100)
        self.assertEqual(result["cost_periods"], [date(2025, 9, 1)])
        fact.refresh_from_db()
        self.assertEqual(fact.costo_estimado, 0)
        self.assertEqual(fact.metadata["costing"]["source"], "missing_recipe")
        self.assertIsNone(fact.receta_id)

    def test_external_identity_requires_unique_code_and_name_not_internal_pk_or_sku(self):
        from pos_bridge.models import PointProduct
        product, source = self.resale()
        fact = self.fact(key="169", producto_nombre="PIROTECNIA")
        self.assertEqual(self.confidence()["margin"], 80)
        PointProduct.objects.create(external_id="official:169", name="Pirotecnia")
        self.assertIsNone(self.confidence()["margin"])
        FactVentaDiaria.objects.filter(pk=fact.pk).update(producto_clave=str(product.pk), producto_nombre="Otro artículo")
        self.assertIsNone(self.confidence()["margin"])
        FactVentaDiaria.objects.filter(pk=fact.pk).update(producto_clave="0169", producto_nombre="Pirotecnia")
        self.assertIsNone(self.confidence()["margin"])

    def test_rejected_or_future_source_uses_valid_prior_cost_and_keeps_its_date(self):
        from reportes.models import ProductoReventaCosto, ProductoReventaCostoHistoricoMensual
        from reportes.sales_confidence import build_sales_confidence
        product, old = self.resale(day=date(2025, 8, 1), cost=10)
        late = ProductoReventaCosto.objects.create(producto_point=product, costo_unitario=20,
            fecha_vigencia=date(2025, 9, 28), source_hash="late", metadata={"point_name": product.name})
        hist = ProductoReventaCostoHistoricoMensual.objects.create(producto_point=product, periodo=date(2025, 9, 1),
            costo_promedio=20, metadata={"source_rows": [late.pk]})
        self.fact(point_product=product)
        result = build_sales_confidence(start_date=date(2025, 9, 1), end_date=date(2025, 9, 15))
        self.assertEqual(result["margin"], 90)
        self.assertEqual(result["prior_cost_sales"], 100)
        self.assertEqual(result["cost_periods"], [date(2025, 8, 1)])
        self.assertEqual(self.confidence()["margin"], 80)
        late.metadata = {"point_name": "Rollos térmicos"}
        late.save()
        self.assertEqual(self.confidence()["margin"], 90)
        hist.refresh_from_db()
        self.assertEqual(hist.costo_promedio, 20)
        old.delete()
        late.metadata = {"point_name": product.name}
        late.save()
        early = build_sales_confidence(start_date=date(2025, 9, 1), end_date=date(2025, 9, 15))
        self.assertIsNone(early["margin"])
        self.assertEqual(early["missing_cost_sales"], 100)

    def test_gift_without_cost_and_positive_sale_without_quantity_remain_pending(self):
        product, source = self.resale()
        self.fact(point_product=product)
        gift = self.fact(key="gift", producto_nombre="Regalo sin costo")
        FactVentaDiaria.objects.filter(pk=gift.pk).update(venta_neta=0, venta_total=0)
        result = self.confidence()
        self.assertEqual(result["cost_coverage_pct"], 100)
        self.assertEqual(result["missing_cost_rows"], 1)
        self.assertIsNone(result["margin"])
        FactVentaDiaria.objects.filter(pk=gift.pk).update(point_product=product, cantidad=0, venta_neta=100,
            producto_clave="1", producto_nombre="", fecha=date(2025,9,11))
        self.assertEqual(self.confidence()["missing_cost_rows"], 1)


    def test_grouped_selection_matches_old_precedence_for_days_branches_nulls_and_ties(self):
        other = Sucursal.objects.create(codigo="OTHER-TEST", nombre="Otra")
        for index, branch in enumerate((self.branch, other, None)):
            for day in (10, 11):
                sources = ("AUTHORITATIVE", "V2_FACT", "LEGACY", "OTHER")
                if day == 11:
                    sources = sources[index + 1:]
                for source in sources:
                    FactVentaDiaria.objects.create(fecha=date(2025,9,day), sucursal=branch,
                        producto_clave=f"{index}-{day}-{source}", source_kind=source,
                        venta_total=100, venta_neta=100, cantidad=1)
        candidates = FactVentaDiaria.objects.filter(fecha__range=(date(2025,9,1),date(2025,9,30))).order_by().annotate(
            priority=_source_priority(), identity=Coalesce("sucursal_id", Value(-1)))
        preferred = candidates.filter(fecha=OuterRef("fecha"), identity=OuterRef("identity"), priority__lt=OuterRef("priority"))
        expected = set(candidates.annotate(preferred=Exists(preferred)).filter(preferred=False).values_list("pk",flat=True))
        actual = selected_sales_facts(start_date=date(2025,9,1), end_date=date(2025,9,30))
        self.assertEqual(set(actual.values_list("pk",flat=True)), expected)
        self.assertTrue(all(value is False for value in actual.values_list("has_preferred",flat=True)))
        self.assertTrue(actual.filter(sucursal=None).exists())
        self.assertTrue(actual.filter(source_kind="OTHER").exists())

    def test_source_priority_is_for_the_day_not_an_alternate_product(self):
        original = self.fact(key="official", source="AUTHORITATIVE")
        self.fact(key="different-product", source="V2_FACT")
        facts = selected_sales_facts(start_date=date(2025,9,1), end_date=date(2025,9,30))
        self.assertEqual(list(facts.values_list("pk",flat=True)), [original.pk])


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

    @patch("rentabilidad.views_rentabilidad.asignar_costos_filas")
    @patch("pos_bridge.models.sales.PointDailySale")
    def test_missing_cost_and_advance_are_not_profit_leaders(self, sales, costs):
        sales.objects.filter.return_value.values.return_value.annotate.return_value = [dict(receta_id=None, product_id=1, cost_evidence=None, cost_is_resale=True, cantidad=1, venta=1000, product__name="Pastel sin costo", product__category="Pastel", sucursales=1)]
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
        from rentabilidad.services_costos import _costos_recetas_para_periodo
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
