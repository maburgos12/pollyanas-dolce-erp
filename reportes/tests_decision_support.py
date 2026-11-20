from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.db import connection
from django.template.loader import render_to_string
from django.test import SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.models import Receta
from ventas.services.proyecciones_engine import calcular_proyeccion_operativa
from reportes.dashboard_full_dataset import build_dashboard_full_payload
from reportes.decision_score_service import build_decision_score_context
from reportes import forecast_service
from reportes.forecast_service import build_daily_forecast_context
from reportes.models import (
    FactProduccionDiaria,
    FactVentaDiaria,
    ForecastCalibrationProfile,
    ProductoCostoOperativoMensual,
    ProductoSucursalContribucionMensual,
)
from reportes.opportunity_service import build_opportunity_context
from reportes.production_recommendation_service import build_production_recommendation_context
from reportes.waste_detection_service import build_waste_detection_context


class DecisionSupportServicesTests(TestCase):
    def setUp(self):
        self.target_date = date(2026, 4, 6)
        self.branch = Sucursal.objects.create(codigo="SUC-AI", nombre="Sucursal AI")
        self.point_branch = PointBranch.objects.create(external_id="PB-AI", name="Sucursal AI", erp_branch=self.branch)
        self.sync_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)

        self.recipe_hot = Receta.objects.create(
            nombre="Pastel Fresa",
            codigo_point="PFR001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pasteles",
            categoria="Premium",
            hash_contenido="hash-forecast-hot",
        )
        self.recipe_cold = Receta.objects.create(
            nombre="Rosca Canela",
            codigo_point="ROS001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pan",
            categoria="Temporada",
            hash_contenido="hash-forecast-cold",
        )
        self.point_product_hot = PointProduct.objects.create(external_id="PP-HOT", sku="PFR001", name="Pastel Fresa", active=True)
        self.point_product_cold = PointProduct.objects.create(external_id="PP-COLD", sku="ROS001", name="Rosca Canela", active=True)

        self._seed_sales_history()
        self._seed_production_history()
        self._seed_profitability_rows()
        self._seed_stock_snapshots()

    def _seed_sales_history(self):
        for offset in range(56, 0, -1):
            current_day = self.target_date - timedelta(days=offset)
            weekday_boost = Decimal("1.40") if current_day.weekday() == self.target_date.weekday() else Decimal("1.00")
            hot_qty = Decimal("10") * weekday_boost
            cold_qty = Decimal("4")
            if offset <= 7:
                hot_qty += Decimal("4")
                cold_qty = Decimal("1")
            FactVentaDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_hot,
                point_product=self.point_product_hot,
                producto_clave="PFR001",
                producto_nombre="Pastel Fresa",
                categoria="Premium",
                cantidad=hot_qty,
                tickets=10,
                venta_bruta=hot_qty * Decimal("240"),
                descuento=ZERO,
                venta_total=hot_qty * Decimal("240"),
                venta_neta=hot_qty * Decimal("240"),
                costo_estimado=hot_qty * Decimal("110"),
                margen=hot_qty * Decimal("130"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )
            FactVentaDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_cold,
                point_product=self.point_product_cold,
                producto_clave="ROS001",
                producto_nombre="Rosca Canela",
                categoria="Temporada",
                cantidad=cold_qty,
                tickets=4,
                venta_bruta=cold_qty * Decimal("95"),
                descuento=ZERO,
                venta_total=cold_qty * Decimal("95"),
                venta_neta=cold_qty * Decimal("95"),
                costo_estimado=cold_qty * Decimal("60"),
                margen=cold_qty * Decimal("35"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )

    def _seed_production_history(self):
        for offset in range(28, 0, -1):
            current_day = self.target_date - timedelta(days=offset)
            FactProduccionDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_hot,
                producido=Decimal("15"),
                vendido=Decimal("12"),
                merma=Decimal("1"),
                transferido=Decimal("0"),
            )
            FactProduccionDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_cold,
                producido=Decimal("8"),
                vendido=Decimal("3"),
                merma=Decimal("2"),
                transferido=Decimal("0"),
            )

    def _seed_profitability_rows(self):
        latest_period = date(2026, 3, 1)
        ProductoSucursalContribucionMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_hot,
            sucursal=self.branch,
            unidades_vendidas=Decimal("300"),
            venta_total=Decimal("72000"),
            asp=Decimal("240"),
            costo_producto_unit=Decimal("110"),
            costo_producto_total=Decimal("33000"),
            gasto_comercial_unit=Decimal("18"),
            gasto_comercial_total=Decimal("5400"),
            contribucion_total=Decimal("33600"),
            contribucion_unit=Decimal("112"),
            margen_contribucion_pct=Decimal("0.4667"),
        )
        ProductoSucursalContribucionMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_cold,
            sucursal=self.branch,
            unidades_vendidas=Decimal("90"),
            venta_total=Decimal("8550"),
            asp=Decimal("95"),
            costo_producto_unit=Decimal("60"),
            costo_producto_total=Decimal("5400"),
            gasto_comercial_unit=Decimal("12"),
            gasto_comercial_total=Decimal("1080"),
            contribucion_total=Decimal("2070"),
            contribucion_unit=Decimal("23"),
            margen_contribucion_pct=Decimal("0.2421"),
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_hot,
            unidades_base=Decimal("300"),
            venta_total=Decimal("72000"),
            asp=Decimal("240"),
            costo_mp_unit=Decimal("90"),
            mano_obra_prod_unit=Decimal("10"),
            indirecto_prod_unit=Decimal("6"),
            empaque_prod_unit=Decimal("4"),
            costo_fabricacion_unit=Decimal("110"),
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_cold,
            unidades_base=Decimal("90"),
            venta_total=Decimal("8550"),
            asp=Decimal("95"),
            costo_mp_unit=Decimal("45"),
            mano_obra_prod_unit=Decimal("8"),
            indirecto_prod_unit=Decimal("4"),
            empaque_prod_unit=Decimal("3"),
            costo_fabricacion_unit=Decimal("60"),
        )

    def _seed_stock_snapshots(self):
        captured_at = timezone.make_aware(datetime(2026, 4, 6, 5, 0, 0))
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=self.point_product_hot,
            stock=Decimal("6"),
            min_stock=Decimal("2"),
            max_stock=Decimal("20"),
            captured_at=captured_at,
            sync_job=self.sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=self.point_product_cold,
            stock=Decimal("14"),
            min_stock=Decimal("2"),
            max_stock=Decimal("20"),
            captured_at=captured_at,
            sync_job=self.sync_job,
        )

    def test_waste_exposure_preserves_retail_and_excludes_cedis_commercial_proxy(self):
        cedis, _ = Sucursal.objects.get_or_create(codigo="CEDIS", defaults={"nombre": "Unidad de producción"})
        tienda = Sucursal.objects.create(codigo="TIENDA-CEDIS", nombre="Tienda cerca de CEDIS")
        for branch, recipe, produced, sold, waste in (
            (cedis, self.recipe_hot, 100, 0, 0),
            (cedis, self.recipe_cold, 100, 0, 5),
            (tienda, self.recipe_cold, 100, 85, 5),
        ):
            FactProduccionDiaria.objects.create(
                fecha=self.target_date - timedelta(days=1), sucursal=branch,
                receta=recipe, producido=produced, vendido=sold, merma=waste,
            )
        with patch("reportes.waste_detection_service._latest_unit_cost_map",
                   return_value={self.recipe_hot.pk: Decimal("7"), self.recipe_cold.pk: Decimal("7")}):
            result = build_waste_detection_context(reference_date=self.target_date, top_n=None)
        rows = {(row["branch_id"], row["recipe_id"]): row for row in result["rows"]}
        self.assertNotIn((cedis.pk, self.recipe_hot.pk), rows)
        centro = rows[(cedis.pk, self.recipe_cold.pk)]
        self.assertEqual(centro["waste_units"], Decimal("5"))
        self.assertEqual(centro["realized_impact"], Decimal("35"))
        self.assertIsNone(centro["exposed_impact"])
        self.assertIsNone(centro["rotation_pct"])
        self.assertEqual(centro["risk_level"], "MEDIO")
        tienda_row = rows[(tienda.pk, self.recipe_cold.pk)]
        self.assertEqual(tienda_row["sold_units"], Decimal("85"))
        self.assertEqual(tienda_row["waste_units"], Decimal("5"))
        self.assertEqual(tienda_row["realized_impact"], Decimal("35"))
        self.assertEqual(tienda_row["exposed_impact"], Decimal("105"))
        self.assertEqual(tienda_row["rotation_pct"], Decimal("85"))
        self.assertEqual(tienda_row["risk_level"], "MEDIO")
        self.assertEqual(result["summary"]["exposed_impact"],
                         sum(row["exposed_impact"] for row in result["rows"] if row["branch_id"] != cedis.pk))

    def test_dashboard_does_not_show_stale_cedis_exposure(self):
        html = render_to_string("core/dashboard_executive.html", {
            "dashboard_base_template": "core/dashboard_fragment_base.html",
            "dashboard_exec_ready": True,
            "decision_support": {"waste": {"rows": [
                {"branch_code": "CEDIS", "recipe_name": "Centro QA",
                 "waste_units": Decimal("5.25"), "realized_impact": Decimal("36.75"),
                 "exposed_impact": Decimal("700")},
                {"branch_code": "TIENDA-CEDIS", "recipe_name": "Tienda QA",
                 "waste_units": Decimal("5"), "realized_impact": Decimal("35"),
                 "exposed_impact": Decimal("105")},
            ]}},
        })
        self.assertIn("Centro QA", html)
        self.assertIn("36.75", html)
        self.assertNotIn("$700.00", html)
        self.assertIn("$105.00", html)

    def test_services_return_explainable_recommendations(self):
        forecast_context = build_daily_forecast_context(target_date=self.target_date, reference_date=self.target_date, top_n=10)
        self.assertGreaterEqual(len(forecast_context["rows"]), 2)
        self.assertGreater(forecast_context["validation"]["observations"], 0)

        production_context = build_production_recommendation_context(
            target_date=self.target_date,
            forecast_context=forecast_context,
            top_n=10,
        )
        waste_context = build_waste_detection_context(reference_date=self.target_date, top_n=10)
        opportunity_context = build_opportunity_context(
            target_date=self.target_date,
            forecast_context=forecast_context,
            production_context=production_context,
            waste_context=waste_context,
            top_n=10,
        )
        decision_context = build_decision_score_context(
            target_date=self.target_date,
            forecast_context=forecast_context,
            production_context=production_context,
            waste_context=waste_context,
            opportunity_context=opportunity_context,
            top_n=10,
        )

        hot_forecast = next(row for row in forecast_context["rows"] if row["recipe_id"] == self.recipe_hot.id)
        self.assertGreater(Decimal(str(hot_forecast["forecast_qty"])), Decimal("0"))
        self.assertIn("Promedio ponderado", hot_forecast["why"])

        self.assertTrue(any(row["recipe_id"] == self.recipe_hot.id for row in production_context["rows"]))
        self.assertTrue(any(row["recipe_id"] == self.recipe_cold.id for row in waste_context["rows"]))
        self.assertTrue(any(row["action"] in {"PRODUCIR_MAS", "PROMOCIONAR", "REACTIVAR"} for row in opportunity_context["rows"]))
        self.assertTrue(any(row["priority"] in {"ALTA", "MEDIA", "BAJA"} for row in decision_context["rows"]))

    def test_forecast_without_backtest_preserves_projected_rows_and_amounts(self):
        parameters = {
            "target_date": self.target_date + timedelta(days=7),
            "reference_date": self.target_date,
            "lookback_weeks": 3,
            "top_n": None,
        }
        complete = build_daily_forecast_context(**parameters)
        forecast_only = build_daily_forecast_context(**parameters, include_validation=False)

        self.assertTrue(complete["rows"])
        self.assertEqual(forecast_only["rows"], complete["rows"])
        self.assertEqual(forecast_only["summary"], complete["summary"])
        self.assertEqual(forecast_only["validation"], {})

    def test_future_week_reuses_reads_with_identical_daily_contexts(self):
        days = [self.target_date + timedelta(days=7 + offset) for offset in range(7)]
        FactVentaDiaria.objects.create(
            fecha=days[-1], sucursal=self.branch, receta=self.recipe_cold,
            producto_clave="ROS001", producto_nombre="Rosca Canela",
            categoria="Temporada", cantidad=Decimal("300"), venta_neta=Decimal("30000"),
        )
        short_recipe = Receta.objects.create(
            nombre="Receta sin historia suficiente", tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="forecast-short-history",
        )
        FactVentaDiaria.objects.create(
            fecha=self.target_date - timedelta(days=1), sucursal=self.branch, receta=short_recipe,
            producto_clave="SHORT-FC", producto_nombre=short_recipe.nombre,
            cantidad=Decimal("21"), venta_neta=Decimal("2000"),
        )

        for pattern, bias in (
            (ForecastCalibrationProfile.PATTERN_MON_WED, Decimal("1.10")),
            (ForecastCalibrationProfile.PATTERN_THU_SUN, Decimal("0.80")),
        ):
            ForecastCalibrationProfile.objects.create(
                reference_date=self.target_date, sucursal=self.branch, familia="Pasteles",
                weekly_pattern=pattern, rotation_band=ForecastCalibrationProfile.ROTATION_HIGH,
                sample_size=25, recent_weight=Decimal("0.55"), mid_weight=Decimal("0.30"),
                older_weight=Decimal("0.15"), bias_adjustment=bias,
            )

        with CaptureQueriesContext(connection) as baseline_queries:
            expected = [
                build_daily_forecast_context(
                    target_date=day, reference_date=self.target_date, lookback_weeks=3,
                    top_n=None, include_validation=False,
                )
                for day in days
            ]
        with CaptureQueriesContext(connection) as batch_queries, patch.object(
            forecast_service, "load_latest_calibration_profiles",
            wraps=forecast_service.load_latest_calibration_profiles,
        ) as calibration:
            actual = list(forecast_service.iter_future_forecast_contexts(
                days, reference_date=self.target_date, lookback_weeks=3,
            ))

        def grouped_fact_reads(captured):
            return sum(
                '"reportes_factventadiaria"' in row["sql"] and "GROUP BY" in row["sql"]
                for row in captured.captured_queries
            )

        self.assertEqual(grouped_fact_reads(baseline_queries), 7)
        self.assertEqual(grouped_fact_reads(batch_queries), 1)
        calibration.assert_called_once_with(reference_date=self.target_date)
        self.assertTrue(all(len(context["rows"]) == 2 for context in actual))
        self.assertEqual(actual, expected)

    def test_weekly_projection_preserves_totals_filters_and_special_event(self):
        start = date(2026, 4, 27)
        end = date(2026, 5, 3)
        event = date(2026, 4, 30)
        filters = {"sucursal_ids": {self.branch.id}, "point_product_ids": {self.point_product_hot.id}}
        with patch.object(timezone, "localdate", return_value=self.target_date), patch(
            "ventas.services.proyecciones_engine._selected_recipe_ids", return_value={self.recipe_hot.id},
        ), patch(
            "ventas.services.proyecciones_engine._context_uplift_lookup",
            return_value={(event, self.branch.id, self.recipe_hot.id): Decimal("1.5")},
        ):
            expected = calcular_proyeccion_operativa(start, end, include_validation=False, **filters)
            actual = calcular_proyeccion_operativa(
                start, end, include_validation=False, reuse_weekly_forecast_reads=True, **filters,
            )

        self.assertEqual(actual, expected)
        self.assertEqual({p["receta_id"] for p in actual["por_producto"]}, {self.recipe_hot.id})
        self.assertTrue(any(d["es_fecha_especial"] and d["total_piezas"] > 0 for d in actual["por_dia"]))

    def test_weekly_projection_keeps_daily_reads_when_range_crosses_reference(self):
        start = self.target_date - timedelta(days=1)
        end = start + timedelta(days=6)
        with patch.object(timezone, "localdate", return_value=self.target_date), patch(
            "ventas.services.proyecciones_engine.iter_future_forecast_contexts",
        ) as batch:
            expected = calcular_proyeccion_operativa(start, end, include_validation=False)
            actual = calcular_proyeccion_operativa(
                start, end, include_validation=False, reuse_weekly_forecast_reads=True,
            )

        batch.assert_not_called()
        self.assertEqual(actual, expected)

    def test_future_forecast_uses_latest_real_history_not_future_holes(self):
        current_context = build_daily_forecast_context(
            target_date=self.target_date,
            reference_date=self.target_date,
            lookback_weeks=3,
            top_n=10,
        )
        future_context = build_daily_forecast_context(
            target_date=self.target_date + timedelta(days=14),
            reference_date=self.target_date,
            lookback_weeks=3,
            top_n=10,
        )

        current_row = next(row for row in current_context["rows"] if row["recipe_id"] == self.recipe_hot.id)
        future_row = next(row for row in future_context["rows"] if row["recipe_id"] == self.recipe_hot.id)

        self.assertEqual(future_row["forecast_qty"], current_row["forecast_qty"])
        self.assertEqual(future_row["same_weekday_avg"], current_row["same_weekday_avg"])

    def test_forecast_caps_recent_event_spike_before_projecting_next_week(self):
        event_day = self.target_date - timedelta(days=7)
        FactVentaDiaria.objects.filter(
            fecha=event_day,
            sucursal=self.branch,
            receta=self.recipe_hot,
        ).update(
            cantidad=Decimal("120"),
            venta_bruta=Decimal("28800"),
            venta_total=Decimal("28800"),
            venta_neta=Decimal("28800"),
            margen=Decimal("15600"),
        )

        forecast_context = build_daily_forecast_context(
            target_date=self.target_date,
            reference_date=self.target_date,
            lookback_weeks=3,
            top_n=10,
        )

        hot_forecast = next(row for row in forecast_context["rows"] if row["recipe_id"] == self.recipe_hot.id)
        self.assertLess(Decimal(str(hot_forecast["forecast_qty"])), Decimal("40"))
        self.assertLess(Decimal(str(hot_forecast["same_weekday_avg"])), Decimal("40"))

    @patch("reportes.dashboard_full_dataset.get_dashboard_sales_dataset")
    @patch("reportes.dashboard_full_dataset.get_dashboard_daily_ops_dataset")
    @patch("reportes.dashboard_full_dataset.build_executive_bi_panels")
    @patch("reportes.dashboard_full_dataset.get_dashboard_production_dataset")
    @patch("core.views._build_dashboard_purchase_snapshot")
    @patch("core.views._build_canonical_inventory_dashboard_metrics")
    def test_dashboard_full_payload_includes_decision_support(
        self,
        inventory_metrics_mock,
        purchase_snapshot_mock,
        production_dataset_mock,
        executive_panels_mock,
        daily_ops_mock,
        sales_dataset_mock,
    ):
        inventory_metrics_mock.return_value = {"criticos_count": 2, "bajo_reorden_count": 3}
        purchase_snapshot_mock.return_value = {"ordenes_abiertas": 1, "recepciones_abiertas": 2}
        production_dataset_mock.return_value = {"weekly_rows": [], "category_rows": []}
        executive_panels_mock.return_value = {
            "forecast_panel": {},
            "yoy_panel": {},
            "profitability_panel": {},
            "production_sales_panel": {"cutoff_date": self.target_date},
            "central_flow_panel": {},
            "inventory_ledger_panel": {},
        }
        daily_ops_mock.return_value = {"summary": {}}
        sales_dataset_mock.return_value = {"daily_sales_snapshot": {"total_amount": Decimal("1000"), "total_tickets": 5}}

        payload = build_dashboard_full_payload(months_window=6)
        self.assertIn("decision_support", payload)
        self.assertIn("forecast", payload["decision_support"])
        self.assertIn("production", payload["decision_support"])
        self.assertIn("waste", payload["decision_support"])
        self.assertIn("opportunities", payload["decision_support"])
        self.assertIn("score", payload["decision_support"])


ZERO = Decimal("0")


class ForecastHistoricalStatsTests(SimpleTestCase):
    def test_complete_contexts_keep_order_calibration_and_history_edges(self):
        reference = date(2026, 4, 6)
        days = [reference + timedelta(days=offset) for offset in (7, 1, 5, 2, 6, 3, 4)]
        sales = []
        for recipe_id in range(1, 7):
            for offset in range(1, 57):
                qty = Decimal("3")
                if recipe_id == 2 and offset > 14:
                    qty = Decimal("0")
                if recipe_id == 3 and offset > 13:
                    qty = Decimal("-1")
                if recipe_id == 4 and offset == 1:
                    qty = Decimal("1000")
                sales.append({
                    "sucursal_id": 1, "receta_id": recipe_id,
                    "fecha": reference - timedelta(days=offset),
                    "receta__familia": "Familia", "qty": qty, "revenue": qty * 10,
                })
        sales.reverse()
        sales.append(dict(sales[-1], qty=Decimal("6"), revenue=Decimal("60")))
        sales.append(dict(sales[-1], fecha=reference + timedelta(days=4), qty=Decimal("900")))
        profiles = {
            (1, "Familia", pattern, band): {"bias_adjustment": bias if band == "HIGH" else bias + Decimal("0.2")}
            for pattern, bias in (("MON_WED", Decimal("1.1")), ("THU_SUN", Decimal("0.8")))
            for band in ("LOW", "HIGH")
        }
        with patch.object(forecast_service, "_read_forecast_sales", return_value=sales) as reads, patch.object(
            forecast_service, "load_latest_calibration_profiles", return_value=(profiles, {}),
        ) as calibration, patch.object(forecast_service, "_latest_contribution_map", return_value={}) as contribution:
            expected = [forecast_service._daily_forecast_context(
                target_date=day, reference_date=reference, lookback_weeks=8,
                top_n=None, validation_days=14, include_validation=False,
                sales_rows=[row for row in sales if row["fecha"] <= day],
            ) for day in days]
            calibration.reset_mock()
            contribution.reset_mock()
            with patch.object(forecast_service, "_historical_forecast_stats", wraps=forecast_service._historical_forecast_stats) as stats:
                actual = list(forecast_service.iter_future_forecast_contexts(days, reference_date=reference, lookback_weeks=8))
            self.assertEqual(stats.call_count, 5)
            reads.assert_called_once()
            calibration.assert_called_once_with(reference_date=reference)
            self.assertEqual(contribution.call_count, 7)
        self.assertEqual(actual, expected)
        self.assertEqual({row["recipe_id"] for row in actual[0]["rows"]}, {1, 2, 4, 5, 6})
        self.assertEqual(next(row["history_days"] for row in actual[0]["rows"] if row["recipe_id"] == 2), 14)
        order = [row["recipe_id"] for row in actual[0]["rows"]]
        self.assertLess(order.index(6), order.index(5))
        outlier = next(row for row in actual[0]["rows"] if row["recipe_id"] == 4)
        self.assertEqual(outlier["rotation_band"], "LOW")
        self.assertEqual(outlier["bias_adjustment"], Decimal("1.1000"))
        self.assertEqual(next(row for row in actual[2]["rows"] if row["recipe_id"] == 4)["bias_adjustment"], Decimal("0.8000"))
        actual[0]["rows"][0]["why"] = "mutated"
        self.assertNotEqual(actual[1]["rows"][0]["why"], "mutated")
