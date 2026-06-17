from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.models import Receta
from reportes.dashboard_full_dataset import build_dashboard_full_payload
from reportes.decision_score_service import build_decision_score_context
from reportes.forecast_service import build_daily_forecast_context
from reportes.models import FactProduccionDiaria, FactVentaDiaria, ProductoCostoOperativoMensual, ProductoSucursalContribucionMensual
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
