from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase
from django.urls import reverse

from ventas.services.pronostico_engine import (
    _apply_special_context_forecast,
    _special_context_comparable_days,
    _special_context_explanations,
    _previous_special_context_day,
    _special_day_name,
)
from ventas.services.sales_freshness import (
    build_forecast_sales_freshness,
    queue_forecast_sales_refresh_if_needed,
)


class VentasModuleTests(SimpleTestCase):
    def test_placeholder(self):
        self.assertTrue(True)

    def test_legacy_eventos_route_redirects_to_pronostico(self):
        response = self.client.get(reverse("ventas:eventos"))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], reverse("ventas:pronostico"))

    def test_legacy_tendencias_route_redirects_to_pronostico(self):
        response = self.client.get(reverse("ventas:tendencias"))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], reverse("ventas:pronostico"))

    def test_saved_forecast_tables_use_non_overlapping_columns_and_actions(self):
        templates_dir = Path(__file__).resolve().parent / "templates" / "ventas"
        full_list = (templates_dir / "pronostico_lista.html").read_text()
        dashboard_list = (templates_dir / "pronostico.html").read_text()

        self.assertIn('class="table saved-forecasts-table"', full_list)
        self.assertIn('class="table saved-forecasts-table"', dashboard_list)
        self.assertIn("forecast-actions-cell", full_list)
        self.assertIn("forecast-actions-cell", dashboard_list)
        self.assertIn("forecast-method-cell", full_list)
        self.assertIn("forecast-method-cell", dashboard_list)

    def test_dia_del_padre_uses_movable_third_sunday(self):
        self.assertEqual(_special_day_name(date(2025, 6, 15)), "Día del Padre")
        self.assertEqual(_special_day_name(date(2026, 6, 21)), "Día del Padre")
        self.assertEqual(_special_day_name(date(2025, 6, 21)), "")

    def test_dia_del_padre_context_maps_to_previous_event_weekend(self):
        selected_days = [date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 21)]

        comparable_days = [_previous_special_context_day(day, selected_days) for day in selected_days]

        self.assertEqual(comparable_days, [date(2025, 6, 13), date(2025, 6, 14), date(2025, 6, 15)])

    def test_special_context_forecast_replaces_flat_average_with_event_weekend_history(self):
        selected_days = [date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 21)]
        history = {
            date(2025, 6, 13): Decimal("10"),
            date(2025, 6, 14): Decimal("20"),
            date(2025, 6, 15): Decimal("30"),
        }
        series = pd.Series(
            [10.0, 20.0, 30.0],
            index=pd.to_datetime([date(2025, 6, 13), date(2025, 6, 14), date(2025, 6, 15)]),
        )
        forecast_result = {
            "recomendado": [5, 5, 5],
            "conservador": [4, 4, 4],
            "agresivo": [6, 6, 6],
            "confianza": 0.55,
            "metodo": "promedio-simple",
        }

        with patch("ventas.services.pronostico_engine._stl_trend_ratio", return_value=1.0):
            adjusted = _apply_special_context_forecast(
                forecast_result,
                series=series,
                history=history,
                selected_days=selected_days,
                trend_start=date(2026, 4, 20),
                history_end=date(2026, 5, 19),
            )

        self.assertEqual(adjusted["recomendado"], [10, 20, 30])
        self.assertEqual(adjusted["metodo"], "evento-comparable+fecha-especial")

    def test_father_day_context_uses_same_event_position_across_years(self):
        selected_days = [date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 21)]

        comparables = _special_context_comparable_days(date(2026, 6, 21), selected_days)

        self.assertEqual(comparables, [date(2025, 6, 15), date(2024, 6, 16), date(2023, 6, 18)])

    def test_special_context_forecast_uses_weighted_multi_year_event_history(self):
        selected_days = [date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 21)]
        history = {
            date(2025, 6, 13): Decimal("10"),
            date(2024, 6, 14): Decimal("20"),
            date(2023, 6, 16): Decimal("30"),
            date(2025, 6, 14): Decimal("20"),
            date(2024, 6, 15): Decimal("30"),
            date(2023, 6, 17): Decimal("40"),
            date(2025, 6, 15): Decimal("30"),
            date(2024, 6, 16): Decimal("40"),
            date(2023, 6, 18): Decimal("50"),
        }
        series = pd.Series(
            [10.0, 20.0, 30.0],
            index=pd.to_datetime([date(2025, 6, 13), date(2025, 6, 14), date(2025, 6, 15)]),
        )
        forecast_result = {
            "recomendado": [5, 5, 5],
            "conservador": [4, 4, 4],
            "agresivo": [6, 6, 6],
            "confianza": 0.55,
            "metodo": "promedio-simple",
        }

        with patch("ventas.services.pronostico_engine._stl_trend_ratio", return_value=1.0):
            adjusted = _apply_special_context_forecast(
                forecast_result,
                series=series,
                history=history,
                selected_days=selected_days,
                trend_start=date(2026, 4, 20),
                history_end=date(2026, 5, 19),
            )

        self.assertEqual(adjusted["recomendado"], [16, 26, 36])

    def test_special_context_explains_target_dates_and_comparables(self):
        selected_days = [date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 21)]

        explanations = _special_context_explanations(selected_days)

        self.assertEqual(explanations[0]["evento"], "Día del Padre")
        self.assertEqual(explanations[0]["fecha_evento"], "2026-06-21")
        self.assertEqual(explanations[0]["relacion_evento"], "2 días antes de Día del Padre")
        self.assertEqual(
            [item["fecha_iso"] for item in explanations[0]["comparables"]],
            ["2025-06-13", "2024-06-14", "2023-06-16"],
        )

    def test_forecast_sales_freshness_detects_stale_point_sales(self):
        freshness = build_forecast_sales_freshness(
            latest_sale_date=date(2026, 4, 19),
            reference_date=date(2026, 5, 20),
        )

        self.assertFalse(freshness.is_fresh)
        self.assertEqual(freshness.target_sale_date, date(2026, 5, 19))
        self.assertEqual(freshness.missing_days, 30)
        self.assertEqual(freshness.refresh_days, 30)

    def test_forecast_sales_freshness_accepts_current_closed_sales_day(self):
        freshness = build_forecast_sales_freshness(
            latest_sale_date=date(2026, 5, 19),
            reference_date=date(2026, 5, 20),
        )

        self.assertTrue(freshness.is_fresh)
        self.assertEqual(freshness.refresh_days, 0)

    @patch("ventas.services.sales_freshness.task_daily_sales_sync.delay")
    @patch("ventas.services.sales_freshness.latest_sales_fact_date")
    def test_stale_forecast_sales_queues_only_sales_refresh(self, latest_date, delay):
        latest_date.return_value = date(2026, 4, 19)
        delay.return_value.id = "sales-task-1"

        freshness = queue_forecast_sales_refresh_if_needed(
            triggered_by_id=7,
            reference_date=date(2026, 5, 20),
        )

        self.assertFalse(freshness.is_fresh)
        self.assertEqual(freshness.refresh_task_id, "sales-task-1")
        delay.assert_called_once_with(days=30, lag_days=1, triggered_by_id=7)

    @patch("ventas.services.sales_freshness.task_daily_sales_sync.delay")
    @patch("ventas.services.sales_freshness.latest_sales_fact_date")
    def test_fresh_forecast_sales_does_not_queue_refresh(self, latest_date, delay):
        latest_date.return_value = date(2026, 5, 19)

        freshness = queue_forecast_sales_refresh_if_needed(
            triggered_by_id=7,
            reference_date=date(2026, 5, 20),
        )

        self.assertTrue(freshness.is_fresh)
        delay.assert_not_called()
