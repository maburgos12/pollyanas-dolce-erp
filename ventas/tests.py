from datetime import date
from decimal import Decimal
import inspect
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase
from django.urls import reverse

from ventas.services.pronostico_engine import (
    _apply_special_context_forecast,
    _simple_average_forecast,
    _special_context_comparable_days,
    _special_context_explanations,
    _previous_special_context_day,
    _special_day_name,
)
from ventas.services.sales_freshness import (
    build_forecast_sales_freshness,
    queue_forecast_sales_refresh_if_needed,
)
import ventas.views as ventas_views
from ventas.views import _apply_manual_adjustments, _build_adjustment_rows, _projection_presets


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
        css_dir = Path(__file__).resolve().parent.parent / "static" / "css" / "template_modules"
        full_list = (templates_dir / "pronostico_lista.html").read_text()
        dashboard_list = (templates_dir / "pronostico.html").read_text()
        full_list_surface = full_list + (css_dir / "ventas-templates-ventas-pronostico-lista.css").read_text()
        dashboard_surface = dashboard_list + (css_dir / "ventas-templates-ventas-pronostico.css").read_text()

        self.assertIn('class="table saved-forecasts-table"', full_list)
        self.assertIn('class="table saved-forecasts-table"', dashboard_list)
        self.assertIn("--table-min-width", full_list_surface)
        self.assertIn("--table-min-width", dashboard_surface)
        self.assertIn(".saved-forecasts-wrap > table.saved-forecasts-table td:nth-child(n+3)", full_list_surface)
        self.assertIn(".saved-forecasts-wrap > table.saved-forecasts-table td:nth-child(n+3)", dashboard_surface)
        self.assertIn("white-space:normal", full_list_surface)
        self.assertIn("white-space:normal", dashboard_surface)
        self.assertIn("forecast-actions-cell", full_list)
        self.assertIn("forecast-actions-cell", dashboard_list)
        self.assertIn("forecast-method-cell", full_list)
        self.assertIn("forecast-method-cell", dashboard_list)

    def test_pronostico_includes_projection_tab_and_range_presets(self):
        template = (Path(__file__).resolve().parent / "templates" / "ventas" / "pronostico.html").read_text()

        self.assertIn("?tab=pronosticos", template)
        self.assertIn("?tab=proyecciones", template)
        self.assertIn('active_tab == "pronosticos"', template)
        self.assertIn('active_tab == "proyecciones"', template)
        self.assertNotIn('href="#proyecciones"', template)
        self.assertIn("Proyecciones", template)
        self.assertIn("Generar pronóstico", template)
        self.assertIn("data-projection-days", template)
        self.assertIn("dataset.projectionDays === '15'", template)
        self.assertIn('name="tab" value="{{ active_tab }}"', template)
        self.assertIn("Nada se guarda hasta confirmar aquí", template)
        self.assertIn("Guardar {% if active_tab", template)
        self.assertNotIn("open-save-forecast", template)
        self.assertIn("fecha_inicio", template)
        self.assertIn("fecha_fin", template)

        presets = _projection_presets()
        self.assertEqual([preset["label"] for preset in presets], ["Semana", "15 días", "30 días"])
        self.assertEqual([preset["days"] for preset in presets], [7, 15, 30])

    def test_post_tab_is_not_forced_to_proyecciones(self):
        source = inspect.getsource(ventas_views)

        self.assertIn('active_tab = request.POST.get("tab")', source)
        self.assertNotIn('active_tab = "proyecciones" if request.method == "POST"', source)

    def test_simple_average_forecast_uses_weekday_pattern(self):
        index = pd.date_range(start="2026-05-04", periods=28, freq="D")
        values = [10 if day.weekday() < 5 else 30 for day in index]
        forecast = _simple_average_forecast(pd.Series(values, index=index), 7).round(0).astype(int).tolist()

        self.assertEqual(forecast, [10, 10, 10, 10, 10, 30, 30])

    def test_forecast_adjustment_rows_apply_manual_delta(self):
        rows, totals = _build_adjustment_rows(
            {
                "ajustes_ventas": {"p10": {"ajuste": 5, "nota": "subir fin de semana"}},
                "por_categoria": [
                    {
                        "categoria": "Pastel Grande",
                        "productos": [
                            {
                                "point_product_id": 10,
                                "nombre": "Pastel prueba",
                                "total_piezas": 12,
                                "precio": "100.00",
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(rows[0]["base"], 12)
        self.assertEqual(rows[0]["ajuste"], 5)
        self.assertEqual(rows[0]["total_final"], 17)
        self.assertEqual(rows[0]["nota"], "subir fin de semana")
        self.assertEqual(totals["total_final"], 17)
        self.assertEqual(totals["ingreso_final"], Decimal("1700.00"))

    def test_manual_adjustments_update_saved_result_summary(self):
        class PostData(dict):
            def get(self, key, default=None):
                return super().get(key, default)

        resultados, totals = _apply_manual_adjustments(
            {
                "resumen": {"total_piezas": 12, "total_ingreso": "1200.00"},
                "por_categoria": [
                    {
                        "categoria": "Pastel Grande",
                        "productos": [
                            {
                                "point_product_id": 10,
                                "nombre": "Pastel prueba",
                                "total_piezas": 12,
                                "precio": "100.00",
                            }
                        ],
                    }
                ],
            },
            PostData({"ajuste_p10": "5", "nota_p10": "subir fin de semana"}),
        )

        self.assertEqual(totals["total_base"], 12)
        self.assertEqual(totals["total_ajuste"], 5)
        self.assertEqual(resultados["resumen"]["total_piezas"], 17)
        self.assertEqual(resultados["resumen"]["ajuste_piezas_ventas"], 5)
        self.assertEqual(resultados["ajustes_ventas"]["p10"]["nota"], "subir fin de semana")

    def test_saved_forecast_detail_labels_confidence_as_saved_snapshot(self):
        template = (Path(__file__).resolve().parent / "templates" / "ventas" / "pronostico_detalle.html").read_text()

        self.assertIn("Confianza guardada", template)
        self.assertIn("cálculo generado", template)
        self.assertIn("vuelve a generar el pronóstico", template)
        self.assertIn("Revisión de ventas", template)
        self.assertIn("ventas:pronostico_ajustes", template)

    def test_saved_forecast_print_uses_standalone_document(self):
        detail_template = (Path(__file__).resolve().parent / "templates" / "ventas" / "pronostico_detalle.html").read_text()
        print_template = (Path(__file__).resolve().parent / "templates" / "ventas" / "pronostico_print.html").read_text()

        self.assertEqual(reverse("ventas:pronostico_print", args=[123]), "/ventas/pronostico/guardados/123/imprimir/")
        self.assertIn("ventas:pronostico_print", detail_template)
        self.assertNotIn('onclick="window.print()"', detail_template)
        self.assertIn("window.print()", print_template)
        self.assertNotIn('{% extends "base.html" %}', print_template)
        self.assertIn("Resumen general", print_template)
        self.assertIn("Por sucursal", print_template)

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
