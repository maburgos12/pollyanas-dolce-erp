from datetime import date
from decimal import Decimal
import inspect
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase, TestCase
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
from ventas.services.proyecciones_engine import _context_uplift_lookup, _season_name, calcular_proyeccion_operativa
from core.models import Sucursal
import ventas.views as ventas_views
from ventas.views import _apply_manual_adjustments, _build_adjustment_rows, _projection_presets, _save_manual_adjustments


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
        self.assertIn("forecast-loading-overlay", template)
        self.assertIn("data-forecast-loading-form", template)
        self.assertIn("Preparando matriz de ajustes por día", template)
        self.assertIn("No cierres esta ventana", template)

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
                "fechas_tabla": [{"iso": "2026-06-21", "label": "Dom 21 Jun"}],
                "por_categoria": [
                    {
                        "categoria": "Pastel Grande",
                        "productos": [
                            {
                                "point_product_id": 10,
                                "nombre": "Pastel prueba",
                                "total_piezas": 12,
                                "precio": "100.00",
                                "dias_lista": [{"fecha_iso": "2026-06-21", "recomendado": 12}],
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(rows[0]["base"], 12)
        self.assertEqual(rows[0]["propuesta"], 12)
        self.assertEqual(rows[0]["ajuste"], 5)
        self.assertEqual(rows[0]["ajuste_dias"], 0)
        self.assertEqual(rows[0]["total_final"], 17)
        self.assertEqual(rows[0]["nota"], "subir fin de semana")
        self.assertEqual(rows[0]["dias"][0]["sistema"], 12)
        self.assertEqual(totals["total_final"], 17)
        self.assertEqual(totals["ingreso_final"], Decimal("1700.00"))

    def test_forecast_adjustment_rows_unify_general_and_daily_adjustments(self):
        rows, totals = _build_adjustment_rows(
            {
                "ajustes_ventas": {
                    "p10": {
                        "ajuste": 5,
                        "dias": {
                            "2026-06-21": {"ajuste": 2},
                            "2026-06-22": {"ajuste": -1},
                        },
                    }
                },
                "fechas_tabla": [
                    {"iso": "2026-06-21", "label": "Dom 21 Jun"},
                    {"iso": "2026-06-22", "label": "Lun 22 Jun"},
                ],
                "por_categoria": [
                    {
                        "categoria": "Pastel Grande",
                        "productos": [
                            {
                                "point_product_id": 10,
                                "nombre": "Pastel prueba",
                                "total_piezas": 30,
                                "precio": "100.00",
                                "dias_lista": [
                                    {"fecha_iso": "2026-06-21", "recomendado": 20},
                                    {"fecha_iso": "2026-06-22", "recomendado": 10},
                                ],
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(rows[0]["propuesta"], 30)
        self.assertEqual(rows[0]["ajuste"], 5)
        self.assertEqual(rows[0]["ajuste_dias"], 1)
        self.assertEqual(rows[0]["total_final"], 36)
        self.assertEqual(rows[0]["dias"][0]["final"], 22)
        self.assertEqual(rows[0]["dias"][1]["final"], 9)
        self.assertEqual(totals["total_ajuste_general"], 5)
        self.assertEqual(totals["total_ajuste_dias"], 1)
        self.assertEqual(totals["total_final"], 36)

    def test_forecast_adjustment_rows_use_recipe_key_without_point_product(self):
        rows, _totals = _build_adjustment_rows(
            {
                "ajustes_ventas": {"r77": {"ajuste": -2, "nota": "criterio ventas"}},
                "por_categoria": [
                    {
                        "categoria": "Pay Grande",
                        "productos": [
                            {
                                "point_product_id": None,
                                "receta_id": 77,
                                "nombre": "Pay prueba",
                                "total_piezas": 10,
                                "precio": "120.00",
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(rows[0]["key"], "r77")
        self.assertEqual(rows[0]["total_final"], 8)
        self.assertEqual(rows[0]["nota"], "criterio ventas")

    def test_manual_adjustments_update_saved_result_summary(self):
        class PostData(dict):
            def get(self, key, default=None):
                return super().get(key, default)

        resultados, totals = _apply_manual_adjustments(
            {
                "resumen": {"total_piezas": 12, "total_ingreso": "1200.00"},
                "fechas_tabla": [{"iso": "2026-06-21", "label": "Dom 21 Jun"}],
                "por_categoria": [
                    {
                        "categoria": "Pastel Grande",
                        "productos": [
                            {
                                "point_product_id": 10,
                                "nombre": "Pastel prueba",
                                "total_piezas": 12,
                                "precio": "100.00",
                                "dias_lista": [{"fecha_iso": "2026-06-21", "recomendado": 12}],
                            }
                        ],
                    }
                ],
            },
            PostData({"ajuste_p10": "5", "nota_p10": "subir fin de semana", "ajuste_dia_p10_2026-06-21": "2"}),
        )

        self.assertEqual(totals["total_base"], 12)
        self.assertEqual(totals["total_ajuste"], 7)
        self.assertEqual(resultados["resumen"]["total_piezas"], 19)
        self.assertEqual(resultados["resumen"]["ajuste_piezas_ventas"], 7)
        self.assertEqual(resultados["ajustes_ventas"]["p10"]["nota"], "subir fin de semana")
        self.assertEqual(resultados["ajustes_ventas"]["p10"]["dias"]["2026-06-21"]["ajuste"], 2)

    def test_save_manual_adjustments_updates_snapshot_summary(self):
        class Forecast:
            resultado_json = {
                "resumen": {"total_piezas": 12, "total_ingreso": "1200.00"},
                "fechas_tabla": [{"iso": "2026-06-21", "label": "Dom 21 Jun"}],
                "por_categoria": [
                    {
                        "categoria": "Pastel Grande",
                        "productos": [
                            {
                                "point_product_id": 10,
                                "nombre": "Pastel prueba",
                                "total_piezas": 12,
                                "precio": "100.00",
                                "dias_lista": [{"fecha_iso": "2026-06-21", "recomendado": 12}],
                            }
                        ],
                    }
                ],
            }

            def save(self, update_fields):
                self.saved_fields = update_fields

        pronostico = Forecast()
        _save_manual_adjustments(pronostico, {"ajuste_dia_p10_2026-06-21": "2"})

        self.assertEqual(pronostico.resultado_json["resumen"]["total_piezas"], 14)
        self.assertEqual(pronostico.resultado_json["resumen"]["ajuste_piezas_ventas"], 2)
        self.assertEqual(pronostico.total_piezas, 14)

    def test_saved_forecast_detail_labels_confidence_as_saved_snapshot(self):
        template = (Path(__file__).resolve().parent / "templates" / "ventas" / "pronostico_detalle.html").read_text()

        self.assertIn("Confianza guardada", template)
        self.assertIn("cálculo generado", template)
        self.assertIn("vuelve a generar el pronóstico", template)
        self.assertIn("Revisión de ventas", template)
        self.assertIn("ventas:pronostico_ajustes", template)
        self.assertIn("Propuesta ERP", template)
        self.assertIn("Matriz por día", template)
        self.assertIn("ajuste_dia_{{ row.key }}_{{ day.fecha_iso }}", template)

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


class VentasProjectionEngineTests(TestCase):
    def test_projection_uses_operational_daily_forecast_with_three_week_lookback(self):
        branch = Sucursal.objects.create(codigo="GSV", nombre="Guasave")
        calls = []

        def fake_forecast(*, target_date, lookback_weeks, top_n):
            calls.append((target_date, lookback_weeks, top_n))
            qty = Decimal("18") if target_date.weekday() == 5 else Decimal("9")
            return {
                "rows": [
                    {
                        "branch_id": branch.id,
                        "recipe_id": 77,
                        "recipe_name": "Pay prueba",
                        "family": "Pay Grande",
                        "category": "Pay Grande",
                        "forecast_qty": qty,
                        "forecast_min_qty": qty - Decimal("2"),
                        "forecast_max_qty": qty + Decimal("3"),
                        "forecast_amount": qty * Decimal("100"),
                        "trend_factor": Decimal("1.12"),
                    }
                ],
                "validation": {"wape_pct": Decimal("12.5")},
            }

        with patch("ventas.services.proyecciones_engine._selected_recipe_ids", return_value=None), patch(
            "ventas.services.proyecciones_engine.build_daily_forecast_context",
            side_effect=fake_forecast,
        ):
            result = calcular_proyeccion_operativa(
                date(2026, 6, 19),
                date(2026, 6, 20),
                {branch.id},
                skus_incluidos=None,
            )

        self.assertEqual(calls, [(date(2026, 6, 19), 3, None), (date(2026, 6, 20), 3, None)])
        self.assertEqual(result["resumen"]["metodo"], "forecast-operativo-3-semanas")
        self.assertEqual(result["por_dia"][0]["total_piezas"], 9)
        self.assertEqual(result["por_dia"][1]["total_piezas"], 18)
        self.assertEqual(result["por_producto"][0]["por_dia"]["2026-06-20"], 18)
        self.assertEqual(result["por_producto"][0]["tendencia"], "sube")

    def test_projection_applies_special_event_uplift_without_copying_old_units(self):
        branch = Sucursal.objects.create(codigo="GSV", nombre="Guasave")

        def fake_forecast(*, target_date, lookback_weeks, top_n):
            return {
                "rows": [
                    {
                        "branch_id": branch.id,
                        "recipe_id": 77,
                        "recipe_name": "Pay prueba",
                        "family": "Pay Grande",
                        "category": "Pay Grande",
                        "forecast_qty": Decimal("10"),
                        "forecast_min_qty": Decimal("8"),
                        "forecast_max_qty": Decimal("12"),
                        "forecast_amount": Decimal("1000"),
                        "avg_price": Decimal("100"),
                        "trend_factor": Decimal("1"),
                    }
                ],
                "validation": {"wape_pct": Decimal("10")},
            }

        uplift_lookup = {
            (date(2026, 6, 21), branch.id, 77): Decimal("3"),
        }
        with patch("ventas.services.proyecciones_engine._selected_recipe_ids", return_value=None), patch(
            "ventas.services.proyecciones_engine.build_daily_forecast_context",
            side_effect=fake_forecast,
        ), patch("ventas.services.proyecciones_engine._context_uplift_lookup", return_value=uplift_lookup):
            result = calcular_proyeccion_operativa(
                date(2026, 6, 19),
                date(2026, 6, 21),
                {branch.id},
                skus_incluidos=None,
            )

        self.assertEqual(result["resumen"]["metodo"], "forecast-operativo-3-semanas+uplift-evento")
        self.assertEqual(result["por_producto"][0]["por_dia"]["2026-06-21"], 30)
        self.assertEqual(result["por_producto"][0]["por_dia"]["2026-06-19"], 10)
        self.assertEqual(result["resumen"]["comparables_evento"][2]["comparables"][0]["fecha_iso"], "2025-06-15")

    def test_context_uplift_uses_event_against_normal_comparable_days(self):
        branch = Sucursal.objects.create(codigo="GSV", nombre="Guasave")

        def fake_rows():
            yield {"fecha": date(2025, 6, 15), "sucursal_id": branch.id, "receta_id": 77, "qty": Decimal("30")}
            yield {"fecha": date(2025, 6, 8), "sucursal_id": branch.id, "receta_id": 77, "qty": Decimal("10")}
            yield {"fecha": date(2025, 6, 22), "sucursal_id": branch.id, "receta_id": 77, "qty": Decimal("10")}

        with patch("ventas.services.proyecciones_engine.FactVentaDiaria.objects") as manager:
            manager.filter.return_value.values.return_value.annotate.return_value = list(fake_rows())
            uplifts = _context_uplift_lookup([date(2026, 6, 21)], {branch.id})

        self.assertEqual(uplifts[(date(2026, 6, 21), branch.id, 77)], Decimal("3"))

    def test_projection_detects_high_season_dates(self):
        self.assertEqual(_season_name(date(2026, 12, 22)), "Temporada Navidad")
        self.assertEqual(_season_name(date(2026, 5, 9)), "Temporada Día de las Madres")
