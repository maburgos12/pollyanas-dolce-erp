from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase
from django.urls import reverse

from ventas.services.pronostico_engine import (
    _apply_special_context_forecast,
    _previous_special_context_day,
    _special_day_name,
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
