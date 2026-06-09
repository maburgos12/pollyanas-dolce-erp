from __future__ import annotations

from datetime import date

from django.test import SimpleTestCase, TestCase, override_settings

from sat_client.models import LogDescargaSat
from sat_client.tasks import periodos_mensuales_a_descargar
from sat_client.tasks import ejecutar_descarga_sat_nocturna


class SatTaskPeriodTests(SimpleTestCase):
    def test_periodos_mensuales_a_descargar_uses_complete_previous_months(self):
        periodos = periodos_mensuales_a_descargar(2, hoy=date(2026, 6, 8))

        self.assertEqual(
            periodos,
            [
                (date(2026, 4, 1), date(2026, 4, 30)),
                (date(2026, 5, 1), date(2026, 5, 31)),
            ],
        )


class SatTaskEnabledFlagTests(TestCase):
    @override_settings(SAT_DESCARGA_ENABLED=False)
    def test_task_exits_without_logs_when_disabled(self):
        result = ejecutar_descarga_sat_nocturna.run()

        self.assertEqual(result, {"status": "deshabilitada"})
        self.assertEqual(LogDescargaSat.objects.count(), 0)
