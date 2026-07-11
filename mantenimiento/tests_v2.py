from datetime import datetime
from zoneinfo import ZoneInfo

from django.test import SimpleTestCase

from mantenimiento.services_history import canonical_status, period_bounds


class MaintenanceHistoryDomainTests(SimpleTestCase):
    def test_30d_uses_mazatlan_inclusive_start_exclusive_end(self):
        now = datetime(2026, 7, 11, 15, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("30d", now=now)

        self.assertEqual(start.isoformat(), "2026-06-12T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-12T00:00:00-07:00")

    def test_90d_uses_inclusive_start_and_exclusive_end(self):
        now = datetime(2026, 7, 11, 15, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("90d", now=now)

        self.assertEqual(start.isoformat(), "2026-04-13T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-12T00:00:00-07:00")

    def test_week_starts_on_monday_and_ends_next_monday(self):
        now = datetime(2026, 7, 8, 12, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("semana", now=now)

        self.assertEqual(start.isoformat(), "2026-07-06T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-13T00:00:00-07:00")

    def test_week_on_sunday_keeps_same_monday_boundaries(self):
        now = datetime(2026, 7, 12, 23, 59, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("semana", now=now)

        self.assertEqual(start.isoformat(), "2026-07-06T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-13T00:00:00-07:00")

    def test_month_uses_first_day_and_next_month_exclusive(self):
        now = datetime(2026, 7, 31, 23, 59, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("mes", now=now)

        self.assertEqual(start.isoformat(), "2026-07-01T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-08-01T00:00:00-07:00")

    def test_todo_has_no_start_and_ends_after_current_local_day(self):
        now = datetime(2026, 7, 11, 15, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("todo", now=now)

        self.assertIsNone(start)
        self.assertEqual(end.isoformat(), "2026-07-12T00:00:00-07:00")

    def test_invalid_period_raises_value_error(self):
        with self.assertRaisesMessage(ValueError, "Periodo no soportado"):
            period_bounds("trimestre")

    def test_source_statuses_map_without_losing_programmed(self):
        self.assertEqual(canonical_status("orden", "PENDIENTE"), "abierto")
        self.assertEqual(canonical_status("orden", "EN_PROCESO"), "en_proceso")
        self.assertEqual(canonical_status("orden", "CERRADA"), "cerrado")
        self.assertEqual(canonical_status("orden", "CANCELADA"), "cancelado")
        self.assertEqual(canonical_status("reporte_unidad", "ABIERTO"), "abierto")
        self.assertEqual(canonical_status("reporte_unidad", "EN_PROCESO"), "en_proceso")
        self.assertEqual(canonical_status("reporte_unidad", "PROGRAMADO"), "programado")
        self.assertEqual(canonical_status("reporte_unidad", "CERRADO"), "cerrado")
        self.assertEqual(canonical_status("reporte_unidad", "CANCELADO"), "cancelado")
