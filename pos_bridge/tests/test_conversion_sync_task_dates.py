from datetime import date
from unittest.mock import patch

from django.conf import settings
from django.test import SimpleTestCase

from pos_bridge.tasks.celery_tasks import task_conversion_sync


class ConversionSyncTaskDateTests(SimpleTestCase):
    def test_conversion_sync_is_scheduled_daily(self):
        schedule = settings.CELERY_BEAT_SCHEDULE["pos_bridge: sync conversiones mensual"]["schedule"]

        self.assertEqual(schedule.day_of_month, frozenset(range(1, 32)))

    @patch("pos_bridge.services.conversion_sync_service.sync_conversion_lines")
    @patch("pos_bridge.tasks.celery_tasks.timezone.localdate", return_value=date(2026, 9, 4))
    def test_default_sync_covers_closed_days_of_current_month(self, _localdate, sync_conversion_lines):
        sync_conversion_lines.return_value = {"ok": True}

        task_conversion_sync()

        sync_conversion_lines.assert_called_once_with(
            date_from=date(2026, 9, 3),
            date_to=date(2026, 9, 3),
        )
