from __future__ import annotations

import os

from django.core.management import call_command
from django.test import TestCase, override_settings


@override_settings(TIME_ZONE="America/Mazatlan")
class SetupCelerySchedulesCommandTests(TestCase):
    def test_registers_expected_periodic_tasks_idempotently(self):
        from django_celery_beat.models import PeriodicTask

        original = os.environ.get("POS_BRIDGE_REALTIME_INTERVAL_MINUTES")
        os.environ["POS_BRIDGE_REALTIME_INTERVAL_MINUTES"] = "5"
        try:
            call_command("setup_celery_schedules")
            call_command("setup_celery_schedules")
        finally:
            if original is None:
                os.environ.pop("POS_BRIDGE_REALTIME_INTERVAL_MINUTES", None)
            else:
                os.environ["POS_BRIDGE_REALTIME_INTERVAL_MINUTES"] = original

        task_names = set(PeriodicTask.objects.values_list("name", flat=True))
        self.assertEqual(
            task_names,
            {
                "pos_bridge: ventas cerradas diario",
                "pos_bridge: inventario completo diario",
                "pos_bridge: mermas diario",
                "pos_bridge: produccion diario",
                "pos_bridge: transferencias diario",
                "pos_bridge: inventario realtime",
                "pos_bridge: recetas semanal",
                "pos_bridge: retry jobs fallidos",
                "pos_bridge: auditoria recetas semanal",
            },
        )
        self.assertEqual(PeriodicTask.objects.count(), 9)
        realtime = PeriodicTask.objects.get(name="pos_bridge: inventario realtime")
        self.assertEqual(realtime.interval.every, 5)
