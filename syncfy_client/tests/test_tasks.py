from __future__ import annotations

from django.test import TestCase, override_settings

from syncfy_client.models import LogSyncfy
from syncfy_client.tasks import sincronizar_movimientos_bancarios


class SyncfyTaskGuardTests(TestCase):
    @override_settings(SYNCFY_ENABLED=False)
    def test_task_exits_without_logs_when_disabled(self):
        result = sincronizar_movimientos_bancarios.run()

        self.assertEqual(result, {"status": "deshabilitada"})
        self.assertEqual(LogSyncfy.objects.count(), 0)
