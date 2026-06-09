from __future__ import annotations

from django.test import TestCase, override_settings

from syncfy_client.models import CuentaBancaria, LogSyncfy
from syncfy_client.tasks import _sincronizar_cuenta, sincronizar_movimientos_bancarios


class SyncfyTaskGuardTests(TestCase):
    @override_settings(SYNCFY_ENABLED=False)
    def test_task_exits_without_logs_when_disabled(self):
        result = sincronizar_movimientos_bancarios.run()

        self.assertEqual(result, {"status": "deshabilitada"})
        self.assertEqual(LogSyncfy.objects.count(), 0)

    def test_sincronizar_cuenta_omits_account_without_id_account(self):
        cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="BanBajio Principal",
            id_site_syncfy="site-1",
            id_credential="cred-1",
        )

        result = _sincronizar_cuenta(cuenta, token="token-1")

        self.assertEqual(result, {"status": "sin_account", "total": 0, "nuevos": 0})
        self.assertEqual(LogSyncfy.objects.filter(cuenta=cuenta, nivel=LogSyncfy.NIVEL_WARN).count(), 1)
