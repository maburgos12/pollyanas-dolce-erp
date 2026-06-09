from __future__ import annotations

from unittest.mock import patch

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

    @patch("syncfy_client.tasks.rango_unix_syncfy", return_value=(100, 200))
    @patch("syncfy_client.tasks.guardar_transacciones", return_value=(1, 1))
    @patch("syncfy_client.tasks.descargar_transacciones", return_value=[{"id_transaction": "tx-1"}])
    @patch("syncfy_client.tasks.actualizar_cuenta_desde_syncfy")
    @patch(
        "syncfy_client.tasks.obtener_cuentas",
        return_value=[{"id_account": "account-1", "number": "410641890201"}],
    )
    def test_sincronizar_cuenta_downloads_transactions_by_account(
        self,
        _obtener_cuentas,
        _actualizar_cuenta,
        descargar_transacciones,
        _guardar_transacciones,
        _rango_unix_syncfy,
    ):
        cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="BanBajio Principal",
            id_site_syncfy="site-1",
            id_credential="cred-1",
            id_account="account-1",
        )

        result = _sincronizar_cuenta(cuenta, token="token-1")

        self.assertEqual(result, {"status": "ok", "total": 1, "nuevos": 1})
        descargar_transacciones.assert_called_once_with(
            id_credential="cred-1",
            id_account="account-1",
            token="token-1",
            dt_refresh_from=100,
            dt_refresh_to=200,
        )
