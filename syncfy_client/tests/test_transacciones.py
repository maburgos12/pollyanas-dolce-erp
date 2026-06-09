from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.test import TestCase, override_settings

from syncfy_client.models import CuentaBancaria, MovimientoBancario
from syncfy_client.services.transacciones import (
    descargar_transacciones,
    guardar_transacciones,
    rango_unix_syncfy,
    timestamp_to_datetime,
)


@override_settings(TIME_ZONE="America/Mazatlan")
class SyncfyTransaccionesTests(TestCase):
    def test_timestamp_to_datetime_returns_aware_datetime_in_project_timezone(self):
        result = timestamp_to_datetime(1_700_000_000)

        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.tzinfo.key, "America/Mazatlan")

    def test_rango_unix_syncfy_uses_configured_days(self):
        now = datetime(2026, 6, 9, 12, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        dt_from, dt_to = rango_unix_syncfy(dias_atras=7, now=now)

        self.assertEqual(dt_to - dt_from, 7 * 24 * 60 * 60)

    def test_guardar_transacciones_is_idempotent_by_transaction_id(self):
        cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="BanBajio Principal",
            id_site_syncfy="site-1",
            id_credential="cred-1",
        )
        transacciones = [
            {
                "id_transaction": "tx-1",
                "id_account": "account-1",
                "id_credential": "cred-1",
                "description": "SPEI ENVIADO",
                "amount": "-123.45",
                "currency": "MXN",
                "dt_transaction": 1_700_000_000,
                "dt_refresh": 1_700_000_100,
                "extra": {"reference": "abc"},
            }
        ]

        first_total, first_new = guardar_transacciones(cuenta=cuenta, transacciones=transacciones)
        second_total, second_new = guardar_transacciones(cuenta=cuenta, transacciones=transacciones)

        self.assertEqual((first_total, first_new), (1, 1))
        self.assertEqual((second_total, second_new), (1, 0))
        self.assertEqual(MovimientoBancario.objects.count(), 1)
        movimiento = MovimientoBancario.objects.get(id_transaction="tx-1")
        self.assertEqual(movimiento.tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(movimiento.monto, Decimal("123.45"))

    def test_descargar_transacciones_filters_by_account(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def get(self, path, *, params, token):
                self.calls.append((path, params, token))
                return [{"id_transaction": "tx-1"}]

        client = FakeClient()

        result = descargar_transacciones(
            id_credential="cred-1",
            id_account="account-1",
            token="token-1",
            dt_refresh_from=100,
            dt_refresh_to=200,
            client=client,
            limit=500,
        )

        self.assertEqual(result, [{"id_transaction": "tx-1"}])
        self.assertEqual(client.calls[0][0], "/transactions")
        self.assertEqual(client.calls[0][1]["id_credential"], "cred-1")
        self.assertEqual(client.calls[0][1]["id_account"], "account-1")
        self.assertEqual(client.calls[0][2], "token-1")
