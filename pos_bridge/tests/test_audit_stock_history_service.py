from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from django.test import TestCase

from pos_bridge.models import (
    PointBranch,
    PointProduct,
    PointProductHistoryImport,
    PointProductHistoryRow,
)
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService


def _row(
    movement_id,
    movement,
    stamp,
    quantity,
    previous,
    new,
    *,
    cancelled=False,
):
    return {
        "FK_Movimiento": movement_id,
        "FK_Tipo_Movimiento": movement_id,
        "Movimiento": movement,
        "Fecha": stamp,
        "Cantidad": quantity,
        "Existencia_anterior": previous,
        "Existencia_nueva": new,
        "Costo_Total": 0,
        "Costo_Unitario": 0,
        "Cancelado": cancelled,
    }


class _FakePointClient:
    def __init__(self, rows):
        self.rows = rows
        self.login_calls = 0
        self.history_calls = []

    def login(self):
        self.login_calls += 1

    def get_stock_history(self, product_id, branch_id, *, movements=500):
        self.history_calls.append((str(product_id), str(branch_id), movements))
        return list(self.rows)


class AuditStockHistoryServiceTests(TestCase):
    def setUp(self):
        self.month = date(2026, 8, 1)
        self.branch = PointBranch.objects.create(external_id="8", name="CEDIS")
        self.product = PointProduct.objects.create(
            external_id="109",
            sku="0109",
            name="Pastel de 3 Pecados Chico",
        )

    def test_cache_must_have_been_fetched_after_the_month_finished(self):
        metadata = {"fetched_rows": 2, "history_limit": 500}
        record = SimpleNamespace(raw_metadata=metadata)
        for fetched_at in (None, "invalid", "2026-09-30T23:59:59-07:00", "2026-10-01T06:59:59+00:00", "2026-10-01T07:00:00"):
            with self.subTest(fetched_at=fetched_at):
                metadata["fetched_at"] = fetched_at
                self.assertFalse(AuditStockHistoryService._covers_month(record, date(2026, 9, 1)))
        metadata["fetched_at"] = "2026-10-01T07:00:00+00:00"
        self.assertTrue(AuditStockHistoryService._covers_month(record, date(2026, 9, 1)))

    def test_repeated_capture_upserts_the_same_point_movements(self):
        client = _FakePointClient(
            [
                _row(
                    101,
                    "ENTRADA POR PRODUCCIÓN",
                    "2026-08-02T08:00:00-07:00",
                    2,
                    23,
                    25,
                )
            ]
        )
        service = AuditStockHistoryService(client=client)

        first = service.capture(self.branch, self.product, self.month)
        second = service.capture(self.branch, self.product, self.month, force=True)

        self.assertEqual(PointProductHistoryImport.objects.count(), 1)
        self.assertEqual(PointProductHistoryRow.objects.count(), 1)
        self.assertEqual(first.movement_ids, second.movement_ids)
        self.assertEqual(client.history_calls, [("109", "8", 500), ("109", "8", 500)])

    def test_complete_cached_history_avoids_a_second_point_call(self):
        client = _FakePointClient(
            [
                _row(
                    102,
                    "SALIDA POR CONVERSIÓN",
                    "2026-07-31T23:00:00-07:00",
                    -1,
                    24,
                    23,
                ),
                _row(
                    103,
                    "ENTRADA POR CONVERSIÓN",
                    "2026-08-14T10:00:00-07:00",
                    2,
                    4,
                    6,
                ),
            ]
        )
        service = AuditStockHistoryService(client=client)

        service.capture(self.branch, self.product, self.month)
        service.capture(self.branch, self.product, self.month)

        self.assertEqual(len(client.history_calls), 1)

    def test_reconciles_three_sins_and_ignores_cancelled_rows(self):
        rows = [
            _row(1, "ENTRADA POR PRODUCCIÓN", "2026-08-02T08:00:00-07:00", 518, 23, 541),
            _row(2, "ENTRADA POR CONVERSIÓN", "2026-08-14T10:00:00-07:00", 2, 541, 543),
            _row(3, "AJUSTE ENTRADA INVENTARIO", "2026-08-15T10:00:00-07:00", 4, 543, 547),
            _row(4, "RETORNO POR TRANSFERENCIA", "2026-08-16T10:00:00-07:00", 2, 547, 549),
            _row(5, "SALIDA POR CONVERSIÓN", "2026-08-20T10:00:00-07:00", 10, 549, 539),
            _row(6, "SALIDA POR TRANSFERENCIA", "2026-08-31T20:00:00-07:00", 533, 539, 6),
            _row(
                7,
                "SALIDA POR CONVERSIÓN",
                "2026-08-31T21:00:00-07:00",
                -99,
                6,
                -93,
                cancelled=True,
            ),
        ]
        rows[4]["Cancelado"] = "false"
        result = AuditStockHistoryService(client=_FakePointClient(rows)).capture(
            self.branch,
            self.product,
            self.month,
        )

        self.assertEqual(result.coverage_status, "COMPLETE")
        self.assertEqual(result.production, Decimal("518"))
        self.assertEqual(result.conversion_in, Decimal("2"))
        self.assertEqual(result.conversion_out, Decimal("10"))
        self.assertEqual(result.transfer_in, Decimal("2"))
        self.assertEqual(result.transfer_out, Decimal("533"))
        self.assertEqual(result.identified_adjustment, Decimal("4"))
        self.assertEqual(result.expected_closing(Decimal("23")), Decimal("6"))
        self.assertEqual(result.unexplained_remainder(Decimal("23"), Decimal("6")), Decimal("0"))
        self.assertNotIn(7, result.movement_ids)

    def test_history_at_limit_that_does_not_reach_month_start_is_incomplete(self):
        rows = [
            _row(
                movement_id,
                "SALIDA POR TRANSFERENCIA",
                f"2026-08-{(movement_id % 20) + 2:02d}T10:00:00-07:00",
                -1,
                1000 - movement_id,
                999 - movement_id,
            )
            for movement_id in range(1, 501)
        ]

        result = AuditStockHistoryService(client=_FakePointClient(rows)).capture(
            self.branch,
            self.product,
            self.month,
        )

        self.assertEqual(result.coverage_status, "INCOMPLETE")

    def test_reconcile_many_loads_all_cached_histories_in_two_queries(self):
        second_product = PointProduct.objects.create(
            external_id="110",
            sku="0110",
            name="Segundo producto",
        )
        client = _FakePointClient(
            [
                _row(
                    201,
                    "AJUSTE ENTRADA INVENTARIO",
                    "2026-08-10T10:00:00-07:00",
                    1,
                    0,
                    1,
                )
            ]
        )
        service = AuditStockHistoryService(client=client)
        service.capture(self.branch, self.product, self.month)
        service.capture(self.branch, second_product, self.month, force=True)
        lines = [
            SimpleNamespace(
                branch=self.branch,
                product=self.product,
                difference=Decimal("-1"),
            ),
            SimpleNamespace(
                branch=self.branch,
                product=second_product,
                difference=Decimal("-1"),
            ),
        ]

        with self.assertNumQueries(2):
            reconciliations = AuditStockHistoryService().reconcile_many(
                lines,
                self.month,
            )

        self.assertEqual(len(reconciliations), 2)
