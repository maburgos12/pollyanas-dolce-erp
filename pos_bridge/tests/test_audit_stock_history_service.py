from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from django.test import TestCase
from django.db import connection
from django.test.utils import CaptureQueriesContext

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
    def _stored_history(self, rows, *, metadata=None):
        service = AuditStockHistoryService()
        record = service._canonical_import(self.branch, self.product)
        record.raw_metadata = {
            "source": "POINT_STOCK_HISTORY_API", "fetched_rows": len(rows),
            "history_limit": 500, "fetched_at": "2026-10-02T07:00:00+00:00",
            **(metadata or {}),
        }
        record.save()
        for raw in rows:
            movement_id, defaults = service._parse_row(raw)
            PointProductHistoryRow.objects.create(
                import_record=record, row_number=movement_id, **defaults,
            )
        return record

    def test_raw_utc_month_cut_is_read_only_and_two_queries(self):
        record = self._stored_history([
            _row(910, "VENTA", "2026-09-01T01:00:00", 9, 10, 1),
            _row(911, "VENTA", "2026-10-01T01:00:00", 2, 2, 0),
            _row(912, "VENTA", "2026-10-01T07:00:00", 3, 3, 0),
        ])
        original = list(record.rows.values())
        line = SimpleNamespace(branch=self.branch, product=self.product, difference=1)
        with self.assertNumQueries(2):
            result = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        history = result[(self.branch.pk, self.product.pk)]
        self.assertEqual(history.sales, Decimal("2"))
        self.assertEqual(history.movement_ids, (911,))
        self.assertEqual(history.coverage_status, "COMPLETE")
        self.assertEqual(list(record.rows.values()), original)
        self.assertEqual(AuditStockHistoryService().reconcile(
            self.branch, self.product, date(2026, 9, 1)), history)

    def test_truncated_coverage_uses_raw_of_exact_last_capture_boundary(self):
        record = self._stored_history([
            _row(920, "VENTA", "2026-09-01T06:00:00", 1, 2, 1),
        ], metadata={
            "fetched_rows": 500, "earliest_movement_at": "2026-09-01T13:00:00+00:00",
            "fetched_movement_ids": [920],
        })
        line = SimpleNamespace(branch=self.branch, product=self.product, difference=1)
        with self.assertNumQueries(2):
            history = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        self.assertEqual(history[(self.branch.pk, self.product.pk)].coverage_status, "COMPLETE")
        # An old retained row cannot grant coverage to a newer truncated capture.
        movement_id, defaults = AuditStockHistoryService._parse_row(
            _row(921, "VENTA", "2026-09-02T06:00:00", 1, 2, 1))
        PointProductHistoryRow.objects.create(import_record=record, row_number=movement_id, **defaults)
        record.raw_metadata.update({
            "earliest_movement_at": "2026-09-02T13:00:00+00:00", "fetched_movement_ids": [921],
        })
        record.save()
        with self.assertNumQueries(2):
            history = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        self.assertEqual(history[(self.branch.pk, self.product.pk)].coverage_status, "INCOMPLETE")

    def test_explicit_offset_boundary_is_not_shifted_again(self):
        self._stored_history([
            _row(922, "VENTA", "2026-09-01T08:00:00+00:00", 1, 2, 1),
        ], metadata={"fetched_rows": 500, "earliest_movement_at": "2026-09-01T08:00:00+00:00"})
        result = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertEqual(result.coverage_status, "INCOMPLETE")
        self.assertEqual(result.sales, Decimal("1"))

    def test_malformed_raw_date_cannot_use_persisted_date_as_authority(self):
        record = self._stored_history([_row(923, "VENTA", "2026-09-15T12:00:00", 1, 2, 1)])
        PointProductHistoryRow.objects.filter(import_record=record).update(raw_payload={"Fecha": "invalid"})
        history = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertEqual(history.coverage_status, "INCOMPLETE")
        self.assertEqual(history.sales, Decimal("0"))
        self.assertEqual(history.unknown_movement_ids, (923,))

    def test_capture_locks_effective_and_legacy_month_before_row_write(self):
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(924, "VENTA", "2026-10-01T01:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 9, 1))
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql) if "pg_advisory_xact_lock" in statement]
        self.assertTrue(any("202609" in statement for _, statement in locks))
        self.assertTrue(any("202610" in statement for _, statement in locks))
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))

    def test_capture_also_locks_previous_months_of_an_updated_identity(self):
        record = self._stored_history([
            _row(925, "VENTA", "2026-08-01T01:00:00", 1, 2, 1),
        ])
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(925, "VENTA", "2026-10-01T01:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 9, 1), force=True)
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql) if "pg_advisory_xact_lock" in statement]
        for month in ("202607", "202608", "202609", "202610"):
            self.assertTrue(any(month in statement for _, statement in locks))
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'UPDATE "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))
        self.assertEqual(record.rows.count(), 1)

    def test_truncated_boundary_outside_window_is_prefetched_in_two_queries(self):
        self._stored_history([
            _row(926, "VENTA", "2026-08-01T01:00:00", 1, 2, 1),
            _row(927, "VENTA", "2026-09-10T01:00:00", 1, 2, 1),
        ], metadata={
            "fetched_rows": 500, "earliest_movement_at": "2026-08-01T08:00:00+00:00",
            "fetched_movement_ids": [926, 927],
        })
        line = SimpleNamespace(branch=self.branch, product=self.product, difference=1)
        with self.assertNumQueries(2):
            result = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        history = result[(self.branch.pk, self.product.pk)]
        self.assertEqual(history.coverage_status, "COMPLETE")
        self.assertEqual(history.movement_ids, (927,))

    def test_recapture_metadata_locks_retained_and_empty_months_before_writes(self):
        record = self._stored_history([
            _row(940, "VENTA", "2026-07-15T12:00:00", 1, 2, 1),
        ])
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(1000 + i, "VENTA", "2026-10-03T12:00:00", 1, 2, 1)
            for i in range(500)
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 10, 1), force=True)
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql)
                 if "pg_advisory_xact_lock" in statement]
        for month in ("202607", "202608", "202609", "202610", "202611"):
            self.assertTrue(any(month in statement for _, statement in locks), month)
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))
        self.assertEqual(record.rows.count(), 501)

    def test_recapture_locks_existing_closure_without_history_rows_and_its_successor(self):
        from recetas.models import ProductoMonthClosure
        ProductoMonthClosure.objects.create(
            month_start=date(2026, 5, 1), month_end=date(2026, 5, 31),
        )
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(950, "VENTA", "2026-10-03T12:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 10, 1))
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql)
                 if "pg_advisory_xact_lock" in statement]
        for month in ("202605", "202606"):
            self.assertTrue(any(month in statement for _, statement in locks), month)
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))

    def test_recapture_locks_historical_closing_source_without_case_or_month_closure(self):
        from pos_bridge.models import PointHistoricalInventoryClosing, PointHistoricalInventoryClosingLine
        closing = PointHistoricalInventoryClosing.objects.create(
            operational_date=date(2026, 5, 31), source="POINT_STOCK_HISTORY",
            source_fingerprint="historical-closing-before-first-audit",
        )
        PointHistoricalInventoryClosingLine.objects.create(
            closing=closing, branch=self.branch, product=self.product, stock=1,
        )
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(951, "VENTA", "2026-10-03T12:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 10, 1))
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql)
                 if "pg_advisory_xact_lock" in statement]
        for month in ("202605", "202606"):
            self.assertTrue(any(month in statement for _, statement in locks), month)
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))

    def test_complete_continuous_utc_history_exposes_documentary_boundaries(self):
        self._stored_history([
            _row(930, "VENTA", "2026-09-01T08:00:00", 1, 4, 3),
            _row(931, "VENTA", "2026-10-01T06:00:00", 2, 3, 1),
        ])
        history = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertEqual(getattr(history, "documentary_opening", None), Decimal("4"))
        self.assertEqual(getattr(history, "documentary_closing", None), Decimal("1"))
        self.assertEqual(getattr(history, "documentary_boundary_movement_ids", ()), (930, 931))
        serialized = history.as_dict(opening=Decimal("4"), point_closing=Decimal("1"))
        self.assertEqual(serialized.get("documentary_opening"), "4.000")
        self.assertEqual(serialized.get("documentary_closing"), "1.000")
        self.assertEqual(serialized.get("documentary_boundary_movement_ids"), [930, 931])

    def test_documentary_boundaries_require_complete_known_continuous_rows(self):
        scenarios = (
            ([], {}),
            ([_row(932, "VENTA", "2026-09-10T08:00:00", 1, 4, 3)],
             {"fetched_at": "2026-10-01T01:00:00+00:00"}),
            ([_row(932, "VENTA", "2026-09-10T08:00:00", 1, 4, 3),
              _row(933, "VENTA", "2026-09-11T08:00:00", 1, 5, 4)], {}),
            ([_row(932, "UNKNOWN", "2026-09-10T08:00:00", 1, 4, 3)], {}),
            ([_row(932, "VENTA", "2026-09-10T08:00:00", 1, 4, 2)], {}),
        )
        for rows, metadata in scenarios:
            with self.subTest(rows=rows, metadata=metadata):
                PointProductHistoryImport.objects.all().delete()
                self._stored_history(rows, metadata=metadata)
                history = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
                self.assertIsNone(getattr(history, "documentary_opening", None))
                self.assertIsNone(getattr(history, "documentary_closing", None))
                self.assertEqual(getattr(history, "documentary_boundary_movement_ids", ()), ())

    def test_zero_difference_trace_case_can_review_cached_history(self):
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(900, 'ENTRADA POR PRODUCCIÓN', '2026-08-10T10:00:00-07:00', 1, 0, 1)]))
        service.capture(self.branch, self.product, self.month)
        case = SimpleNamespace(branch=self.branch, product=self.product, difference=Decimal('0'))
        result = AuditStockHistoryService().reconcile_many([case], self.month, include_zero_difference=True)
        self.assertEqual(result[(self.branch.pk, self.product.pk)].production, Decimal('1'))

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

    def test_impossible_capture_counts_never_grant_month_coverage(self):
        for fetched_rows, history_limit in ((-1, 500), (1, 0), (1, -1), (501, 500)):
            with self.subTest(fetched_rows=fetched_rows, history_limit=history_limit):
                record = SimpleNamespace(raw_metadata={
                    "fetched_rows": fetched_rows, "history_limit": history_limit,
                    "fetched_at": "2026-10-02T07:00:00+00:00",
                    "earliest_movement_at": "2026-08-01T08:00:00+00:00",
                })
                self.assertFalse(AuditStockHistoryService._covers_month(
                    record, date(2026, 9, 1), boundary_rows=[]))

    def test_adjustment_uses_verified_stock_effect_not_raw_quantity_sign(self):
        scenarios = (
            ("AJUSTE SALIDA INVENTARIO", 10, 20, 10, -10),
            ("AJUSTE SALIDA INVENTARIO", -10, 20, 10, -10),
            ("AJUSTE ENTRADA INVENTARIO", 10, 10, 20, 10),
            ("AJUSTE INVENTARIO", -10, 20, 10, -10),
            ("AJUSTE INVENTARIO", 10, 10, 20, 10),
            ("AJUSTE SALIDA INVENTARIO", 10, 20, 11, None),
            ("AJUSTE SALIDA INVENTARIO", 10, 10, 20, None),
            ("AJUSTE ENTRADA INVENTARIO", 10, 20, 10, None),
            ("AJUSTE SALIDA INVENTARIO", 10, 20, 20, None),
        )
        service = AuditStockHistoryService()
        record = SimpleNamespace(raw_metadata={})
        for movement, quantity, previous, new, expected in scenarios:
            with self.subTest(movement=movement, quantity=quantity, new=new):
                row = SimpleNamespace(
                    row_number=901, movement_type=movement,
                    quantity=Decimal(quantity), previous_existence=Decimal(previous),
                    new_existence=Decimal(new),
                )
                result = service._reconcile_record(record, self.month, [row])
                if expected is None:
                    self.assertEqual(result.identified_adjustment, Decimal("0"))
                    self.assertEqual(result.unknown_movement_ids, (901,))
                    self.assertNotIn("identified_adjustment", result.movement_ids_by_category)
                else:
                    self.assertEqual(result.identified_adjustment, Decimal(expected))
                    self.assertEqual(result.expected_closing(Decimal(previous)), Decimal(new))
                    self.assertEqual(result.unknown_movement_ids, ())
                    self.assertEqual(result.movement_ids_by_category["identified_adjustment"], (901,))

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

    def test_sale_cancellation_reduces_net_sales_when_stock_is_returned(self):
        rows = [
            _row(301, "VENTA", "2026-08-20T10:00:00-07:00", 1, 2, 1),
            _row(302, "CANCELACION VENTA", "2026-08-20T11:00:00-07:00", 1, 1, 2),
        ]
        result = AuditStockHistoryService(client=_FakePointClient(rows)).capture(self.branch, self.product, self.month)
        self.assertEqual(result.sales, Decimal("0"))
        self.assertEqual(result.expected_closing(Decimal("2")), Decimal("2"))
        self.assertEqual(result.movement_ids_by_category["sales"], (301, 302))
        self.assertEqual(result.unknown_movement_ids, ())

    def test_unproven_cancellation_remains_unknown(self):
        for movement, previous, new in (("CANCELACION VENTA", 1, 0), ("CANCELACION VENTA", 1, 3), ("CANCELACION TRANSFERENCIA", 1, 2)):
            with self.subTest(movement=movement, previous=previous, new=new):
                result = AuditStockHistoryService(client=_FakePointClient([
                    _row(303, movement, "2026-08-20T11:00:00-07:00", 1, previous, new),
                ])).capture(self.branch, self.product, self.month, force=True)
                self.assertEqual(result.unknown_movement_ids, (303,))

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
