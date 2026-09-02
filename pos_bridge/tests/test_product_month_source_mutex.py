from datetime import date, datetime, timezone as datetime_timezone
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import patch

from django.db import close_old_connections, transaction
from django.test import SimpleTestCase, TransactionTestCase, override_settings

from pos_bridge.services.product_month_source_mutex import (
    lock_product_month_sources,
    month_start,
    snapshot_affected_months,
)
from pos_bridge.services.movement_sync_service import PointMovementSyncService


class ProductMonthSourceDateTests(SimpleTestCase):
    def test_utc_timestamp_belongs_to_business_month(self):
        self.assertEqual(
            month_start(datetime(2026, 9, 1, 1, tzinfo=datetime_timezone.utc)),
            date(2026, 8, 1),
        )

    @override_settings(PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS=3)
    def test_snapshot_coordinates_target_month_ends_within_tolerance(self):
        expected = (date(2026, 8, 1), date(2026, 9, 1))
        for captured in (date(2026, 8, 29), date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 3)):
            with self.subTest(captured=captured):
                self.assertEqual(snapshot_affected_months(captured), expected)
        self.assertEqual(snapshot_affected_months(date(2026, 9, 4)), ())
        self.assertEqual(snapshot_affected_months(date(2026, 9, 15)), ())


class ProductMonthSourceMutexBoundaryTests(TransactionTestCase):
    def test_waste_writer_uses_local_august_for_utc_september_timestamp(self):
        acquired_months = []
        service = PointMovementSyncService()
        item = SimpleNamespace(
            movement_at=datetime(2026, 9, 1, 1, tzinfo=datetime_timezone.utc),
            branch={},
        )

        def acquire(values):
            result = lock_product_month_sources(values)
            acquired_months.extend(result)
            return result

        with (
            patch("pos_bridge.services.movement_sync_service.lock_product_month_sources", side_effect=acquire),
            patch.object(service, "_upsert_branch", side_effect=RuntimeError("stop after mutex")),
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after mutex"):
                service.persist_waste_lines(SimpleNamespace(), [item])
        self.assertEqual(acquired_months, [date(2026, 8, 1)])

    def test_boundary_capture_blocks_august_but_not_distant_month(self):
        errors = []
        august_acquired = Event()
        distant_acquired = Event()

        def acquire(month, acquired):
            close_old_connections()
            try:
                with transaction.atomic():
                    lock_product_month_sources([month])
                    acquired.set()
            except Exception as exc:
                errors.append(exc)
            finally:
                close_old_connections()

        with transaction.atomic():
            lock_product_month_sources(snapshot_affected_months(date(2026, 9, 2)))
            august = Thread(target=acquire, args=(date(2026, 8, 1), august_acquired))
            distant = Thread(target=acquire, args=(date(2026, 12, 1), distant_acquired))
            august.start()
            distant.start()
            self.assertTrue(distant_acquired.wait(3))
            self.assertFalse(august_acquired.wait(0.2))
        august.join(3)
        distant.join(3)
        self.assertTrue(august_acquired.is_set())
        self.assertEqual(errors, [])
