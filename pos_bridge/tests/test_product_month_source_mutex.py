from datetime import date, datetime, timezone as datetime_timezone
from threading import Event, Thread
from time import monotonic
from types import SimpleNamespace
from unittest.mock import patch

from django.db import close_old_connections, connection, transaction
from django.test import SimpleTestCase, TransactionTestCase, override_settings

from pos_bridge.models import PointBranch, PointSyncJob, PointTransferLine
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.services.product_month_source_mutex import (
    lock_product_month_sources,
    month_start,
    snapshot_affected_months,
)
from pos_bridge.services.sync_service import PointSyncService


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
    def test_legacy_sales_writer_waits_for_its_local_month_not_utc_month(self):
        for locked_month, should_wait in ((date(2026, 8, 1), True), (date(2026, 9, 1), False)):
            with self.subTest(locked_month=locked_month):
                finished = Event()
                errors = []

                def write():
                    close_old_connections()
                    try:
                        result = SimpleNamespace(
                            sale_date=datetime(2026, 9, 1, 1, tzinfo=datetime_timezone.utc),
                            branch={
                                "external_id": f"legacy-mutex-{locked_month.month}",
                                "name": "Legacy mutex",
                                "status": "ACTIVE",
                                "metadata": {},
                            },
                            sales_rows=[],
                        )
                        PointSyncService().persist_daily_sales(SimpleNamespace(), result)
                        finished.set()
                    except Exception as exc:
                        errors.append(exc)
                    finally:
                        close_old_connections()

                writer = Thread(target=write)
                try:
                    with transaction.atomic():
                        lock_product_month_sources([locked_month])
                        writer.start()
                        if should_wait:
                            self.assertFalse(finished.wait(0.2))
                        else:
                            self.assertTrue(finished.wait(3), errors)
                finally:
                    writer.join(3)
                self.assertFalse(writer.is_alive())
                self.assertEqual(errors, [])
                self.assertTrue(finished.is_set())

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

    def test_transfer_writer_locks_all_operational_months_before_first_write(self):
        acquired_months = []
        lock_depths = []
        service = PointMovementSyncService()
        item = SimpleNamespace(
            registered_at=datetime(2026, 8, 15, 12, tzinfo=datetime_timezone.utc),
            sent_at=datetime(2026, 9, 15, 12, tzinfo=datetime_timezone.utc),
            received_at=datetime(2026, 10, 15, 12, tzinfo=datetime_timezone.utc),
            transfer_external_id="T-1",
            detail_external_id="D-1",
            source_hash="TRANSFER-MUTEX-T-1-D-1",
            origin_branch={},
        )

        def acquire(values):
            lock_depths.append(len(connection.atomic_blocks))
            result = lock_product_month_sources(values)
            acquired_months.extend(result)
            return result

        with (
            patch(
                "pos_bridge.services.movement_sync_service.lock_product_month_sources",
                side_effect=acquire,
            ),
            patch.object(service, "_upsert_branch", side_effect=RuntimeError("stop after mutex")),
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after mutex"):
                service.persist_transfer_lines(
                    SimpleNamespace(
                        parameters={
                            "start_date": "2026-07-31",
                            "end_date": "2026-09-02",
                        }
                    ),
                    [item],
                )

        self.assertEqual(
            acquired_months,
            [
                date(2026, 7, 1),
                date(2026, 8, 1),
                date(2026, 9, 1),
                date(2026, 10, 1),
            ],
        )
        self.assertEqual(lock_depths, [1])

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


class TransferWriterHistoricalMonthMutexTests(TransactionTestCase):
    AUGUST = date(2026, 8, 1)

    def setUp(self):
        self.origin = PointBranch.objects.create(external_id="ORIGIN", name="Origin")
        self.destination = PointBranch.objects.create(external_id="DEST", name="Destination")

    def _job(self, month: int) -> PointSyncJob:
        return PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            parameters={
                "start_date": f"2026-{month:02d}-01",
                "end_date": f"2026-{month:02d}-28",
            },
        )

    @staticmethod
    def _branch_payload(branch: PointBranch) -> dict:
        return {
            "external_id": branch.external_id,
            "name": branch.name,
            "status": PointBranch.STATUS_ACTIVE,
            "metadata": {},
        }

    def _item(
        self,
        *,
        source_hash: str,
        transfer_external_id: str,
        detail_external_id: str,
        month: int,
        origin: PointBranch | None = None,
        destination: PointBranch | None = None,
    ) -> SimpleNamespace:
        stamp = datetime(2026, month, 15, 18, tzinfo=datetime_timezone.utc)
        return SimpleNamespace(
            origin_branch=self._branch_payload(origin or self.origin),
            destination_branch=self._branch_payload(destination or self.destination),
            transfer_external_id=transfer_external_id,
            detail_external_id=detail_external_id,
            registered_at=stamp,
            sent_at=stamp,
            received_at=stamp,
            requested_by="Operación",
            sent_by="Operación",
            received_by="Operación",
            item_name="Producto de prueba",
            item_code="TEST",
            unit="PZA",
            unit_cost=0,
            requested_quantity=1,
            sent_quantity=1,
            received_quantity=1,
            is_insumo=False,
            is_received=False,
            is_cancelled=True,
            is_finalized=True,
            is_open=False,
            raw_payload={},
            source_hash=source_hash,
        )

    def _existing(
        self,
        *,
        source_hash: str,
        transfer_external_id: str,
        detail_external_id: str,
        month: int = 8,
    ) -> PointTransferLine:
        stamp = datetime(2026, month, 15, 18, tzinfo=datetime_timezone.utc)
        return PointTransferLine.objects.create(
            origin_branch=self.origin,
            destination_branch=self.destination,
            transfer_external_id=transfer_external_id,
            detail_external_id=detail_external_id,
            source_hash=source_hash,
            registered_at=stamp,
            sent_at=stamp,
            received_at=stamp,
            item_name="Producto de prueba",
            is_cancelled=True,
            is_finalized=True,
            is_current_snapshot=True,
        )

    @staticmethod
    def _start_writer(*, job_id, item, ready, finished, backend_pid, results, errors, service=None):
        close_old_connections()
        try:
            connection.ensure_connection()
            backend_pid["pid"] = connection.connection.get_backend_pid()
            ready.set()
            job = PointSyncJob.objects.get(pk=job_id)
            writer = service or PointMovementSyncService()
            results.append(writer.persist_transfer_lines(job, [item]))
            finished.set()
        except BaseException as exc:  # pragma: no cover - surfaced by assertions
            errors.append(exc)
        finally:
            close_old_connections()

    def _wait_until_blocked(self, backend_pid: int) -> None:
        deadline = monotonic() + 3
        while monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks
                        WHERE pid = %s
                          AND NOT granted
                    )
                    """,
                    [backend_pid],
                )
                if cursor.fetchone()[0]:
                    return
            Event().wait(0.01)
        self.fail("El escritor de transferencias no esperó el bloqueo requerido.")

    def _assert_writer_waits_for_august(self, item) -> tuple[dict, list]:
        job = self._job(9)
        ready = Event()
        finished = Event()
        backend_pid = {}
        results = []
        errors = []
        writer = Thread(
            target=self._start_writer,
            kwargs={
                "job_id": job.pk,
                "item": item,
                "ready": ready,
                "finished": finished,
                "backend_pid": backend_pid,
                "results": results,
                "errors": errors,
            },
        )
        try:
            with transaction.atomic():
                lock_product_month_sources([self.AUGUST])
                writer.start()
                self.assertTrue(ready.wait(timeout=3))
                self._wait_until_blocked(backend_pid["pid"])
                self.assertFalse(finished.is_set())
        finally:
            writer.join(timeout=5)
        self.assertFalse(writer.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(finished.is_set())
        return results[0], errors

    def test_update_locks_existing_august_before_moving_received_at_to_september(self):
        existing = self._existing(
            source_hash="MOVE-MONTH",
            transfer_external_id="T-MOVE",
            detail_external_id="D-MOVE",
        )
        incoming = self._item(
            source_hash=existing.source_hash,
            transfer_external_id=existing.transfer_external_id,
            detail_external_id=existing.detail_external_id,
            month=9,
        )

        summary, _errors = self._assert_writer_waits_for_august(incoming)

        existing.refresh_from_db()
        self.assertEqual(existing.received_at.month, 9)
        self.assertEqual(summary["transfer_lines_updated"], 1)

    def test_supersede_locks_august_snapshot_before_marking_it_not_current(self):
        stale = self._existing(
            source_hash="STALE-AUGUST",
            transfer_external_id="T-SNAPSHOT",
            detail_external_id="D-OLD",
        )
        incoming = self._item(
            source_hash="CURRENT-SEPTEMBER",
            transfer_external_id=stale.transfer_external_id,
            detail_external_id="D-NEW",
            month=9,
        )

        summary, _errors = self._assert_writer_waits_for_august(incoming)

        stale.refresh_from_db()
        self.assertFalse(stale.is_current_snapshot)
        self.assertEqual(summary["transfer_details_superseded"], 1)

    def test_concurrent_writers_are_serialized_before_discovering_existing_months(self):
        origin_2 = PointBranch.objects.create(external_id="ORIGIN-2", name="Origin 2")
        destination_2 = PointBranch.objects.create(external_id="DEST-2", name="Destination 2")
        first_item = self._item(
            source_hash="RACING-SOURCE",
            transfer_external_id="T-RACE",
            detail_external_id="D-RACE",
            month=8,
        )
        second_item = self._item(
            source_hash=first_item.source_hash,
            transfer_external_id=first_item.transfer_external_id,
            detail_external_id=first_item.detail_external_id,
            month=9,
            origin=origin_2,
            destination=destination_2,
        )
        first_job = self._job(8)
        second_job = self._job(9)
        first_entered = Event()
        release_first = Event()
        first_ready, second_ready = Event(), Event()
        first_finished, second_finished = Event(), Event()
        first_pid, second_pid = {}, {}
        results, errors = [], []
        first_service = PointMovementSyncService()
        original_upsert = first_service._upsert_branch

        def pause_first_writer(payload):
            branch = original_upsert(payload)
            if not first_entered.is_set():
                first_entered.set()
                if not release_first.wait(timeout=5):
                    raise AssertionError("No se liberó el primer escritor.")
            return branch

        first_service._upsert_branch = pause_first_writer
        first_writer = Thread(
            target=self._start_writer,
            kwargs={
                "job_id": first_job.pk,
                "item": first_item,
                "ready": first_ready,
                "finished": first_finished,
                "backend_pid": first_pid,
                "results": results,
                "errors": errors,
                "service": first_service,
            },
        )
        second_writer = Thread(
            target=self._start_writer,
            kwargs={
                "job_id": second_job.pk,
                "item": second_item,
                "ready": second_ready,
                "finished": second_finished,
                "backend_pid": second_pid,
                "results": results,
                "errors": errors,
            },
        )

        first_writer.start()
        self.assertTrue(first_ready.wait(timeout=3))
        self.assertTrue(first_entered.wait(timeout=3))
        second_writer.start()
        self.assertTrue(second_ready.wait(timeout=3))
        try:
            self._wait_until_blocked(second_pid["pid"])
            self.assertFalse(second_finished.is_set())
        finally:
            release_first.set()
            first_writer.join(timeout=5)
            second_writer.join(timeout=5)

        self.assertFalse(first_writer.is_alive())
        self.assertFalse(second_writer.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(first_finished.is_set())
        self.assertTrue(second_finished.is_set())
        final = PointTransferLine.objects.get(source_hash=first_item.source_hash)
        self.assertEqual(final.received_at.month, 9)
