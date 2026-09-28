from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from celery.exceptions import Retry
from django.test import SimpleTestCase

from pos_bridge.tasks.celery_tasks import (
    task_open_transfer_closing_snapshot,
    task_open_transfer_sync,
)


class OpenTransferTaskTests(SimpleTestCase):
    @staticmethod
    def _job(*, status="SUCCESS", error_message=""):
        return SimpleNamespace(
            id=321,
            status=status,
            result_summary={"open_transfer_manifest": {}},
            error_message=error_message,
        )

    @patch("pos_bridge.tasks.celery_tasks.point_account_session_lock")
    @patch("pos_bridge.tasks.celery_tasks.OpenTransferSyncService")
    @patch("pos_bridge.tasks.celery_tasks.timezone.localdate", return_value=date(2026, 9, 1))
    def test_closing_snapshot_uses_prior_local_operational_date(
        self,
        _localdate,
        service_class,
        session_lock,
    ):
        session_lock.return_value.__enter__.return_value = True
        service_class.return_value.sync_open_transfers.return_value = self._job()

        result = task_open_transfer_closing_snapshot()

        session_lock.assert_called_once_with(wait=True)
        service_class.return_value.sync_open_transfers.assert_called_once_with(
            fecha=date(2026, 8, 31),
            branch_filter=None,
            triggered_by=None,
        )
        self.assertEqual(result["job_id"], 321)

    @patch("pos_bridge.tasks.celery_tasks.OpenTransferSyncService")
    @patch("pos_bridge.tasks.celery_tasks.point_account_session_lock")
    def test_lock_contention_retries_without_calling_point(self, session_lock, service_class):
        session_lock.return_value.__enter__.return_value = False

        with patch.object(
            task_open_transfer_closing_snapshot,
            "retry",
            side_effect=Retry(),
        ) as retry:
            with self.assertRaises(Retry):
                task_open_transfer_closing_snapshot()

        retry.assert_called_once()
        service_class.return_value.sync_open_transfers.assert_not_called()

    @patch("pos_bridge.tasks.celery_tasks.OpenTransferSyncService")
    @patch("pos_bridge.tasks.celery_tasks.point_account_session_lock")
    def test_failed_job_retries(self, session_lock, service_class):
        session_lock.return_value.__enter__.return_value = True
        service_class.return_value.sync_open_transfers.return_value = self._job(
            status="FAILED",
            error_message="Point no respondió",
        )

        with patch.object(
            task_open_transfer_closing_snapshot,
            "retry",
            side_effect=Retry(),
        ) as retry:
            with self.assertRaises(Retry):
                task_open_transfer_closing_snapshot()

        retry.assert_called_once()
        self.assertIn("Point no respondió", str(retry.call_args.kwargs["exc"]))

    @patch("pos_bridge.tasks.celery_tasks.OpenTransferSyncService")
    @patch("pos_bridge.tasks.celery_tasks.point_account_session_lock")
    def test_success_serializes_without_retry(self, session_lock, service_class):
        session_lock.return_value.__enter__.return_value = True
        service_class.return_value.sync_open_transfers.return_value = self._job()

        with patch.object(task_open_transfer_closing_snapshot, "retry") as retry:
            result = task_open_transfer_closing_snapshot()

        retry.assert_not_called()
        self.assertEqual(result["status"], "SUCCESS")

    @patch("pos_bridge.tasks.celery_tasks.OpenTransferSyncService")
    @patch("pos_bridge.tasks.celery_tasks.point_account_session_lock")
    def test_exhausted_retries_preserve_final_failed_job(self, session_lock, service_class):
        session_lock.return_value.__enter__.return_value = True
        service_class.return_value.sync_open_transfers.return_value = self._job(
            status="FAILED",
            error_message="Point siguió sin responder",
        )

        task_open_transfer_closing_snapshot.push_request(
            retries=task_open_transfer_closing_snapshot.max_retries
        )
        try:
            with patch.object(task_open_transfer_closing_snapshot, "retry") as retry:
                result = task_open_transfer_closing_snapshot.run()
        finally:
            task_open_transfer_closing_snapshot.pop_request()

        retry.assert_not_called()
        self.assertEqual(result["job_id"], 321)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["error_message"], "Point siguió sin responder")

    def test_closing_snapshot_preserves_open_sync_retry_contract(self):
        self.assertEqual(
            task_open_transfer_closing_snapshot.max_retries,
            task_open_transfer_sync.max_retries,
        )
        self.assertEqual(
            task_open_transfer_closing_snapshot.default_retry_delay,
            task_open_transfer_sync.default_retry_delay,
        )
        self.assertEqual(
            task_open_transfer_closing_snapshot.time_limit,
            task_open_transfer_sync.time_limit,
        )
