from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from pos_bridge.tasks.celery_tasks import (
    task_open_transfer_closing_snapshot,
    task_open_transfer_sync,
)


class OpenTransferTaskTests(SimpleTestCase):
    @patch("pos_bridge.tasks.celery_tasks.OpenTransferSyncService")
    @patch("pos_bridge.tasks.celery_tasks.timezone.localdate", return_value=date(2026, 9, 1))
    def test_closing_snapshot_uses_prior_local_operational_date(
        self,
        _localdate,
        service_class,
    ):
        service_class.return_value.sync_open_transfers.return_value = SimpleNamespace(
            id=321,
            status="SUCCESS",
            result_summary={"open_transfer_manifest": {}},
            error_message="",
        )

        result = task_open_transfer_closing_snapshot()

        service_class.return_value.sync_open_transfers.assert_called_once_with(
            fecha=date(2026, 8, 31),
            branch_filter=None,
            triggered_by=None,
        )
        self.assertEqual(result["job_id"], 321)

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
