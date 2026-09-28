from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import TestCase
from django.utils import timezone

from pos_bridge.models import PointSyncJob
from pos_bridge.services.open_transfer_sync_service import (
    OPEN_TRANSFER_MANIFEST_KEY,
    OpenTransferSyncService,
    build_open_transfer_manifest,
)
from pos_bridge.services.transfer_extractor import ExtractedTransferLine


class OpenTransferSyncServiceTests(TestCase):
    def _line(self, *, transfer_id, detail_id, quantity):
        local_tz = timezone.get_current_timezone()
        return ExtractedTransferLine(
            origin_branch={"external_id": "CEDIS", "name": "CEDIS"},
            destination_branch={"external_id": "PLAZA", "name": "Plaza"},
            transfer_external_id=transfer_id,
            detail_external_id=detail_id,
            registered_at=datetime(2026, 8, 31, 20, 0, tzinfo=local_tz),
            sent_at=datetime(2026, 8, 31, 21, 0, tzinfo=local_tz),
            received_at=None,
            requested_by="solicita",
            sent_by="envia",
            received_by="",
            item_name="Pastel",
            item_code="PASTEL-001",
            unit="PZA",
            unit_cost=Decimal("10.00"),
            requested_quantity=Decimal(quantity),
            sent_quantity=Decimal(quantity),
            received_quantity=Decimal("0"),
            is_insumo=False,
            is_received=False,
            is_cancelled=False,
            is_finalized=True,
            is_open=True,
            source_hash=f"hash-{transfer_id}-{detail_id}",
        )

    def test_manifest_hash_is_deterministic_and_order_independent(self):
        captured_at = datetime(
            2026, 9, 1, 2, 0, tzinfo=timezone.get_current_timezone()
        )
        first = self._line(transfer_id="T-2", detail_id="D-2", quantity="2.00")
        second = self._line(transfer_id="T-1", detail_id="D-1", quantity="4")

        forward = build_open_transfer_manifest(
            [first, second],
            operational_date=date(2026, 8, 31),
            captured_at=captured_at,
        )
        reverse = build_open_transfer_manifest(
            [second, first],
            operational_date=date(2026, 8, 31),
            captured_at=captured_at,
        )

        self.assertEqual(forward, reverse)
        self.assertEqual(forward["row_count"], 2)
        self.assertEqual(len(forward["sha256"]), 64)
        self.assertEqual(forward["operational_date"], "2026-08-31")
        self.assertEqual(forward["captured_at"], captured_at.isoformat())

    def test_unrestricted_success_persists_manifest_in_result_summary(self):
        lines = [self._line(transfer_id="T-1", detail_id="D-1", quantity="4")]
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_RUNNING,
        )
        movement_service = Mock()
        movement_service.create_job.return_value = job
        movement_service.transfer_extractor.extract_open.return_value = lines
        movement_service.persist_transfer_lines.return_value = {
            "transfer_lines_seen": 1,
            "transfer_lines_created": 1,
            "transfer_lines_updated": 0,
        }
        movement_service._mark_success.side_effect = lambda sync_job, summary: SimpleNamespace(
            id=sync_job.id,
            result_summary=summary,
        )
        service = OpenTransferSyncService(movement_service=movement_service)

        with patch.object(service, "_requesting_branch_ids", return_value=set()):
            result = service.sync_open_transfers(fecha=date(2026, 8, 31))

        manifest = result.result_summary[OPEN_TRANSFER_MANIFEST_KEY]
        self.assertEqual(manifest["row_count"], 1)
        self.assertEqual(manifest["operational_date"], "2026-08-31")
        self.assertRegex(manifest["sha256"], r"^[0-9a-f]{64}$")
        self.assertTrue(manifest["captured_at"])

    def test_restricted_success_does_not_publish_authoritative_manifest(self):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_RUNNING,
        )
        movement_service = Mock()
        movement_service.create_job.return_value = job
        movement_service.transfer_extractor.extract_open.return_value = []
        movement_service.persist_transfer_lines.return_value = {
            "transfer_lines_seen": 0,
            "transfer_lines_created": 0,
            "transfer_lines_updated": 0,
        }
        movement_service._mark_success.side_effect = lambda sync_job, summary: SimpleNamespace(
            id=sync_job.id,
            result_summary=summary,
        )
        service = OpenTransferSyncService(movement_service=movement_service)

        with patch.object(service, "_requesting_branch_ids", return_value=set()):
            result = service.sync_open_transfers(
                fecha=date(2026, 8, 31), branch_filter="PLAZA"
            )

        self.assertNotIn(OPEN_TRANSFER_MANIFEST_KEY, result.result_summary)
