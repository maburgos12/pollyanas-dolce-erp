from datetime import date, datetime, time, timedelta
from io import StringIO
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test import TestCase, override_settings

from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.models.snapshot import PointInventoryPayload
from pos_bridge.services.inventory_payload_store import store_exact_inventory_payloads


@override_settings(POINT_INVENTORY_SHARE_PAYLOADS=True)
class CompactInventoryEvidenceTests(TestCase):
    day = date(2025, 1, 15)

    def setUp(self):
        self.branch = PointBranch.objects.create(external_id="MOCK-01", name="Guamuchil")
        self.product = PointProduct.objects.create(external_id="MOCK-01", name="Pastel")
        self.job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY)

    def capture(self, hour, value, day=None):
        return PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=self.product,
            sync_job=self.job,
            captured_at=datetime.combine(day or self.day, time(hour), ZoneInfo("America/Mazatlan")),
            stock=value,
            raw_payload={"row": [value]},
        )

    def compact(self, **kwargs):
        output = StringIO()
        call_command("compact_point_inventory_evidence", self.day.isoformat(), stdout=output, **kwargs)
        return output.getvalue()

    def test_dry_run_batches_and_retries_preserve_exact_cuts(self):
        opening = self.capture(8, 5)
        repeated = self.capture(12, 5)
        closing = self.capture(21, 2)
        neighbor = self.capture(8, 5, self.day + timedelta(days=1))
        self.assertIn("updated=0", self.compact())
        self.assertFalse(PointInventoryPayload.objects.exists())
        self.assertIn("updated=2", self.compact(apply=True, batch_size=2))
        self.assertIn("updated=1", self.compact(apply=True, after_id=repeated.id))
        self.assertIn("updated=0", self.compact(apply=True))
        for persisted in (opening, repeated, closing):
            source = PointInventorySnapshot.objects.get(pk=persisted.pk)
            self.assertEqual(source.raw_payload, {})
            self.assertEqual(source.effective_raw_payload, persisted.raw_payload)
            self.assertEqual(source.captured_at, persisted.captured_at)
            self.assertEqual(source.stock, persisted.stock)
            self.assertEqual(source.sync_job_id, persisted.sync_job_id)
        opening.refresh_from_db()
        repeated.refresh_from_db()
        neighbor.refresh_from_db()
        self.assertEqual(opening.shared_payload_id, repeated.shared_payload_id)
        self.assertIsNone(neighbor.shared_payload_id)
        self.assertEqual(PointInventoryPayload.objects.count(), 2)

    def test_mismatch_rolls_back_the_whole_batch(self):
        correct, incorrect = self.capture(8, 5), self.capture(9, 2)

        def mismatched(documents):
            payload = PointInventoryPayload.objects.create(digest="0" * 64, payload={"row": [5]})
            return [payload.pk] * len(documents)

        with patch(
            "pos_bridge.management.commands.compact_point_inventory_evidence.store_exact_inventory_payloads",
            side_effect=mismatched,
        ), self.assertRaisesMessage(CommandError, "no coincide"):
            self.compact(apply=True)

        for row in (correct, incorrect):
            original = PointInventorySnapshot.objects.get(pk=row.pk)
            self.assertEqual(original.raw_payload, row.raw_payload)
            self.assertIsNone(original.shared_payload_id)
        self.assertFalse(PointInventoryPayload.objects.exists())

    def test_history_and_new_evidence_share_the_same_document_without_numeric_rounding(self):
        row = self.capture(8, 5)
        document = '{"n": 0.123456789012345678912345}'
        [shared_id] = store_exact_inventory_payloads([document])
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE pos_bridge_inventory_snapshots SET raw_payload=%s::jsonb WHERE id=%s",
                [document, row.pk],
            )
        self.assertIn("updated=1", self.compact(apply=True))
        row.refresh_from_db()
        self.assertEqual(row.shared_payload_id, shared_id)
        with connection.cursor() as cursor:
            cursor.execute("SELECT payload::text FROM pos_bridge_inventory_payloads WHERE id=%s", [shared_id])
            self.assertEqual(cursor.fetchone()[0], document)
