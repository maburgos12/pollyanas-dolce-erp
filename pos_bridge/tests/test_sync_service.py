from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from types import SimpleNamespace

from pos_bridge.models import PointBranch, PointExtractionLog, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.inventory_extractor import PointInventoryExtractor
from pos_bridge.services.inventory_extractor import ExtractedBranchInventory
from pos_bridge.services.sync_service import PointSyncService


class FakeExtractor:
    def extract(self, *, branch_filter=None, limit_branches=None):
        rows = [
            {
                "external_id": "SKU-001",
                "sku": "SKU-001",
                "name": "Pastel Chocolate",
                "category": "Pasteles",
                "stock": "10",
                "min_stock": "2",
                "max_stock": "20",
                "raw_payload": {"row": ["SKU-001", "Pastel Chocolate", "10", "2", "20"]},
            }
        ]
        if branch_filter == "empty":
            return []
        return [
            ExtractedBranchInventory(
                branch={"external_id": "SUC-01", "name": "Centro", "status": "ACTIVE", "metadata": {}},
                inventory_rows=rows,
                captured_at=timezone.now(),
                raw_export_path="/tmp/point.json",
            )
        ]


class PointSyncServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_pos_bridge",
            email="admin_pos_bridge@example.com",
            password="test12345",
        )

    def test_run_inventory_sync_persists_entities(self):
        service = PointSyncService(extractor=FakeExtractor())
        sync_job = service.run_inventory_sync(triggered_by=self.user)

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(PointBranch.objects.count(), 1)
        self.assertEqual(PointProduct.objects.count(), 1)
        self.assertEqual(PointInventorySnapshot.objects.count(), 1)
        self.assertGreaterEqual(PointExtractionLog.objects.count(), 2)

    def test_run_inventory_sync_marks_failure_when_no_data(self):
        service = PointSyncService(extractor=FakeExtractor())
        sync_job = service.run_inventory_sync(triggered_by=self.user, branch_filter="empty")

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_FAILED)
        self.assertIn("no devolvió", sync_job.error_message.lower())

    def test_branch_filter_prefers_exact_match_before_partial(self):
        extractor = PointInventoryExtractor(
            bridge_settings=SimpleNamespace(
                base_url="https://app.pointmeup.com",
                username="demo",
                password="demo",
                timeout_ms=30000,
                raw_exports_dir="/tmp",
            )
        )
        branches = [
            {"value": "2", "label": "Crucero"},
            {"value": "10", "label": "Produccion Crucero"},
        ]

        filtered = extractor._apply_branch_filter(branches, "Crucero")

        self.assertEqual(filtered, [{"value": "2", "label": "Crucero"}])
