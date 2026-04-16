from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from types import SimpleNamespace

from pos_bridge.models import PointBranch, PointExtractionLog, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.inventory_extractor import PointInventoryExtractor
from pos_bridge.services.inventory_extractor import ExtractedBranchInventory
from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureResult
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.utils.exceptions import ExtractionError


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


class FakeInventoryCostCaptureService:
    def __init__(self, *, should_fail: bool = False):
        self.calls = []
        self.should_fail = should_fail

    def capture_and_persist_all(self, *, branch_hint="ALMACEN", supplier_name="POINT EXISTENCIA ALMACEN", sample_limit=12):
        self.calls.append(
            {
                "branch_hint": branch_hint,
                "supplier_name": supplier_name,
                "sample_limit": sample_limit,
            }
        )
        if self.should_fail:
            raise ExtractionError("No se pudo capturar costos desde Point/Existencias.")
        return PointInventoryCostCaptureResult(
            branch_name=branch_hint,
            rows_seen=25,
            matches_found=10,
            costs_created=3,
            costs_existing=5,
            unresolved_matches=1,
            zero_cost_matches=1,
            unresolved_samples=[{"point_code": "ABC", "point_name": "Insumo sin match", "category": "Insumos"}],
            zero_cost_samples=[{"point_code": "XYZ", "point_name": "Insumo costo cero", "category": "Empaque"}],
        )


class PointSyncServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_pos_bridge",
            email="admin_pos_bridge@example.com",
            password="test12345",
        )

    def test_run_inventory_sync_persists_entities(self):
        cost_capture = FakeInventoryCostCaptureService()
        service = PointSyncService(extractor=FakeExtractor(), inventory_cost_capture_service=cost_capture)
        sync_job = service.run_inventory_sync(triggered_by=self.user)

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(PointBranch.objects.count(), 1)
        self.assertEqual(PointProduct.objects.count(), 1)
        self.assertEqual(PointInventorySnapshot.objects.count(), 1)
        self.assertGreaterEqual(PointExtractionLog.objects.count(), 2)
        self.assertEqual(sync_job.result_summary["inventory_cost_status"], "SUCCESS")
        self.assertEqual(sync_job.result_summary["inventory_cost_costs_created"], 3)
        self.assertEqual(sync_job.result_summary["inventory_cost_unresolved_matches"], 1)
        self.assertEqual(cost_capture.calls[0]["branch_hint"], service.settings.inventory_cost_capture_branch)

    def test_run_inventory_sync_marks_failure_when_no_data(self):
        service = PointSyncService(extractor=FakeExtractor(), inventory_cost_capture_service=FakeInventoryCostCaptureService())
        sync_job = service.run_inventory_sync(triggered_by=self.user, branch_filter="empty")

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_FAILED)
        self.assertIn("no devolvió", sync_job.error_message.lower())

    def test_run_inventory_sync_skips_cost_capture_for_filtered_branch(self):
        cost_capture = FakeInventoryCostCaptureService()
        service = PointSyncService(extractor=FakeExtractor(), inventory_cost_capture_service=cost_capture)

        sync_job = service.run_inventory_sync(triggered_by=self.user, branch_filter="Centro")

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(sync_job.result_summary["inventory_cost_status"], "SKIPPED")
        self.assertEqual(cost_capture.calls, [])

    def test_run_inventory_sync_marks_partial_when_cost_capture_fails(self):
        service = PointSyncService(
            extractor=FakeExtractor(),
            inventory_cost_capture_service=FakeInventoryCostCaptureService(should_fail=True),
        )

        sync_job = service.run_inventory_sync(triggered_by=self.user)

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_PARTIAL)
        self.assertEqual(sync_job.result_summary["inventory_cost_status"], "FAILED")
        self.assertIn("costos unitarios", sync_job.error_message.lower())

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
