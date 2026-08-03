from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pos_bridge.models import PointBranch, PointExtractionLog, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.inventory_extractor import PointInventoryExtractor
from pos_bridge.services.inventory_extractor import ExtractedBranchInventory
from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureResult
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.utils.exceptions import ExtractionError, NavigationError


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

    def test_full_inventory_excludes_non_commercial_point_locations(self):
        extractor = PointInventoryExtractor(
            bridge_settings=SimpleNamespace(
                sales_excluded_branches=["CEDIS", "ALMACEN", "PRODUCCION CRUCERO", "DEVOLUCIONES"]
            )
        )
        branches = [
            {"value": "1", "label": "MATRIZ"},
            {"value": "8", "label": "CEDIS"},
            {"value": "9", "label": "ALMACEN"},
            {"value": "10", "label": "PRODUCCION CRUCERO"},
            {"value": "12", "label": "DEVOLUCIONES"},
        ]

        filtered = extractor._exclude_non_commercial_branches(branches)

        self.assertEqual(filtered, [{"value": "1", "label": "MATRIZ"}])

    def test_retry_failed_jobs_only_claims_inventory_once(self):
        inventory_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_FAILED,
            attempt_count=1,
            parameters={"branch_filter": "Centro"},
            triggered_by=self.user,
        )
        PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_FAILED,
            attempt_count=1,
            parameters={},
            triggered_by=self.user,
        )
        service = PointSyncService(
            extractor=FakeExtractor(),
            inventory_cost_capture_service=FakeInventoryCostCaptureService(),
        )

        with patch.object(service, "run_inventory_sync") as run_inventory_sync:
            run_inventory_sync.return_value = SimpleNamespace(id=999)
            retried = service.retry_failed_jobs(limit=5, max_attempts=3)
            second_pass = service.retry_failed_jobs(limit=5, max_attempts=3)

        self.assertEqual(retried, [run_inventory_sync.return_value])
        self.assertEqual(second_pass, [])
        run_inventory_sync.assert_called_once_with(
            triggered_by=self.user,
            branch_filter="Centro",
            limit_branches=None,
            attempt_count=2,
        )
        inventory_job.refresh_from_db()
        self.assertTrue(inventory_job.parameters["retry_scheduled"])
        self.assertIn("retry_scheduled_at", inventory_job.parameters)

    def test_unfiltered_inventory_uses_matriz_workspace_context(self):
        extractor = PointInventoryExtractor(
            bridge_settings=SimpleNamespace(timeout_ms=30000)
        )
        extractor.auth_service = Mock()
        session = SimpleNamespace(page=Mock())

        with (
            patch("pos_bridge.services.inventory_extractor.PlaywrightBrowserClient"),
            patch("pos_bridge.services.inventory_extractor.BrowserSessionManager") as manager,
            patch("pos_bridge.services.inventory_extractor.PointInventoryPage") as inventory_page,
        ):
            manager.return_value.__enter__.return_value = session
            inventory_page.return_value.open_inventory_module.side_effect = NavigationError("detener prueba")

            with self.assertRaises(NavigationError):
                extractor.extract()

        extractor.auth_service.login.assert_called_once_with(session, branch_hint="MATRIZ")

    def test_unfiltered_inventory_renews_browser_session_for_each_branch(self):
        extractor = PointInventoryExtractor(
            bridge_settings=SimpleNamespace(timeout_ms=30000)
        )
        extractor.auth_service = Mock()
        first_session = SimpleNamespace(page=Mock())
        second_session = SimpleNamespace(page=Mock())
        branches = [
            {"value": "1", "label": "MATRIZ"},
            {"value": "13", "label": "GUAMUCHIL"},
        ]
        first_result = Mock()
        second_result = Mock()

        with (
            patch("pos_bridge.services.inventory_extractor.PlaywrightBrowserClient"),
            patch("pos_bridge.services.inventory_extractor.BrowserSessionManager") as manager,
            patch("pos_bridge.services.inventory_extractor.PointInventoryPage") as inventory_page,
            patch.object(extractor, "_extract_branch", side_effect=[first_result, second_result]) as extract_branch,
        ):
            manager.return_value.__enter__.side_effect = [first_session, second_session]
            first_page = Mock()
            first_page.list_branches.return_value = branches
            second_page = Mock()
            inventory_page.side_effect = [first_page, second_page]

            result = extractor.extract()

        self.assertEqual(result, [first_result, second_result])
        self.assertEqual(manager.call_count, 2)
        self.assertEqual(
            extractor.auth_service.login.call_args_list,
            [
                ((first_session,), {"branch_hint": "MATRIZ"}),
                ((second_session,), {"branch_hint": "GUAMUCHIL"}),
            ],
        )
        self.assertEqual(extract_branch.call_count, 2)

    def test_extract_branch_rejects_empty_product_rows(self):
        extractor = PointInventoryExtractor(
            bridge_settings=SimpleNamespace(timeout_ms=30000)
        )
        inventory_page = Mock()
        inventory_page.select_branch.return_value = {"value": "13", "label": "GUAMUCHIL"}
        inventory_page.extract_inventory_table.return_value = {"headers": [], "rows": []}
        extractor._extract_product_rows_by_category = Mock(return_value=([], []))

        with self.assertRaisesMessage(ExtractionError, "cero productos"):
            extractor._extract_branch(inventory_page, {"value": "13", "label": "GUAMUCHIL"})
