from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
from contextlib import contextmanager
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from pos_bridge.browser.inventory_page import PointInventoryPage
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

    def test_inventory_source_sessions_hold_account_lock(self):
        from pos_bridge.services.point_account_session_lock import POINT_ACCOUNT_SESSION_LOCK_ID
        from django.db import connection

        def assert_locked(*args, **kwargs):
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' "
                    "AND pid=pg_backend_pid() AND classid=%s AND objid=%s AND granted)",
                    [POINT_ACCOUNT_SESSION_LOCK_ID >> 32, POINT_ACCOUNT_SESSION_LOCK_ID & 0xffffffff],
                )
                self.assertTrue(cursor.fetchone()[0], 'Inventory must protect the shared Point session')

        extractor = FakeExtractor()
        original_extract = extractor.extract
        def extract(**kwargs):
            assert_locked()
            return original_extract(**kwargs)
        extractor.extract = extract
        costs = FakeInventoryCostCaptureService()
        original_costs = costs.capture_and_persist_all
        def capture(**kwargs):
            assert_locked()
            return original_costs(**kwargs)
        costs.capture_and_persist_all = capture
        job = PointSyncService(extractor=extractor, inventory_cost_capture_service=costs).run_inventory_sync()
        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS, job.error_message)
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=%s AND objid=%s", [POINT_ACCOUNT_SESSION_LOCK_ID >> 32, POINT_ACCOUNT_SESSION_LOCK_ID & 0xffffffff])
            self.assertEqual(cursor.fetchone()[0], 0)

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

    def test_inventory_omission_reports_products_and_preserves_the_visible_cut(self):
        from django.urls import reverse

        [centro] = FakeExtractor().extract()
        product = centro.inventory_rows[0]
        missing = {**product, "external_id": "SKU-002", "sku": "SKU-002", "name": "Vela Alegría"}
        centro.inventory_rows.append(missing)
        norte = ExtractedBranchInventory(
            branch={**centro.branch, "external_id": "SUC-02", "name": "Norte"},
            inventory_rows=list(centro.inventory_rows),
            captured_at=centro.captured_at,
            raw_export_path=centro.raw_export_path,
        )
        extractor = Mock()
        extractor.extract.return_value = [centro, norte]
        service = PointSyncService(extractor=extractor)
        complete = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(complete.status, PointSyncJob.STATUS_SUCCESS)

        self.client.force_login(self.user)
        url = reverse("pos_bridge_api:pos_bridge_inventory-current")
        before = self.client.get(url)
        self.assertEqual(before.status_code, 200)
        self.assertEqual(before.json()["count"], 2)

        changed_at = timezone.now()
        centro.captured_at = norte.captured_at = changed_at
        centro.inventory_rows = [{**product, "stock": "0"}, missing]
        norte.inventory_rows = [{**product, "stock": "-1"}]
        job = service.run_inventory_sync(capture_costs=False)

        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        self.assertTrue(job.result_summary["retryable"])
        self.assertEqual(job.artifacts["missing_inventory_products"], [{
            "branch_external_id": "SUC-02",
            "branch_name": "Norte",
            "reference_job_id": complete.id,
            "products": [{"product_external_id": "SKU-002", "sku": "SKU-002", "name": "Vela Alegría"}],
        }])
        self.assertEqual(
            job.logs.get(level=PointExtractionLog.LEVEL_ERROR).context["missing_inventory_products"],
            job.artifacts["missing_inventory_products"],
        )
        self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=job).exists())
        self.assertEqual(self.client.get(url).json(), before.json())

        norte.inventory_rows.append(missing)
        recovered = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(recovered.status, PointSyncJob.STATUS_SUCCESS)
        self.assertCountEqual(
            PointInventorySnapshot.objects.filter(sync_job=recovered, product__external_id="SKU-001")
            .values_list("stock", flat=True),
            [0, -1],
        )

        extractor.extract.return_value = [centro]
        missing_branch = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(missing_branch.status, PointSyncJob.STATUS_FAILED)
        self.assertEqual(missing_branch.artifacts["missing_inventory_products"][0]["branch_external_id"], "SUC-02")
        self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=missing_branch).exists())

        with patch.object(service, "_upsert_branch", side_effect=[PointBranch.objects.get(external_id="SUC-01"), RuntimeError("Fallo sintético")]):
            extractor.extract.return_value = [centro, norte]
            interrupted = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(interrupted.status, PointSyncJob.STATUS_FAILED)
        self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=interrupted).exists())

        PointProduct.objects.filter(external_id="SKU-002").update(active=False)
        extractor.extract.return_value = [centro]
        centro.inventory_rows = [{**product, "stock": "0"}]
        scoped = service.run_inventory_sync(branch_filter="Centro", capture_costs=False)
        self.assertEqual(scoped.status, PointSyncJob.STATUS_SUCCESS, scoped.error_message)

    def test_inventory_rejects_same_product_id_with_a_different_sku(self):
        [result] = FakeExtractor().extract()
        row = result.inventory_rows[0]
        extractor = Mock()
        extractor.extract.return_value = [result]
        service = PointSyncService(extractor=extractor)
        previous = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(previous.status, PointSyncJob.STATUS_SUCCESS)

        result.inventory_rows = [{**row, "sku": "SKU-INCORRECTO"}]
        job = service.run_inventory_sync(capture_costs=False)

        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        [branch] = job.artifacts["missing_inventory_products"]
        self.assertEqual(branch["products"][0]["sku"], row["sku"])
        self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=job).exists())
        self.assertEqual(PointProduct.objects.get(external_id=row["external_id"]).sku, row["sku"])

    def test_filtered_inventory_rejects_wrong_branch_id_even_with_the_requested_name(self):
        [result] = FakeExtractor().extract()
        extractor = Mock()
        extractor.extract.return_value = [result]
        service = PointSyncService(extractor=extractor)
        complete = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(complete.status, PointSyncJob.STATUS_SUCCESS)

        result.branch = {**result.branch, "external_id": "SUC-02"}
        job = service.run_inventory_sync(branch_filter="Centro", capture_costs=False)

        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        self.assertEqual(job.artifacts["requested_inventory_branch_ids"], ["SUC-01"])
        self.assertEqual(job.artifacts["observed_inventory_branch_ids"], ["SUC-02"])
        self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=job).exists())
        self.assertFalse(PointBranch.objects.filter(external_id="SUC-02").exists())
        self.assertEqual(PointInventorySnapshot.objects.get().sync_job_id, complete.pk)

    def test_run_inventory_sync_skips_cost_capture_for_filtered_branch(self):
        cost_capture = FakeInventoryCostCaptureService()
        service = PointSyncService(extractor=FakeExtractor(), inventory_cost_capture_service=cost_capture)

        sync_job = service.run_inventory_sync(triggered_by=self.user, branch_filter="Centro")

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(sync_job.result_summary["inventory_cost_status"], "SKIPPED")
        self.assertEqual(cost_capture.calls, [])

    def test_inventory_starts_from_certified_cut_then_advances_to_validated_publications(self):
        from datetime import timedelta
        from django.utils.dateparse import parse_datetime

        [result] = FakeExtractor().extract()
        product = result.inventory_rows[0]
        missing = {**product, "external_id": "SKU-002", "sku": "SKU-002", "name": "Producto activo"}
        candles = ("CND000943", "CND000945", "CND000949", "CND000978", "CND000979", "CND001092", "CND001093")
        branch = PointBranch.objects.create(external_id="SUC-01", name="Centro")
        original = PointSyncJob.objects.create(
            pk=147041, status=PointSyncJob.STATUS_SUCCESS,
            started_at=parse_datetime("2026-11-16T04:05:18.676533Z"),
        )
        legacy = PointSyncJob.objects.create(status=PointSyncJob.STATUS_SUCCESS)
        for sku, job in [
            ("SKU-001", original), ("SKU-002", original), ("SKU-001", legacy),
            *((sku, legacy) for sku in candles),
        ]:
            item, _ = PointProduct.objects.get_or_create(external_id=sku, defaults={"sku": sku, "name": sku})
            PointInventorySnapshot.objects.create(
                branch=branch, product=item, stock=1, sync_job=job,
                captured_at=result.captured_at - timedelta(hours=2 if job == original else 1),
            )

        extractor = Mock()
        extractor.extract.return_value = [result]
        service = PointSyncService(extractor=extractor)
        for branch_filter in (None, "Centro"):
            job = service.run_inventory_sync(branch_filter=branch_filter, capture_costs=False)
            self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
            [warning] = job.artifacts["missing_inventory_products"]
            self.assertEqual(warning["branch_external_id"], "SUC-01")
            self.assertEqual(warning["reference_job_id"], original.pk)
            self.assertEqual([item["sku"] for item in warning["products"]], ["SKU-002"])
            self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=job).exists())

        added = {**product, "external_id": "SKU-003", "sku": "SKU-003", "name": "Nuevo producto"}
        result.inventory_rows.extend([missing, added])
        validated = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(validated.status, PointSyncJob.STATUS_SUCCESS, validated.error_message)

        result.inventory_rows.remove(added)
        result.captured_at = timezone.now()
        rejected = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(rejected.status, PointSyncJob.STATUS_FAILED)
        [warning] = rejected.artifacts["missing_inventory_products"]
        self.assertEqual(warning["reference_job_id"], validated.pk)
        self.assertEqual([item["sku"] for item in warning["products"]], ["SKU-003"])

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
            parameters={"branch_filter": "Centro", "capture_costs": False},
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
            capture_costs=False,
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

    def test_unconfirmed_point_selection_does_not_persist_inventory(self):
        service = PointSyncService(extractor=FakeExtractor())
        complete = service.run_inventory_sync(capture_costs=False)
        self.assertEqual(complete.status, PointSyncJob.STATUS_SUCCESS)

        settings = SimpleNamespace(selector_overrides={}, timeout_ms=30000)
        page = PointInventoryPage(Mock(), settings)
        extractor = PointInventoryExtractor(bridge_settings=settings)
        target = {"value": "SUC-01", "label": "Centro"}
        locator = Mock()
        for selector, selected, message in (
            (None, None, "No se encontró selector"),
            (locator, None, "Point no confirmó la sucursal"),
            (locator, {"value": "SUC-02", "label": "Centro"}, "Point no confirmó la sucursal"),
        ):
            with self.subTest(message=message, selected=selected):
                locator.evaluate.return_value = selected
                with (
                    patch("pos_bridge.browser.inventory_page.find_first", return_value=selector),
                    patch.object(page, "extract_inventory_table") as extract_table,
                    patch.object(service.extractor, "extract", side_effect=lambda **_: [extractor._extract_branch(page, target)]),
                ):
                    job = service.run_inventory_sync(branch_filter="Centro", capture_costs=False)

                self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
                self.assertIn(message, job.error_message)
                extract_table.assert_not_called()
                self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=job).exists())
                self.assertEqual(PointInventorySnapshot.objects.get().sync_job_id, complete.pk)

    def test_extract_branch_rejects_empty_product_rows(self):
        extractor = PointInventoryExtractor(
            bridge_settings=SimpleNamespace(timeout_ms=30000)
        )
        inventory_page = Mock()
        inventory_page.select_branch.return_value = {"value": "13", "label": "GUAMUCHIL"}
        inventory_page.extract_inventory_table.return_value = {"headers": [], "rows": []}
        extractor._extract_product_rows_by_category = Mock(return_value=([], [], {"headers": [], "rows": []}))

        with self.assertRaisesMessage(ExtractionError, "cero productos"):
            extractor._extract_branch(inventory_page, {"value": "13", "label": "GUAMUCHIL"})

    def test_late_wrong_warehouse_and_missing_expected_response_preserve_previous_cut(self):
        service = PointSyncService(extractor=FakeExtractor())
        complete = service.run_inventory_sync(capture_costs=False)
        original = PointInventorySnapshot.objects.get(sync_job=complete)
        settings = SimpleNamespace(timeout_ms=1000)
        extractor = PointInventoryExtractor(bridge_settings=settings)
        browser = Mock(url="https://point.example/Stock/Index")
        page = PointInventoryPage(browser, settings)
        target = {"value": "SUC-01", "label": "Centro"}

        def response(branch, category, stock):
            item = Mock(
                url=f"https://point.example/Stock/GetProductosPA?almacen={branch}&categoria={category}",
                request=SimpleNamespace(method="GET"), ok=True,
            )
            item.json.return_value = [{
                "PK_articulo": "SKU-001", "Codigo": "SKU-001", "Nombre": "Pastel Chocolate",
                "Categoria": "Pasteles", "Cantidad": stock, "Unidad": "PZA",
            }]
            return item

        for expected in (None, response("SUC-01", "12", "22")):
            late_a = response("SUC-02", "12", "11")
            wrong_category = response("SUC-01", "21", "33")

            @contextmanager
            def expect(predicate, *, timeout):
                pending = SimpleNamespace()
                yield pending
                events = [wrong_category, late_a] if expected is None else [expected, late_a, wrong_category]
                matches = [event for event in events if predicate(event)]
                if not matches:
                    raise PlaywrightTimeoutError("Expected warehouse response did not arrive")
                self.assertEqual(matches, [expected])
                pending.value = matches[0]

            browser.expect_response.side_effect = expect
            with (
                patch.object(page, "select_branch", return_value=target),
                patch.object(page, "list_category_options", return_value=[{"value": "12", "label": "Pasteles"}]),
                patch.object(page, "select_category"),
                patch.object(page, "extract_inventory_table") as visible_table,
                patch.object(extractor, "_write_raw_export", return_value=Path("/tmp/point-test.json")),
                patch.object(service.extractor, "extract", side_effect=lambda **_: [extractor._extract_branch(page, target)]),
            ):
                job = service.run_inventory_sync(branch_filter="Centro", capture_costs=False)

            visible_table.assert_not_called()
            original.refresh_from_db()
            self.assertEqual(original.stock, 10)
            if expected is None:
                self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
                self.assertFalse(PointInventorySnapshot.objects.filter(sync_job=job).exists())
            else:
                self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
                snapshot = PointInventorySnapshot.objects.get(sync_job=job)
                self.assertEqual(snapshot.stock, 22)
                self.assertEqual(snapshot.branch_id, original.branch_id)
                self.assertEqual(PointInventorySnapshot.objects.count(), 2)
