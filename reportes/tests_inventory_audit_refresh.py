from datetime import date
from importlib import import_module, util
from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase
from django.db import transaction

from recetas.models import ProductoMonthClosure
from reportes.tests_inventory_traceability_materializer import TraceabilityTestFixtures


class InventoryAuditRefreshTests(TestCase):
    def setUp(self):
        cache.clear()

    def service(self):
        name = "reportes.services_inventory_audit_refresh"
        self.assertIsNotNone(util.find_spec(name), "Falta coordinación automática del auditor")
        return import_module(name)

    def test_locked_month_is_never_rebuilt(self):
        ProductoMonthClosure.objects.create(month_start=date(2026, 8, 1), month_end=date(2026, 8, 31), is_locked=True)
        result = self.service().refresh_inventory_audit_month("2026-08")
        self.assertEqual(result["status"], "locked")

    def test_daily_months_use_operational_timezone_and_pending_only(self):
        service = self.service()
        with patch.object(service.timezone, "localdate", return_value=date(2026, 10, 1)):
            self.assertEqual(service.inventory_audit_review_months(), [date(2026, 9, 1), date(2026, 10, 1)])

    def test_publish_occurs_after_commit_and_coalesces_same_month(self):
        service = self.service()
        with patch("reportes.tasks.refresh_inventory_audit_month_task.apply_async") as send:
            with self.captureOnCommitCallbacks(execute=True):
                service.enqueue_inventory_audit_months([date(2026, 8, 1)] * 3)
                service.enqueue_inventory_audit_months([date(2026, 8, 1)])
                send.assert_not_called()
            self.assertEqual(send.call_count, 1)

    def test_rollback_does_not_enqueue(self):
        service = self.service()
        with patch("reportes.tasks.refresh_inventory_audit_month_task.apply_async") as send:
            with self.captureOnCommitCallbacks(execute=True):
                try:
                    with transaction.atomic():
                        service.enqueue_inventory_audit_months([date(2026, 8, 1)])
                        raise ValueError("rollback de prueba")
                except ValueError:
                    pass
            send.assert_not_called()

    def test_incomplete_sources_do_not_replace_previous_success_or_create_point_jobs(self):
        from pos_bridge.models import PointSyncJob
        from reportes.models import ProductInventoryAuditRun
        from django.utils import timezone
        stamp = timezone.now()
        run = ProductInventoryAuditRun.objects.create(month=date(2026, 8, 1), last_successful_rebuild_at=stamp)
        result = self.service().refresh_inventory_audit_month("2026-08")
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(PointSyncJob.objects.count(), 0)
        run.refresh_from_db()
        self.assertEqual(run.last_successful_rebuild_at, stamp)

    def test_active_source_job_defers_without_rebuilding(self):
        from pos_bridge.models import PointSyncJob
        from reportes.models import ProductInventoryAuditRun
        PointSyncJob.objects.create(job_type="production", status="RUNNING", parameters={"start_date": "2026-08-01", "end_date": "2026-08-31"})
        result = self.service().refresh_inventory_audit_month("2026-08")
        self.assertEqual(result["status"], "deferred")
        self.assertFalse(ProductInventoryAuditRun.objects.exists())

    def test_timestamp_inputs_are_normalized_to_operational_month(self):
        from datetime import datetime, timezone
        service = self.service()
        self.assertEqual(service._month(datetime(2026, 9, 1, 1, tzinfo=timezone.utc)), date(2026, 8, 1))


class InventoryAuditRefreshMaterializationTests(TraceabilityTestFixtures, TestCase):
    def setUp(self):
        cache.clear()

    def test_repeat_refresh_preserves_approved_quantities_and_does_not_create_point_jobs(self):
        from pos_bridge.models import PointSyncJob
        from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditRun
        from reportes.services_inventory_audit_refresh import refresh_inventory_audit_month
        with patch("reportes.services_inventory_traceability.BranchInventoryTraceabilityService") as source:
            source.return_value.build.return_value = self._result(self._line())
            with self.captureOnCommitCallbacks(execute=True):
                first = refresh_inventory_audit_month("2026-08")
            case = ProductInventoryAuditCase.objects.get()
            self._approve_case(case)
            before = ProductInventoryAuditRun.objects.get().last_successful_rebuild_at
            second = refresh_inventory_audit_month("2026-08")
            case.refresh_from_db()
            self.assertEqual(first["status"], "updated")
            self.assertEqual(second["status"], "unchanged")
            self.assertEqual(case.movement_status, "RESOLVED")
            self.assertEqual(case.expected_closing, 10)
            self.assertEqual(case.point_closing, 11)
            self.assertEqual(ProductInventoryAuditRun.objects.get().last_successful_rebuild_at, before)
            self.assertEqual(PointSyncJob.objects.count(), 0)
            self.assertEqual(source.return_value.build.call_count, 1)

    def test_close_snapshot_enqueues_both_closing_and_opening_months(self):
        from datetime import datetime
        from django.utils import timezone
        from pos_bridge.models import PointInventorySnapshot, PointSyncJob
        job = PointSyncJob.objects.create(job_type="inventory", status="SUCCESS")
        with patch("reportes.tasks.refresh_inventory_audit_month_task.apply_async") as broker:
            with self.captureOnCommitCallbacks(execute=True):
                PointInventorySnapshot.objects.create(branch=self.branch, product=self.product, stock=5, sync_job=job,
                    captured_at=timezone.make_aware(datetime(2026, 9, 1, 1, 1)))
            months = {call.kwargs["kwargs"]["month"] for call in broker.call_args_list}
        self.assertEqual(months, {"2026-08", "2026-09"})
