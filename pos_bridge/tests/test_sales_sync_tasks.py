from __future__ import annotations

import os
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from pos_bridge.models import PointSyncJob
from pos_bridge.services.product_month_closure_service import ProductMonthClosureError
from pos_bridge.tasks.celery_tasks import task_visible_cut_refresh_cycle
from pos_bridge.tasks.run_monthly_product_closure import run_monthly_product_closure
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync
from pos_bridge.tasks.run_production_sync import run_production_sync
from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync
from pos_bridge.tasks.run_waste_sync import run_waste_sync


class PointSalesSyncTaskRoutingTests(SimpleTestCase):
    def test_monthly_closure_is_not_duplicated_in_static_beat_schedule(self):
        from django.conf import settings

        self.assertNotIn(
            "pos_bridge.monthly_product_closure",
            [entry.get("task") for entry in settings.CELERY_BEAT_SCHEDULE.values()],
        )

    def test_pos_bridge_tasks_exports_dashboard_refresh_tasks_for_celery_autodiscovery(self):
        import pos_bridge.tasks as tasks

        expected_task_exports = {
            "task_analytics_refresh_cycle",
            "task_catalog_recipe_sync",
            "task_operations_automation_cycle",
            "task_visible_cut_refresh_cycle",
        }

        self.assertTrue(expected_task_exports.issubset(set(tasks.__all__)))
        for task_name in expected_task_exports:
            self.assertTrue(hasattr(tasks, task_name))

    @override_settings(POINT_SALES_SYNC_SOURCE_MODE="OFFICIAL", POINT_SALES_SYNC_CREDITO_SCOPES=["null"])
    def test_run_sales_history_sync_defaults_to_official_source(self):
        fake_job = object()
        with patch("pos_bridge.tasks.run_sales_history_sync.OfficialSalesBackfillService") as service_cls:
            service_cls.return_value.run.return_value = fake_job
            result = run_sales_history_sync(
                start_date=date(2025, 9, 1),
                end_date=date(2025, 9, 3),
            )

        self.assertIs(result, fake_job)
        service_cls.return_value.run.assert_called_once_with(
            start_date=date(2025, 9, 1),
            end_date=date(2025, 9, 3),
            branch_filter=None,
            credito_scopes=["null"],
            excluded_ranges=None,
            max_days=None,
            triggered_by=None,
        )

    @override_settings(POINT_SALES_SYNC_SOURCE_MODE="LEGACY")
    def test_run_sales_history_sync_can_use_legacy_source(self):
        fake_job = object()
        # load_point_bridge_settings prioriza la variable de entorno sobre el
        # setting Django, y settings.py carga .env (POINT_SALES_SYNC_SOURCE_MODE
        # =OFFICIAL) en os.environ; hay que fijar también el entorno.
        with patch.dict(os.environ, {"POINT_SALES_SYNC_SOURCE_MODE": "LEGACY"}), patch(
            "pos_bridge.tasks.run_sales_history_sync.PointSyncService"
        ) as service_cls:
            service_cls.return_value.run_sales_sync.return_value = fake_job
            result = run_sales_history_sync(
                start_date=date(2025, 9, 1),
                end_date=date(2025, 9, 3),
            )

        self.assertIs(result, fake_job)
        service_cls.return_value.run_sales_sync.assert_called_once_with(
            start_date=date(2025, 9, 1),
            end_date=date(2025, 9, 3),
            excluded_ranges=None,
            triggered_by=None,
            branch_filter=None,
            max_days=None,
        )

    @override_settings(POINT_SALES_SYNC_SOURCE_MODE="OFFICIAL", POINT_SALES_SYNC_CREDITO_SCOPES=["null"])
    def test_run_daily_sales_sync_passes_window_and_mode_to_history_task(self):
        # Desde 79a13d38 la task consulta sync_job.status tras el sync; el stub
        # necesita status (FAILED evita los pasos post-sync que tocan DB).
        fake_job = SimpleNamespace(id=1, status=PointSyncJob.STATUS_FAILED)
        with patch("pos_bridge.tasks.run_daily_sales_sync.run_sales_history_sync", return_value=fake_job) as task_mock:
            result = run_daily_sales_sync(
                lookback_days=3,
                lag_days=1,
                anchor_date=date(2025, 9, 5),
                source_mode="OFFICIAL",
                credito_scopes=["null"],
                publish_analytics=False,
            )

        self.assertIs(result, fake_job)
        task_mock.assert_called_once_with(
            start_date=date(2025, 9, 2),
            end_date=date(2025, 9, 4),
            excluded_ranges=None,
            triggered_by=None,
            branch_filter=None,
            source_mode="OFFICIAL",
            credito_scopes=["null"],
        )

    def test_run_daily_sales_sync_refreshes_analytics_after_successful_sync(self):
        fake_job = SimpleNamespace(
            id=77,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
            save=lambda **kwargs: None,
        )
        fake_summary = SimpleNamespace(
            sales_rows=12,
            inventory_rows=3,
            production_rows=4,
            forecast_rows=5,
            calibration_rows=1,
        )
        with (
            patch("pos_bridge.tasks.run_daily_sales_sync.run_sales_history_sync", return_value=fake_job),
            patch("pos_bridge.tasks.run_daily_sales_sync.refresh_incremental", return_value=fake_summary) as refresh_mock,
            patch(
                "pos_bridge.tasks.run_daily_sales_sync.ensure_sales_dashboard_freshness",
                return_value=SimpleNamespace(
                    target_date=date(2025, 9, 4),
                    point_latest_date=date(2025, 9, 4),
                    fact_latest_date_before=date(2025, 9, 2),
                    fact_latest_date_after=date(2025, 9, 4),
                    visible_cut_date_before=date(2025, 9, 2),
                    visible_cut_date_after=date(2025, 9, 4),
                    catchup_attempted=True,
                    catchup_succeeded=True,
                    lag_days_before=2,
                    lag_days_after=0,
                ),
            ) as freshness_mock,
            patch("pos_bridge.tasks.run_daily_sales_sync.log_event") as log_mock,
        ):
            result = run_daily_sales_sync(
                lookback_days=3,
                lag_days=1,
                anchor_date=date(2025, 9, 5),
            )

        self.assertIs(result, fake_job)
        refresh_mock.assert_called_once_with(reference_date=date(2025, 9, 4), lookback_days=2)
        freshness_mock.assert_called_once_with(
            reference_date=date(2025, 9, 4),
            lookback_days=2,
            triggered_by=None,
            trigger="point_daily_sales_sync",
        )
        self.assertEqual(
            fake_job.result_summary["analytics_refresh"],
            {
                "reference_date": "2025-09-04",
                "lookback_days": 2,
                "sales_rows": 12,
                "inventory_rows": 3,
                "production_rows": 4,
                "forecast_rows": 5,
                "calibration_rows": 1,
                "trigger": "point_daily_sales_sync",
            },
        )
        self.assertEqual(
            fake_job.result_summary["sales_dashboard_freshness"],
            {
                "target_date": "2025-09-04",
                "point_latest_date": "2025-09-04",
                "fact_latest_date_before": "2025-09-02",
                "fact_latest_date_after": "2025-09-04",
                "visible_cut_date_before": "2025-09-02",
                "visible_cut_date_after": "2025-09-04",
                "catchup_attempted": True,
                "catchup_succeeded": True,
                "lag_days_before": 2,
                "lag_days_after": 0,
            },
        )
        log_mock.assert_called_once()

    def test_run_daily_sales_sync_logs_failed_analytics_refresh(self):
        fake_job = SimpleNamespace(
            id=88,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
            save=lambda **kwargs: None,
        )
        with (
            patch("pos_bridge.tasks.run_daily_sales_sync.run_sales_history_sync", return_value=fake_job),
            patch("pos_bridge.tasks.run_daily_sales_sync.refresh_incremental", side_effect=RuntimeError("analytics down")),
            patch("pos_bridge.tasks.run_daily_sales_sync.log_event") as log_mock,
        ):
            with self.assertRaises(RuntimeError):
                run_daily_sales_sync(
                    lookback_days=3,
                    lag_days=1,
                    anchor_date=date(2025, 9, 5),
                )

        log_mock.assert_called_once()
        _, kwargs = log_mock.call_args
        self.assertEqual(kwargs["payload"]["reference_date"], "2025-09-04")
        self.assertEqual(kwargs["payload"]["trigger"], "point_daily_sales_sync")
        self.assertIn("analytics down", kwargs["payload"]["error"])

    def test_run_production_sync_refreshes_analytics_after_successful_sync(self):
        fake_job = SimpleNamespace(
            id=91,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
            save=lambda **kwargs: None,
        )
        fake_summary = SimpleNamespace(
            sales_rows=2,
            inventory_rows=7,
            production_rows=11,
            forecast_rows=3,
            calibration_rows=0,
        )
        with (
            patch("pos_bridge.tasks.run_production_sync.PointMovementSyncService") as service_cls,
            patch("pos_bridge.tasks.run_production_sync.refresh_incremental", return_value=fake_summary) as refresh_mock,
            patch("pos_bridge.tasks.run_production_sync.log_event") as log_mock,
        ):
            service_cls.return_value.run_production_sync.return_value = fake_job
            result = run_production_sync(
                lookback_days=1,
                lag_days=1,
                anchor_date=date(2025, 9, 5),
            )

        self.assertIs(result, fake_job)
        refresh_mock.assert_called_once_with(reference_date=date(2025, 9, 4), lookback_days=1)
        self.assertEqual(fake_job.result_summary["analytics_refresh"]["trigger"], "point_production_sync")
        self.assertEqual(fake_job.result_summary["analytics_refresh"]["production_rows"], 11)
        log_mock.assert_called_once()

    def test_run_waste_sync_refreshes_analytics_after_successful_sync(self):
        fake_job = SimpleNamespace(
            id=92,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
            save=lambda **kwargs: None,
        )
        fake_summary = SimpleNamespace(
            sales_rows=1,
            inventory_rows=4,
            production_rows=9,
            forecast_rows=2,
            calibration_rows=0,
        )
        with (
            patch("pos_bridge.tasks.run_waste_sync.PointMovementSyncService") as service_cls,
            patch("pos_bridge.tasks.run_waste_sync.refresh_incremental", return_value=fake_summary) as refresh_mock,
            patch("pos_bridge.tasks.run_waste_sync.log_event") as log_mock,
        ):
            service_cls.return_value.run_waste_sync.return_value = fake_job
            result = run_waste_sync(
                lookback_days=1,
                lag_days=1,
                anchor_date=date(2025, 9, 5),
            )

        self.assertIs(result, fake_job)
        refresh_mock.assert_called_once_with(reference_date=date(2025, 9, 4), lookback_days=1)
        self.assertEqual(fake_job.result_summary["analytics_refresh"]["trigger"], "point_waste_sync")
        self.assertEqual(fake_job.result_summary["analytics_refresh"]["production_rows"], 9)
        log_mock.assert_called_once()

    def test_run_production_sync_logs_failed_analytics_refresh(self):
        fake_job = SimpleNamespace(
            id=93,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
            save=lambda **kwargs: None,
        )
        with (
            patch("pos_bridge.tasks.run_production_sync.PointMovementSyncService") as service_cls,
            patch("pos_bridge.tasks.run_production_sync.refresh_incremental", side_effect=RuntimeError("prod analytics down")),
            patch("pos_bridge.tasks.run_production_sync.log_event") as log_mock,
        ):
            service_cls.return_value.run_production_sync.return_value = fake_job
            with self.assertRaises(RuntimeError):
                run_production_sync(
                    lookback_days=1,
                    lag_days=1,
                    anchor_date=date(2025, 9, 5),
                )

        log_mock.assert_called_once()
        _, kwargs = log_mock.call_args
        self.assertEqual(kwargs["payload"]["trigger"], "point_production_sync")
        self.assertIn("prod analytics down", kwargs["payload"]["error"])

    def test_run_waste_sync_logs_failed_analytics_refresh(self):
        fake_job = SimpleNamespace(
            id=94,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
            save=lambda **kwargs: None,
        )
        with (
            patch("pos_bridge.tasks.run_waste_sync.PointMovementSyncService") as service_cls,
            patch("pos_bridge.tasks.run_waste_sync.refresh_incremental", side_effect=RuntimeError("waste analytics down")),
            patch("pos_bridge.tasks.run_waste_sync.log_event") as log_mock,
        ):
            service_cls.return_value.run_waste_sync.return_value = fake_job
            with self.assertRaises(RuntimeError):
                run_waste_sync(
                    lookback_days=1,
                    lag_days=1,
                    anchor_date=date(2025, 9, 5),
                )

        log_mock.assert_called_once()
        _, kwargs = log_mock.call_args
        self.assertEqual(kwargs["payload"]["trigger"], "point_waste_sync")
        self.assertIn("waste analytics down", kwargs["payload"]["error"])

    def test_run_monthly_product_closure_targets_previous_month_by_default(self):
        fake_closure = type(
            "FakeClosure",
            (),
            {
                "id": 99,
                "status": "BUILT",
                "is_locked": False,
                "metadata": {"validation": {"lock_ready": True}},
                "get_status_display": lambda self: "Construido",
            },
        )()
        with (
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductoMonthClosure.objects.filter") as filter_mock,
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductMonthClosureService") as service_cls,
            patch(
                "pos_bridge.tasks.run_monthly_product_closure._refresh_month_sources",
                return_value=[
                    {"name": name, "status": PointSyncJob.STATUS_SUCCESS}
                    for name in ("sales", "production", "waste", "conversions")
                ],
            ),
        ):
            filter_mock.return_value.order_by.return_value.first.return_value = None
            service_cls.return_value.build.return_value = fake_closure
            result = run_monthly_product_closure(anchor_date=date(2026, 3, 27))

        self.assertEqual(result["action"], "built")
        self.assertEqual(result["month"], "2026-02")
        service_cls.return_value.build.assert_called_once_with(
            month=date(2026, 2, 1),
            rebuild=False,
            lock_after_build=False,
            built_by=None,
            approval_reason="scheduled_monthly_automation",
            approval_channel="celery_monthly_product_closure",
        )

    def test_monthly_closure_refreshes_exact_official_month_before_build_and_locks_when_ready(self):
        fake_job = lambda job_id: SimpleNamespace(  # noqa: E731
            id=job_id,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
            error_message="",
        )
        built = SimpleNamespace(
            id=101,
            status="BUILT",
            is_locked=False,
            metadata={
                "validation": {"lock_ready": True},
                "opening_meta": {"authoritative": True, "effective_date": "2026-07-31"},
                "closing_inventory_meta": {"authoritative": True, "effective_date": "2026-08-31"},
            },
            get_status_display=lambda: "Construido",
        )
        locked = SimpleNamespace(**{**built.__dict__, "status": "LOCKED", "is_locked": True})
        locked.get_status_display = lambda: "Bloqueado"

        with (
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductoMonthClosure.objects.filter") as filter_mock,
            patch("pos_bridge.tasks.run_monthly_product_closure.run_sales_history_sync", return_value=fake_job(1)) as sales,
            patch("pos_bridge.tasks.run_monthly_product_closure.PointMovementSyncService") as movements,
            patch(
                "pos_bridge.tasks.run_monthly_product_closure.sync_conversion_lines",
                return_value={
                    "status": PointSyncJob.STATUS_SUCCESS,
                    "job_id": 4,
                    "issues": [],
                    "provenance": {
                        "source": "point_conversion_lines",
                        "date_from": "2026-08-01",
                        "date_to": "2026-08-31",
                        "branch_filter": "",
                    },
                },
            ) as conversions,
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductMonthClosureService") as closure_service,
        ):
            filter_mock.return_value.order_by.return_value.first.return_value = None
            movements.return_value.run_production_sync.return_value = fake_job(2)
            movements.return_value.run_waste_sync.return_value = fake_job(3)
            closure_service.return_value.build.return_value = built
            closure_service.return_value.lock.return_value = locked

            result = run_monthly_product_closure(month="2026-08", lock_after_build=True)

        exact_range = {"start_date": date(2026, 8, 1), "end_date": date(2026, 8, 31)}
        sales.assert_called_once_with(
            **exact_range,
            excluded_ranges=None,
            triggered_by=None,
            branch_filter=None,
            max_days=None,
            source_mode="OFFICIAL",
        )
        movements.return_value.run_production_sync.assert_called_once_with(
            **exact_range, branch_filter=None, triggered_by=None
        )
        movements.return_value.run_waste_sync.assert_called_once_with(
            **exact_range, branch_filter=None, triggered_by=None
        )
        conversions.assert_called_once_with(
            date_from=date(2026, 8, 1), date_to=date(2026, 8, 31), branch_filter=None
        )
        closure_service.return_value.build.assert_called_once_with(
            month=date(2026, 8, 1),
            rebuild=False,
            lock_after_build=False,
            built_by=None,
            approval_reason="scheduled_monthly_automation",
            approval_channel="celery_monthly_product_closure",
        )
        closure_service.return_value.lock.assert_called_once()
        self.assertEqual(result["action"], "built")
        self.assertTrue(result["is_locked"])
        self.assertTrue(result["inventory_authority"]["opening"]["authoritative"])
        self.assertEqual(result["inventory_authority"]["opening"]["target_date"], "2026-07-31")
        self.assertEqual(result["inventory_authority"]["closing"]["target_date"], "2026-08-31")
        self.assertEqual([step["name"] for step in result["source_refresh"]], ["sales", "production", "waste", "conversions"])
        conversion_step = result["source_refresh"][-1]
        self.assertEqual(conversion_step["job_id"], 4)
        self.assertEqual(conversion_step["summary"]["provenance"]["date_from"], "2026-08-01")

    def test_monthly_closure_partial_source_builds_reviewable_draft_without_lock(self):
        success = SimpleNamespace(id=1, status=PointSyncJob.STATUS_SUCCESS, result_summary={}, error_message="")
        partial = SimpleNamespace(
            id=2,
            status=PointSyncJob.STATUS_PARTIAL,
            result_summary={"issues": ["PRODUCTION_BRANCH_COVERAGE_INCOMPLETE"]},
            error_message="",
        )
        closure = SimpleNamespace(
            id=102,
            status="BUILT",
            is_locked=False,
            metadata={"validation": {"lock_ready": False, "blocking_issues": ["MONTH_SOURCE_INCOMPLETE"]}},
            get_status_display=lambda: "Construido",
        )
        with (
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductoMonthClosure.objects.filter") as filter_mock,
            patch("pos_bridge.tasks.run_monthly_product_closure.run_sales_history_sync", return_value=success),
            patch("pos_bridge.tasks.run_monthly_product_closure.PointMovementSyncService") as movements,
            patch(
                "pos_bridge.tasks.run_monthly_product_closure.sync_conversion_lines",
                return_value={"status": PointSyncJob.STATUS_SUCCESS, "issues": []},
            ),
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductMonthClosureService") as closure_service,
        ):
            filter_mock.return_value.order_by.return_value.first.return_value = None
            movements.return_value.run_production_sync.return_value = partial
            movements.return_value.run_waste_sync.return_value = success
            closure_service.return_value.build.return_value = closure

            result = run_monthly_product_closure(month="2026-08", lock_after_build=True)

        closure_service.return_value.lock.assert_not_called()
        self.assertFalse(result["is_locked"])
        self.assertEqual(result["automation_status"], "REVIEW")
        self.assertIn("production", result["failed_or_partial_sources"])

    def test_monthly_closure_lock_guard_failure_keeps_built_closure_for_review(self):
        success = SimpleNamespace(id=1, status=PointSyncJob.STATUS_SUCCESS, result_summary={}, error_message="")
        closure = SimpleNamespace(
            id=103,
            status="BUILT",
            is_locked=False,
            metadata={"validation": {"lock_ready": True}},
            get_status_display=lambda: "Construido",
        )
        with (
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductoMonthClosure.objects.filter") as filter_mock,
            patch("pos_bridge.tasks.run_monthly_product_closure.run_sales_history_sync", return_value=success),
            patch("pos_bridge.tasks.run_monthly_product_closure.PointMovementSyncService") as movements,
            patch(
                "pos_bridge.tasks.run_monthly_product_closure.sync_conversion_lines",
                return_value={"status": PointSyncJob.STATUS_SUCCESS, "issues": []},
            ),
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductMonthClosureService") as closure_service,
        ):
            filter_mock.return_value.order_by.return_value.first.return_value = None
            movements.return_value.run_production_sync.return_value = success
            movements.return_value.run_waste_sync.return_value = success
            closure_service.return_value.build.return_value = closure
            closure_service.return_value.lock.side_effect = ProductMonthClosureError("source fingerprint changed")

            result = run_monthly_product_closure(month="2026-08", lock_after_build=True)

        self.assertFalse(result["is_locked"])
        self.assertEqual(result["automation_status"], "REVIEW")
        self.assertIn("lock", result["failed_or_partial_sources"])
        self.assertIn("source fingerprint changed", result["automation_reviews"])

    def test_monthly_closure_rebuilds_existing_unlocked_draft_after_refresh(self):
        existing = SimpleNamespace(id=77, is_locked=False, status="DRAFT")
        success = SimpleNamespace(id=1, status=PointSyncJob.STATUS_SUCCESS, result_summary={}, error_message="")
        closure = SimpleNamespace(
            id=77,
            status="BUILT",
            is_locked=False,
            metadata={"validation": {"lock_ready": True}},
            get_status_display=lambda: "Construido",
        )
        with (
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductoMonthClosure.objects.filter") as filter_mock,
            patch("pos_bridge.tasks.run_monthly_product_closure.run_sales_history_sync", return_value=success),
            patch("pos_bridge.tasks.run_monthly_product_closure.PointMovementSyncService") as movements,
            patch(
                "pos_bridge.tasks.run_monthly_product_closure.sync_conversion_lines",
                return_value={"status": PointSyncJob.STATUS_SUCCESS, "issues": []},
            ),
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductMonthClosureService") as closure_service,
        ):
            filter_mock.return_value.order_by.return_value.first.return_value = existing
            movements.return_value.run_production_sync.return_value = success
            movements.return_value.run_waste_sync.return_value = success
            closure_service.return_value.build.return_value = closure
            result = run_monthly_product_closure(month="2026-08")

        self.assertEqual(result["action"], "rebuilt")
        self.assertTrue(closure_service.return_value.build.call_args.kwargs["rebuild"])

    def test_monthly_closure_skips_locked_month_without_refresh_or_mutation(self):
        locked = SimpleNamespace(
            id=88,
            is_locked=True,
            status="LOCKED",
            metadata={"validation": {"lock_ready": True}},
            get_status_display=lambda: "Bloqueado",
        )
        with (
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductoMonthClosure.objects.filter") as filter_mock,
            patch("pos_bridge.tasks.run_monthly_product_closure.run_sales_history_sync") as sales,
            patch("pos_bridge.tasks.run_monthly_product_closure.PointMovementSyncService") as movements,
            patch("pos_bridge.tasks.run_monthly_product_closure.sync_conversion_lines") as conversions,
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductMonthClosureService") as closure_service,
        ):
            filter_mock.return_value.order_by.return_value.first.return_value = locked
            result = run_monthly_product_closure(month="2026-08", rebuild=True, lock_after_build=True)

        sales.assert_not_called()
        movements.assert_not_called()
        conversions.assert_not_called()
        closure_service.assert_not_called()
        self.assertEqual(result["action"], "skipped_locked")


class VisibleCutRefreshTaskTests(SimpleTestCase):
    def test_visible_cut_refresh_cycle_records_validation_totals(self):
        fake_job = SimpleNamespace(id=321, status=PointSyncJob.STATUS_SUCCESS)

        with (
            patch("pos_bridge.tasks.celery_tasks.run_daily_sales_sync", return_value=fake_job) as sync_mock,
            patch(
                "pos_bridge.tasks.celery_tasks._validate_visible_cut_refresh",
                return_value={
                    "fact_total": "92035.99",
                    "indicator_total": "92035.99",
                    "materialized_total": "92035.99",
                    "materialized_date": "2026-04-21",
                },
            ) as validate_mock,
            patch("pos_bridge.tasks.celery_tasks._record_visible_cut_audit") as audit_mock,
            patch("pos_bridge.tasks.celery_tasks.log_event") as log_mock,
            patch("pos_bridge.tasks.celery_tasks.cache.delete"),
        ):
            payload = task_visible_cut_refresh_cycle.run(reference_date_iso="2026-04-21", triggered_by_id=None)

        sync_mock.assert_called_once()
        validate_mock.assert_called_once_with(reference_date=date(2026, 4, 21))
        self.assertEqual(payload["sync_job_id"], 321)
        self.assertEqual(payload["sync_status"], PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(payload["fact_total"], "92035.99")
        self.assertEqual(payload["materialized_date"], "2026-04-21")
        audit_mock.assert_called_once()
        log_mock.assert_called_once()

    def test_visible_cut_refresh_cycle_fails_when_validation_detects_mismatch(self):
        fake_job = SimpleNamespace(id=654, status=PointSyncJob.STATUS_SUCCESS)

        with (
            patch("pos_bridge.tasks.celery_tasks.run_daily_sales_sync", return_value=fake_job),
            patch(
                "pos_bridge.tasks.celery_tasks._validate_visible_cut_refresh",
                side_effect=RuntimeError("Visible cut mismatch after sync"),
            ),
            patch("pos_bridge.tasks.celery_tasks._record_visible_cut_audit") as audit_mock,
            patch("pos_bridge.tasks.celery_tasks.log_event") as log_mock,
            patch("pos_bridge.tasks.celery_tasks.cache.delete"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Visible cut mismatch after sync"):
                task_visible_cut_refresh_cycle.run(reference_date_iso="2026-04-21", triggered_by_id=None)

        audit_mock.assert_called_once()
        self.assertEqual(log_mock.call_count, 1)
        args, kwargs = log_mock.call_args
        self.assertEqual(args[1], "INTEGRATIONS_OPERATIONAL_REFRESH_FAILED")
        self.assertIn("Visible cut mismatch after sync", kwargs["payload"]["error"])
