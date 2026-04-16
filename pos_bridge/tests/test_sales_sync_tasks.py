from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from pos_bridge.models import PointSyncJob
from pos_bridge.tasks.run_monthly_product_closure import run_monthly_product_closure
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync
from pos_bridge.tasks.run_production_sync import run_production_sync
from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync
from pos_bridge.tasks.run_waste_sync import run_waste_sync


class PointSalesSyncTaskRoutingTests(SimpleTestCase):
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
        with patch("pos_bridge.tasks.run_sales_history_sync.PointSyncService") as service_cls:
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
        fake_job = object()
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
            },
        )()
        with (
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductoMonthClosure.objects.filter") as filter_mock,
            patch("pos_bridge.tasks.run_monthly_product_closure.ProductMonthClosureService") as service_cls,
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
