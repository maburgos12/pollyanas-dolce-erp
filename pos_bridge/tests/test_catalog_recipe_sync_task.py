from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from pos_bridge.models import PointSyncJob
from pos_bridge.tasks.celery_tasks import task_catalog_recipe_sync


class CatalogRecipeSyncTaskTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="catalog_recipe_sync",
            password="test12345",
        )

    @patch("pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_full_sync_updates_precreated_job_and_snapshot(self, service_cls, weekly_snapshot):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ALL_RECIPES", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        service_cls.return_value.sync.return_value = SimpleNamespace(
            summary={"products_selected": 12, "recipes_completed_successfully": 11},
            raw_export_path="/tmp/point-recipes.json",
        )
        weekly_snapshot.return_value = {"week_start": "2026-07-27", "total_items": 12}

        payload = task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(job.result_summary["products_selected"], 12)
        self.assertEqual(job.result_summary["weekly_cost_snapshot"]["total_items"], 12)
        self.assertEqual(job.artifacts["raw_export_path"], "/tmp/point-recipes.json")
        service_cls.return_value.sync.assert_called_once_with(
            branch_hint="MATRIZ",
            product_codes=None,
            include_without_recipe=False,
            sync_job=job,
        )
        self.assertEqual(payload["job_id"], job.id)
        self.assertEqual(payload["status"], PointSyncJob.STATUS_SUCCESS)

    @patch("pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_new_sync_finishes_cleanly_when_point_has_no_new_recipes(self, service_cls, weekly_snapshot):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        service_cls.return_value.discover_new_product_codes.return_value = {
            "new_codes": [],
            "blocked_candidates_count": 0,
        }

        payload = task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(job.result_summary["products_selected"], 0)
        self.assertEqual(job.result_summary["new_products_imported"], 0)
        self.assertEqual(job.result_summary["discovery"]["new_codes"], [])
        service_cls.return_value.sync.assert_not_called()
        weekly_snapshot.assert_not_called()
        self.assertEqual(payload["status"], PointSyncJob.STATUS_SUCCESS)

    @patch("pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_new_sync_preserves_blocked_candidates_for_catalog_status(self, service_cls, weekly_snapshot):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        service_cls.return_value.discover_new_product_codes.return_value = {
            "new_codes": [],
            "blocked_candidates_count": 1,
            "blocked_candidates": [
                {
                    "codigo_point": "EXTRA-FRESA-C",
                    "nombre": "Extra Fresa Chico",
                    "detection_reason": "POINT_NO_BOM",
                }
            ],
        }

        task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(job.result_summary["discovery"]["blocked_candidates_count"], 1)
        self.assertEqual(
            job.result_summary["discovery"]["blocked_candidates"][0]["codigo_point"],
            "EXTRA-FRESA-C",
        )
        service_cls.return_value.sync.assert_not_called()
        weekly_snapshot.assert_not_called()

    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_already_successful_job_is_not_executed_twice(self, service_cls):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_SUCCESS,
            parameters={"action": "SYNC_ALL_RECIPES", "branch_hint": "MATRIZ"},
            result_summary={"products_selected": 12},
            triggered_by=self.user,
        )

        payload = task_catalog_recipe_sync(job_id=job.id)

        self.assertEqual(payload["job_id"], job.id)
        self.assertEqual(payload["status"], PointSyncJob.STATUS_SUCCESS)
        service_cls.assert_not_called()

    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_sync_failure_is_persisted_on_job(self, service_cls):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ALL_RECIPES", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        service_cls.return_value.sync.side_effect = RuntimeError("Point timeout")

        with self.assertRaisesRegex(RuntimeError, "Point timeout"):
            task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        self.assertIn("Point timeout", job.error_message)
