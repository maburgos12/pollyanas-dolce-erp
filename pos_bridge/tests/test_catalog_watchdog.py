from datetime import timedelta
from unittest.mock import patch

from celery.exceptions import SoftTimeLimitExceeded
from django.core.management import call_command
from django.test import SimpleTestCase, TransactionTestCase
from django.utils import timezone

from config.celery import app
from pos_bridge.models import PointSyncJob
from pos_bridge.services.catalog_recipe_execution import recover_abandoned_catalog_jobs
from pos_bridge.tasks import task_catalog_recipe_sync
from pos_bridge.services.product_recipe_sync_service import (
    PointProductRecipeSyncService,
)
from recetas.models import Receta, LineaReceta


class LegacyCatalogRoutingTests(SimpleTestCase):
    @patch.object(task_catalog_recipe_sync, "apply_async")
    def test_message_already_in_general_queue_is_forwarded_without_executing(
        self, publish
    ):
        task_catalog_recipe_sync.push_request(delivery_info={"routing_key": "celery"})
        try:
            result = task_catalog_recipe_sync.run(job_id=123)
        finally:
            task_catalog_recipe_sync.pop_request()
        self.assertEqual(result["status"], "PENDING")
        publish.assert_called_once_with(
            kwargs={"job_id": 123}, queue="recipes", retry=False, ignore_result=True
        )


class CatalogWatchdogTests(TransactionTestCase):
    def test_interrupted_resolution_preserves_previous_recipe(self):
        recipe = Receta.objects.create(
            nombre="Previous complete recipe", codigo_point="PROBE"
        )
        line = LineaReceta.objects.create(
            receta=recipe, insumo_texto="Existing", cantidad=1
        )
        service = PointProductRecipeSyncService()
        from unittest.mock import Mock

        with patch.object(
            service, "_resolve_component", side_effect=SoftTimeLimitExceeded()
        ):
            with self.assertRaises(SoftTimeLimitExceeded):
                service._materialize_node_lines(
                    client=Mock(),
                    run=Mock(),
                    node=Mock(),
                    receta=recipe,
                    bom_rows=[{"Cantidad": 1}],
                    depth=0,
                    max_depth=3,
                    visited={},
                    summary={},
                    node_outcomes={},
                )
        self.assertTrue(LineaReceta.objects.filter(pk=line.pk).exists())

    def job(self, **parameters):
        job = PointSyncJob.objects.create(
            job_type="recipes",
            status="RUNNING",
            parameters={
                "action": "SYNC_ONLY_NEW_PRODUCTS",
                "resume_codes": ["4358"],
                **parameters,
            },
        )
        self.age(job)
        return job

    def age(self, job):
        PointSyncJob.objects.filter(pk=job.pk).update(
            updated_at=timezone.now() - timedelta(minutes=6)
        )

    @patch.object(task_catalog_recipe_sync, "apply_async")
    def test_abandoned_job_is_republished_once_and_keeps_resume_codes(self, publish):
        job = self.job()
        recover_abandoned_catalog_jobs()
        recover_abandoned_catalog_jobs()
        job.refresh_from_db()
        self.assertEqual(job.status, "PENDING")
        self.assertEqual(job.parameters["resume_codes"], ["4358"])
        self.assertEqual(job.parameters["auto_recoveries"], 1)
        publish.assert_called_once_with(
            kwargs={"job_id": job.id}, retry=False, ignore_result=True
        )

    @patch.object(
        task_catalog_recipe_sync,
        "apply_async",
        side_effect=ConnectionError("broker offline"),
    )
    def test_broker_outage_is_bounded_and_does_not_crash_page(self, publish):
        job = self.job()
        for _ in range(3):
            recover_abandoned_catalog_jobs()
            self.age(job)
        job.refresh_from_db()
        self.assertEqual(job.status, "FAILED")
        self.assertEqual(publish.call_count, 2)
        self.assertIn("automátic", job.error_message)
        self.assertEqual(job.parameters["resume_codes"], ["4358"])

    @patch.object(task_catalog_recipe_sync, "apply_async")
    def test_exhaustion_does_not_publish_or_touch_terminal_jobs(self, publish):
        job = self.job(auto_recoveries=2)
        done = self.job()
        PointSyncJob.objects.filter(pk=done.pk).update(status="SUCCESS")
        recover_abandoned_catalog_jobs()
        job.refresh_from_db()
        done.refresh_from_db()
        self.assertEqual(job.status, "FAILED")
        self.assertEqual(done.status, "SUCCESS")
        publish.assert_not_called()

    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_soft_timeout_waits_for_automatic_recovery(self, service):
        job = self.job()
        PointSyncJob.objects.filter(pk=job.pk).update(status="PENDING")
        service.return_value.discover_new_product_codes.side_effect = (
            SoftTimeLimitExceeded()
        )
        with self.assertRaises(SoftTimeLimitExceeded):
            task_catalog_recipe_sync(job_id=job.id)
        job.refresh_from_db()
        self.assertEqual(job.status, "PENDING")
        self.assertIsNone(job.finished_at)
        self.assertIn("automátic", job.parameters["progress"]["detail"])

    @patch.object(task_catalog_recipe_sync, "apply_async")
    def test_supervisor_recovers_without_web_request(self, publish):
        job = self.job()
        call_command("watch_catalog_recipes", once=True)
        job.refresh_from_db()
        self.assertEqual(job.status, "PENDING")
        publish.assert_called_once()

    def test_catalog_routed_to_its_own_queue(self):
        route = app.amqp.router.route(
            {}, task_catalog_recipe_sync.name, args=(), kwargs={"job_id": 1}
        )
        self.assertEqual(route["queue"].name, "recipes")
