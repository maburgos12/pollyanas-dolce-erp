from types import SimpleNamespace
from unittest.mock import patch
from celery.exceptions import Retry
from django.test import TestCase
from pos_bridge.models import PointSyncJob
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.tasks.celery_tasks import task_inventory_sync


class InventorySyncTaskTests(TestCase):
    def test_failed_job_uses_bounded_celery_retry(self):
        job = PointSyncJob.objects.create(job_type='inventory', status='FAILED', parameters={'capture_costs': False}, error_message='No se detectaron tarjetas de sucursal en Point.')
        with patch('pos_bridge.tasks.celery_tasks.run_inventory_sync', return_value=job), patch.object(task_inventory_sync, 'retry', side_effect=Retry()) as retry:
            with self.assertRaises(Retry):
                task_inventory_sync.run(capture_costs=False)
        job.refresh_from_db()
        self.assertTrue(job.parameters['retry_scheduled'])
        self.assertFalse(job.parameters['capture_costs'])
        service = PointSyncService()
        with patch.object(service, 'run_inventory_sync') as background_retry:
            self.assertEqual(service.retry_failed_jobs(), [])
        background_retry.assert_not_called()
        retry.assert_called_once()
        self.assertEqual(task_inventory_sync.max_retries, 2)
        self.assertIn('tarjetas', str(retry.call_args.kwargs['exc']))

    def test_success_and_partial_preserve_result_without_retry(self):
        for status in ['SUCCESS', 'PARTIAL']:
            with self.subTest(status=status):
                job = SimpleNamespace(id=2, status=status, result_summary={'snapshots_created':4}, error_message='')
                with patch('pos_bridge.tasks.celery_tasks.run_inventory_sync', return_value=job) as run, patch.object(task_inventory_sync, 'retry') as retry:
                    result = task_inventory_sync.run(branch_filter='BAMOA', capture_costs=False)
                self.assertEqual(result['status'], status)
                run.assert_called_once_with(triggered_by=None, branch_filter='BAMOA', limit_branches=None, capture_costs=False)
                retry.assert_not_called()

    def test_job_claimed_by_background_recovery_is_not_retried_twice(self):
        job = PointSyncJob.objects.create(job_type='inventory', status='FAILED', parameters={'retry_scheduled': True})
        with patch('pos_bridge.tasks.celery_tasks.run_inventory_sync', return_value=job), patch.object(task_inventory_sync, 'retry') as retry:
            result = task_inventory_sync.run(capture_costs=False)
        self.assertEqual(result['status'], 'FAILED')
        retry.assert_not_called()
