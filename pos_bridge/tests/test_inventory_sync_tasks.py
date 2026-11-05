from types import SimpleNamespace
from unittest.mock import patch
from celery.exceptions import Retry
from django.test import SimpleTestCase
from pos_bridge.tasks.celery_tasks import task_inventory_sync


class InventorySyncTaskTests(SimpleTestCase):
    def test_failed_job_uses_bounded_celery_retry(self):
        job = SimpleNamespace(id=1, status='FAILED', result_summary={}, error_message='No se detectaron tarjetas de sucursal en Point.')
        with patch('pos_bridge.tasks.celery_tasks.run_inventory_sync', return_value=job), patch.object(task_inventory_sync, 'retry', side_effect=Retry()) as retry:
            with self.assertRaises(Retry):
                task_inventory_sync.run(capture_costs=False)
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
