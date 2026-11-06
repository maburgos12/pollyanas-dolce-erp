import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'ops/erp-autofix'))


class WorkerTests(unittest.TestCase):
    def test_observer_contract_normalizes_failures_and_skips(self):
        import worker
        self.assertEqual(worker.normalize_observation({'kind':'execution_failed'})['checks'], [])
        value=worker.normalize_observation({'kind':'doctor_fail','checks':[{'name':'x','status':'SKIPPED'},{'name':'y','status':'FAIL'}]})
        self.assertEqual(value['checks'], [{'name':'y','status':'FAIL'}])
        with self.assertRaises(ValueError):
            worker.normalize_observation({'kind':'doctor_fail','checks':[{'name':'x','status':'SECRET'}]})
        with self.assertRaises(ValueError):
            worker.normalize_observation({'kind':'doctor_fail','checks':[None]})

    def test_interruption_retires_only_matching_resources_and_blocks_retry(self):
        import worker
        import json
        from unittest.mock import Mock
        job_id = 'autofix-0123456789abcdef-1'
        entry = dict(job_id=job_id, status='running')
        ledger = Mock()
        ledger.read.return_value = {'causes': {'cause': entry}}
        with tempfile.TemporaryDirectory() as temporary, patch.object(worker, 'STATE', Path(temporary)), \
                patch.object(worker, '_stop_model_unit') as stop, patch.object(worker, '_docker') as docker, \
                patch.object(worker, '_inspect') as inspect, patch.object(worker, 'command'), patch.object(worker, 'GitHub'):
            job = Path(temporary) / 'jobs' / job_id
            job.mkdir(parents=True)
            (job / 'intent.json').write_text(json.dumps(dict(owner='erp-autofix', entry=entry,
                                                            tools_image='tools', pg_image='pg')))
            docker.side_effect = ['container-id', '', '']
            inspect.return_value = dict(Id='container-id', Image='pg', Mounts=[],
                                        Config={'Labels': {'codex.task': job_id, 'codex.owner': 'erp-autofix'}})
            worker.reconcile({}, ledger)
            self.assertEqual(stop.call_count, 3)
            docker.assert_any_call(['rm', '--force', 'container-id'])
            ledger.update.assert_called_once_with('cause', status='blocked', reason='interrupted_requires_review')
            self.assertTrue((job / 'interruption.json').exists())
            ledger.update.reset_mock()
            docker.side_effect = ['container-id']
            inspect.return_value['Config']['Labels']['codex.owner'] = 'another-task'
            with self.assertRaisesRegex(ValueError, 'interrupted_resource_mismatch'):
                worker.reconcile({}, ledger)
            ledger.update.assert_not_called()

    def test_red_requires_real_assertion_failures_without_import_errors(self):
        import worker
        self.assertEqual(worker.assertion_failure('Ran 3 tests in 1s\nFAILED (failures=1)'), 3)
        for value in ['Ran 3 tests\nFAILED (errors=1)', 'Ran 0 tests\nFAILED (failures=1)',
                      'PermissionError', 'Ran 3 tests\nFAILED (failures=1, errors=1)']:
            with self.assertRaises(ValueError):
                worker.assertion_failure(value)

    def test_light_sensor_needs_two_failures_and_resets_on_success(self):
        import worker
        with tempfile.TemporaryDirectory() as temporary, patch.object(worker,'STATE',Path(temporary)), \
                patch.object(worker,'observation',return_value=None), \
                patch.object(worker.urllib.request,'build_opener') as opener:
            opener.return_value.open.side_effect=OSError('network')
            self.assertIsNone(worker.detect())
            self.assertEqual(worker.detect()['kind'],'http_unavailable')
            opener.return_value.open.side_effect=None
            response=opener.return_value.open.return_value.__enter__.return_value
            response.status=200;response.read.return_value=b'{"status":"ok"}'
            self.assertIsNone(worker.detect())

    def test_test_commands_are_derived_without_shell(self):
        import worker
        self.assertEqual(worker.test_commands(['scripts/tests/test_erp_observer.py']),
                         [['-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_erp_observer.py']])
        with self.assertRaises(ValueError):
            worker.test_commands(['-c print(1)'])

    def test_patch_headers_binary_and_removed_tests_are_rejected(self):
        import worker
        for patch in ['not a diff', 'GIT binary patch',
                      'diff --git a/scripts/tests/test_x.py b/scripts/tests/test_x.py\n--- a/scripts/tests/test_x.py\n+++ b/scripts/tests/test_x.py\n@@ -1 +1 @@\n-assert True\n+pass\n']:
            with self.assertRaises(ValueError):
                worker.patch_paths(patch)

    def test_new_regression_patch_is_accepted(self):
        import worker
        patch = 'diff --git a/scripts/tests/test_x.py b/scripts/tests/test_x.py\n--- a/scripts/tests/test_x.py\n+++ b/scripts/tests/test_x.py\n@@ -1 +1,2 @@\n a\n+assert True\n'
        self.assertEqual(worker.patch_paths(patch), ['scripts/tests/test_x.py'])


if __name__ == '__main__':
    unittest.main()
