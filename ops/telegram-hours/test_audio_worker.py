import base64
import hashlib
import importlib.util
import json
import os
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
import subprocess
import sys
import unittest

spec = importlib.util.spec_from_file_location('hours_worker', Path(__file__).with_name('audio_worker.py'))
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


class WorkerTests(unittest.TestCase):
    def job(self):
        data = b'\0\0' * 16000
        return {'id':1, 'lease':'a'*32, 'sha256':hashlib.sha256(data).hexdigest(), 'pcm':base64.b64encode(data).decode()}

    def test_checks_pcm_hash_and_lease(self):
        self.assertEqual(len(worker.validate_job(self.job())), 32000)
        for key, value in [('lease', 'z'*32), ('sha256', 'b'*64), ('id', True), ('pcm', '')]:
            job = self.job(); job[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                worker.validate_job(job)

    def test_ssh_has_explicit_secret_free_environment(self):
        with patch.dict(os.environ, {'ERP_AUDIO_SECRET_SENTINEL':'must-not-leak'}), patch.object(worker.subprocess, 'run', return_value=SimpleNamespace(stdout=b'{}')) as run:
            worker.rpc({'action':'claim'})
        self.assertEqual(run.call_args.kwargs.get('env'), {'PATH':'/usr/bin:/bin', 'LANG':'C'})

    def test_whisper_child_only_receives_cache_home_and_fixed_environment(self):
        with patch.dict(os.environ, {'ERP_AUDIO_SECRET_SENTINEL':'must-not-leak'}), patch.object(worker, 'rpc', side_effect=[self.job(), {'accepted':True}]), patch.object(worker.subprocess, 'run', return_value=SimpleNamespace(stdout=json.dumps({'text':'Todas hoy cerradas'}).encode())) as run:
            worker.work()
        self.assertEqual(run.call_args.kwargs.get('env'), {'PATH':'/usr/bin:/bin', 'LANG':'C', 'HOME':str(Path.home())})

    def test_forced_wrapper_rejects_commands_and_sanitizes(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('audio_rpc.py'))],
            input=b'{"action":"shell","command":"SECRET"}', capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b'{"error":"unavailable"}')
        self.assertEqual(result.stderr, b'')


if __name__ == '__main__':
    unittest.main()
