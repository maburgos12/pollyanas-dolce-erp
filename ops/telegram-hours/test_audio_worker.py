import base64
import hashlib
import importlib.util
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

    def test_forced_wrapper_rejects_commands_and_sanitizes(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('audio_rpc.py'))],
            input=b'{"action":"shell","command":"SECRET"}', capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b'{"error":"unavailable"}')
        self.assertEqual(result.stderr, b'')


if __name__ == '__main__':
    unittest.main()
