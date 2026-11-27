"""Collector budget admission; synthetic Python sources only, no Django/Redis.

Linux enforces address-space ceilings. macOS rejects RLIMIT_AS at this ceiling;
that platform gets a fail-closed check, never a claimed memory-enforcement PASS.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release/erp_release_host.py'
spec = importlib.util.spec_from_file_location('collector_budget_host', SOURCE)
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
BUDGET = 192 * 1024 * 1024


def request(source, data=None):
    return json.dumps({'sources': {'erp_release_preservation': 'pass',
                                  'erp_release_snapshot': source}, 'request': data or {}}).encode()


def run(source, data=None, bootstrap=''):
    return subprocess.run([sys.executable, '-c', bootstrap + h.COLLECTOR_RUNNER],
                          input=request(source, data), capture_output=True, timeout=30)


class CollectorBudgetTests(unittest.TestCase):
    def test_limits_precede_child_communication_portable(self):
        # Execute the real runner/cap function with a synthetic Popen backend;
        # record ordering without pretending macOS enforces Linux RLIMIT_AS.
        bootstrap = '''
import json,resource,subprocess
observed=[]
resource.setrlimit=lambda key,value: observed.append([key,list(value)])
class Child:
 def __init__(self,*a,**kw):
  kw['preexec_fn']()
  self.out=kw['stdout'];self.returncode=None
 def communicate(self,data,timeout):
  assert observed==[[resource.RLIMIT_AS,[192*1024*1024]*2],
                    [resource.RLIMIT_CPU,[25,25]],
                    [resource.RLIMIT_FSIZE,[8*1024*1024]*2]]
  self.out.write(json.dumps({'limits':observed,'communication_after_limits':True}).encode())
  self.out.flush();self.returncode=0
subprocess.Popen=Child
'''
        result = run('def main(): pass', bootstrap=bootstrap)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt['communication_after_limits'])
        self.assertEqual(receipt['reader'], {'reaped': True, 'exit_code': 0, 'killed': False})

    @unittest.skipUnless(sys.platform.startswith('linux'), 'real RLIMIT_AS enforcement requires Linux')
    def test_real_limits_and_small_child_success(self):
        source = '''
import json,resource
def main():
 print(json.dumps({'as':resource.getrlimit(resource.RLIMIT_AS),
                   'cpu':resource.getrlimit(resource.RLIMIT_CPU)}))
'''
        result = run(source)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        receipt = json.loads(result.stdout)
        self.assertEqual(receipt['as'], [BUDGET, BUDGET])
        self.assertEqual(receipt['cpu'], [25, 25])
        self.assertTrue(receipt['reader']['reaped'])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'real oversized allocation requires Linux')
    def test_oversized_child_reaped_nonzero_parent_survives(self):
        source = '''
import json,os,pathlib,sys
def main():
 request=json.load(sys.stdin)
 pathlib.Path(request['pid']).write_text(str(os.getpid()))
 payload=bytearray(256*1024*1024)
 print(json.dumps({'unexpected':len(payload)}))
'''
        with tempfile.TemporaryDirectory() as directory:
            pid_path = Path(directory) / 'owned-reader.pid'
            result = run(source, {'pid': str(pid_path)})
            self.assertEqual(result.returncode, 4, result.stderr.decode())
            self.assertEqual(result.stdout, b'')
            pid = int(pid_path.read_text())
            with self.assertRaises(ProcessLookupError): os.kill(pid, 0)
        # Parent remained alive and can collect a new bounded small child.
        success = run('import json\ndef main(): print(json.dumps({"alive":True}))')
        self.assertEqual(success.returncode, 0, success.stderr.decode())
        self.assertTrue(json.loads(success.stdout)['alive'])

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS unsupported-limit behavior')
    def test_macos_unsupported_limit_fails_before_collector(self):
        result = run('def main(): print("collector_must_not_start")')
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(b'collector_must_not_start', result.stdout)
        self.assertIn(b'preexec_fn', result.stderr)


if __name__ == '__main__': unittest.main()
