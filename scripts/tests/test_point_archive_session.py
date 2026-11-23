"""Actual subprocess protocol checks without Docker, network or Django."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ops/erp-telegram-release'))
import erp_point_archive as archive
import erp_telegram_cleanup as cleanup

POPEN=subprocess.Popen
ECHO="import sys,json\nfor line in sys.stdin:\n r=json.loads(line);print(json.dumps({'id':r['id'],'result':{'entries':[]}}),flush=True)\n"


class InventorySessionTests(unittest.TestCase):
    def test_future_manifest_100_files_but_not_101_or_100mb_over(self):
        entries=[]
        for i in range(101):
            name='source_'+str(i)+'.xls';digest='a'*64
            entries.append(dict(schema=1,id=archive.entry_id(name,digest),relative_path=name,
                sha256=digest,bytes=1,nas_key='point-archive/objects/'+digest+'.xls',
                verified_at=1,consumers=['category_report']))
        def manifest(files):
            return dict(type='point_archive_manifest',id=cleanup.point_manifest_id(files),files=files,
                        recovery={'files_verified':len(files),'verified_at':1})
        cleanup.validate_point_manifest(manifest(entries[:100]))
        self.assertEqual(archive.MAX_PILOT_FILES,100)
        with self.assertRaises(ValueError):cleanup.validate_point_manifest(manifest(entries))
        entries[0]['bytes']=100_000_000
        with self.assertRaises(ValueError):cleanup.validate_point_manifest(manifest(entries[:2]))

    def session(self,script=ECHO):
        children=[]
        def spawn(*args,**kwargs):
            child=POPEN([sys.executable,'-u','-c',script],**kwargs)
            children.append(child)
            return child
        mocked=patch.object(archive.subprocess,'Popen',side_effect=spawn)
        mocked.start();self.addCleanup(mocked.stop)
        session=archive.InventorySession()
        self.addCleanup(session.close)
        return session,children

    def test_two_fresh_requests_one_process_and_reaped(self):
        session,children=self.session()
        for args in (['--after-id','0'],['--name','one.xls']):
            self.assertEqual(session.request(args),{'entries':[]})
        self.assertEqual(len(children),1)
        session.close()
        self.assertEqual(children[0].returncode,0)
        self.assertTrue(all(p.closed for p in (children[0].stdin,children[0].stdout,children[0].stderr)))

    def test_rotation_between_requests_reaps_previous_process(self):
        session,children=self.session()
        session.request(['--after-id','0']);session.count=480
        self.assertEqual(session.request(['--after-id','0']),{'entries':[]})
        self.assertEqual(len(children),2)
        self.assertEqual(children[0].returncode,0)
        session.close();self.assertEqual(children[1].returncode,0)

    def test_invalid_request_size_sends_no_request(self):
        session,children=self.session()
        with self.assertRaises(ValueError):session.request(['--name','x'*4096])
        self.assertEqual(session.count,0)
        self.assertIsNotNone(children[0].returncode)

    def test_wrong_id_and_eof_never_retry(self):
        for script in ("print('{\"id\":99,\"result\":{}}',flush=True)","pass"):
            with self.subTest(script=script):
                session,children=self.session(script)
                with self.assertRaises((ValueError,RuntimeError)):
                    session.request(['--after-id','0'])
                with self.assertRaises((ValueError,RuntimeError)):
                    session.request(['--after-id','0'])
                session.close()
                self.assertEqual(len(children),1)
                self.assertIsNotNone(children[0].returncode)

    def test_stderr_flood_is_bounded_and_reaped(self):
        session,children=self.session("import os;os.write(2,b'x'*17000)")
        with self.assertRaises((ValueError,RuntimeError)):
            session.request(['--after-id','0'])
        session.close();self.assertIsNotNone(children[0].returncode)

    def test_oversize_response_rejected(self):
        session,_=self.session("import os;os.write(1,b'x'*(1048576+1))")
        with self.assertRaises((ValueError,RuntimeError)):
            session.request(['--after-id','0'])

    def test_inner_alarm_terminates_timeout_without_resend(self):
        script="import signal,time;signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.alarm(1);time.sleep(20)"
        session,children=self.session(script)
        with patch.object(archive,'SESSION_REPLY_SECONDS',0.1):
            with self.assertRaises(TimeoutError):session.request(['--after-id','0'])
        session.close()
        self.assertLess(children[0].returncode,0)
        self.assertEqual(len(children),1)

    def test_inner_alarm_eof_retains_existing_transient_timeout_policy(self):
        script="import signal,time;signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.alarm(1);time.sleep(20)"
        session,children=self.session(script)
        with self.assertRaises(subprocess.TimeoutExpired) as caught:
            session.request(['--after-id','0'])
        self.assertTrue(archive.failure_detail(caught.exception,'inventory')['transient'])
        self.assertTrue(archive.failure_detail(TimeoutError(),'inventory')['transient'])
        self.assertEqual(len(children),1)
        self.assertIsNotNone(children[0].returncode)

    def test_command_contains_inner_hard_lifetime_and_pre_django_alarm(self):
        session,children=self.session()
        self.assertEqual(session.request(['--after-id','0']),{'entries':[]})
        # Production argv is asserted independently from the injected test process.
        argv=archive.InventorySession.argv()
        self.assertIn('900',argv)
        boot=argv[-1]
        self.assertLess(boot.index('signal.alarm(85)'),boot.index('django.core.management'))


if __name__=='__main__':unittest.main()
