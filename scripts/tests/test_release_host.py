import datetime as dt
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release/erp_release_host.py'
spec = importlib.util.spec_from_file_location('host_under_test', MODULE)
host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)


class HostTests(unittest.TestCase):
    def adapter(self, **kwargs):
        return host.HostAdapter(0, lease_check=lambda: True, **kwargs)

    def test_database_settings_initialized_without_gunicorn_environment(self):
        prefix="""import os,sys,types
os.environ.pop('DJANGO_SETTINGS_MODULE',None)
class FakeSettings:
 @property
 def DATABASES(self):
  assert os.environ['DJANGO_SETTINGS_MODULE']=='config.settings'
  return {'default':{'NAME':'synthetic','USER':'application_role','HOST':'db','PORT':'5432','PASSWORD':'never-expose'}}
django=types.ModuleType('django');conf=types.ModuleType('django.conf');conf.settings=FakeSettings()
sys.modules['django']=django;sys.modules['django.conf']=conf
"""
        result=host.subprocess.run([sys.executable,'-c',prefix+host.DATABASE_CONFIG_SOURCE],capture_output=True,text=True,check=True)
        self.assertEqual(host.json.loads(result.stdout),{'NAME':'synthetic','USER':'application_role','HOST':'db','PORT':'5432'})
        self.assertNotIn('never-expose',result.stdout+result.stderr)

    def test_missing_leases_fail_closed(self):
        with self.assertRaisesRegex(host.HostHold, 'shared_leases_unproved'):
            host.HostAdapter(0).leases()

    def test_profile_rejects_contract_changes(self):
        for path in ('ops/x.py', 'scripts/deploy_web_safe.sh', 'config/settings.py',
                     'rrhh/migrations/0001.py', 'requirements.txt', 'Dockerfile', 'docker-compose.yml'):
            with self.subTest(path=path), self.assertRaises(host.HostHold):
                host.HostAdapter.profile(['reportes/views.py', path])
        host.HostAdapter.profile(['reportes/views.py', 'templates/x.html'])

    def test_exact_seven_nodes_idle_and_missing_reply(self):
        adapter = self.adapter()
        nodes = [q+'@abc' for q in host.QUEUES.values()]
        cut = {'inspect_started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
               'replies': {phase: {n: ({'ok':'pong'} if phase == 'ping' else []) for n in nodes}
                           for phase in ('ping','active','reserved','scheduled')}}
        adapter.idle({'nodes':nodes}, cut)
        del cut['replies']['reserved'][nodes[0]]
        with self.assertRaisesRegex(host.HostHold, 'inspect_coverage'):
            adapter.idle({'nodes':nodes}, cut)

    def test_snapshot_revision_bound_and_input_only(self):
        calls = []
        def run(args, **kw):
            calls.append((args, kw))
            return '{"reader":{"reaped":true,"exit_code":0,"killed":false},"versions":{"celery":"5.4.0","kombu":"5.6.2"}}'
        adapter = self.adapter(runner=run)
        pins = {'collector_sha256': {k:host.hashlib.sha256(v.encode()).hexdigest() for k,v in adapter.sources.items()},
                'peers':{'web':{'id':'a'*64}}, 'nodes':[]}
        adapter.snapshot(pins)
        self.assertEqual(calls[0][1]['timeout'], 30)
        self.assertIn('sources', host.json.loads(calls[0][1]['input']))
        pins['collector_sha256']['erp_release_snapshot'] = 'bad'
        with self.assertRaisesRegex(host.HostHold, 'collector_revision'):adapter.snapshot(pins)
        self.assertEqual(len(calls), 1)

    def test_snapshot_rejects_missing_or_unvalidated_transport_versions(self):
        for versions in (None,{}, {'celery':'5.4.0'}, {'celery':'5.4.0','kombu':'5.6.3'}, {'celery':'5.5.0','kombu':'5.6.2'}):
            effects=[]
            response={'reader':{'reaped':True,'exit_code':0,'killed':False},'versions':versions}
            adapter=self.adapter(runner=lambda *a,**kw:host.json.dumps(response),effectful_runner=lambda *a,**kw:effects.append(a))
            pins={'collector_sha256':{k:host.hashlib.sha256(v.encode()).hexdigest() for k,v in adapter.sources.items()},
                  'peers':{'web':{'id':'a'*64}},'nodes':[]}
            with self.subTest(versions=versions),self.assertRaisesRegex(host.HostHold,'collector_versions_unvalidated'):
                adapter.snapshot(pins)
            self.assertEqual(effects,[])

    def installed_fixture(self):
        source='versioned worker source\n'
        expected=host.hashlib.sha256(source.encode()).hexdigest()
        env={'sha256':'e'*64,'uid':1000,'gid':1000,'mode':384}
        roles={role:{'id':str(i)*64,'state':{'Running':True,'OOMKilled':False,'StartedAt':'pinned'}}
               for i,role in enumerate((*host.QUEUES,*host.PEERS))}
        calls=[]
        def runner(args,**kwargs):
            calls.append((args,kwargs))
            if args[0]=='git':
                if 'rev-parse' in args:return 'a'*40+'\n'
                if 'status' in args:return ''
                if 'show' in args:return source
            if args[:2]==['docker','exec']:
                return host.json.dumps({'env':env,'source_sha256':expected})
            raise AssertionError(args)
        return runner,calls,env,roles

    def test_installed_runtime_readonly_exact_consumers(self):
        runner,calls,env,roles=self.installed_fixture()
        with patch.object(host.HostAdapter,'metadata',return_value=roles),patch.object(host.HostAdapter,'env',return_value=env):
            receipt=host.verify_installed_runtime('a'*40,runner=runner)
        self.assertTrue(receipt['verified'])
        self.assertEqual(set(receipt['roles']),set(host.QUEUES)|set(host.PEERS))
        self.assertEqual(len(receipt['source_consumers']),10)
        self.assertTrue(all(0<kw['timeout']<=10 for _,kw in calls))
        self.assertEqual(sum(args[:2]==['docker','exec'] for args,_ in calls),10)
        self.assertNotIn('kill',str(calls));self.assertNotIn('psql',str(calls))
        self.assertNotIn('flock',str(calls));self.assertNotIn('redis',str(calls))

    def test_installed_runtime_rejects_missing_stopped_or_oom_roles(self):
        for kind in ('missing','stopped','oom'):
            runner,calls,env,roles=self.installed_fixture()
            if kind=='missing':roles.pop('web')
            elif kind=='stopped':roles['web']['state']['Running']=False
            else:roles['worker']['state']['OOMKilled']=True
            with self.subTest(kind=kind),patch.object(host.HostAdapter,'metadata',return_value=roles),patch.object(host.HostAdapter,'env',return_value=env):
                with self.assertRaises(host.HostHold):host.verify_installed_runtime('a'*40,runner=runner)
            self.assertFalse(any(args[:2]==['docker','exec'] for args,_ in calls))

    def test_installed_runtime_rejects_source_env_and_checkout_drift(self):
        for kind in ('source','env','checkout','container'):
            runner,calls,env,roles=self.installed_fixture()
            def changed(args,**kwargs):
                value=runner(args,**kwargs)
                if args[:2]==['docker','exec']:
                    value=host.json.loads(value)
                    if kind=='source':value['source_sha256']='b'*64
                    if kind=='env':value['env']=dict(env,uid=0)
                    return host.json.dumps(value)
                if kind=='checkout' and 'rev-parse' in args:return 'b'*40
                return value
            drift={**roles,'web':{**roles['web'],'id':'new'}}
            with self.subTest(kind=kind),patch.object(host.HostAdapter,'metadata',side_effect=[roles,drift] if kind=='container' else None,return_value=roles),patch.object(host.HostAdapter,'env',return_value=env):
                with self.assertRaises(host.HostHold):host.verify_installed_runtime('a'*40,runner=changed)

    def test_stop_term_once_no_escalation(self):
        calls=[]
        ticks=iter([0, 1, 301, 301])
        adapter=self.adapter(runner=lambda args, **kw:calls.append(args), clock=lambda:next(ticks), sleep=lambda _:None)
        pins={'workers':{r:{'id':str(i)*64, 'state':{'StartedAt':'start'}} for i,r in enumerate(host.QUEUES)}}
        adapter.verify_pins=lambda *a,**kw:None
        adapter.point_alive=lambda:None
        adapter.last_idle=dt.datetime.now(dt.timezone.utc)
        adapter.metadata=lambda:{r:{**v,'state':{'Running':True,'StartedAt':'start'}} for r,v in pins['workers'].items()}
        with self.assertRaisesRegex(host.HostHold,'natural_exit_timeout'):adapter.stop(pins, tuple(host.QUEUES))
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][:3],['docker','kill','--signal=TERM'])
        self.assertNotIn('KILL',' '.join(calls[0]))

    def test_official_fd_expected_revisions(self):
        calls=[]
        adapter=self.adapter(effectful_runner=lambda args,**kw:calls.append((args,kw)))
        adapter.stopped=True
        adapter.verify_pins=lambda *a,**kw:None
        adapter.point_alive=lambda:None
        adapter.last_idle=dt.datetime.now(dt.timezone.utc)
        adapter.official('b'*40,'a'*40,{'base':'a'*40})
        args,kw=calls[0]
        self.assertEqual(args[-6:],['--expected-sha','b'*40,'--expected-base','a'*40,'--supervised-lock-fd','0'])
        self.assertEqual(kw['pass_fds'],(0,))
        self.assertEqual(kw['timeout'],300)

    def test_env_mode_and_owner_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            env=Path(root)/'.env';env.write_text('private');env.chmod(0o644)
            adapter=self.adapter(root=root)
            with self.assertRaisesRegex(host.HostHold,'env_regular_mode600'):adapter.env()
            env.chmod(0o600)
            if env.stat().st_uid != 1000:
                with self.assertRaisesRegex(host.HostHold,'env_owner'):adapter.env()

    def test_pin_environment_change_rejects_before_docker(self):
        adapter=self.adapter()
        adapter.env=lambda:{'sha256':'changed','uid':1000,'gid':1000,'mode':384}
        with self.assertRaisesRegex(host.HostHold,'env_changed'):
            adapter.verify_pins({'env':{'sha256':'old','uid':1000,'gid':1000,'mode':384}})

    def test_point_lease_protocol_owned_child_reaped(self):
        adapter=self.adapter()
        adapter.point_outcome='held'
        script="import sys; print('HELD',flush=True)\nfor line in sys.stdin:\n if 'pg_advisory_unlock' in line: print('RELEASED',flush=True)\n elif line.startswith('THEN'): print('HELD',flush=True)\n elif line.strip()==chr(92)+'q': break"
        adapter.point_process=host.subprocess.Popen([sys.executable,'-u','-c',script],
            stdin=host.subprocess.PIPE,stdout=host.subprocess.PIPE,bufsize=0)
        self.assertEqual(adapter._point_reply(),b'HELD')
        adapter.point_alive()
        receipt=adapter.close()
        self.assertTrue(receipt['point_transport_reaped'])
        self.assertTrue(receipt['release_acknowledged'])
        self.assertEqual(receipt['exit_code'],0)

    def test_point_closed_transport_never_allows_effect(self):
        adapter=self.adapter()
        with self.assertRaisesRegex(host.HostHold,'point_lease_not_live'):adapter.point_alive()

    def test_begin_distinguishes_busy_from_unproved_protocol(self):
        for reply,message in [('BUSY','point_lease_busy'),('HOLD','point_lease_protocol_unproved')]:
            with self.subTest(reply=reply),tempfile.TemporaryDirectory() as root:
                helper=Path(root)/'pos_bridge/services/point_account_session_lock.py'
                helper.parent.mkdir(parents=True);helper.write_text('known helper')
                script="import sys; print("+repr(reply)+",flush=True)\nfor line in sys.stdin:\n if line.strip()==chr(92)+'q': break"
                process=host.subprocess.Popen([sys.executable,'-u','-c',script],
                    stdin=host.subprocess.PIPE,stdout=host.subprocess.PIPE,bufsize=0)
                adapter=self.adapter(root=root,runner=lambda args,**kw:'known helper' if args[0]=='git' else '{"NAME":"test","USER":"postgres","HOST":"db","PORT":"5432"}')
                adapter.database_pin=lambda:{'id':'d'*64,'host':'dbhost','project':'test'}
                with patch.object(host.subprocess,'Popen',return_value=process) as launch:
                    with self.assertRaisesRegex(host.HostHold,message):
                        adapter.begin_point_lease({'base':'a'*40,'peers':{'web':{'id':'b'*64,'project':'test'}}})
                command=launch.call_args.args[0]
                self.assertEqual(command[3],'d'*64)
                self.assertIn('--no-password',command)
                self.assertNotIn('python',command)
                if reply=='BUSY':self.assertEqual(adapter.close()['outcome'],'busy')
                else:
                    with self.assertRaisesRegex(host.HostHold,'point_release_not_clean'):adapter.close()

    def test_effectful_stall_waits_late_child_and_retains_lock(self):
        import fcntl
        import time
        with tempfile.TemporaryDirectory() as root:
            lock=Path(root)/'deploy.lock';marker=Path(root)/'late-mutation'
            fd=os.open(lock,os.O_CREAT|os.O_RDWR,0o600)
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            child="import time,pathlib; time.sleep(.5); pathlib.Path("+repr(str(marker))+").write_text('completed')"
            shell="import subprocess,sys; subprocess.Popen([sys.executable,'-c',"+repr(child)+"],close_fds=False)"
            alarms=[]
            def alarm(code):
                alarms.append(code)
                test="import fcntl,os; f=os.open("+repr(str(lock))+",os.O_RDWR); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)"
                self.assertNotEqual(host.subprocess.run([sys.executable,'-c',test],capture_output=True).returncode,0)
                raise RuntimeError('notification failure must not stop the group')
            started=time.monotonic()
            try:
                with self.assertRaisesRegex(host.HostHold,'effectful_deadline_terminal_hold'):
                    host.wait_effectful([sys.executable,'-c',shell],timeout=.05,pass_fds=(fd,),on_stall=alarm)
                self.assertTrue(marker.exists())
                self.assertGreaterEqual(time.monotonic()-started,.5)
                self.assertEqual(len(alarms),1)
            finally:os.close(fd)

    def test_effectful_drains_excess_and_waits_terminal(self):
        with tempfile.TemporaryDirectory() as root:
            marker=Path(root)/'completed'
            source="import sys,pathlib; sys.stdout.write('x'*(9*1024*1024)); sys.stdout.flush(); pathlib.Path("+repr(str(marker))+").write_text('done')"
            with self.assertRaisesRegex(host.HostHold,'effectful_output_terminal_hold'):
                host.wait_effectful([sys.executable,'-c',source],timeout=5)
            self.assertTrue(marker.exists())

    @unittest.skipUnless(os.getenv('ERP_RELEASE_HOST_PG_TEST') == '51649', 'owned synthetic PG16 opt-in')
    def test_point_source_real_postgres_lease(self):
        import psycopg2
        import uuid
        db=psycopg2.connect(host='127.0.0.1',port=51649,dbname='erp_publisher_test',
                            user='postgres',password='task-synthetic',connect_timeout=3)
        db.autocommit=True
        key=7532026080700001
        children=[]
        tag='publisher-lease-test-'+uuid.uuid4().hex
        evidence={'postgres_port':51649,'database':'erp_publisher_test','application_name':tag,
                  'source_sha256':host.hashlib.sha256(host.POINT_LEASE_SOURCE.encode()).hexdigest()}
        container='65e5ccba483f8dd6e584fcd91e8939ab9032a3872e7fef0b18031b5ddec0f004'
        metadata=host.json.loads(host.subprocess.check_output(['docker','inspect',container]))[0]
        self.assertEqual(metadata['Config']['Labels']['codex.task'],'telegram-supervised-publisher-20261127')
        self.assertEqual(metadata['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort'],'51649')
        def start():
            adapter=self.adapter()
            adapter.point_process=host.subprocess.Popen(['docker','exec','-i',container,'psql','-X','-qAt','-v','ON_ERROR_STOP=1',
                '-h','/var/run/postgresql','-U','postgres','-d','erp_publisher_test'],
                stdin=host.subprocess.PIPE,stdout=host.subprocess.PIPE,stderr=host.subprocess.DEVNULL,bufsize=0)
            adapter.point_process.stdin.write(("SET application_name TO '"+tag+"';\n"+host.POINT_LEASE_SOURCE).encode())
            adapter.point_process.stdin.flush()
            children.append(adapter)
            return adapter
        try:
            with db.cursor() as cursor:
                cursor.execute('SHOW server_version_num')
                evidence['postgres_version_num']=int(cursor.fetchone()[0])
                self.assertEqual(evidence['postgres_version_num']//10000,16)
                cursor.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=%s AND objid=%s AND objsubid=1 AND granted",[key>>32,key&0xffffffff])
                self.assertEqual(cursor.fetchone()[0],0)
            first=start();self.assertEqual(first._point_reply(timeout=25),b'HELD');first.point_outcome='held'
            first.point_alive();evidence['first_ping']='HELD'
            blocked=start();self.assertEqual(blocked._point_reply(timeout=25),b'BUSY');blocked.point_outcome='busy'
            evidence['busy_close']=blocked.close()
            evidence['release_close']=first.close()
            replacement=start();self.assertEqual(replacement._point_reply(timeout=25),b'HELD');replacement.point_outcome='held'
            with db.cursor() as cursor:
                cursor.execute("SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.application_name=%s AND a.datname='erp_publisher_test' AND l.locktype='advisory' AND l.classid=%s AND l.objid=%s AND l.objsubid=1 AND l.granted",[tag,key>>32,key&0xffffffff])
                rows=cursor.fetchall();self.assertEqual(len(rows),1)
                own_pid=rows[0][0];evidence['terminated_owned_backend_pid']=own_pid
                cursor.execute('SELECT pg_terminate_backend(%s)',[own_pid]);self.assertTrue(cursor.fetchone()[0])
            with self.assertRaises(host.HostHold):replacement.point_alive()
            evidence['lost_backend_ping']='REJECTED'
            with self.assertRaisesRegex(host.HostHold,'point_release_not_clean'):replacement.close()
            with db.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name=%s AND l.locktype='advisory'",[tag])
                self.assertEqual(cursor.fetchone()[0],0)
            evidence['all_owned_children_reaped']=all(a.point_process is None for a in children)
            evidence['status']='PASS'
            artifact=Path('/Users/mauricioburgos/.codex/task-artifacts/telegram-supervised-publisher-20261127/point-lease-psql-test.json')
            artifact.parent.mkdir(parents=True,exist_ok=True)
            artifact.write_text(host.json.dumps(evidence,indent=2)+'\n')
            artifact.chmod(0o600)
        finally:
            for adapter in children:
                if adapter.point_process is not None:
                    try:adapter.close()
                    except host.HostHold:pass
            db.close()

    def test_bounded_output_and_timeout(self):
        self.assertEqual(host.bounded([sys.executable,'-c','print("ok")']), 'ok\n')
        with self.assertRaisesRegex(host.HostHold,'command_timeout_child_reaped_descendants_unknown'):
            host.bounded([sys.executable,'-c','import time; time.sleep(3)'],timeout=.05)
        with self.assertRaises(host.HostHold):
            host.bounded([sys.executable,'-c','import os; os.write(1,b"x"*(9*1024*1024))'])


if __name__=='__main__':unittest.main()
