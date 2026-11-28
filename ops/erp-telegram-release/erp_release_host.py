"""Bounded host effects for the canonical supervised publisher.

Locks belong to the controller. No fetch, timer changes, forced worker stop,
rollback or restart fallback is provided here.
"""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import selectors
import shlex
import stat
import subprocess
import tempfile
import time
import threading

ROOT = Path('/opt/pastelerias-erp')
LIMIT = 8 * 1024 * 1024
QUEUES = {'worker': 'celery', 'worker_notificaciones': 'notificaciones',
          'worker_sat': 'sat', 'worker_recetas': 'recipes',
          'worker_logistica_point': 'logistica_point',
          'worker_hours_control': 'hours_control', 'worker_hours_publish': 'hours_publish'}
PEERS = ('web', 'beat', 'recetas_watchdog')
SUPPORTED_TRANSPORT = {'celery': '5.4.0', 'kombu': '5.6.2'}


class HostHold(RuntimeError):
    pass


def require(value, code):
    if not value:
        raise HostHold(code)


def bounded(argv, *, input=None, timeout=30, pass_fds=()):
    """Bound both output files before execution; never expose stderr secrets."""
    def cap():
        resource.setrlimit(resource.RLIMIT_FSIZE, (LIMIT, LIMIT))
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        try:
            result = subprocess.run(argv, input=input, stdout=out, stderr=err,
                                    timeout=timeout, pass_fds=pass_fds, preexec_fn=cap)
        except subprocess.TimeoutExpired:
            raise HostHold('command_timeout_child_reaped_descendants_unknown') from None
        size = out.tell() + err.tell()
        require(out.tell() < LIMIT and err.tell() < LIMIT and size <= LIMIT, 'command_output_limit')
        require(result.returncode == 0, 'command_failed')
        out.seek(0)
        return out.read().decode('utf-8')


# A container-side supervisor reaps its read-only child even if docker exec's
# client disappears. Source is sent on stdin; no file is written to the VPS.
COLLECTOR_RUNNER = r'''
import io,json,resource,subprocess,sys,tempfile
payload=json.loads(sys.stdin.buffer.read(8*1024*1024+1))
loader="import sys,types\n"
for name in ('erp_release_preservation','erp_release_snapshot'):
 source=payload['sources'][name]
 loader+="m=types.ModuleType("+repr(name)+");sys.modules["+repr(name)+"]=m;exec(compile("+repr(source)+","+repr(name)+",'exec'),m.__dict__)\n"
loader+="sys.modules['erp_release_snapshot'].main()\n"
def cap():
 # Bound Redis response allocation before imports/reads inside the web cgroup.
 # 192 MiB allows the read-only Django/Celery imports plus the 4 MiB wire budget;
 # this is an admission ceiling, not permission to grow with a broker payload.
 resource.setrlimit(resource.RLIMIT_AS,(192*1024*1024,192*1024*1024))
 resource.setrlimit(resource.RLIMIT_CPU,(25,25))
 resource.setrlimit(resource.RLIMIT_FSIZE,(8*1024*1024,8*1024*1024))
with tempfile.TemporaryFile() as out,tempfile.TemporaryFile() as err:
 child=subprocess.Popen([sys.executable,'-c',loader],stdin=subprocess.PIPE,stdout=out,stderr=err,preexec_fn=cap)
 try:child.communicate(json.dumps(payload['request']).encode(),timeout=20)
 except subprocess.TimeoutExpired:
  child.kill();child.wait();raise SystemExit(3)
 if child.returncode or out.tell()>=8*1024*1024 or err.tell()>=8*1024*1024 or out.tell()+err.tell()>8*1024*1024:raise SystemExit(4)
 out.seek(0);result=json.load(out)
 result['reader']={'reaped':True,'exit_code':0,'killed':False}
 print(json.dumps(result))
'''


DATABASE_CONFIG_SOURCE = """import contextlib,json,os
os.environ.setdefault('DJANGO_SETTINGS_MODULE','config.settings')
with open(os.devnull,'w') as quiet,contextlib.redirect_stdout(quiet):
 from django.conf import settings
 value={k:str(settings.DATABASES['default'].get(k,'')) for k in ('NAME','USER','HOST','PORT')}
print(json.dumps(value))
"""


POINT_KEY = 7532026080700001
POINT_LEASE_SOURCE = r"""SET statement_timeout TO '3s';
SELECT pg_backend_pid() AS lease_pid, backend_start::text AS lease_start
FROM pg_stat_activity WHERE pid=pg_backend_pid() \gset
SELECT CASE WHEN pg_try_advisory_lock(7532026080700001) THEN 'HELD' ELSE 'BUSY' END;
"""
POINT_PING = f"""SELECT CASE WHEN pg_backend_pid() = :lease_pid
AND (SELECT backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()) = :'lease_start'
AND EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory'
 AND pid=pg_backend_pid() AND granted AND mode='ExclusiveLock'
 AND classid={POINT_KEY >> 32} AND objid={POINT_KEY & 0xffffffff} AND objsubid=1
 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))
THEN 'HELD' ELSE 'HOLD' END;
"""
POINT_RELEASE = "SELECT CASE WHEN pg_advisory_unlock(7532026080700001) THEN 'RELEASED' ELSE 'HOLD' END;\n\\q\n"


def owned_group_live(pgid, *, reap=False):
    """Linux zombies are terminal; reap only adoptable children in this group."""
    if Path('/proc/self/stat').exists():
        live=False
        for path in Path('/proc').iterdir():
            if not path.name.isdigit():continue
            try:
                raw=(path/'stat').read_text()
                fields=raw[raw.rfind(')')+2:].split()
                if int(fields[2])==pgid and fields[0] not in ('Z','X'):live=True
            except (FileNotFoundError,ProcessLookupError):continue
            except (PermissionError,ValueError,IndexError):return True
        while reap:
            try:
                if os.waitpid(-pgid,os.WNOHANG)[0]==0:break
            except ChildProcessError:break
        return live
    try:os.killpg(pgid,0);return True
    except ProcessLookupError:return False


def wait_effectful(argv, *, timeout=300, pass_fds=(), on_stall=None, on_tick=None):
    """A deadline raises an alarm, never signals a mutating process tree.

    Wait for the original shell AND its session process group. Readers drain
    pipes beyond the private receipt cap, preventing SIGXFSZ or pipe deadlock.
    A stalled/failed operation becomes HOLD only after the group is terminal.
    """
    started=time.monotonic()
    proc=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,pass_fds=pass_fds,start_new_session=True)
    stalled=False
    faults=set()
    counts=[0,0]
    def alarm(code):
        if on_stall is not None:
            try:on_stall(code)
            except BaseException:pass  # Notification failure cannot abort mutation.
    with tempfile.TemporaryFile() as out,tempfile.TemporaryFile() as err:
        def drain(stream,destination,index):
            try:
                while True:
                    chunk=stream.read(65536)
                    if not chunk:break
                    available=max(0,LIMIT//2-counts[index])
                    if available:
                        try:destination.write(chunk[:available])
                        except BaseException:faults.add('receipt_write_failed')
                    counts[index]+=len(chunk)
            except BaseException:
                faults.add('output_reader_failed')
            finally:stream.close()
        readers=[threading.Thread(target=drain,args=(stream,target,i))
                 for i,(stream,target) in enumerate(((proc.stdout,out),(proc.stderr,err)))]
        for reader in readers:reader.start()
        next_tick=started
        while True:
            status=proc.poll()  # Reap the direct shell before checking its group.
            group_live=owned_group_live(proc.pid,reap=status is not None)
            if status is not None and not group_live:break
            now=time.monotonic()
            if not stalled and now-started>=timeout:
                stalled=True;alarm('effectful_deadline_waiting_owned_group')
            if on_tick is not None and now>=next_tick:
                try:on_tick()
                except BaseException:
                    if 'lease_lost' not in faults:alarm('effectful_lease_lost_waiting_owned_group')
                    faults.add('lease_lost')
                next_tick=now+5
            time.sleep(.1)
        for reader in readers:reader.join()
        require(not stalled,'effectful_deadline_terminal_hold')
        require(not faults,'effectful_supervision_terminal_hold')
        require(sum(counts)<=LIMIT and all(n<LIMIT//2 for n in counts),'effectful_output_terminal_hold')
        require(proc.returncode==0,'effectful_failed_terminal_hold')
        out.seek(0)
        return out.read().decode('utf-8')


class HostAdapter:
    def __init__(self, deploy_fd, *, lease_check=None, runner=bounded, root=ROOT,
                 clock=time.monotonic, sleep=time.sleep, effectful_runner=wait_effectful, on_stall=None):
        self.deploy_fd = deploy_fd
        self.lease_check = lease_check
        self.run = runner
        self.effectful_run = effectful_runner
        self.on_stall = on_stall
        self.root = Path(root)
        self.clock, self.sleep = clock, sleep
        self.stopped = False
        self.point_process = None
        self.point_outcome = None
        self.database_pins = None
        self.last_idle = None
        self.sources = {name: Path(__file__).with_name(name + '.py').read_text()
                        for name in ('erp_release_preservation', 'erp_release_snapshot')}

    def leases(self):
        require(self.lease_check is not None and self.lease_check() is True, 'shared_leases_unproved')
        require(type(self.deploy_fd) is int and self.deploy_fd >= 0, 'deploy_descriptor')
        os.fstat(self.deploy_fd)

    def _point_reply(self, timeout=5):
        proc = self.point_process
        require(proc is not None and proc.stdout is not None, 'point_lease_missing')
        deadline = self.clock() + timeout
        data = bytearray()
        with selectors.DefaultSelector() as selector:
            selector.register(proc.stdout, selectors.EVENT_READ)
            while self.clock() < deadline:
                require(selector.select(max(0, deadline-self.clock())), 'point_lease_timeout')
                byte = os.read(proc.stdout.fileno(), 1)
                require(byte, 'point_lease_closed')
                data.extend(byte)
                require(len(data) <= 32, 'point_protocol_limit')
                if byte == b'\n':return bytes(data).strip()
        raise HostHold('point_lease_timeout')

    def database_pin(self):
        identifier=self.compose('ps','--all','-q','db').strip()
        require(re.fullmatch('[0-9a-f]{64}',identifier),'database_coverage')
        fmt='{"id":{{json .Id}},"image":{{json .Image}},"host":{{json .Config.Hostname}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},"project":{{json (index .Config.Labels "com.docker.compose.project")}},"workdir":{{json (index .Config.Labels "com.docker.compose.project.working_dir")}},"running":{{json .State.Running}},"started_at":{{json .State.StartedAt}}}'
        value=json.loads(self.run(['docker','inspect','--format',fmt,identifier],timeout=10))
        require(value['service']=='db' and value['workdir']==str(self.root) and value['running'],'database_owner_state')
        return value

    def begin_point_lease(self, pins):
        require(self.point_process is None, 'point_lease_already_started')
        helper = 'pos_bridge/services/point_account_session_lock.py'
        expected = self.run(['git', '-C', str(self.root), 'show', pins['base']+':'+helper], timeout=10).encode()
        require((self.root/helper).read_bytes() == expected, 'point_lock_source_changed')
        database=self.database_pin()
        require(database['project']==pins['peers']['web']['project'],'database_project')
        config=json.loads(self.run(['docker','exec',pins['peers']['web']['id'],'python','-c',DATABASE_CONFIG_SOURCE],timeout=10))
        require(config['HOST']=='db' and config['PORT'] in ('','5432'),'database_socket_identity_unproved')
        require(re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,62}',config['USER']),'database_user')
        require(re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,62}',config['NAME']),'database_name')
        pins['database']=database
        self.database_pins=database
        command=['docker','exec','-i',database['id'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1',
                 '--no-password','-h','/var/run/postgresql','--username',config['USER'],'--dbname',config['NAME']]
        self.point_process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                                             stderr=subprocess.DEVNULL,bufsize=0)
        self.point_process.stdin.write(POINT_LEASE_SOURCE.encode());self.point_process.stdin.flush()
        reply=self._point_reply(timeout=25)
        if reply==b'BUSY':
            self.point_outcome='busy'
            raise HostHold('point_lease_busy')
        require(reply==b'HELD','point_lease_protocol_unproved')
        self.point_outcome='held'

    def point_alive(self):
        proc=self.point_process
        require(proc is not None and proc.poll() is None,'point_lease_not_live')
        try:
            proc.stdin.write(POINT_PING.encode());proc.stdin.flush()
        except (BrokenPipeError,OSError):raise HostHold('point_lease_broken') from None
        require(self._point_reply()==b'HELD','point_lease_not_held')

    def close(self):
        proc=self.point_process
        if proc is None:return {'point_lease_started':False}
        released=False
        try:
            if proc.poll() is None:
                if self.point_outcome=='held':
                    proc.stdin.write(POINT_RELEASE.encode());proc.stdin.flush()
                    released=self._point_reply()==b'RELEASED'
                else:
                    proc.stdin.write(b'\\q\n');proc.stdin.flush()
        except (BrokenPipeError,OSError,HostHold):pass
        finally:
            try:
                proc.stdin.close();proc.wait(timeout=25)
            except subprocess.TimeoutExpired:
                proc.kill();proc.wait()  # Only owned read-only psql transport; never deploy.
                raise HostHold('point_release_uncertain_client_reaped') from None
            finally:
                proc.stdout.close();self.point_process=None
        require(proc.returncode==0 and (released or self.point_outcome=='busy'),'point_release_not_clean')
        return {'point_transport_reaped':True,'exit_code':proc.returncode,
                'release_acknowledged':released,'outcome':self.point_outcome}

    def git(self, *args):
        return self.run(['git', '-C', str(self.root), *args], timeout=10).strip()

    def compose(self, *args):
        return self.run(['docker', 'compose', '-f', str(self.root / 'docker-compose.yml'), *args], timeout=10)

    def env(self):
        path = self.root / '.env'
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            value = os.fstat(fd)
            require(stat.S_ISREG(value.st_mode) and stat.S_IMODE(value.st_mode) == 0o600,
                    'env_regular_mode600')
            require(value.st_uid == value.st_gid == 1000, 'env_owner')
            digest = hashlib.sha256()
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:break
                digest.update(chunk)
            after = os.stat(path, follow_symlinks=False)
            require(all(getattr(after, k) == getattr(value, k) for k in ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_gid', 'st_size', 'st_mtime_ns', 'st_ctime_ns')), 'env_changed_during_capture')
            return {'sha256': digest.hexdigest(), 'uid': value.st_uid,
                    'gid': value.st_gid, 'mode': stat.S_IMODE(value.st_mode)}
        finally:
            os.close(fd)

    def metadata(self):
        ids = self.compose('ps', '--all', '-q', *QUEUES, *PEERS).split()
        require(len(ids) == len(set(ids)) == 10 and all(re.fullmatch('[0-9a-f]{64}', i) for i in ids), 'container_coverage')
        # Never retrieve Config.Env or credentials in inspect output.
        fmt = '{"id":{{json .Id}},"image":{{json .Image}},"host":{{json .Config.Hostname}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},"project":{{json (index .Config.Labels "com.docker.compose.project")}},"workdir":{{json (index .Config.Labels "com.docker.compose.project.working_dir")}},"cmd":{{json .Config.Cmd}},"state":{"Running":{{json .State.Running}},"StartedAt":{{json .State.StartedAt}},"ExitCode":{{json .State.ExitCode}},"OOMKilled":{{json .State.OOMKilled}}}}'
        raw = self.run(['docker', 'inspect', '--format', fmt, *ids], timeout=10)
        items = [json.loads(line) for line in raw.splitlines()]
        require(len(items) == 10 and {v['service'] for v in items} == set(QUEUES) | set(PEERS), 'role_coverage')
        projects = set()
        for value in items:
            require(value['workdir'] == str(self.root), 'container_owner')
            projects.add(value['project'])
            tokens = shlex.split(' '.join(value.pop('cmd')))
            queues = []
            for i, token in enumerate(tokens):
                if token in ('-Q', '--queues'):queues.extend(tokens[i+1].split(','))
                elif token.startswith('--queues='):queues.extend(token.split('=', 1)[1].split(','))
            if value['service'] in QUEUES:
                require(queues == [QUEUES[value['service']]], 'queue_ownership')
            value['queues'] = queues
            state = value['state']
            value['state'] = {k: state[k] for k in ('Running', 'StartedAt', 'ExitCode', 'OOMKilled')}
            require(not state['OOMKilled'], 'oom')
        require(len(projects) == 1 and None not in projects, 'compose_project')
        return {v['service']: v for v in items}

    @staticmethod
    def profile(paths):
        require(bool(paths), 'empty_change')
        for path in paths:
            require(Path(path).name not in {'package.json', 'package-lock.json', 'pnpm-lock.yaml', 'yarn.lock', 'Pipfile', 'Pipfile.lock', 'setup.py', 'setup.cfg', 'uv.lock', 'poetry.lock'}, 'routine_dependency_excluded')
            require(not (path.startswith(('ops/', 'scripts/', 'config/', '.github/')) or
                         '/migrations/' in path or path == 'pos_bridge/services/point_account_session_lock.py' or path.startswith(('Dockerfile', 'docker-compose', 'requirements', 'pyproject', 'uv.lock', 'poetry.lock', '.env'))),
                    'routine_profile_excluded')
        require(any(path.endswith('.py') for path in paths), 'routine_requires_python_restart')

    def preflight(self, row):
        self.leases()
        base, head = row['base_sha'], row['head_sha']
        require(all(re.fullmatch('[0-9a-f]{40}', v) for v in (base, head)), 'revision')
        require(self.git('rev-parse', 'HEAD') == base and not self.git('status', '--porcelain'), 'base_clean')
        self.git('cat-file', '-e', head + '^{commit}')  # Missing objects HOLD; never fetch here.
        paths = self.git('diff', '--name-only', base, head).splitlines()
        self.profile(paths)
        values = self.metadata()
        require(all(v['state']['Running'] for v in values.values()), 'initial_running')
        env = self.env()
        for value in values.values():
            self.run(['docker', 'exec', '--user', '1000:1000', value['id'], 'python', '-c',
                      "import os; assert os.geteuid()==1000; f=open('/app/.env','rb'); f.read(1); f.close()"], timeout=10)
        workers = {k: values[k] for k in QUEUES}
        pins = {'base': base, 'head': head, 'roles': list(QUEUES), 'workers': workers,
                'peers': {k: values[k] for k in PEERS}, 'env': env,
                'nodes': [QUEUES[k] + '@' + workers[k]['host'] for k in QUEUES],
                'paths': paths, 'collector_sha256': {k: hashlib.sha256(v.encode()).hexdigest() for k,v in self.sources.items()}}
        self.begin_point_lease(pins)
        return pins

    def snapshot(self, pins, before=None):
        self.leases()
        require(pins['collector_sha256'] == {k: hashlib.sha256(v.encode()).hexdigest() for k,v in self.sources.items()}, 'collector_revision')
        payload = {'sources': self.sources, 'request': {'before': before, 'nodes': [] if self.stopped else pins['nodes']}}
        data = json.dumps(payload).encode()
        require(len(data) <= LIMIT, 'collector_input_limit')
        result = json.loads(self.run(['docker', 'exec', '-i', pins['peers']['web']['id'], 'python', '-c', COLLECTOR_RUNNER], input=data, timeout=30))
        require(result.get('reader') == {'reaped': True, 'exit_code': 0, 'killed': False}, 'collector_reap')
        require(result.get('versions') == SUPPORTED_TRANSPORT, 'collector_versions_unvalidated')
        return result

    def idle(self, pins, cut):
        nodes = pins['nodes']
        require(len(nodes) == len(set(nodes)) == 7 and {n.split('@')[0] for n in nodes} == set(QUEUES.values()), 'node_coverage')
        require(set(cut.get('replies', {})) == {'ping', 'active', 'reserved', 'scheduled'}, 'inspect_phases')
        for phase, replies in cut['replies'].items():
            require(set(replies) == set(nodes), 'inspect_coverage')
            require(all(v == ({'ok': 'pong'} if phase == 'ping' else []) for v in replies.values()), 'worker_busy')
        now = dt.datetime.now(dt.timezone.utc)
        self.last_idle = dt.datetime.fromisoformat(cut['inspect_started_at'])
        require(0 <= (now-self.last_idle).total_seconds() < 20, 'inspect_stale')

    def verify_pins(self, pins, stopped=()):
        self.leases()
        require(self.env() == pins['env'], 'env_changed')
        require(self.git('rev-parse', 'HEAD') == pins['base'] and not self.git('status', '--porcelain'), 'base_changed')
        require(self.database_pin()==pins['database'],'database_changed')
        values = self.metadata()
        for role, old in {**pins['workers'], **pins['peers']}.items():
            new = values[role]
            require(all(new[k] == old[k] for k in ('id', 'image', 'host', 'service', 'project', 'workdir', 'queues')), 'identity_changed')
            require(new['state']['StartedAt'] == old['state']['StartedAt'], 'start_changed')
            require(new['state']['Running'] == (role not in stopped), 'worker_state')
            if role in stopped:require(new['state']['ExitCode'] == 0, 'natural_exit_nonzero')
        return values

    def stop(self, pins, roles):
        require(set(roles) == set(QUEUES) and len(roles) == 7, 'stop_coverage')
        self.verify_pins(pins)
        self.point_alive()
        require(self.last_idle is not None and 0 <= (dt.datetime.now(dt.timezone.utc)-self.last_idle).total_seconds() < 20, 'inspect_stale_before_term')
        ids = [pins['workers'][role]['id'] for role in QUEUES]
        self.run(['docker', 'kill', '--signal=TERM', *ids], timeout=15)
        deadline = self.clock() + 300
        while self.clock() < deadline:
            self.leases()
            values = self.metadata()
            require(all(values[r]['id'] == pins['workers'][r]['id'] and values[r]['state']['StartedAt'] == pins['workers'][r]['state']['StartedAt'] for r in QUEUES), 'stop_identity_changed')
            if all(not values[r]['state']['Running'] for r in QUEUES):
                self.verify_pins(pins, stopped=tuple(QUEUES))
                self.stopped = True
                return
            self.sleep(min(2, max(0, deadline-self.clock())))
        raise HostHold('natural_exit_timeout_no_further_signal')

    def official(self, release, base, pins):
        require(re.fullmatch('[0-9a-f]{40}', release) and base == pins['base'], 'official_binding')
        require(self.stopped, 'workers_not_stopped')
        self.verify_pins(pins, stopped=tuple(QUEUES))
        self.point_alive()
        self.effectful_run(['bash', str(self.root / 'scripts/deploy_web_safe.sh'), '--expected-sha', release,
                  '--expected-base', base, '--supervised-lock-fd', str(self.deploy_fd)],
                 timeout=300, pass_fds=(self.deploy_fd,), on_stall=self.on_stall, on_tick=self.point_alive)


    def verify_runtime(self, release, pins):
        self.leases()
        require(self.git('rev-parse', 'HEAD') == release and not self.git('status', '--porcelain'), 'runtime_checkout')
        require(self.env() == pins['env'], 'runtime_env')
        values = self.metadata()
        require(all(v['state']['Running'] for v in values.values()), 'runtime_running')
        require(values['web']['state']['StartedAt'] > pins['peers']['web']['state']['StartedAt'], 'runtime_web_not_restarted')
        require(self.database_pin()==pins['database'],'runtime_database_changed')
        self.point_alive()
        paths = [p for p in pins['paths'] if p.endswith('.py')]
        expected = {}
        for path in paths:
            # Deleted files must actually be absent in each running consumer.
            if self.git('ls-tree', release, '--', path):
                expected[path] = hashlib.sha256(self.run(['git', '-C', str(self.root), 'show', release+':'+path], timeout=10).encode()).hexdigest()
            else:expected[path] = None
        code = "import hashlib,json,pathlib,sys; expected=json.loads(sys.stdin.read()); result={p:(hashlib.sha256(pathlib.Path('/app',p).read_bytes()).hexdigest() if pathlib.Path('/app',p).is_file() else None) for p in expected}; print(json.dumps(result))"
        for role in (*QUEUES, 'web'):
            old = (pins['workers'] | pins['peers'])[role]
            require(values[role]['state']['StartedAt'] > old['state']['StartedAt'], 'runtime_not_restarted')
            observed = json.loads(self.run(['docker', 'exec', '-i', values[role]['id'], 'python', '-c', code], input=json.dumps(expected).encode(), timeout=10))
            require(observed == expected, 'runtime_sources')
        return {'release': release, 'consumer_verified': True, 'runtime': values,
                'source_hashes': expected, 'functional_qa_required': True}


INSTALLED_SOURCE = 'ops/erp-telegram-release/erp_release_worker.py'
INSTALLED_READ_SOURCE = """import hashlib,json,os,pathlib,stat
assert os.geteuid()==os.getegid()==1000
p=pathlib.Path('/app/.env')
fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
try:
 before=os.fstat(fd)
 assert stat.S_ISREG(before.st_mode) and before.st_uid==before.st_gid==1000 and stat.S_IMODE(before.st_mode)==0o600
 assert before.st_size<=1048576
 with os.fdopen(fd,'rb',closefd=False) as stream: digest=hashlib.sha256(stream.read(1048577)).hexdigest()
 after=os.stat(p,follow_symlinks=False)
 assert all(getattr(before,k)==getattr(after,k) for k in ('st_dev','st_ino','st_mode','st_uid','st_gid','st_size','st_mtime_ns','st_ctime_ns'))
 env={'sha256':digest,'uid':before.st_uid,'gid':before.st_gid,'mode':stat.S_IMODE(before.st_mode)}
finally:os.close(fd)
source=pathlib.Path('/app/ops/erp-telegram-release/erp_release_worker.py')
assert source.is_file() and not source.is_symlink() and source.stat().st_size<=4194304
print(json.dumps({'env':env,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest()}))
"""


def verify_installed_runtime(release, runner=bounded):
    """Fresh, read-only bootstrap evidence; no locks, SQL, RPC or service changes.

    Proves stable running container identities, readable private environment and
    versioned worker source inside consumers. It does not claim task execution
    or that an arbitrary Python module is already imported into a process.
    """
    require(isinstance(release,str) and re.fullmatch('[0-9a-f]{40}',release),'installed_release')
    deadline=time.monotonic()+120
    def limited(argv, **kwargs):
        remaining=deadline-time.monotonic()
        require(remaining>0,'installed_verification_deadline')
        kwargs['timeout']=min(kwargs.get('timeout',30),remaining)
        return runner(argv,**kwargs)
    adapter=HostAdapter(-1,runner=limited,root=ROOT)
    require(adapter.git('rev-parse','HEAD')==release and not adapter.git('status','--porcelain'),'installed_checkout')
    source=limited(['git','-C',str(ROOT),'show',release+':'+INSTALLED_SOURCE],timeout=10)
    require(source and len(source.encode())<=4194304,'installed_worker_source')
    expected=hashlib.sha256(source.encode()).hexdigest()
    roles=adapter.metadata()
    require(set(roles)==set(QUEUES)|set(PEERS) and len(roles)==10,'installed_role_coverage')
    require(all(v['state']['Running'] and not v['state']['OOMKilled'] for v in roles.values()),'installed_roles_not_running')
    env=adapter.env()
    consumers={}
    # All ten identities must read the exact private environment. The source
    # check on beat/watchdog is additional to the required web+seven workers.
    for role,value in roles.items():
        observed=json.loads(limited(['docker','exec','--user','1000:1000',value['id'],
                                     'python','-c',INSTALLED_READ_SOURCE],timeout=10))
        require(observed.get('env')==env,'installed_environment_changed')
        require(observed.get('source_sha256')==expected,'installed_worker_source_changed')
        consumers[role]=expected
    require(adapter.metadata()==roles,'installed_container_drift')
    require(adapter.env()==env,'installed_host_environment_changed')
    require(adapter.git('rev-parse','HEAD')==release and not adapter.git('status','--porcelain'),'installed_checkout_changed')
    require(time.monotonic()<deadline,'installed_verification_deadline')
    return {'release':release,'verified':True,'roles':roles,'source_hashes':{INSTALLED_SOURCE:expected},
            'source_consumers':consumers,'env':env,'captured_at':dt.datetime.now(dt.timezone.utc).isoformat(),
            'scope':'running_consumers_and_versioned_source_no_task_execution_claim'}
