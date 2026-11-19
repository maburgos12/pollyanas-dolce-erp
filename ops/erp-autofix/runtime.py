"""Privileged launch boundaries; model and candidate code never inherit service secrets."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import pwd
import re
import resource
import signal
import subprocess
import time
import uuid

JOB = re.compile(r'autofix-[0-9a-f]{16}-[1-3]')
MODEL_ROOT = Path('/var/lib/erp-codex/jobs')
CODEX_HOME = Path('/var/lib/erp-codex/home')
CODEX = '/usr/local/libexec/erp-codex-0.144.1'
DOCKER = '/usr/bin/docker'
CLEAN_ENV = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}


class RuntimeFailure(RuntimeError):
    def __init__(self, reason_code):
        self.reason_code = reason_code
        super().__init__(reason_code)


def _private_file(path, content):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
        stream.write(content)


def _limited(command, log_path, timeout=600, limit=4 * 1024 * 1024, stderr_path=None):
    def limits():
        resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
    with ExitStack() as stack:
        def stream(path):
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            return stack.enter_context(os.fdopen(descriptor, 'wb'))
        output = stream(log_path)
        errors = stream(stderr_path) if stderr_path else output
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output, stderr=errors,
                                   env=CLEAN_ENV, start_new_session=True, preexec_fn=limits)
        try:
            code = process.wait(timeout=timeout)
        except BaseException as error:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            if isinstance(error, subprocess.TimeoutExpired):
                raise RuntimeFailure('execution_timeout') from None
            raise
    if any(path and path.stat().st_size >= limit for path in (log_path, stderr_path)):
        raise RuntimeFailure('output_limit')
    return subprocess.CompletedProcess(command, code, b'', b'')


def _stop_model_unit(unit):
    try:
        subprocess.run(['/usr/bin/systemctl', 'stop', unit], env=CLEAN_ENV,
                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=30, check=False)
        state = subprocess.run(['/usr/bin/systemctl', 'is-active', '--quiet', unit], env=CLEAN_ENV,
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=10, check=False)
        if state.returncode not in (3, 4):
            raise RuntimeFailure('model_cleanup_failed')
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeFailure('model_cleanup_failed') from None


def _json(content):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate key')
            result[key] = value
        return result
    def constant(value):
        raise ValueError('non-finite JSON')
    return json.loads(content, object_pairs_hook=unique, parse_constant=constant)


def _validate_schema(value, schema):
    kind = schema.get('type')
    valid = {'object': lambda x: type(x) is dict, 'array': lambda x: type(x) is list,
             'string': lambda x: type(x) is str, 'integer': lambda x: type(x) is int,
             'number': lambda x: type(x) in (int, float), 'boolean': lambda x: type(x) is bool,
             'null': lambda x: x is None}
    if kind not in valid or not valid[kind](value):
        raise RuntimeFailure('model_schema_invalid')
    if 'enum' in schema and value not in schema['enum']:
        raise RuntimeFailure('model_schema_invalid')
    if kind == 'object':
        properties = schema.get('properties', {})
        if set(schema.get('required', [])) - set(value):
            raise RuntimeFailure('model_schema_invalid')
        if schema.get('additionalProperties') is not False or set(value) - set(properties):
            raise RuntimeFailure('model_schema_invalid')
        for key, item in value.items():
            _validate_schema(item, properties[key])
    elif kind == 'array':
        if len(value) > schema.get('maxItems', 1024) or len(value) < schema.get('minItems', 0):
            raise RuntimeFailure('model_schema_invalid')
        for item in value:
            _validate_schema(item, schema['items'])
    elif kind == 'string' and (len(value) > schema.get('maxLength', 128 * 1024) or
                              len(value) < schema.get('minLength', 0)):
        raise RuntimeFailure('model_schema_invalid')


def _validate_events(content):
    completed = 0
    try:
        for line in content.splitlines():
            event = _json(line)
            kind = event['type']
            if kind == 'turn.completed':
                completed += 1
            elif kind in {'thread.started', 'turn.started'}:
                continue
            elif kind in {'item.started', 'item.updated', 'item.completed'}:
                if event['item']['type'] not in {'agent_message', 'reasoning'}:
                    raise RuntimeFailure('model_tool_event')
            else:
                raise RuntimeFailure('model_event_invalid')
    except (ValueError, KeyError, TypeError):
        raise RuntimeFailure('model_event_invalid') from None
    if completed != 1:
        raise RuntimeFailure('model_turn_incomplete')


def model(job_dir, phase, prompt, schema):
    job_dir = Path(job_dir)
    if not JOB.fullmatch(job_dir.name) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,31}', phase):
        raise RuntimeFailure('invalid_model_identity')
    account = pwd.getpwnam('erp-codex')
    model_parent = MODEL_ROOT / job_dir.name
    if MODEL_ROOT.is_symlink() or model_parent.is_symlink():
        raise RuntimeFailure('invalid_model_directory')
    model_parent.mkdir(mode=0o711, exist_ok=True)
    if model_parent.stat().st_uid != 0:
        raise RuntimeFailure('invalid_model_directory')
    os.chmod(model_parent, 0o711)
    directory = model_parent / phase
    directory.mkdir(mode=0o700)
    os.chown(directory, account.pw_uid, account.pw_gid)
    for name, content in [('prompt.txt', prompt), ('schema.json', json.dumps(schema))]:
        path = directory / name
        _private_file(path, content)
        os.chown(path, account.pw_uid, account.pw_gid)
    # The wrapper is root-owned; only its input/output files belong to the model user.
    wrapper = model_parent / ('run-' + phase + '.sh')
    _private_file(wrapper, '#!/bin/sh\nexec /usr/bin/flock -n /var/lib/erp-codex/home/session.lock '
                  + CODEX + ' exec --ignore-user-config --ignore-rules --ephemeral --skip-git-repo-check --sandbox read-only '
                  '--json --color never --disable shell_tool --disable apps --disable plugins '
                  '-c approval_policy=\'"never"\' -c forced_login_method=\'"chatgpt"\' -c web_search=\'"disabled"\' '
                  '--output-schema ' + str(directory / 'schema.json') + ' '
                  '--output-last-message ' + str(directory / 'result.json') + ' - '
                  '< ' + str(directory / 'prompt.txt') + '\n')
    os.chmod(wrapper, 0o755)
    command = ['/usr/bin/systemd-run', '--quiet', '--wait', '--collect', '--pipe',
               '--unit=erp-autofix-model-' + job_dir.name + '-' + phase,
               '--property=User=erp-codex', '--property=Group=erp-codex',
               '--property=ProtectSystem=strict', '--property=ProtectHome=yes',
               '--property=PrivateTmp=yes', '--property=NoNewPrivileges=yes',
               '--property=InaccessiblePaths=/opt /srv /root /home /etc/erp-autofix /var/lib/docker /run/docker.sock /var/lib/erp-autofix /var/lib/erp-observer',
               '--property=ReadWritePaths=' + str(CODEX_HOME) + ' ' + str(directory),
               '--property=CPUQuota=100%', '--property=MemoryMax=768M',
               '--property=RuntimeMaxSec=600', '--property=TasksMax=64',
               '--property=LimitFSIZE=16777216', '--property=WorkingDirectory=' + str(directory),
               '/usr/bin/env', '-i', 'PATH=/usr/bin:/bin', 'HOME=' + str(CODEX_HOME),
               'CODEX_HOME=' + str(CODEX_HOME), 'LANG=C.UTF-8', '/bin/sh', str(wrapper)]
    try:
        result = _limited(command, job_dir / (phase + '-model-events.jsonl'), timeout=620,
                          stderr_path=job_dir / (phase + '-model-stderr.log'))
    finally:
        _stop_model_unit('erp-autofix-model-' + job_dir.name + '-' + phase)
    if result.returncode:
        raise RuntimeFailure('model_execution_failed')
    try:
        events = (job_dir / (phase + '-model-events.jsonl')).read_text(encoding='utf-8')
        _validate_events(events)
        path = directory / 'result.json'
        if path.is_symlink() or path.stat().st_size > 128 * 1024:
            raise RuntimeFailure('model_output_limit')
        value = _json(path.read_text(encoding='utf-8'))
        if type(value) is not dict:
            raise RuntimeFailure('model_schema_invalid')
        _validate_schema(value, schema)
        return value
    except (OSError, ValueError, UnicodeError):
        raise RuntimeFailure('model_result_invalid') from None


def validate_arguments(arguments):
    if not isinstance(arguments, list) or not arguments or any(type(x) is not str for x in arguments):
        raise RuntimeFailure('invalid_test_arguments')
    if arguments in (['manage.py', 'check'], ['manage.py', 'migrate', '--noinput'],
                     ['manage.py', 'migrate', '--check'],
                     ['-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_*.py']):
        return
    if (len(arguments) == 7 and arguments[:6] == ['-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p']
            and re.fullmatch(r'test_[A-Za-z0-9_]+\.py', arguments[6])):
        return
    if (len(arguments) >= 3 and arguments[:2] == ['manage.py', 'test'] and
            all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*', x)
                for x in arguments[2:])):
        return
    raise RuntimeFailure('invalid_test_arguments')


def _docker(arguments):
    result = subprocess.run([DOCKER, *arguments], env=CLEAN_ENV, stdin=subprocess.DEVNULL,
                            capture_output=True, timeout=30, check=False)
    if result.returncode:
        raise RuntimeFailure('docker_command_failed')
    return result.stdout.decode('utf-8').strip()


def _inspect(identifier):
    try:
        result = json.loads(_docker(['inspect', identifier]))
        if len(result) != 1:
            raise ValueError()
        return result[0]
    except (ValueError, TypeError):
        raise RuntimeFailure('docker_inspection_invalid') from None


class Sandbox:
    def __init__(self, worktree, jobdir, tools_image, pg_image):
        self.worktree = Path(worktree)
        self.job_dir = Path(jobdir)
        self.job_id = self.job_dir.name
        self.tools_image = tools_image
        self.pg_image = pg_image
        self.pg_id = None
        self.socket = self.job_dir / 'socket'

    def _labels(self):
        return ['--label=codex.task=' + self.job_id, '--label=codex.owner=erp-autofix']

    def _owned(self, identifier):
        item = _inspect(identifier)
        labels = item.get('Config', {}).get('Labels', {}) or {}
        if item.get('Id') != identifier or labels.get('codex.task') != self.job_id or labels.get('codex.owner') != 'erp-autofix':
            raise RuntimeFailure('resource_ownership_mismatch')
        return item

    def __enter__(self):
        if (not JOB.fullmatch(self.job_id) or self.job_dir.is_symlink() or self.worktree.is_symlink()
                or not self.job_dir.is_absolute() or not self.worktree.is_absolute()
                or ':' in str(self.job_dir) or ':' in str(self.worktree)
                or any(not isinstance(image, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/:@-]*', image)
                       for image in (self.tools_image, self.pg_image))):
            raise RuntimeFailure('invalid_sandbox_identity')
        self._plan('planned-resource.json', 'erp-autofix-pg-' + self.job_id, self.pg_image,
                   [{'type': 'bind', 'source': str(self.socket), 'target': '/pgsocket', 'read_only': False}],
                   {'/var/lib/postgresql/data': 'rw,size=512m,mode=0700', '/tmp': 'rw,size=64m,mode=1777'})
        self.socket.mkdir(mode=0o1777)
        os.chmod(self.socket, 0o1777)
        command = ['create', '--name=erp-autofix-pg-' + self.job_id, *self._labels(),
                   '--network=none', '--memory=512m', '--cpus=0.5', '--pids-limit=64',
                   '--tmpfs=/var/lib/postgresql/data:rw,size=512m,mode=0700',
                   '--tmpfs=/tmp:rw,size=64m,mode=1777', '-v', str(self.socket) + ':/pgsocket:rw',
                   '-e', 'POSTGRES_DB=pastelerias_erp', '-e', 'POSTGRES_USER=postgres',
                   '-e', 'POSTGRES_PASSWORD=postgres', self.pg_image, 'postgres',
                   '-c', 'listen_addresses=', '-c', 'unix_socket_directories=/var/run/postgresql,/pgsocket']
        self.pg_id = _docker(command)
        if not re.fullmatch(r'[0-9a-f]{64}', self.pg_id):
            raise RuntimeFailure('invalid_container_id')
        try:
            pg = self._owned(self.pg_id)
            self._record('resources-before.json', pg)
            self._validate_policy(pg, candidate=False)
            _docker(['start', self.pg_id])
            for _ in range(30):
                if self._postgres_ready():
                    return self
                time.sleep(1)
            raise RuntimeFailure('postgres_not_ready')
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def _postgres_ready(self):
        # initdb's temporary server also accepts connections; only PID 1 is final.
        pid = subprocess.run([DOCKER, 'exec', self.pg_id, 'head', '-n', '1',
                              '/var/lib/postgresql/data/postmaster.pid'],
                             env=CLEAN_ENV, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, timeout=10, check=False)
        if pid.returncode or pid.stdout.strip() != b'1':
            return False
        ready = subprocess.run([DOCKER, 'exec', self.pg_id, 'pg_isready', '-h', '/pgsocket',
                                '-U', 'postgres', '-d', 'pastelerias_erp'],
                               env=CLEAN_ENV, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=10, check=False)
        return ready.returncode == 0

    def _plan(self, filename, name, image, mounts, tmpfs):
        _private_file(self.job_dir / filename, json.dumps({
            'job_id': self.job_id, 'id': None, 'name': name, 'image': image,
            'labels': {'codex.task': self.job_id, 'codex.owner': 'erp-autofix'},
            'mounts': mounts, 'tmpfs': tmpfs, 'network': 'none', 'volumes': [],
            'discardable': 'synthetic data; no production imports'}))

    def _validate_policy(self, item, candidate):
        host = item.get('HostConfig', {})
        mounts = item.get('Mounts', [])
        expected = {(str(self.socket), '/pgsocket', not candidate)}
        if candidate:
            expected.add((str(self.worktree), '/app', False))
        binds = {(mount.get('Source'), mount.get('Destination'), mount.get('RW'))
                 for mount in mounts if mount.get('Type') == 'bind'}
        if (host.get('NetworkMode') != 'none' or binds != expected
                or any(mount.get('Type') not in {'bind', 'tmpfs'} for mount in mounts)
                or host.get('Memory') != (1073741824 if candidate else 536870912)
                or host.get('PidsLimit') != 64
                or host.get('NanoCpus') != (1000000000 if candidate else 500000000)):
            raise RuntimeFailure('container_policy_mismatch')
        if candidate:
            if (host.get('ReadonlyRootfs') is not True
                    or item.get('Config', {}).get('User') != '65534:65534'
                    or 'ALL' not in (host.get('CapDrop') or [])
                    or 'no-new-privileges' not in (host.get('SecurityOpt') or [])
                    or host.get('Privileged') is not False):
                raise RuntimeFailure('container_policy_mismatch')
        elif '/var/lib/postgresql/data' not in (host.get('Tmpfs') or {}):
            raise RuntimeFailure('container_policy_mismatch')

    def _record(self, name, item):
        _private_file(self.job_dir / name, json.dumps({'job_id': self.job_id, 'id': item['Id'],
                      'labels': item['Config']['Labels'],
                      'user': item['Config'].get('User'),
                      'host_policy': {key: item.get('HostConfig', {}).get(key) for key in
                                      ('NetworkMode', 'ReadonlyRootfs', 'CapDrop', 'SecurityOpt',
                                       'Memory', 'PidsLimit', 'NanoCpus', 'Privileged')},
                      'mounts': item.get('Mounts', []), 'tmpfs': item.get('HostConfig', {}).get('Tmpfs', {}),
                      'removed': item.get('removed', False),
                      'discardable': 'synthetic PostgreSQL on tmpfs; no production imports'}))

    def __exit__(self, exc_type, exc, traceback):
        if self.pg_id:
            item = self._owned(self.pg_id)
            _docker(['rm', '--force', self.pg_id])
            if _docker(['ps', '-aq', '--no-trunc', '--filter=id=' + self.pg_id]):
                raise RuntimeFailure('resource_cleanup_failed')
            self._record('resources-after.json', {**item, 'removed': True})
            self.pg_id = None
        return False

    def _test_command(self, arguments, name):
        return [DOCKER, 'run', '--name=' + name, *self._labels(), '--network=none',
                '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
                '--user=65534:65534', '--memory=1g', '--cpus=1', '--pids-limit=64',
                '--tmpfs=/tmp:rw,size=256m,mode=1777', '-v', str(self.worktree) + ':/app:ro',
                '-v', str(self.socket) + ':/pgsocket:ro', '--workdir=/app',
                '-e', 'APP_ENV=development', '-e', 'ALLOW_INSECURE_LOCAL_SECRET_KEY=1',
                '-e', 'SECRET_KEY=autofix-synthetic-secret-key-never-production',
                '-e', 'DATABASE_URL=postgresql://postgres:postgres@/pastelerias_erp?host=/pgsocket',
                '-e', 'HOME=/tmp', '-e', 'PYTHONDONTWRITEBYTECODE=1',
                self.tools_image, '/opt/venv/bin/python', '-B', *arguments]

    def run(self, arguments, log_name):
        validate_arguments(arguments)
        if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}', log_name):
            raise RuntimeFailure('invalid_log_name')
        if not self.pg_id:
            raise RuntimeFailure('sandbox_not_started')
        name = 'erp-autofix-test-' + self.job_id + '-' + uuid.uuid4().hex[:8]
        command = self._test_command(arguments, name)
        command[1] = 'create'
        self._plan(log_name + '-planned-resource.json', name, self.tools_image,
                   [{'type': 'bind', 'source': str(self.worktree), 'target': '/app', 'read_only': True},
                    {'type': 'bind', 'source': str(self.socket), 'target': '/pgsocket', 'read_only': True}],
                   {'/tmp': 'rw,size=256m,mode=1777'})
        identifier = _docker(command[1:])
        if not re.fullmatch(r'[0-9a-f]{64}', identifier):
            raise RuntimeFailure('invalid_container_id')
        item = self._owned(identifier)
        try:
            self._record(log_name + '-resources-before.json', item)
            self._validate_policy(item, candidate=True)
            attached = _limited([DOCKER, 'start', '--attach', identifier], self.job_dir / log_name,
                                timeout=900 if arguments == ['manage.py', 'migrate', '--noinput'] else 600)
            state = self._owned(identifier).get('State', {})
            if (state.get('Running') is not False or type(state.get('ExitCode')) is not int
                    or attached.returncode not in (0, state['ExitCode'])):
                raise RuntimeFailure('test_execution_invalid')
            if state['ExitCode'] != 0:
                raise RuntimeFailure('test_execution_failed')
            return subprocess.CompletedProcess(command, 0, b'', b'')
        finally:
            # A timed-out docker client does not stop its container.
            if _docker(['ps', '-aq', '--no-trunc', '--filter=id=' + identifier]):
                item = self._owned(identifier)
                _docker(['rm', '--force', identifier])
            if _docker(['ps', '-aq', '--no-trunc', '--filter=id=' + identifier]):
                raise RuntimeFailure('resource_cleanup_failed')
            self._record(log_name + '-resources-after.json', {**item, 'removed': True})
