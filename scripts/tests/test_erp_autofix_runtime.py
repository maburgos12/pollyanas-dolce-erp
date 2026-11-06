import importlib.util
import json
import os
from types import SimpleNamespace
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('autofix_runtime', Path(__file__).resolve().parents[2] / 'ops/erp-autofix/runtime.py')
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)


class RuntimeTests(unittest.TestCase):
    @staticmethod
    def candidate_policy(sandbox):
        return {'Config': {'User': '65534:65534',
                           'Labels': {'codex.task': sandbox.job_id, 'codex.owner': 'erp-autofix'}},
                'HostConfig': {'NetworkMode': 'none', 'ReadonlyRootfs': True,
                               'CapDrop': ['ALL'], 'SecurityOpt': ['no-new-privileges'],
                               'Memory': 1073741824, 'PidsLimit': 64, 'NanoCpus': 1000000000,
                               'Privileged': False},
                'Mounts': [{'Type': 'bind', 'Source': str(sandbox.worktree), 'Destination': '/app', 'RW': False},
                           {'Type': 'bind', 'Source': str(sandbox.socket), 'Destination': '/pgsocket', 'RW': False}]}

    def test_command_allowlist(self):
        for args in (['manage.py', 'check'], ['manage.py', 'migrate', '--noinput'],
                     ['manage.py', 'migrate', '--check'], ['manage.py', 'test', 'core.tests'],
                     ['-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_erp_observer.py'],
                     ['-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_*.py']):
            runtime.validate_arguments(args)
        for args in (['-c', 'print(1)'], ['manage.py', 'shell'], ['manage.py', 'test', '--keepdb'],
                     ['manage.py', 'migrate'], ['manage.py', 'test', '../bad'], ['manage.py', 'test', 'core;id']):
            with self.assertRaises(runtime.RuntimeFailure):
                runtime.validate_arguments(args)

    def test_test_container_has_boundaries(self):
        with tempfile.TemporaryDirectory(prefix='autofix-') as temporary:
            root = Path(temporary)
            sandbox = runtime.Sandbox(root / 'source', root / 'autofix-' / 'unused', 'tools@sha256:' + 'a'*64, 'postgres@sha256:' + 'b'*64)
            sandbox.job_id = 'autofix-' + 'a'*16 + '-1'
            command = sandbox._test_command(['manage.py', 'check'], 'test-container')
            joined = ' '.join(command)
            for boundary in ('--network=none', '--read-only', '--cap-drop=ALL', '--user=65534:65534', '--security-opt=no-new-privileges', '/app:ro', '/opt/venv/bin/python', '-B'):
                self.assertIn(boundary, joined)
            self.assertNotIn('--env-file', joined)
            self.assertNotIn('/var/run/docker.sock', joined)

    def test_cleanup_refuses_foreign_resources(self):
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = runtime.Sandbox(Path(temporary), Path(temporary), 'tools', 'postgres')
            sandbox.job_id = 'autofix-' + 'a'*16 + '-1'
            sandbox.pg_id = 'a'*64
            foreign = {'Id': sandbox.pg_id, 'Config': {'Labels': {'codex.task': 'other', 'codex.owner': 'erp-autofix'}}, 'Mounts': []}
            with patch.object(runtime, '_inspect', return_value=foreign), patch.object(runtime, '_docker') as docker:
                with self.assertRaises(runtime.RuntimeFailure):
                    sandbox.__exit__(None, None, None)
                docker.assert_not_called()

    def test_schema_and_events_fail_closed(self):
        schema = {'type': 'object', 'required': ['verdict'], 'additionalProperties': False, 'properties': {'verdict': {'type': 'string', 'enum': ['pass']}}}
        runtime._validate_schema({'verdict': 'pass'}, schema)
        for result in ({}, {'verdict': 'bad'}, {'verdict': 'pass', 'extra': True}):
            with self.assertRaises(runtime.RuntimeFailure):
                runtime._validate_schema(result, schema)
        runtime._validate_events('{"type":"thread.started"}\n{"type":"turn.completed"}\n')
        for events in ('{"type":"turn.failed"}\n', '{"type":"item.started","item":{"type":"command_execution"}}\n{"type":"turn.completed"}\n'):
            with self.assertRaises(runtime.RuntimeFailure):
                runtime._validate_events(events)

    def test_model_uses_only_chatgpt_and_isolated_systemd(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            model_root = root / 'model'
            model_root.mkdir()
            job = root / ('autofix-' + 'a'*16 + '-1')
            job.mkdir()
            schema = {'type': 'object', 'required': ['ok'], 'additionalProperties': False,
                      'properties': {'ok': {'type': 'boolean'}}}
            original_stat = os.stat
            def root_stat(*args, **kwargs):
                parts = list(original_stat(*args, **kwargs))
                parts[4] = 0
                return os.stat_result(parts)
            def launch(command, log_path, **kwargs):
                log_path.write_text('{"type":"turn.completed"}\n')
                (model_root / job.name / 'diagnose/result.json').write_text('{"ok":true}')
                self.assertIn('--pipe', command)
                self.assertIn('--property=ProtectSystem=strict', command)
                self.assertIn('CODEX_HOME=' + str(runtime.CODEX_HOME), command)
                self.assertNotIn('OPENAI_API_KEY', ' '.join(command))
                return subprocess.CompletedProcess(command, 0)
            with patch.object(runtime, 'MODEL_ROOT', model_root), patch.object(runtime.pwd, 'getpwnam', return_value=SimpleNamespace(pw_uid=123, pw_gid=123)), patch.object(runtime.os, 'chown'), patch.object(runtime.os, 'stat', side_effect=root_stat), patch.object(runtime, '_limited', side_effect=launch) as limited, patch.object(runtime, '_stop_model_unit') as stop:
                self.assertEqual(runtime.model(job, 'diagnose', 'private prompt', schema), {'ok': True})
                limited.side_effect = runtime.RuntimeFailure('execution_timeout')
                with self.assertRaisesRegex(runtime.RuntimeFailure, 'execution_timeout'):
                    runtime.model(job, 'timeout', 'private prompt', schema)
                stop.assert_called_with('erp-autofix-model-' + job.name + '-timeout')
            wrapper = (model_root / job.name / 'run-diagnose.sh').read_text()
            for flag in ('--ignore-user-config', '--ignore-rules', '--disable shell_tool', '--disable apps', '--disable plugins', 'forced_login_method', 'chatgpt', 'flock -n'):
                self.assertIn(flag, wrapper)

    def test_cleanup_checks_absence_and_preserves_mount_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = runtime.Sandbox(Path(temporary), Path(temporary), 'tools', 'postgres')
            sandbox.job_id = 'autofix-' + 'a'*16 + '-1'
            sandbox.pg_id = 'a'*64
            owned = {'Id': sandbox.pg_id, 'Config': {'Labels': {'codex.task': sandbox.job_id, 'codex.owner': 'erp-autofix'}},
                     'Mounts': [{'Type': 'bind', 'Destination': '/pgsocket'}]}
            with patch.object(runtime, '_inspect', return_value=owned), patch.object(runtime, '_docker', return_value='') as docker:
                sandbox.__exit__(None, None, None)
            self.assertEqual(docker.call_args_list[0].args[0], ['rm', '--force', 'a'*64])
            after = json.loads((Path(temporary) / 'resources-after.json').read_text())
            self.assertTrue(after['removed'])
            self.assertEqual(after['mounts'], owned['Mounts'])

    def test_candidate_exit_code_is_inspected_then_exact_id_removed(self):
        for code in (0, 1, 7):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temporary:
                sandbox = runtime.Sandbox(Path(temporary), Path(temporary), 'tools', 'postgres')
                sandbox.job_id = 'autofix-' + 'a'*16 + '-1'
                sandbox.pg_id = 'b'*64
                identifier = 'c'*64
                owned = {**self.candidate_policy(sandbox), 'Id': identifier,
                         'State': {'Running': False, 'ExitCode': code}}
                with patch.object(runtime, '_docker', side_effect=[identifier, identifier, '', '']) as docker, patch.object(runtime, '_inspect', return_value=owned), patch.object(runtime, '_limited', return_value=subprocess.CompletedProcess([], code)):
                    if code:
                        with self.assertRaisesRegex(runtime.RuntimeFailure, 'test_execution_failed'):
                            sandbox.run(['manage.py', 'check'], 'check.log')
                    else:
                        self.assertEqual(sandbox.run(['manage.py', 'check'], 'check.log').returncode, 0)
                self.assertIn(unittest.mock.call(['rm', '--force', identifier]), docker.call_args_list)
                self.assertNotIn('--rm', docker.call_args_list[0].args[0])
                plan = json.loads((Path(temporary) / 'check.log-planned-resource.json').read_text())
                self.assertIsNone(plan['id'])
                self.assertEqual(plan['labels']['codex.owner'], 'erp-autofix')
                self.assertEqual(plan['volumes'], [])

    def test_enter_start_failure_removes_owned_postgres(self):
        with tempfile.TemporaryDirectory() as temporary:
            job = Path(temporary) / ('autofix-' + 'a'*16 + '-1')
            job.mkdir()
            sandbox = runtime.Sandbox(Path(temporary), job, 'tools', 'postgres')
            identifier = 'b'*64
            item = {'Id': identifier, 'Config': {'Labels': {'codex.task': sandbox.job_id, 'codex.owner': 'erp-autofix'}},
                    'HostConfig': {'NetworkMode': 'none', 'Memory': 536870912, 'PidsLimit': 64, 'NanoCpus': 500000000,
                                   'Tmpfs': {'/var/lib/postgresql/data': 'rw,size=512m,mode=0700'}},
                    'Mounts': [{'Type': 'bind', 'Source': str(sandbox.socket), 'Destination': '/pgsocket', 'RW': True}]}
            calls = iter([identifier, runtime.RuntimeFailure('docker_command_failed'), '', ''])
            def execute(arguments):
                if arguments[0] == 'create':
                    self.assertIn('unix_socket_directories=/var/run/postgresql,/pgsocket', arguments)
                    self.assertIn('listen_addresses=', arguments)
                    plan = json.loads((job / 'planned-resource.json').read_text())
                    self.assertEqual(plan['name'], 'erp-autofix-pg-' + job.name)
                    self.assertIn('/var/lib/postgresql/data', plan['tmpfs'])
                result = next(calls)
                if isinstance(result, Exception):
                    raise result
                return result
            with patch.object(runtime, '_docker', side_effect=execute) as docker, patch.object(runtime, '_inspect', return_value=item):
                with self.assertRaisesRegex(runtime.RuntimeFailure, 'docker_command_failed'):
                    sandbox.__enter__()
                self.assertIn(unittest.mock.call(['rm', '--force', identifier]), docker.call_args_list)
                self.assertIsNone(sandbox.pg_id)

    def test_limited_timeout_kills_client_and_keeps_stderr_separate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            process = unittest.mock.Mock(pid=123)
            process.wait.side_effect = [subprocess.TimeoutExpired('command', 1), 0]
            with patch.object(runtime.subprocess, 'Popen', return_value=process) as popen, patch.object(runtime.os, 'killpg') as kill:
                with self.assertRaisesRegex(runtime.RuntimeFailure, 'execution_timeout'):
                    runtime._limited(['fixed'], root / 'out', timeout=1, stderr_path=root / 'err')
                kill.assert_called_once_with(123, runtime.signal.SIGKILL)
                self.assertIsNot(popen.call_args.kwargs['stdout'], popen.call_args.kwargs['stderr'])
            self.assertEqual((root / 'err').stat().st_mode & 0o777, 0o600)

    def test_model_cleanup_stops_exact_unit_and_checks_inactive(self):
        unit = 'erp-autofix-model-autofix-' + 'a'*16 + '-1-diagnose'
        with patch.object(runtime.subprocess, 'run', side_effect=[subprocess.CompletedProcess([], 0), subprocess.CompletedProcess([], 3)]) as run:
            runtime._stop_model_unit(unit)
        self.assertEqual(run.call_args_list[0].args[0], ['/usr/bin/systemctl', 'stop', unit])
        self.assertEqual(run.call_args_list[1].args[0][-1], unit)

    def test_candidate_policy_mismatch_is_removed_without_starting(self):
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = runtime.Sandbox(Path(temporary), Path(temporary), 'tools', 'postgres')
            sandbox.job_id = 'autofix-' + 'a'*16 + '-1'
            sandbox.pg_id = 'b'*64
            identifier = 'c'*64
            item = {**self.candidate_policy(sandbox), 'Id': identifier}
            item['HostConfig']['ReadonlyRootfs'] = False
            with patch.object(runtime, '_docker', side_effect=[identifier, identifier, '', '']) as docker, patch.object(runtime, '_inspect', return_value=item), patch.object(runtime, '_limited') as start:
                with self.assertRaisesRegex(runtime.RuntimeFailure, 'container_policy_mismatch'):
                    sandbox.run(['manage.py', 'check'], 'bad.log')
                start.assert_not_called()
                self.assertIn(unittest.mock.call(['rm', '--force', identifier]), docker.call_args_list)
            evidence = json.loads((Path(temporary) / 'bad.log-resources-before.json').read_text())
            self.assertIs(evidence['host_policy']['ReadonlyRootfs'], False)
            self.assertNotIn('Env', evidence)

    def test_postgres_readiness_rejects_temporary_initdb_server(self):
        sandbox = runtime.Sandbox(Path('/source'), Path('/job'), 'tools', 'postgres')
        sandbox.pg_id = 'a'*64
        with patch.object(runtime.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, b'42\n')) as run:
            self.assertFalse(sandbox._postgres_ready())
            self.assertEqual(run.call_count, 1)
        with patch.object(runtime.subprocess, 'run', side_effect=[subprocess.CompletedProcess([], 0, b'1\n'), subprocess.CompletedProcess([], 0)]) as run:
            self.assertTrue(sandbox._postgres_ready())
            self.assertIn('postmaster.pid', run.call_args_list[0].args[0][-1])
            self.assertEqual(run.call_args_list[1].args[0][3], 'pg_isready')
        with patch.object(runtime.subprocess, 'run', side_effect=[subprocess.CompletedProcess([], 0, b'1\n'), subprocess.CompletedProcess([], 1)]):
            self.assertFalse(sandbox._postgres_ready())

    def test_only_initial_migration_gets_extended_timeout(self):
        for arguments, expected in ((['manage.py', 'migrate', '--noinput'], 900),
                                    (['manage.py', 'migrate', '--check'], 600),
                                    (['manage.py', 'test', 'core.tests'], 600)):
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as temporary:
                sandbox = runtime.Sandbox(Path(temporary), Path(temporary), 'tools', 'postgres')
                sandbox.job_id = 'autofix-' + 'a'*16 + '-1'
                sandbox.pg_id = 'b'*64
                identifier = 'c'*64
                item = {**self.candidate_policy(sandbox), 'Id': identifier,
                        'State': {'Running': False, 'ExitCode': 0}}
                with patch.object(runtime, '_docker', side_effect=[identifier, identifier, '', '']), patch.object(runtime, '_inspect', return_value=item), patch.object(runtime, '_limited', return_value=subprocess.CompletedProcess([], 0)) as limited:
                    sandbox.run(arguments, 'check.log')
                    self.assertEqual(limited.call_args.kwargs['timeout'], expected)


if __name__ == '__main__':
    unittest.main()
