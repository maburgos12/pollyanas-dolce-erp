"""Cheap idle claims must never bypass the authoritative Django work path."""
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'ops/telegram-hours' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rpc = load('audio_rpc')


class AudioRPCTests(unittest.TestCase):
    def invoke(self, payload, outputs):
        output = io.StringIO()
        with patch.object(rpc.sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(json.dumps(payload).encode()))), patch.object(rpc.sys, 'stdout', output), patch.object(rpc.signal, 'alarm'), patch.object(rpc.subprocess, 'run', side_effect=outputs) as run:
            code = rpc.main()
        return code, json.loads(output.getvalue()), run

    def test_idle_claim_skips_django(self):
        code, value, run = self.invoke({'action':'claim'}, [SimpleNamespace(stdout=b'0\n')])
        self.assertEqual((code, value), (0, {}))
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0][-2:], ['python', '/app/ops/telegram-hours/audio_probe.py'])
        self.assertEqual(run.call_args.kwargs['timeout'], 5)
        self.assertEqual(run.call_args.kwargs['input'], b'')
        self.assertEqual(run.call_args.kwargs['env'], {'PATH':'/usr/bin:/bin', 'HOME':'/root'})

    def test_pending_claim_uses_original_authorization_and_lease(self):
        code, value, run = self.invoke({'action':'claim'}, [SimpleNamespace(stdout=b'1\n'), SimpleNamespace(stdout=b'{"id":123}')])
        self.assertEqual((code, value), (0, {'id':123}))
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args.args[0][-3:], ['python', 'manage.py', 'hours_audio_rpc'])
        self.assertEqual(json.loads(run.call_args.kwargs['input']), {'action':'claim'})

    def test_finish_never_uses_probe(self):
        payload = {'action':'finish', 'id':1, 'lease':'a'*32, 'sha256':'b'*64, 'text':'Todas hoy cerradas'}
        code, value, run = self.invoke(payload, [SimpleNamespace(stdout=b'{"accepted":false}')])
        self.assertEqual((code, value), (0, {'accepted':False}))
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0][-3:], ['python', 'manage.py', 'hours_audio_rpc'])

    def test_probe_invalid_output_and_timeout_fail_closed(self):
        for result in [SimpleNamespace(stdout=b'1\nSECRET'), SimpleNamespace(stdout=b'0\n'*1000), subprocess.TimeoutExpired('SECRET', 5)]:
            code, value, run = self.invoke({'action':'claim'}, [result])
            self.assertEqual((code, value), (1, {'error':'unavailable'}))
            self.assertEqual(run.call_count, 1)

    def test_claim_cannot_choose_query_or_path(self):
        for payload in [{'action':'claim', 'sql':'SECRET'}, {'action':'claim', 'path':'SECRET'}, {'action':'probe'}]:
            code, value, run = self.invoke(payload, [])
            self.assertEqual((code, value), (1, {'error':'unavailable'}))
            run.assert_not_called()


class AudioProbeTests(unittest.TestCase):
    def test_fixed_read_only_query_and_timeouts(self):
        probe = load('audio_probe')
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (False,)
        with patch.dict(os.environ, {'DATABASE_URL':'private-sentinel'}), patch.object(probe.psycopg2, 'connect', return_value=connection) as connect:
            self.assertFalse(probe.has_pending_audio())
        connect.assert_called_once_with('private-sentinel', connect_timeout=2,
            options='-c default_transaction_read_only=on -c statement_timeout=1000 -c lock_timeout=500')
        cursor.execute.assert_called_once_with("SELECT EXISTS(SELECT 1 FROM horarios_especiales_telegramhoursupdate WHERE state = 'AUDIO_WAIT')")
        connection.close.assert_called_once()

    def test_probe_error_never_exposes_connection_or_query(self):
        probe = load('audio_probe')
        output = io.StringIO()
        with patch.object(probe, 'has_pending_audio', side_effect=RuntimeError('SECRET')), patch.object(probe.sys, 'stdout', output):
            self.assertEqual(probe.main(), 1)
        self.assertEqual(output.getvalue(), '')


if __name__ == '__main__':
    unittest.main()
