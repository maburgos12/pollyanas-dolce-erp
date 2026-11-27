"""Injected-only crash/effect-order tests: never connects to an ERP service."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release'
sys.path.insert(0, str(SOURCE))
import erp_release_supervisor as s
spec = importlib.util.spec_from_file_location('supervisor_test_wires', Path(__file__).with_name('test_release_preservation.py'))
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)
BASE = 'a' * 40
RELEASE = 'b' * 40
ROW = {'nonce': '1' * 24, 'head_sha': 'c' * 40, 'base_sha': BASE, 'production_sha': BASE}


class Fake:
    def __init__(self):
        self.calls = []
        self.fail = None
        self.cuts = [w.cut(w.wire()) for _ in range(3)]
        self.pins = {'base': BASE, 'roles': list(s.WORKERS)}
        self.receipt = {'release': RELEASE, 'consumer_verified': True}

    def call(self, name):
        self.calls.append(name)
        if self.fail == name:
            raise RuntimeError('secret must not enter journal')

    def preflight(self, row):
        self.call('preflight'); return self.pins

    def snapshot(self, pins, before=None):
        self.call('snapshot'); return self.cuts.pop(0)

    def idle(self, pins, cut):
        self.call('idle')

    def verify_pins(self, pins, stopped=()):
        self.call('verify_stopped' if stopped else 'verify_live')
        if stopped:
            assert tuple(stopped) == s.WORKERS

    def stop(self, pins, roles):
        assert tuple(roles) == s.WORKERS
        self.call('stop')

    def official(self, release, base, pins):
        self.call('official')

    def verify_runtime(self, release, pins):
        self.call('runtime'); return self.receipt

    def close(self):
        self.call('close')
        return {'point_transport_reaped': True, 'release_acknowledged': True, 'exit_code': 0}


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.path = self.root / 'attempt'
        self.adapter = Fake()
        self.supervisor = s.Supervisor(self.adapter, self.path)

    def tearDown(self):
        self.supervisor.close()
        self.tmp.cleanup()

    def ready(self):
        self.supervisor.preflight(copy.deepcopy(ROW))

    def rejected(self, row=ROW):
        with self.assertRaises(s.SupervisorError):
            self.supervisor.preflight(copy.deepcopy(row))
        self.assertNotIn('stop', self.adapter.calls)
        self.assertNotIn('official', self.adapter.calls)

    def held(self):
        with self.assertRaises(s.SupervisorError):
            self.supervisor(RELEASE, BASE)
        self.assertEqual(self.supervisor.state, 'HOLD')
        previous = list(self.adapter.calls)
        with self.assertRaises(s.SupervisorError):
            self.supervisor(RELEASE, BASE)
        self.assertEqual(previous, self.adapter.calls)

    def test_success_durable_order(self):
        self.ready()
        self.assertEqual(self.supervisor(RELEASE, BASE), self.adapter.receipt)
        states = [json.loads(p.read_text())['state'] for p in sorted(self.path.iterdir())]
        self.assertEqual(states[-1], 'RUNTIME_VERIFIED')
        self.assertLess(states.index('MERGED'), states.index('STOP_INTENT'))
        self.assertLess(states.index('PRESERVED'), states.index('OFFICIAL_INTENT'))

    def test_failed_journal_initialization_closes_opened_descriptor(self):
        original_open = os.open
        for operation in ('fchmod', 'fsync'):
            opened = []
            name = 'broken-' + operation
            def tracked_open(path, *args, **kwargs):
                fd = original_open(path, *args, **kwargs)
                if path == name:
                    opened.append(fd)
                return fd
            with patch.object(s.os, 'open', side_effect=tracked_open), patch.object(
                    s.os, operation, side_effect=OSError('injected IO failure')):
                with self.assertRaises(OSError):
                    s.Journal(self.root / name)
            self.assertEqual(len(opened), 1)
            with self.assertRaises(OSError):
                os.fstat(opened[0])

    def test_preflight_busy_no_effect(self):
        self.adapter.fail = 'idle'; self.rejected()

    def test_bad_binding_no_effect(self):
        self.rejected({**ROW, 'head_sha': 'wrong'})

    def test_production_base_required(self):
        self.rejected({**ROW, 'production_sha': RELEASE})

    def test_pin_base_required(self):
        self.adapter.pins['base'] = RELEASE; self.rejected()

    def test_seven_roles_required(self):
        self.adapter.pins['roles'].pop(); self.rejected()

    def test_callback_not_before_preflight(self):
        with self.assertRaises(s.SupervisorError): self.supervisor(RELEASE, BASE)
        self.assertEqual(self.adapter.calls, [])

    def test_wrong_callback_base_holds_without_stop(self):
        self.ready()
        with self.assertRaises(s.SupervisorError): self.supervisor(RELEASE, 'd' * 40)
        self.assertEqual(self.supervisor.state, 'HOLD')
        self.assertNotIn('stop', self.adapter.calls)

    def test_exact_runtime_revision_required(self):
        self.ready(); self.adapter.receipt['release'] = BASE; self.held()

    def test_lease_release_failure_prevents_published_callback_success(self):
        self.ready(); self.adapter.fail = 'close'; self.held()
        self.assertEqual(self.adapter.calls.count('official'), 1)

    def test_consumer_proof_required(self):
        self.ready(); self.adapter.receipt['consumer_verified'] = False; self.held()

    def test_payload_changed_no_official(self):
        self.ready()
        changed = w.message(); changed['body'] = 'changed'
        self.adapter.cuts[-1] = w.cut(w.wire(changed))
        self.held(); self.assertNotIn('official', self.adapter.calls)

    def test_missing_not_success(self):
        self.ready(); self.adapter.cuts[-1] = w.cut()
        self.held(); self.assertNotIn('official', self.adapter.calls)

    def test_postmerge_busy_no_stop(self):
        self.ready(); self.adapter.fail = 'idle'
        self.held(); self.assertNotIn('stop', self.adapter.calls)

    def test_stop_failure_no_restore_or_official(self):
        self.ready(); self.adapter.fail = 'stop'
        self.held(); self.assertNotIn('official', self.adapter.calls)
        self.assertEqual(self.adapter.calls.count('stop'), 1)

    def test_stopped_verification_failure(self):
        self.ready(); self.adapter.fail = 'verify_stopped'
        self.held(); self.assertNotIn('official', self.adapter.calls)

    def test_official_failure_never_retried(self):
        self.ready(); self.adapter.fail = 'official'; self.held()
        self.assertEqual(self.adapter.calls.count('official'), 1)

    def test_existing_journal_never_replayed(self):
        with self.assertRaises(FileExistsError): s.Supervisor(Fake(), self.path)

    def test_symlink_path_rejected(self):
        link = self.root / 'link'; link.symlink_to(self.path, target_is_directory=True)
        with self.assertRaises(OSError): s.Supervisor(Fake(), link / 'child')

    def test_modes_and_redacted_errors(self):
        self.adapter.fail = 'preflight'; self.rejected()
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o700)
        for path in self.path.iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn('secret', path.read_text())

    def test_journal_bound_before_file_created(self):
        with self.assertRaises(s.SupervisorError): self.supervisor.journal.append('LARGE', value='x' * s.LIMIT)
        self.assertEqual(list(self.path.iterdir()), [])

    def test_crash_before_stop_intent_no_stop(self):
        self.ready()
        original = self.supervisor.journal.append
        def failing(state, **details):
            if state == 'STOP_INTENT': raise OSError('disk full')
            return original(state, **details)
        with patch.object(self.supervisor.journal, 'append', side_effect=failing): self.held()
        self.assertNotIn('stop', self.adapter.calls)

    def test_crash_before_official_intent_no_official(self):
        self.ready()
        original = self.supervisor.journal.append
        def failing(state, **details):
            if state == 'OFFICIAL_INTENT': raise OSError('disk full')
            return original(state, **details)
        with patch.object(self.supervisor.journal, 'append', side_effect=failing): self.held()
        self.assertNotIn('official', self.adapter.calls)

    def test_keyboard_interrupt_after_merge_holds(self):
        self.ready()
        with patch.object(self.adapter, 'stop', side_effect=KeyboardInterrupt): self.held()

    def test_preflight_cannot_repeat(self):
        self.ready()
        with self.assertRaises(s.SupervisorError): self.supervisor.preflight(ROW)


if __name__ == '__main__':
    unittest.main()
