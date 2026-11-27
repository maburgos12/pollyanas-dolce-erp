"""Supervised recurring worker: private real ledgers/locks and fake host/Git."""
from contextlib import ExitStack
import fcntl
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

MODULES = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release'
if str(MODULES) not in sys.path:
    sys.path.insert(0, str(MODULES))
import erp_release_worker as w

c = w.canonical
BASE, HEAD, RELEASE, TREE = 'a' * 40, 'b' * 40, 'c' * 40, 'd' * 40
ORIGINAL_PUBLISH = c.publish


def record(nonce='1' * 24, head=HEAD, state='approved', expired=False):
    return {'repo': c.REPO, 'pr': 42, 'head_sha': head, 'base_sha': BASE,
            'production_sha': BASE, 'nonce': nonce, 'expires': int(time.time()) + (-1 if expired else 3600),
            'message_id': 1, 'chat_id': 7, 'state': state,
            'decision': 'rejected' if state == 'rejected' else 'approved',
            'decision_at': int(time.time()) - 10, 'release_sha': None, 'notification_id': None}


class WorkerTests(unittest.TestCase):
    def test_approved_default_publisher_cannot_merge_without_supervisor(self):
        row = record()
        self.ledger.write(row)
        with self.assertRaisesRegex(ValueError, 'supervised_deployer_required'):
            ORIGINAL_PUBLISH(row['nonce'], self.api, self.ledger, production=lambda: BASE)
        self.assertEqual(self.ledger.read(row['nonce'])['state'], 'approved')
        self.assertEqual(self.calls, [])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledger = c.Ledger(self.root / 'ledger')
        self.paths = [self.root / name for name in ('deploy', 'router', 'native1', 'native2', 'native3')]
        for path in self.paths:
            path.touch(mode=0o600)
        self.adapters, self.supervisors, self.calls, self.sends = [], [], [], []
        self.mode = 'success'
        self.after_preflight = None
        self.callback_count = 0
        self.fetch_count = 0

    def send(self, config, method, data):
        self.sends.append((method, data))
        return {'message_id': 1, 'chat': {'id': data['chat_id'], 'type': 'private'}}

    def api(self, method, path, data=None):
        self.calls.append((method, path, data))
        if method == 'POST' and path.endswith('/comments'): return {'id': 1}
        if method == 'PUT' and path.endswith('/merge'): return {'merged': True, 'sha': RELEASE}
        if method == 'GET' and path.endswith('/git/commits/' + RELEASE):
            return {'parents': [{'sha': BASE}, {'sha': HEAD}], 'tree': {'sha': TREE}}
        if method == 'GET' and '/git/commits/' in path: return {'tree': {'sha': TREE}}
        if method == 'GET' and path.endswith('/git/ref/heads/main'): return {'object': {'sha': RELEASE}}
        raise AssertionError((method, path))

    def adapter(self, fd, lease_check):
        owner = self
        class Adapter:
            root = owner.root
            closed = 0
            def __init__(self): self.fd = fd; self.lease_check = lease_check
            def close(self): self.closed += 1
        obj = Adapter()
        self.adapters.append(obj)
        return obj

    def supervisor(self, adapter, attempt):
        owner = self
        class FakeSupervisor:
            state = 'NEW'
            retryable = False
            closed = 0
            def preflight(self, row):
                if owner.mode in ('busy', 'failure', 'precheck_exception'):
                    self.retryable = owner.mode == 'busy'
                    self.state = 'PRECHECK' if owner.mode == 'precheck_exception' else 'REJECTED'
                    raise RuntimeError('simulated preflight')
                self.state = 'PRECHECK'
                if owner.after_preflight: owner.after_preflight(row)
            def __call__(self, release, base):
                owner.callback_count += 1
                owner.assertEqual((release, base), (RELEASE, BASE))
                owner.assertTrue(adapter.lease_check())
                os.fstat(adapter.fd)  # Controller still owns live shared lease.
                if owner.mode == 'callback_failure': raise RuntimeError('simulated callback')
                self.state = 'RUNTIME_VERIFIED'
            def close(self): self.closed += 1
        obj = FakeSupervisor()
        self.supervisors.append(obj)
        return obj

    def run_tick(self, nonce, **kwargs):
        def loop(api, ledger, config, send, *, publisher):
            with ledger.lock(worker=True):
                return publisher(nonce, api, ledger)
        def fetch(*args): self.fetch_count += 1
        def publish(nonce, api, ledger, deployer=None):
            return ORIGINAL_PUBLISH(nonce, api, ledger, deployer=deployer, production=lambda: BASE)
        with patch.object(c, 'worker', side_effect=loop), patch.object(c, 'eligible', return_value={'draft': False}), \
             patch.object(c, 'publish', side_effect=publish):
            w.run(self.api, self.ledger, {}, self.send, adapter_factory=self.adapter,
                  supervisor_factory=self.supervisor, fetch=fetch, provenance=lambda root, base: lambda: True,
                  deploy_path=self.paths[0], router_path=self.paths[1], native_paths=self.paths[2:], **kwargs)

    def test_supervised_callback_once_and_close_finally(self):
        for mode, state in [('success', 'published'), ('callback_failure', 'uncertain')]:
            with self.subTest(mode=mode):
                self.mode = mode
                row = record(nonce=('2' if mode == 'success' else '3') * 24)
                # Isolate global uncertain from the previous successful row.
                self.ledger.write(row)
                previous = self.callback_count
                self.run_tick(row['nonce'])
                self.assertEqual(self.callback_count, previous + 1)
                self.assertEqual(self.ledger.read(row['nonce'])['state'], state)
                self.assertEqual(self.adapters[-1].closed, 1)
                self.assertEqual(self.supervisors[-1].closed, 1)
                with self.assertRaises(OSError): os.fstat(self.adapters[-1].fd)

    def test_three_failure_limit_follows_head_not_nonce(self):
        self.mode = 'failure'
        for number in range(1, 5):
            row = record(nonce=f'{number:024x}')
            self.ledger.write(row)
            self.run_tick(row['nonce'])
            self.assertEqual(self.ledger.read(row['nonce'])['state'], 'approved')
        self.assertEqual(len(self.supervisors), 3)
        state = w.read_state(self.ledger.directory / 'supervision' / ('failures-' + HEAD + '.json'))
        self.assertEqual(state['count'], 3)
        self.assertEqual(self.calls, [])
        other = record(nonce='f' * 24, head='e' * 40)
        self.ledger.write(other); self.run_tick(other['nonce'])
        self.assertEqual(len(self.supervisors), 4)

    def test_busy_does_not_spend_third_failure_or_approval(self):
        row = record(); self.ledger.write(row)
        directory = w.private_directory(self.ledger.directory / 'supervision')
        counts = directory / ('failures-' + HEAD + '.json')
        w.save_state(counts, {'head_sha': HEAD, 'count': 2})
        self.mode = 'busy'
        self.run_tick(row['nonce'])
        self.assertEqual(w.read_state(counts)['count'], 2)
        self.assertEqual(self.ledger.read(row['nonce']), row)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.adapters[-1].closed, 1)
        self.assertEqual(self.supervisors[-1].closed, 1)

    def test_exception_precheck_no_publish(self):
        self.mode = 'failure'; row = record(); self.ledger.write(row)
        self.run_tick(row['nonce'])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.callback_count, 0)
        self.assertEqual(self.adapters[-1].closed, 1)
        self.assertEqual(self.supervisors[-1].closed, 1)

    def test_precheck_exception_with_precheck_state_still_closes(self):
        self.mode = 'precheck_exception'; row = record(); self.ledger.write(row)
        self.run_tick(row['nonce'])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.adapters[-1].closed, 1)
        self.assertEqual(self.supervisors[-1].closed, 1)

    def test_rejected_and_expired_route_canonical_blocked(self):
        for state, expired, nonce in [('rejected', False, '1' * 24), ('approved', True, '2' * 24)]:
            row = record(nonce=nonce, state=state, expired=expired); self.ledger.write(row)
            self.run_tick(nonce)
            self.assertEqual(self.ledger.read(nonce)['state'], 'blocked')
        self.assertEqual(self.adapters, [])
        self.assertEqual(self.callback_count, 0)
        self.assertEqual([call[0] for call in self.calls], ['POST', 'POST'])
        self.assertTrue(all(path.endswith('/comments') for _, path, _ in self.calls))

    def test_global_uncertain_or_executing_blocks_effects(self):
        row = record(); self.ledger.write(row)
        for state in ['uncertain', 'executing']:
            self.ledger.write(record(nonce='f' * 24, state=state))
            self.run_tick(row['nonce'])
            self.assertEqual(self.ledger.read(row['nonce']), row)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.adapters, [])

    def test_explicit_resolution_skips_only_historical_guard_not_replay(self):
        row = record(); self.ledger.write(row)
        old = record(nonce='f' * 24, state='uncertain'); self.ledger.write(old)
        with patch.object(w, 'resolved', return_value=True) as check:
            self.run_tick(row['nonce'])
        check.assert_called_once_with(old, self.api, self.ledger)
        self.assertEqual(self.ledger.read(old['nonce']), old)
        self.assertEqual(self.ledger.read(row['nonce'])['state'], 'published')
        self.assertEqual(self.callback_count, 1)

    def test_resolution_cannot_exempt_executing(self):
        row = record(); self.ledger.write(row)
        self.ledger.write(record(nonce='f' * 24, state='executing'))
        with patch.object(w, 'resolved', return_value=True) as check:
            self.run_tick(row['nonce'])
        check.assert_not_called()
        self.assertEqual(self.callback_count, 0)
        self.assertEqual(self.calls, [])

    def test_outer_lock_busy_prevents_all_calls(self):
        row = record(); self.ledger.write(row)
        for path in self.paths[:2]:
            with w.lease(path), self.assertRaises(BlockingIOError): self.run_tick(row['nonce'])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.sends, [])
        self.assertEqual(self.adapters, [])
        self.assertEqual(self.fetch_count, 0)

    def test_native_lock_busy_preserves_decision(self):
        row = record(); self.ledger.write(row)
        with w.lease(self.paths[2]): self.run_tick(row['nonce'])
        self.assertEqual(self.ledger.read(row['nonce']), row)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.adapters, [])
        self.assertFalse((self.ledger.directory / 'supervision' / ('failures-' + HEAD + '.json')).exists())

    def test_lease_rejects_symlink_ownership_mode_and_hardlink(self):
        target = self.paths[0]
        link = self.root / 'link'; link.symlink_to(target)
        with self.assertRaises(OSError), w.lease(link): pass
        with patch.object(w.os, 'geteuid', return_value=os.geteuid() + 1), self.assertRaises(ValueError), w.lease(target): pass
        target.chmod(0o622)
        with self.assertRaises(ValueError), w.lease(target): pass
        target.chmod(0o600)
        hard = self.root / 'hard'; os.link(target, hard)
        with self.assertRaises(ValueError), w.lease(target): pass

    def test_lease_identity_replacement_detected(self):
        target = self.paths[0]
        with w.lease(target) as fd:
            self.assertTrue(w.leases_intact([(target, fd)]))
            target.unlink(); target.touch(mode=0o600)
            self.assertFalse(w.leases_intact([(target, fd)]))

    def test_revoke_after_precheck_never_merges_or_deploys(self):
        row = record(); self.ledger.write(row)
        def revoke(old):
            updated = self.ledger.read(old['nonce'])
            updated.update(state='rejected', decision='rejected')
            self.ledger.write(updated)
        self.after_preflight = revoke
        self.run_tick(row['nonce'])
        self.assertEqual(self.ledger.read(row['nonce'])['state'], 'blocked')
        self.assertEqual(self.callback_count, 0)
        self.assertFalse(any(method == 'PUT' for method, _, _ in self.calls))
        self.assertEqual(self.adapters[-1].closed, 1)
        self.assertEqual(self.supervisors[-1].closed, 1)


if __name__ == '__main__': unittest.main()
