"""Installer tests with synthetic files, ledger and systemd; no real services."""
from contextlib import nullcontext, contextmanager
import fcntl
import importlib.util
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release/install.py'
spec = importlib.util.spec_from_file_location('supervised_installer', SOURCE)
i = importlib.util.module_from_spec(spec)
spec.loader.exec_module(i)


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / 'app/ops/release'
        self.target, self.units = self.root / 'target', self.root / 'units'
        for path in (self.source, self.target, self.units): path.mkdir(parents=True)
        for name in i.MODULES: (self.source / name).write_text('NEW = True\n')
        for name in i.MODULES[:2]: (self.target / name).write_text('OLD = True\n')
        for name in i.UNITS:
            (self.source / name).write_text('[Service]\n# NEW\n')
            (self.units / name).write_text('# OLD\n')
        self.consumer = self.target / 'decidir_corte_point.py'
        self.original = b'from erp_telegram_release import handle\nfrom erp_telegram_cleanup import handle\n# NAS and MacIA preserved\n'
        self.consumer.write_bytes(self.original)
        self.row = {'state': 'published', 'decision': 'approved', 'release_sha': 'f' * 40}
        self.active = False
        self.fail = None
        self.calls = []
        self.service_states = []
        self.ledger = SimpleNamespace(lock=lambda **kw: nullcontext(), read=lambda nonce: dict(self.row))
        self.api = object()
        self.module_patch = patch.dict(sys.modules, {'erp_telegram_release': SimpleNamespace(
            Ledger=lambda state: self.ledger, GitHub=lambda: SimpleNamespace(api=self.api))})
        self.module_patch.start()
        self.policy = patch.object(i, 'check_policy')
        self.policy.start()

    def tearDown(self):
        self.policy.stop(); self.module_patch.stop(); self.temp.cleanup()

    def fake_run(self, command, **kwargs):
        self.calls.append(command)
        if self.fail and self.fail(command): raise RuntimeError('injected failure')
        stdout = ''
        if command[-2:] == ['rev-parse', 'HEAD']: stdout = 'f' * 40
        if '--property=ActiveState' in command:
            stdout = self.service_states.pop(0) if self.service_states else 'inactive'
        if '--property=LoadState' in command: stdout = 'loaded'
        code = 0
        if command == ['systemctl', 'is-active', '--quiet', 'erp-telegram-release.timer']:
            code = 0 if self.active else 3
        return SimpleNamespace(returncode=code, stdout=stdout)

    def refresh(self, activate=False, reconcile=()):
        i.refresh(self.source, 'a' * 24, self.target, self.units, self.fake_run,
                  self.root / 'state', self.root / 'deploy.lock', activate_supervised=activate,
                  reconcile_uncertain=reconcile)

    def test_refresh_timer_race_aborts_before_files_or_consumer_restart(self):
        self.service_states = ['inactive', 'activating']
        before = {p: p.read_bytes() for p in self.target.iterdir()}
        with self.assertRaisesRegex(ValueError, 'publication_active_refresh'):
            self.refresh(True)
        self.assertEqual({p: p.read_bytes() for p in self.target.iterdir()}, before)
        self.assertFalse(any(c[1] in ('restart', 'daemon-reload', 'enable', 'start')
                             for c in self.calls if c[0] == 'systemctl'))

    def test_install_timer_race_aborts_before_backup_or_module_writes(self):
        baseline = b"def handle(query, config, state, telegram, now=None):\n    pass\n"
        self.consumer.write_bytes(baseline)
        before = {p: p.read_bytes() for p in self.target.iterdir()}
        self.service_states = ['inactive', 'active']
        with patch.object(i, 'BASELINE', i.hashlib.sha256(baseline).hexdigest()):
            with self.assertRaisesRegex(ValueError, 'publication_active_install'):
                i.install(self.source, self.target, self.units, self.fake_run,
                          self.root / 'state', preflight=lambda: None)
        self.assertEqual({p: p.read_bytes() for p in self.target.iterdir()}, before)
        self.assertFalse(any(c[1] in ('restart', 'daemon-reload', 'enable', 'start')
                             for c in self.calls if c[0] == 'systemctl'))

    def test_explicit_resolution_runs_under_both_locks_before_activation(self):
        resolutions = []
        held = [False]
        @contextmanager
        def worker_lock(**kwargs):
            self.assertEqual(kwargs, {'worker': True})
            held[0] = True
            try: yield
            finally: held[0] = False
        self.ledger.lock = worker_lock
        def resolve(nonce, current_nonce, api):
            self.assertTrue(held[0])
            self.assertIs(api, self.api)
            with (self.root / 'deploy.lock').open('r+') as lock:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIn(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'], self.calls)
            self.assertNotIn(['systemctl', 'enable', '--now', 'erp-telegram-release.timer'], self.calls)
            resolutions.append((nonce, current_nonce))
        self.ledger.resolve_fixture = resolve
        (self.source / 'erp_release_host.py').write_text(
            'def verify_installed_runtime(release):\n'
            '    return {"release":release,"verified":True,"fixture":"new_source"}\n')
        (self.source / 'erp_release_resolution.py').write_text(
            'def resolve_legacy(nonce,current_nonce,api,ledger,*,runtime_verify):\n'
            '    proof=runtime_verify(ledger.read(current_nonce)["release_sha"])\n'
            '    assert proof=={"release":"f"*40,"verified":True,"fixture":"new_source"}\n'
            '    return ledger.resolve_fixture(nonce,current_nonce,api)\n')
        before = dict(self.row)
        def stale_runtime(_release):
            raise AssertionError('stale imported host must not be used')
        with patch.dict(sys.modules, {'erp_release_host': SimpleNamespace(verify_installed_runtime=stale_runtime)}):
            self.refresh(True, ('b' * 24, 'c' * 24))
        self.assertEqual(resolutions, [('b' * 24, 'a' * 24), ('c' * 24, 'a' * 24)])
        self.assertEqual(self.row, before)

    def test_resolution_never_implicit_and_failure_keeps_timer_paused(self):
        (self.source / 'erp_release_resolution.py').write_text(
            'def resolve_legacy(*args, runtime_verify):\n    raise ValueError("resolution_guard")\n')
        (self.source / 'erp_release_host.py').write_text(
            'def verify_installed_runtime(release):\n    raise AssertionError("not reached")\n')
        self.refresh()  # No operator nonce: resolver is not invoked.
        self.calls.clear()
        with self.assertRaisesRegex(ValueError, 'resolution_guard'):
            self.refresh(True, ('b' * 24,))
        self.assertNotIn(['systemctl', 'enable', '--now', 'erp-telegram-release.timer'], self.calls)
        self.assertEqual(self.calls[-1], ['systemctl', 'stop', 'erp-telegram-release.timer'])
        self.assertEqual((self.target / i.MODULES[0]).read_text(), 'NEW = True\n')

    def test_reconciliation_nonce_list_rejected_before_lock_or_mutation(self):
        for nonces in [('bad',), ('a' * 24,), ('b' * 24, 'b' * 24)]:
            with self.subTest(nonces=nonces), self.assertRaisesRegex(ValueError, 'invalid_explicit'):
                self.refresh(True, nonces)
        self.assertFalse((self.root / 'deploy.lock').exists())
        self.assertEqual(self.calls, [])

    def test_cli_repeatable_reconciliation_requires_current_release(self):
        with patch.object(sys, 'argv', ['install.py', '--reconcile-uncertain', 'b' * 24]):
            with self.assertRaises(SystemExit): i.main()
        arguments = ['install.py', '--release-nonce', 'a' * 24, '--activate-supervised',
                     '--reconcile-uncertain', 'b' * 24, '--reconcile-uncertain', 'c' * 24]
        with patch.object(sys, 'argv', arguments), patch.object(i.os, 'geteuid', return_value=0), \
             patch.object(i, '__file__', '/opt/pastelerias-erp/ops/erp-telegram-release/install.py'), \
             patch.object(i.subprocess, 'check_output', return_value=b''), patch.object(i, 'refresh') as refresh:
            i.main()
        self.assertEqual(refresh.call_args.args[1], 'a' * 24)
        self.assertEqual(refresh.call_args.kwargs,
                         {'activate_supervised': True, 'reconcile_uncertain': ['b' * 24, 'c' * 24]})

    def test_new_modules_and_service_installed_but_paused(self):
        self.refresh()
        for name in i.MODULES:
            self.assertEqual((self.target / name).read_text(), 'NEW = True\n')
            self.assertEqual((self.target / name).stat().st_mode & 0o777, 0o600)
        self.assertIn('NEW', (self.units / i.UNITS[0]).read_text())
        self.assertEqual(self.consumer.read_bytes(), self.original)
        self.assertNotIn(['systemctl', 'start', 'erp-telegram-release.timer'], self.calls)
        self.assertNotIn(['systemctl', 'enable', '--now', 'erp-telegram-release.timer'], self.calls)

    def test_explicit_activation_after_receiver_health(self):
        self.refresh(True)
        activate = self.calls.index(['systemctl', 'enable', '--now', 'erp-telegram-release.timer'])
        healthy = self.calls.index(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'])
        self.assertGreater(activate, healthy)
        self.assertEqual([c for c in self.calls if c[:2] == ['systemctl', 'restart']],
                         [['systemctl', 'restart', 'erp-point-decision.service']])

    def test_original_active_restored_without_flag(self):
        self.active = True; self.refresh()
        self.assertIn(['systemctl', 'start', 'erp-telegram-release.timer'], self.calls)

    def test_missing_module_aborts_before_writes(self):
        (self.source / i.MODULES[-1]).unlink()
        with self.assertRaises(FileNotFoundError): self.refresh(True)
        self.assertEqual(self.calls, [])
        self.assertFalse((self.root / 'deploy.lock').exists())
        self.assertFalse((self.target / 'module-backups').exists())

    def test_invalid_module_aborts_before_writes(self):
        (self.source / i.MODULES[-1]).write_text('def broken(')
        with self.assertRaises(SyntaxError): self.refresh(True)
        self.assertEqual(self.calls, [])

    def test_exact_published_revision_mandatory(self):
        for change in ({'release_sha': 'e' * 40}, {'state': 'approved'}, {'decision': 'rejected'}):
            original = dict(self.row); self.row.update(change)
            with self.assertRaisesRegex(ValueError, 'exact_published'): self.refresh(True)
            self.row = original
        self.assertFalse(any(c[0] == 'systemctl' for c in self.calls))

    def test_policy_failure_no_install(self):
        with patch.object(i, 'check_policy', side_effect=ValueError('protection')):
            with self.assertRaisesRegex(ValueError, 'protection'): self.refresh(True)
        self.assertEqual((self.target / i.MODULES[0]).read_text(), 'OLD = True\n')
        self.assertFalse((self.target / 'module-backups').exists())

    def test_activation_failure_keeps_new_code_paused(self):
        self.fail = lambda c: c == ['systemctl', 'enable', '--now', 'erp-telegram-release.timer']
        with self.assertRaises(RuntimeError): self.refresh(True)
        self.assertEqual((self.target / i.MODULES[0]).read_text(), 'NEW = True\n')
        self.assertEqual(self.calls[-1], ['systemctl', 'stop', 'erp-telegram-release.timer'])
        self.assertEqual(self.consumer.read_bytes(), self.original)

    def test_rollback_missing_backups_does_not_delete_new_files(self):
        failures = [0]
        def fail(c):
            if c == ['systemctl', 'restart', 'erp-point-decision.service']:
                failures[0] += 1
                return failures[0] == 1
            return False
        self.fail = fail
        with self.assertRaises(RuntimeError): self.refresh(True)
        self.assertEqual((self.target / i.MODULES[0]).read_text(), 'OLD = True\n')
        self.assertEqual((self.target / i.MODULES[-1]).read_text(), 'NEW = True\n')
        self.assertEqual(self.consumer.read_bytes(), self.original)
        self.assertNotIn(['systemctl', 'enable', '--now', 'erp-telegram-release.timer'], self.calls)

    def test_foreign_edit_not_overwritten_on_rollback(self):
        def fail(c):
            if c == ['systemctl', 'daemon-reload']:
                (self.target / i.MODULES[0]).write_text('FOREIGN = True\n')
                return True
            return False
        self.fail = fail
        with self.assertRaisesRegex(ValueError, 'rollback_concurrent'): self.refresh()
        self.assertEqual((self.target / i.MODULES[0]).read_text(), 'FOREIGN = True\n')

    def test_fresh_install_activation_failure_preserves_compatible_modules(self):
        baseline = b"def handle(query, config, state, telegram, now=None):\n    pass\n"
        self.consumer.write_bytes(baseline)
        self.fail = lambda c: c == ['systemctl', 'enable', '--now', 'erp-telegram-release.timer']
        with patch.object(i, 'BASELINE', i.hashlib.sha256(baseline).hexdigest()):
            with self.assertRaises(RuntimeError):
                i.install(self.source, self.target, self.units, self.fake_run,
                          self.root / 'fresh-state', preflight=lambda: None)
        self.assertEqual((self.target / i.MODULES[0]).read_text(), 'NEW = True\n')
        self.assertIn(b'from erp_telegram_release import handle', self.consumer.read_bytes())
        self.assertEqual(self.calls[-1], ['systemctl', 'stop', 'erp-telegram-release.timer'])

    def test_fresh_missing_module_does_not_create_state(self):
        (self.source / i.MODULES[-1]).unlink()
        with self.assertRaises(FileNotFoundError):
            i.install(self.source, self.target, self.units, self.fake_run,
                      self.root / 'fresh-state', preflight=lambda: None)
        self.assertFalse((self.root / 'fresh-state').exists())
        self.assertEqual(self.calls, [])

    def test_source_symlink_rejected(self):
        source = self.source / i.MODULES[-1]
        source.unlink(); source.symlink_to(self.source / i.MODULES[0])
        with self.assertRaises(OSError): self.refresh()
        self.assertEqual(self.calls, [])

    def test_private_backup_and_no_environment_write(self):
        self.refresh()
        self.assertEqual((self.target / 'module-backups').stat().st_mode & 0o777, 0o700)
        for path in (self.target / 'module-backups').iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertFalse(list(self.root.rglob('.env')))


if __name__ == '__main__': unittest.main()
