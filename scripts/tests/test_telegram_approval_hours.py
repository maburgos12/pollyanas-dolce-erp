"""Offline clocks, restart and concurrent delivery: never contact Telegram or NAS."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
import json
import hashlib
import importlib.util
import tempfile
import unittest
from zoneinfo import ZoneInfo

from scripts.tests import test_telegram_release as fixture
from scripts.tests import test_telegram_cleanup as cleanup_fixture
from scripts.tests import test_cleanup_prepare as prepare_fixture

r = fixture.r
c = cleanup_fixture.c

def epoch(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=ZoneInfo('America/Mazatlan')).timestamp())

class ApprovalHoursTests(unittest.TestCase):
    def setUp(self):
        self.f = fixture.ReleaseTests()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.f.ledger.path(self.f.row['nonce']).unlink()
        self.send = Mock(return_value={'message_id': 99, 'chat': {'id': 17, 'type': 'private'}})

    def request(self):
        return r.request_release(1, self.f.api, self.f.ledger, self.f.config, self.send,
                                 lambda: fixture.B, fixture.H)

    def test_boundaries_in_local_timezone(self):
        for clock, allowed in [('06:59:59', False), ('07:00:00', True), ('19:59:59', True),
                               ('20:00:00', False), ('00:00:00', False)]:
            with self.subTest(clock=clock):
                self.assertEqual(r.approval_window(epoch('2026-11-17T'+clock)), (allowed, '2026-11-17'))

    def test_night_has_no_nonce_then_seven_revalidates_after_restart(self):
        with patch('time.time', return_value=epoch('2026-11-17T02:00:00')):
            row = self.request()
            self.assertEqual(row['state'], 'deferred')
            self.assertIsNone(row['nonce'])
            self.assertFalse(list(self.f.ledger.directory.glob('*.json')))
            self.send.assert_not_called()
        self.f.ledger = r.Ledger(self.f.ledger.directory)
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            with patch.object(r, 'operational_module') as module:
                r.process_scheduled(self.f.api, self.f.ledger, self.f.config, self.send, lambda: fixture.B)
        self.assertEqual(self.send.call_count, 1)
        self.assertIn('Mazatlán', self.send.call_args.args[2]['text'])
        self.assertIn('reply_markup', self.send.call_args.args[2])

    def test_changed_revision_or_ci_never_sends_at_seven(self):
        for change in ('head', 'base', 'ci'):
            with self.subTest(change=change):
                self.f.pr_head, self.f.main = fixture.H, fixture.B
                self.f.checks[0]['conclusion'] = 'success'
                ledger = r.Ledger(self.f.ledger.directory / change)
                with patch('time.time', return_value=epoch('2026-11-17T03:00:00')):
                    r.request_release(1, self.f.api, ledger, self.f.config, self.send, lambda: fixture.B, fixture.H)
                if change == 'head': self.f.pr_head = 'e'*40
                if change == 'base': self.f.main = 'e'*40
                if change == 'ci': self.f.checks[0]['conclusion'] = 'failure'
                with patch('time.time', return_value=epoch('2026-11-17T07:00:00')), self.assertRaises(ValueError):
                    r.request_release(1, self.f.api, ledger, self.f.config, self.send, lambda: fixture.B, fixture.H)
        self.send.assert_not_called()

    def test_expiry_waits_next_day_and_never_renews_a_decision(self):
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            first = self.request()
        with patch('time.time', return_value=epoch('2026-11-17T09:00:00')):
            self.assertEqual(self.request()['state'], 'deferred')
        with patch('time.time', return_value=epoch('2026-11-18T07:00:00')):
            second = self.request()
        self.assertNotEqual(first['nonce'], second['nonce'])
        second.update(state='rejected', decision='rejected', decision_at=epoch('2026-11-18T07:01:00'))
        self.f.ledger.write(second)
        with patch('time.time', return_value=epoch('2026-11-19T07:00:00')), self.assertRaises(ValueError):
            self.request()
        self.assertEqual(self.send.call_count, 2)

    def test_concurrent_requests_create_one_message(self):
        def run():
            try: return self.request()['state']
            except ValueError: return 'blocked'
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: run(), range(2)))
        self.assertCountEqual(results, ['pending', 'blocked'])
        self.assertEqual(self.send.call_count, 1)
        self.assertEqual(len(list(self.f.ledger.directory.glob('*.json'))), 1)

    def test_lost_response_and_restart_never_replays_send(self):
        self.send.side_effect = RuntimeError('lost response')
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')), self.assertRaises(RuntimeError):
            self.request()
        self.f.ledger = r.Ledger(self.f.ledger.directory)
        with patch('time.time', return_value=epoch('2026-11-18T07:00:00')), self.assertRaises(ValueError):
            self.request()
        self.assertEqual(self.send.call_count, 1)

    def test_revalidation_crossing_twenty_does_not_send(self):
        clock = [epoch('2026-11-17T19:59:59')]
        check = lambda: clock.__setitem__(0, epoch('2026-11-17T20:00:00'))
        with patch('time.time', side_effect=lambda: clock[0]):
            result = r.schedule_request('release', {'pr': 1, 'head': fixture.H}, self.f.ledger,
                                        lambda _: False, self.send, check)
        self.assertEqual(result['state'], 'deferred')
        self.send.assert_not_called()

    def test_rejection_during_preflight_is_not_renewed(self):
        row = dict(self.f.row, expires=epoch('2026-11-16T08:00:00'))
        self.f.ledger.write(row)
        original = r.eligible
        def reject(*args, **kwargs):
            result = original(*args, **kwargs)
            self.f.ledger.write(dict(row, state='rejected', decision='rejected', decision_at=1))
            return result
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')), patch.object(r, 'eligible', side_effect=reject):
            with self.assertRaisesRegex(ValueError, 'existing_request'):
                self.request()
        self.send.assert_not_called()

    def test_two_legacy_prs_are_both_revalidated(self):
        for number, nonce in [(1, 'a'*24), (2, 'b'*24)]:
            row = dict(self.f.row, pr=number, nonce=nonce, expires=epoch('2026-11-16T08:00:00'))
            self.f.ledger.write(row)
        def api(method, endpoint, data=None):
            return self.f.api(method, endpoint.replace('/pulls/2', '/pulls/1'), data)
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')), patch.object(r, 'operational_module'):
            r.process_scheduled(api, self.f.ledger, self.f.config, self.send, lambda: fixture.B)
        self.assertEqual(self.send.call_count, 2)
        pending = [self.f.ledger.read(p.stem) for p in self.f.ledger.directory.glob('*.json')]
        self.assertEqual(sorted(row['pr'] for row in pending if row['state']=='pending'), [1, 2])

    def test_second_preflight_crosses_twenty_and_can_retry_tomorrow(self):
        clock = [epoch('2026-11-17T19:59:59')]
        calls = [0]
        original = r.eligible
        def check(*args, **kwargs):
            result = original(*args, **kwargs)
            calls[0] += 1
            if calls[0] == 2: clock[0] = epoch('2026-11-17T20:00:00')
            return result
        with patch('time.time', side_effect=lambda: clock[0]), patch.object(r, 'eligible', side_effect=check):
            with self.assertRaisesRegex(ValueError, 'outside_approval_hours'): self.request()
            self.send.assert_not_called()
            clock[0] = epoch('2026-11-18T07:00:00')
            self.assertEqual(self.request()['state'], 'pending')
        self.assertEqual(self.send.call_count, 1)

class CleanupHoursTests(unittest.TestCase):
    def setUp(self):
        self.f = cleanup_fixture.CleanupTests()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.f.ledger = r.Ledger(Path(self.f.temp.name) / 'hours', validator=c.validate)
        self.f.send.reset_mock()

    def test_recovery_pending_preserves_day_and_retries_same_intent_once(self):
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            with patch.object(c, 'revalidate_proposal', side_effect=c.RecoveryPending('backup_recovery_pending')):
                for _ in range(2):
                    result = c.request(self.f.path, self.f.ledger, self.f.config, self.f.send)
                    self.assertEqual(result['reason'], 'backup_recovery_pending')
            intent_path = next((self.f.ledger.directory / 'scheduled').glob('*.json'))
            self.assertEqual(json.loads(intent_path.read_text())['last_day'], '')
            self.f.send.assert_not_called()
            c.process_scheduled(self.f.ledger, self.f.config, self.f.send)
            c.process_scheduled(self.f.ledger, self.f.config, self.f.send)
            self.assertEqual(self.f.send.call_count, 2)

    def test_late_health_gate_before_send_does_not_consume_day(self):
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            with patch.object(c, 'require_recovery', side_effect=c.RecoveryPending('backup_recovery_pending')):
                result = c.request(self.f.path, self.f.ledger, self.f.config, self.f.send)
            self.assertEqual(result['state'], 'deferred')
            self.assertFalse(list(self.f.ledger.directory.glob('*.json')))
            self.f.send.assert_not_called()
            self.assertEqual(c.request(self.f.path, self.f.ledger, self.f.config, self.f.send)['state'], 'pending')
            self.assertEqual(self.f.send.call_count, 2)

    def test_late_health_check_crossing_twenty_never_sends(self):
        clock = [epoch('2026-11-17T19:59:59')]
        def slow_health():
            clock[0] = epoch('2026-11-17T20:00:00')
        with patch('time.time', side_effect=lambda: clock[0]), patch.object(c, 'require_recovery', side_effect=slow_health):
            with self.assertRaisesRegex(ValueError, 'outside_approval_hours'):
                c.request(self.f.path, self.f.ledger, self.f.config, self.f.send)
        self.f.send.assert_not_called()
        self.assertFalse(list(self.f.ledger.directory.glob('*.json')))

    def test_deferred_cleanup_recovery_failure_creates_no_buttons(self):
        with patch('time.time', return_value=epoch('2026-11-17T02:00:00')):
            result = c.request(self.f.path, self.f.ledger, self.f.config, self.f.send)
        self.assertIsNone(result['nonce'])
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            with patch.object(c, 'revalidate_proposal', side_effect=ValueError('NAS changed')):
                c.process_scheduled(self.f.ledger, self.f.config, self.f.send)
        self.f.send.assert_not_called()
        self.assertFalse(list(self.f.ledger.directory.glob('*.json')))

    def test_deferred_cleanup_emits_once_then_rejection_stays_terminal(self):
        with patch('time.time', return_value=epoch('2026-11-17T02:00:00')):
            c.request(self.f.path, self.f.ledger, self.f.config, self.f.send)
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            c.process_scheduled(self.f.ledger, self.f.config, self.f.send)
            c.process_scheduled(self.f.ledger, self.f.config, self.f.send)
        self.assertEqual(self.f.send.call_count, 2)  # One message and its bound buttons.
        row = self.f.ledger.read(next(self.f.ledger.directory.glob('*.json')).stem)
        row.update(state='rejected', decision_at=epoch('2026-11-17T07:01:00'))
        self.f.ledger.write(row)
        with patch('time.time', return_value=epoch('2026-11-18T07:00:00')):
            c.process_scheduled(self.f.ledger, self.f.config, self.f.send)
        self.assertEqual(self.f.send.call_count, 2)

    def test_legacy_image_has_no_automatic_recovery_claim(self):
        with patch.object(c, 'revalidate_proposal', wraps=CleanupHoursTests.original_revalidator):
            with self.assertRaisesRegex(ValueError, 'operator_review'):
                c.revalidate_proposal(self.f.proposal, self.f.ledger.directory)

    def test_same_content_different_proposals_cannot_race(self):
        second = self.f.path.with_name('second.json')
        second.write_text(json.dumps(dict(self.f.proposal, id='VPS-LIM-20261111-02')))
        def run(path):
            try: return c.request(path, self.f.ledger, self.f.config, self.f.send)['state']
            except ValueError: return 'blocked'
        with patch('time.time', return_value=epoch('2026-11-17T07:00:00')):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(run, [self.f.path, second]))
        self.assertCountEqual(results, ['pending', 'blocked'])
        self.assertEqual(self.f.send.call_count, 2)

    def test_actual_archive_readback_and_corruption(self):
        prep = prepare_fixture.p
        body = b'rotated log\n'
        entry = dict(path='/var/log/syslog.1', bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), metadata=[])
        nas = prepare_fixture.FakeNAS(body)
        digest, size = nas.upload(self.f.proposal['id'], [entry])
        nas.command = Mock(return_value=str(size))
        item = self.f.manifest()
        item.update(files=[{k: entry[k] for k in ('path', 'bytes', 'sha256')}], build_cache=[])
        item['recovery']['sha256'] = digest
        self.f.proposal['delete_candidates'] = [item]
        with patch.object(c, 'operational_module', return_value=prep), patch.object(prep, 'NAS', return_value=nas), \
             patch.object(prep, 'snapshot', return_value=entry), patch.object(prep, 'unchanged') as unchanged, \
             patch.object(c.shutil, 'disk_usage', return_value=Mock(free=20*1024**3)):
            self.original_revalidator(self.f.proposal, self.f.ledger.directory)
            self.assertIn('readback', nas.events)
            self.assertEqual(unchanged.call_count, 2)
            self.assertFalse(list(self.f.ledger.directory.glob('approval-recovery-*')))
            nas.fail = 'readback'
            with self.assertRaisesRegex(ValueError, 'nas_readback_changed'):
                self.original_revalidator(self.f.proposal, self.f.ledger.directory)
            self.assertFalse(list(self.f.ledger.directory.glob('approval-recovery-*')))
            nas.fail = 'receipt'
            with self.assertRaisesRegex(ValueError, 'nas_recovery_changed'):
                self.original_revalidator(self.f.proposal, self.f.ledger.directory)

    def test_actual_operational_imports_preserve_recovery_functions(self):
        self.assertTrue(callable(r.operational_module('erp_cleanup_prepare').verify_archive))
        self.assertTrue(callable(r.operational_module('erp_telegram_cleanup').process_scheduled))
        with self.assertRaisesRegex(ValueError, 'unknown_operational_module'):
            r.operational_module('../unknown')

    original_revalidator = staticmethod(c.revalidate_proposal)

class RefreshInstallTests(unittest.TestCase):
    def test_refresh_exact_approval_and_preserve_consumer(self):
        spec = importlib.util.spec_from_file_location('hours_install', fixture.SPEC.origin.replace('erp_telegram_release.py', 'install.py'))
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'app/ops/erp-telegram-release'
            target, units = root / 'installed', root / 'units'
            for path in (source, target, units): path.mkdir(parents=True)
            for name in ('erp_telegram_release.py', 'erp_telegram_cleanup.py'):
                (source / name).write_text('updated = True\n')
                (target / name).write_text('old = True\n')
            timer = 'erp-telegram-release.timer'
            (source / timer).write_text('new timer')
            (units / timer).write_text('old timer')
            consumer = target / 'decidir_corte_point.py'
            consumer.write_text('from erp_telegram_release import handle\nfrom erp_telegram_cleanup import handle\n# Mac-IA preserved\n')
            original = consumer.read_bytes()
            state = root / 'releases'
            ledger = r.Ledger(state)
            row = dict(repo=r.REPO, pr=1, head_sha=fixture.H, base_sha=fixture.B,
                       production_sha=fixture.B, nonce='a'*24, expires=2, message_id=42,
                       chat_id=17, state='published', decision='approved', decision_at=1,
                       release_sha=fixture.M, notification_id=None)
            ledger.write(row)
            calls = []
            def run(command, **kwargs):
                calls.append(command)
                return Mock(returncode=0, stdout=fixture.M if command[-2:] == ['rev-parse', 'HEAD'] else '')
            with patch.object(installer, 'check_policy'):
                installer.refresh(source, row['nonce'], target, units, run, state, root / 'deploy.lock')
            self.assertEqual(consumer.read_bytes(), original)
            self.assertEqual((target / 'erp_telegram_release.py').read_text(), 'updated = True\n')
            self.assertTrue(list((target / 'module-backups').glob('*')))
            self.assertNotIn(['systemctl', 'disable', '--now', 'erp-mobile-approval.timer'], calls)
            (source / 'erp_telegram_release.py').write_text('newer = True\n')
            failed = [False]
            def failing(command, **kwargs):
                if command == ['systemctl', 'restart', 'erp-point-decision.service'] and not failed[0]:
                    failed[0] = True
                    raise RuntimeError('restart failed')
                return run(command, **kwargs)
            with patch.object(installer, 'check_policy'), self.assertRaisesRegex(RuntimeError, 'restart failed'):
                installer.refresh(source, row['nonce'], target, units, failing, state, root / 'deploy.lock')
            self.assertEqual((target / 'erp_telegram_release.py').read_text(), 'updated = True\n')
            self.assertEqual(consumer.read_bytes(), original)
            row['release_sha'] = fixture.B
            ledger.write(row)
            calls.clear()
            with self.assertRaisesRegex(ValueError, 'exact_published_approval'):
                installer.refresh(source, row['nonce'], target, units, run, state, root / 'deploy.lock')
            self.assertFalse(any(command[0] == 'systemctl' for command in calls))

if __name__ == '__main__':
    unittest.main()
