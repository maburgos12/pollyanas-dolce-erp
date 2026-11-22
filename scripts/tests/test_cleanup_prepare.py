"""Offline recovery and failure tests: no SSH, Telegram or production writes."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import sys
import tarfile
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

MODULES = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release'
sys.path.insert(0, str(MODULES))
spec = importlib.util.spec_from_file_location('erp_cleanup_prepare', MODULES / 'erp_cleanup_prepare.py')
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class OldInstalledContractTests(unittest.TestCase):
    def test_checkout_imports_with_old_installed_modules_defer_before_work(self):
        old_cleanup = types.ModuleType('erp_telegram_cleanup')
        old_release = types.ModuleType('erp_telegram_release')
        old_release.Ledger = p.Ledger
        old_release.telegram = Mock()
        old_release.approval_window = lambda: (True, '2026-11-19')
        with patch.dict(sys.modules, erp_telegram_cleanup=old_cleanup, erp_telegram_release=old_release):
            spec = importlib.util.spec_from_file_location('pending_prepare', MODULES / 'erp_cleanup_prepare.py')
            pending = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(pending)
            self.assertEqual(pending.prepare(Path('/must-not-write'), None, {}), 'activation_pending')
            with patch.dict(sys.modules, erp_cleanup_prepare=pending):
                spec = importlib.util.spec_from_file_location('pending_point', MODULES / 'erp_point_archive.py')
                point = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(point)
                broker = object.__new__(point.Broker)
                self.assertEqual(broker.prepare(None, {}), 'activation_pending')
        old_release.telegram.assert_not_called()


class FakeNAS:
    def __init__(self, contents=b'rotated log\n', fail=None):
        self.contents, self.fail, self.events = contents, fail, []

    def path(self, identity):
        return f'{p.NAS_ROOT}/{identity}/files-recovery.tar.gz'

    def reserve(self, identity, required):
        self.events.append('reserve')
        if self.fail == 'capacity':
            raise ValueError('nas_capacity')

    def upload(self, identity, entries):
        self.events.append('upload')
        out = io.BytesIO()
        with tarfile.open(fileobj=out, mode='w:gz') as archive:
            for entry in entries:
                info = tarfile.TarInfo(entry['path'].lstrip('/'))
                info.size = len(self.contents)
                archive.addfile(info, io.BytesIO(self.contents))
        self.data = out.getvalue()
        self.hash = hashlib.sha256(self.data).hexdigest()
        return self.hash, len(self.data)

    def digest(self, identity):
        self.events.append('receipt')
        return '0' * 64 if self.fail == 'receipt' else self.hash

    def download(self, identity, destination, size):
        self.events.append('readback')
        if self.fail == 'download':
            raise TimeoutError()
        destination.write_bytes(self.data + b'corrupt' if self.fail == 'readback' else self.data)


class PrepareTests(unittest.TestCase):
    def setUp(self):
        health = patch.object(p.cleanup, 'require_recovery', create=True)
        health.start()
        self.addCleanup(health.stop)
        recovery = patch.object(p.cleanup, "revalidate_proposal")
        recovery.start()
        self.addCleanup(recovery.stop)
        clock = patch("time.time", return_value=1794938400)
        clock.start()
        self.addCleanup(clock.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root / 'prepare'
        self.state.mkdir(mode=0o700)
        self.ledger = p.Ledger(self.root / 'ledger', validator=p.cleanup.validate)
        self.contents = b'rotated log\n'
        self.entries = [dict(path='/var/log/syslog.1', bytes=len(self.contents), sha256=hashlib.sha256(self.contents).hexdigest(), metadata=[1, 2, len(self.contents), 3, 4, 33152, 1])]
        self.nas = FakeNAS(self.contents)
        self.config = dict(token='offline', chat_id=17)
        self.sent = []
        def send(config, method, payload):
            self.sent.append((method, payload))
            return dict(message_id=42, chat=dict(id=17, type='private'), **({'reply_markup': payload['reply_markup']} if 'reply_markup' in payload else {}))
        self.send = send
        for name, replacement in [('disk_percent', Mock(return_value=80)), ('MANUAL_PROPOSALS', self.root / 'manual'), ('candidates', Mock(return_value=self.entries)), ('unchanged', Mock()), ('disk_usage', None)]:
            target = 'shutil.disk_usage' if name == 'disk_usage' else name
            value = replacement if replacement is not None else Mock(return_value=Mock(total=100, used=80, free=20 * 1024**3))
            handle = patch.object(p, target, value) if '.' not in target else patch.object(p.shutil, 'disk_usage', value)
            handle.start()
            self.addCleanup(handle.stop)

    def run_once(self, now=1_800_000_000):
        return p.prepare(self.state, self.ledger, self.config, self.nas, self.send, now)

    def row(self, state='pending', expired=False, identity=None):
        row = dict(proposal_id='VPS-LIM-20260101-01', proposal_sha256='a'*64,
                   image_id=identity or p.manifest_id([{k: e[k] for k in ('path', 'bytes', 'sha256')} for e in self.entries]),
                   nonce='a'*24, expires=1 if expired else 2_000_000_000, chat_id=17,
                   message_id=None if state == 'notifying' else 42, state=state,
                   decision_at=2 if state in {'approved', 'rejected'} else None)
        self.ledger.write(row)

    def test_old_installed_contract_waits_for_activation_without_work(self):
        with patch.object(p.cleanup, 'require_recovery', None):
            self.assertEqual(self.run_once(), 'activation_pending')
        self.assertFalse(self.nas.events)
        self.assertFalse(self.sent)
        self.assertFalse(list(self.state.glob('VPS-LIM-*')))
        self.assertFalse((self.state / 'status.json').exists())

    def test_recovery_pending_is_quiet_and_retries_without_spending_attempts(self):
        with patch.object(p.cleanup, 'require_recovery', side_effect=p.cleanup.RecoveryPending('backup_recovery_pending')):
            for now in (1_800_000_000, 1_800_000_300):
                self.assertEqual(self.run_once(now), 'recovery_pending')
        self.assertFalse(list(self.state.glob('VPS-LIM-*')))
        self.assertFalse((self.state / 'status.json').exists())
        self.assertFalse(self.nas.events)
        self.assertFalse(self.sent)
        self.assertEqual(self.run_once(1_800_000_301), 'requested')

    def test_recovery_lost_before_buttons_keeps_exact_prepared_intent(self):
        with patch.object(p.cleanup, 'require_recovery', side_effect=[None, p.cleanup.RecoveryPending('backup_recovery_pending')]):
            self.assertEqual(self.run_once(), 'recovery_pending')
        evidence = [path for path in self.state.glob('VPS-LIM-*.json')]
        self.assertEqual(len(evidence), 1)
        self.assertEqual(json.loads(evidence[0].read_text())['phase'], 'requesting')
        self.assertFalse((self.state / 'status.json').exists())
        self.assertFalse(self.sent)
        self.assertEqual(self.run_once(), 'existing_scope_or_request')
        p.cleanup.process_scheduled(self.ledger, self.config, self.send)
        self.assertEqual(len(self.sent), 2)
        self.assertEqual(len(list(self.state.glob('VPS-LIM-*.json'))), 1)

    def test_threshold_strictly_greater_than_75(self):
        for used in (0, 74, 75):
            p.disk_percent.return_value = used
            self.assertEqual(self.run_once(), 'below_threshold')
        self.assertFalse(self.sent)
        self.assertFalse(self.nas.events)

    def test_verified_recovery_before_request_and_no_duplicate(self):
        def send(config, method, payload):
            self.assertEqual(self.nas.events, ['reserve', 'upload', 'receipt', 'readback'])
            self.assertFalse(list(self.state.glob('verify-*')))
            return self.send(config, method, payload)
        self.assertEqual(p.prepare(self.state, self.ledger, self.config, self.nas, send, 1_800_000_000), 'requested')
        proposal_path = next(self.state.glob('*.proposal'))
        proposal, _ = p.cleanup.read_proposal(proposal_path)
        self.assertEqual(proposal['delete_candidates'][0]['recovery']['files_verified'], 1)
        self.assertEqual(self.run_once(), 'existing_scope_or_request')
        self.assertEqual(len(self.sent), 2)

    def test_failures_never_request_and_back_off(self):
        for failure in ('capacity', 'receipt', 'readback', 'download'):
            with self.subTest(failure=failure):
                state = self.state / failure
                state.mkdir(mode=0o700)
                nas, send = FakeNAS(self.contents, failure), Mock()
                self.assertEqual(p.prepare(state, self.ledger, self.config, nas, send, 1_800_000_000), 'blocked')
                self.assertEqual(len(list(self.ledger.directory.glob('*.json'))), 0)
                self.assertEqual(send.call_count, 1)  # Bounded failure notification only.
                self.assertFalse(list(state.glob('verify-*')))
                self.assertEqual(p.prepare(state, self.ledger, self.config, nas, send, 1_800_000_001), 'backoff')
                self.assertEqual(send.call_count, 1)

    def test_oserror_diagnostic_preserves_errno_and_path_without_request(self):
        p.candidates.side_effect = OSError(5, 'Input/output error', '/proc/123/fdinfo/1')
        self.assertEqual(self.run_once(), 'blocked')
        status = json.loads((self.state / 'status.json').read_text())
        self.assertEqual(status['error'], {'type': 'OSError', 'errno': 5, 'filename': '/proc/123/fdinfo/1', 'message': 'Input/output error'})
        self.assertFalse(self.nas.events)
        self.assertFalse(list(self.ledger.directory.glob('*.json')))

    def test_local_capacity_blocks_before_nas(self):
        p.shutil.disk_usage.return_value = Mock(total=100, used=80, free=p.RESERVE)
        self.assertEqual(self.run_once(), 'blocked')
        self.assertFalse(self.nas.events)

    def test_changed_or_open_source_blocks_before_nas_upload(self):
        p.unchanged.side_effect = ValueError('source_changed_or_in_use')
        self.assertEqual(self.run_once(), 'blocked')
        self.assertNotIn('upload', self.nas.events)
        self.assertFalse(list(self.ledger.directory.glob('*.json')))

    def test_final_change_blocks_request(self):
        p.unchanged.side_effect = [None, None, None, ValueError('source_changed_or_in_use')]
        self.assertEqual(self.run_once(), 'blocked')
        self.assertFalse(list(self.ledger.directory.glob('*.json')))

    def test_all_historical_same_manifest_states_block(self):
        for state in ('notifying', 'pending', 'approved', 'rejected'):
            self.row(state, expired=True)
            self.assertEqual(self.run_once(), 'existing_scope_or_request')
        self.assertFalse(self.sent)

    def test_live_other_scope_blocks_but_expired_other_scope_does_not(self):
        self.row('approved', identity='sha256:'+'b'*64)
        self.assertEqual(self.run_once(), 'existing_scope_or_request')
        self.row('approved', expired=True, identity='sha256:'+'b'*64)
        self.assertEqual(self.run_once(), 'requested')

    def execution(self, state):
        row=self.ledger.read('a'*24)
        (self.ledger.directory/'executions').mkdir(mode=0o700,exist_ok=True)
        p.atomic_json(self.ledger.directory/'executions'/('a'*24+'.json'),
                      dict(nonce=row['nonce'],proposal_sha256=row['proposal_sha256'],state=state))

    def test_completed_other_scope_unblocks_without_changing_approval(self):
        self.row('approved',identity='sha256:'+'b'*64)
        before=self.ledger.read('a'*24)
        self.execution('completed')
        self.assertFalse(p.blocked(self.ledger,None,1_800_000_000))
        self.assertEqual(self.ledger.read('a'*24),before)
        self.assertTrue(p.blocked(self.ledger,before['image_id'],1_800_000_000))

    def test_unfinished_other_execution_blocks_even_after_expiry(self):
        for state in ('executing','partial','uncertain','blocked'):
            with self.subTest(state=state):
                self.row('approved',expired=True,identity='sha256:'+'b'*64)
                self.execution(state)
                self.assertTrue(p.blocked(self.ledger,None,1_800_000_000))

    def test_execution_with_wrong_digest_never_unblocks(self):
        self.row('approved',identity='sha256:'+'b'*64)
        path=self.ledger.directory/'executions'/('a'*24+'.json')
        path.parent.mkdir(mode=0o700,exist_ok=True)
        p.atomic_json(path,dict(nonce='a'*24,proposal_sha256='c'*64,state='completed'))
        with self.assertRaisesRegex(ValueError,'execution_proposal_mismatch'):
            p.blocked(self.ledger,None,1_800_000_000)

    def test_ambiguous_send_terminal_even_after_backoff(self):
        send = Mock(side_effect=TimeoutError())
        self.assertEqual(p.prepare(self.state, self.ledger, self.config, self.nas, send, 1_800_000_000), 'blocked')
        self.assertEqual(next(iter(self.ledger.directory.glob('*.json'))).is_file(), True)
        self.assertEqual(p.prepare(self.state, self.ledger, self.config, self.nas, send, 1_800_030_000), 'existing_scope_or_request')
        self.assertEqual(send.call_count, 2)  # Ambiguous request and failure report, never a request retry.
        self.assertEqual(json.loads((self.state / 'status.json').read_text())['alert_error'], 'TimeoutError')

    def test_recovery_attempt_limit(self):
        self.nas.fail = 'capacity'
        for n in range(4):
            self.assertEqual(self.run_once(1_800_000_000 + n * p.RETRY), 'blocked')
        self.assertEqual(self.nas.events.count('reserve'), 3)
        self.assertEqual(json.loads((self.state / 'status.json').read_text())['reason'], 'recovery_attempt_limit_requires_review')

    def test_manual_history_reserves_identical_content_only(self):
        self.assertEqual(self.run_once(), 'requested')
        proposal = next(self.state.glob('*.proposal'))
        p.MANUAL_PROPOSALS.mkdir()
        (p.MANUAL_PROPOSALS / 'historic.json').write_bytes(proposal.read_bytes())
        other = self.root / 'fresh'
        other.mkdir(mode=0o700)
        self.assertIn((self.entries[0]['path'], self.entries[0]['sha256']), p.reserved_files(other))
        self.assertNotIn((self.entries[0]['path'], 'a'*64), p.reserved_files(other))

    def test_no_candidates_reports_once_per_six_hours(self):
        p.candidates.return_value = []
        self.assertEqual(self.run_once(), 'blocked')
        self.assertEqual(self.run_once(1_800_000_001), 'backoff')
        self.assertEqual(len(self.sent), 1)
        self.assertIn('no_eligible_new_candidates', self.sent[0][1]['text'])

    def test_extract_rejects_unexpected_and_symlink_members(self):
        for kind in ('symlink', 'unexpected', 'missing', 'duplicate', 'hash'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory(dir=self.root) as directory:
                path, out = Path(directory) / 'a.tar.gz', Path(directory) / 'out'
                out.mkdir()
                with tarfile.open(path, 'w:gz') as archive:
                    if kind != 'missing':
                        info = tarfile.TarInfo('../escape' if kind == 'unexpected' else 'var/log/syslog.1')
                        info.size = len(self.contents)
                        if kind == 'symlink':
                            info.type, info.linkname = tarfile.SYMTYPE, '/etc/passwd'
                        archive.addfile(info, io.BytesIO(b'x'*len(self.contents) if kind == 'hash' else self.contents))
                        if kind == 'duplicate':
                            archive.addfile(info, io.BytesIO(self.contents))
                with self.assertRaises(ValueError):
                    p.verify_archive(path, self.entries, out)


class FileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def test_snapshot_rejects_symlink_hardlink_and_detects_change(self):
        source = self.root / 'syslog.1'
        source.write_bytes(b'log')
        with patch.object(p.cleanup, 'FILE_PATH', re.compile(re.escape(str(self.root)) + r'/.*')):
            before = p.snapshot(source)
            source.write_bytes(b'new')
            self.assertNotEqual(before, p.snapshot(source))
            link = self.root / 'link.1'
            link.symlink_to(source)
            with self.assertRaises(ValueError):
                p.snapshot(link)
            link.unlink()
            p.os.link(source, link)
            with self.assertRaises(ValueError):
                p.snapshot(source)

    def test_real_stream_archive_roundtrip(self):
        source = self.root / 'syslog.1'
        source.write_bytes(b'archive fixture'*100)
        with patch.object(p.cleanup, 'FILE_PATH', re.compile(re.escape(str(self.root)) + r'/.*')):
            entry = p.snapshot(source)
        archive = self.root / 'copy.tar.gz'
        with archive.open('wb') as stream:
            digest, size = p.write_archive(stream, [entry])
        self.assertEqual(digest, hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(size, archive.stat().st_size)
        destination = self.root / 'restore'
        destination.mkdir()
        p.verify_archive(archive, [entry], destination)
        self.assertEqual((destination/'1').read_bytes(), source.read_bytes())

    def proc_fixture(self, target='/unavailable/nas/log', mount_id=7):
        pid = self.root / '123'
        (pid / 'fdinfo').mkdir(parents=True)
        (pid / 'fd').mkdir()
        (pid / 'fd' / '1').symlink_to(target)
        (pid / 'fdinfo' / '1').write_text(f'pos:\t0\nflags:\t0100000\nmnt_id:\t{mount_id}\nino:\t42\n')
        (pid / 'mountinfo').write_text('7 1 8:2 / / rw - ext4 /dev/x rw\n')
        (pid / 'maps').write_text('1000-2000 r--p 00000000 08:02 43 /mapped/log (deleted)\n')
        return pid

    def test_proc_scan_fd_and_mapped_files_without_following_targets(self):
        self.proc_fixture()
        with patch.object(Path, 'stat', side_effect=AssertionError('must not stat remote targets')):
            used = p.in_use(self.root)
        self.assertEqual(used, {(p.os.makedev(8, 2), 42), (p.os.makedev(8, 2), 43)})

    def test_proc_scan_synthetic_descriptors_without_mount_reserve_only_inode(self):
        pid = self.proc_fixture(mount_id=99)
        for target in ('socket:[42]', 'pipe:[42]', 'anon_inode:[eventpoll]', 'net:[4026531840]'):
            with self.subTest(target=target):
                (pid / 'fd' / '1').unlink()
                (pid / 'fd' / '1').symlink_to(target)
                self.assertEqual(p.in_use(self.root), {(None, 42), (p.os.makedev(8, 2), 43)})

    def test_proc_scan_unknown_mount_reserves_inode_on_all_devices(self):
        self.proc_fixture(mount_id=99)
        self.assertIn((None, 42), p.in_use(self.root))
        with patch.object(p, 'in_use', return_value={(None, 42)}), patch.object(p, 'snapshot') as snapshot:
            with self.assertRaisesRegex(ValueError, 'source_changed_or_in_use'):
                p.unchanged([{'path': '/var/log/syslog.1', 'metadata': [123, 42]}])
            snapshot.assert_not_called()

    def test_proc_scan_permission_denied_fails_closed(self):
        self.proc_fixture()
        with patch.object(Path, 'read_text', side_effect=PermissionError(13, 'denied', '/proc/123/maps')):
            with self.assertRaises(PermissionError):
                p.in_use(self.root)

    def test_proc_scan_closed_descriptor_is_ignored(self):
        pid = self.proc_fixture()
        (pid / 'fdinfo' / '1').unlink()
        self.assertEqual(p.in_use(self.root), {(p.os.makedev(8, 2), 43)})

    def test_proc_scan_empty_process_does_not_read_mountinfo(self):
        pid = self.proc_fixture()
        (pid / 'fdinfo' / '1').unlink()
        (pid / 'mountinfo').unlink()
        self.assertEqual(p.in_use(self.root), {(p.os.makedev(8, 2), 43)})

    def test_unknown_mount_inode_is_excluded_from_candidates(self):
        walk = [('/var/log', [], ['syslog.1'])]
        with patch.object(p.os, 'walk', side_effect=[[], walk]), patch.object(p, 'in_use', return_value={(None, 42)}), patch.object(Path, 'lstat', return_value=Mock(st_dev=123, st_ino=42)), patch.object(p, 'snapshot') as snapshot:
            self.assertEqual(p.candidates(), [])
            snapshot.assert_not_called()

    def test_proc_scan_malformed_metadata_fails_closed(self):
        pid = self.proc_fixture()
        (pid / 'fdinfo' / '1').write_text('mnt_id: invalid\nino: 42\n')
        with self.assertRaises(ValueError):
            p.in_use(self.root)

    def test_historical_files_do_not_consume_batch_budget(self):
        entries = [dict(path=f'/var/log/log{i}.1', bytes=10, sha256=str(i), metadata=[1, i]) for i in range(3)]
        reserved = {(entry['path'], entry['sha256']) for entry in entries[:2]}
        by_path = {entry['path']: entry for entry in entries}
        walk = [('/var/log', [], ['log0.1', 'log1.1', 'log2.1'])]
        with patch.object(p.os, 'walk', side_effect=[[], walk]), patch.object(p, 'MAX_FILES', 1), patch.object(p, 'MAX_BYTES', 10), patch.object(p, 'in_use', return_value=set()), patch.object(Path, 'lstat', return_value=Mock(st_dev=1, st_ino=0)), patch.object(p, 'snapshot', side_effect=lambda path: by_path[str(path)]):
            self.assertEqual(p.candidates(reserved), entries[2:])

    def test_df_percent_is_monitor_semantics(self):
        with patch.object(p.subprocess, 'run', return_value=Mock(stdout='Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/x 100 74 25 76% /\n')) as run:
            self.assertEqual(p.disk_percent(), 76)
            self.assertEqual(run.call_args.kwargs['timeout'], 5)

    def test_nas_capacity_and_exclusive_directory(self):
        nas = p.NAS()
        nas.command = Mock(return_value='Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/x 100 80 20 80% /share')
        with self.assertRaisesRegex(ValueError, 'nas_capacity'):
            nas.reserve('VPS-LIM-20261116-01', p.ARCHIVE_LIMIT)
        self.assertEqual(nas.command.call_count, 1)
        nas.command = Mock(side_effect=['Filesystem blocks Used Available Capacity Mounted on\n/dev/x 100 80 999999999 80% /share', ''])
        nas.reserve('VPS-LIM-20261116-01', p.ARCHIVE_LIMIT)
        self.assertIn('mkdir /share/ERP_BACKUPS/vps-cleanup/VPS-LIM-20261116-01', nas.command.call_args.args[0])
        self.assertNotIn('mkdir -p', nas.command.call_args.args[0])

    def test_nas_fixed_backing_share_guard_and_invalid_id(self):
        nas = p.NAS()
        guarded = nas.guarded('df -Pk ' + p.NAS_ROOT)
        self.assertIn('readlink -f /share/ERP_BACKUPS/vps-cleanup', guarded)
        self.assertIn('= /share/CACHEDEV1_DATA/ERP_BACKUPS/vps-cleanup', guarded)
        with self.assertRaisesRegex(ValueError, 'invalid_id'):
            nas.reserve('../unsafe', 1)
        with patch.object(p.subprocess, 'run', return_value=Mock(stdout='ok')) as run:
            nas.command('df -Pk ' + p.NAS_ROOT)
            self.assertIn('readlink -f', run.call_args.args[0][-1])

    def test_archive_bound(self):
        with patch.object(p, 'ARCHIVE_LIMIT', 1):
            with self.assertRaisesRegex(ValueError, 'archive_limit'):
                p.HashWriter(io.BytesIO()).write(b'xx')


if __name__ == '__main__':
    unittest.main()
