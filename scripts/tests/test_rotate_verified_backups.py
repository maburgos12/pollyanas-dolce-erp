import importlib.util
import fcntl
import hashlib
import os
import signal
import sys
import subprocess
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('rotator', Path(__file__).parents[1] / 'rotate_verified_backups.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


class RetirePointTests(unittest.TestCase):
    def setUp(self):
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(tmp).resolve()
        self.export = self.root / 'export'
        self.receipts = self.root / 'verification-receipts'
        self.export.mkdir()
        self.receipts.mkdir()
        self.lock = self.root / '.backup.flock'
        self.lock.touch()
        self.old, self.other, self.newest = (f'backup_202611{day}_020001' for day in (17, 18, 19))
        for stem in (self.old, self.other, self.newest):
            rows = []
            for suffix in r.PAYLOADS:
                path = self.root / f'{stem}.{suffix}'
                payload = path.name.encode()
                path.write_bytes(payload)
                os.link(path, self.export / path.name)
                rows.append(f'{hashlib.sha256(payload).hexdigest()}  {path.name}\n')
            manifest = self.root / f'{stem}.manifest'
            manifest.write_text(''.join(rows))
            os.link(manifest, self.export / manifest.name)

    def retire(self, stem, during_probe=None):
        manifest, checksum, payloads = r.local_set(self.root, stem)
        def probe(*args):
            with self.lock.open() as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                if during_probe:
                    during_probe()
            return manifest, payloads, 'latest'
        with patch.object(r, 'read_receipt', return_value={'manifest_sha256': checksum}), patch.object(r, 'bounded_check_proof', side_effect=probe):
            r.retire_point(self.root, self.export, self.receipts, Path('/test-nas'), stem)

    def test_removes_only_the_exact_set_and_does_not_lock_during_nas_read(self):
        self.retire(self.old)
        for directory in (self.root, self.export):
            self.assertFalse(list(directory.glob(self.old + '.*')))
            self.assertEqual(len(list(directory.glob(self.other + '.*'))), 5)
            self.assertEqual(len(list(directory.glob(self.newest + '.*'))), 5)

    def test_latest_and_incomplete_sets_are_preserved(self):
        with self.assertRaisesRegex(ValueError, 'último respaldo'):
            self.retire(self.newest)
        with self.assertRaisesRegex(ValueError, 'incompleto'):
            self.retire(self.old, lambda: (self.root / (self.newest + '.incomplete')).touch())
        self.assertEqual(len(list(self.root.glob(self.old + '.*'))), 5)

    def test_local_change_or_foreign_link_prevents_unlink(self):
        payload = self.root / (self.old + '.sql.gz')
        with self.assertRaisesRegex(ValueError, 'no coincide'):
            self.retire(self.old, lambda: payload.write_bytes(b'changed'))
        payload.write_bytes(payload.name.encode())
        os.link(payload, self.root / 'foreign-link')
        with self.assertRaisesRegex(ValueError, 'otro propietario'):
            self.retire(self.old)
        self.assertEqual(len(list(self.root.glob(self.old + '.*'))), 5)


class BoundedNASProofTests(unittest.TestCase):
    def test_timeout_kills_without_second_wait_or_parent_nas_access(self):
        child = Mock(pid=12345)
        child.wait.side_effect = subprocess.TimeoutExpired('probe', 1)
        child.poll.return_value = None
        with tempfile.TemporaryDirectory() as tmp, patch.object(r.subprocess, 'Popen', return_value=child) as spawn, patch.object(r.os, 'killpg') as kill, patch.object(r, 'actual_nas_device') as device:
            with self.assertRaisesRegex(ValueError, 'NAS verification timed out.*12345'):
                r.bounded_check_proof(Path(tmp), Path('/unavailable-nas'), 'backup_20261119_020001', {'manifest_sha256': 'a' * 64}, timeout=0.01)
            self.assertEqual(child.wait.call_count, 1)
            device.assert_not_called()
            self.assertTrue(spawn.call_args.kwargs['close_fds'])
            self.assertTrue(spawn.call_args.kwargs['start_new_session'])
            kill.assert_called_once()

    def test_failed_probe_preserves_backup_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            (root / '.backup.flock').touch()
            for date in ('20261118', '20261119'):
                (root / ('backup_' + date + '_020001.manifest')).touch()
            initial = set(root.iterdir())
            with patch.object(r, 'local_set', return_value=('manifest', 'a' * 64, {})), patch.object(r, 'read_receipt', return_value={}), patch.object(r, 'bounded_check_proof', side_effect=ValueError('NAS timeout')):
                r.rotate(root, root, root, Path('/unavailable-nas'), True)
            self.assertEqual(set(root.iterdir()), initial)

    def test_success_does_not_inherit_lock_and_revalidates_local_content(self):
        real_popen = subprocess.Popen
        stem = 'backup_20261119_020001'
        manifest, checksum, payloads = stem + '.manifest', 'a' * 64, {'payload': 'b' * 64}
        with tempfile.TemporaryDirectory() as tmp, (Path(tmp) / 'backup.lock').open('w') as lock:
            os.set_inheritable(lock.fileno(), True)
            code = """import os,sys,json
try:
    os.fstat(int(sys.argv[1]))
except OSError:
    pass
else:
    raise RuntimeError('backup lock inherited')
r=json.load(sys.stdin)
print(json.dumps({'request':r,'result':[sys.argv[2],{'payload':'b'*64},'latest']}))
"""
            def launch(_args, **kwargs):
                return real_popen([sys.executable, '-c', code, str(lock.fileno()), manifest], **kwargs)
            with patch.object(r.subprocess, 'Popen', side_effect=launch), patch.object(r, 'local_set', return_value=(manifest, checksum, payloads)) as local:
                result = r.bounded_check_proof(Path(tmp), Path('/unused'), stem, {'manifest_sha256': checksum}, timeout=5)
                self.assertEqual(result, (manifest, payloads, 'latest'))
                local.assert_called_once_with(Path(tmp), stem)
            with patch.object(r.subprocess, 'Popen', side_effect=launch), patch.object(r, 'local_set', return_value=(manifest, 'changed', payloads)):
                with self.assertRaisesRegex(ValueError, 'manifiesto local cambió'):
                    r.bounded_check_proof(Path(tmp), Path('/unused'), stem, {'manifest_sha256': checksum}, timeout=5)

    def test_changed_receipt_after_success_never_unlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            (root / '.backup.flock').touch()
            old = 'backup_20261118_020001'
            newest = 'backup_20261119_020001'
            for point in (old, newest):
                (root / (point + '.manifest')).touch()
            before = set(root.iterdir())
            with patch.object(r, 'local_set', return_value=('manifest', 'a' * 64, {})), patch.object(r, 'read_receipt', side_effect=[{'proof': 1}, {'proof': 2}]), patch.object(r, 'bounded_check_proof', return_value=(old + '.manifest', {}, 'latest')):
                r.rotate(root, root, root, Path('/unused-nas'), True)
            self.assertEqual(set(root.iterdir()), before)

    def test_truncated_or_unbound_probe_response_fails_closed(self):
        for response in (b'{', b'{"request":{},"result":[]}'):
            def launch(_args, **kwargs):
                kwargs['stdout'].write(response)
                return Mock(pid=99999999, wait=Mock(return_value=0))
            with self.subTest(response=response), patch.object(r.subprocess, 'Popen', side_effect=launch), patch.object(r, 'local_set') as local:
                with self.assertRaises(ValueError):
                    r.bounded_check_proof(Path('/unused'), Path('/unused-nas'), 'backup_20261119_020001', {})
                local.assert_not_called()

    def test_incomplete_marker_during_probe_preserves_old_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            (root / '.backup.flock').touch()
            old = root / 'backup_20261118_020001.manifest'
            newest = root / 'backup_20261119_020001.manifest'
            old.touch()
            newest.touch()
            def probe(*args, **kwargs):
                (root / 'backup_20261119_030001.incomplete').touch()
                return old.name, {}, 'latest'
            with patch.object(r, 'local_set', return_value=('manifest', 'a' * 64, {})), patch.object(r, 'read_receipt', return_value={}), patch.object(r, 'bounded_check_proof', side_effect=probe):
                r.rotate(root, root, root, Path('/unused-nas'), True)
            self.assertTrue(old.exists())
            self.assertTrue(newest.exists())

    def test_rotation_budget_limits_second_probe_and_preserves_on_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            (root / '.backup.flock').touch()
            for day in (17, 18, 19):
                (root / f'backup_202611{day}_020001.manifest').touch()
            before = set(root.iterdir())
            with patch.object(r.time, 'monotonic', side_effect=[0, 0, 1200]), patch.object(r, 'local_set'), patch.object(r, 'read_receipt', return_value={}), patch.object(r, 'bounded_check_proof', side_effect=ValueError('timeout')) as probe:
                r.rotate(root, root, root, Path('/unused-nas'), True)
            self.assertEqual([call.kwargs['timeout'] for call in probe.call_args_list], [2040, 840])
            self.assertEqual(set(root.iterdir()), before)

    def test_exhausted_rotation_budget_does_not_start_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            (root / '.backup.flock').touch()
            for day in (18, 19):
                (root / f'backup_202611{day}_020001.manifest').touch()
            with patch.object(r.time, 'monotonic', side_effect=[0, 2040]), patch.object(r, 'local_set'), patch.object(r, 'read_receipt', return_value={}), patch.object(r, 'bounded_check_proof') as probe:
                r.rotate(root, root, root, Path('/unused-nas'), True)
            probe.assert_not_called()
            self.assertTrue((root / 'backup_20261118_020001.manifest').exists())

    def test_budget_expires_after_verification_before_any_unlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            export = root / 'export'
            export.mkdir()
            (root / '.backup.flock').touch()
            for day in (18, 19):
                (root / f'backup_202611{day}_020001.manifest').touch()
            old = root / 'backup_20261118_020001.manifest'
            with patch.object(r.time, 'monotonic', side_effect=[0, 0, 2040]), patch.object(r, 'local_set'), patch.object(r, 'read_receipt', return_value={}), patch.object(r, 'bounded_check_proof', return_value=(old.name, {}, 'latest')):
                r.rotate(root, export, root, Path('/unused-nas'), True)
            self.assertTrue(old.exists())

    def test_sigterm_relays_without_wait_and_restores_previous_handler(self):
        original = signal.getsignal(signal.SIGTERM)
        for previous in (signal.SIG_DFL, lambda *_: None):
            with self.subTest(previous=previous):
                signal.signal(signal.SIGTERM, previous)
                child = Mock(pid=99999999)
                child.wait.side_effect = lambda **_: signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                try:
                    with patch.object(r.subprocess, 'Popen', return_value=child), patch.object(r.os, 'killpg') as kill:
                        with self.assertRaises(SystemExit) as error:
                            r.bounded_check_proof(Path('/unused'), Path('/unused-nas'), 'backup_20261119_020001', {})
                        self.assertEqual(error.exception.code, 128 + signal.SIGTERM)
                        kill.assert_called_once_with(child.pid, signal.SIGKILL)
                        self.assertEqual(child.wait.call_count, 1)
                        self.assertIs(signal.getsignal(signal.SIGTERM), previous)
                finally:
                    signal.signal(signal.SIGTERM, original)

    def test_spawn_failure_restores_signal_state(self):
        previous = signal.getsignal(signal.SIGTERM)
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        with patch.object(r.subprocess, 'Popen', side_effect=OSError('spawn failed')), patch.object(r.os, 'killpg') as kill:
            with self.assertRaisesRegex(OSError, 'spawn failed'):
                r.bounded_check_proof(Path('/unused'), Path('/unused-nas'), 'backup_20261119_020001', {})
            kill.assert_not_called()
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, set()), previous_mask)

    def test_child_rejects_concurrent_probe(self):
        import fcntl
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / 'probe.lock'
            with lock.open('a') as held:
                fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(r, 'PROBE_LOCK', lock), patch.object(r, 'check_proof') as check:
                    with self.assertRaises(BlockingIOError):
                        r.probe({'root': tmp, 'nas': tmp, 'stem': 'backup_20261119_020001', 'proof': {}})
                    check.assert_not_called()


if __name__ == '__main__':
    unittest.main()
