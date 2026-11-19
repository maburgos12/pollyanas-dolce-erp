import importlib.util
import os
import sys
import subprocess
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('rotator', Path(__file__).parents[1] / 'rotate_verified_backups.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


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
            def probe(*args):
                (root / 'backup_20261119_030001.incomplete').touch()
                return old.name, {}, 'latest'
            with patch.object(r, 'local_set', return_value=('manifest', 'a' * 64, {})), patch.object(r, 'read_receipt', return_value={}), patch.object(r, 'bounded_check_proof', side_effect=probe):
                r.rotate(root, root, root, Path('/unused-nas'), True)
            self.assertTrue(old.exists())
            self.assertTrue(newest.exists())

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
