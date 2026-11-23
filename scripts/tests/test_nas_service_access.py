"""The NAS service key can verify backups, never execute arbitrary shell."""
import contextlib
import base64
import hashlib
import io
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

from scripts.tests.test_vps_backup_health import module


nas = module('nas_restore_daily')


class ServiceCommandTests(unittest.TestCase):
    def test_status_exposes_no_secret_or_arbitrary_file(self):
        output = io.StringIO()
        command = getattr(nas, 'ssh_command', None)
        self.assertTrue(callable(command), 'restricted SSH command is missing')
        with contextlib.redirect_stdout(output):
            command('status')
        self.assertEqual(json.loads(output.getvalue()), {'version': 1, 'state': 'READY'})

    def test_only_exact_point_and_hash_reach_restore(self):
        command = getattr(nas, 'ssh_command', None)
        self.assertTrue(callable(command), 'restricted SSH command is missing')
        point, checksum = 'backup_20261123_020001', 'a' * 64
        with patch.object(nas, 'restore') as restore:
            command('restore ' + point + ' ' + checksum)
            restore.assert_called_once_with(point, checksum)
            restore.reset_mock()
            for invalid in ('', 'true', 'sh', 'cat /etc/passwd', 'status; id',
                            'status\n', 'status extra', 'restore ../secret ' + checksum,
                            'restore ' + point + ' ' + 'b' * 63,
                            'restore ' + point + ' ' + checksum + ' extra'):
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    command(invalid)
            restore.assert_not_called()

    def test_preflight_rejects_missing_media_without_starting_database(self):
        preflight = getattr(nas, 'preflight', None)
        self.assertTrue(callable(preflight), 'readability preflight is missing')
        with tempfile.TemporaryDirectory() as tmp, patch.object(nas, 'NAS', Path(tmp)), patch.object(nas, 'manifest'):
            point = 'backup_20261123_020001'
            (Path(tmp) / (point + '.media.json')).write_text(json.dumps(
                {'version': 1, 'files': [{'path': 'missing.txt', 'bytes': 3, 'sha256': 'a' * 64}]}))
            with patch.object(nas.subprocess, 'run') as run:
                with self.assertRaises((OSError, ValueError)):
                    preflight(point, 'b' * 64)
                run.assert_not_called()


class ServiceKeyTests(unittest.TestCase):
    def public_key(self):
        return base64.b64encode(struct.pack('>I', 11) + b'ssh-ed25519' +
                                struct.pack('>I', 32) + b'a' * 32).decode()

    def test_key_install_preserves_existing_key_and_rejects_drift(self):
        install = getattr(nas, 'install_service_key', None)
        self.assertTrue(callable(install), 'restricted key installation is missing')
        with tempfile.TemporaryDirectory() as tmp, patch.object(nas, 'HOME', Path(tmp)):
            # HOME normally ends in .erp-backup-verifier, alongside .ssh.
            home = Path(tmp) / '.erp-backup-verifier'
            home.mkdir(mode=0o700)
            folder = Path(tmp) / '.ssh'
            folder.mkdir(mode=0o700)
            target = folder / 'authorized_keys'
            original = b'ssh-ed25519 EXISTING_KEY keep\n'
            target.write_bytes(original)
            target.chmod(0o600)
            checksum = hashlib.sha256(original).hexdigest()
            with patch.object(nas, 'HOME', home), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(ValueError):
                    install(self.public_key(), '0' * 64)
                self.assertEqual(target.read_bytes(), original)
                install(self.public_key(), checksum)
                result = target.read_bytes()
                self.assertTrue(result.startswith(original))
                self.assertIn(b'from="10.77.216.1"', result)
                self.assertIn(b'--ssh-command', result)
                self.assertIn(b'no-user-rc,no-agent-forwarding,no-port-forwarding,no-pty,no-X11-forwarding', result)
                self.assertEqual(target.stat().st_mode & 0o777, 0o600)
                self.assertEqual((folder / ('authorized_keys.before-erp-verify-' + checksum)).read_bytes(), original)
                install(self.public_key(), checksum)
                self.assertEqual(target.read_bytes(), result)

    def test_key_rejects_wrong_wire_format_and_unsafe_authorized_file(self):
        install = getattr(nas, 'install_service_key', None)
        self.assertTrue(callable(install), 'restricted key installation is missing')
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / '.erp-backup-verifier'
            home.mkdir(mode=0o700)
            folder = Path(tmp) / '.ssh'
            folder.mkdir(mode=0o700)
            target = folder / 'authorized_keys'
            target.write_bytes(b'old\n')
            target.chmod(0o600)
            checksum = hashlib.sha256(target.read_bytes()).hexdigest()
            with patch.object(nas, 'HOME', home):
                for key in ('bad value', base64.b64encode(b'wrong').decode()):
                    with self.subTest(key=key), self.assertRaises(ValueError):
                        install(key, checksum)
                target.chmod(0o666)
                with self.assertRaises(ValueError):
                    install(self.public_key(), checksum)
            self.assertEqual(target.read_bytes(), b'old\n')


if __name__ == '__main__':
    unittest.main()
