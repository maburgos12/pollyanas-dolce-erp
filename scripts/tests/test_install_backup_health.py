"""Exercise the installer guard only against isolated fake host paths and commands."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class InstallerTests(unittest.TestCase):
    def test_unknown_rotator_is_preserved_and_known_wrapper_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo, binary, systemd = root / 'repo', root / 'bin', root / 'systemd'
            for path in (repo / 'scripts/ops', binary, systemd):
                path.mkdir(parents=True)
            for name in ('rotate_verified_backups.py', 'vps_backup_health.py', 'nas_restore_daily.py'):
                (repo / 'scripts' / name).write_text('pass\n')
            for name in ('erp-backup-health.service', 'erp-backup-health.timer'):
                (repo / 'scripts/ops' / name).touch()
            calls = root / 'calls'
            for name in ('id', 'ssh', 'systemd-analyze', 'systemctl'):
                code = 'echo 0' if name == 'id' else 'echo ' + name + ' >> "' + str(calls) + '"\ncat >/dev/null'
                command = binary / name
                command.write_text('#!/bin/sh\n' + code + '\n')
                command.chmod(0o755)
            target = root / 'rotator'
            script = Path(__file__).parents[1] / 'ops/install_backup_health.sh'
            text = script.read_text().replace('/opt/pastelerias-erp', str(repo)).replace('/usr/local/sbin/erp-rotate-verified-backups', str(target)).replace('/etc/systemd/system', str(systemd)).replace('/usr/bin/python3', sys.executable)
            original = '#!/usr/bin/python3\nprint("verified original rotator")\n'
            checksum = hashlib.sha256(original.encode()).hexdigest()
            text = text.replace('e8cba9307cf6486aa2429004a0f81e62bdbd472f8afe6b6117f9da6a28de1e2f', checksum)
            baseline = Path(str(target) + '.baseline.' + checksum)
            installer = root / 'install.sh'
            installer.write_text(text)
            target.write_text('#!/bin/sh\necho custom-operator-version\n')
            target.chmod(0o755)
            env = {**os.environ, 'PATH': str(binary) + os.pathsep + os.environ['PATH']}
            result = subprocess.run(['bash', str(installer)], env=env, capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('custom-operator-version', target.read_text())
            self.assertFalse(calls.exists(), 'guard must run before any NAS/systemd command')
            expected = f'#!/bin/sh\nexec {sys.executable} {repo}/scripts/rotate_verified_backups.py "$@"\n'
            target.write_text(original)
            baseline.write_text('unrecognized previous backup')
            result = subprocess.run(['bash', str(installer)], env=env, capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(baseline.read_text(), 'unrecognized previous backup')
            self.assertFalse(calls.exists())
            baseline.unlink()
            result = subprocess.run(['bash', str(installer)], env=env, capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(baseline.read_text(), original)
            self.assertEqual(baseline.stat().st_mode & 0o777, 0o700)
            baseline_inode = baseline.stat().st_ino
            for _ in range(2):
                result = subprocess.run(['bash', str(installer)], env=env, capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(target.read_text(), expected)
                self.assertEqual(target.stat().st_mode & 0o777, 0o700)
                self.assertEqual(baseline.read_text(), original)
                self.assertEqual(baseline.stat().st_ino, baseline_inode)
            previous_calls = calls.read_text()
            baseline.unlink()
            result = subprocess.run(['bash', str(installer)], env=env, capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
            self.assertNotEqual(result.returncode, 0, 'a wrapper without its baseline cannot provide rollback')
            self.assertEqual(target.read_text(), expected)
            self.assertEqual(calls.read_text(), previous_calls)
