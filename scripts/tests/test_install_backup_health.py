"""Exercise the installer guard only against isolated fake host paths and commands."""
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
            target.write_text(expected)
            for _ in range(2):
                result = subprocess.run(['bash', str(installer)], env=env, capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(target.read_text(), expected)
