#!/usr/bin/env python3
"""Activate a recovered, dedicated NAS key without replacing the bootstrap key."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from nas_restore_daily import private_bytes, private_replace, require

ROOT = Path('/etc/erp-nas-access')
CONFIG = ROOT / 'ssh_config'
NAS_SCRIPT = '/share/homes/DolcePasteleria/.erp-backup-verifier/nas_restore_daily.py'
NAS_PYTHON = '/share/CACHEDEV1_DATA/.qpkg/HybridBackup/CloudConnector3/python/bin/python3'


def run(args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE, timeout=30).strip()


def bootstrap(*args):
    return run(['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                '-o', 'ConnectTimeout=10', 'qnap-nasdolce-archive', *args])


def config_block():
    return ('\nHost qnap-nasdolce-verify\n'
            '  HostName 10.77.216.2\n  User DolcePasteleria\n'
            '  IdentityFile /etc/erp-nas-access/id_ed25519\n'
            '  UserKnownHostsFile /etc/erp-nas-access/known_hosts\n'
            '  IdentitiesOnly yes\n  IdentityAgent none\n  BatchMode yes\n'
            '  StrictHostKeyChecking yes\n  PasswordAuthentication no\n'
            '  KbdInteractiveAuthentication no\n  RequestTTY no\n'
            '  ClearAllForwardings yes\n  ConnectTimeout 20\n')


def install(expected_head):
    repo = Path(__file__).resolve().parents[2]
    require(re.fullmatch('[a-f0-9]{40}', expected_head)
            and run(['git', '-C', str(repo), 'rev-parse', 'HEAD']) == expected_head,
            'Wrong installed revision')
    require(not run(['git', '-C', str(repo), 'status', '--porcelain', '--untracked-files=normal']),
            'Dirty installation source')
    info = ROOT.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and not info.st_mode & 0o077, 'Unsafe credential directory')
    key = private_bytes(ROOT / 'id_ed25519')
    public = run(['ssh-keygen', '-y', '-f', str(ROOT / 'id_ed25519')])
    require(public.startswith('ssh-ed25519 '), 'Wrong service key type')
    proof = json.loads(private_bytes(ROOT / 'recovery-proof.json'))
    require(proof.get('version') == 1 and proof.get('verified_decrypt') is True
            and proof.get('public_key') == public
            and proof.get('private_key_sha256') == hashlib.sha256(key).hexdigest()
            and re.fullmatch('[a-f0-9]{64}', proof.get('encrypted_backup_sha256', '')),
            'Verified encrypted recovery copy required')
    expected_script = hashlib.sha256((repo / 'scripts/nas_restore_daily.py').read_bytes()).hexdigest()
    require(bootstrap('sha256sum', NAS_SCRIPT).split()[0] == expected_script,
            'NAS must run this reviewed revision first')
    known = '\n'.join(line for line in run(['ssh-keygen', '-F', '10.77.216.2',
                                           '-f', '/root/.ssh/known_hosts']).splitlines()
                      if line and not line.startswith('#')).encode() + b'\n'
    require(known.strip(), 'Missing existing trusted host key')
    host_file = ROOT / 'known_hosts'
    if host_file.exists() or host_file.is_symlink():
        require(private_bytes(host_file) == known, 'Trusted host key changed')
    else:
        private_replace(host_file, known, None)
    block = config_block().encode()
    if CONFIG.exists() or CONFIG.is_symlink():
        require(private_bytes(CONFIG) == block, 'Service alias already has another policy')
    else:
        # A separate config cannot inherit broad keys from earlier Host * entries.
        private_replace(CONFIG, block, None)
    old_keys_sha = bootstrap('sha256sum', '/share/homes/DolcePasteleria/.ssh/authorized_keys').split()[0]
    bootstrap(NAS_PYTHON, '-I', '-B', NAS_SCRIPT, '--install-service-key',
              public.split()[1], old_keys_sha)
    result = json.loads(run(['ssh', '-F', str(CONFIG), '-T', 'qnap-nasdolce-verify', 'status']))
    require(result == dict(version=1, state='READY'), 'Service key acceptance failed')
    denied = subprocess.run(['ssh', '-F', str(CONFIG), '-T', 'qnap-nasdolce-verify', 'sh'],
                            capture_output=True, text=True, timeout=30)
    require(denied.returncode == 2 and not denied.stdout
            and json.loads(denied.stderr) == dict(state='FAILED', error_class='ValueError'),
            'Service key allowed an unsupported command')
    print(json.dumps(dict(state='INSTALLED_RESTRICTED_KEY', bootstrap_preserved=True,
                         fingerprint=run(['ssh-keygen', '-lf', str(ROOT / 'id_ed25519')]))))


if __name__ == '__main__':
    require(os.getuid() == 0 and len(sys.argv) == 2, 'Root and exact release SHA required')
    try:
        install(sys.argv[1])
    except Exception as exc:
        print(json.dumps(dict(state='INSTALL_HOLD', error_class=type(exc).__name__, retry=False)))
        raise SystemExit(2)
