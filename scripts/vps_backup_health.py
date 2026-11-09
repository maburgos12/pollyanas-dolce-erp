#!/usr/bin/env python3
"""VPS scheduler entrypoint: restore proof first, existing guarded rotation second."""
import fcntl
import hashlib
import importlib.machinery
import json
import os
from pathlib import Path
import subprocess
import sys
import time

STATE = Path('/var/lib/erp-backup-restore')
NAS_PYTHON = '/share/CACHEDEV1_DATA/.qpkg/HybridBackup/CloudConnector3/python/bin/python3'
NAS_RUNNER = '/share/homes/DolcePasteleria/.erp-backup-verifier/nas_restore_daily.py'
ROTATOR = '/usr/local/sbin/erp-rotate-verified-backups'


def run(args):
    return subprocess.check_output(args, text=True, timeout=7 * 3600)


def snapshot():
    return dict(df=run(['df', '-B1', '/']), docker=run(['docker', 'system', 'df']),
                containers=run(['docker', 'ps', '-a', '--format', '{{.ID}} {{.Names}} {{.State}} {{.Status}}']),
                volumes=run(['docker', 'volume', 'ls', '-q']), memory=run(['free', '-h']))


def atomic(path, text):
    temp = path.with_suffix(path.suffix + '.next')
    with temp.open('w') as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temp, 0o600)
    temp.replace(path)


def verify_point(rotator, stem, nas_device):
    manifest, checksum, _ = rotator.local_set(rotator.BACKUPS, stem)
    path = rotator.RECEIPTS / (stem + '.json')
    try:
        proof = rotator.read_receipt(path, stem)
        rotator.require(proof['manifest_sha256'] == checksum, 'Changed manifest')
    except (OSError, ValueError, KeyError, TypeError):
        payload = json.loads(run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                                  '-o', 'ConnectTimeout=20', 'qnap-nasdolce-archive',
                                  NAS_PYTHON, NAS_RUNNER, stem, checksum]))
        proof, evidence = payload['receipt'], payload['evidence']
        rotator.require(proof['point'] == stem and proof['manifest_sha256'] == checksum, 'Wrong remote proof')
        rotator.require(hashlib.sha256(evidence.encode()).hexdigest() == proof['evidence_sha256'], 'Changed remote log')
        # Reuse only root-owned aliases; current byte/hash validation below remains mandatory.
        aliases = {}
        names = {row['path'] for row in json.loads((rotator.BACKUPS / (stem + '.media.json')).read_text())['files']}
        for previous in sorted(rotator.RECEIPTS.glob('*.json')):
            try:
                old = rotator.read_receipt(previous, previous.stem)
                aliases.update({k: v for k, v in old.get('media_aliases', {}).items() if k in names})
            except (OSError, ValueError, KeyError, TypeError):
                continue
        proof['media_aliases'] = aliases
        rotator.check_proof(rotator.BACKUPS, rotator.NAS, stem, proof, nas_device)
        atomic(path.with_suffix('.restore.log'), evidence)
        atomic(path, json.dumps(proof, indent=2) + '\n')
        proof = rotator.read_receipt(path, stem)
    rotator.check_proof(rotator.BACKUPS, rotator.NAS, stem, proof, nas_device)
    return {'point': stem, 'manifest_sha256': checksum, 'media': proof['verified_media']}


def maintain(rotator):
    # This lock also serializes with the daily backup and existing cron rotator.
    with (rotator.BACKUPS / '.backup.flock').open('r') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        points = sorted(p.stem for p in rotator.BACKUPS.glob('backup_*.manifest') if rotator.POINT.fullmatch(p.stem))
        rotator.require(points, 'No complete backup')
        rotator.require(not list(rotator.BACKUPS.glob('backup_*.incomplete')), 'Incomplete backup requires review')
        newest = points[-1]
        rotator.require(time.time() - (rotator.BACKUPS / (newest + '.manifest')).stat().st_mtime < 48 * 3600,
                        'Newest backup is over 48 hours old')
        run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=20',
             'qnap-nasdolce-archive', 'true'])
        nas_device = rotator.actual_nas_device(rotator.NAS)
        verified = [verify_point(rotator, point, nas_device) for point in points]
    # The rotator reacquires its lock and checks the current newest set and every proof.
    rotation = run([ROTATOR, '--apply'])
    rotator.local_set(rotator.BACKUPS, newest)
    return dict(verified=verified, retained=newest, rotation=rotation)


def main():
    os.umask(0o077)
    STATE.mkdir(exist_ok=True)
    report = dict(started_epoch=time.time(), success=False)
    with (STATE / 'health.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            report['before'] = snapshot()
            rotator = importlib.machinery.SourceFileLoader('rotator', ROTATOR).load_module()
            report.update(maintain(rotator))
            report['success'] = True
        except Exception as error:
            report['error'] = str(error)
        finally:
            try:
                report['after'] = snapshot()
                before = report['before']['df'].splitlines()[-1].split()
                after = report['after']['df'].splitlines()[-1].split()
                report['freed_bytes_net'] = int(after[3]) - int(before[3])
                report['used_percent'] = int(after[4].rstrip('%'))
                report['level'] = 'URGENTE' if report['used_percent'] >= 85 else 'ALERTA' if report['used_percent'] >= 75 else 'OK'
            except Exception as error:
                report['measurement_error'] = str(error)
                report['success'] = False
            report['finished_epoch'] = time.time()
            encoded = json.dumps(report, indent=2) + '\n'
            atomic(STATE / 'health-latest.json', encoded)
            atomic(STATE / ('health-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()) + '.json'), encoded)
            print(encoded, flush=True)
    return 0 if report['success'] else 1


if __name__ == '__main__':
    sys.exit(main())
