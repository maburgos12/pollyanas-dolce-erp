#!/usr/bin/env python3
"""VPS scheduler entrypoint: restore proof first, existing guarded rotation second."""
import argparse
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
ROTATOR = str(Path(__file__).with_name('rotate_verified_backups.py'))
DOCKER_MAINTENANCE = Path('/usr/local/sbin/docker-disk-maintenance.sh')
DOCKER_MAINTENANCE_SHA = '06e7e1dab800f2a7c92cf33a71c79f3535a1cfbfa03e12a1c57f4dfb1d229799'


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


def verify_point(rotator, stem):
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
        rotator.bounded_check_proof(rotator.BACKUPS, rotator.NAS, stem, proof)
        atomic(path.with_suffix('.restore.log'), evidence)
        atomic(path, json.dumps(proof, indent=2) + '\n')
        rotator.require(rotator.read_receipt(path, stem) == proof, 'Published receipt changed')
    else:
        rotator.bounded_check_proof(rotator.BACKUPS, rotator.NAS, stem, proof)
    return {'point': stem, 'manifest_sha256': checksum, 'media': proof['verified_media']}


def maintain(rotator, verify_only=False):
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
        verified = [verify_point(rotator, point) for point in points]
    if verify_only:
        return dict(verified=verified, retained=newest, rotation="Not requested (verify-only)")
    # The rotator reacquires its lock and checks the current newest set and every proof.
    rotation = run([sys.executable, ROTATOR, '--apply'])
    # A daily backup may complete between our verification and the rotator's lock.
    with (rotator.BACKUPS / '.backup.flock').open('r') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current = sorted(p.stem for p in rotator.BACKUPS.glob('backup_*.manifest') if rotator.POINT.fullmatch(p.stem))
        rotator.require(current and not list(rotator.BACKUPS.glob('backup_*.incomplete')),
                        'Current backup set incomplete after rotation')
        newest = current[-1]
        rotator.local_set(rotator.BACKUPS, newest)
        rotator.require(not any((rotator.BACKUPS / (point + '.manifest')).exists() for point in points[:-1]),
                        'Rotation preserved older points; inspect NAS/rotator log')
    if int(run(['df', '-P', '/']).splitlines()[-1].split()[4].rstrip('%')) >= 75:
        rotator.secure_file(DOCKER_MAINTENANCE)
        rotator.require(rotator.digest(DOCKER_MAINTENANCE) == DOCKER_MAINTENANCE_SHA, 'Docker maintenance changed; review required')
        run([str(DOCKER_MAINTENANCE)])
    return dict(verified=verified, retained=newest, rotation=rotation)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verify-only', action='store_true', help='Verify recovery without rotation or Docker maintenance')
    args = parser.parse_args()
    os.umask(0o077)
    STATE.mkdir(exist_ok=True)
    report = dict(started_epoch=time.time(), success=False)
    with (STATE / 'health.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            report['before'] = snapshot()
            rotator = importlib.machinery.SourceFileLoader('rotator', ROTATOR).load_module()
            report.update(maintain(rotator, verify_only=args.verify_only))
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
                running = lambda snapshot: {line.split()[1] for line in snapshot['containers'].splitlines() if line.split()[2] == 'running'}
                report['unexpected_stopped'] = sorted(running(report['before']) - running(report['after']))
                report['unexpected_removed_volumes'] = sorted(set(report['before']['volumes'].splitlines()) - set(report['after']['volumes'].splitlines()))
                if report['unexpected_stopped'] or report['unexpected_removed_volumes']:
                    report['success'] = False
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
