#!/usr/bin/env python3
"""VPS scheduler entrypoint: restore proof first, existing guarded rotation second."""
import argparse
import fcntl
import hashlib
import importlib.machinery
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

STATE = Path('/var/lib/erp-backup-restore')
NAS_ALIAS = 'qnap-nasdolce-verify'
NAS_CONFIG = '/etc/erp-nas-access/ssh_config'
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


def find_receipt(rotator, directories, stem, checksum):
    for directory in directories:
        try:
            candidate = rotator.read_receipt(directory / (stem + '.json'), stem)
            if candidate['manifest_sha256'] == checksum:
                return directory, candidate
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return None, None


def verify_point(rotator, stem, receipts=None, *, nas_side=False, smb_direct=False):
    _, checksum, _ = rotator.local_set(rotator.BACKUPS, stem)
    directories = list(dict.fromkeys([receipts or rotator.RECEIPTS, rotator.RECEIPTS]))
    path = directories[0] / (stem + '.json')
    _, proof = find_receipt(rotator, directories, stem, checksum)
    options = {'nas_side': True} if nas_side else {}
    if smb_direct:
        options['smb_direct'] = True
    if proof is None:
        payload = json.loads(run(['ssh', '-F', NAS_CONFIG, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                                  '-o', 'ConnectTimeout=20', NAS_ALIAS, 'restore', stem, checksum]))
        proof, evidence = payload['receipt'], payload['evidence']
        rotator.require(proof['point'] == stem and proof['manifest_sha256'] == checksum, 'Wrong remote proof')
        rotator.require(hashlib.sha256(evidence.encode()).hexdigest() == proof['evidence_sha256'], 'Changed remote log')
        # Reuse only root-owned aliases; current byte/hash validation below remains mandatory.
        aliases = {}
        names = {row['path'] for row in json.loads((rotator.BACKUPS / (stem + '.media.json')).read_text())['files']}
        previous_receipts = {p for directory in directories for p in directory.glob('*.json')}
        for previous in sorted(previous_receipts, key=lambda p: (p.stem, str(p.parent))):
            try:
                old = rotator.read_receipt(previous, previous.stem)
                aliases.update({k: v for k, v in old.get('media_aliases', {}).items() if k in names})
            except (OSError, ValueError, KeyError, TypeError):
                continue
        proof['media_aliases'] = aliases
        rotator.bounded_check_proof(rotator.BACKUPS, rotator.NAS, stem, proof, **options)
        atomic(path.with_suffix('.restore.log'), evidence)
        atomic(path, json.dumps(proof, indent=2) + '\n')
        rotator.require(rotator.read_receipt(path, stem) == proof, 'Published receipt changed')
    else:
        rotator.bounded_check_proof(rotator.BACKUPS, rotator.NAS, stem, proof, **options)
    return {'point': stem, 'manifest_sha256': checksum, 'media': proof['verified_media']}


def retain_latest_local(rotator, verified, receipts):
    result = dict(rotation='Retire verified local copies only', retired_local=[])
    for item in verified[:-1]:
        stem = item['point']
        try:
            directory, _ = find_receipt(rotator, (receipts, rotator.RECEIPTS), stem, item['manifest_sha256'])
            rotator.require(directory is not None, 'No matching restore proof for retirement')
            # The rotator revalidates NAS bytes and rechecks the newest point under its lock.
            rotator.retire_point(rotator.BACKUPS, rotator.EXPORT, directory, rotator.NAS,
                                 stem, nas_side=True, smb_direct=True)
        except Exception as error:
            result['error'] = f'Local retirement stopped at {stem}: {error}'
            break
        result['retired_local'].append(stem)
    return result


def maintain(rotator, verify_only=False, *, nas_side=False, smb_direct=False, retire_verified_local=False):
    rotator.require(not retire_verified_local or (verify_only and nas_side and smb_direct),
                    'Local retirement requires verify-only and NAS-side Direct SMB')
    rotator.require(not nas_side or verify_only, 'NAS-side verification requires verify-only')
    rotator.require(not smb_direct or nas_side, 'Direct SMB requires NAS-side verification')
    receipts = None
    if verify_only:
        receipts = STATE / 'verification-receipts'
        for directory in (STATE, receipts):
            if directory == receipts:
                directory.mkdir(mode=0o700, exist_ok=True)
            info = directory.lstat()
            forbidden_mode = 0o077 if directory == receipts else 0o022
            rotator.require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid()
                            and not info.st_mode & forbidden_mode, 'Verification receipts require a private owned directory and safe parent')
    # This lock also serializes with the daily backup and existing cron rotator.
    with (rotator.BACKUPS / '.backup.flock').open('r') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        points = sorted(p.stem for p in rotator.BACKUPS.glob('backup_*.manifest') if rotator.POINT.fullmatch(p.stem))
        rotator.require(points, 'No complete backup')
        rotator.require(not list(rotator.BACKUPS.glob('backup_*.incomplete')), 'Incomplete backup requires review')
        newest = points[-1]
        rotator.require(time.time() - (rotator.BACKUPS / (newest + '.manifest')).stat().st_mtime < 48 * 3600,
                        'Newest backup is over 48 hours old')
        run(['ssh', '-F', NAS_CONFIG, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=20',
             NAS_ALIAS, 'status'])
        options = {'nas_side': True} if nas_side else {}
        if smb_direct:
            options['smb_direct'] = True
        verified = [verify_point(rotator, point, receipts=receipts, **options) for point in points]
    if verify_only:
        result = dict(verified=verified, retained=newest, rotation="Not requested (verify-only)")
        if retire_verified_local:
            result.update(retain_latest_local(rotator, verified, receipts))
        return result
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
    parser.add_argument('--verify-only', action='store_true', help='Verify recovery, without Docker maintenance')
    parser.add_argument('--nas-side', action='store_true', help='Hash on NAS and separately check SMB access')
    parser.add_argument('--smb-direct', action='store_true', help='Use encrypted SMB without the kernel mount')
    parser.add_argument('--retire-verified-local', action='store_true',
                        help='After full verification, retire older verified local copies only')
    args = parser.parse_args()
    if args.smb_direct and not args.nas_side:
        parser.error('--smb-direct requires --nas-side')
    if args.nas_side and not args.verify_only:
        parser.error('--nas-side requires --verify-only')
    if args.retire_verified_local and not (args.verify_only and args.nas_side and args.smb_direct):
        parser.error('--retire-verified-local requires --verify-only --nas-side --smb-direct')
    os.umask(0o077)
    STATE.mkdir(exist_ok=True)
    report = dict(started_epoch=time.time(), success=False)
    with (STATE / 'health.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            report['before'] = snapshot()
            rotator = importlib.machinery.SourceFileLoader('rotator', ROTATOR).load_module()
            options = {'nas_side': True} if args.nas_side else {}
            if args.smb_direct:
                options['smb_direct'] = True
            if args.retire_verified_local:
                options['retire_verified_local'] = True
            report.update(maintain(rotator, verify_only=args.verify_only, **options))
            report['success'] = 'error' not in report
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
