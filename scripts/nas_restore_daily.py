#!/usr/bin/env python3
"""Recover one immutable daily ERP point on the NAS; never remove source backups."""
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time

HOME = Path('/share/homes/DolcePasteleria/.erp-backup-verifier')
NAS = Path('/share/ERP_BACKUPS')
POINT = re.compile(r'backup_[0-9]{8}_[0-9]{6}')
SUFFIXES = ('sql.gz', 'conteos.tar.gz', 'inventory-audit.tar.gz', 'media.json')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(path):
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        result = hashlib.file_digest(stream, 'sha256').hexdigest()
        after = os.fstat(stream.fileno())
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), 'Source changed')
    return result


def relative(value):
    path = PurePosixPath(value)
    require(path.parts and not path.is_absolute() and '..' not in path.parts, 'Unsafe path')
    return path


def manifest(point, expected):
    require(POINT.fullmatch(point), 'Invalid point')
    path = NAS / (point + '.manifest')
    require(sha(path) == expected, 'NAS manifest differs from VPS')
    hashes = {}
    for line in path.read_text().splitlines():
        checksum, name = line.split()
        require(name not in hashes and re.fullmatch('[a-f0-9]{64}', checksum), 'Invalid manifest')
        hashes[name] = checksum
    require(set(hashes) == {point + '.' + suffix for suffix in SUFFIXES}, 'Incomplete set')
    for name, checksum in hashes.items():
        require(sha(NAS / name) == checksum, 'NAS payload mismatch: ' + name)
    return hashes


def recover_files(point, scratch):
    inventory = json.loads((NAS / (point + '.media.json')).read_text())
    require(inventory['version'] == 1, 'Unsupported media inventory')
    versions = NAS / 'ERP_ACTIVE_MEDIA_VERSIONS/ERP medios activos - 7 versiones'
    source = (versions / 'latest/ERP_BACKUPS/ERP_ACTIVE_MEDIA_STAGING').resolve()
    require(source.is_relative_to(versions.resolve()), 'Unsafe generation')
    target = scratch / 'media'
    target.mkdir()
    seen = set()
    for row in inventory['files']:
        name = str(relative(row['path']))
        require(name not in seen, 'Duplicate media path')
        original = source / name
        require(original.resolve().is_relative_to(source) and original.is_file(), 'Missing/unsafe media')
        restored = target / name
        restored.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, restored)
        require(restored.stat().st_size == row['bytes'] and sha(restored) == row['sha256'], 'Recovered media differs')
        seen.add(name)
    require(seen, 'Empty media inventory')
    for suffix in SUFFIXES[1:3]:
        folder = scratch / suffix
        folder.mkdir()
        with tarfile.open(NAS / (point + '.' + suffix), 'r:gz') as archive:
            members = [m for m in archive.getmembers() if m.name not in ('.', './')]
            for member in members:
                relative(member.name)
                require(member.isfile() or member.isdir(), 'Special archive member')
            archive.extractall(folder, members=members)
            for member in members:
                if member.isfile():
                    with archive.extractfile(member) as stream:
                        expected = hashlib.file_digest(stream, 'sha256').hexdigest()
                    require(sha(folder / member.name) == expected, 'Evidence recovery differs')
    return seen


def restore(point, expected):
    require(POINT.fullmatch(point) and re.fullmatch('[a-f0-9]{64}', expected), 'Invalid arguments')
    require(HOME.is_dir() and HOME.stat().st_uid == os.getuid(), 'Unprovisioned verifier')
    os.umask(0o077)
    os.nice(10)
    def interrupted(signum, frame):
        raise RuntimeError('Restore interrupted: ' + str(signum))
    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT, signal.SIGALRM):
        signal.signal(signum, interrupted)
    signal.alarm(6 * 3600)
    with (HOME / 'restore.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        require(not list(HOME.glob('restore-*')), 'Previous scratch requires ownership/process review')
        manifest(point, expected)
        require(shutil.disk_usage(HOME).free >= 80 * 1024**3, 'Less than 80 GiB restore space')
        run = HOME / 'evidence' / (point + '-' + str(time.time_ns()))
        run.mkdir(parents=True)
        scratch = Path(tempfile.mkdtemp(prefix='restore-', dir=HOME))
        (scratch / 'owner').write_text('erp-native-backup-health\n')
        binary = HOME / 'runtime/usr/local/bin'
        db = scratch / 'db'
        socket = scratch / 'socket'
        socket.mkdir()
        env = dict(os.environ, PGHOST=str(socket), PGPORT='55481', PGUSER='postgres', PGCONNECT_TIMEOUT='10')
        env.pop('PGPASSWORD', None)
        started = False
        with (run / 'restore.log').open('w') as log:
            def command(name, *args, capture=False):
                return subprocess.run([str(binary / name), *args], env=env, check=True,
                                      stdout=subprocess.PIPE if capture else log,
                                      stderr=log, text=True).stdout
            try:
                require(command('postgres', '--version', capture=True).startswith('postgres (PostgreSQL) 16.'), 'Wrong PostgreSQL major')
                command('initdb', '-D', str(db), '--data-checksums', '--locale=C', '-E', 'UTF8', '-U', 'postgres', '--auth-local=trust', '--auth-host=reject')
                with (db / 'postgresql.conf').open('a') as config:
                    config.write("\nlisten_addresses=''\nport=55481\nunix_socket_directories='" + str(socket) + "'\nunix_socket_permissions=0700\nmax_connections=8\nshared_buffers='128MB'\nwork_mem='8MB'\nmaintenance_work_mem='256MB'\nmax_parallel_workers=0\nmax_locks_per_transaction=8192\nwal_level=minimal\nmax_wal_senders=0\nfsync=on\n")
                command('pg_ctl', '-D', str(db), '-l', str(run / 'postgres.log'), '-w', 'start')
                started = True
                require(command('psql', '-XAt', '-d', 'postgres', '-c', 'SHOW data_directory', capture=True).strip() == str(db), 'Wrong database target')
                command('createdb', 'recovered')
                with (run / 'sql.log').open('w') as sql_log:
                    proc = subprocess.Popen([str(binary / 'psql'), '-X', '-1', '-q', '-v', 'ON_ERROR_STOP=1', '-d', 'recovered', '-f', '-'],
                                            env=env, stdin=subprocess.PIPE, stdout=sql_log, stderr=subprocess.STDOUT)
                    try:
                        with gzip.open(NAS / (point + '.sql.gz'), 'rb') as sql:
                            shutil.copyfileobj(sql, proc.stdin)
                        proc.stdin.close()
                    except BaseException:
                        proc.kill()
                        proc.wait()
                        raise
                    require(proc.wait() == 0, 'SQL restore failed; inspect sql.log')
                def query(sql):
                    return command('psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-d', 'recovered', '-c', sql, capture=True).strip()
                require(query('SELECT count(*) FROM pg_index WHERE NOT indisvalid') == '0', 'Invalid restored indexes')
                require(int(query('SELECT count(*) FROM django_migrations')) > 0, 'Missing ERP migrations')
                tables = int(query("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p')"))
                paths = recover_files(point, scratch)
                # Same consumer check as the previously verified recovery, without LIMIT.
                refs = query("SELECT archivo FROM logistica_documentounidad WHERE archivo <> ''").splitlines()
                require(refs and all(path in paths for path in refs), 'Unrecoverable document references')
                manifest(point, expected)  # Reject a concurrently replaced source set.
                proof = dict(point=point, postgresql_major=16, sql_restore_exit=0, media_restore_exit=0,
                             verified_media=len(paths), verified_references=len(refs), media_generations=['latest'],
                             manifest_sha256=expected, public_tables=tables, verified_epoch=time.time())
                log.write(json.dumps(proof) + '\n')
                log.flush()
            finally:
                signal.alarm(0)
                if started or (db / 'postmaster.pid').exists():
                    command('pg_ctl', '-D', str(db), '-m', 'fast', '-w', 'stop')
                require((scratch / 'owner').read_text() == 'erp-native-backup-health\n', 'Wrong scratch owner')
                shutil.rmtree(scratch)
        proof['evidence_sha256'] = sha(run / 'restore.log')
        (run / 'receipt.json').write_text(json.dumps(proof) + '\n')
        print(json.dumps(dict(receipt=proof, evidence=(run / 'restore.log').read_text(), run=str(run))), flush=True)


def prepare_runtime():
    runtime = HOME / 'runtime'
    binary = runtime / 'usr/local/bin'
    loader = runtime / 'lib/ld-musl-aarch64.so.1'
    require(loader.is_file(), 'Missing pinned ARM64 runtime')
    for name in ('postgres', 'initdb', 'pg_ctl', 'psql', 'createdb'):
        target = binary / name
        real = binary / (name + '.real')
        if real.exists():
            continue
        require(target.read_bytes()[:4] == b'\x7fELF', 'Invalid runtime binary')
        target.rename(real)
        target.write_text('#!/bin/sh\nexport ICU_DATA="' + str(runtime / 'usr/share/icu/76.1') + '"\nexec "' + str(loader) + '" --library-path "' + ':'.join(str(runtime / part) for part in ('lib', 'usr/lib', 'usr/local/lib')) + '" "' + str(real) + '" "$@"\n')
        target.chmod(0o700)
    subprocess.run([str(binary / 'postgres'), '--version'], check=True)


if __name__ == '__main__':
    if sys.argv[1:] == ['--prepare-runtime']:
        prepare_runtime()
    else:
        restore(*sys.argv[1:])
