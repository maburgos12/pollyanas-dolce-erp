#!/usr/bin/python3
"""Retire only exact ERP backup sets already restored from the actual NAS."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import signal
import tempfile
import subprocess
import sys
import time

BACKUPS = Path('/opt/backups/erp')
EXPORT = BACKUPS / 'hbs-export/daily'
RECEIPTS = Path('/var/lib/erp-backup-restore/receipts')
VERIFICATION_RECEIPTS = RECEIPTS.with_name('verification-receipts')
NAS = Path('/opt/pastelerias-erp/storage/nas/compartidos/erp_backups')
NAS_SOURCE = '//10.77.216.2/ERP_BACKUPS'
VERSIONS = Path('ERP_ACTIVE_MEDIA_VERSIONS/ERP medios activos - 7 versiones')
POINT = re.compile(r'backup_[0-9]{8}_[0-9]{6}')
GENERATION = re.compile(r'latest|[0-9]{12}\.Q')
SHA256 = re.compile(r'[a-f0-9]{64}')
PROBE_LOCK = Path('/var/lib/erp-backup-restore/nas-probe.lock')
PROBE_TIMEOUT = 60 * 60  # Full NAS media checks exceed 20 minutes; rotation caps its own total.
ROTATION_TIMEOUT = 34 * 60  # Existing cron kills the entire rotation after 35 minutes.

PAYLOADS = ('sql.gz', 'conteos.tar.gz', 'inventory-audit.tar.gz', 'media.json')

def require(condition, reason):
    if not condition:
        raise ValueError(reason)

def secure_file(path, private=False):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid(),
            'archivo local ausente, no regular o de otro propietario')
    require(not info.st_mode & (0o077 if private else 0o022), 'permisos locales inesperados')
    return info

def digest(path, device=None, size=None):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as f:
        first = os.fstat(f.fileno())
        require(stat.S_ISREG(first.st_mode), 'contenido no regular')
        require(device is None or first.st_dev == device, 'el archivo no reside en el NAS')
        require(size is None or first.st_size == size, 'tamaño de medio distinto')
        checksum = hashlib.file_digest(f, 'sha256').hexdigest()
        last = os.fstat(f.fileno())
        require((first.st_size, first.st_mtime_ns) == (last.st_size, last.st_mtime_ns),
                'archivo cambió durante la lectura')
    return checksum

def local_set(root, stem):
    manifest = root / (stem + '.manifest')
    secure_file(manifest)
    raw = manifest.read_bytes()
    payloads = {}
    for line in raw.decode('ascii').splitlines():
        checksum, name = line.split()
        require(SHA256.fullmatch(checksum) and name not in payloads, 'manifiesto inválido')
        payloads[name] = checksum
    require(set(payloads) == {stem + '.' + suffix for suffix in PAYLOADS},
            'conjunto incompleto o formato histórico')
    for name, checksum in payloads.items():
        path = root / name
        secure_file(path)
        require(digest(path) == checksum, 'el respaldo local no coincide con su manifiesto')
    return manifest.name, hashlib.sha256(raw).hexdigest(), payloads

def read_receipt(path, stem):
    secure_file(path, private=True)
    proof = json.loads(path.read_text())
    require(proof['point'] == stem and proof['postgresql_major'] == 16
            and proof['sql_restore_exit'] == proof['media_restore_exit'] == 0
            and proof['verified_media'] > 0 and proof['verified_references'] > 0
            and SHA256.fullmatch(proof['manifest_sha256'])
            and SHA256.fullmatch(proof['evidence_sha256']), 'prueba de recuperación inválida')
    evidence = path.with_suffix('.restore.log')
    secure_file(evidence, private=True)
    require(digest(evidence) == proof['evidence_sha256'], 'evidencia de restauración ausente o distinta')
    generations = proof['media_generations']
    require(isinstance(generations, list) and 1 <= len(generations) <= 3
            and all(isinstance(g, str) and GENERATION.fullmatch(g) for g in generations),
            'generación HBS no registrada')
    return proof

def actual_nas_device(nas):
    (nas / 'ERP_ACTIVE_MEDIA_VERSIONS').stat()  # Activate the existing automount.
    line = subprocess.check_output(['/usr/bin/findmnt', '-rn', '-t', 'cifs',
                                    '--mountpoint', str(nas), '-o', 'SOURCE,OPTIONS'], text=True)
    source, options = line.strip().split()
    require(source == NAS_SOURCE and 'ro' in options.split(','), 'montaje NAS no verificado')
    return nas.stat().st_dev

def check_proof(root, nas, stem, proof, nas_device):
    manifest, checksum, payloads = local_set(root, stem)
    require(checksum == proof['manifest_sha256'], 'prueba corresponde a otro manifiesto')
    require(digest(nas / manifest, nas_device) == checksum, 'manifiesto NAS distinto')
    for name, expected in payloads.items():
        require(digest(nas / name, nas_device) == expected, 'copia NAS incompleta o distinta')
    media = json.loads((root / (stem + '.media.json')).read_text())
    require(media['version'] == 1 and len(media['files']) == proof['verified_media'],
            'inventario de medios distinto de la prueba')
    seen = set()
    for row in media['files']:
        relative = PurePosixPath(row['path'])
        require(relative.parts and not relative.is_absolute() and '..' not in relative.parts
                and relative not in seen and SHA256.fullmatch(row['sha256'])
                and isinstance(row['bytes'], int) and row['bytes'] >= 0,
                'ruta o huella de medios inválida')
        seen.add(relative)
    aliases = proof.get('media_aliases', {})
    require(isinstance(aliases, dict) and len(set(aliases.values())) == len(aliases),
            'alias NAS inválidos')
    for name, alias in aliases.items():
        original, exposed = PurePosixPath(name), PurePosixPath(alias)
        require(original in seen and exposed.parts and not exposed.is_absolute()
                and '..' not in exposed.parts and exposed.parent == original.parent
                and exposed != original, 'alias NAS fuera de la ruta comprobada')
    for generation in proof['media_generations']:
        directory = nas / VERSIONS / generation / 'ERP_BACKUPS/ERP_ACTIVE_MEDIA_STAGING'
        if not directory.is_dir() or directory.stat().st_dev != nas_device:
            continue
        def verify(row):
            name = row['path']
            try:
                checksum = digest(directory / name, nas_device, row['bytes'])
            except FileNotFoundError:
                # A root-approved receipt may bind the exact CIFS alias to this restored file.
                if name not in aliases:
                    raise
                checksum = digest(directory / aliases[name], nas_device, row['bytes'])
            return checksum == row['sha256']
        try:
            pool = ThreadPoolExecutor(max_workers=3)
            try:
                require(all(pool.map(verify, media['files'])), 'huella de medio distinta')
            finally:
                pool.shutdown(cancel_futures=True)
            return manifest, payloads, generation
        except (OSError, ValueError):
            continue
    raise ValueError('los medios verificados ya no coinciden con una generación accesible del NAS')

def probe(request):
    # Only this reader holds the NAS lock; never inherit the parent's backup lock.
    with PROBE_LOCK.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        root, nas = Path(request['root']), Path(request['nas'])
        stem, proof = request['stem'], request['proof']
        require(POINT.fullmatch(stem), 'identificador inválido')
        result = check_proof(root, nas, stem, proof, actual_nas_device(nas))
        return {'request': request, 'result': result}


def bounded_check_proof(root, nas, stem, proof, timeout=PROBE_TIMEOUT):
    request = dict(root=str(root), nas=str(nas), stem=stem, proof=proof)
    # Regular local files avoid pipe EOF waits when an unkillable reader survives.
    with tempfile.TemporaryFile() as source, tempfile.TemporaryFile() as output:
        source.write(json.dumps(request).encode())
        source.seek(0)
        child = None
        def terminate(signum, _frame):
            raise SystemExit(128 + signum)
        # Block TERM only across spawn/handler setup so the reader cannot escape that race.
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
        previous_handler = signal.signal(signal.SIGTERM, terminate)
        try:
            child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--nas-probe'],
                                     stdin=source, stdout=output, stderr=output,
                                     close_fds=True, start_new_session=True)
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            try:
                start_ticks = Path(f'/proc/{child.pid}/stat').read_text().rpartition(')')[2].split()[19]
            except (OSError, IndexError):
                start_ticks = 'unavailable'
            try:
                code = child.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raise ValueError(f'NAS verification timed out; reader PID {child.pid}, start_ticks {start_ticks}; backups preserved') from None
        except BaseException:
            if child is not None:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            # A CIFS task in D cannot be reaped yet; never wait while holding backup.flock.
            raise
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        output.seek(0)
        raw = output.read(1024 * 1024 + 1)
        require(len(raw) <= 1024 * 1024, 'respuesta NAS demasiado grande')
        require(code == 0, 'NAS verification failed: ' + raw.decode(errors='replace')[-2000:])
        response = json.loads(raw)
        require(response['request'] == request, 'respuesta NAS corresponde a otra solicitud')
        manifest, checksum, payloads = local_set(root, stem)
        require(checksum == proof['manifest_sha256'], 'manifiesto local cambió durante la verificación NAS')
        result = response['result']
        require(result[:2] == [manifest, payloads] and GENERATION.fullmatch(result[2]),
                'resultado NAS no coincide con el conjunto local')
        return manifest, payloads, result[2]


def retirement_links(root, export, manifest, payloads):
    # Remove completion markers first and reject links owned by another workflow.
    links = []
    for name in (manifest, *payloads):
        original, published = root / name, export / name
        info = secure_file(original)
        if published.exists() or published.is_symlink():
            secure_file(published)
            require(published.samefile(original), 'exportación apunta a otro archivo')
            require(info.st_nlink == 2, 'otro propietario conserva un enlace local')
            links.append(published)
        else:
            require(info.st_nlink == 1, 'otro propietario conserva un enlace local')
        links.append(original)
    return links


def retire_point(root, export, receipts, nas, stem):
    require(POINT.fullmatch(stem), 'identificador inválido')
    for path in (root, export, receipts):
        info = path.lstat()
        require(path.resolve() == path and stat.S_ISDIR(info.st_mode)
                and info.st_uid == os.geteuid() and not info.st_mode & 0o022,
                'directorio inesperado o no protegido')
    lock_path = root / '.backup.flock'
    secure_file(lock_path)
    with lock_path.open('r') as lock:
        def protected_latest():
            points = sorted(p.stem for p in root.glob('backup_*.manifest')
                            if POINT.fullmatch(p.stem))
            require(stem in points and stem != points[-1], 'el objetivo falta o es el último respaldo local')
            require(not list(root.glob('backup_*.incomplete')), 'respaldo local incompleto')
            return points[-1]

        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        protected_latest()
        receipt = receipts / (stem + '.json')
        proof = read_receipt(receipt, stem)
        fcntl.flock(lock, fcntl.LOCK_UN)
        manifest, payloads, generation = bounded_check_proof(root, nas, stem, proof)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        newest = protected_latest()
        require(read_receipt(receipt, stem) == proof, 'prueba local cambió')
        require(local_set(root, stem) == (manifest, proof['manifest_sha256'], payloads),
                'respaldo objetivo cambió durante la verificación')
        local_set(root, newest)
        for path in retirement_links(root, export, manifest, payloads):
            path.unlink()
    print(f'Retirado {stem}: NAS y medios {generation} verificados; conservado {newest}', flush=True)


def rotate(root, export, receipts, nas, apply):
    deadline = time.monotonic() + ROTATION_TIMEOUT
    for path in (root, export, receipts):
        require(path.resolve() == path and path.is_dir(), 'ruta local inesperada')
    lock = root / '.backup.flock'
    secure_file(lock)
    with lock.open('r') as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stems = sorted(p.stem for p in root.glob('backup_*.manifest') if POINT.fullmatch(p.stem))
        if not stems:
            return
        newest = stems.pop()
        print(f'Conservado {newest}: último punto local', flush=True)
        if not stems:
            return
        require(not (root / (newest + '.incomplete')).exists(), 'punto más reciente incompleto')
        local_set(root, newest)
        for stem in stems:
            try:
                proof = read_receipt(receipts / (stem + '.json'), stem)
                require(not (root / (stem + '.incomplete')).exists(), 'conjunto con marca incompleta')
                remaining = deadline - time.monotonic()
                require(remaining > 0, 'presupuesto total de rotación agotado; conjunto conservado')
                manifest, payloads, generation = bounded_check_proof(root, nas, stem, proof,
                                                                    timeout=min(PROBE_TIMEOUT, remaining))
                require(read_receipt(receipts / (stem + '.json'), stem) == proof, 'prueba local cambió')
                require(not list(root.glob('backup_*.incomplete')), 'respaldo incompleto durante la verificación')
                local_set(root, newest)
                links = retirement_links(root, export, manifest, payloads)
                require(time.monotonic() < deadline, 'presupuesto total de rotación agotado; conjunto conservado')
            except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as e:
                reason = str(e) if isinstance(e, ValueError) else type(e).__name__
                print(f'Conservado {stem}: {reason}', flush=True)
                continue
            if apply:
                for path in links:
                    path.unlink()
            print(f'{"Retirado" if apply else "Retirable"} {stem}: NAS y medios {generation} verificados',
                  flush=True)

def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply', action='store_true')
    mode.add_argument('--retire-point', metavar='PUNTO', help='Retirar solo un respaldo antiguo tras revalidar el NAS')
    mode.add_argument('--check-proof')
    mode.add_argument('--nas-probe', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.nas_probe:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM})
        print(json.dumps(probe(json.load(sys.stdin))), flush=True)
    elif args.retire_point:
        stem = args.retire_point
        require(POINT.fullmatch(stem), 'identificador inválido')
        receipts = VERIFICATION_RECEIPTS if (VERIFICATION_RECEIPTS / (stem + '.json')).exists() else RECEIPTS
        retire_point(BACKUPS, EXPORT, receipts, NAS, stem)
    elif args.check_proof:
        stem = args.check_proof
        require(POINT.fullmatch(stem), 'identificador inválido')
        proof = read_receipt(RECEIPTS / (stem + '.json'), stem)
        _, _, generation = bounded_check_proof(BACKUPS, NAS, stem, proof)
        print(f'Prueba y copia NAS coinciden para {stem}, medios {generation}')
    else:
        rotate(BACKUPS, EXPORT, RECEIPTS, NAS, args.apply)

if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f'Rotación detenida: {error}', file=sys.stderr)
        sys.exit(1)
