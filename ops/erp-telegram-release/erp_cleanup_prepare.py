"""Prepare bounded, recoverable cleanup proposals; never remove candidates."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time

# Use the authorization receiver's installed contract when running on the VPS.
if Path('/usr/local/libexec/erp-point-decision/erp_telegram_cleanup.py').is_file():
    sys.path.insert(0, '/usr/local/libexec/erp-point-decision')
import erp_telegram_cleanup as cleanup
from erp_telegram_release import Ledger, telegram, approval_window

STATE = Path('/var/lib/erp-cleanup-prepare')
MANUAL_PROPOSALS = Path('/var/lib/erp-point-decision/cleanup-proposals')
MAX_BYTES = 1_000_000_000
MAX_FILES = 256
ARCHIVE_LIMIT = MAX_BYTES + 2_000_000
RESERVE = 5 * 1024**3
RETRY = 6 * 3600
NAS_ROOT = '/share/ERP_BACKUPS/vps-cleanup'
NAS_RESOLVED_ROOT = '/share/CACHEDEV1_DATA/ERP_BACKUPS/vps-cleanup'
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o',
       'ConnectionAttempts=1', '-o', 'ConnectTimeout=15', 'qnap-nasdolce-archive']


def atomic_json(path, value):
    temporary = path.with_suffix('.pending')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def metadata(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode, info.st_nlink]


def snapshot(path):
    if path.resolve() != path or not cleanup.FILE_PATH.fullmatch(str(path)) or 'backup' in str(path).lower():
        raise ValueError('unsafe_source_path')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or not 0 < before.st_size <= MAX_BYTES:
            raise ValueError('unsafe_source_metadata')
        digest = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            chunk = stream.read(min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError('source_changed')
            digest.update(chunk)
            remaining -= len(chunk)
        if stream.read(1) or metadata(before) != metadata(os.fstat(stream.fileno())) or metadata(before) != metadata(path.lstat()):
            raise ValueError('source_changed')
    return dict(path=str(path), bytes=before.st_size, sha256=digest.hexdigest(), metadata=metadata(before))


def in_use(proc=Path('/proc')):
    used = set()
    for pid in proc.iterdir():
        if not pid.name.isdigit():
            continue
        for directory in ('fd', 'map_files'):
            try:
                for link in (pid / directory).iterdir():
                    try:
                        info = link.stat()
                        used.add((info.st_dev, info.st_ino))
                    except FileNotFoundError:
                        pass  # Process or descriptor exited during enumeration.
            except FileNotFoundError:
                pass
    return used


def candidates(reserved=frozenset()):
    paths = []
    for root in (Path('/var/cache/apt'), Path('/var/log')):
        for directory, dirs, files in os.walk(root, followlinks=False):
            depth = len(Path(directory).relative_to(root).parts)
            dirs[:] = [name for name in dirs if depth < 2 and not (Path(directory) / name).is_symlink()]
            paths.extend(Path(directory) / name for name in files if cleanup.FILE_PATH.fullmatch(str(Path(directory) / name)))
    used, result, total = in_use(), [], 0
    for path in sorted(paths):
        if len(result) >= MAX_FILES:
            break
        try:
            info = path.lstat()
            if (info.st_dev, info.st_ino) in used:
                continue
            entry = snapshot(path)
        except (FileNotFoundError, ValueError):
            continue
        if (entry['path'], entry['sha256']) in reserved or entry['bytes'] + total > MAX_BYTES:
            continue
        result.append(entry)
        total += entry['bytes']
    return result


def unchanged(entries):
    used = in_use()
    for entry in entries:
        if tuple(entry['metadata'][:2]) in used or snapshot(Path(entry['path'])) != entry:
            raise ValueError('source_changed_or_in_use')


class HashWriter:
    def __init__(self, stream):
        self.stream, self.digest, self.size = stream, hashlib.sha256(), 0

    def write(self, data):
        self.size += len(data)
        if self.size > ARCHIVE_LIMIT:
            raise ValueError('archive_limit')
        self.digest.update(data)
        return self.stream.write(data)

    def flush(self):
        self.stream.flush()


def write_archive(stream, entries):
    writer = HashWriter(stream)
    with tarfile.open(fileobj=writer, mode='w|gz', format=tarfile.USTAR_FORMAT) as archive:
        for entry in entries:
            path = Path(entry['path'])
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as source:
                if metadata(os.fstat(source.fileno())) != entry['metadata']:
                    raise ValueError('source_changed')
                info = tarfile.TarInfo(str(path).lstrip('/'))
                info.size, info.mode = entry['bytes'], 0o600
                archive.addfile(info, source)
                if metadata(os.fstat(source.fileno())) != entry['metadata']:
                    raise ValueError('source_changed')
    return writer.digest.hexdigest(), writer.size


@contextmanager
def ssh_stream(command, **kwargs):
    process = subprocess.Popen(SSH + [command], stderr=subprocess.DEVNULL, **kwargs)
    timer = threading.Timer(240, process.kill)
    timer.start()
    try:
        yield process
        if process.wait(timeout=15):
            raise ValueError('nas_transfer_failed')
    finally:
        timer.cancel()
        if process.poll() is None:
            process.kill()
        process.wait(timeout=15)


class NAS:
    def guarded(self, command):
        # QNAP's share symlink is intentional; bind every operation to its fixed backing directory.
        return (f'test "$(readlink -f {NAS_ROOT})" = {NAS_RESOLVED_ROOT} '
                f'&& test -d {NAS_ROOT} && test ! -L {NAS_ROOT} && ( {command} )')

    def command(self, command):
        return subprocess.run(SSH + [self.guarded(command)], check=True, timeout=30, capture_output=True, text=True).stdout.strip()

    def path(self, proposal_id):
        if not cleanup.PROPOSAL_ID.fullmatch(proposal_id):
            raise ValueError('invalid_id')
        return f'{NAS_ROOT}/{proposal_id}/files-recovery.tar.gz'

    def reserve(self, proposal_id, required):
        # Fixed parent must already be provisioned; never create or follow unknown parents.
        self.path(proposal_id)
        output = self.command(f'df -Pk {NAS_ROOT}')
        available = int(output.splitlines()[-1].split()[3]) * 1024
        if available < required + RESERVE:
            raise ValueError('nas_capacity')
        self.command(f'umask 077; mkdir {NAS_ROOT}/{proposal_id}')

    def upload(self, proposal_id, entries):
        path = self.path(proposal_id)
        with ssh_stream(self.guarded(f'umask 077; set -C; cat > {path}'), stdin=subprocess.PIPE) as process:
            result = write_archive(process.stdin, entries)
            process.stdin.close()
        return result

    def digest(self, proposal_id):
        path = self.path(proposal_id)
        value = self.command(f'test ! -L {path} && test -f {path} && sha256sum {path}').split()[0]
        if not cleanup.DIGEST.fullmatch(value):
            raise ValueError('nas_hash_invalid')
        return value

    def download(self, proposal_id, destination, size):
        with ssh_stream(self.guarded(f'test ! -L {self.path(proposal_id)} && test -f {self.path(proposal_id)} && cat {self.path(proposal_id)}'), stdout=subprocess.PIPE) as process:
            with destination.open('xb') as stream:
                copied = 0
                while True:
                    chunk = process.stdout.read(1024 * 1024)
                    if not chunk:
                        break
                    copied += len(chunk)
                    if copied > size or copied > ARCHIVE_LIMIT:
                        raise ValueError('nas_readback_limit')
                    stream.write(chunk)
                if copied != size:
                    raise ValueError('nas_readback_size')


def verify_archive(path, entries, destination):
    expected = {entry['path'].lstrip('/'): entry for entry in entries}
    seen = set()
    # Extract only validated regular members to flat owned files, never archive paths.
    with tarfile.open(path, mode='r|gz') as archive:
        for member in archive:
            if member.name not in expected or member.name in seen or not member.isreg() or member.size != expected[member.name]['bytes']:
                raise ValueError('invalid_archive_member')
            seen.add(member.name)
            digest = hashlib.sha256()
            restored = destination / str(len(seen))
            with archive.extractfile(member) as source, restored.open('xb') as target:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)
                    digest.update(chunk)
                target.flush()
                os.fsync(target.fileno())
            if digest.hexdigest() != expected[member.name]['sha256']:
                raise ValueError('recovery_hash_mismatch')
            digest = hashlib.sha256()
            with restored.open('rb') as verified:
                while chunk := verified.read(1024 * 1024):
                    digest.update(chunk)
            if digest.hexdigest() != expected[member.name]['sha256']:
                raise ValueError('recovery_hash_mismatch')
    if seen != set(expected):
        raise ValueError('recovery_missing_files')


def recover(nas, proposal_id, entries, state):
    total = sum(entry['bytes'] for entry in entries)
    if shutil.disk_usage(state).free < RESERVE + total + ARCHIVE_LIMIT:
        raise ValueError('local_capacity')
    nas.reserve(proposal_id, ARCHIVE_LIMIT)
    unchanged(entries)
    digest, size = nas.upload(proposal_id, entries)
    unchanged(entries)
    if nas.digest(proposal_id) != digest:
        raise ValueError('nas_receipt_hash')
    with tempfile.TemporaryDirectory(prefix='verify-', dir=state) as scratch:
        root = Path(scratch)
        archive = root / 'readback.tar.gz'
        nas.download(proposal_id, archive, size)
        with archive.open('rb') as stream:
            hasher = hashlib.sha256()
            while chunk := stream.read(1024 * 1024):
                hasher.update(chunk)
            received = hasher.hexdigest()
        if received != digest:
            raise ValueError('nas_readback_hash')
        verify_archive(archive, entries, root)
    unchanged(entries)
    return dict(archive=nas.path(proposal_id), sha256=digest, files_verified=len(entries))


def manifest_id(files):
    content = json.dumps(dict(files=files, build_cache=[]), sort_keys=True, separators=(',', ':')).encode()
    return 'sha256:' + hashlib.sha256(content).hexdigest()


def blocked(ledger, identity, now):
    with ledger.lock():
        for file in ledger.directory.glob('*.json'):
            row = ledger.read(file.stem)
            if row['image_id'] == identity or row['state'] == 'notifying' or (row['state'] in {'pending', 'approved'} and row['expires'] > now):
                return True
    return False


def disk_percent():
    result = subprocess.run(['df', '-P', '/'], check=True, capture_output=True, text=True, timeout=5)
    value = result.stdout.splitlines()[-1].split()[4]
    if not value.endswith('%') or not value[:-1].isdigit():
        raise ValueError('invalid_disk_usage')
    return int(value[:-1])


def reserved_files(state):
    reserved = set()
    for prior in state.glob('VPS-LIM-*.json'):
        old = json.loads(prior.read_text())
        if old['phase'] in {'requesting', 'requested'}:
            reserved.update((item['path'], item['sha256']) for item in old['sources'])
    for prior in MANUAL_PROPOSALS.glob('*.json'):
        proposal, _ = cleanup.read_proposal(prior)
        item = proposal['delete_candidates'][0]
        if item['type'] == 'cache_log_manifest':
            reserved.update((entry['path'], entry['sha256']) for entry in item['files'])
    return reserved


def prepare(state, ledger, config, nas=None, send=telegram, now=None):
    if not approval_window()[0]:
        return 'outside_approval_hours'
    now = int(time.time()) if now is None else now
    owner = Ledger(state)
    with owner.lock(worker=True):
        status_path = state / 'status.json'
        status = json.loads(status_path.read_text()) if status_path.exists() else {}
        if status.get('retry_after', 0) > now:
            return 'backoff'
        try:
            if disk_percent() <= 75:
                return 'below_threshold'
            if blocked(ledger, None, now):
                return 'existing_scope_or_request'
            # Filter history before the batch budget so old files cannot starve new candidates.
            entries = candidates(reserved_files(state))
            if not entries:
                raise ValueError('no_eligible_new_candidates')
            files = [{key: entry[key] for key in ('path', 'bytes', 'sha256')} for entry in entries]
            identity = manifest_id(files)
            if blocked(ledger, identity, now):
                return 'existing_scope_or_request'
            attempts = sum(json.loads(path.read_text()).get('manifest_id') == identity for path in state.glob('VPS-LIM-*.json'))
            if attempts >= 3:
                raise ValueError('recovery_attempt_limit_requires_review')
            day = time.strftime('%Y%m%d', time.gmtime(now))
            proposal_id = next((f'VPS-LIM-{day}-{n:02d}' for n in range(1, 100) if not (state / f'VPS-LIM-{day}-{n:02d}.json').exists() and not (MANUAL_PROPOSALS / f'VPS-LIM-{day}-{n:02d}.json').exists()), None)
            if proposal_id is None:
                raise ValueError('daily_id_limit')
            evidence_path = state / f'{proposal_id}.json'
            evidence = dict(phase='preparing', sources=entries, manifest_id=identity, started=now)
            atomic_json(evidence_path, evidence)
            recovery = recover(nas or NAS(), proposal_id, entries, state)
            proposal = dict(id=proposal_id, host='68.183.165.47', status='awaiting_explicit_approval',
                execution='Ningún borrado automático; requiere aprobación vigente y ejecución manual verificada.',
                expected_recovery=f"Hasta {sum(item['bytes'] for item in files)} bytes lógicos; espacio físico por medir.",
                expected_usage='No garantiza bajar del 75%; el monitor conserva su funcionamiento.',
                risk='Los archivos históricos solo estarán disponibles restaurando la copia NAS si se autoriza retirarlos.',
                preconditions=['Revalidar archivos sin uso, metadatos, hashes, recuperación y aprobación vigente antes de retirar.'],
                preserve=['Todos los respaldos, imágenes, bases, volúmenes, contenedores, medios, logs activos y servicios.'],
                delete_candidates=[dict(type='cache_log_manifest', id=identity, files=files, build_cache=[], recovery=recovery)])
            proposal_path = state / f'{proposal_id}.proposal'
            atomic_json(proposal_path, proposal)
            cleanup.read_proposal(proposal_path)
            unchanged(entries)
            if blocked(ledger, identity, now):
                raise ValueError('concurrent_request')
            evidence.update(phase='requesting', recovery=recovery, verified_at=int(time.time()))
            atomic_json(evidence_path, evidence)
            # Mark before contacting Telegram; an ambiguous send is terminal, never blindly retried.
            row = cleanup.request(proposal_path, ledger, config, send)
            evidence.update(phase='requested', nonce=row['nonce'], message_id=row['message_id'])
            atomic_json(evidence_path, evidence)
            return 'requested'
        except Exception as exc:
            reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            reason = reason if reason.replace('_', '').isalnum() and len(reason) <= 80 else 'preparation_failed'
            failure = dict(retry_after=now + RETRY, reason=reason, failed_at=now)
            atomic_json(status_path, failure)
            explanations = {
                'no_eligible_new_candidates': 'No hay archivos nuevos elegibles fuera de los alcances ya propuestos.',
                'local_capacity': 'Falta espacio local para verificar la recuperación conservando 5 GiB libres.',
                'nas_capacity': 'Falta espacio en el NAS conservando 5 GiB libres.',
                'nas_receipt_hash': 'La copia recibida en NAS no coincide con el archivo enviado.',
                'nas_readback_hash': 'La lectura de regreso del NAS no coincide con el archivo enviado.',
                'recovery_hash_mismatch': 'Un archivo restaurado no coincide con su fuente.',
                'source_changed_or_in_use': 'Un archivo cambió o está en uso.',
                'source_changed': 'Un archivo cambió durante la copia.',
                'recovery_attempt_limit_requires_review': 'Tres intentos de recuperación fallaron; se requiere revisión manual.',
            }
            explanation = explanations.get(reason, 'No se pudo verificar de forma segura la preparación o su comunicación.')
            try:
                send(config, 'sendMessage', {'chat_id': config['chat_id'], 'text': f'Preparación limpieza VPS bloqueada. {explanation} Código: {reason}. Nada borrado. Revisión en no menos de 6 horas; solicitudes ambiguas no se reenvían.'})
            except Exception as alert_error:
                failure['alert_error'] = type(alert_error).__name__
                atomic_json(status_path, failure)
            return 'blocked'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true', required=True)
    parser.parse_args()
    if os.geteuid() != 0:
        parser.error('root required')
    os.umask(0o077)
    config = json.loads((Path(os.environ['CREDENTIALS_DIRECTORY']) / 'telegram.json').read_text())
    ledger = Ledger(cleanup.STATE, validator=cleanup.validate)
    result = prepare(STATE, ledger, config)
    print(result)
    if result == 'blocked':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
