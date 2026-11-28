"""One recurring worker; exact Telegram approval plus a durable supervisor.

No second Telegram consumer, default deploy fallback, timer mutation or automatic
reconciliation. Contention before merge preserves the decision for a later tick.
"""
from contextlib import contextmanager, ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import time

from erp_release_host import HostAdapter, bounded
from erp_release_supervisor import Supervisor
from erp_release_resolution import resolved
import erp_telegram_release as canonical

DEPLOY = Path('/run/lock/erp-deploy.lock')
ROUTER = Path('/run/pastelerias-mac-ia/worker.lock')
NATIVE_LOCKS = (Path('/var/lib/erp-point-archive/.worker.lock'),
                Path('/var/lib/erp-cleanup-prepare/.worker.lock'),
                Path('/var/lib/erp-point-decision/cleanups/.worker.lock'))


@contextmanager
def lease(path):
    # Native services create their own lock files; do not invent a second lock.
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_mode & 0o022 or info.st_nlink != 1):
            raise ValueError('unsafe_shared_lock')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield fd
    finally:
        os.close(fd)


def leases_intact(held):
    for path, fd in held:
        a, b = os.fstat(fd), path.lstat()
        if not stat.S_ISREG(b.st_mode) or (a.st_dev, a.st_ino) != (b.st_dev, b.st_ino):
            return False
        if b.st_uid != os.geteuid() or b.st_mode & 0o022 or b.st_nlink != 1:
            return False
    return True


def private_directory(path):
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError('unsafe_supervision_directory')
    return path


def read_state(path):
    if not path.exists():
        return {}
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('unsafe_supervision_receipt')
        raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError('supervision_receipt_limit')
        return json.loads(raw)


def save_state(path, value):
    raw = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    if len(raw) > 65536:
        raise ValueError('supervision_receipt_limit')
    temporary = path.with_suffix('.pending')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def notice(path, row, reason, config, send):
    state = read_state(path)
    now = int(time.time())
    if state.get('reason') == reason and now - state.get('notified_at', 0) < 6 * 3600:
        return
    messages = {
        'busy': 'Aprobación registrada. El servidor está ocupado; se revalidará en el siguiente ciclo sin interrumpir tareas.',
        'precheck': 'Aprobación registrada. La comprobación de despliegue seguro no pasó; requiere revisión. No se reinició ningún servicio.',
        'hold': 'Publicación detenida para revisión. Se conserva la evidencia y no se repetirá el despliegue automáticamente.',
        'stall': 'El despliegue superó su plazo de supervisión y aún puede estar trabajando. Se conservan los candados hasta que termine; requiere revisión y no se enviará otro reinicio.',
        'limit': 'La revisión alcanzó el límite de tres fallos previos al despliegue. Se requiere corregir la causa antes de continuar.',
    }
    # Mark before sending: a lost response must not flood the private chat.
    state.update(reason=reason, notified_at=now, nonce=row['nonce'], head_sha=row['head_sha'])
    save_state(path, state)
    result = send(config, 'sendMessage', {'chat_id': row['chat_id'], 'text':
        f"ERP PR #{row['pr']} · {messages[reason]}\nRevisión: {row['head_sha']}"})
    canonical.verify_message(result, row['chat_id'])


def prepare_objects(api, row, root):
    canonical.eligible(api, row['pr'], row['head_sha'], row['base_sha'])
    # Fetch only objects from the fixed repository. Never pull, checkout or merge.
    remote = bounded(['/usr/bin/git', '-C', str(root), 'remote', 'get-url', 'origin']).strip()
    if remote not in ('git@github.com:' + canonical.REPO + '.git',
                      'https://github.com/' + canonical.REPO + '.git'):
        raise ValueError('unexpected_repository')
    bounded(['/usr/bin/git', '-C', str(root), 'fetch', '--no-tags', 'origin', row['head_sha']], timeout=60)


def native_provenance(root, base):
    """Bind the actual native consumers to the lock contract in reviewed Git."""
    files = {}
    for name in ('erp-point-archive', 'erp-cleanup-prepare', 'erp-cleanup-execute'):
        unit = name + '.service'
        installed = Path('/etc/systemd/system') / unit
        fragment = bounded(['systemctl', 'show', '--property=FragmentPath', '--value', unit]).strip()
        dropins = bounded(['systemctl', 'show', '--property=DropInPaths', '--value', unit]).strip()
        if fragment != str(installed) or dropins:
            raise ValueError('native_unit_override')
        files[installed] = ('scripts/ops/' + unit)
    for name in ('erp_point_archive.py', 'erp_cleanup_prepare.py', 'erp_cleanup_execute.py'):
        files[root / 'ops/erp-telegram-release' / name] = 'ops/erp-telegram-release/' + name
    for name in ('erp_telegram_release.py', 'erp_telegram_cleanup.py',
                 'erp_release_preservation.py', 'erp_release_snapshot.py',
                 'erp_release_supervisor.py', 'erp_release_host.py',
                 'erp_release_worker.py', 'erp_release_resolution.py'):
        files[Path('/usr/local/libexec/erp-point-decision') / name] = 'ops/erp-telegram-release/' + name
    pins = {}
    for path, relative in files.items():
        expected = bounded(['/usr/bin/git', '-C', str(root), 'show', base + ':' + relative]).encode()
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
                or info.st_nlink != 1 or path.read_bytes() != expected):
            raise ValueError('native_source_mismatch')
        pins[path] = hashlib.sha256(expected).hexdigest()
    def unchanged():
        for path, digest in pins.items():
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                return False
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                return False
        return True
    return unchanged


def run(api, ledger, config, send, *, adapter_factory=HostAdapter,
        supervisor_factory=Supervisor, fetch=prepare_objects,
        provenance=native_provenance,
        deploy_path=DEPLOY, router_path=ROUTER, native_paths=NATIVE_LOCKS):
    directory = private_directory(ledger.directory / 'supervision')
    with ExitStack() as stack:
        # Same order as official deploy/cleanup. Nonblocking: no circular waits.
        held = [(deploy_path, stack.enter_context(lease(deploy_path))),
                (router_path, stack.enter_context(lease(router_path)))]

        def publisher(nonce, api, ledger):
            row = ledger.read(nonce)
            if row['state'] == 'rejected' or int(time.time()) >= row['expires']:
                return canonical.publish(nonce, api, ledger, deployer=None)
            status = directory / (nonce + '.json')
            # A merge/stop/official attempt with uncertain outcome blocks further
            # mutation globally, even after a service crash. Only operator review
            # can reconcile the canonical row; changing a nonce cannot reset it.
            for item in ledger.directory.glob('*.json'):
                historical = ledger.read(item.stem)
                if (historical['state'] == 'executing'
                        or (historical['state'] == 'uncertain' and not resolved(historical, api, ledger))):
                    notice(status, row, 'hold', config, send)
                    return row
            counts = directory / ('failures-' + row['head_sha'] + '.json')
            failures = read_state(counts).get('count', 0)
            if type(failures) is not int or failures >= 3:
                notice(status, row, 'limit', config, send)
                return row
            with ExitStack() as native:
                try:
                    local = [(p, native.enter_context(lease(p))) for p in native_paths]
                except BlockingIOError:
                    notice(status, row, 'busy', config, send)
                    return row
                code_intact = lambda: False
                adapter = adapter_factory(held[0][1], lease_check=lambda: leases_intact(held + local) and code_intact())
                adapter.on_stall = lambda _code: notice(status, row, 'stall', config, send)
                supervisor = None
                ready = False
                try:
                    fetch(api, row, adapter.root)
                    code_intact = provenance(adapter.root, row['base_sha'])
                    attempt = directory / (nonce + '-' + str(time.time_ns()))
                    supervisor = supervisor_factory(adapter, attempt)
                    supervisor.preflight(row)
                    ready = True
                except Exception:
                    busy = supervisor is not None and supervisor.retryable
                    if not busy:
                        save_state(counts, {'head_sha': row['head_sha'], 'count': failures + 1})
                    notice(status, row, 'busy' if busy else 'precheck', config, send)
                    return row
                finally:
                    if not ready:
                        if supervisor is not None:
                            supervisor.close()
                        adapter.close()
                try:
                    # publish revalidates expiry, current approval, CI, base,
                    # parents/tree and remote main before invoking this callback.
                    return canonical.publish(nonce, api, ledger, deployer=supervisor)
                finally:
                    supervisor.close()
                    adapter.close()

        canonical.worker(api, ledger, config, send, publisher=publisher)
