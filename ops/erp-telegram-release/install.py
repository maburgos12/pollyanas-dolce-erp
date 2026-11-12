"""Install from the reviewed, clean production Git revision; preserve other consumers."""
import fcntl
import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import stat

BASELINE = 'a91ec99928754323c6113c43abaac30e315a82f599520fd5b3132078b6979069'
TARGET = Path('/usr/local/libexec/erp-point-decision')
HOOK = """    if isinstance(query.get('data'), str) and query['data'].startswith('d:'):
        from erp_telegram_release import handle as release_handle
        return release_handle(query, config, state, telegram)
"""
CLEANUP_HOOK = """    if isinstance(query.get('data'), str) and query['data'].startswith('c:'):
        from erp_telegram_cleanup import handle as cleanup_handle
        return cleanup_handle(query, config, state, telegram)
"""
ANCHOR = 'def handle(query, config, state, telegram, now=None):\n'


def patched(source, cleanup=True):
    if hashlib.sha256(source).hexdigest() != BASELINE:
        raise ValueError('Point consumer changed: review new baseline before installation')
    text = source.decode()
    if text.count(ANCHOR) != 1:
        raise ValueError('invalid_hook_anchor')
    return text.replace(ANCHOR, ANCHOR + HOOK + (CLEANUP_HOOK if cleanup else '')).encode()


def replace_file(path, content, mode=0o644):
    temporary = path.with_suffix(path.suffix + '.release-pending')
    with temporary.open('wb') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.chmod(mode)
    temporary.replace(path)


def install(source, target=TARGET, units=Path('/etc/systemd/system'), run=subprocess.run,
            state=Path('/var/lib/erp-point-decision/releases'), preflight=None):
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = state.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError('unsafe_ledger_directory')
    descriptor = os.open(state / '.worker.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _install(source, target, units, run, preflight)


def check_policy(source):
    spec = importlib.util.spec_from_file_location('release_install_preflight', source / 'erp_telegram_release.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.validate_protection(module.GitHub().api)


def _install(source, target, units, run, preflight):
    consumer = target / 'decidir_corte_point.py'
    current = consumer.read_bytes()
    backup = target / ('decidir_corte_point.' + BASELINE + '.py')
    original = backup.read_bytes() if backup.exists() else current
    updated = patched(original)
    if current not in (original, patched(original, cleanup=False), updated):
        raise ValueError('Point consumer changed since baseline')
    compile(updated, str(consumer), 'exec')
    module = (source / 'erp_telegram_release.py').read_bytes()
    compile(module, 'erp_telegram_release.py', 'exec')
    cleanup_module = (source / 'erp_telegram_cleanup.py').read_bytes()
    compile(cleanup_module, 'erp_telegram_cleanup.py', 'exec')
    (preflight if preflight is not None else lambda: check_policy(source))()
    names = ('erp-telegram-release.service', 'erp-telegram-release.timer')
    run(['systemd-analyze', 'verify', *[str(source / name) for name in names]], check=True)
    active = run(['systemctl', 'show', '--property=ActiveState', '--value',
                  'erp-telegram-release.service'], check=True, capture_output=True, text=True).stdout.strip()
    if active not in {'inactive', 'failed'}:
        raise ValueError('publication_active_install_refused')
    timer_loaded = run(['systemctl', 'show', '--property=LoadState', '--value',
                        'erp-telegram-release.timer'], check=True, capture_output=True, text=True).stdout.strip()
    if timer_loaded not in {'loaded', 'not-found'}:
        raise ValueError('unexpected_release_timer_state')
    timers = ('erp-telegram-release.timer', 'erp-mobile-approval.timer')
    prior = {name: {kind: run(['systemctl', 'is-' + kind, '--quiet', name], check=False).returncode == 0
                    for kind in ('enabled', 'active')} for name in timers}
    changes = {target / 'erp_telegram_release.py': module,
               target / 'erp_telegram_cleanup.py': cleanup_module,
               **{units / name: (source / name).read_bytes() for name in names}, consumer: updated}
    before = {path: path.read_bytes() if path.exists() else None for path in changes}
    if not backup.exists():
        replace_file(backup, original, 0o600)
    if backup.read_bytes() != original:
        raise ValueError('backup_verification_failed')
    try:
        # Quiesce scheduling, but never interrupt a publication already in progress.
        if timer_loaded == 'loaded':
            run(['systemctl', 'stop', 'erp-telegram-release.timer'], check=True)
        for path, content in changes.items():
            replace_file(path, content)
        run(['systemctl', 'daemon-reload'], check=True)
        run(['systemctl', 'restart', 'erp-point-decision.service'], check=True)
        run(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'], check=True)
        run(['systemctl', 'enable', '--now', 'erp-telegram-release.timer'], check=True)
        run(['systemctl', 'disable', '--now', 'erp-mobile-approval.timer'], check=True)
    except Exception:
        run(['systemctl', 'stop', 'erp-telegram-release.timer'], check=False)
        for path, content in before.items():
            if content is not None:
                replace_file(path, content)
            # Newly installed inactive files remain for diagnosis; no resources are deleted.
        run(['systemctl', 'daemon-reload'], check=True)
        run(['systemctl', 'restart', 'erp-point-decision.service'], check=True)
        run(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'], check=True)
        for name, states in prior.items():
            run(['systemctl', 'enable' if states['enabled'] else 'disable', name], check=True)
            run(['systemctl', 'start' if states['active'] else 'stop', name], check=True)
        raise
    print('Installed release hook; Point baseline backup verified. No resource cleanup performed.')


def main():
    if os.geteuid() != 0:
        raise ValueError('root required')
    source = Path(__file__).resolve().parent
    if source != Path('/opt/pastelerias-erp/ops/erp-telegram-release'):
        raise ValueError('install only from official production Git checkout')
    if subprocess.check_output(['git', '-C', '/opt/pastelerias-erp', 'status', '--porcelain']).strip():
        raise ValueError('dirty production checkout')
    install(source)


if __name__ == '__main__':
    main()
