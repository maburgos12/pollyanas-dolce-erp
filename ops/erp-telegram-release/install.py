"""Install from the reviewed, clean production Git revision; preserve other consumers."""
import argparse
import fcntl
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import subprocess
import stat
import tempfile

BASELINE = 'a91ec99928754323c6113c43abaac30e315a82f599520fd5b3132078b6979069'
TARGET = Path('/usr/local/libexec/erp-point-decision')
MODULES = ('erp_telegram_release.py', 'erp_telegram_cleanup.py',
           *('erp_release_' + name + '.py' for name in
             ('preservation', 'snapshot', 'supervisor', 'host', 'worker', 'resolution')))
UNITS = ('erp-telegram-release.service', 'erp-telegram-release.timer')
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


def read_regular(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise ValueError('unsafe_install_file')
        return stream.read()


def replace_file(path, content, mode=0o600):
    # Random exclusive temporary, never follow or truncate an existing sidecar.
    if path.exists() or path.is_symlink():
        read_regular(path)
    descriptor, name = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def reviewed_files(source):
    modules = {name: read_regular(source / name) for name in MODULES}
    for name, content in modules.items():
        compile(content, name, 'exec')
    units = {name: read_regular(source / name) for name in UNITS}
    return modules, units


def restore_files(before, changes):
    for path, content in before.items():
        # A foreign edit must never be overwritten as part of our rollback.
        current = read_regular(path) if path.exists() else None
        if current not in (content, changes[path]):
            raise ValueError('rollback_concurrent_file_change')
        if content is not None and current != content:
            replace_file(path, content, 0o644 if path.suffix in ('.service', '.timer') else 0o600)
        # Newly installed files stay inactive for diagnosis; no deletion of others.


def install(source, target=TARGET, units=Path('/etc/systemd/system'), run=subprocess.run,
            state=Path('/var/lib/erp-point-decision/releases'), preflight=None):
    files = reviewed_files(source)  # Missing/syntax-invalid dependencies cause no writes.
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = state.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError('unsafe_ledger_directory')
    descriptor = os.open(state / '.worker.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _install(source, target, units, run, preflight, files)


def check_policy(source):
    spec = importlib.util.spec_from_file_location('release_install_preflight', source / 'erp_telegram_release.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.validate_protection(module.GitHub().api)


def ensure_publisher_inactive(run, operation):
    active = run(['systemctl', 'show', '--property=ActiveState', '--value',
                  'erp-telegram-release.service'], check=True, capture_output=True, text=True).stdout.strip()
    if active not in {'inactive', 'failed'}:
        raise ValueError('publication_active_' + operation + '_refused')


def reconcile_legacy(source, nonces, current_nonce, ledger):
    """Explicit operator request only, called under worker and deployment locks."""
    if not nonces:
        return
    from erp_telegram_release import GitHub
    spec = importlib.util.spec_from_file_location('release_install_resolution', source / 'erp_release_resolution.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    host_spec = importlib.util.spec_from_file_location('release_install_runtime', source / 'erp_release_host.py')
    host = importlib.util.module_from_spec(host_spec)
    host_spec.loader.exec_module(host)
    api = GitHub().api
    for nonce in nonces:
        module.resolve_legacy(nonce, current_nonce, api, ledger,
                              runtime_verify=host.verify_installed_runtime)


def _install(source, target, units, run, preflight, files):
    consumer = target / 'decidir_corte_point.py'
    current = consumer.read_bytes()
    backup = target / ('decidir_corte_point.' + BASELINE + '.py')
    original = backup.read_bytes() if backup.exists() else current
    updated = patched(original)
    if current not in (original, patched(original, cleanup=False), updated):
        raise ValueError('Point consumer changed since baseline')
    compile(updated, str(consumer), 'exec')
    modules, unit_files = files
    (preflight if preflight is not None else lambda: check_policy(source))()
    names = UNITS
    run(['systemd-analyze', 'verify', *[str(source / name) for name in names]], check=True)
    ensure_publisher_inactive(run, 'install')
    timer_loaded = run(['systemctl', 'show', '--property=LoadState', '--value',
                        'erp-telegram-release.timer'], check=True, capture_output=True, text=True).stdout.strip()
    if timer_loaded not in {'loaded', 'not-found'}:
        raise ValueError('unexpected_release_timer_state')
    timers = ('erp-telegram-release.timer', 'erp-mobile-approval.timer')
    prior = {name: {kind: run(['systemctl', 'is-' + kind, '--quiet', name], check=False).returncode == 0
                    for kind in ('enabled', 'active')} for name in timers}
    if timer_loaded == 'loaded':
        run(['systemctl', 'stop', 'erp-telegram-release.timer'], check=True)
    # A tick may have started between the first observation and timer stop.
    # Abort outside rollback: no modules/backup/consumer have been written yet.
    ensure_publisher_inactive(run, 'install')
    changes = {**{target / name: content for name, content in modules.items()},
               **{units / name: content for name, content in unit_files.items()}, consumer: updated}
    before = {path: path.read_bytes() if path.exists() else None for path in changes}
    if not backup.exists():
        replace_file(backup, original, 0o600)
    if backup.read_bytes() != original:
        raise ValueError('backup_verification_failed')
    try:
        for path, content in changes.items():
            replace_file(path, content, 0o644 if path.suffix in ('.service', '.timer') else 0o600)
        run(['systemctl', 'daemon-reload'], check=True)
        run(['systemctl', 'restart', 'erp-point-decision.service'], check=True)
        run(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'], check=True)
    except Exception:
        run(['systemctl', 'stop', 'erp-telegram-release.timer'], check=False)
        restore_files(before, changes)
        run(['systemctl', 'daemon-reload'], check=True)
        run(['systemctl', 'restart', 'erp-point-decision.service'], check=True)
        run(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'], check=True)
        for name, states in prior.items():
            run(['systemctl', 'enable' if states['enabled'] else 'disable', name], check=True)
            run(['systemctl', 'start' if states['active'] else 'stop', name], check=True)
        raise
    try:
        run(['systemctl', 'enable', '--now', 'erp-telegram-release.timer'], check=True)
        run(['systemctl', 'disable', '--now', 'erp-mobile-approval.timer'], check=True)
    except Exception:
        # Activation may already have dispatched a worker: keep compatible code.
        run(['systemctl', 'stop', 'erp-telegram-release.timer'], check=True)
        raise
    print('Installed release hook; Point baseline backup verified. No resource cleanup performed.')


def refresh(source, nonce, target=TARGET, units=Path('/etc/systemd/system'), run=subprocess.run,
            state=Path('/var/lib/erp-point-decision/releases'), deploy_lock=Path('/run/lock/erp-deploy.lock'),
            activate_supervised=False, reconcile_uncertain=()):
    """Exact published approval refresh; activation is a separate explicit step."""
    modules, unit_files = reviewed_files(source)
    reconcile_uncertain = tuple(reconcile_uncertain)
    if (len(set(reconcile_uncertain)) != len(reconcile_uncertain)
            or any(not isinstance(value, str) or re.fullmatch('[0-9a-f]{24}', value) is None
                   or value == nonce for value in reconcile_uncertain)):
        raise ValueError('invalid_explicit_reconciliation_nonces')
    from erp_telegram_release import Ledger
    ledger = Ledger(state)
    with ledger.lock(worker=True):
        descriptor = os.open(deploy_lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'r+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _refresh_locked(source, nonce, target, units, run, ledger,
                            modules, unit_files, activate_supervised, reconcile_uncertain)
    print('Approved supervised modules refreshed; shared consumer and history preserved.')


def _refresh_locked(source, nonce, target, units, run, ledger, modules, unit_files, activate, reconcile):
    app = source.parents[1]
    if run(['git', '-C', str(app), 'status', '--porcelain'], check=True, capture_output=True, text=True).stdout.strip():
        raise ValueError('dirty_production_checkout')
    head = run(['git', '-C', str(app), 'rev-parse', 'HEAD'], check=True, capture_output=True, text=True).stdout.strip()
    row = ledger.read(nonce)
    if row['state'] != 'published' or row['decision'] != 'approved' or row['release_sha'] != head:
        raise ValueError('exact_published_approval_required')
    consumer = target / 'decidir_corte_point.py'
    original = read_regular(consumer)
    if b'from erp_telegram_release import handle' not in original or b'from erp_telegram_cleanup import handle' not in original:
        raise ValueError('existing_hooks_required')
    changes = {**{target / name: content for name, content in modules.items()},
               **{units / name: content for name, content in unit_files.items()}}
    check_policy(source)
    run(['systemd-analyze', 'verify', *[str(source / name) for name in UNITS]], check=True)
    ensure_publisher_inactive(run, 'refresh')
    timer = 'erp-telegram-release.timer'
    active = run(['systemctl', 'is-active', '--quiet', timer], check=False).returncode == 0
    run(['systemctl', 'stop', timer], check=True)
    ensure_publisher_inactive(run, 'refresh')
    before = {path: read_regular(path) if path.exists() else None for path in changes}
    backup = target / 'module-backups'
    backup.mkdir(mode=0o700, exist_ok=True)
    info = backup.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError('unsafe_backup_directory')
    for path, content in before.items():
        if content is None:
            continue
        saved = backup / (path.name + '.' + hashlib.sha256(content).hexdigest())
        if not saved.exists():
            replace_file(saved, content, 0o600)
        if read_regular(saved) != content:
            raise ValueError('backup_mismatch')
    try:
        for path, content in changes.items():
            replace_file(path, content, 0o644 if path.suffix in ('.service', '.timer') else 0o600)
        if read_regular(consumer) != original:
            raise ValueError('consumer_changed')
        run(['systemctl', 'daemon-reload'], check=True)
        run(['systemd-analyze', 'verify', *[str(units / name) for name in UNITS]], check=True)
        run(['systemctl', 'restart', 'erp-point-decision.service'], check=True)
        run(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'], check=True)
        if read_regular(consumer) != original:
            raise ValueError('consumer_changed')
    except Exception:
        # Restore only our files, retain new diagnostic files, keep publication
        # paused if rollback/receiver health cannot itself be established.
        restore_files(before, changes)
        run(['systemctl', 'daemon-reload'], check=True)
        run(['systemctl', 'restart', 'erp-point-decision.service'], check=True)
        run(['systemctl', 'is-active', '--quiet', 'erp-point-decision.service'], check=True)
        if active:
            run(['systemctl', 'start', timer], check=True)
        raise
    # Once activation is attempted, never roll back to an unsupervised publisher.
    # A start may have taken effect even if the CLI reports failure: stop the
    # timer, preserve compatible new code, and leave any worker alone.
    try:
        reconcile_legacy(source, reconcile, nonce, ledger)
        if activate:
            run(['systemctl', 'enable', '--now', timer], check=True)
        elif active:
            run(['systemctl', 'start', timer], check=True)
    except Exception:
        run(['systemctl', 'stop', timer], check=True)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-nonce', help='Refresh existing hooks from this exact published approval')
    parser.add_argument('--activate-supervised', action='store_true',
                        help='Activate timer after an exact published approval refresh')
    parser.add_argument('--reconcile-uncertain', action='append', default=[], metavar='NONCE',
                        help='Explicitly certify this historical uncertain row; repeat for each exact nonce')
    args = parser.parse_args()
    if args.activate_supervised and not args.release_nonce:
        parser.error('--activate-supervised requires --release-nonce')
    if args.reconcile_uncertain and not args.release_nonce:
        parser.error('--reconcile-uncertain requires --release-nonce')
    if os.geteuid() != 0:
        raise ValueError('root required')
    source = Path(__file__).resolve().parent
    if source != Path('/opt/pastelerias-erp/ops/erp-telegram-release'):
        raise ValueError('install only from official production Git checkout')
    if subprocess.check_output(['git', '-C', '/opt/pastelerias-erp', 'status', '--porcelain']).strip():
        raise ValueError('dirty production checkout')
    if args.release_nonce:
        refresh(source, args.release_nonce, activate_supervised=args.activate_supervised,
                reconcile_uncertain=args.reconcile_uncertain)
    else:
        install(source)


if __name__ == '__main__':
    main()
