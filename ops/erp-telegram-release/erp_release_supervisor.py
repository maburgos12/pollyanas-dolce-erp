"""Single-use durable supervisor. Transport and effectful operations are injected.

An existing attempt is evidence, never a resumable program. After a merge every
failure requires human reconciliation; this module never restarts or restores.
"""
import json
import os
from pathlib import Path
import re

import erp_release_preservation as preservation

LIMIT = 8 * 1024 * 1024
WORKERS = ('worker', 'worker_notificaciones', 'worker_sat', 'worker_recetas',
           'worker_logistica_point', 'worker_hours_control', 'worker_hours_publish')


class SupervisorError(RuntimeError):
    """Only fixed codes are exposed; exception messages may contain credentials."""


def require(condition, code):
    if not condition:
        raise SupervisorError(code)


def encoded(value):
    data = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    require(len(data) <= LIMIT, 'journal_limit')
    return data


def clone(value):
    return json.loads(encoded(value))


class Journal:
    """Exclusive attempt directory, append-only durable numbered events."""
    def __init__(self, path):
        path = Path(path)
        require(path.is_absolute() and '..' not in path.parts and path.name not in ('', '.'), 'journal_path')
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        self.fd = None
        parent = os.open('/', flags)
        try:
            for part in path.parts[1:-1]:
                child = os.open(part, flags, dir_fd=parent)
                os.close(parent)
                parent = child
            os.mkdir(path.name, 0o700, dir_fd=parent)  # EEXIST deliberately rejects replay.
            self.fd = os.open(path.name, flags, dir_fd=parent)
            os.fchmod(self.fd, 0o700)
            os.fsync(self.fd)
            os.fsync(parent)
        except BaseException:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            raise
        finally:
            os.close(parent)
        self.sequence = 0

    def append(self, state, **details):
        data = encoded({'state': state, **details})
        name = f'{self.sequence:04d}-{state}.json'
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=self.fd)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'wb', closefd=False) as stream:
                stream.write(data)
                stream.flush()
                os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(self.fd)
        self.sequence += 1

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class Supervisor:
    def __init__(self, adapter, journal):
        self.adapter = adapter
        self.journal = Journal(journal)
        self.state = 'NEW'
        self.pins = None
        self.binding = None
        self.retryable = False

    def close(self):
        self.journal.close()

    def _record(self, state, **details):
        self.journal.append(state, **details)
        self.state = state

    def _fail(self, merged, stage):
        # Never persist arbitrary exception text, payloads, tokens or command lines.
        self.state = 'HOLD' if merged else 'REJECTED'
        self.journal.append(self.state, code='operation_failed', stage=stage)

    def preflight(self, row):
        require(self.state == 'NEW', 'attempt_already_used')
        stage = 'binding'
        try:
            require(re.fullmatch(r'[0-9a-f]{24}', row.get('nonce', '')) is not None, 'nonce')
            for key in ('head_sha', 'base_sha', 'production_sha'):
                require(re.fullmatch(r'[0-9a-f]{40}', row.get(key, '')) is not None, 'revision')
            require(row['base_sha'] == row['production_sha'], 'production_base')
            self.binding = {key: row[key] for key in ('nonce', 'head_sha', 'base_sha', 'production_sha')}
            stage = 'preflight'
            self.pins = clone(self.adapter.preflight(clone(row)))
            require(self.pins.get('base') == row['base_sha'], 'pins_base')
            roles = self.pins.get('roles', [])
            require(len(roles) == 7 and set(roles) == set(WORKERS), 'worker_coverage')
            stage = 'precheck_snapshot'
            cut = clone(self.adapter.snapshot(clone(self.pins)))
            preservation.validate_before(cut)
            self.journal.append('PRECHECK_CAPTURE', binding=self.binding, pins=self.pins, cut=cut)
            stage = 'idle'
            self.adapter.idle(clone(self.pins), clone(cut))
            self.adapter.verify_pins(clone(self.pins), stopped=())
            self._record('PRECHECK', binding=self.binding, pins=self.pins)
            return clone(self.pins)
        except BaseException as exc:
            # Only known, pre-effect contention may be reconsidered automatically.
            self.retryable = str(exc) in {'worker_busy', 'point_lease_busy'}
            self._fail(False, stage)
            raise SupervisorError('precheck_rejected') from None

    def __call__(self, release, base):
        require(self.state == 'PRECHECK', 'attempt_not_ready')
        # Canonical publisher owns merge parent/tree/head checks. Here the callback
        # binds its generated merge SHA to the exact prechecked base, once only.
        stage = 'merge_binding'
        try:
            self._record('MERGED', release=release, base=base, binding=self.binding)
            require(isinstance(release, str) and re.fullmatch(r'[0-9a-f]{40}', release) is not None, 'release')
            require(base == self.binding['base_sha'] == self.pins['base'] and release != base, 'base')
            stage = 'before_stop'
            before = clone(self.adapter.snapshot(clone(self.pins)))
            preservation.validate_before(before)
            self.journal.append('BEFORE_STOP', cut=before)
            self.adapter.idle(clone(self.pins), clone(before))
            self.adapter.verify_pins(clone(self.pins), stopped=())
            self._record('STOP_INTENT', roles=WORKERS)
            stage = 'stop'
            self.adapter.stop(clone(self.pins), WORKERS)
            self.adapter.verify_pins(clone(self.pins), stopped=WORKERS)
            stage = 'preservation'
            after = clone(self.adapter.snapshot(clone(self.pins), before=clone(before)))
            self.journal.append('AFTER_STOP', cut=after)
            proof = preservation.validate_restored(before, after, scope='all', all_stopped=True)
            self._record('PRESERVED', proof=proof)
            self._record('OFFICIAL_INTENT', release=release, base=base)
            stage = 'official'
            self.adapter.official(release, base, clone(self.pins))
            stage = 'runtime'
            receipt = clone(self.adapter.verify_runtime(release, clone(self.pins)))
            require(receipt.get('release') == release and receipt.get('consumer_verified') is True, 'runtime_proof')
            stage = 'lease_release'
            lease_receipt = clone(self.adapter.close())
            require(lease_receipt.get('point_transport_reaped') is True
                    and lease_receipt.get('release_acknowledged') is True
                    and lease_receipt.get('exit_code') == 0, 'lease_release_unproved')
            self.journal.append('LEASE_RELEASED', receipt=lease_receipt)
            self._record('RUNTIME_VERIFIED', receipt=receipt)
            return receipt
        except BaseException:
            self._fail(True, stage)
            raise SupervisorError('publication_hold') from None
