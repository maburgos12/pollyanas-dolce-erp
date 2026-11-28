"""Explicit operator resolution; no historical publication is replayed.

The caller MUST hold the existing publisher-worker and deployment locks and run
as their owning operator for resolve_legacy, which only writes a certificate;
worker code may call resolved to check one, never to create one. Uncertain rows
are not changed. A current canonical
published row must additionally have a fresh verified runtime receipt when the
operator creates a certificate; a Git-only claim is insufficient.
supersede_before_publish instead acquires these locks itself and may change only
an approved row's state to blocked after exact no-effects evidence and a durable
journal. Its decision, bindings and notification history are preserved.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import time

import erp_telegram_release as canonical

LIMIT = 65536
FIELDS = {'schema', 'nonce', 'binding_sha256', 'current_nonce', 'current_release', 'runtime_receipt', 'runtime_receipt_sha256', 'outcome', 'epoch'}


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def encoded(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    require(len(raw) <= LIMIT, 'resolution_size')
    return raw


def strict_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate_resolution_key')
            result[key] = value
        return result
    def finite(value):
        raise ValueError('nonfinite_resolution')
    return json.loads(raw, object_pairs_hook=unique, parse_constant=finite)


def binding(row):
    canonical.validate(row)
    return hashlib.sha256(encoded({k: v for k, v in row.items()
                                  if k not in {'notification_id', 'receipt_id'}})).hexdigest()


def private(info, directory=False):
    require((stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
            and info.st_uid == os.geteuid()
            and stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600)
            and (directory or info.st_nlink == 1), 'unsafe_resolution_storage')


@contextmanager
def certificate_directory(ledger, *, create=False):
    """Traverse without symlinks, using directory descriptors for every write."""
    path = Path(ledger.directory)
    require(path.is_absolute() and '..' not in path.parts, 'resolution_directory_path')
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open('/', flags)
    try:
        for part in path.parts[1:]:
            following = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = following
        private(os.fstat(fd), directory=True)
        for part in ('supervision', 'resolutions'):
            if create:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
            following = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = following
            private(os.fstat(fd), directory=True)
        yield fd
    finally:
        os.close(fd)


def read_certificate(nonce, ledger):
    require(isinstance(nonce, str) and canonical.NONCE.fullmatch(nonce), 'resolution_nonce')
    with certificate_directory(ledger) as directory:
        fd = os.open(nonce + '.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, 'rb') as stream:
            private(os.fstat(stream.fileno()))
            raw = stream.read(LIMIT + 1)
            require(len(raw) <= LIMIT, 'resolution_size')
    value = strict_json(raw)
    require(isinstance(value, dict) and set(value) == FIELDS and type(value['schema']) is int
            and value['schema'] == 1, 'resolution_schema')
    require(value['nonce'] == nonce and isinstance(value['binding_sha256'], str)
            and len(value['binding_sha256']) == 64
            and set(value['binding_sha256']) <= set('0123456789abcdef'), 'resolution_binding')
    require(isinstance(value['current_nonce'], str) and canonical.NONCE.fullmatch(value['current_nonce'])
            and value['current_nonce'] != nonce, 'resolution_current_nonce')
    require(isinstance(value['current_release'], str) and canonical.SHA.fullmatch(value['current_release']),
            'resolution_current_release')
    require(isinstance(value['runtime_receipt_sha256'], str)
            and len(value['runtime_receipt_sha256']) == 64
            and set(value['runtime_receipt_sha256']) <= set('0123456789abcdef'), 'resolution_runtime_receipt')
    receipt = value['runtime_receipt']
    require(isinstance(receipt, dict) and receipt.get('verified') is True
            and receipt.get('release') == value['current_release']
            and hashlib.sha256(encoded(receipt)).hexdigest() == value['runtime_receipt_sha256'],
            'resolution_runtime_receipt_changed')
    require(value['outcome'] in {'no_merge_verified', 'superseded_verified'}
            and type(value['epoch']) is int and value['epoch'] > 0, 'resolution_outcome')
    return value


def current_publication(nonce, ledger):
    row = ledger.read(nonce)
    canonical.validate(row)
    require(row['state'] == 'published' and row['decision'] == 'approved'
            and isinstance(row['release_sha'], str) and canonical.SHA.fullmatch(row['release_sha']),
            'supervised_current_publication_required')
    return row


def ancestor(api, older, newer):
    require(isinstance(older, str) and canonical.SHA.fullmatch(older)
            and isinstance(newer, str) and canonical.SHA.fullmatch(newer), 'resolution_revision')
    compared = api('GET', canonical.PREFIX + '/compare/' + older + '...' + newer)
    require(compared.get('status') in {'ahead', 'identical'}
            and type(compared.get('behind_by')) is int and compared['behind_by'] == 0
            and type(compared.get('ahead_by')) is int
            and compared['ahead_by'] >= 0
            and compared['base_commit']['sha'] == older
            and compared['merge_base_commit']['sha'] == older, 'resolution_not_ancestor')
    require((compared['status'] == 'identical' and older == newer and compared['ahead_by'] == 0)
            or (compared['status'] == 'ahead' and older != newer and compared['ahead_by'] > 0),
            'resolution_compare_inconsistent')


def resolve_legacy(nonce, current_nonce, api, ledger, *, runtime_verify, production=canonical.production_sha):
    """Operator-only, under worker+deploy locks; no mutation of canonical rows."""
    require(nonce != current_nonce, 'distinct_current_publication_required')
    old = ledger.read(nonce)
    require(old['state'] == 'uncertain', 'only_uncertain_can_be_resolved')
    old_binding = binding(old)
    current = current_publication(current_nonce, ledger)
    release = current['release_sha']
    require(production() == release and api('GET', canonical.PREFIX + '/git/ref/heads/main')['object']['sha'] == release,
            'current_publication_not_live_main')
    pr = api('GET', canonical.PREFIX + '/pulls/' + str(old['pr']))
    require(pr['head']['sha'] == old['head_sha'] and pr['base']['sha'] == old['base_sha'],
            'historical_pr_binding_changed')
    require(pr['head']['repo']['full_name'] == canonical.REPO
            and pr['base']['repo']['full_name'] == canonical.REPO, 'historical_repository_changed')
    if old['release_sha'] is None:
        require(pr['merged'] is False, 'historical_merge_unresolved')
        # GitHub can expose a synthetic test-merge SHA on an unmerged PR.
        # The old canonical release is None; no completed merge is inferred.
        outcome = 'no_merge_verified'
    else:
        require(pr['merged'] is True and pr['merge_commit_sha'] == old['release_sha'], 'historical_merge_changed')
        commit = api('GET', canonical.PREFIX + '/git/commits/' + old['release_sha'])
        candidate = api('GET', canonical.PREFIX + '/git/commits/' + old['head_sha'])
        require([p['sha'] for p in commit['parents']] == [old['base_sha'], old['head_sha']]
                and commit['tree']['sha'] == candidate['tree']['sha'], 'historical_merge_shape_changed')
        ancestor(api, old['release_sha'], release)
        outcome = 'superseded_verified'
    runtime_receipt = runtime_verify(release)
    require(isinstance(runtime_receipt, dict) and runtime_receipt.get('verified') is True
            and runtime_receipt.get('release') == release, 'current_runtime_unverified')
    runtime_digest = hashlib.sha256(encoded(runtime_receipt)).hexdigest()
    # Caller locks prevent compliant writers; these checks also detect observed
    # drift during the read-only API round trip before materializing permission.
    require(binding(ledger.read(nonce)) == old_binding, 'historical_row_changed')
    require(current_publication(current_nonce, ledger)['release_sha'] == release
            and production() == release
            and api('GET', canonical.PREFIX + '/git/ref/heads/main')['object']['sha'] == release,
            'current_publication_changed')
    certificate = {'schema': 1, 'nonce': nonce, 'binding_sha256': old_binding,
                   'current_nonce': current_nonce, 'current_release': release,
                   'runtime_receipt': runtime_receipt, 'runtime_receipt_sha256': runtime_digest, 'outcome': outcome, 'epoch': int(time.time())}
    raw = encoded(certificate)
    with certificate_directory(ledger, create=True) as directory:
        fd = os.open(nonce + '.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(directory)
    return certificate


def resolved(row, api, ledger):
    """Fail closed; only an explicit certificate exempts a legacy global guard.

This does not permit publishing the old row. No certificate is inferred from
elapsed time, merge metadata alone or filesystem HEAD.
"""
    try:
        require(row['state'] == 'uncertain', 'only_uncertain_can_be_resolved')
        current_old = ledger.read(row['nonce'])
        require(binding(current_old) == binding(row), 'historical_row_changed')
        certificate = read_certificate(row['nonce'], ledger)
        require(certificate['binding_sha256'] == binding(row), 'historical_binding_changed')
        require(certificate['outcome'] == ('no_merge_verified' if row['release_sha'] is None else 'superseded_verified'),
                'historical_outcome_changed')
        current = current_publication(certificate['current_nonce'], ledger)
        require(current['release_sha'] == certificate['current_release'], 'current_publication_changed')
        live = canonical.production_sha()
        require(api('GET', canonical.PREFIX + '/git/ref/heads/main')['object']['sha'] == live,
                'live_main_changed')
        require(any(candidate['state'] == 'published' and candidate['decision'] == 'approved'
                    and candidate['release_sha'] == live
                    for candidate in (ledger.read(path.stem) for path in ledger.directory.glob('*.json'))),
                'live_canonical_publication_required')
        ancestor(api, certificate['current_release'], live)
        return True
    except (ValueError, TypeError, KeyError, OSError, RuntimeError):
        return False


def _private_bytes(path):
    """Bounded private evidence read, refusing symlinks in every component."""
    path = Path(path)
    require(path.is_absolute() and '..' not in path.parts, 'evidence_path')
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory = os.open('/', flags)
    try:
        for part in path.parts[1:-1]:
            following = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = following
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, 'rb') as stream:
            private(os.fstat(stream.fileno()))
            raw = stream.read(LIMIT + 1)
            require(len(raw) <= LIMIT, 'evidence_size')
            return raw
    finally:
        os.close(directory)


def supersede_before_publish(nonce, new_head, new_base, api, ledger, *,
                             expected_row_sha256, abort_receipt, abort_sha256, journal,
                             service_inactive, runtime_verify, production=canonical.production_sha,
                             deploy_lock=Path('/run/lock/erp-deploy.lock')):
    """Explicit operator-only invalidation; no merge, deploy, decision or replay.

    expected_row_sha256 binds RAW ledger bytes, not canonicalized JSON. The Root
    receipt must bind the audited source and reaped transport before publish.
    service_inactive returns service/timer/main_pid evidence. A fresh exclusive
    Journal records intent before the sole state change and completion after it.
    Any interrupted attempt is for operator review, never automatically resumed.
    """
    from erp_release_supervisor import Journal
    from erp_release_worker import lease, leases_intact

    def digest(raw):
        return hashlib.sha256(raw).hexdigest()

    def sha256(value):
        return isinstance(value, str) and len(value) == 64 and set(value) <= set('0123456789abcdef')

    require(isinstance(nonce, str) and canonical.NONCE.fullmatch(nonce), 'supersession_nonce')
    require(all(isinstance(v, str) and canonical.SHA.fullmatch(v) for v in (new_head, new_base)),
            'supersession_revision')
    require(sha256(expected_row_sha256) and sha256(abort_sha256), 'supersession_digest')
    deploy_lock = Path(deploy_lock)
    worker_path = ledger.directory / '.worker.lock'
    with lease(deploy_lock) as deploy_fd, lease(worker_path) as worker_fd:
        held = [(deploy_lock, deploy_fd), (worker_path, worker_fd)]
        audit = Journal(journal)  # Existing path always refuses replay.
        stage = 'precheck'
        try:
            with ledger.lock():
                before = ledger.read(nonce)
                before_raw = _private_bytes(ledger.path(nonce))
                require(strict_json(before_raw) == before and digest(before_raw) == expected_row_sha256,
                        'supersession_row_changed')
            require(before['state'] == 'approved' and before['decision'] == 'approved'
                    and before['release_sha'] is None, 'only_unexecuted_approval_can_be_superseded')
            require(new_head != before['head_sha'], 'supersession_requires_new_head')
            raw = _private_bytes(abort_receipt)
            require(digest(raw) == abort_sha256, 'supersession_abort_digest')
            aborted = strict_json(raw)
            require(isinstance(aborted, dict) and type(aborted.get('schema')) is int
                    and aborted['schema'] == 1, 'supersession_abort_schema')
            require(all(aborted.get(k) == before[k] for k in
                        ('nonce', 'pr', 'head_sha', 'base_sha', 'production_sha'))
                    and aborted.get('row_sha256') == expected_row_sha256, 'supersession_abort_binding')
            effects = aborted.get('effects')
            evidence = aborted.get('evidence_sha256')
            require(aborted.get('phase') == 'before_publish' and aborted.get('publish_entered') is False
                    and aborted.get('reaped') is True and isinstance(effects, dict)
                    and set(effects) == {'ledger', 'merge', 'stop', 'official'}
                    and all(v is False for v in effects.values()), 'supersession_noeffects_unproved')
            require(sha256(aborted.get('operator_sha256')) and isinstance(evidence, dict)
                    and {'source', 'reap'} <= set(evidence) and all(sha256(v) for v in evidence.values())
                    and type(aborted.get('epoch')) is int
                    and before['decision_at'] <= aborted['epoch'] <= int(time.time()),
                    'supersession_abort_provenance')

            def check_services():
                proof = service_inactive()
                require(isinstance(proof, dict) and proof.get('service') in {'inactive', 'failed'}
                        and proof.get('timer') == 'inactive' and type(proof.get('main_pid')) is int
                        and proof['main_pid'] == 0, 'supersession_publisher_not_quiescent')
                return strict_json(encoded(proof))

            def check_successor():
                require(production() == new_base
                        and api('GET', canonical.PREFIX + '/git/ref/heads/main')['object']['sha'] == new_base,
                        'supersession_live_base_changed')
                pr = api('GET', canonical.PREFIX + '/pulls/' + str(before['pr']))
                require(pr['number'] == before['pr'] and pr['state'] == 'open' and pr['merged'] is False
                        and pr['head']['sha'] == new_head and pr['base']['sha'] == new_base
                        and pr['head']['repo']['full_name'] == canonical.REPO
                        and pr['base']['repo']['full_name'] == canonical.REPO,
                        'supersession_successor_changed')

            services_before = check_services()
            check_successor()
            runtime = runtime_verify(new_base)
            require(isinstance(runtime, dict) and runtime.get('verified') is True
                    and runtime.get('release') == new_base, 'supersession_runtime_unverified')
            runtime = strict_json(encoded(runtime))
            after = dict(before, state='blocked')
            canonical.validate(after)
            with ledger.lock():
                check_successor()
                services_after = check_services()
                require(leases_intact(held), 'supersession_lease_changed')
                require(digest(_private_bytes(abort_receipt)) == abort_sha256, 'supersession_abort_changed')
                require(digest(_private_bytes(ledger.path(nonce))) == expected_row_sha256,
                        'supersession_row_changed')
                audit.append('SUPERSEDE_INTENT', before=before, after=after,
                             before_raw_sha256=expected_row_sha256, abort_receipt=aborted,
                             abort_raw_sha256=abort_sha256, new_head=new_head, new_base=new_base,
                             runtime_receipt=runtime, runtime_receipt_sha256=digest(encoded(runtime)),
                             services_before=services_before, services_after=services_after)
                stage = 'state_write'
                require(digest(_private_bytes(ledger.path(nonce))) == expected_row_sha256,
                        'supersession_row_changed')
                ledger.write(after)  # The only operational mutation: decision/history stay intact.
                require(ledger.read(nonce) == after, 'supersession_write_unconfirmed')
                audit.append('SUPERSEDED', nonce=nonce, after_raw_sha256=digest(_private_bytes(ledger.path(nonce))))
            return after
        except BaseException:
            audit.append('HOLD', stage=stage, code='supersession_requires_operator_review')
            raise
        finally:
            audit.close()
