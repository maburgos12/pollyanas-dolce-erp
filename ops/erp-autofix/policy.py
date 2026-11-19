"""Fail-closed pure policies. The trusted dispatcher owns the exclusive flock."""
import copy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile

REPOSITORY = 'maburgos12/pollyanas-dolce-erp'
SHA = re.compile(r'[0-9a-f]{40}')
CAUSE = re.compile(r'[0-9a-f]{64}')
STATUSES = {'running', 'failed', 'awaiting_approval', 'blocked', 'completed'}
PROTECTED = re.compile(r'auth|permission|middleware|model|migration|setting|deploy|backup|'
                       r'service.?worker|(^|[/._-])sw([/._-]|$)|requirement|agents|claude', re.I)


def approval(pr, comments, sha, actor_id=260828669):
    """Check current REST evidence/edits; approve using a new comment, never a quote.

    Caller must fetch all current comments and independently gate current CI.
    """
    def result(approved, reason):
        return {'approved': approved, 'reason': reason, 'sha': sha}
    if not isinstance(sha, str) or not SHA.fullmatch(sha):
        return result(False, 'invalid_sha')
    try:
        if (pr['state'] != 'open' or pr['base']['ref'] != 'main' or
                pr['head']['sha'] != sha or pr['head']['repo']['full_name'] != REPOSITORY):
            return result(False, 'pr_mismatch')
        decisions = []
        for comment in comments:
            user = comment.get('user', {})
            if user.get('id') != actor_id or user.get('type') != 'User':
                continue
            body = comment.get('body')
            if body not in ('APRUEBO PUBLICAR ' + sha, 'REVOCO PUBLICAR ' + sha):
                continue
            number = comment.get('id')
            if type(number) is not int or number <= 0:
                return result(False, 'invalid_comment_id')
            updated = None
            if 'updated_at' in comment:
                try:
                    updated = datetime.fromisoformat(comment['updated_at'].replace('Z', '+00:00'))
                    if updated.utcoffset() is None:
                        raise ValueError('naive timestamp')
                except (ValueError, TypeError, AttributeError):
                    return result(False, 'invalid_comment_timestamp')
            decisions.append((number, body, updated))
        if not decisions:
            return result(False, 'no_exact_approval')
        if len({item[0] for item in decisions}) != len(decisions):
            return result(False, 'duplicate_comment_id')
        if any(item[2] is not None for item in decisions):
            if any(item[2] is None for item in decisions):
                return result(False, 'incomplete_comment_timestamps')
            latest = max(decisions, key=lambda item: (item[2], item[1].startswith('REVOCO'), item[0]))
        else:
            # shortcut: ID ordering is for timestamp-free fixtures; REST must retain updated_at.
            latest = max(decisions, key=lambda item: item[0])
        approved = latest[1] == 'APRUEBO PUBLICAR ' + sha
        return result(approved, 'exact_approval' if approved else 'revoked')
    except (KeyError, TypeError, AttributeError):
        return result(False, 'invalid_evidence')


def validate_paths(paths):
    """Return a narrow lexical allowlist; the dispatcher separately rejects symlinks."""
    if (not isinstance(paths, list) or not 1 <= len(paths) <= 12 or
            any(not isinstance(path, str) for path in paths) or len(set(paths)) != len(paths)):
        raise ValueError('expected 1..12 distinct paths')
    for path in paths:
        if not isinstance(path, str) or not path or '\\' in path or any(ord(c) < 32 for c in path):
            raise ValueError('invalid path')
        parts = path.split('/')
        if any(not part or part.startswith('.') for part in parts) or str(PurePosixPath(path)) != path:
            raise ValueError('path must be normal relative POSIX')
        if PROTECTED.search(path):
            raise ValueError('protected path: ' + path)
        allowed = (len(parts) == 3 and parts[:2] in (['scripts', 'tests'], ['ops', 'erp-observer'])
                   and path.endswith('.py'))
        allowed |= path == 'core/email_rendering.py'
        allowed |= (parts[0] == 'core' and path.endswith('.py') and
                    (parts[1] in {'views', 'utils', 'templatetags'} and len(parts) >= 3 or
                     len(parts) == 2 and parts[1] == 'utils.py'))
        if not allowed:
            raise ValueError('outside automatic scope: ' + path)
    return list(paths)


def cause_key(observation):
    """Only class and failing check identities affect the lifetime retry budget."""
    if (not isinstance(observation, dict) or not isinstance(observation.get('kind'), str) or
            observation['kind'] not in {
            'doctor_fail', 'execution_failed', 'http_unavailable', 'synthetic'}):
        raise ValueError('invalid observation kind')
    checks = observation.get('checks')
    if not isinstance(checks, list):
        raise ValueError('invalid observation checks')
    failures = set()
    for check in checks:
        if (not isinstance(check, dict) or not isinstance(check.get('name'), str) or
                not check['name'] or not isinstance(check.get('status'), str) or
                check['status'] not in {'FAIL', 'WARN', 'OK'}):
            raise ValueError('invalid check')
        if check['status'] != 'OK':
            failures.add((check['name'], check['status']))
    canonical = json.dumps([observation['kind'], sorted(failures)], ensure_ascii=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate JSON key')
        value[key] = item
    return value


class Ledger:
    """Atomic durable state; unknown/corrupt state stops work rather than resetting it."""
    def __init__(self, path):
        self.path = Path(path)

    @staticmethod
    def _validate(data):
        if (not isinstance(data, dict) or type(data.get('schema')) is not int or data['schema'] != 1 or
                not isinstance(data.get('causes'), dict) or len(data['causes']) > 256):
            raise ValueError('invalid ledger schema/capacity')
        for cause, entry in data['causes'].items():
            if not isinstance(cause, str) or not CAUSE.fullmatch(cause) or not isinstance(entry, dict):
                raise ValueError('invalid ledger cause')
            records = entry.get('history')
            if not isinstance(records, list):
                raise ValueError('invalid ledger history')
            for record in [*records, entry]:
                if not isinstance(record, dict):
                    raise ValueError('invalid ledger record')
                attempt = record.get('attempts')
                if (type(attempt) is not int or not 1 <= attempt <= 3 or
                        not isinstance(record.get('status'), str) or record['status'] not in STATUSES or
                        record.get('job_id') != f'autofix-{cause[:16]}-{attempt}' or
                        not isinstance(record.get('revision'), str) or not SHA.fullmatch(record['revision']) or
                        not isinstance(record.get('source_id'), str) or not record['source_id']):
                    raise ValueError('invalid ledger record')
            if any(record['attempts'] > entry['attempts'] for record in records):
                raise ValueError('invalid ledger attempt history')
        return data

    def read(self):
        if self.path.is_symlink():
            raise ValueError('ledger symlink refused')
        try:
            with self.path.open(encoding='utf-8') as stream:
                data = json.load(stream, object_pairs_hook=_unique_object)
        except FileNotFoundError:
            return {'schema': 1, 'causes': {}}
        except (json.JSONDecodeError, UnicodeError) as error:
            raise ValueError('corrupt ledger') from error
        return self._validate(data)

    def _write(self, data):
        self._validate(data)
        content = json.dumps(data, sort_keys=True, ensure_ascii=True, allow_nan=False) + '\n'
        descriptor, temporary = tempfile.mkstemp(prefix='.ledger-', dir=self.path.parent)
        try:
            with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    @staticmethod
    def _snapshot(entry):
        return copy.deepcopy({key: value for key, value in entry.items() if key != 'history'})

    def claim(self, cause, revision, source_id):
        if (not isinstance(cause, str) or not CAUSE.fullmatch(cause) or
                not isinstance(revision, str) or not SHA.fullmatch(revision) or
                not isinstance(source_id, str) or not source_id):
            raise ValueError('invalid claim identity')
        data = self.read()
        previous = data['causes'].get(cause)
        if previous and (previous['status'] != 'failed' or previous['attempts'] >= 3):
            return None
        if previous is None and len(data['causes']) >= 256:
            raise ValueError('ledger capacity exhausted')
        attempt = previous['attempts'] + 1 if previous else 1
        history = previous['history'] + [self._snapshot(previous)] if previous else []
        entry = dict(attempts=attempt, status='running', job_id=f'autofix-{cause[:16]}-{attempt}',
                     revision=revision, source_id=source_id, history=history)
        data['causes'][cause] = entry
        self._write(data)
        return copy.deepcopy(entry)

    def update(self, cause, **fields):
        if not fields or set(fields) - {'status', 'phase', 'artifact', 'pr_url', 'candidate_sha', 'reason'}:
            raise ValueError('unsupported ledger update')
        data = self.read()
        if cause not in data['causes']:
            raise ValueError('unclaimed cause')
        entry = data['causes'][cause]
        entry['history'].append(self._snapshot(entry))
        entry.update(fields)
        self._write(data)
        return copy.deepcopy(entry)
