"""Root-only Telegram release ledger. No polling, generated commands or automatic retries."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.request import Request, build_opener, ProxyHandler, HTTPRedirectHandler

REPO = 'maburgos12/pollyanas-dolce-erp'
PREFIX = '/repos/' + REPO
STATE = Path('/var/lib/erp-point-decision/releases')
APP = Path('/opt/pastelerias-erp')
SHA = re.compile(r'[0-9a-f]{40}')
NONCE = re.compile(r'[0-9a-f]{24}')
CALLBACK = re.compile(r'd:([ar]):([0-9a-f]{24}):([0-9a-f]{24})')
STATES = {'notifying', 'pending', 'approved', 'rejected', 'executing', 'published', 'blocked', 'uncertain'}
FIELDS = {'repo', 'pr', 'head_sha', 'base_sha', 'production_sha', 'nonce', 'expires',
          'message_id', 'chat_id', 'state', 'decision', 'decision_at', 'release_sha', 'notification_id'}


def validate(row):
    if not isinstance(row, dict) or set(row) not in (FIELDS, FIELDS | {'receipt_id'}) or row['repo'] != REPO:
        raise ValueError('invalid_ledger')
    for key in ('head_sha', 'base_sha', 'production_sha'):
        if not isinstance(row[key], str) or not SHA.fullmatch(row[key]):
            raise ValueError('invalid_sha')
    if row['release_sha'] is not None and not SHA.fullmatch(row['release_sha']):
        raise ValueError('invalid_release')
    if (not isinstance(row['nonce'], str) or not NONCE.fullmatch(row['nonce'])
            or row['state'] not in STATES or row['production_sha'] != row['base_sha']):
        raise ValueError('invalid_ledger')
    if row['decision'] not in {None, 'approved', 'rejected'}:
        raise ValueError('invalid_decision')
    if ((row['state'] in {'pending', 'notifying'} and row['decision'] is not None)
            or (row['state'] in {'approved', 'rejected'} and row['decision'] != row['state'])
            or (row['state'] in {'executing', 'uncertain'} and row['decision'] is None)
            or (row['state'] == 'published' and row['decision'] != 'approved')
            or ((row['decision'] is None) != (row['decision_at'] is None))):
        raise ValueError('inconsistent_decision')
    for key in ('pr', 'expires', 'chat_id'):
        if type(row[key]) is not int or row[key] <= 0:
            raise ValueError('invalid_number')
    for key in ('message_id', 'decision_at', 'notification_id', 'receipt_id'):
        if row.get(key) is not None and (type(row[key]) is not int or row[key] <= 0):
            raise ValueError('invalid_number')
    return row


class Ledger:
    def __init__(self, directory=STATE, validator=validate):
        self.validate = validator
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('unsafe_ledger_directory')

    @contextmanager
    def lock(self, worker=False):
        descriptor = os.open(self.directory / ('.worker.lock' if worker else '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'r+') as file:
            fcntl.flock(file, fcntl.LOCK_EX | (fcntl.LOCK_NB if worker else 0))
            yield

    def path(self, nonce):
        if not isinstance(nonce, str) or not NONCE.fullmatch(nonce):
            raise ValueError('invalid_nonce')
        return self.directory / (nonce + '.json')

    def read(self, nonce):
        path = self.path(nonce)
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('unsafe_ledger_file')
        row = self.validate(json.loads(path.read_text()))
        if row['nonce'] != nonce:
            raise ValueError('nonce_mismatch')
        return row

    def write(self, row):
        self.validate(row)
        path = self.path(row['nonce'])
        temporary = path.with_suffix('.pending')
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'w') as file:
            json.dump(row, file, sort_keys=True)
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(path)
        descriptor = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def signed(config, row, action):
    body = f"d:{action}:{row['nonce']}"
    binding = '|'.join(str(row[k]) for k in ('repo', 'pr', 'head_sha', 'base_sha', 'production_sha', 'expires', 'chat_id'))
    tag = hmac.new(config['token'].encode(), (body + '|' + binding).encode(), hashlib.sha256).hexdigest()[:24]
    return body + ':' + tag


def decide(query, config, ledger, now=None, send=None):
    now = int(time.time()) if now is None else now
    match = CALLBACK.fullmatch(query.get('data', ''))
    if not match:
        raise ValueError('invalid_callback')
    action, nonce, _ = match.groups()
    with ledger.lock():
        row = ledger.read(nonce)
        message = query.get('message', {})
        chat = message.get('chat', {})
        if (chat.get('type') != 'private' or chat.get('id') != config['chat_id']
                or row['chat_id'] != config['chat_id'] or query.get('from', {}).get('id') != config['chat_id']
                or message.get('message_id') != row['message_id'] or row['message_id'] is None
                or not hmac.compare_digest(query['data'], signed(config, row, action))):
            raise ValueError('unauthorized_callback')
        can_decide = row['state'] == 'pending' or (row['state'] == 'approved' and action == 'r')
        if can_decide and now < row['expires']:
            decision = 'approved' if action == 'a' else 'rejected'
            row.update(state=decision, decision=decision, decision_at=now, receipt_id=None)
            ledger.write(row)
        elif row['state'] in {'pending', 'notifying'}:
            raise ValueError('expired')
        # Keep the receipt before the publisher's claim: it must not overwrite a newer state.
        if send is not None and row['state'] in {'approved', 'rejected'} and row.get('receipt_id') is None:
            try:
                result = send(config, 'sendMessage', {'chat_id': row['chat_id'], 'text':
                    f"ERP PR #{row['pr']} · {decision_status(row, now)}\nCommit solicitado: {row['head_sha']}"})
                verify_message(result, row['chat_id'])
                confirmed = dict(row, receipt_id=result['message_id'])
                ledger.write(confirmed)
                row = confirmed
            except Exception:
                # A lost response can duplicate this receipt on another click, never the decision.
                print('Decision recorded; Telegram receipt not confirmed.', flush=True)
        return row


def decision_status(row, now=None):
    now = int(time.time()) if now is None else now
    if row['state'] == 'approved':
        if now >= row['expires']:
            return 'Aprobación registrada, pero vencida. No autoriza una publicación nueva.'
        return 'Aprobación registrada. Publicación pendiente de verificaciones; todavía no confirma despliegue.'
    if row['state'] == 'executing' and row['decision'] == 'rejected':
        return 'Rechazo registrado. Cierre en curso; no se realizará ningún despliegue.'
    if row['state'] == 'rejected' or (row['state'] == 'blocked' and row['decision'] == 'rejected'):
        return 'Rechazo registrado. Esta solicitud no se publicará.'
    return {
        'executing': 'Publicación en curso; resultado aún sin confirmar. No se registró otra autorización.',
        'published': 'Publicado correctamente. No se registró otra autorización.',
        'blocked': 'Solicitud vencida. No se realizó ningún despliegue.',
        'uncertain': 'Publicación sin confirmar. Requiere revisión; no se repetirá automáticamente.',
    }[row['state']]


def handle(query, config, state, telegram):
    """Called only by the existing Point consumer, before its original parser."""
    try:
        row = decide(query, config, Ledger(Path(state) / 'releases'), send=telegram)
        text = f"PR #{row['pr']}: {decision_status(row)}"
        if row['state'] in {'approved', 'rejected'} and row.get('receipt_id') is None:
            text += ' Aviso persistente sin confirmar.'
    except (OSError, ValueError, KeyError, TypeError):
        text = 'Solicitud inválida o vencida. No se autorizó una publicación nueva.'
    try:
        telegram(config, 'answerCallbackQuery', {'callback_query_id': query['id'], 'text': text})
    except Exception:
        print('Telegram callback acknowledgement not confirmed.', flush=True)


class GitHub:
    def __init__(self):
        path = '/opt/erp-autofix/current/ops/erp-autofix/github.py'
        spec = importlib.util.spec_from_file_location('release_github_credentials', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        # This isolated import belongs only to the publisher; the generator on disk is unchanged.
        module.PERMISSIONS = {**module.PERMISSIONS, 'administration': 'read'}
        self.adapter = module.GitHub()

    def api(self, method, endpoint, data=None):
        # All callers supply fixed endpoint templates; no model/log/request URL is accepted.
        if endpoint.startswith(PREFIX + '/branches/main/protection'):
            if method != 'GET' or data is not None or endpoint != PREFIX + '/branches/main/protection':
                raise ValueError('protection_is_read_only')
        elif endpoint == '/graphql':
            if method != 'POST' or not isinstance(data, dict) or not data.get('query', '').startswith('mutation($id:ID!){markPullRequestReadyForReview('):
                raise ValueError('graphql_operation_denied')
        elif not endpoint.startswith(PREFIX + '/'):
            raise ValueError('wrong_repository')
        return self.adapter._request(method, endpoint, data, self.adapter._token())


def pages(api, path, key=None):
    rows = []
    for page in range(1, 21):
        separator = '&' if '?' in path else '?'
        response = api('GET', path + f'{separator}per_page=100&page={page}')
        items = response if key is None else response[key]
        if not isinstance(items, list) or len(items) > 100 or any(not isinstance(i, dict) for i in items):
            raise ValueError('invalid_page')
        rows.extend(items)
        if len(items) < 100:
            if key == 'check_runs' and response['total_count'] != len(rows):
                raise ValueError('incomplete_checks')
            return rows
    raise ValueError('pagination_limit')


def validate_protection(api):
    protection = api('GET', PREFIX + '/branches/main/protection')
    required = protection.get('required_status_checks') or {}
    bypass = (protection.get('required_pull_request_reviews') or {}).get('bypass_pull_request_allowances', {})
    if (required.get('strict') is not True or protection.get('enforce_admins', {}).get('enabled') is not True
            or {'context': 'test', 'app_id': 15368} not in required.get('checks', [])
            or any(bypass.get(kind) for kind in ('apps', 'users', 'teams'))):
        raise ValueError('strict_branch_protection_required')


def eligible(api, number, head=None, base=None):
    validate_protection(api)
    pr = api('GET', PREFIX + f'/pulls/{number}')
    if (pr['state'] != 'open' or pr['base']['ref'] != 'main'
            or pr['head']['repo']['full_name'] != REPO or pr['base']['repo']['full_name'] != REPO
            or pr.get('merged') or (head and pr['head']['sha'] != head) or (base and pr['base']['sha'] != base)):
        raise ValueError('pr_changed')
    head, base = pr['head']['sha'], pr['base']['sha']
    if not SHA.fullmatch(head) or not SHA.fullmatch(base):
        raise ValueError('invalid_sha')
    if api('GET', PREFIX + '/git/ref/heads/main')['object']['sha'] != base:
        raise ValueError('main_changed')
    comparison = api('GET', PREFIX + f'/compare/{base}...{head}')
    if comparison['behind_by'] != 0 or comparison['ahead_by'] < 1 or comparison['merge_base_commit']['sha'] != base:
        raise ValueError('candidate_needs_rebase')
    checks = pages(api, PREFIX + f'/commits/{head}/check-runs?filter=latest', 'check_runs')
    statuses = pages(api, PREFIX + f'/commits/{head}/statuses')
    tests = [c for c in checks if c['name'] == 'test' and c.get('app', {}).get('slug') == 'github-actions']
    if (len(tests) != 1 or tests[0]['conclusion'] != 'success'
            or any(c['head_sha'] != head or c['status'] != 'completed' or c['conclusion'] not in {'success', 'skipped', 'neutral'} for c in checks)):
        raise ValueError('ci_not_green')
    latest = {}
    for status in statuses:  # GitHub returns newest first per context.
        latest.setdefault(status['context'], status['state'])
    if any(value != 'success' for value in latest.values()):
        raise ValueError('statuses_not_green')
    return pr


def production_sha():
    return subprocess.check_output(['/usr/bin/git', '-C', str(APP), 'rev-parse', 'HEAD'], text=True).strip()


def approval_time():
    return int(time.time())


def operational_module(name):
    if name not in {'erp_telegram_cleanup', 'erp_cleanup_prepare', 'erp_point_archive'}:
        raise ValueError('unknown_operational_module')
    source = Path(__file__).with_name(name + '.py')
    if not source.is_file():
        source = APP / 'ops/erp-telegram-release' / (name + '.py')
    if str(source.parent) not in sys.path:
        sys.path.insert(0, str(source.parent))
    spec = importlib.util.spec_from_file_location(name, source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def approval_window(now=None):
    local = datetime.fromtimestamp(approval_time() if now is None else now, ZoneInfo('America/Mazatlan'))
    return 7 <= local.hour < 20, local.date().isoformat()


def validate_intent(row):
    if (set(row) != {'nonce', 'kind', 'binding', 'phase', 'last_day', 'error'}
            or not NONCE.fullmatch(row['nonce']) or row['kind'] not in {'release', 'cleanup'}
            or row['phase'] not in {'waiting', 'sending', 'closed', 'uncertain'}
            or not isinstance(row['binding'], dict)
            or not isinstance(row['last_day'], str) or not isinstance(row['error'], str)):
        raise ValueError('invalid_approval_intent')
    binding = row['binding']
    if row['kind'] == 'release':
        if (set(binding) != {'pr', 'head'} or type(binding['pr']) is not int or binding['pr'] <= 0
                or not isinstance(binding['head'], str) or not SHA.fullmatch(binding['head'])):
            raise ValueError('invalid_release_intent')
    elif (set(binding) != {'digest', 'proposal_id'} or not isinstance(binding['digest'], str)
          or not re.fullmatch('[0-9a-f]{64}', binding['digest'])
          or not isinstance(binding['proposal_id'], str)
          or not re.fullmatch(r'VPS-LIM-[0-9]{8}-[0-9]{2}', binding['proposal_id'])):
        raise ValueError('invalid_cleanup_intent')
    if row['nonce'] != hashlib.sha256(json.dumps([row['kind'], binding], sort_keys=True).encode()).hexdigest()[:24]:
        raise ValueError('intent_identity_changed')
    return row


def schedule_request(kind, binding, ledger, matching, issue, revalidate):
    """A durable intent is not an authorization; only issue() creates signed buttons."""
    queue = Ledger(ledger.directory / 'scheduled', validator=validate_intent)
    key = hashlib.sha256(json.dumps([kind, binding], sort_keys=True).encode()).hexdigest()[:24]
    with queue.lock():
        item = queue.read(key) if queue.path(key).exists() else dict(
            nonce=key, kind=kind, binding=binding, phase='waiting', last_day='', error='')
        if item['binding'] != binding:
            raise ValueError('approval_intent_collision')
        with ledger.lock():
            previous = [ledger.read(p.stem) for p in ledger.directory.glob('*.json')]
        previous = [row for row in previous if matching(row)]
        if any(row.get('decision_at') is not None or row['state'] in {'approved', 'rejected', 'executing', 'published', 'uncertain'} for row in previous):
            item['phase'] = 'closed'
        elif any(row['state'] == 'notifying' for row in previous):
            item['phase'] = 'uncertain'
        elif item['phase'] == 'sending':
            # A crash after claiming delivery cannot prove that Telegram received nothing.
            item['phase'] = 'uncertain'
        queue.write(item)
        deferred = {'state': 'deferred', 'nonce': None, 'message_id': None,
                    'head_sha': binding.get('head'), 'expires': None, 'intent_id': key}
        if item['phase'] != 'waiting':
            raise ValueError('existing_request_requires_resolution')
        now = approval_time()
        allowed, day = approval_window(now)
        live = [row for row in previous if row['state'] == 'pending' and now < row['expires']]
        if live:
            raise ValueError('existing_request_requires_resolution')
        if any(row['state'] == 'pending' and approval_window(row['expires'] - 3600)[1] == day for row in previous):
            return deferred
        if not allowed or item['last_day'] == day:
            return deferred
        # Persist the day's attempt before expensive recovery checks; failures do not busy-loop.
        item.update(last_day=day, error='')
        queue.write(item)
        try:
            revalidate()
        except (ValueError, OSError, RuntimeError, KeyError, subprocess.SubprocessError) as exc:
            item['error'] = type(exc).__name__
            queue.write(item)
            raise
        if not approval_window()[0]:
            return deferred
        # issue() persists the request before Telegram. A failed preflight is not an ambiguous send.
        result = issue()
        item['phase'] = 'waiting'
        queue.write(item)
        return result


def request_release(number, api, ledger, config, telegram, production=production_sha, expected_head=None):
    if type(number) is not int or number <= 0:
        raise ValueError('invalid_pr')
    head = expected_head or api('GET', PREFIX + f'/pulls/{number}')['head']['sha']
    if not isinstance(head, str) or not SHA.fullmatch(head):
        raise ValueError('invalid_sha')
    def check():
        pr = eligible(api, number, head)
        if production() != pr['base']['sha']:
            raise ValueError('production_not_at_base')
    return schedule_request('release', {'pr': number, 'head': head}, ledger,
        lambda row: row['pr'] == number and row['head_sha'] == head,
        lambda: _request_release(number, api, ledger, config, telegram, production, head), check)


def _request_release(number, api, ledger, config, telegram, production=production_sha, expected_head=None):
    if type(number) is not int or number <= 0:
        raise ValueError('invalid_pr')
    pr = eligible(api, number, expected_head)
    base, head = pr['base']['sha'], pr['head']['sha']
    if production() != base:
        raise ValueError('production_not_at_base')
    if not approval_window()[0]:
        raise ValueError('outside_approval_hours')
    row = dict(repo=REPO, pr=number, head_sha=head, base_sha=base, production_sha=base,
               nonce=secrets.token_hex(12), expires=int(time.time()) + 3600, message_id=None,
               chat_id=config['chat_id'], state='notifying', decision=None, decision_at=None, release_sha=None, notification_id=None)
    with ledger.lock():
        # One live request per PR: a new request must not leave older buttons usable.
        for path in ledger.directory.glob('*.json'):
            previous = ledger.read(path.stem)
            if previous['pr'] == number and previous['state'] == 'pending' and int(time.time()) >= previous['expires']:
                previous['state'] = 'blocked'
                ledger.write(previous)
            if previous['pr'] == number and previous['state'] in {'notifying', 'pending', 'approved', 'executing', 'uncertain'}:
                raise ValueError('existing_request_requires_resolution')
        ledger.write(row)
        deadline = datetime.fromtimestamp(row['expires'], ZoneInfo('America/Mazatlan')).strftime('%d/%m %H:%M')
        text = (f'Aprobar y desplegar ERP · PR #{number}\nhttps://github.com/{REPO}/pull/{number}\n'
                f'Commit: {head}\nBase y producción: {base}\nVence en 1 hora. '
                f'Vencimiento: {deadline}, hora de Mazatlán. '
                'Solo este commit; cualquier cambio exige nueva solicitud. Rechazar revoca hasta iniciar publicación.')
        if not approval_window()[0]:
            row['state'] = 'blocked'
            ledger.write(row)
            raise ValueError('outside_approval_hours')
        result = telegram(config, 'sendMessage', {'chat_id': config['chat_id'], 'text': text,
            'reply_markup': {'inline_keyboard': [[{'text': label, 'callback_data': signed(config, row, action)}
                for action, label in [('a', 'Aprobar y desplegar'), ('r', 'Rechazar / revocar')]]]}})
        verify_message(result, config['chat_id'])
        row.update(message_id=result['message_id'], state='pending')
        ledger.write(row)
    return row


def deploy(release, base):
    subprocess.run(['/bin/bash', str(APP / 'scripts/deploy_web_safe.sh'), '--expected-sha', release,
                    '--expected-base', base], check=True, timeout=1800,
                   env={'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin', 'HOME': '/root'})
    if production_sha() != release:
        raise ValueError('production_sha_mismatch')


def publish(nonce, api, ledger, deployer=deploy, production=production_sha):
    with ledger.lock():
        row = ledger.read(nonce)
        if row['state'] not in {'approved', 'rejected'}:
            return row
        expired = int(time.time()) >= row['expires']
        decision = row['state']
        row['state'] = 'executing'  # Persist before the first external mutation; never auto-repeat.
        ledger.write(row)
    try:
        api('POST', PREFIX + f"/issues/{row['pr']}/comments", {'body':
            f"ERP Telegram release: {decision}; nonce {nonce}; SHA {row['head_sha']}; "
            f"base {row['base_sha']}; Mauricio via private Telegram; "
            f"decision epoch {row['decision_at']}; {'expired no execution' if expired else 'decision recorded'}. "
            "Recorded by GitHub App, not a human GitHub review."})
        if decision == 'rejected' or expired:
            row['state'] = 'blocked'
        else:
            if production() != row['production_sha']:
                raise ValueError('production_changed')
            pr = eligible(api, row['pr'], row['head_sha'], row['base_sha'])
            if pr['draft']:
                response = api('POST', '/graphql', {'query': 'mutation($id:ID!){markPullRequestReadyForReview(input:{pullRequestId:$id}){pullRequest{id isDraft}}}', 'variables': {'id': pr['node_id']}})
                if response.get('errors') or response['data']['markPullRequestReadyForReview']['pullRequest']['isDraft']:
                    raise ValueError('draft_transition_failed')
            eligible(api, row['pr'], row['head_sha'], row['base_sha'])
            if int(time.time()) >= row['expires']:
                raise ValueError('approval_expired_before_merge')
            merged = api('PUT', PREFIX + f"/pulls/{row['pr']}/merge", {'sha': row['head_sha'], 'merge_method': 'merge'})
            if merged.get('merged') is not True or not SHA.fullmatch(merged.get('sha', '')):
                raise ValueError('merge_not_confirmed')
            release = merged['sha']
            commit = api('GET', PREFIX + '/git/commits/' + release)
            candidate = api('GET', PREFIX + '/git/commits/' + row['head_sha'])
            if ([p['sha'] for p in commit['parents']] != [row['base_sha'], row['head_sha']]
                    or commit['tree']['sha'] != candidate['tree']['sha']
                    or api('GET', PREFIX + '/git/ref/heads/main')['object']['sha'] != release):
                raise ValueError('merged_revision_mismatch')
            row['release_sha'] = release
            with ledger.lock():
                ledger.write(row)
            deployer(release, row['production_sha'])
            row['state'] = 'published'
    except Exception:
        row['state'] = 'uncertain'  # Includes lost responses; operator must reconcile evidence.
    with ledger.lock():
        ledger.write(row)
    return row


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def telegram(config, method, data):
    try:
        if method not in {'sendMessage', 'answerCallbackQuery', 'editMessageReplyMarkup'}:
            raise ValueError('invalid_method')
        request = Request('https://api.telegram.org/bot' + config['token'] + '/' + method,
                          data=json.dumps(data).encode(), headers={'Content-Type': 'application/json'})
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=20) as response:
            raw = response.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError('response_size')
        result = json.loads(raw)
        if not result.get('ok'):
            raise ValueError('response_failed')
        return result['result']
    except Exception:
        # Telegram errors include the credential-bearing URL; never retain them in logs.
        raise RuntimeError('telegram_transport_failed') from None


def verify_message(result, chat_id):
    if (not isinstance(result, dict) or type(result.get('message_id')) is not int
            or result['message_id'] <= 0 or result.get('chat', {}).get('id') != chat_id
            or result.get('chat', {}).get('type') != 'private'):
        raise ValueError('telegram_message_not_confirmed')


def notify_result(nonce, ledger, config, send):
    with ledger.lock():
        row = ledger.read(nonce)
        if row['state'] not in {'published', 'blocked', 'uncertain', 'executing'} or row['notification_id'] is not None:
            return
        # Only delivery is retried. A lost Telegram response can duplicate this informational message.
        if row['state'] == 'published':
            title = 'Publicado correctamente'
            detail = f"Despliegue completado. Commit publicado: {row['release_sha']}"
        elif row['state'] == 'blocked':
            title = 'Rechazado' if row['decision'] == 'rejected' else 'Solicitud vencida'
            detail = 'No se realizó ningún despliegue. Para continuar se necesita una nueva solicitud y aprobación.'
        else:
            title = 'Problema: publicación sin confirmar'
            detail = ('No se pudo confirmar el resultado. Requiere revisión antes de continuar; '
                      'no se repetirá el despliegue automáticamente.')
            if row['release_sha']:
                detail += f"\nCommit de integración: {row['release_sha']} (no confirma el despliegue)."
        result = send(config, 'sendMessage', {'chat_id': row['chat_id'], 'text':
            f"ERP PR #{row['pr']} · {title}\nCommit solicitado: {row['head_sha']}\n{detail}"})
        verify_message(result, row['chat_id'])
        row['notification_id'] = result['message_id']
        ledger.write(row)


def discover_requests(api, ledger, config, send, source=Path('/var/lib/erp-autofix/ledger.json'), production=production_sha):
    info = source.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise ValueError('unsafe_candidate_source')
    candidates = json.loads(source.read_text())['causes']
    for candidate in candidates.values():
        if (candidate.get('status') != 'awaiting_approval' or candidate.get('phase') != 'draft_complete'
                or candidate.get('source_id', '').startswith('operator-')):
            continue
        match = re.fullmatch(r'https://github.com/' + re.escape(REPO) + r'/pull/([1-9][0-9]*)', candidate.get('pr_url', ''))
        head = candidate.get('candidate_sha', '')
        if not match or not SHA.fullmatch(head):
            continue
        number = int(match[1])
        with ledger.lock():
            known = [ledger.read(path.stem) for path in ledger.directory.glob('*.json')]
        if any(row['pr'] == number and row['head_sha'] == head for row in known):
            continue
        try:
            eligible(api, number, head)
            request_release(number, api, ledger, config, send, production, expected_head=head)
        except (ValueError, RuntimeError, OSError, KeyError):
            # A changed candidate or unfinished CI is reconsidered on the next bounded timer tick.
            print('Candidate not eligible for a release request.', flush=True)


def worker(api, ledger, config, send, discover=discover_requests, publisher=publish):
    # Shared with manual request CLI: one release worker/request process at a time.
    with ledger.lock(worker=True):
        for path in sorted(ledger.directory.glob('*.json')):
            with ledger.lock():
                row = ledger.read(path.stem)
            if row['state'] in {'approved', 'rejected'}:
                publisher(path.stem, api, ledger)
            try:
                notify_result(path.stem, ledger, config, send)
            except (ValueError, RuntimeError, OSError, KeyError):
                print('Release notification pending; publication will not be repeated.', flush=True)
        try:
            process_scheduled(api, ledger, config, send)
            discover(api, ledger, config, send)
        except (ValueError, RuntimeError, OSError, KeyError, TypeError, AttributeError):
            print('Candidate discovery unavailable; recorded decisions remain processable.', flush=True)


def process_scheduled(api, ledger, config, send, production=production_sha):
    if not approval_window()[0]:
        return
    queue = Ledger(ledger.directory / 'scheduled', validator=validate_intent)
    # Bring pre-upgrade undecided requests into the same durable daily gate.
    for path in sorted(ledger.directory.glob('*.json')):
        row = ledger.read(path.stem)
        if row['state'] == 'pending' and row['decision'] is None and row['expires'] <= approval_time():
            try:
                request_release(row['pr'], api, ledger, config, send, production, expected_head=row['head_sha'])
            except (ValueError, OSError, RuntimeError, KeyError, subprocess.SubprocessError):
                pass
    for path in sorted(queue.directory.glob('*.json')):
        item = queue.read(path.stem)
        if item['kind'] != 'release' or item['phase'] != 'waiting':
            continue
        try:
            request_release(item['binding']['pr'], api, ledger, config, send, production,
                            expected_head=item['binding']['head'])
        except (ValueError, OSError, RuntimeError, KeyError, subprocess.SubprocessError):
            print('Scheduled approval requires revalidation; no publication authorized.', flush=True)
    # Cleanup has its own ledger and recovery validator; no deletion occurs here.
    cleanup = operational_module('erp_telegram_cleanup')
    cleanup.process_scheduled(Ledger(ledger.directory.parent / 'cleanups', validator=cleanup.validate), config, send)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['request', 'worker'])
    parser.add_argument('--pr', type=int)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('root required')
    config = json.loads((Path(os.environ['CREDENTIALS_DIRECTORY']) / 'telegram.json').read_text())
    api, ledger = GitHub().api, Ledger()
    if args.command == 'request':
        with ledger.lock(worker=True):
            row = request_release(args.pr, api, ledger, config, telegram)
        print(json.dumps({'nonce': row['nonce'], 'state': row['state'], 'head_sha': row['head_sha']}))
    else:
        worker(api, ledger, config, telegram)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        raise SystemExit('Release operation failed; inspect private ledger. No automatic replay.') from None
