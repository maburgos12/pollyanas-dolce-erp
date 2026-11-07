"""Restricted GitHub App adapter; credentials stay in root-owned process memory."""
import base64
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

_SPEC = importlib.util.spec_from_file_location('erp_autofix_policy', Path(__file__).with_name('policy.py'))
policy = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(policy)

PREFIX = '/repos/maburgos12/pollyanas-dolce-erp'
BRANCH = re.compile(r'codex/autofix-[0-9a-f]{16}-[1-9][0-9]*')
SHA = re.compile(r'[0-9a-f]{40}')
TITLE = 'ERP autofix: revisión técnica'
LIMIT = 2 * 1024 * 1024
PERMISSIONS = {'contents': 'write', 'pull_requests': 'write', 'checks': 'read', 'statuses': 'read'}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _text(value, limit):
    return (isinstance(value, str) and len(value) <= limit and
            not any(ord(char) < 32 and char not in '\n\t' for char in value))


def _number(value):
    if type(value) is not int or value <= 0:
        raise ValueError('invalid_pr_number')
    return str(value)


def _sha(value):
    if not isinstance(value, str) or not SHA.fullmatch(value):
        raise ValueError('invalid_sha')
    return value


def _branch(value):
    if not isinstance(value, str) or not BRANCH.fullmatch(value):
        raise ValueError('invalid_autofix_branch')
    return value


class GitHub:
    def __init__(self, config=Path('/etc/erp-autofix/github-app.json')):
        try:
            data = json.loads(Path(config).read_text(encoding='utf-8'))
            if (not isinstance(data, dict) or set(data) != {'app_id', 'installation_id', 'private_key'} or
                    type(data['app_id']) is not int or data['app_id'] <= 0 or
                    type(data['installation_id']) is not int or data['installation_id'] <= 0 or
                    data['private_key'] != '/etc/erp-autofix/app.pem'):
                raise ValueError('invalid config')
        except (OSError, ValueError, TypeError):
            raise ValueError('invalid_github_app_config') from None
        self.config = data
        self._access_token = None
        self._expires_at = 0

    def _request(self, method, path, data, authorization):
        if not isinstance(path, str) or not path.startswith('/') or path.startswith('//') or '#' in path:
            raise ValueError('invalid_api_path')
        headers = {'Authorization': 'Bearer ' + authorization, 'Accept': 'application/vnd.github+json',
                   'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'erp-autofix-root'}
        body = None if data is None else json.dumps(data, ensure_ascii=True, allow_nan=False).encode()
        if body is not None:
            headers['Content-Type'] = 'application/json'
        request = urllib.request.Request('https://api.github.com' + path, data=body, headers=headers, method=method)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        try:
            with opener.open(request, timeout=20) as response:
                if not 200 <= response.status < 300:
                    raise RuntimeError('github_http_status')
                raw = response.read(LIMIT + 1)
            if len(raw) > LIMIT:
                raise RuntimeError('github_response_too_large')
            return json.loads(raw)
        except (urllib.error.URLError, OSError, ValueError):
            # Errors never retain response bodies, Authorization headers or signed requests.
            raise RuntimeError('github_request_failed') from None

    def _token(self):
        now = int(time.time())
        if self._access_token and now + 60 < self._expires_at:
            return self._access_token
        def encode(value):
            return base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).rstrip(b'=')
        unsigned = encode({'alg': 'RS256', 'typ': 'JWT'}) + b'.' + encode({
            'iat': now - 60, 'exp': now + 540, 'iss': str(self.config['app_id'])})
        try:
            signed = subprocess.run(['/usr/bin/openssl', 'dgst', '-sha256', '-sign', self.config['private_key']],
                                    input=unsigned, capture_output=True, timeout=10,
                                    env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'OPENSSL_CONF': '/dev/null'})
        except (OSError, subprocess.SubprocessError):
            raise RuntimeError('github_signing_failed') from None
        if signed.returncode or not signed.stdout:
            raise RuntimeError('github_signing_failed')
        jwt = (unsigned + b'.' + base64.urlsafe_b64encode(signed.stdout).rstrip(b'=')).decode('ascii')
        data = self._request('POST', f"/app/installations/{self.config['installation_id']}/access_tokens",
                             {'repositories': ['pollyanas-dolce-erp'], 'permissions': dict(PERMISSIONS)}, jwt)
        try:
            token = data['token']
            expires = datetime.fromisoformat(data['expires_at'].replace('Z', '+00:00'))
            if (not isinstance(token, str) or not token or any(ord(char) <= 32 for char in token) or
                    expires.utcoffset() is None or expires.timestamp() <= now + 60):
                raise ValueError('invalid token')
        except (KeyError, AttributeError, TypeError, ValueError):
            raise RuntimeError('github_invalid_token_response') from None
        self._access_token, self._expires_at = token, expires.timestamp()
        return token

    @staticmethod
    def _endpoint(method, path, data):
        if method not in {'GET', 'POST'} or not isinstance(path, str):
            raise ValueError('github_endpoint_denied')
        parsed = urllib.parse.urlsplit(path)
        if parsed.scheme or parsed.netloc or parsed.fragment or not path.startswith(PREFIX + '/'):
            raise ValueError('github_endpoint_denied')
        try:
            pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            raise ValueError('github_query_denied') from None
        query = dict(pairs)
        if len(query) != len(pairs):
            raise ValueError('github_query_denied')
        relative = parsed.path[len(PREFIX):]
        if method == 'GET' and data is None:
            if re.fullmatch(r'/pulls/[1-9][0-9]*', relative) and not query:
                return
            paged = (set(query) == {'per_page', 'page'} and query['per_page'] == '100' and
                     query['page'] in {str(number) for number in range(1, 21)})
            if paged and (re.fullmatch(r'/issues/[1-9][0-9]*/comments', relative) or
                          re.fullmatch(r'/commits/[0-9a-f]{40}/(check-runs|status)', relative)):
                return
            if (relative == '/pulls' and set(query) == {'state', 'per_page', 'page'} and
                    query['state'] == 'open' and query['per_page'] == '100' and
                    query['page'] in {'1', '2', '3', '4', '5'}):
                return
            if (relative == '/pulls' and set(query) == {'head', 'state', 'per_page'} and
                    query['state'] == 'all' and query['per_page'] == '100' and
                    query['head'].startswith('maburgos12:')):
                _branch(query['head'][len('maburgos12:'):])
                return
        if method == 'POST' and not query and isinstance(data, dict):
            if (relative == '/pulls' and set(data) == {'head', 'base', 'draft', 'title', 'body'} and
                    data['base'] == 'main' and data['draft'] is True and data['title'] == TITLE and
                    _text(data['body'], 12000)):
                _branch(data['head'])
                return
            if (relative == '/issues/1659/comments' and set(data) == {'body'} and
                    _text(data['body'], 4000) and data['body'].startswith('ERP autofix:') and
                    '```' not in data['body']):
                return
        raise ValueError('github_endpoint_denied')

    def api(self, method, path, data=None):
        self._endpoint(method, path, data)
        return self._request(method, path, data, self._token())

    def pr(self, number):
        data = self.api('GET', PREFIX + '/pulls/' + _number(number))
        if not isinstance(data, dict):
            raise RuntimeError('github_invalid_pr')
        return data

    def _pages(self, path, key=None):
        items, total = [], None
        for page in range(1, 21):
            data = self.api('GET', path + f'?per_page=100&page={page}')
            rows = data if key is None else data.get(key) if isinstance(data, dict) else None
            if not isinstance(rows, list) or len(rows) > 100 or any(not isinstance(row, dict) for row in rows):
                raise RuntimeError('github_invalid_page')
            if key is not None:
                count = data.get('total_count')
                if type(count) is not int or not 0 <= count <= 2000 or total is not None and count != total:
                    raise RuntimeError('github_invalid_page_count')
                total = count
            items.extend(rows)
            if total is not None and len(items) > total:
                raise RuntimeError('github_invalid_page_count')
            if len(rows) < 100 or total is not None and len(items) == total:
                if total is not None and len(items) != total:
                    raise RuntimeError('github_incomplete_pages')
                return items
        raise RuntimeError('github_pagination_exceeded')

    def comments(self, number):
        return self._pages(PREFIX + '/issues/' + _number(number) + '/comments')

    def checks(self, sha):
        path = PREFIX + '/commits/' + _sha(sha)
        runs = self._pages(path + '/check-runs', 'check_runs')
        statuses = self._pages(path + '/status', 'statuses')
        result = []
        for run in runs:
            if not isinstance(run.get('name'), str) or not isinstance(run.get('status'), str):
                raise RuntimeError('github_invalid_check')
            conclusion = run.get('conclusion')
            if conclusion is not None and not isinstance(conclusion, str):
                raise RuntimeError('github_invalid_check')
            result.append({'name': run['name'], 'status': run['status'].upper(),
                           'conclusion': conclusion.upper() if conclusion else None})
        for status in statuses:
            if (not isinstance(status.get('context'), str) or not isinstance(status.get('state'), str) or
                    status['state'] not in {'pending', 'success', 'failure', 'error'}):
                raise RuntimeError('github_invalid_status')
            state = status['state']
            result.append({'name': status['context'], 'status': 'PENDING' if state == 'pending' else 'COMPLETED',
                           'conclusion': None if state == 'pending' else state.upper()})
        return result

    def open_prs(self):
        items = []
        for page in range(1, 6):
            rows = self.api('GET', PREFIX + f'/pulls?state=open&per_page=100&page={page}')
            if not isinstance(rows, list) or len(rows) > 100 or any(not isinstance(row, dict) for row in rows):
                raise RuntimeError('github_invalid_open_pr_page')
            items.extend(rows)
            if len(rows) < 100:
                return items
        raise RuntimeError('github_open_pr_pagination_exceeded')

    def draft(self, branch, body):
        branch = _branch(branch)
        query = urllib.parse.urlencode({'head': 'maburgos12:' + branch, 'state': 'all', 'per_page': '100'})
        existing = self.api('GET', PREFIX + '/pulls?' + query)
        if not isinstance(existing, list) or len(existing) > 1:
            raise RuntimeError('github_ambiguous_existing_pr')
        if existing:
            if not isinstance(existing[0], dict) or existing[0].get('state') != 'open':
                raise RuntimeError('github_existing_pr_closed')
            return existing[0]
        return self.api('POST', PREFIX + '/pulls',
                        {'head': branch, 'base': 'main', 'draft': True, 'title': TITLE, 'body': body})

    def comment_control(self, body):
        return self.api('POST', PREFIX + '/issues/1659/comments', {'body': body})

    def push_env(self):
        encoded = base64.b64encode(('x-access-token:' + self._token()).encode()).decode('ascii')
        return {'GIT_CONFIG_COUNT': '1', 'GIT_CONFIG_KEY_0': 'http.https://github.com/.extraheader',
                'GIT_CONFIG_VALUE_0': 'AUTHORIZATION: basic ' + encoded, 'GIT_TERMINAL_PROMPT': '0'}

    def approval(self, number, sha):
        _sha(sha)
        current = self.pr(number)
        comments = self.comments(number)
        result = policy.approval(current, comments, sha)
        checks = self.checks(sha)
        valid_ci = bool(checks) and all(
            check['status'] == 'COMPLETED' and check['conclusion'] in {'SUCCESS', 'SKIPPED', 'NEUTRAL'}
            for check in checks)
        required = [check for check in checks if check['name'] == 'test']
        valid_ci = valid_ci and len(required) == 1 and required[0]['conclusion'] == 'SUCCESS'
        if not valid_ci:
            result = {'approved': False, 'reason': 'ci_not_green', 'sha': sha}
        final_comments = self.comments(number)
        final = policy.approval(self.pr(number), final_comments, sha)
        if final['reason'] in {'pr_mismatch', 'invalid_evidence'}:
            return {'approved': False, 'reason': 'pr_changed', 'sha': sha}
        return final if valid_ci else result
