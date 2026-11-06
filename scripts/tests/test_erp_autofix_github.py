"""GitHub App boundary tests; all HTTP/signing are mocked, no external effects."""
import base64
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('autofix_github', ROOT / 'ops/erp-autofix/github.py')
github = importlib.util.module_from_spec(SPEC)
if Path(SPEC.origin).exists():
    SPEC.loader.exec_module(github)
SHA = 'a'*40
BRANCH = 'codex/autofix-'+'b'*16+'-1'
PREFIX = '/repos/maburgos12/pollyanas-dolce-erp'


class ContractTests(unittest.TestCase):
    def test_adapter_available(self):
        self.assertTrue(hasattr(github, 'GitHub'))


class GitHubTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.config = Path(temporary.name) / 'github-app.json'
        self.config.write_text(json.dumps({'app_id': 123, 'installation_id': 456,
                                          'private_key': '/etc/erp-autofix/app.pem'}))
        self.client = github.GitHub(config=self.config)

    def response(self, value, code=200):
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        response = SimpleNamespace(status=code, read=lambda size: raw[:size], getcode=lambda: code)
        return SimpleNamespace(__enter__=lambda _: response, __exit__=lambda *args: None)

    def test_token_signing_permissions_and_memory_cache(self):
        expires = (datetime.now(timezone.utc)+timedelta(minutes=30)).isoformat()
        token = {'token': 'installation-secret', 'expires_at': expires}
        opener = unittest.mock.MagicMock()
        opener.open.return_value.__enter__.return_value.read.return_value = json.dumps(token).encode()
        opener.open.return_value.__enter__.return_value.status = 201
        with patch.object(github.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=b'signed', stderr=b'')) as sign, \
             patch.object(github.urllib.request, 'build_opener', return_value=opener):
            self.assertEqual(self.client._token(), 'installation-secret')
            self.assertEqual(self.client._token(), 'installation-secret')
        self.assertEqual(sign.call_count, 1)
        self.assertEqual(opener.open.call_count, 1)
        args, kwargs = sign.call_args
        self.assertEqual(args[0], ['/usr/bin/openssl', 'dgst', '-sha256', '-sign', '/etc/erp-autofix/app.pem'])
        self.assertNotIn('installation-secret', repr(args[0]))
        head, payload = kwargs['input'].decode().split('.')
        decoded = json.loads(base64.urlsafe_b64decode(payload+'='*(-len(payload)%4)))
        self.assertEqual(decoded['iss'], '123')
        self.assertLessEqual(decoded['exp']-decoded['iat'], 600)
        request = opener.open.call_args.args[0]
        self.assertEqual(request.full_url, 'https://api.github.com/app/installations/456/access_tokens')
        data = json.loads(request.data)
        self.assertEqual(data['repositories'], ['pollyanas-dolce-erp'])
        self.assertEqual(data['permissions'], {'contents': 'write', 'pull_requests': 'write',
                                              'checks': 'read', 'statuses': 'read'})
        self.assertEqual(opener.open.call_args.kwargs['timeout'], 20)

    def test_signer_and_token_errors_do_not_expose_secrets(self):
        with patch.object(github.subprocess, 'run', return_value=SimpleNamespace(returncode=1, stdout=b'', stderr=b'private secret')):
            with self.assertRaises(RuntimeError) as error:
                self.client._token()
        self.assertNotIn('private secret', str(error.exception))
        with patch.object(self.client, '_request', return_value={'token': 'secret', 'expires_at': 'invalid'}), \
             patch.object(github.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=b'signed', stderr=b'')):
            with self.assertRaises(RuntimeError):
                self.client._token()

    def test_token_refreshes_after_expiry_and_invalid_config_stops(self):
        now = 2000000000
        def token(value, seconds):
            return {'token': value, 'expires_at': datetime.fromtimestamp(seconds, timezone.utc).isoformat()}
        with patch.object(github.time, 'time', side_effect=[now, now+1800]), \
             patch.object(self.client, '_request', side_effect=[token('first', now+1200), token('next', now+3600)]) as request, \
             patch.object(github.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=b'signed')):
            self.assertEqual(self.client._token(), 'first')
            self.assertEqual(self.client._token(), 'next')
            self.assertEqual(request.call_count, 2)
        for data in [{'app_id': True, 'installation_id': 1, 'private_key': '/etc/erp-autofix/app.pem'},
                     {'app_id': 1, 'installation_id': 1, 'private_key': '/tmp/other.pem'}]:
            self.config.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                github.GitHub(self.config)

    def test_api_rejects_host_repo_and_write_escalations_before_auth(self):
        cases = [('GET', 'https://evil.test/'), ('GET', '//evil.test/'),
                 ('GET', '/repos/attacker/repo/pulls/1'), ('PUT', PREFIX+'/contents/core/views.py'),
                 ('PUT', PREFIX+'/pulls/1/merge'), ('PATCH', PREFIX+'/git/refs/heads/main'),
                 ('GET', PREFIX+'/actions/secrets'), ('POST', PREFIX+'/issues/1/comments'),
                 ('GET', PREFIX+'/pulls/1#fragment'), ('GET', PREFIX+'/pulls/1?unexpected=1')]
        with patch.object(self.client, '_token') as token:
            for method, path in cases:
                with self.subTest(path=path), self.assertRaises(ValueError):
                    self.client.api(method, path)
        token.assert_not_called()

    def test_create_payload_cannot_publish_or_target_main(self):
        for payload in [{'head': 'main'}, {'head': BRANCH, 'base': 'main', 'draft': False, 'title': 'x', 'body': 'x'},
                        {'head': 'codex/other', 'base': 'main', 'draft': True, 'title': 'x', 'body': 'x'}]:
            with self.assertRaises(ValueError):
                self.client.api('POST', PREFIX+'/pulls', payload)

    def test_request_refuses_redirect_and_oversized_json(self):
        opener = unittest.mock.MagicMock()
        with patch.object(github.urllib.request, 'build_opener', return_value=opener), \
             patch.object(self.client, '_token', return_value='secret'):
            opener.open.side_effect = HTTPError('https://api.github.com/x', 302, 'redirect', {}, None)
            with self.assertRaises(RuntimeError) as error:
                self.client.pr(1)
            self.assertNotIn('secret', str(error.exception))
            opener.open.side_effect = None
            response = opener.open.return_value.__enter__.return_value
            response.status = 200
            response.read.return_value = b' '*(2*1024*1024+1)
            with self.assertRaises(RuntimeError):
                self.client.pr(1)
        handler = github.NoRedirect()
        self.assertIsNone(handler.redirect_request(None, None, 302, 'redirect', {}, 'https://evil.test'))

    def test_comments_pagination_and_excess_fail_closed(self):
        with patch.object(self.client, 'api', side_effect=[[{'id': n} for n in range(100)], [{'id': 101}]]) as api:
            self.assertEqual(len(self.client.comments(3)), 101)
            self.assertIn('page=2', api.call_args.args[1])
        with patch.object(self.client, 'api', return_value=[{'id': n} for n in range(100)]) as api:
            with self.assertRaises(RuntimeError):
                self.client.comments(3)
            self.assertEqual(api.call_count, 20)

    def test_checks_include_status_contexts(self):
        with patch.object(self.client, 'api', side_effect=[
                {'total_count': 1, 'check_runs': [{'name': 'test', 'status': 'completed', 'conclusion': 'success'}]},
                {'total_count': 1, 'statuses': [{'context': 'legacy', 'state': 'pending'}]}]):
            checks = self.client.checks(SHA)
        self.assertEqual(checks, [{'name': 'test', 'status': 'COMPLETED', 'conclusion': 'SUCCESS'},
                                  {'name': 'legacy', 'status': 'PENDING', 'conclusion': None}])

    def test_check_pagination_count_and_unknown_status_fail_closed(self):
        for data in [{'total_count': 2001, 'check_runs': []},
                     {'total_count': 2, 'check_runs': [{'name': 'test', 'status': 'completed', 'conclusion': 'success'}]},
                     {'total_count': True, 'check_runs': []}]:
            with patch.object(self.client, 'api', return_value=data), self.assertRaises(RuntimeError):
                self.client.checks(SHA)
        with patch.object(self.client, 'api', side_effect=[{'total_count': 0, 'check_runs': []},
                {'total_count': 1, 'statuses': [{'context': 'legacy', 'state': 'unknown'}]}]):
            with self.assertRaises(RuntimeError):
                self.client.checks(SHA)

    def test_control_comment_is_fixed_scope_and_technical_envelope(self):
        with patch.object(self.client, '_token', return_value='secret'), patch.object(self.client, '_request') as request:
            self.client.comment_control('ERP autofix: job=autofix-123 reason=awaiting_approval')
            self.assertEqual(request.call_args.args[:2], ('POST', PREFIX+'/issues/1659/comments'))
            for body in ['raw model reasoning', 'ERP autofix: ```code```', 'ERP autofix: '+'x'*4000]:
                with self.assertRaises(ValueError):
                    self.client.comment_control(body)

    def test_open_prs_paginates_with_explicit_five_page_limit(self):
        first = [{'number': number, 'state': 'open'} for number in range(1, 101)]
        with patch.object(self.client, 'api', side_effect=[first, [{'number': 101, 'state': 'open'}]]) as api:
            self.assertEqual(len(self.client.open_prs()), 101)
            self.assertEqual(api.call_args.args, ('GET', PREFIX+'/pulls?state=open&per_page=100&page=2'))
        with patch.object(self.client, 'api', return_value=first) as api:
            with self.assertRaises(RuntimeError):
                self.client.open_prs()
            self.assertEqual(api.call_count, 5)
        with patch.object(self.client, '_token', return_value='secret'), patch.object(self.client, '_request', return_value=[]) as request:
            self.assertEqual(self.client.open_prs(), [])
            request.assert_called_once()
            with self.assertRaises(ValueError):
                self.client.api('GET', PREFIX+'/pulls?state=open&per_page=100&page=6')
            with self.assertRaises(ValueError):
                self.client.api('GET', PREFIX+'/pulls?state=closed&per_page=100&page=1')

    def test_draft_reuses_open_and_blocks_closed(self):
        for state in ['open', 'closed']:
            with patch.object(self.client, 'api', return_value=[{'number': 10, 'state': state}]) as api:
                if state == 'open':
                    self.assertEqual(self.client.draft(BRANCH, 'Technical evidence')['number'], 10)
                else:
                    with self.assertRaises(RuntimeError):
                        self.client.draft(BRANCH, 'Technical evidence')
                self.assertEqual(api.call_count, 1)
        with patch.object(self.client, 'api', side_effect=[[], {'number': 11, 'state': 'open'}]) as api:
            self.client.draft(BRANCH, 'Technical evidence')
            payload = api.call_args.args[2]
            self.assertIs(payload['draft'], True)
            self.assertEqual(payload['base'], 'main')
        with self.assertRaises(ValueError):
            self.client.draft('main', 'Technical evidence')

    def test_push_environment_never_contains_token_argv_or_personal_env(self):
        with patch.object(self.client, '_token', return_value='secret'):
            env = self.client.push_env()
        self.assertEqual(env['GIT_CONFIG_COUNT'], '1')
        self.assertEqual(env['GIT_CONFIG_KEY_0'], 'http.https://github.com/.extraheader')
        encoded = env['GIT_CONFIG_VALUE_0'].split()[-1]
        self.assertEqual(base64.b64decode(encoded), b'x-access-token:secret')
        self.assertNotIn('HOME', env)

    def test_approval_binds_actor_ci_and_final_sha(self):
        pr = {'state': 'open', 'base': {'ref': 'main'},
              'head': {'sha': SHA, 'repo': {'full_name': 'maburgos12/pollyanas-dolce-erp'}}}
        comment = {'id': 1, 'user': {'id': 260828669, 'type': 'User'}, 'body': 'APRUEBO PUBLICAR '+SHA}
        checks = [{'name': 'test', 'status': 'COMPLETED', 'conclusion': 'SUCCESS'}]
        with patch.object(self.client, 'pr', side_effect=[pr, pr]), \
             patch.object(self.client, 'comments', return_value=[comment]), \
             patch.object(self.client, 'checks', return_value=checks):
            self.assertTrue(self.client.approval(1, SHA)['approved'])
        changed = dict(pr, head=dict(pr['head'], sha='b'*40))
        with patch.object(self.client, 'pr', side_effect=[pr, changed]), \
             patch.object(self.client, 'comments', return_value=[comment]), \
             patch.object(self.client, 'checks', return_value=checks):
            self.assertFalse(self.client.approval(1, SHA)['approved'])
        for invalid in [[], [{'name': 'test', 'status': 'PENDING', 'conclusion': None}],
                        [{'name': 'lint', 'status': 'COMPLETED', 'conclusion': 'SUCCESS'}],
                        checks + [{'name': 'lint', 'status': 'COMPLETED', 'conclusion': 'FAILURE'}],
                        checks + [{'name': 'lint', 'status': 'PENDING', 'conclusion': None}],
                        checks + checks]:
            with patch.object(self.client, 'pr', return_value=pr), \
                 patch.object(self.client, 'comments', return_value=[comment]), \
                 patch.object(self.client, 'checks', return_value=invalid):
                self.assertFalse(self.client.approval(1, SHA)['approved'])
        comment['user']['id'] = 1
        with patch.object(self.client, 'pr', return_value=pr), \
             patch.object(self.client, 'comments', return_value=[comment]), \
             patch.object(self.client, 'checks', return_value=checks):
            self.assertFalse(self.client.approval(1, SHA)['approved'])


if __name__ == '__main__':
    unittest.main()
