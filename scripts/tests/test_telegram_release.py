import importlib.util
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('release', Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release/erp_telegram_release.py')
r = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r)
H, B, M, T = 'a'*40, 'b'*40, 'c'*40, 'd'*40


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ledger = r.Ledger(self.temp.name)
        self.config = {'token': 'local-test-secret', 'chat_id': 17}
        self.row = dict(repo=r.REPO, pr=1, head_sha=H, base_sha=B, production_sha=B,
                        nonce='a'*24, expires=int(time.time())+3600, message_id=42, chat_id=17,
                        state='pending', decision=None, decision_at=None, release_sha=None, notification_id=None)
        self.ledger.write(self.row)
        self.calls = []
        self.main = B
        self.pr_head = H
        self.pr_base = B
        self.checks = [{'name': 'test', 'head_sha': H, 'app': {'slug': 'github-actions'},
                        'status': 'completed', 'conclusion': 'success'}]
        self.deploy = Mock()

    def query(self, action='a'):
        return {'id': 'query', 'data': r.signed(self.config, self.row, action), 'from': {'id': 17},
                'message': {'message_id': 42, 'chat': {'id': 17, 'type': 'private'}}}

    def api(self, method, endpoint, data=None):
        self.calls.append((method, endpoint, data))
        if endpoint.endswith('/branches/main/protection'):
            return {'required_status_checks': {'strict': True, 'checks': [{'context': 'test', 'app_id': 15368}]},
                    'enforce_admins': {'enabled': True}}
        if endpoint.endswith('/pulls/1'):
            return {'state': 'open', 'merged': False, 'draft': False, 'node_id': 'node',
                    'head': {'sha': self.pr_head, 'repo': {'full_name': r.REPO}},
                    'base': {'sha': self.pr_base, 'ref': 'main', 'repo': {'full_name': r.REPO}}}
        if endpoint.endswith('/git/ref/heads/main'):
            return {'object': {'sha': self.main}}
        if '/compare/' in endpoint:
            return {'behind_by': 0, 'ahead_by': 1, 'merge_base_commit': {'sha': B}}
        if '/check-runs?' in endpoint:
            return {'check_runs': self.checks, 'total_count': len(self.checks)}
        if '/statuses?' in endpoint:
            return []
        if endpoint.endswith('/comments'):
            return {'id': 12}
        if endpoint.endswith('/merge'):
            self.main = M
            return {'merged': True, 'sha': M}
        if '/git/commits/' in endpoint:
            return {'parents': [{'sha': B}, {'sha': H}], 'tree': {'sha': T}}
        raise AssertionError(endpoint)

    def approve(self):
        return r.decide(self.query(), self.config, self.ledger)

    def publish(self):
        return r.publish(self.row['nonce'], self.api, self.ledger, self.deploy, lambda: B)

    def test_approved_exact_revision_published_once(self):
        self.approve()
        result = self.publish()
        self.assertEqual(result['state'], 'published')
        self.assertEqual(result['decision'], 'approved')
        self.deploy.assert_called_once_with(M, B)
        self.publish()
        self.deploy.assert_called_once()
        merges = [c for c in self.calls if c[0] == 'PUT']
        self.assertEqual(merges[0][2], {'sha': H, 'merge_method': 'merge'})

    def test_impersonation_chat_message_and_tampered_hmac_rejected(self):
        for key in ('user', 'chat', 'type', 'message', 'signature'):
            with self.subTest(key=key):
                q = self.query()
                if key == 'user': q['from']['id'] = 18
                if key == 'chat': q['message']['chat']['id'] = 18
                if key == 'type': q['message']['chat']['type'] = 'group'
                if key == 'message': q['message']['message_id'] = 43
                if key == 'signature': q['data'] = q['data'][:-1] + ('0' if q['data'][-1] != '0' else '1')
                with self.assertRaises(ValueError): r.decide(q, self.config, self.ledger)
        self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'pending')

    def test_expiry_and_replay(self):
        with self.assertRaisesRegex(ValueError, 'expired'):
            r.decide(self.query(), self.config, self.ledger, self.row['expires'])
        self.approve()
        with self.assertRaisesRegex(ValueError, 'already_decided'): self.approve()

    def test_revocation_before_claim_and_rejection_never_deploy(self):
        self.approve()
        r.decide(self.query('r'), self.config, self.ledger)
        self.assertEqual(self.publish()['state'], 'blocked')
        self.assertEqual(self.ledger.read(self.row['nonce'])['decision'], 'rejected')
        self.deploy.assert_not_called()
        self.assertFalse(any(c[0] == 'PUT' for c in self.calls))

    def test_changed_head_base_missing_ci_and_bad_status_block(self):
        for condition in ('head', 'base', 'missing', 'failed', 'wrong_app'):
            with self.subTest(condition=condition):
                self.setUp()
                self.approve()
                if condition == 'head': self.pr_head = 'e'*40
                if condition == 'base': self.pr_base = 'e'*40
                if condition == 'missing': self.checks = []
                if condition == 'failed': self.checks[0]['conclusion'] = 'failure'
                if condition == 'wrong_app': self.checks[0]['app']['slug'] = 'untrusted'
                self.assertEqual(self.publish()['state'], 'uncertain')
                self.deploy.assert_not_called()
                self.assertFalse(any(c[0] == 'PUT' for c in self.calls))

    def test_lost_merge_response_no_retry(self):
        self.approve()
        def lost(method, endpoint, data=None):
            if method == 'PUT': raise OSError('response lost')
            return self.api(method, endpoint, data)
        self.assertEqual(r.publish(self.row['nonce'], lost, self.ledger, self.deploy, lambda: B)['state'], 'uncertain')
        self.publish()
        self.deploy.assert_not_called()
        with self.assertRaises(ValueError): r.decide(self.query('r'), self.config, self.ledger)

    def test_crash_during_effect_is_durable_executing(self):
        self.approve()
        with self.assertRaises(KeyboardInterrupt):
            r.publish(self.row['nonce'], Mock(side_effect=KeyboardInterrupt), self.ledger, self.deploy, lambda: B)
        self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'executing')
        self.publish()
        self.deploy.assert_not_called()

    def test_wrong_merge_tree_or_parent_blocks_deploy(self):
        for field in ('tree', 'parents'):
            self.setUp()
            self.approve()
            def bad(method, endpoint, data=None):
                value = self.api(method, endpoint, data)
                if endpoint.endswith('/git/commits/' + M):
                    value[field] = {'sha': 'f'*40} if field == 'tree' else [{'sha': 'f'*40}, {'sha': H}]
                return value
            self.assertEqual(r.publish(self.row['nonce'], bad, self.ledger, self.deploy, lambda: B)['state'], 'uncertain')
            self.deploy.assert_not_called()

    def test_pagination_includes_failure_on_second_page(self):
        pages = Mock(side_effect=[{'check_runs': [{}]*100, 'total_count': 101},
                                  {'check_runs': [{'conclusion': 'failure'}], 'total_count': 101}])
        rows = r.pages(pages, '/checks?filter=latest', 'check_runs')
        self.assertEqual(len(rows), 101)
        self.assertEqual(rows[-1]['conclusion'], 'failure')
        self.assertIn('&per_page=100&page=2', pages.call_args[0][1])
        with self.assertRaises(ValueError):
            r.pages(Mock(return_value={'check_runs': [], 'total_count': 1}), '/checks', 'check_runs')

    def test_untrusted_paths_permissions_schema_and_symlinks(self):
        for nonce in ('../../etc/passwd', 'x'*24, '/tmp/a'):
            with self.assertRaises(ValueError): self.ledger.read(nonce)
        row = dict(self.row, command='echo unsafe')
        with self.assertRaises(ValueError): self.ledger.write(row)
        path = self.ledger.path(self.row['nonce'])
        path.chmod(0o644)
        with self.assertRaises(ValueError): self.ledger.read(self.row['nonce'])
        path.unlink()
        path.symlink_to('/etc/passwd')
        with self.assertRaises(ValueError): self.ledger.read(self.row['nonce'])
        Path(self.temp.name).chmod(0o755)
        with self.assertRaises(ValueError): r.Ledger(self.temp.name)

    def test_request_message_bound_and_duplicate_blocked(self):
        self.row['state'] = 'blocked'
        self.ledger.write(self.row)
        send = Mock(return_value={'message_id': 99, 'chat': {'id': 17, 'type': 'private'}})
        row = r.request_release(1, self.api, self.ledger, self.config, send, lambda: B)
        self.assertEqual(row['message_id'], 99)
        self.assertEqual(row['state'], 'pending')
        self.assertLessEqual(len(r.signed(self.config, row, 'a')), 64)
        with self.assertRaises(ValueError): r.request_release(1, self.api, self.ledger, self.config, send, lambda: B)

    def test_expired_pending_replaced_but_uncertain_never_replaced(self):
        self.row['expires'] = 1
        self.ledger.write(self.row)
        send = Mock(return_value={'message_id': 99, 'chat': {'id': 17, 'type': 'private'}})
        fresh = r.request_release(1, self.api, self.ledger, self.config, send, lambda: B)
        self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'blocked')
        fresh.update(state='uncertain', decision='approved', decision_at=int(time.time()))
        self.ledger.write(fresh)
        with self.assertRaises(ValueError):
            r.request_release(1, self.api, self.ledger, self.config, send, lambda: B)

    def test_auto_discovery_excludes_synthetic_and_deduplicates(self):
        import json
        self.row['state'] = 'blocked'
        self.ledger.write(self.row)
        source = Path(self.temp.name) / 'source'
        candidate = dict(status='awaiting_approval', phase='draft_complete', source_id='operator-test',
                         pr_url='https://github.com/' + r.REPO + '/pull/1', candidate_sha=H)
        source.write_text(json.dumps({'causes': {'first': candidate}}))
        source.chmod(0o600)
        send = Mock(return_value={'message_id': 99, 'chat': {'id': 17, 'type': 'private'}})
        r.discover_requests(self.api, self.ledger, self.config, send, source, lambda: B)
        send.assert_not_called()
        candidate['source_id'] = 'real-task'
        source.write_text(json.dumps({'causes': {'first': candidate}}))
        # Previously rejected/blocked revision is not requested again automatically.
        r.discover_requests(self.api, self.ledger, self.config, send, source, lambda: B)
        send.assert_not_called()
        self.ledger.path(self.row['nonce']).unlink()
        r.discover_requests(self.api, self.ledger, self.config, send, source, lambda: B)
        send.assert_called_once()
        r.discover_requests(self.api, self.ledger, self.config, send, source, lambda: B)
        send.assert_called_once()

    def test_install_hook_rejects_unknown_consumer(self):
        spec = importlib.util.spec_from_file_location('installer', Path(r.__file__).with_name('install.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with self.assertRaises(ValueError): module.patched(b'unknown consumer')
        original = b"def handle(query, config, state, telegram, now=None):\n    return 'original'\n"
        module.BASELINE = r.hashlib.sha256(original).hexdigest()
        patched = module.patched(original).decode()
        self.assertIn("return 'original'", patched)
        namespace = {}
        exec(compile(patched, '<test>', 'exec'), namespace)
        for mode in ('p:', 't:', 'g:'):
            self.assertEqual(namespace['handle']({'data': mode}, {}, None, None), 'original')

    def test_strict_protection_and_server_merge_race_fail_closed(self):
        self.approve()
        def not_strict(method, endpoint, data=None):
            result = self.api(method, endpoint, data)
            if endpoint.endswith('/branches/main/protection'):
                result['required_status_checks']['strict'] = False
            return result
        with self.assertRaisesRegex(ValueError, 'strict_branch'):
            r.eligible(not_strict, 1)
        def raced(method, endpoint, data=None):
            if method == 'PUT':
                raise RuntimeError('GitHub strict protection refuses outdated base')
            return self.api(method, endpoint, data)
        self.assertEqual(r.publish(self.row['nonce'], raced, self.ledger, self.deploy, lambda: B)['state'], 'uncertain')
        self.assertEqual(self.main, B)
        self.deploy.assert_not_called()

    def test_transport_never_redirects_or_exposes_token(self):
        from urllib.error import HTTPError
        self.assertIsNone(r.NoRedirect().redirect_request(None, None, 302, 'redirect', {}, 'https://evil.test'))
        with patch.object(r, 'build_opener', return_value=Mock(open=Mock(side_effect=
                HTTPError('https://api.telegram.org/botSECRET/sendMessage', 403, 'SECRET', {}, None)))):
            with self.assertRaisesRegex(RuntimeError, '^telegram_transport_failed$') as error:
                r.telegram(self.config, 'sendMessage', {})
        self.assertTrue(error.exception.__suppress_context__)
        self.assertNotIn('SECRET', str(error.exception))

    def test_outbox_retries_delivery_without_republishing_and_discovery_can_fail(self):
        self.approve()
        publisher = Mock(side_effect=lambda nonce, api, ledger: self.publish())
        discover = Mock(side_effect=FileNotFoundError('missing source'))
        bad = Mock(side_effect=RuntimeError('transport'))
        r.worker(self.api, self.ledger, self.config, bad, discover, publisher)
        self.assertIsNone(self.ledger.read(self.row['nonce'])['notification_id'])
        good = Mock(return_value={'message_id': 99, 'chat': {'id': 17, 'type': 'private'}})
        r.worker(self.api, self.ledger, self.config, good, discover, publisher)
        self.assertEqual(self.ledger.read(self.row['nonce'])['notification_id'], 99)
        publisher.assert_called_once()
        self.deploy.assert_called_once()
        r.worker(self.api, self.ledger, self.config, good, discover, publisher)
        good.assert_called_once()

    def test_notification_describes_each_outcome_without_false_success(self):
        cases = [
            ('published', 'approved', M, 'Publicado correctamente', 'Despliegue completado.'),
            ('blocked', 'rejected', None, 'Rechazado', 'No se realizó ningún despliegue.'),
            ('blocked', 'approved', None, 'Solicitud vencida', 'No se realizó ningún despliegue.'),
            ('blocked', None, None, 'Solicitud vencida', 'No se realizó ningún despliegue.'),
            ('uncertain', 'approved', None, 'Problema: publicación sin confirmar', 'Requiere revisión'),
            ('uncertain', 'approved', M, 'Problema: publicación sin confirmar', 'Requiere revisión'),
            ('uncertain', 'rejected', None, 'Problema: publicación sin confirmar', 'Requiere revisión'),
            ('executing', 'approved', M, 'Problema: publicación sin confirmar', 'Requiere revisión'),
        ]
        for state, decision, release, title, detail in cases:
            with self.subTest(state=state, decision=decision, release=release):
                row = dict(self.row, state=state, decision=decision, release_sha=release,
                           decision_at=int(time.time()) if decision else None)
                self.ledger.write(row)
                send = Mock(return_value={'message_id': 99, 'chat': {'id': 17, 'type': 'private'}})
                r.notify_result(row['nonce'], self.ledger, self.config, send)
                text = send.call_args.args[2]['text']
                self.assertIn('ERP PR #1 · ' + title, text)
                self.assertIn(H, text)
                self.assertIn(detail, text)
                self.assertNotIn('Un resultado incierto', text)
                if state == 'published':
                    self.assertIn('Commit publicado: ' + M, text)
                    self.assertNotIn('Requiere revisión', text)
                    self.assertNotIn('no confirm', text)
                else:
                    self.assertNotIn('Publicado correctamente', text)
                    self.assertNotIn('Commit publicado:', text)
                if state in {'uncertain', 'executing'}:
                    self.assertIn('no se repetirá el despliegue automáticamente', text)
                    self.assertNotIn('No se realizó ningún despliegue', text)
                    if release:
                        self.assertIn('Commit de integración: ' + M, text)
                        self.assertIn('(no confirma el despliegue)', text)
                expected = dict(row, notification_id=99)
                self.assertEqual(self.ledger.read(row['nonce']), expected)
                r.notify_result(row['nonce'], self.ledger, self.config, send)
                send.assert_called_once()

    def test_notification_requires_correct_chat_and_worker_lock_is_nonblocking(self):
        self.approve()
        self.publish()
        with self.assertRaises(ValueError):
            r.notify_result(self.row['nonce'], self.ledger, self.config,
                            Mock(return_value={'message_id': 99, 'chat': {'id': 18, 'type': 'private'}}))
        self.assertIsNone(self.ledger.read(self.row['nonce'])['notification_id'])
        with self.ledger.lock(worker=True):
            with self.assertRaises(BlockingIOError):
                r.worker(self.api, self.ledger, self.config, Mock())

    def test_install_is_idempotent_and_restores_consumer_on_failed_restart(self):
        import subprocess
        spec = importlib.util.spec_from_file_location('installer_fault', Path(r.__file__).with_name('install.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        root = Path(self.temp.name)
        source, target, units = root / 'source', root / 'target', root / 'units'
        for directory in (source, target, units): directory.mkdir()
        original = b"def handle(query, config, state, telegram, now=None):\n    return 'original'\n"
        module.BASELINE = r.hashlib.sha256(original).hexdigest()
        consumer = target / 'decidir_corte_point.py'
        consumer.write_bytes(original)
        (source / 'erp_telegram_release.py').write_text('pass\n')
        for name in ('erp-telegram-release.service', 'erp-telegram-release.timer'):
            (source / name).write_text('[Unit]\nDescription=test\n')
        failed = False
        def runner(command, check=False, **kwargs):
            nonlocal failed
            if command[1] == 'show':
                return subprocess.CompletedProcess(command, 0, stdout='inactive\n' if '--property=ActiveState' in command else 'not-found\n')
            if command[1:3] == ['stop', 'erp-telegram-release.timer'] and not (units / 'erp-telegram-release.timer').exists():
                raise AssertionError('must not stop an unknown timer')
            if command[1:3] == ['restart', 'erp-point-decision.service'] and not failed:
                failed = True
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 1 if command[1].startswith('is-') and not check else 0)
        with self.assertRaises(subprocess.CalledProcessError):
            module.install(source, target, units, runner, root / 'state', preflight=lambda: None)
        self.assertEqual(consumer.read_bytes(), original)
        def safe(command, check=False, **kwargs):
            if command[1] == 'show':
                return subprocess.CompletedProcess(command, 0, stdout='inactive\n' if '--property=ActiveState' in command else 'loaded\n')
            return subprocess.CompletedProcess(command, 1 if command[1].startswith('is-') and not check else 0)
        module.install(source, target, units, safe, root / 'state', preflight=lambda: None)
        patched = consumer.read_bytes()
        module.install(source, target, units, safe, root / 'state', preflight=lambda: None)
        self.assertEqual(consumer.read_bytes(), patched)
        self.assertEqual(patched.count(b'from erp_telegram_release'), 1)

    def test_publisher_permission_is_isolated_and_policy_is_read_only(self):
        from types import SimpleNamespace
        generator_permissions = {'contents': 'write', 'pull_requests': 'write', 'checks': 'read', 'statuses': 'read'}
        adapter = Mock()
        module = SimpleNamespace(PERMISSIONS=generator_permissions, GitHub=Mock(return_value=adapter))
        spec = SimpleNamespace(loader=Mock())
        with patch.object(r.importlib.util, 'spec_from_file_location', return_value=spec), \
                patch.object(r.importlib.util, 'module_from_spec', return_value=module):
            publisher = r.GitHub()
        self.assertEqual(module.PERMISSIONS['administration'], 'read')
        self.assertNotIn('administration', generator_permissions)
        for method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            with self.assertRaisesRegex(ValueError, 'read_only'):
                publisher.api(method, r.PREFIX + '/branches/main/protection', {})
        with self.assertRaisesRegex(ValueError, 'read_only'):
            publisher.api('POST', r.PREFIX + '/branches/main/protection/enforce_admins')
        adapter._request.assert_not_called()
        publisher.api('GET', r.PREFIX + '/branches/main/protection')
        self.assertEqual(adapter._request.call_args[0][0], 'GET')

    def test_installer_excludes_active_oneshot_and_worker_lock(self):
        import subprocess
        spec = importlib.util.spec_from_file_location('installer_busy', Path(r.__file__).with_name('install.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        root = Path(self.temp.name)
        source, target, units = root / 'source', root / 'target', root / 'units'
        for directory in (source, target, units): directory.mkdir()
        original = b"def handle(query, config, state, telegram, now=None):\n    return 'original'\n"
        module.BASELINE = r.hashlib.sha256(original).hexdigest()
        consumer = target / 'decidir_corte_point.py'
        consumer.write_bytes(original)
        (source / 'erp_telegram_release.py').write_text('pass\n')
        for name in ('erp-telegram-release.service', 'erp-telegram-release.timer'):
            (source / name).write_text('[Unit]\nDescription=test\n')
        with self.assertRaisesRegex(RuntimeError, 'policy_unreadable'):
            module.install(source, target, units, Mock(), root / 'state',
                           preflight=Mock(side_effect=RuntimeError('policy_unreadable')))
        self.assertEqual(consumer.read_bytes(), original)
        self.assertEqual(list(units.iterdir()), [])
        for status in ('active', 'activating', 'reloading', 'deactivating'):
            calls = []
            def busy(command, **kwargs):
                calls.append(command)
                return subprocess.CompletedProcess(command, 0, stdout=status + '\n')
            with self.assertRaisesRegex(ValueError, 'publication_active'):
                module.install(source, target, units, busy, root / 'state', preflight=lambda: None)
            self.assertEqual(consumer.read_bytes(), original)
            self.assertFalse(any(c[1] in {'stop', 'restart', 'enable', 'disable'} for c in calls))
        with self.ledger.lock(worker=True):
            with self.assertRaises(BlockingIOError):
                module.install(source, target, units, Mock(), Path(self.temp.name))

    def test_expired_approval_audited_once_without_execution(self):
        self.approve()
        row = self.ledger.read(self.row['nonce'])
        row['expires'] = 1
        self.ledger.write(row)
        def audit(method, endpoint, data=None):
            self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'executing')
            return self.api(method, endpoint, data)
        result = r.publish(self.row['nonce'], audit, self.ledger, self.deploy, lambda: B)
        self.assertEqual(result['state'], 'blocked')
        self.assertEqual(result['decision'], 'approved')
        self.assertEqual(len(self.calls), 1)
        self.assertIn('expired no execution', self.calls[0][2]['body'])
        self.publish()
        self.assertEqual(len(self.calls), 1)
        self.deploy.assert_not_called()

    def test_manual_request_preserves_expired_approval_until_worker_audit(self):
        self.approve()
        row = self.ledger.read(self.row['nonce'])
        row['expires'] = 1
        self.ledger.write(row)
        send = Mock(return_value={'message_id': 99, 'chat': {'id': 17, 'type': 'private'}})
        with self.assertRaisesRegex(ValueError, 'existing_request'):
            r.request_release(1, self.api, self.ledger, self.config, send, lambda: B)
        self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'approved')
        send.assert_not_called()
        result = self.publish()
        self.assertEqual(result['state'], 'blocked')
        self.assertTrue(any('expired no execution' in (c[2] or {}).get('body', '') for c in self.calls))
        fresh = r.request_release(1, self.api, self.ledger, self.config, send, lambda: B)
        self.assertEqual(fresh['state'], 'pending')
        self.assertNotEqual(fresh['nonce'], self.row['nonce'])
        self.assertEqual(self.ledger.read(self.row['nonce'])['decision'], 'approved')
        self.deploy.assert_not_called()


if __name__ == '__main__':
    unittest.main()
