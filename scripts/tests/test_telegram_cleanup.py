import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

MODULES = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release'
spec = importlib.util.spec_from_file_location('erp_telegram_release', MODULES / 'erp_telegram_release.py')
r = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = r
spec.loader.exec_module(r)
spec = importlib.util.spec_from_file_location('cleanup', MODULES / 'erp_telegram_cleanup.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.ledger = r.Ledger(root / 'cleanups', validator=c.validate)
        self.path = root / 'proposal.json'
        self.proposal = dict(id='VPS-LIM-20261111-01', host='68.183.165.47', status='awaiting_explicit_approval',
            execution='none', expected_recovery='up to 3.6 GB', expected_usage='77 percent', risk='may need restoration',
            preconditions=['verified recovery required'], preserve=['latest backup and all data'],
            delete_candidates=[dict(bytes_logical=3599781233, container_references=0, id='sha256:'+'a'*64,
                tag='erp-autofix-deps:'+'a'*12, type='docker_image', unique_size_docker_report='3.6 GB')])
        self.path.write_text(json.dumps(self.proposal))
        self.config = {'token': 'local-test', 'chat_id': 17}
        self.send = Mock(side_effect=lambda config, method, payload: {'message_id': 42, 'chat': {'id': 17, 'type': 'private'}, **({'reply_markup': payload['reply_markup']} if method == 'editMessageReplyMarkup' else {})})
        self.row = c.request(self.path, self.ledger, self.config, self.send)

    def query(self, action='a'):
        return dict(id='callback', data=c.signed(self.config, self.row, action),
                    **{'from': {'id': 17}}, message={'message_id': 42, 'chat': {'id': 17, 'type': 'private'}})

    def manifest(self):
        files = [dict(path='/var/log/syslog.1', bytes=10, sha256='b'*64)]
        cache = [dict(id='a'*25, bytes=20)]
        content = dict(files=files, build_cache=cache)
        digest = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        return dict(type='cache_log_manifest', id='sha256:'+digest, **content,
                    recovery=dict(archive='/share/ERP_BACKUPS/vps-cleanup/VPS-LIM-20261111-01/files-recovery.tar.gz',
                                  sha256='c'*64, files_verified=1))

    def test_manifest_buttons_and_exact_binding(self):
        c.decide(self.query('r'), self.config, self.ledger)
        self.proposal['delete_candidates'] = [self.manifest()]
        self.path.write_text(json.dumps(self.proposal))
        self.send.reset_mock()
        self.row = c.request(self.path, self.ledger, self.config, self.send)
        text = self.send.call_args_list[0].args[2]['text']
        self.assertIn('archivos', text)
        self.assertNotIn('Única imagen', text)
        self.assertIn('recuperación', text)
        self.assertIn('reply_markup', self.send.call_args_list[1].args[2])
        c.decide(self.query(), self.config, self.ledger)
        self.assertTrue(c.status(self.path, self.row['nonce'], self.ledger)['approved'])
        self.proposal['risk'] = 'changed'
        self.path.write_text(json.dumps(self.proposal))
        with self.assertRaisesRegex(ValueError, 'proposal_changed'):
            c.status(self.path, self.row['nonce'], self.ledger)

    def test_manifest_rejects_active_sensitive_paths_and_unverified_recovery(self):
        for path in ['/var/log/syslog', '/opt/backups/a.gz', '/var/log/../lib/a.1',
                     '/var/log/backup.gz', '/var/log/journal/a/system.journal']:
            candidate = self.manifest(); candidate['files'][0]['path'] = path
            content = {key: candidate[key] for key in ('files', 'build_cache')}
            candidate['id'] = 'sha256:' + hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            self.proposal['delete_candidates'] = [candidate]
            self.path.write_text(json.dumps(self.proposal))
            with self.subTest(path=path), self.assertRaises(ValueError):
                c.read_proposal(self.path)
        for key, value in [('files_verified', 0), ('sha256', 'invalid'), ('archive', '/tmp/archive')]:
            candidate = self.manifest(); candidate['recovery'][key] = value
            self.proposal['delete_candidates'] = [candidate]
            self.path.write_text(json.dumps(self.proposal))
            with self.subTest(key=key), self.assertRaises(ValueError):
                c.read_proposal(self.path)

    def test_request_persists_before_exposing_buttons(self):
        self.assertEqual(self.send.call_args_list[0].args[1], 'sendMessage')
        self.assertNotIn('reply_markup', self.send.call_args_list[0].args[2])
        self.assertEqual(self.send.call_args_list[1].args[1], 'editMessageReplyMarkup')
        text = self.send.call_args_list[0].args[2]['text']
        self.assertIn(self.row['proposal_sha256'], text)
        self.assertIn('no ejecuta borrados', text)
        self.assertNotIn('may need restoration', text)  # No arbitrary proposal prose sent.
        self.assertLess(len(text), 4096)
        self.assertLessEqual(len(c.signed(self.config, self.row, 'a').encode()), 64)
        with self.assertRaisesRegex(ValueError, 'existing_request'):
            c.request(self.path, self.ledger, self.config, self.send)

    def test_approve_revoke_and_status_binding(self):
        c.decide(self.query(), self.config, self.ledger)
        self.assertTrue(c.status(self.path, self.row['nonce'], self.ledger)['approved'])
        with self.assertRaisesRegex(ValueError, 'already_decided'):
            c.decide(self.query(), self.config, self.ledger)
        self.assertFalse(c.status(self.path, self.row['nonce'], self.ledger, now=self.row['expires'])['approved'])
        c.decide(self.query('r'), self.config, self.ledger)
        self.assertFalse(c.status(self.path, self.row['nonce'], self.ledger)['approved'])
        with self.assertRaisesRegex(ValueError, 'already_decided'):
            c.decide(self.query(), self.config, self.ledger)
        self.path.write_text(self.path.read_text()+'\n')
        with self.assertRaisesRegex(ValueError, 'proposal_changed'):
            c.status(self.path, self.row['nonce'], self.ledger)

    def test_reject_terminal_and_new_request(self):
        c.decide(self.query('r'), self.config, self.ledger)
        with self.assertRaisesRegex(ValueError, 'already_decided'):
            c.decide(self.query(), self.config, self.ledger)
        new = c.request(self.path, self.ledger, self.config, self.send)
        self.assertNotEqual(new['nonce'], self.row['nonce'])

    def test_auth_expiry_and_namespace(self):
        queries = []
        for field, value in [('from', {'id': 18}), ('data', self.query()['data'].replace('c:', 'd:'))]:
            query = self.query(); query[field] = value; queries.append(query)
        for field, value in [('message_id', 43), ('chat', {'id': 17, 'type': 'group'}), ('chat', {'id': 18, 'type': 'private'})]:
            query = self.query(); query['message'][field] = value; queries.append(query)
        query = self.query(); query['data'] = query['data'][:-1] + ('0' if query['data'][-1] != '0' else '1'); queries.append(query)
        for query in queries:
            with self.subTest(query=query), self.assertRaises(ValueError):
                c.decide(query, self.config, self.ledger)
        with self.assertRaisesRegex(ValueError, 'expired'):
            c.decide(self.query(), self.config, self.ledger, now=self.row['expires'])
        self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'pending')

    def test_hash_and_message_tamper_invalidates_signature(self):
        for key, value in [('proposal_sha256', 'b'*64), ('image_id', 'sha256:'+'b'*64), ('expires', self.row['expires']+1)]:
            altered = {**self.row, key: value}; self.ledger.write(altered)
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'unauthorized'):
                c.decide(self.query(), self.config, self.ledger)
        self.ledger.write(self.row)

    def test_schema_and_size_reject_unreviewed_actions(self):
        variants = []
        for key, value in [('command', 'rm -rf /'), ('host', 'elsewhere'), ('preserve', []), ('risk', '\nsecret')]:
            item = copy.deepcopy(self.proposal); item[key] = value; variants.append(item)
        for key, value in [('id', 'not-an-image'), ('tag', 'erp-autofix-deps:'+'b'*12), ('container_references', 1)]:
            item = copy.deepcopy(self.proposal); item['delete_candidates'][0][key] = value; variants.append(item)
        for proposal in variants:
            self.path.write_text(json.dumps(proposal))
            with self.assertRaises(ValueError): c.read_proposal(self.path)
        self.path.write_text(' '*131073)
        with self.assertRaisesRegex(ValueError, 'too_large'): c.read_proposal(self.path)

    def test_ambiguous_send_does_not_expose_buttons_or_retry(self):
        c.decide(self.query('r'), self.config, self.ledger)
        send = Mock(side_effect=RuntimeError('lost response'))
        with self.assertRaises(RuntimeError): c.request(self.path, self.ledger, self.config, send)
        self.assertEqual(send.call_count, 1)
        with self.assertRaisesRegex(ValueError, 'existing_request'):
            c.request(self.path, self.ledger, self.config, send)
        self.assertEqual(send.call_count, 1)

    def test_ambiguous_or_wrong_buttons_disable_authorization(self):
        for result in (RuntimeError('lost edit'), {'message_id': 42, 'chat': {'id': 17, 'type': 'private'}}):
            with tempfile.TemporaryDirectory() as temp:
                ledger = r.Ledger(temp, validator=c.validate)
                send = Mock(side_effect=[{'message_id': 42, 'chat': {'id': 17, 'type': 'private'}}, result])
                with self.assertRaises((ValueError, RuntimeError)):
                    c.request(self.path, ledger, self.config, send)
                row = ledger.read(next(ledger.directory.glob('*.json')).stem)
                self.assertEqual(row['state'], 'notifying')
                query = self.query(); query['data'] = c.signed(self.config, row, 'a')
                with self.assertRaisesRegex(ValueError, 'already_decided'):
                    c.decide(query, self.config, ledger)

    def test_callback_only_records_no_external_action(self):
        send = Mock()
        c.handle(self.query(), self.config, Path(self.temp.name), send)
        self.assertEqual(send.call_count, 1)
        self.assertEqual(send.call_args.args[1], 'answerCallbackQuery')
        self.assertIn('Nada borrado', send.call_args.args[2]['text'])
        self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'approved')
        c.handle(self.query('r'), self.config, Path(self.temp.name), send)
        self.assertEqual(self.ledger.read(self.row['nonce'])['state'], 'rejected')

    def test_request_through_real_transport_and_closed_allowlist(self):
        from io import BytesIO
        c.decide(self.query('r'), self.config, self.ledger)
        calls = []
        def opened(request, timeout):
            method = request.full_url.rsplit('/', 1)[1]
            payload = json.loads(request.data)
            calls.append(method)
            result = {'message_id': 57, 'chat': {'id': 17, 'type': 'private'}}
            if method == 'editMessageReplyMarkup':
                row = self.ledger.read(next(p.stem for p in self.ledger.directory.glob('*.json') if p.stem != self.row['nonce']))
                self.assertEqual(row['message_id'], 57)
                self.assertEqual(row['state'], 'pending')
                result['reply_markup'] = payload['reply_markup']
            return BytesIO(json.dumps({'ok': True, 'result': result}).encode())
        opener = Mock(open=Mock(side_effect=opened))
        with patch.object(r, 'build_opener', return_value=opener):
            row = c.request(self.path, self.ledger, self.config, r.telegram)
            self.assertEqual(row['state'], 'pending')
            self.assertEqual(calls, ['sendMessage', 'editMessageReplyMarkup'])
            with self.assertRaisesRegex(RuntimeError, '^telegram_transport_failed$'):
                r.telegram(self.config, 'deleteMessage', {})
        self.assertEqual(opener.open.call_count, 2)

    def test_default_release_validator_stays_closed(self):
        with self.assertRaises(ValueError): r.validate(self.row)
        self.assertEqual(self.ledger.read(self.row['nonce']), self.row)
        path = self.ledger.path(self.row['nonce'])
        path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'unsafe_ledger'): self.ledger.read(self.row['nonce'])


if __name__ == '__main__':
    unittest.main()
