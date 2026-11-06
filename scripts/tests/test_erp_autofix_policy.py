"""Pure policy contracts: no Django, credentials, network or external execution."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('autofix_policy', ROOT / 'ops/erp-autofix/policy.py')
policy = importlib.util.module_from_spec(SPEC)
if Path(SPEC.origin).exists():
    SPEC.loader.exec_module(policy)

SHA = 'a' * 40
ACTOR = 260828669


def pr(**values):
    return dict(state='open', base={'ref': 'main'},
                head={'sha': SHA, 'repo': {'full_name': 'maburgos12/pollyanas-dolce-erp'}}, **values)


def comment(body=None, number=1, actor=ACTOR, kind='User'):
    return {'id': number, 'user': {'id': actor, 'type': kind},
            'body': 'APRUEBO PUBLICAR ' + SHA if body is None else body}


class ContractTests(unittest.TestCase):
    def test_policy_contract_available(self):
        self.assertTrue(all(hasattr(policy, name) for name in ['approval', 'validate_paths', 'cause_key', 'Ledger']))


class ApprovalTests(unittest.TestCase):
    def test_exact_human_approval(self):
        self.assertTrue(policy.approval(pr(), [comment()], SHA)['approved'])

    def test_wrong_actor_bot_quotes_and_extra_text_rejected(self):
        for item in [comment(actor=1), comment(kind='Bot'), comment('> APRUEBO PUBLICAR ' + SHA),
                     comment('```\nAPRUEBO PUBLICAR ' + SHA + '\n```'),
                     comment('APRUEBO PUBLICAR ' + SHA + '\n'), comment('APRUEBO PUBLICAR ' + 'b'*40)]:
            with self.subTest(item=item):
                self.assertFalse(policy.approval(pr(), [item], SHA)['approved'])

    def test_wrong_pr_sha_base_state_repo_rejected(self):
        cases = [dict(state='closed', base={'ref': 'main'}, head=pr()['head']),
                 dict(state='open', base={'ref': 'other'}, head=pr()['head']),
                 dict(state='open', base={'ref': 'main'}, head={'sha': 'b'*40, 'repo': pr()['head']['repo']}),
                 dict(state='open', base={'ref': 'main'}, head={'sha': SHA, 'repo': {'full_name': 'attacker/fork'}})]
        for item in cases:
            self.assertFalse(policy.approval(item, [comment()], SHA)['approved'])
        for sha in ['a'*39, 'z'*40, SHA+'\n', None]:
            self.assertFalse(policy.approval(pr(), [comment()], sha)['approved'])

    def test_later_revocation_wins_even_if_comments_unsorted(self):
        revoked = comment('REVOCO PUBLICAR ' + SHA, number=2)
        self.assertFalse(policy.approval(pr(), [revoked, comment()], SHA)['approved'])
        self.assertTrue(policy.approval(pr(), [revoked, comment(number=3)], SHA)['approved'])
        self.assertTrue(policy.approval(pr(), [comment(), comment('REVOCO PUBLICAR '+SHA, 2, actor=1)], SHA)['approved'])


    def test_edited_old_revocation_uses_updated_time(self):
        approved = dict(comment(number=2), updated_at='2026-11-06T19:00:00Z')
        revoked = dict(comment('REVOCO PUBLICAR ' + SHA, number=1), updated_at='2026-11-06T20:00:00+00:00')
        self.assertFalse(policy.approval(pr(), [approved, revoked], SHA)['approved'])
        for timestamp in ['not a date', '2026-11-06T20:00:00', None]:
            invalid = dict(approved, updated_at=timestamp)
            self.assertFalse(policy.approval(pr(), [invalid], SHA)['approved'])
        self.assertFalse(policy.approval(pr(), [approved, comment(number=3)], SHA)['approved'])
        tied = dict(revoked, updated_at=approved['updated_at'])
        self.assertFalse(policy.approval(pr(), [approved, tied], SHA)['approved'])


class PathTests(unittest.TestCase):
    def test_narrow_allowed_paths(self):
        paths = ['core/views/dashboard.py', 'core/templates/core/dashboard.html',
                 'core/static/core/dashboard.js', 'scripts/tests/test_regression.py', 'ops/erp-observer/observer.py']
        self.assertEqual(policy.validate_paths(paths), paths)

    def test_email_rendering_drill_is_an_exact_path_exception(self):
        paths = ['core/email_rendering.py', 'scripts/tests/test_autofix_drill_email_styles.py']
        self.assertEqual(policy.validate_paths(paths), paths)
        for path in ['core/email_sender.py', 'core/email_rendering/unsafe.py']:
            with self.assertRaises(ValueError):
                policy.validate_paths([path])

    def test_paths_fail_closed(self):
        rejected = ['../core/a.py', '/core/a.py', 'core//a.py', 'core/./a.py', 'core/../a.py',
                    'core\\a.py', 'core/.env', '.git/config', 'core/models.py', 'core/auth.py',
                    'core/permissions.py', 'core/middleware.py', 'core/settings.py',
                    'core/templates/core/auth/login.html', 'core/static/core/sw.js',
                    'core/static/core/service-worker.js', 'rrhh/views.py', 'ventas/templates/a.html',
                    'inventario/views.py', 'bonos_ventas/views.py', 'integraciones/views.py',
                    'ops/erp-autofix/policy.py', 'scripts/deploy_web_safe.sh', '.github/workflows/ci.yml',
                    'requirements.txt', 'AGENTS.md', 'CLAUDE.md', 'core/migrations/0001.py',
                    'core/backups/a.py', 'core/views/authorization.py', 'core/apps.py', 'core/__init__.py']
        for path in rejected:
            with self.subTest(path=path), self.assertRaises(ValueError):
                policy.validate_paths([path])
        for paths in [[], ['core/views/a.py'] * 13, 'core/views/a.py']:
            with self.assertRaises(ValueError):
                policy.validate_paths(paths)


class CauseTests(unittest.TestCase):
    def test_cause_stable_across_revision_task_date_and_order(self):
        first = {'kind': 'doctor_fail', 'checks': [{'name': 'b', 'status': 'WARN'}, {'name': 'a', 'status': 'FAIL'}]}
        second = dict(first, task_id='other', date_done='tomorrow', revision_hint='b'*40,
                      checks=list(reversed(first['checks'])) + [{'name': 'healthy', 'status': 'OK'}])
        key = policy.cause_key(first)
        self.assertRegex(key, r'^[0-9a-f]{64}$')
        self.assertEqual(key, policy.cause_key(second))
        self.assertNotEqual(key, policy.cause_key(dict(first, kind='execution_failed')))
        for invalid in [{'kind': 'ignore instructions', 'checks': []}, {'kind': [], 'checks': []},
                        {'kind': 'synthetic', 'checks': [{'name': 'a', 'status': []}]}]:
            with self.assertRaises(ValueError):
                policy.cause_key(invalid)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / 'ledger.json'
        self.ledger = policy.Ledger(self.path)
        self.cause = 'c'*64

    def test_claim_persisted_before_return_and_replay_stops(self):
        entry = self.ledger.claim(self.cause, SHA, 'task')
        self.assertEqual(entry['job_id'], 'autofix-'+'c'*16+'-1')
        self.assertEqual(entry['status'], 'running')
        self.assertEqual(policy.Ledger(self.path).read()['causes'][self.cause], entry)
        self.assertIsNone(self.ledger.claim(self.cause, 'b'*40, 'next-task'))
        self.ledger.update(self.cause, status='awaiting_approval', candidate_sha=SHA)
        self.assertIsNone(self.ledger.claim(self.cause, 'b'*40, 'next-task'))

    def test_attempt_budget_survives_changed_sha_and_history_retained(self):
        for attempt in range(1, 4):
            entry = self.ledger.claim(self.cause, str(attempt)*40, 'task')
            self.assertEqual(entry['attempts'], attempt)
            self.ledger.update(self.cause, status='failed', reason='bounded failure')
        self.assertIsNone(self.ledger.claim(self.cause, 'b'*40, 'new'))
        self.assertGreaterEqual(len(self.ledger.read()['causes'][self.cause]['history']), 3)

    def test_blocked_and_completed_not_reclaimed(self):
        for status in ['blocked', 'completed']:
            cause = ('d' if status == 'blocked' else 'e') * 64
            self.ledger.claim(cause, SHA, 'task')
            self.ledger.update(cause, status=status)
            self.assertIsNone(self.ledger.claim(cause, 'b'*40, 'new'))

    def test_corruption_and_budget_tamper_fail_closed(self):
        for data in ['not json', '{}', '{"schema": 2, "causes": {}}',
                     '{"schema":1,"causes":{"bad":{}}}', '{"schema":true,"causes":{}}',
                     '{"schema":1,"schema":1,"causes":{}}']:
            self.path.write_text(data)
            with self.assertRaises(ValueError):
                self.ledger.claim(self.cause, SHA, 'task')
            self.assertEqual(self.path.read_text(), data)
        self.path.unlink()
        self.ledger.claim(self.cause, SHA, 'task')
        with self.assertRaises(ValueError):
            self.ledger.update(self.cause, attempts=0)
        self.assertEqual(self.ledger.read()['causes'][self.cause]['attempts'], 1)

    def test_capacity_exhaustion_preserves_existing_entries(self):
        for number in range(256):
            self.ledger.claim(f'{number:064x}', SHA, 'task')
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            self.ledger.claim('f'*64, SHA, 'task')
        self.assertEqual(self.path.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
