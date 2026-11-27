"""Explicit historical resolution using private synthetic ledgers and GET-only Git."""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release'
if str(SOURCE) not in sys.path: sys.path.insert(0, str(SOURCE))
import erp_release_resolution as r
c = r.canonical
BASE, HEAD, OLD, LIVE, TREE, NEXT = [ch * 40 for ch in 'abcdef']
NONCE, CURRENT = '1' * 24, '2' * 24


def row(nonce=NONCE, state='uncertain', release=OLD):
    return {'repo': c.REPO, 'pr': 42, 'head_sha': HEAD, 'base_sha': BASE,
            'production_sha': BASE, 'nonce': nonce, 'expires': int(time.time()) - 100,
            'message_id': 1, 'chat_id': 7, 'state': state, 'decision': 'approved',
            'decision_at': int(time.time()) - 200, 'release_sha': release,
            'notification_id': None}


class ResolutionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.ledger = c.Ledger(self.root / 'ledger')
        self.old = row()
        self.current = row(CURRENT, 'published', LIVE)
        self.ledger.write(self.old); self.ledger.write(self.current)
        self.main = LIVE
        self.runtime = {'verified': True, 'release': LIVE, 'proof': 'synthetic_runtime_proof'}
        self.calls = []
        self.pr = {'head': {'sha': HEAD, 'repo': {'full_name': c.REPO}},
                   'base': {'sha': BASE, 'repo': {'full_name': c.REPO}},
                   'merged': True, 'merge_commit_sha': OLD}
        self.parents = [BASE, HEAD]
        self.tree = TREE
        self.compare_status = None
        self.production = patch.object(c, 'production_sha', return_value=LIVE)
        self.production.start(); self.addCleanup(self.production.stop)
        self.path = self.ledger.directory / 'supervision/resolutions' / (NONCE + '.json')

    def api(self, method, path, data=None):
        self.assertEqual(method, 'GET'); self.assertIsNone(data)
        self.calls.append(path)
        if path.endswith('/git/ref/heads/main'): return {'object': {'sha': self.main}}
        if '/pulls/' in path: return copy.deepcopy(self.pr)
        if path.endswith('/git/commits/' + OLD):
            return {'parents': [{'sha': p} for p in self.parents], 'tree': {'sha': self.tree}}
        if path.endswith('/git/commits/' + HEAD): return {'tree': {'sha': TREE}}
        if '/compare/' in path:
            older, newer = path.split('/compare/')[1].split('...')
            status = self.compare_status or ('identical' if older == newer else 'ahead')
            return {'status': status, 'behind_by': 0, 'ahead_by': 0 if older == newer else 1,
                    'base_commit': {'sha': older}, 'merge_base_commit': {'sha': older}}
        raise AssertionError(path)

    def resolve(self):
        return r.resolve_legacy(NONCE, CURRENT, self.api, self.ledger, runtime_verify=lambda release: dict(self.runtime), production=c.production_sha)

    def test_superseded_exact_certificate_no_history_write(self):
        before = self.ledger.path(NONCE).read_bytes()
        current_before = self.ledger.path(CURRENT).read_bytes()
        certificate = self.resolve()
        self.assertEqual(certificate['outcome'], 'superseded_verified')
        self.assertEqual(self.ledger.path(NONCE).read_bytes(), before)
        self.assertEqual(self.ledger.path(CURRENT).read_bytes(), current_before)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.path.parent.stat().st_mode & 0o777, 0o700)
        self.assertTrue(r.resolved(self.old, self.api, self.ledger))
        with self.assertRaises(FileExistsError): self.resolve()

    def test_no_merge_requires_exact_unmerged_pr(self):
        self.old['release_sha'] = None; self.ledger.write(self.old)
        self.pr.update(merged=False, merge_commit_sha=None)
        certificate = self.resolve()
        self.assertEqual(certificate['outcome'], 'no_merge_verified')
        self.assertTrue(r.resolved(self.old, self.api, self.ledger))
        self.assertFalse(any('/git/commits/' in path for path in self.calls))

    def test_synthetic_test_merge_sha_on_unmerged_pr_is_not_a_real_merge(self):
        self.old['release_sha'] = None; self.ledger.write(self.old)
        self.pr.update(merged=False, merge_commit_sha=NEXT)
        self.assertEqual(self.resolve()['outcome'], 'no_merge_verified')
        self.assertTrue(r.resolved(self.old, self.api, self.ledger))

    def test_runtime_verifier_is_required_and_receipt_bound(self):
        with self.assertRaises(TypeError):
            r.resolve_legacy(NONCE, CURRENT, self.api, self.ledger, production=c.production_sha)
        import hashlib
        certificate = self.resolve()
        self.assertEqual(certificate['runtime_receipt_sha256'], hashlib.sha256(r.encoded(self.runtime)).hexdigest())
        self.assertEqual(certificate['runtime_receipt'], self.runtime)

    def test_tampered_runtime_receipt_rejected(self):
        certificate = self.resolve()
        for change in ({'proof': 'modified'}, {'verified': False}, {'release': NEXT}):
            changed = copy.deepcopy(certificate)
            changed['runtime_receipt'].update(change)
            self.path.write_text(json.dumps(changed))
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))
        changed = copy.deepcopy(certificate)
        del changed['runtime_receipt']
        self.path.write_text(json.dumps(changed))
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))

    def test_wrong_missing_or_unverified_runtime_rejected(self):
        for receipt in ({}, {'verified': False, 'release': LIVE}, {'verified': 1, 'release': LIVE},
                        {'verified': True, 'release': NEXT}):
            self.runtime = receipt
            with self.subTest(receipt=receipt), self.assertRaises(ValueError): self.resolve()
            self.assertFalse(self.path.exists())

    def test_live_advance_needs_new_canonical_published_row_and_main(self):
        self.resolve()
        with patch.object(c, 'production_sha', return_value=NEXT):
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))
            self.main = NEXT
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))
            pending = row('3' * 24, 'uncertain', NEXT); self.ledger.write(pending)
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))
            pending['state'] = 'published'; self.ledger.write(pending)
            self.assertTrue(r.resolved(self.old, self.api, self.ledger))

    def test_no_certificate_not_resolved_and_no_creation(self):
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))
        self.assertFalse(self.path.parent.exists())

    def test_no_current_published_authorization_rejected(self):
        for state in ['approved', 'uncertain', 'blocked']:
            changed = dict(self.current, state=state)
            self.ledger.write(changed)
            with self.subTest(state=state), self.assertRaises(ValueError): self.resolve()
        self.assertFalse(self.path.exists())

    def test_current_rejected_decision_rejected(self):
        self.ledger.write(dict(self.current, state='blocked', decision='rejected'))
        with self.assertRaises(ValueError): self.resolve()
        self.assertFalse(self.path.exists())

    def test_ordinary_git_head_without_runtime_publication_insufficient(self):
        self.ledger.write(dict(self.current, state='uncertain'))
        with self.assertRaisesRegex(ValueError, 'supervised_current'): self.resolve()
        self.assertEqual(c.production_sha(), LIVE)

    def test_current_must_match_live_and_main(self):
        self.main = NEXT
        with self.assertRaises(ValueError): self.resolve()
        self.main = LIVE
        with patch.object(c, 'production_sha', return_value=NEXT), self.assertRaises(ValueError): self.resolve()

    def test_cannot_resolve_executing_or_same_nonce(self):
        for state in ['executing', 'published', 'approved', 'blocked']:
            self.ledger.write(dict(self.old, state=state))
            with self.subTest(state=state), self.assertRaises(ValueError): self.resolve()
            self.assertFalse(r.resolved(dict(self.old, state=state), self.api, self.ledger))
        with self.assertRaises(ValueError):
            r.resolve_legacy(NONCE, NONCE, self.api, self.ledger, runtime_verify=lambda release: dict(self.runtime), production=c.production_sha)

    def test_pr_binding_drift_rejected(self):
        for key in ['head', 'base']:
            original = self.pr[key]['sha']; self.pr[key]['sha'] = NEXT
            with self.subTest(key=key), self.assertRaises(ValueError): self.resolve()
            self.pr[key]['sha'] = original
        self.pr['head']['repo']['full_name'] = 'another/repo'
        with self.assertRaises(ValueError): self.resolve()

    def test_merge_commit_parent_tree_ancestry_drift(self):
        variants = [('merge', NEXT), ('parents', [HEAD, BASE]), ('tree', NEXT), ('status', 'diverged')]
        for what, value in variants:
            original = (copy.deepcopy(self.pr), list(self.parents), self.tree, self.compare_status)
            if what == 'merge': self.pr['merge_commit_sha'] = value
            if what == 'parents': self.parents = value
            if what == 'tree': self.tree = value
            if what == 'status': self.compare_status = value
            with self.subTest(what=what), self.assertRaises(ValueError): self.resolve()
            self.pr, self.parents, self.tree, self.compare_status = original
        self.assertFalse(self.path.exists())

    def test_unknown_old_release_never_infers_unmerged(self):
        self.old['release_sha'] = None; self.ledger.write(self.old)
        for merged, commit in [(True, OLD), (None, None)]:
            self.pr.update(merged=merged, merge_commit_sha=commit)
            with self.subTest(merged=merged, commit=commit), self.assertRaises(ValueError): self.resolve()

    def test_semantic_drift_invalidates_but_delivery_receipts_do_not(self):
        self.resolve()
        changed = dict(self.old, notification_id=99, receipt_id=100)
        self.ledger.write(changed)
        self.assertTrue(r.resolved(changed, self.api, self.ledger))
        changed['expires'] += 1; self.ledger.write(changed)
        self.assertFalse(r.resolved(changed, self.api, self.ledger))
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))

    def test_current_ledger_drift_invalidates(self):
        self.resolve()
        for changed in [dict(self.current, release_sha=NEXT), dict(self.current, state='uncertain')]:
            self.ledger.write(changed)
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))

    def test_future_descendant_allowed_diverged_live_rejected(self):
        self.resolve()
        self.main = NEXT
        self.ledger.write(row('3' * 24, 'published', NEXT))
        with patch.object(c, 'production_sha', return_value=NEXT):
            self.assertTrue(r.resolved(self.old, self.api, self.ledger))
            self.compare_status = 'diverged'
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))

    def test_corrupt_binding_schema_outcome_and_duplicate_rejected(self):
        certificate = self.resolve()
        for change in [{'binding_sha256': '0' * 64}, {'schema': True}, {'outcome': 'no_merge_verified'},
                       {'current_nonce': NONCE}, {'epoch': False}]:
            self.path.write_text(json.dumps(dict(certificate, **change)))
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))
        self.path.write_text('{"schema":1,"schema":1}')
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))
        self.path.write_text('not json')
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))

    def test_certificate_symlink_hardlink_mode_and_owner_rejected(self):
        self.resolve()
        copy_path = self.root / 'copy'; copy_path.write_bytes(self.path.read_bytes()); copy_path.chmod(0o600)
        self.path.unlink(); self.path.symlink_to(copy_path)
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))
        self.path.unlink(); os.link(copy_path, self.path)
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))
        self.path.unlink(); self.path.write_bytes(copy_path.read_bytes()); self.path.chmod(0o644)
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))
        self.path.chmod(0o600)
        with patch.object(r.os, 'geteuid', return_value=os.geteuid() + 1):
            self.assertFalse(r.resolved(self.old, self.api, self.ledger))

    def test_directory_symlink_and_public_mode_rejected(self):
        actual = self.root / 'actual'; actual.mkdir(mode=0o700)
        supervision = self.ledger.directory / 'supervision'; supervision.symlink_to(actual, target_is_directory=True)
        with self.assertRaises(OSError): self.resolve()
        supervision.unlink(); supervision.mkdir(mode=0o755)
        with self.assertRaises(ValueError): self.resolve()
        self.assertFalse(self.path.exists())

    def test_api_failure_and_missing_current_fail_closed(self):
        self.resolve()
        def fail(*args): raise RuntimeError('API unavailable')
        self.assertFalse(r.resolved(self.old, fail, self.ledger))
        self.ledger.path(CURRENT).unlink()
        self.assertFalse(r.resolved(self.old, self.api, self.ledger))

    def test_drift_during_api_roundtrip_creates_no_certificate(self):
        def drift(method, path, data=None):
            result = self.api(method, path, data)
            if '/compare/' in path: self.ledger.write(dict(self.old, expires=self.old['expires'] + 1))
            return result
        with self.assertRaises(ValueError): r.resolve_legacy(NONCE, CURRENT, drift, self.ledger, runtime_verify=lambda release: dict(self.runtime), production=c.production_sha)
        self.assertFalse(self.path.exists())


if __name__ == '__main__': unittest.main()
