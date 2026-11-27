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



class SupersedeBeforePublishTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.ledger = c.Ledger(self.root / 'ledger')
        self.old = row(state='approved', release=None)
        self.old['expires'] = int(time.time()) + 3600
        self.lock = self.root / 'deploy.lock'; self.lock.touch(mode=0o600)
        (self.ledger.directory / '.worker.lock').touch(mode=0o600)
        self.abort = self.root / 'root-abort.json'
        self.attempt = 0
        self.calls = []
        self.main = LIVE
        self.pr = {'number': 42, 'state': 'open', 'merged': False,
                   'head': {'sha': NEXT, 'repo': {'full_name': c.REPO}},
                   'base': {'sha': LIVE, 'repo': {'full_name': c.REPO}}}
        self.services = {'service': 'inactive', 'timer': 'inactive', 'main_pid': 0}
        self.runtime = {'verified': True, 'release': LIVE, 'source_hashes': {'existing_base_source.py': 'e'*64}}
        self.reset_evidence()

    def reset_evidence(self):
        self.ledger.write(self.old)
        self.raw_before = self.ledger.path(NONCE).read_bytes()
        self.row_digest = r.hashlib.sha256(self.raw_before).hexdigest()
        self.receipt = {k: self.old[k] for k in ('nonce', 'pr', 'head_sha', 'base_sha', 'production_sha')}
        self.receipt.update(schema=1, row_sha256=self.row_digest, phase='before_publish', publish_entered=False,
                            effects={k: False for k in ('ledger', 'merge', 'stop', 'official')}, reaped=True,
                            operator_sha256='b'*64, evidence_sha256={'source':'c'*64,'reap':'d'*64},
                            epoch=int(time.time()))
        self.save_abort()

    def save_abort(self):
        self.abort.write_bytes(r.encoded(self.receipt)); self.abort.chmod(0o600)
        self.abort_digest = r.hashlib.sha256(self.abort.read_bytes()).hexdigest()

    def api(self, method, path, data=None):
        self.assertEqual(method, 'GET'); self.assertIsNone(data)
        self.calls.append(path)
        if path.endswith('/git/ref/heads/main'): return {'object': {'sha': self.main}}
        if path.endswith('/pulls/42'): return copy.deepcopy(self.pr)
        raise AssertionError(path)

    def supersede(self, **kwargs):
        self.attempt += 1
        options = dict(expected_row_sha256=self.row_digest, abort_receipt=self.abort,
                       abort_sha256=self.abort_digest, journal=self.root / ('attempt-' + str(self.attempt)),
                       service_inactive=lambda: dict(self.services), runtime_verify=lambda release: dict(self.runtime),
                       production=lambda: LIVE, deploy_lock=self.lock)
        options.update(kwargs)
        return r.supersede_before_publish(NONCE, NEXT, LIVE, self.api, self.ledger, **options)

    def assert_unchanged(self):
        self.assertEqual(self.ledger.path(NONCE).read_bytes(), self.raw_before)

    def test_only_state_changes_and_full_intent_precedes_write(self):
        from erp_release_supervisor import Journal
        append = Journal.append
        def inspect(journal, state, **details):
            if state == 'SUPERSEDE_INTENT': self.assert_unchanged()
            return append(journal, state, **details)
        with patch.object(Journal, 'append', inspect): result = self.supersede()
        self.assertEqual(result, dict(self.old, state='blocked'))
        events = [json.loads(p.read_text()) for p in sorted((self.root/'attempt-1').iterdir())]
        self.assertEqual([e['state'] for e in events], ['SUPERSEDE_INTENT', 'SUPERSEDED'])
        self.assertEqual(events[0]['before_raw_sha256'], self.row_digest)
        self.assertEqual(events[0]['abort_receipt'], self.receipt)
        self.assertEqual(events[0]['runtime_receipt'], self.runtime)
        for path in (self.root/'attempt-1').iterdir(): self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertNotIn('vencida', c.decision_status(result))
        self.assertEqual(result['decision_at'], self.old['decision_at'])
        self.assertTrue(self.calls and all('/merge' not in path for path in self.calls))

    def test_noautomatic_replay_same_journal_or_closed_row(self):
        journal = self.root/'one-attempt'
        self.supersede(journal=journal)
        with self.assertRaises(FileExistsError): self.supersede(journal=journal)
        with self.assertRaises(ValueError): self.supersede()
        self.assertEqual(self.ledger.read(NONCE), dict(self.old, state='blocked'))

    def test_rejects_nonapproved_or_existing_release(self):
        for change in ({'state':'executing'}, {'state':'uncertain'}, {'state':'blocked'}, {'release_sha':OLD}):
            self.old = dict(row(state='approved', release=None), **change)
            self.reset_evidence()
            with self.subTest(change=change), self.assertRaises(ValueError): self.supersede()
            self.assert_unchanged()

    def test_same_head_does_not_bypass_existing_decision_gate(self):
        self.old['head_sha'] = NEXT; self.reset_evidence()
        with self.assertRaisesRegex(ValueError, 'requires_new_head'): self.supersede()
        self.assert_unchanged()

    def test_raw_digest_detects_semantically_identical_reformat(self):
        self.ledger.path(NONCE).write_text(json.dumps(self.old, separators=(',', ':')))
        changed = self.ledger.path(NONCE).read_bytes()
        with self.assertRaisesRegex(ValueError, 'row_changed'): self.supersede()
        self.assertEqual(self.ledger.path(NONCE).read_bytes(), changed)

    def test_abort_digest_and_binding_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'abort_digest'): self.supersede(abort_sha256='0'*64)
        for key, value in [('nonce', CURRENT), ('pr', 99), ('head_sha', NEXT), ('base_sha', LIVE), ('row_sha256','0'*64)]:
            original = self.receipt[key]; self.receipt[key] = value; self.save_abort()
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'abort_binding'): self.supersede()
            self.receipt[key] = original
        self.assert_unchanged()

    def test_noeffects_and_reap_must_be_proven_boolean_not_zero(self):
        for change in ({'publish_entered':True}, {'publish_entered':0}, {'reaped':False}, {'phase':'after_merge'},
                       {'effects':{'ledger':False,'merge':True,'stop':False,'official':False}},
                       {'effects':{'ledger':0,'merge':False,'stop':False,'official':False}}):
            original = copy.deepcopy(self.receipt); self.receipt.update(change); self.save_abort()
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'noeffects'): self.supersede()
            self.receipt = original
        self.assert_unchanged()

    def test_missing_source_reap_provenance_rejected(self):
        self.receipt['evidence_sha256'] = {'source':'c'*64}; self.save_abort()
        with self.assertRaisesRegex(ValueError, 'provenance'): self.supersede()
        self.assert_unchanged()

    def test_timer_service_or_pid_active_refused(self):
        for change in ({'timer':'active'}, {'service':'activating'}, {'main_pid':99}, {'main_pid':False}):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'quiescent'):
                self.supersede(service_inactive=lambda: dict(self.services, **change))
        self.assert_unchanged()

    def test_runtime_wrong_unverified_or_missing_refused(self):
        for value in ({}, {'verified':True,'release':NEXT}, {'verified':1,'release':LIVE}):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'runtime'):
                self.supersede(runtime_verify=lambda release: value)
        self.assert_unchanged()

    def test_remote_drift_between_reads_refuses_without_writes(self):
        for field in ('head', 'base', 'merged', 'main'):
            def runtime(release):
                if field in ('head', 'base'): self.pr[field]['sha'] = OLD
                elif field == 'merged': self.pr['merged'] = True
                else: self.main = OLD
                return dict(self.runtime)
            original = copy.deepcopy(self.pr), self.main
            with self.subTest(field=field), self.assertRaises(ValueError): self.supersede(runtime_verify=runtime)
            self.pr, self.main = original
            self.assert_unchanged()

    def test_row_changed_during_runtime_preserves_new_decision(self):
        revoked = dict(self.old, state='rejected', decision='rejected', decision_at=int(time.time()))
        def runtime(release): self.ledger.write(revoked); return dict(self.runtime)
        with self.assertRaisesRegex(ValueError, 'row_changed'): self.supersede(runtime_verify=runtime)
        self.assertEqual(self.ledger.read(NONCE), revoked)

    def test_abort_changed_during_runtime_rejected(self):
        def runtime(release):
            self.abort.write_bytes(self.abort.read_bytes() + b' ')
            return dict(self.runtime)
        with self.assertRaisesRegex(ValueError, 'abort_changed'): self.supersede(runtime_verify=runtime)
        self.assert_unchanged()

    def test_deploy_or_worker_contention_refuses_before_api(self):
        from erp_release_worker import lease
        with lease(self.lock), self.assertRaises(BlockingIOError): self.supersede()
        with self.ledger.lock(worker=True), self.assertRaises(BlockingIOError): self.supersede()
        self.assertEqual(self.calls, [])
        self.assert_unchanged()

    def test_failure_before_intent_does_not_write_and_after_write_not_replayed(self):
        from erp_release_supervisor import Journal
        append = Journal.append
        for fail_state in ('SUPERSEDE_INTENT','SUPERSEDED'):
            self.reset_evidence()
            def fail(journal, state, **details):
                if state == fail_state: raise OSError('injected durable journal failure')
                return append(journal,state,**details)
            with self.subTest(state=fail_state), patch.object(Journal,'append',fail), self.assertRaises(OSError):
                self.supersede()
            if fail_state == 'SUPERSEDE_INTENT': self.assert_unchanged()
            else: self.assertEqual(self.ledger.read(NONCE),dict(self.old,state='blocked'))
            events = [json.loads(p.read_text()) for p in sorted((self.root/('attempt-'+str(self.attempt))).iterdir())]
            self.assertEqual(events[-1]['state'],'HOLD')

    def test_symlink_or_public_abort_receipt_rejected(self):
        self.abort.chmod(0o644)
        with self.assertRaises(ValueError): self.supersede()
        self.abort.chmod(0o600)
        target=self.root/'actual-abort'; self.abort.rename(target); self.abort.symlink_to(target)
        with self.assertRaises(OSError): self.supersede()
        self.assert_unchanged()


if __name__ == '__main__': unittest.main()
