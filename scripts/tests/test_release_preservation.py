"""Pure broker preservation contract; synthetic wires, no network or Django."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[2] / 'ops/erp-telegram-release/erp_release_preservation.py'
spec = importlib.util.spec_from_file_location('release_preservation', SOURCE)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


def message(task_id='one', route='celery', priority=0):
    return {'headers': {'id': task_id, 'task': 'example'}, 'body': 'opaque',
            'properties': {'priority': priority, 'delivery_info': {'exchange': '', 'routing_key': route}}}


def wire(msg=None, location='ready', queue='celery', tag='tag', score=1.0, raw=None):
    msg = message() if msg is None else msg
    if raw is None:
        value = [msg, '', msg['properties']['delivery_info']['routing_key']] if location == 'unacked' else msg
        raw = json.dumps(value).encode()
    row = {'id': msg['headers']['id'], 'route': msg['properties']['delivery_info']['routing_key'],
           'location': location, 'raw_base64': base64.b64encode(raw).decode(),
           'sha256': hashlib.sha256(raw).hexdigest()}
    row.update({'tag': tag, 'score': score} if location == 'unacked' else {'queue': queue})
    return row


def cut(*rows, absent_tags=()):
    lengths = {q: sum(r['location'] == 'ready' and r['queue'] == q for r in rows) for q in p.QUEUE_KEYS}
    count = sum(r['location'] == 'unacked' for r in rows)
    tags = {r['tag']: {'hash_present': True, 'score': r['score']} for r in rows if r['location'] == 'unacked'}
    tags.update({tag: {'hash_present': False, 'score': None} for tag in absent_tags})
    return {'records': list(rows), 'ready_lengths': lengths, 'ready_length_hints': dict(lengths),
            'unacked_count_hint_before': count, 'unacked_count_hint_after': count, 'tag_states': tags}


class PreservationTests(unittest.TestCase):
    def test_exact_pending_and_ready(self):
        before = cut(wire(), wire(message('two'), 'unacked'))
        self.assertTrue(p.validate_restored(before, copy.deepcopy(before))['preserved'])

    def test_payload_change_requires_review(self):
        changed = message(); changed['body'] = 'changed'
        with self.assertRaises(p.NeedsReview):
            p.validate_restored(cut(wire()), cut(wire(changed)))

    def test_ready_queue_change_requires_review(self):
        with self.assertRaises(p.NeedsReview):
            p.validate_restored(cut(wire()), cut(wire(queue='celery\x06\x163')))

    def test_missing_is_not_success(self):
        after = cut(); after['metadata_evidence'] = {'one': {'status': 'SUCCESS'}}
        with self.assertRaises(p.NeedsReview):
            p.validate_restored(cut(wire()), after)

    def test_duplicate_id_and_tag_rejected(self):
        for rows in [(wire(), wire(location='unacked')), (wire(location='unacked'), wire(message('two'), 'unacked'))]:
            with self.subTest(rows=rows), self.assertRaises(p.NeedsReview):
                p.validate(cut(*rows))

    def test_native_restore_exact_priority(self):
        before = cut(wire(message(priority=5), 'unacked'))
        restored = message(priority=5)
        restored['headers']['redelivered'] = True
        restored['properties']['delivery_info']['redelivered'] = True
        after = cut(wire(restored, queue='celery\x06\x163'), absent_tags=['tag'])
        self.assertEqual(p.validate_restored(before, after)['outcomes'], {'one': 'restored_unique_exact'})
        for mutate in ['old_tag', 'payload', 'flag', 'queue']:
            broken = copy.deepcopy(after)
            if mutate == 'old_tag': broken['tag_states']['tag']['hash_present'] = True
            else:
                msg = copy.deepcopy(restored)
                if mutate == 'payload': msg['body'] = 'different'
                if mutate == 'flag': del msg['headers']['redelivered']
                broken = cut(wire(msg, queue='celery' if mutate == 'queue' else 'celery\x06\x163'), absent_tags=['tag'])
            with self.subTest(mutate=mutate), self.assertRaises(p.NeedsReview):
                p.validate_restored(before, broken)

    def test_new_reservation_rejected_but_ready_birth_preserved(self):
        for route, scope, stopped in [('celery', 'general', False), ('sat', 'all', True)]:
            with self.subTest(route=route), self.assertRaises(p.NeedsReview):
                p.validate_restored(cut(), cut(wire(message(route=route), 'unacked')), scope=scope, all_stopped=stopped)
        self.assertTrue(p.validate_restored(cut(), cut(wire()), all_stopped=True)['preserved'])

    def test_secondary_progress_does_not_certify_success(self):
        result = p.validate_restored(cut(wire(message(route='sat'), queue='sat')), cut())
        self.assertTrue(result['no_task_success_inferred'])
        self.assertEqual(len(result['other_consumer_progress']), 1)

    def test_duplicate_json_and_nonfinite_rejected(self):
        raw = json.dumps(message()).encode()
        duplicate = raw.replace(b'"id": "one"', b'"id": "one", "id": "one"')
        for value in [duplicate, raw.replace(b'"priority": 0', b'"priority": NaN'), raw.replace(b'"priority": 0', b'"priority": Infinity')]:
            with self.subTest(value=value), self.assertRaises((p.NeedsReview, ValueError)):
                p.validate(cut(wire(raw=value)))

    def test_hash_score_and_route_rejected(self):
        cases = [wire(), wire(location='unacked'), wire()]
        cases[0]['sha256'] = '0' * 64
        cases[1]['score'] = float('nan')
        cases[2]['route'] = 'sat'
        for row in cases:
            with self.subTest(row=row), self.assertRaises(p.NeedsReview):
                p.validate(cut(row))

    def test_coverage_and_reservation_state_rejected(self):
        for key in ['ready_length_hints', 'unacked_count_hint_after', 'tag_states']:
            value = cut(wire(location='unacked'))
            value[key] = {} if key != 'unacked_count_hint_after' else 0
            with self.subTest(key=key), self.assertRaises(p.NeedsReview): p.validate(value)

    def test_count_raw_and_combined_caps(self):
        for key, value, sample in [('MAX_READY', 0, cut(wire())), ('MAX_UNACKED', 0, cut(wire(location='unacked'))),
                                   ('MAX_RAW', 4, cut(wire())), ('RAW_BUDGET', 4, cut(wire()))]:
            with self.subTest(key=key), patch.object(p, key, value), self.assertRaises(p.NeedsReview):
                p.validate(sample)
        row = wire(); row['raw_base64'] = 'A' * (4 * ((p.MAX_RAW + 2) // 3) + 1)
        with self.assertRaises(p.NeedsReview): p.validate(cut(row))

    def test_descriptor_diff_omits_raw_and_hashes_body(self):
        changed = message(); changed['body'] = 'private body'
        result = p.descriptor_diff(cut(wire()), cut(wire(changed)))
        encoded = json.dumps(result)
        self.assertNotIn('raw_base64', encoded)
        self.assertNotIn('private body', encoded)
        self.assertIn('sha256', encoded)


if __name__ == '__main__':
    unittest.main()
