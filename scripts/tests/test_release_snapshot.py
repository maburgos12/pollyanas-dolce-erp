"""Read-only snapshot tests using in-memory Redis and Celery substitutes."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import time
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
MODULES = ROOT / 'ops/erp-telegram-release'
if str(MODULES) not in sys.path:
    sys.path.insert(0, str(MODULES))
import erp_release_preservation as p
import erp_release_snapshot as s


def raw(task_id='one'):
    return json.dumps({'headers': {'id': task_id}, 'body': 'opaque',
                       'properties': {'delivery_info': {'exchange': '', 'routing_key': 'celery'}}}).encode()


class Redis:
    def __init__(self):
        self.unacked = {b'tag': b'[' + raw() + b', "", "celery"]'}
        self.ready = {'celery': [raw('two')]}
        self.calls = []
        self.closed = False

    def hlen(self, key): self.calls.append(('hlen', key)); return len(self.unacked)
    def hscan(self, key, **kwargs): self.calls.append(('hscan', key)); return 0, self.unacked
    def zscore(self, key, tag): return 1.0 if (tag.encode() if isinstance(tag, str) else tag) in self.unacked else None
    def llen(self, key): return len(self.ready.get(key, []))
    def lrange(self, key, start, end): return self.ready.get(key, [])[start:end + 1]
    def hexists(self, key, tag): return tag.encode() in self.unacked
    def close(self): self.closed = True


NODES = [queue + '@host' for queue in p.QUEUES]


class Control:
    def __init__(self, missing=False): self.calls = []; self.missing = missing
    def inspect(self, **kwargs):
        owner = self
        class Inspector:
            def __getattr__(self, phase):
                def invoke():
                    owner.calls.append((phase, kwargs))
                    selected = NODES[:-1] if owner.missing else NODES
                    return {n: {'ok': 'pong'} if phase == 'ping' else [] for n in selected}
                return invoke
        return Inspector()


class SnapshotTests(unittest.TestCase):
    def test_capture_exact_and_previous_tag_absence(self):
        client = Redis()
        before = s.capture_pending(client, deadline=time.monotonic() + 20)
        self.assertEqual(len(before['records']), 2)
        client.unacked = {}
        after = s.capture_pending(client, before, deadline=time.monotonic() + 20)
        self.assertEqual(after['tag_states']['tag'], {'hash_present': False, 'score': None})
        self.assertFalse(p.assess(before, after)['preserved'])

    def test_coverage_mutation_fails(self):
        client = Redis()
        client.llen = lambda key: 0
        with self.assertRaises(p.NeedsReview):
            s.capture_pending(client, deadline=time.monotonic() + 20)

    def test_changed_reservation_scan_fails(self):
        client = Redis(); calls = 0
        def scan(*args, **kwargs):
            nonlocal calls
            calls += 1
            return 1, {b'tag': b'first' if calls == 1 else b'changed'}
        client.hscan = scan
        with self.assertRaisesRegex(p.NeedsReview, 'changed during scan'):
            s.capture_pending(client, deadline=time.monotonic() + 20)
        self.assertEqual(calls, 2)

    def test_scan_and_count_caps(self):
        client = Redis(); client.hscan = lambda *a, **k: (1, {})
        with self.assertRaisesRegex(ValueError, 'HSCAN8'):
            s.capture_pending(client, deadline=time.monotonic() + 20)
        client = Redis(); client.hlen = lambda key: 33
        with self.assertRaises(p.NeedsReview): s.capture_pending(client, deadline=time.monotonic() + 20)
        client = Redis(); client.ready = {'celery': [b'{}'] * 2001}
        with self.assertRaises(p.NeedsReview): s.capture_pending(client, deadline=time.monotonic() + 20)

    def test_byte_caps_before_json_decode(self):
        for field in ('MAX_RAW', 'RAW_BUDGET'):
            with self.subTest(field=field), patch.object(s, field, 1), self.assertRaises(p.NeedsReview):
                s.capture_pending(Redis(), deadline=time.monotonic() + 20)

    def test_strict_json(self):
        for value in [b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}']:
            with self.subTest(value=value), self.assertRaises((ValueError, p.NeedsReview)):
                s.strict_json(value)
        client = Redis(); client.ready = {'celery': [raw().replace(b'"id": "one"', b'"id":"one","id":"one"')]}
        with self.assertRaises(p.NeedsReview): s.capture_pending(client, deadline=time.monotonic() + 20)

    def test_deadline_prevents_reads(self):
        client = Redis()
        with self.assertRaises(p.NeedsReview): s.capture_pending(client, deadline=time.monotonic() - 1)
        self.assertEqual(client.calls, [])

    def test_exactly_four_addressed_calls(self):
        control = Control()
        result = s.collect_snapshot(Redis(), None, NODES, app=types.SimpleNamespace(control=control),
                                    deadline=time.monotonic() + 20, versions={'celery': 'test', 'kombu': 'test'})
        self.assertEqual([phase for phase, _ in control.calls], list(s.PHASES))
        self.assertTrue(all(options['destination'] == NODES and 0 < options['timeout'] <= 2 for _, options in control.calls))
        self.assertEqual(set(result['replies']), set(s.PHASES))
        self.assertEqual(result['versions']['celery'], 'test')

    def test_missing_response_and_invalid_nodes_fail(self):
        control = Control(missing=True)
        with self.assertRaises(p.NeedsReview):
            s.inspect_nodes(types.SimpleNamespace(control=control), NODES, deadline=time.monotonic() + 20)
        self.assertEqual(len(control.calls), 1)
        for nodes in [NODES[:-1], NODES[:-1] + [NODES[0]], ['other@host'] + NODES[1:]]:
            with self.subTest(nodes=nodes), self.assertRaises(p.NeedsReview): s.validate_nodes(nodes)

    def test_no_rpc_without_nodes(self):
        result = s.collect_snapshot(Redis(), None, [], deadline=time.monotonic() + 20)
        self.assertNotIn('replies', result)

    def test_connect_timeout_configuration(self):
        seen = {}
        def from_url(url, **kwargs): seen.update(url=url, **kwargs); return 'client'
        with patch.dict(sys.modules, {'redis': types.SimpleNamespace(Redis=types.SimpleNamespace(from_url=from_url))}):
            self.assertEqual(s.connect('rediss://broker'), 'client')
        self.assertEqual(seen, {'url': 'rediss://broker', 'socket_timeout': 2, 'socket_connect_timeout': 2})
        with self.assertRaises(p.NeedsReview): s.connect('amqp://broker')

    def test_cli_closes_on_success_and_failure(self):
        modules = {'django.conf': types.SimpleNamespace(settings=types.SimpleNamespace(CELERY_BROKER_URL='redis://fake')),
                   'config.celery': types.SimpleNamespace(app=None)}
        for fail in (False, True):
            client = Redis()
            if fail: client.hlen = lambda key: 33
            stream = types.SimpleNamespace(buffer=io.BytesIO(b'{"before":null,"nodes":[]}'))
            output = io.StringIO()
            with self.subTest(fail=fail), patch.dict(sys.modules, modules), patch.object(sys, 'stdin', stream), \
                 patch.object(s, 'connect', return_value=client), patch.object(s.importlib.metadata, 'version', return_value='test'), \
                 contextlib.redirect_stdout(output):
                if fail:
                    with self.assertRaises(p.NeedsReview): s.main()
                else: s.main()
            self.assertTrue(client.closed)
            if not fail:
                self.assertEqual(json.loads(output.getvalue())['status'], 'complete')
                self.assertEqual(len(output.getvalue().splitlines()), 1)
            else: self.assertEqual(output.getvalue(), '')


if __name__ == '__main__': unittest.main()
