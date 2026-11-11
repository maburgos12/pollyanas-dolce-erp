import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('monitor', Path(__file__).parents[1] / 'nas_vps_monitor.py')
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


class ExternalMonitorTests(unittest.TestCase):
    def test_thresholds_recovery_reminder_and_observer_gap(self):
        send = Mock(return_value=12)
        bad = dict(public_tcp=False, erp_https=False)
        good = dict(public_tcp=True, erp_https=True)
        state = {}
        for now in [1000, 1060]:
            state = monitor.advance(state, bad, now, send)
        send.assert_not_called()
        state = monitor.advance(state, bad, 1120, send)
        self.assertTrue(state['down'])
        self.assertEqual(send.call_count, 1)
        state = monitor.advance(state, good, 1180, send)
        self.assertTrue(state['down'])
        state = monitor.advance(state, good, 1240, send)
        self.assertFalse(state['down'])
        self.assertEqual(send.call_count, 2)
        self.assertIn('RECUPERADO', send.call_args.args[0])
        state = monitor.advance(state, bad, 1300, send)
        state = monitor.advance(state, bad, 2000, send)
        self.assertEqual(state['failures'], 1)
        state = monitor.advance(state, bad, 2060, send)
        state = monitor.advance(state, bad, 2120, send)
        self.assertEqual(send.call_count, 3)
        state = monitor.advance(state, bad, 2180, send)
        self.assertEqual(send.call_count, 3)
        state = monitor.advance(state, bad, 2120 + 21600, send)
        self.assertEqual(send.call_count, 3)
        state = monitor.advance(state, bad, 2120 + 21660, send)
        monitor.advance(state, bad, 2120 + 21720, send)
        self.assertEqual(send.call_count, 4)

    def test_send_failure_retries_and_does_not_claim_recovery(self):
        send = Mock(side_effect=OSError('secret URL'))
        before = dict(failures=2, checked_at=1000)
        bad = dict(public_tcp=True, erp_https=False)
        state = monitor.advance(before, bad, 1060, send)
        self.assertFalse(state.get('down', False))
        self.assertNotIn('secret', str(state))
        send.side_effect = None
        send.return_value = 9
        state = monitor.advance(state, bad, 1120, send)
        self.assertTrue(state['down'])
        self.assertEqual(state['message_id'], 9)
        self.assertEqual(before, dict(failures=2, checked_at=1000))

    def test_probe_failures_and_direct_connections(self):
        with patch.object(monitor.socket, 'create_connection', side_effect=TimeoutError), patch.object(monitor.urllib.request, 'build_opener') as opener:
            opener.return_value.open.side_effect = OSError
            self.assertEqual(monitor.probe(), dict(public_tcp=False, erp_https=False))
            self.assertEqual(opener.return_value.open.call_args.kwargs['timeout'], 12)

    def test_recipient_must_match(self):
        with patch.object(monitor.urllib.request, 'build_opener') as opener, patch.object(monitor.json, 'load', return_value={'ok': True, 'result': {'chat': {'id': 999}, 'message_id': 10}}):
            with self.assertRaises(ValueError):
                monitor.notify({'token': 'test-token', 'chat_id': 123}, 'test')
            self.assertEqual(opener.return_value.open.call_args.kwargs['timeout'], 15)
