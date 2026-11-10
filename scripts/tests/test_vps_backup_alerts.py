import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import subprocess

spec = importlib.util.spec_from_file_location('alerts', Path(__file__).parents[1] / 'vps_backup_alerts.py')
alerts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(alerts)


class BackupAlertTests(unittest.TestCase):
    def test_thresholds_and_causes(self):
        report = dict(success=True, finished_epoch=100)
        for percent, expected in ((74, set()), (75, {'disk_warning'}), (84, {'disk_warning'}), (85, {'disk_urgent'})):
            with self.subTest(percent=percent):
                self.assertEqual(set(alerts.issues(percent, True, report, 'success', 101)), expected)
        self.assertIn('nas', alerts.issues(74, False, report, 'success', 101))
        self.assertIn('verification', alerts.issues(74, True, dict(success=False), 'success', 101))
        self.assertIn('verification', alerts.issues(74, True, report, 'timeout', 101))
        self.assertIn('verification', alerts.issues(74, True, report, 'success', 36 * 3600 + 101))
        self.assertIn('verification', alerts.issues(74, True, None, 'success', 101))

    def test_dedup_escalation_reminder_and_recovery(self):
        sender = Mock(return_value=123)
        state, receipt = alerts.deliver({'disk_warning': '75%'}, {}, 75, 100, sender)
        self.assertEqual(receipt, 123)
        same, receipt = alerts.deliver({'disk_warning': '76%'}, state, 76, 101, sender)
        self.assertIsNone(receipt)
        self.assertEqual(sender.call_count, 1)
        _, receipt = alerts.deliver({'disk_warning': '76%'}, same, 76, 100 + alerts.REPEAT_SECONDS, sender)
        self.assertEqual(receipt, 123)
        urgent, _ = alerts.deliver({'disk_urgent': '85%'}, same, 85, 102, sender)
        self.assertNotIn('Condiciones resueltas:', sender.call_args.args[0])
        alerts.deliver({'disk_warning': '76%'}, urgent, 76, 103, sender)
        self.assertNotIn('Condiciones resueltas:', sender.call_args.args[0])
        recovered, receipt = alerts.deliver({}, urgent, 74, 103, sender)
        self.assertEqual(receipt, 123)
        self.assertEqual(recovered['active'], [])
        self.assertIn('Recuperado:', sender.call_args.args[0])
        _, receipt = alerts.deliver({}, recovered, 74, 104, sender)
        self.assertIsNone(receipt)

    def test_delivery_failure_is_retried_without_advancing_state(self):
        state = {}
        sender = Mock(side_effect=[OSError('offline'), 12])
        with self.assertRaises(OSError):
            alerts.deliver({'nas': 'NAS offline'}, state, 74, 100, sender)
        self.assertEqual(state, {})
        updated, receipt = alerts.deliver({'nas': 'NAS offline'}, state, 74, 101, sender)
        self.assertEqual(updated['active'], ['nas'])
        self.assertEqual(receipt, 12)

    def test_probe_timeouts_and_missing_report_are_alerts(self):
        with patch.object(alerts, 'read_command', side_effect=subprocess.TimeoutExpired('probe', 5)), patch.object(alerts, 'REPORT') as report:
            report.read_text.side_effect = FileNotFoundError()
            found, percent = alerts.collect(100)
        self.assertEqual(set(found), {'nas', 'verification', 'monitor'})
        self.assertIsNone(percent)


if __name__ == '__main__':
    unittest.main()
