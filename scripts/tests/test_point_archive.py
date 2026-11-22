"""Filesystem integration of the resident Point lifecycle; network/host boundaries injected."""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'ops/erp-telegram-release'))
sys.path.insert(0, str(ROOT))
import erp_point_archive as archive
import erp_cleanup_execute as execute
import erp_telegram_cleanup as cleanup
from erp_telegram_release import Ledger
from pos_bridge.services.archive_access import ArchivePending, ArchiveStore, ArchiveUnavailable


class CopyNAS:
    def __init__(self, root):
        self.root = root
        self.fail_get = False
        self.corrupt_get = False
        self.get_calls = 0

    def provision(self):
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, entry, source):
        self.provision()
        shutil.copyfile(source, self.root / entry['sha256'])

    def get(self, entry, destination):
        self.get_calls += 1
        if self.fail_get:
            raise OSError('NAS unavailable')
        with destination.open('xb') as target:
            target.write(b'corrupt' if self.corrupt_get else (self.root / entry['sha256']).read_bytes())


class IsolatedHost(execute.HostAdapter):
    def __init__(self, broker, root):
        self.point = broker
        self.deploy_lock = root / 'deploy.lock'

    def preflight(self, item):
        self.point.verify_batch(item['files'])

    def measure(self):
        return dict(free_bytes=shutil.disk_usage(self.point.store.sources_root).free,
                    percent=80, containers={'erp': [True, 'unchanged', 'image']},
                    images=['image'], volumes=['postgres'], backups={'backup.sql': [1, 100, 1]},
                    monitors='active\nactive\nactive', http='200')


class PointLifecycleFixture:
    def __init__(self, root, content=b'category workbook', parse=None):
        self.root = root
        self.store = ArchiveStore(root=root / 'archive', sources_root=root / 'raw_exports', wait_seconds=0)
        self.store.sources_root.mkdir()
        self.source = self.store.sources_root / '20260101_010101_point_sales_category_2025-12-01_2025-12-31_1_null.xls'
        self.source.write_bytes(content)
        self.parse = parse or self.byte_semantic
        self.candidate = dict(relative_path=self.source.name, task_id=1,
                              **self.store.snapshot(self.source), **self.parse(self.source))
        self.nas = CopyNAS(root / 'nas')
        self.broker = archive.Broker(store=self.store, nas=self.nas, state=root / 'broker',
                                     inventory=self.inventory, parse=self.parse)
        self.ledger = Ledger(root / 'ledger', validator=cleanup.validate)
        self.config = {'token': 'local-test-only', 'chat_id': 17}
        self.send = Mock(side_effect=lambda config, method, payload: {
            'message_id': 42, 'chat': {'id': 17, 'type': 'private'},
            **({'reply_markup': payload['reply_markup']} if method == 'editMessageReplyMarkup' else {})})
        self.adapter = IsolatedHost(self.broker, root)

    @staticmethod
    def byte_semantic(path):
        return {'semantic_sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest(), 'rows': 1}

    def inventory(self, name=None, after_id=0):
        entries = [self.candidate] if name in (None, self.source.name) else []
        return {'entries': entries, 'next_id': 1, 'has_more': False}

    def prepare(self):
        with patch.object(archive.prep, 'disk_percent', return_value=80), \
             patch.object(cleanup, 'operational_module', return_value=Mock(Broker=lambda: self.broker, collect=lambda now: ({}, 80))):
            return self.broker.prepare(self.ledger, self.config, self.send)

    def row(self):
        return self.ledger.read(next(self.ledger.directory.glob('*.json')).stem)

    def decide(self, action='a', *, sender=17):
        row = self.row()
        query = dict(id='callback', data=cleanup.signed(self.config, row, action),
                     **{'from': {'id': sender}},
                     message={'message_id': 42, 'chat': {'id': 17, 'type': 'private'}})
        return cleanup.decide(query, self.config, self.ledger)

    def execute(self):
        return execute.execute(self.row()['nonce'], self.ledger, self.adapter, now=time.time)


class PointBrokerTests(unittest.TestCase):
    def test_inventory_command_keeps_bounded_budget_for_measured_full_page(self):
        with patch.object(archive.subprocess, 'run', return_value=Mock(stdout='{"entries":[]}')) as run:
            self.assertEqual(archive.Broker().command('--after-id', '3395'), {'entries': []})
        command = run.call_args.args[0]
        self.assertEqual(command[:2], ['timeout', '90'])
        self.assertEqual(command[5:7], ['timeout', '85'])
        self.assertEqual(run.call_args.kwargs['timeout'], 95)
        self.assertTrue(run.call_args.kwargs['check'])

    def setUp(self):
        clock=patch('time.time',return_value=1794938400);clock.start();self.addCleanup(clock.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        health = patch.object(cleanup, 'require_recovery', create=True)
        health.start()
        self.addCleanup(health.stop)
        self.fixture = PointLifecycleFixture(Path(self.temp.name).resolve())

    def test_transient_inventory_failure_retries_bounded_and_preserves_sources(self):
        f = self.fixture
        f.broker.inventory = Mock(side_effect=archive.subprocess.CalledProcessError(
            124, ['inventory'], stderr='secret=must-not-leak'))
        start = time.time()
        for offset, count, delay in ((0, 1, 60), (60, 2, 300), (360, 3, 21600)):
            with patch('time.time', return_value=start + offset):
                self.assertEqual(f.prepare(), 'blocked')
            status = json.loads((f.broker.state / 'status.json').read_text())
            self.assertEqual(status['failures'], count)
            self.assertEqual(status['next_at'], start + offset + delay)
            self.assertEqual(status['last_failure']['stage'], 'inventory')
            self.assertNotIn('must-not-leak', json.dumps(status))
        with patch('time.time', return_value=start + 30000):
            self.assertEqual(f.prepare(), 'attempt_limit')
        self.assertEqual(f.broker.inventory.call_count, 3)
        self.assertTrue(f.source.exists())
        self.assertEqual(list(f.ledger.directory.glob('*.json')), [])
        self.assertNotIn('must-not-leak', str(f.send.call_args_list))

    def test_unknown_failure_is_not_classified_as_transient(self):
        error = archive.subprocess.CalledProcessError(1, ['unknown'], stderr='permission denied')
        self.assertFalse(archive.failure_detail(error, 'inventory')['transient'])
        self.assertFalse(archive.failure_detail(ValueError('object_hash_mismatch'), 'copy_and_readback')['transient'])
        self.assertFalse(archive.failure_detail(archive.subprocess.TimeoutExpired('send', 30), 'request')['transient'])

    def test_recognized_transient_errors_are_safe_codes_only(self):
        for stderr, reason in (
            ('canceling statement due to statement timeout', 'database_timeout'),
            ('Container abc is not running', 'worker_unavailable'),
            ('@ERROR: max connections (2) reached', 'nas_busy'),
            ('Connection timed out', 'connection_timeout'),
            ('Connection reset by peer', 'connection_reset'),
        ):
            with self.subTest(reason=reason):
                detail = archive.failure_detail(archive.subprocess.CalledProcessError(1, ['cmd'], stderr=stderr), 'inventory')
                self.assertTrue(detail['transient'])
                self.assertEqual(detail['reason'], reason)
                self.assertNotIn('stderr', detail)

    def test_inventory_timeout_is_inside_container(self):
        f = self.fixture
        with patch.object(archive.subprocess, 'run', return_value=Mock(stdout='{}')) as run:
            self.assertEqual(f.broker.command('--after-id', '3295'), {})
        args = run.call_args.args[0]
        self.assertEqual(args[3:8], ['exec', 'pastelerias-erp-worker-1', 'timeout', '85', 'python'])
        self.assertGreater(run.call_args.kwargs['timeout'], 90)

    def test_old_installed_contract_waits_for_activation(self):
        f = self.fixture
        with patch.object(cleanup, 'require_recovery', None):
            self.assertEqual(f.prepare(), 'activation_pending')
        f.send.assert_not_called()
        self.assertEqual(f.nas.get_calls, 0)
        self.assertFalse((f.broker.state / 'status.json').exists())

    def test_global_recovery_pending_does_not_copy_or_consume_failures(self):
        f = self.fixture
        with patch.object(cleanup, 'require_recovery', side_effect=cleanup.RecoveryPending('backup_recovery_pending')):
            self.assertEqual(f.prepare(), 'recovery_pending')
            self.assertEqual(f.prepare(), 'recovery_pending')
        f.send.assert_not_called()
        self.assertFalse((f.broker.state / 'status.json').exists())
        self.assertEqual(f.nas.get_calls, 0)
        self.assertTrue(f.source.exists())
        self.assertEqual(f.prepare(), 'requested')

    def test_night_has_no_preparation_or_request(self):
        f = self.fixture
        with patch('time.time', return_value=1794938400 - 9*3600):
            self.assertEqual(f.prepare(), 'outside_approval_hours')
        f.send.assert_not_called()
        self.assertEqual(f.nas.get_calls, 0)
        self.assertTrue(f.source.exists())

    def test_crossing_twenty_keeps_exact_lot_if_broker_runs_first(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        f = self.fixture
        start = datetime(2026, 11, 17, 19, 59, 59, tzinfo=ZoneInfo('America/Mazatlan')).timestamp()
        clock = [start]
        put = f.nas.put
        def slow_put(*args):
            result = put(*args)
            clock[0] = start + 1
            return result
        with patch('time.time', side_effect=lambda: clock[0]), patch.object(f.nas, 'put', side_effect=slow_put):
            self.assertEqual(f.prepare(), 'deferred')
            f.send.assert_not_called()
            self.assertFalse(list(f.ledger.directory.glob('*.json')))
            proposal = next(f.broker.state.glob('*.proposal'))
            original = proposal.read_bytes()
            clock[0] = start + 1 + 11*3600
            self.assertEqual(f.prepare(), 'no_eligible_point_sources')
            self.assertEqual(len(list(f.broker.state.glob('*.proposal'))), 1)
            with patch.object(cleanup, 'operational_module', return_value=Mock(Broker=lambda: f.broker)):
                cleanup.process_scheduled(f.ledger, f.config, f.send)
            self.assertEqual(f.send.call_count, 2)
            self.assertEqual(f.row()['proposal_sha256'], hashlib.sha256(original).hexdigest())
            self.assertTrue(f.source.exists())

    def test_pending_point_lot_revalidated_once_next_morning(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        f = self.fixture
        day = datetime(2026, 11, 17, 7, tzinfo=ZoneInfo('America/Mazatlan')).timestamp()
        with patch('time.time', return_value=day):
            self.assertEqual(f.prepare(), 'requested')
            first = f.row()
        before = f.nas.get_calls
        with patch('time.time', return_value=day+86400), \
             patch.object(cleanup, 'operational_module', return_value=Mock(Broker=lambda: f.broker)):
            cleanup.process_scheduled(f.ledger, f.config, f.send)
            cleanup.process_scheduled(f.ledger, f.config, f.send)
            self.assertEqual(f.prepare(), 'waiting')
        self.assertEqual(f.send.call_count, 4)
        self.assertEqual(f.nas.get_calls, before+1)
        rows = [f.ledger.read(p.stem) for p in f.ledger.directory.glob('*.json')]
        self.assertEqual(len(rows), 2)
        self.assertEqual({r['proposal_sha256'] for r in rows}, {first['proposal_sha256']})
        self.assertTrue(f.source.exists())

    def test_prepare_approve_retire_queue_recover_preserves_bytes_and_evidence(self):
        f = self.fixture
        before = f.parse(f.source)
        self.assertEqual(f.prepare(), 'requested')
        self.assertTrue(f.source.exists())
        entry = f.store.catalog_entry(relative_path=f.source.name)
        self.assertEqual(f.store.catalog_entry(entry_id=entry['id']), entry)
        self.assertEqual(f.nas.get_calls, 2)
        self.assertEqual(f.execute(), 'not_approved')
        f.decide()
        self.assertEqual(f.execute(), 'completed')
        self.assertFalse(f.source.exists())
        with self.assertRaises(ArchivePending):
            with f.store.open_source(f.source):
                pass
        self.assertEqual(f.broker.recover_requests(), [{'id': entry['id'], 'state': 'recovered'}])
        with f.store.open_source(f.source) as source:
            self.assertEqual(f.parse(source.local_path), before)
            self.assertEqual(source.logical_path, f.source)
        state = cleanup.execution_state(f.ledger, f.row()['nonce'])
        self.assertEqual(state['removed'], [entry['id']])
        self.assertEqual(state['before']['backups'], state['after']['backups'])
        self.assertEqual(list((f.store.root / 'requests').glob('*.json')), [])
        self.assertEqual(list((f.store.root / 'staging').iterdir()), [])
        self.assertEqual(f.execute(), 'completed')
        self.assertTrue(f.source.exists())  # Completed authorization cannot delete a recovered original again.

    def test_rejection_and_wrong_signer_never_remove_original(self):
        f = self.fixture
        self.assertEqual(f.prepare(), 'requested')
        with self.assertRaises(ValueError):
            f.decide(sender=18)
        self.assertEqual(f.execute(), 'not_approved')
        f.decide('r')
        self.assertEqual(f.execute(), 'not_approved')
        self.assertTrue(f.source.exists())

    def test_original_changed_after_approval_is_preserved(self):
        f = self.fixture
        self.assertEqual(f.prepare(), 'requested')
        f.decide()
        f.source.write_bytes(b'new original')
        self.assertEqual(f.execute(), 'partial')
        self.assertEqual(f.source.read_bytes(), b'new original')
        self.assertEqual(cleanup.execution_state(f.ledger, f.row()['nonce'])['removed'], [])

    def test_full_historical_catalog_does_not_block_next_bounded_lot(self):
        f = self.fixture
        for index in range(archive.MAX_PILOT_FILES):
            name = f'20260101_010101_point_sales_category_old_{index}.xls'
            old = dict(schema=1, id=archive.entry_id(name, 'a'*64), relative_path=name,
                       sha256='a'*64, bytes=1, nas_key='point-archive/objects/'+'a'*64+'.xls',
                       verified_at=int(time.time()), consumers=['category_report'])
            f.store.register(old)
        self.assertEqual(f.prepare(), 'requested')
        proposal, _ = cleanup.read_proposal(next(f.broker.state.glob('*.proposal')))
        entries = proposal['delete_candidates'][0]['files']
        self.assertEqual([e['relative_path'] for e in entries], [f.source.name])
        self.assertLessEqual(len(entries), archive.MAX_PILOT_FILES)
        self.assertEqual(len(list((f.store.root/'catalog/objects').glob('*.json'))), archive.MAX_PILOT_FILES+1)
        self.assertTrue(f.source.exists())
        self.assertEqual(f.execute(), 'not_approved')

    def test_bounded_lots_resume_at_first_unprepared_candidate(self):
        f = self.fixture
        candidates = []
        for task_id in range(1, 16):
            path = f.store.sources_root / f'20260101_010101_point_sales_category_{task_id}.xls'
            path.write_bytes(f'workbook {task_id}'.encode())
            candidates.append(dict(relative_path=path.name, task_id=task_id,
                **f.store.snapshot(path), **f.parse(path)))
        def inventory(name=None, after_id=0):
            return {'entries':[x for x in candidates if x['relative_path']==name] if name else
                [x for x in candidates if x['task_id']>after_id], 'next_id':15, 'has_more':False}
        f.broker.inventory = inventory
        self.assertEqual(f.prepare(), 'requested')
        status = json.loads((f.broker.state/'status.json').read_text())
        self.assertEqual(status['cursor'], 10)
        first_paths = {p.name for p in f.broker.state.glob('*.proposal')}
        f.decide(); self.assertEqual(f.execute(), 'completed')
        with patch('time.time', return_value=time.time()+301):
            self.assertEqual(f.prepare(), 'requested')
        second = next(p for p in f.broker.state.glob('*.proposal') if p.name not in first_paths)
        proposal, _ = cleanup.read_proposal(second)
        self.assertEqual(len(proposal['delete_candidates'][0]['files']), 5)
        self.assertEqual(json.loads((f.broker.state/'status.json').read_text())['cursor'], 0)
        self.assertEqual(len(list((f.store.root/'catalog/objects').glob('*.json'))), 15)
        self.assertTrue(all((f.store.sources_root/x['relative_path']).exists() for x in candidates[10:]))

    def test_nas_failure_prevents_catalog_and_proposal(self):
        f = self.fixture
        f.nas.fail_get = True
        self.assertEqual(f.prepare(), 'blocked')
        self.assertTrue(f.source.exists())
        self.assertEqual(list(f.ledger.directory.glob('*.json')), [])
        with self.assertRaises(FileNotFoundError):
            f.store.catalog_entry(relative_path=f.source.name)

    def test_corrupt_readback_prevents_catalog_and_retirement(self):
        f = self.fixture
        f.nas.corrupt_get = True
        self.assertEqual(f.prepare(), 'blocked')
        self.assertTrue(f.source.exists())
        with self.assertRaises(FileNotFoundError):
            f.store.catalog_entry(relative_path=f.source.name)

    def test_semantic_mismatch_prevents_catalog(self):
        f = self.fixture
        f.broker.parse = lambda path: {'semantic_sha256': '0' * 64, 'rows': 0}
        self.assertEqual(f.prepare(), 'blocked')
        self.assertTrue(f.source.exists())
        with self.assertRaises(FileNotFoundError):
            f.store.catalog_entry(relative_path=f.source.name)

    def test_recovery_stops_after_three_attempts_with_backoff(self):
        f = self.fixture
        self.assertEqual(f.prepare(), 'requested')
        entry = f.store.catalog_entry(relative_path=f.source.name)
        f.source.unlink()
        f.store.enqueue(entry)
        f.nas.fail_get = True
        start = int(time.time())
        self.assertEqual(f.broker.recover_requests(now=start)[0]['state'], 'retry')
        self.assertEqual(f.broker.recover_requests(now=start + 59), [])
        self.assertEqual(f.broker.recover_requests(now=start + 60)[0]['state'], 'retry')
        self.assertEqual(f.broker.recover_requests(now=start + 360)[0]['state'], 'blocked')
        calls = f.nas.get_calls
        self.assertEqual(f.broker.recover_requests(now=start + 10000), [{'id':entry['id'],'state':'blocked'}])
        self.assertEqual(f.nas.get_calls, calls)
        self.assertFalse(f.source.exists())

    def test_symlink_and_arbitrary_queue_payload_are_not_followed(self):
        f = self.fixture
        self.assertEqual(f.prepare(), 'requested')
        entry = f.store.catalog_entry(relative_path=f.source.name)
        f.source.unlink()
        f.store.enqueue(entry)
        request = f.store.root / 'requests' / (entry['id'] + '.json')
        outside = f.root / 'outside.json'
        outside.write_text(json.dumps({'id': entry['id']}))
        request.unlink()
        request.symlink_to(outside)
        calls = f.nas.get_calls
        self.assertEqual(f.broker.recover_requests(), [])
        self.assertEqual(f.nas.get_calls, calls)
        request.unlink()
        request.write_text(json.dumps({'id': entry['id'], 'path': str(outside), 'command': 'not-executed'}))
        self.assertEqual(f.broker.recover_requests()[0]['state'], 'retry')
        self.assertEqual(f.nas.get_calls, calls)
        self.assertTrue(outside.exists())
        self.assertFalse(f.source.exists())

    def test_source_symlink_does_not_enter_catalog(self):
        f = self.fixture
        outside = f.root / 'outside.xls'
        f.source.rename(outside)
        f.source.symlink_to(outside)
        self.assertEqual(f.prepare(), 'blocked')
        self.assertTrue(outside.exists())
        with self.assertRaises(FileNotFoundError):
            f.store.catalog_entry(relative_path=f.source.name)
class ActivationRecoveryTests(unittest.TestCase):
    def test_two_private_cycles_preserve_operational_source(self):
        from verify_lifecycle import verify
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();fixture=PointLifecycleFixture(root)
            fixture.store.root.mkdir();fixture.nas.provision()
            proof=verify(fixture.broker,fixture.source,fixture.parse,'a'*40,root/'proof.json')
            self.assertEqual(proof['successful_recovery_cycles'],2)
            self.assertEqual(fixture.source.read_bytes(),b'category workbook')
            self.assertEqual(list(fixture.store.root.glob('verification-*')),[])

class NASIndexTests(unittest.TestCase):
    def test_remote_catalog_must_match_immutable_identity(self):
        name='source.xls';digest=hashlib.sha256(b'bytes').hexdigest()
        entry=dict(schema=1,id=hashlib.sha256((name+'\0'+digest).encode()).hexdigest(),relative_path=name,sha256=digest,bytes=5,nas_key='point-archive/objects/'+digest+'.xls',verified_at=1,consumers=['category_report'])
        nas=archive.ObjectNAS();metadata={k:v for k,v in entry.items() if k!='verified_at'}
        nas.command=Mock(return_value=json.dumps(metadata));nas.verify_index(entry)
        entry['verified_at']=2;nas.verify_index(entry)
        metadata['relative_path']='other.xls';nas.command.return_value=json.dumps(metadata)
        with self.assertRaisesRegex(ValueError,'index_mismatch'):nas.verify_index(entry)
