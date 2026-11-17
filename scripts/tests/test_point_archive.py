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
        with patch.object(archive.prep, 'disk_percent', return_value=80):
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
        return execute.execute(self.row()['nonce'], self.ledger, self.adapter)


class PointBrokerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = PointLifecycleFixture(Path(self.temp.name).resolve())

    def test_prepare_approve_retire_queue_recover_preserves_bytes_and_evidence(self):
        f = self.fixture
        before = f.parse(f.source)
        self.assertEqual(f.prepare(), 'requested')
        self.assertTrue(f.source.exists())
        entry = f.store.catalog_entry(relative_path=f.source.name)
        self.assertEqual(f.store.catalog_entry(entry_id=entry['id']), entry)
        self.assertEqual(f.nas.get_calls, 1)
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
