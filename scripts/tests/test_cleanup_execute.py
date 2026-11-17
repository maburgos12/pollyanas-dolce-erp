import copy
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock,patch

MODULES=Path(__file__).resolve().parents[2]/'ops/erp-telegram-release'
sys.path.insert(0,str(MODULES))
import erp_telegram_cleanup as c
import erp_cleanup_execute as e
from erp_telegram_release import Ledger,TelegramRejected


class Adapter:
    def __init__(self,root,entry):
        self.root=root;self.deploy_lock=root/'deploy.lock';self.file=root/entry['relative_path'];self.file.write_bytes(b'original');self.entry=entry;self.fail=False;self.kill=False
    def preflight(self,item):
        if self.fail:raise ValueError('nas_down')
    def measure(self):return dict(free_bytes=100 if self.file.exists() else 108,percent=77 if self.file.exists() else 75,backups='unchanged')
    def verify_preserved(self,before,after):assert before['backups']==after['backups']
    def identity(self,entry):return entry['id']
    @contextmanager
    def lease(self,entry):yield
    def verify(self,entry):return self.file.exists() and hashlib.sha256(self.file.read_bytes()).hexdigest()==entry['sha256']
    def reconcile(self,entry):return 'absent' if not self.file.exists() else 'same' if self.verify(entry) else 'different'
    def remove(self,entry):
        self.file.unlink()
        if self.kill:raise KeyboardInterrupt('simulate abrupt exit after unlink')


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        clock=patch('time.time',return_value=1794938400);clock.start();self.addCleanup(clock.stop)
        recovery=patch.object(c,'revalidate_proposal');recovery.start();self.addCleanup(recovery.stop)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        self.ledger=Ledger(self.root/'cleanups',validator=c.validate);self.config={'token':'test','chat_id':17};digest=hashlib.sha256(b'original').hexdigest();name='20260101_010101_point_sales_category_test.xls'
        self.entry=dict(schema=1,id=hashlib.sha256((name+'\0'+digest).encode()).hexdigest(),relative_path=name,sha256=digest,bytes=8,nas_key='point-archive/objects/'+digest+'.xls',verified_at=int(time.time()),consumers=['category_report'])
        self.item=dict(type='point_archive_manifest',id=c.point_manifest_id([self.entry]),files=[self.entry],recovery={'files_verified':1,'verified_at':int(time.time())})
        self.proposal=dict(schema=2,executor='resident-v1',id='VPS-LIM-20261116-01',host='68.183.165.47',status='awaiting_explicit_approval',execution='resident',expected_recovery='verified',expected_usage='measured',risk='NAS',preconditions=['verify'],preserve=['backups'],delete_candidates=[self.item])
        self.path=self.root/'proposal.json';self.path.write_text(json.dumps(self.proposal));self.send=Mock(side_effect=lambda cfg,m,p:dict(message_id=42,chat={'id':17,'type':'private'},**({'reply_markup':p['reply_markup']} if 'reply_markup' in p else {})))
        self.row=c.request(self.path,self.ledger,self.config,self.send);self.adapter=Adapter(self.root,self.entry)
    def query(self,action='a'):
        return dict(id='query',data=c.signed(self.config,self.row,action),**{'from':{'id':17}},message=dict(message_id=42,chat={'id':17,'type':'private'}))
    def approve(self):c.decide(self.query(),self.config,self.ledger)
    def test_real_preflight_checks_latest_recovery_manifest(self):
        backups=self.root/'backups';backups.mkdir();manifest=backups/'backup_20261116_020000.manifest';manifest.write_bytes(b'verified manifest')
        health=self.root/'health.json';health.write_text(json.dumps({'success':True,'finished_epoch':time.time(),'retained':manifest.stem,'verified':[{'point':manifest.stem,'manifest_sha256':hashlib.sha256(manifest.read_bytes()).hexdigest()}]}))
        adapter=object.__new__(e.HostAdapter);adapter.point=Mock()
        real_path=Path
        def paths(value):
            return health if value=='/var/lib/erp-backup-restore/health-latest.json' else backups if value=='/opt/backups/erp' else real_path(value)
        with patch.object(e,'Path',side_effect=paths):
            adapter.preflight(self.item);adapter.point.verify_batch.assert_called_once_with(self.item['files'])
            manifest.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'manifest_changed'):adapter.preflight(self.item)
    def test_pending_rejected_do_not_delete(self):
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter),'not_approved');c.decide(self.query('r'),self.config,self.ledger);self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter),'not_approved');self.assertTrue(self.adapter.file.exists())
    def test_approve_executes_once_and_keeps_decision(self):
        self.approve();self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter),'completed');self.assertFalse(self.adapter.file.exists());self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter),'completed');self.assertEqual(self.ledger.read(self.row['nonce'])['state'],'approved')
        with self.assertRaises(c.RecordedDecision) as ctx:c.decide(self.query(),self.config,self.ledger)
        self.assertEqual(ctx.exception.state,'completed')
    def test_expiry_during_verify_stops_before_remove(self):
        self.approve();t=[time.time()];self.adapter.verify=lambda entry: (t.__setitem__(0,self.row['expires']+1) or True)
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=lambda:t[0]),'partial');self.assertTrue(self.adapter.file.exists())
    def test_restart_after_unlink_reconciles_without_second_remove(self):
        self.approve();self.adapter.kill=True
        with self.assertRaises(KeyboardInterrupt):e.execute(self.row['nonce'],self.ledger,self.adapter)
        self.adapter.remove=Mock(side_effect=AssertionError('must not repeat unlink'))
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter),'completed');self.adapter.remove.assert_not_called()
    def test_restart_preflight_failure_preserves_intent_and_history(self):
        self.approve();self.adapter.kill=True
        with self.assertRaises(KeyboardInterrupt):e.execute(self.row['nonce'],self.ledger,self.adapter)
        original=c.execution_state(self.ledger,self.row['nonce']);self.adapter.fail=True;t=time.time()
        for offset in (0,61,362):e.execute(self.row['nonce'],self.ledger,self.adapter,now=lambda:t+offset)
        record=c.execution_state(self.ledger,self.row['nonce'])
        self.assertEqual(record['state'],'uncertain');self.assertEqual(record['intent'],original['intent']);self.assertEqual(record['before'],original['before'])
    def test_terminal_receipt_recovers_without_repeating_remove(self):
        self.approve()
        with patch.object(c,'enqueue',side_effect=OSError('disk')):
            with self.assertRaises(OSError):e.execute(self.row['nonce'],self.ledger,self.adapter)
        self.adapter.remove=Mock(side_effect=AssertionError('repeat'))
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter),'completed')
        self.adapter.remove.assert_not_called()
    def test_changed_source_is_not_removed(self):
        self.approve();self.adapter.file.write_bytes(b'changed');self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter),'partial');self.assertEqual(self.adapter.file.read_bytes(),b'changed')
    def test_preflight_backoff_and_three_attempt_cap(self):
        self.approve();self.adapter.fail=True;t=time.time()
        for offset,expected in [(0,'retry'),(1,'backoff'),(61,'retry'),(362,'blocked'),(1000,'blocked')]:self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=lambda:t+offset),expected)
        self.assertTrue(self.adapter.file.exists())
    def test_manifest_change_or_other_chat_no_authority(self):
        q=self.query();q['from']['id']=18
        with self.assertRaises(ValueError):c.decide(q,self.config,self.ledger)
        self.approve();frozen=self.ledger.directory/'proposals'/(self.row['nonce']+'.json');p=json.loads(frozen.read_text());p['risk']='changed';frozen.write_text(json.dumps(p))
        with self.assertRaisesRegex(ValueError,'proposal_changed'):e.execute(self.row['nonce'],self.ledger,self.adapter)
        self.assertTrue(self.adapter.file.exists())
    def test_explicit_notification_failure_retries_but_ambiguity_does_not(self):
        c.enqueue(self.ledger,self.row,'event','test');fail=Mock(side_effect=TelegramRejected('not accepted'));c.flush(self.ledger,self.config,fail)
        box=Ledger(self.ledger.directory/'receipts',validator=c._receipt);record=box.read(self.row['nonce']);self.assertEqual(record['events']['event']['state'],'pending');record['events']['event']['next_at']=0;box.write(record)
        c.flush(self.ledger,self.config,Mock(side_effect=RuntimeError('ambiguous')));self.assertEqual(box.read(self.row['nonce'])['events']['event']['state'],'uncertain')
    def test_resident_rejects_old_scopes_and_traversal(self):
        altered=copy.deepcopy(self.proposal);altered['delete_candidates'][0]['files'][0]['relative_path']='../escape.xls';self.path.write_text(json.dumps(altered))
        with self.assertRaises(ValueError):c.read_proposal(self.path)
        altered=copy.deepcopy(self.proposal);del altered['schema'];del altered['executor'];self.path.write_text(json.dumps(altered))
        with self.assertRaisesRegex(ValueError,'point_requires'):c.read_proposal(self.path)

if __name__=='__main__':unittest.main()
