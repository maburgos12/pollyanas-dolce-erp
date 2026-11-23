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
        health = patch.object(c, 'require_recovery', create=True)
        health.start()
        self.addCleanup(health.stop)
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
            (backups/'backup_20261117_020000.manifest').write_bytes(b'new point')
            with self.assertRaisesRegex(c.RecoveryPending,'latest_backup_not_verified'):
                adapter.preflight(self.item)
    def test_pending_rejected_do_not_delete(self):
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time),'not_approved');c.decide(self.query('r'),self.config,self.ledger);self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time),'not_approved');self.assertTrue(self.adapter.file.exists())
    def test_approve_executes_once_and_keeps_decision(self):
        self.approve();self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time),'completed');self.assertFalse(self.adapter.file.exists());self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time),'completed');self.assertEqual(self.ledger.read(self.row['nonce'])['state'],'approved')
        with self.assertRaises(c.RecordedDecision) as ctx:c.decide(self.query(),self.config,self.ledger)
        self.assertEqual(ctx.exception.state,'completed')
    def test_expiry_during_verify_stops_before_remove(self):
        self.approve();t=[time.time()];self.adapter.verify=lambda entry: (t.__setitem__(0,self.row['expires']+1) or True)
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=lambda:t[0]),'partial');self.assertTrue(self.adapter.file.exists())
    def test_restart_after_unlink_reconciles_without_second_remove(self):
        self.approve();self.adapter.kill=True
        with self.assertRaises(KeyboardInterrupt):e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time)
        self.adapter.remove=Mock(side_effect=AssertionError('must not repeat unlink'))
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time),'completed');self.adapter.remove.assert_not_called()
    def test_restart_preflight_failure_preserves_intent_and_history(self):
        self.approve();self.adapter.kill=True
        with self.assertRaises(KeyboardInterrupt):e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time)
        original=c.execution_state(self.ledger,self.row['nonce']);self.adapter.fail=True;t=time.time()
        for offset in (0,61,362):e.execute(self.row['nonce'],self.ledger,self.adapter,now=lambda:t+offset)
        record=c.execution_state(self.ledger,self.row['nonce'])
        self.assertEqual(record['state'],'uncertain');self.assertEqual(record['intent'],original['intent']);self.assertEqual(record['before'],original['before'])
    def test_terminal_receipt_recovers_without_repeating_remove(self):
        self.approve()
        with patch.object(c,'enqueue',side_effect=OSError('disk')):
            with self.assertRaises(OSError):e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time)
        self.adapter.remove=Mock(side_effect=AssertionError('repeat'))
        self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time),'completed')
        self.adapter.remove.assert_not_called()
    def test_changed_source_is_not_removed(self):
        self.approve();self.adapter.file.write_bytes(b'changed');self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time),'partial');self.assertEqual(self.adapter.file.read_bytes(),b'changed')
    def test_preflight_backoff_and_three_attempt_cap(self):
        self.approve();self.adapter.fail=True;t=time.time()
        for offset,expected in [(0,'retry'),(1,'backoff'),(61,'retry'),(362,'blocked'),(1000,'blocked')]:self.assertEqual(e.execute(self.row['nonce'],self.ledger,self.adapter,now=lambda:t+offset),expected)
        self.assertTrue(self.adapter.file.exists())
    def test_pending_backup_never_consumes_attempts_and_resumes(self):
        self.approve()
        nonce=self.row['nonce']; t=time.time()
        with patch.object(self.adapter,'preflight',side_effect=c.RecoveryPending('latest_backup_not_verified')):
            for offset in (0,61,362,900):
                self.assertEqual(e.execute(nonce,self.ledger,self.adapter,now=lambda:t+offset),'recovery_pending')
        self.assertIsNone(c.execution_state(self.ledger,nonce))
        self.assertFalse((self.ledger.directory/'executions'/(nonce+'.attempts')).exists())
        self.assertTrue(self.adapter.file.exists())
        self.assertEqual(e.execute(nonce,self.ledger,self.adapter,now=lambda:self.row['expires']),'not_approved')
        self.assertTrue(self.adapter.file.exists())
        self.assertEqual(e.execute(nonce,self.ledger,self.adapter,now=lambda:t+901),'completed')

    def test_expired_preflight_block_can_request_exact_batch_once(self):
        self.approve()
        nonce=self.row['nonce']; frozen=self.ledger.directory/'proposals'/(nonce+'.json')
        path=e.execution_path(self.ledger,nonce)
        e.atomic_json(path,dict(nonce=nonce,proposal_sha256=self.row['proposal_sha256'],
                               state='blocked',reason='preflight_attempt_limit',removed=[],skipped=[]))
        original=path.read_bytes()
        with self.assertRaisesRegex(ValueError,'replacement_not_allowed'):
            c._request(frozen,self.ledger,self.config,self.send,replaces=nonce)
        with patch('time.time',return_value=self.row['expires']+8*3600), patch.object(c,'approval_window',return_value=(True,None)):
            self.path.write_text(json.dumps({**self.proposal,'risk':'changed'}))
            with self.assertRaisesRegex(ValueError,'proposal_changed'):
                c._request(self.path,self.ledger,self.config,self.send,replaces=nonce)
            adapter=Mock(deploy_lock=self.adapter.deploy_lock)
            adapter.preflight.side_effect=c.RecoveryPending('latest_backup_not_verified')
            with patch.object(e,'HostAdapter',return_value=adapter), patch.object(c,'_request',wraps=c._request) as request:
                with self.assertRaises(c.RecoveryPending):
                    e.reissue_expired(nonce,self.ledger,self.config)
                request.assert_not_called()
            new=c._request(frozen,self.ledger,self.config,self.send,replaces=nonce)
            with self.assertRaisesRegex(ValueError,'existing_request_requires_resolution'):
                c._request(frozen,self.ledger,self.config,self.send,replaces=nonce)
        self.assertNotEqual(new['nonce'],nonce)
        self.assertEqual(new['proposal_sha256'],self.row['proposal_sha256'])
        self.assertEqual(new['state'],'pending')
        self.assertEqual(path.read_bytes(),original)
    def test_manifest_change_or_other_chat_no_authority(self):
        q=self.query();q['from']['id']=18
        with self.assertRaises(ValueError):c.decide(q,self.config,self.ledger)
        self.approve();frozen=self.ledger.directory/'proposals'/(self.row['nonce']+'.json');p=json.loads(frozen.read_text());p['risk']='changed';frozen.write_text(json.dumps(p))
        with self.assertRaisesRegex(ValueError,'proposal_changed'):e.execute(self.row['nonce'],self.ledger,self.adapter,now=time.time)
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

class HostPreservationTests(unittest.TestCase):
    def state(self, publisher='inactive'):
        return dict(monitors='active\nactive\n'+publisher, http='200',
                    containers={'erp':'unchanged'}, images=[], volumes=[], backups={})

    def test_paused_publisher_is_preserved_without_reactivation(self):
        before=self.state()
        e.HostAdapter.verify_preserved(before,copy.deepcopy(before))
        for changed in ('active','failed','activating'):
            after=self.state(changed)
            with self.assertRaises(ValueError):e.HostAdapter.verify_preserved(before,after)

    def test_recovery_and_receiver_must_stay_active(self):
        for states in ('inactive\nactive\ninactive','active\ninactive\ninactive','active\nactive\nfailed'):
            before=self.state();before['monitors']=states
            with self.assertRaises(ValueError):e.HostAdapter.verify_preserved(before,before)

    def test_measure_records_paused_publisher_via_show(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); real_path=Path
            adapter=object.__new__(e.HostAdapter)
            def command(*args):
                if args[:2]==('systemctl','show'):
                    return 'inactive' if args[-1]=='erp-telegram-release.timer' else 'active'
                if args[:2]==('systemctl','is-active'):raise AssertionError('inactive is not command failure')
                if args[0]=='df':return 'Filesystem blocks used available capacity mounted\nx 100 80 20 80% /'
                if args[0]=='curl':return '200'
                return ''
            adapter.command=Mock(side_effect=command)
            with patch.object(e,'Path',side_effect=lambda value:root if value=='/opt/backups/erp' else real_path(value)):
                self.assertEqual(adapter.measure()['monitors'],'active\nactive\ninactive')

if __name__=='__main__':unittest.main()
