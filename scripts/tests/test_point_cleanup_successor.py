"""A new exact decision can succeed a zero-retirement preflight block; never reset it."""
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ops/erp-telegram-release'))
import erp_telegram_cleanup as c
import erp_telegram_release as r
import erp_cleanup_prepare as prep
import erp_cleanup_execute as execute
from scripts.tests.test_cleanup_execute import Adapter

class SuccessorTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.root=Path(self.tmp.name);self.ledger=r.Ledger(self.root/'cleanups',validator=c.validate)
  clock=patch.object(r,'approval_time',return_value=1800025200);self.clock=clock.start();self.addCleanup(clock.stop)
  now=patch.object(time,'time',return_value=1800025200);self.now=now.start();self.addCleanup(now.stop)
  self.recovery=patch.object(c,'require_recovery');self.recovery.start();self.addCleanup(self.recovery.stop)
  self.verify=patch.object(c,'revalidate_proposal');self.verify.start();self.addCleanup(self.verify.stop)
  self.config={'token':'test','chat_id':17}
  self.send=Mock(side_effect=lambda config,method,payload:{'message_id':42,'chat':{'id':17,'type':'private'},**({'reply_markup':payload['reply_markup']} if method=='editMessageReplyMarkup' else {})})
  name='historical.xls';digest=hashlib.sha256(b'original').hexdigest()
  entry=dict(schema=1,id=hashlib.sha256((name+'\0'+digest).encode()).hexdigest(),relative_path=name,sha256=digest,bytes=8,nas_key='point-archive/objects/'+digest+'.xls',verified_at=1,consumers=['category_report'])
  item=dict(type='point_archive_manifest',id=c.point_manifest_id([entry]),files=[entry],recovery={'files_verified':1,'verified_at':1})
  self.proposal=dict(schema=2,executor='resident-v1',id='VPS-LIM-20261123-01',host='68.183.165.47',status='awaiting_explicit_approval',execution='resident',expected_recovery='NAS',expected_usage='measured',risk='NAS required',preconditions=['fresh recovery'],preserve=['all others'],delete_candidates=[item])
  self.path=self.root/'parent.json';self.path.write_text(json.dumps(self.proposal))
  self.parent=c.request(self.path,self.ledger,self.config,self.send)
  os.chmod(self.ledger.directory/'proposals'/(self.parent['nonce']+'.json'),0o600)
  self.parent.update(state='approved',decision_at=int(time.time()));self.ledger.write(self.parent)
  self.record=dict(nonce=self.parent['nonce'],proposal_sha256=self.parent['proposal_sha256'],state='blocked',reason='preflight_attempt_limit',removed=[],skipped=[])
  self.exec=self.ledger.directory/'executions'/(self.parent['nonce']+'.json');self.attempts=self.exec.with_suffix('.attempts')
  self.exec.parent.mkdir(mode=0o700)
  prep.atomic_json(self.exec,self.record);prep.atomic_json(self.attempts,{'count':3,'next_at':1,'reason':'ValueError'})
  self.before={p:p.read_bytes() for p in [self.ledger.path(self.parent['nonce']),self.exec,self.attempts]}
 def successor(self):
  return c.successor_proposal(self.ledger,self.parent['nonce'],'VPS-LIM-20261123-02')
 def request(self,proposal):
  p=self.root/'child.json';p.write_text(json.dumps(proposal));return c.request(p,self.ledger,self.config,self.send)
 def test_new_exact_approval_and_completed_resolves_only_ancestor(self):
  proposal=self.successor();row=self.request(proposal)
  self.assertNotEqual(row['nonce'],self.parent['nonce']);self.assertEqual(row['state'],'pending')
  self.assertFalse(c.predecessor_resolved(self.ledger,self.parent))
  self.assertEqual(proposal['delete_candidates'],self.proposal['delete_candidates'])
  row.update(state='approved',decision_at=int(time.time()));self.ledger.write(row)
  done=dict(nonce=row['nonce'],proposal_sha256=row['proposal_sha256'],state='completed',removed=[self.proposal['delete_candidates'][0]['files'][0]['id']],skipped=[],intent=None)
  prep.atomic_json(self.ledger.directory/'executions'/(row['nonce']+'.json'),done)
  self.assertTrue(c.predecessor_resolved(self.ledger,self.parent))
  self.assertFalse(prep.blocked(self.ledger,None,int(time.time())))
  done['state']='uncertain';prep.atomic_json(self.ledger.directory/'executions'/(row['nonce']+'.json'),done)
  self.assertTrue(prep.blocked(self.ledger,None,int(time.time())))
  for p,raw in self.before.items():self.assertEqual(p.read_bytes(),raw)
 def test_rejects_unsafe_parent_and_tampered_evidence(self):
  for delta in [dict(state='partial'),dict(state='uncertain'),dict(state='executing'),dict(reason='other'),dict(removed=['x']),dict(skipped=['x']),dict(intent='x')]:
   prep.atomic_json(self.exec,{**self.record,**delta})
   with self.subTest(delta=delta),self.assertRaises(ValueError):self.successor()
  prep.atomic_json(self.exec,self.record)
  proposal=self.successor();prep.atomic_json(self.attempts,{'count':2,'next_at':1,'reason':'ValueError'})
  with self.assertRaises(ValueError):self.request(proposal)
 def test_duplicate_or_changed_scope_never_gets_buttons(self):
  proposal=self.successor();self.request(proposal);self.send.reset_mock()
  with self.assertRaises(ValueError):self.request(proposal)
  self.send.assert_not_called()
  altered=copy.deepcopy(proposal);altered['id']='VPS-LIM-20261123-03';altered['delete_candidates'][0]['files'][0]['bytes']=13
  altered['delete_candidates'][0]['id']=c.point_manifest_id(altered['delete_candidates'][0]['files'])
  with self.assertRaises(ValueError):self.request(altered)
 def test_unrelated_block_still_prevents_continuity(self):
  row=dict(self.parent,nonce='b'*24,image_id='sha256:'+'b'*64);self.ledger.write(row)
  prep.atomic_json(self.ledger.directory/'executions'/(row['nonce']+'.json'),dict(self.record,nonce=row['nonce']))
  self.assertTrue(prep.blocked(self.ledger,None,int(time.time())))
  self.send.reset_mock()
  with self.assertRaisesRegex(ValueError,'unresolved_other_execution'):self.request(self.successor())
  self.send.assert_not_called()
 def test_native_executor_requires_new_signed_decision_and_keeps_parent(self):
  proposal=self.successor();row=self.request(proposal)
  entry=proposal['delete_candidates'][0]['files'][0];adapter=Adapter(self.root,entry)
  self.assertEqual(execute.execute(row['nonce'],self.ledger,adapter,now=time.time),'not_approved')
  self.assertTrue(adapter.file.exists())
  query=dict(id='test',data=c.signed(self.config,self.parent,'a'),**{'from':{'id':17}},message={'message_id':42,'chat':{'id':17,'type':'private'}})
  with self.assertRaises(c.RecordedDecision):c.decide(query,self.config,self.ledger)
  self.assertEqual(self.ledger.read(row['nonce'])['state'],'pending')
  query['data']=c.signed(self.config,row,'a');c.decide(query,self.config,self.ledger)
  self.assertFalse(c.status(self.root/'child.json',row['nonce'],self.ledger,now=row['expires'])['approved'])
  self.assertEqual(execute.execute(row['nonce'],self.ledger,adapter,now=time.time),'completed')
  self.assertFalse(adapter.file.exists());self.assertTrue(c.predecessor_resolved(self.ledger,self.parent))
  self.assertEqual(c.execution_state(self.ledger,row['nonce'])['freed_bytes'],8)
  for p,raw in self.before.items():self.assertEqual(p.read_bytes(),raw)
 def test_parent_changed_after_new_approval_blocks_before_remove(self):
  row=self.request(self.successor());row.update(state='approved',decision_at=int(time.time()));self.ledger.write(row)
  prep.atomic_json(self.attempts,{'count':3,'next_at':2,'reason':'ValueError'})
  adapter=Adapter(self.root,self.proposal['delete_candidates'][0]['files'][0])
  with self.assertRaises(ValueError):execute.execute(row['nonce'],self.ledger,adapter,now=time.time)
  self.assertTrue(adapter.file.exists());self.assertIsNone(c.execution_state(self.ledger,row['nonce']))
 def test_expired_successor_cannot_renew_the_next_day(self):
  proposal=self.successor();row=self.request(proposal)
  before={p.name:p.read_bytes() for p in self.ledger.directory.glob('*.json')}
  self.send.reset_mock();self.now.return_value+=86400;self.clock.return_value+=86400
  with self.assertRaisesRegex(ValueError,'existing_successor'):self.request(proposal)
  self.send.assert_not_called()
  self.assertEqual({p.name:p.read_bytes() for p in self.ledger.directory.glob('*.json')},before)
  self.assertEqual(self.ledger.read(row['nonce'])['state'],'pending')
  for p,raw in self.before.items():self.assertEqual(p.read_bytes(),raw)
