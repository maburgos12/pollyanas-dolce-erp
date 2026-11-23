"""Host-only Point archive preparation and bounded recovery of registered XLS sources."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from pos_bridge.services.archive_access import ArchiveStore, entry_id, validate_entry
import erp_cleanup_prepare as prep
import erp_telegram_cleanup as cleanup
from erp_telegram_release import Ledger, telegram, approval_window

APP=Path('/opt/pastelerias-erp')
STATE=Path('/var/lib/erp-point-archive')
MAX_PILOT_FILES=100
MAX_PILOT_BYTES=100_000_000
SESSION_REPLY_SECONDS=95


class InventorySession:
    """One bounded child; every request still runs fresh native integrity checks."""
    def __init__(self):
        self.proc=None;self.failed=False;self.count=0;self.started=0;self.stderr_bytes=0

    @staticmethod
    def argv():
        boot=("import os,signal;signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.alarm(85);"
              "os.environ.setdefault('DJANGO_SETTINGS_MODULE','config.settings');"
              "from django.core.management import execute_from_command_line;"
              "execute_from_command_line(['manage.py','point_archive_inventory','--stream'])")
        return ['timeout','--signal=KILL','910','docker','exec','-i','pastelerias-erp-worker-1',
                'timeout','--signal=KILL','900','python','-B','-c',boot]

    def request(self,args):
        if self.failed:
            raise RuntimeError('inventory_session_failed')
        try:
            if self.proc is not None and (self.count>=480 or time.monotonic()-self.started>=700):
                self.close()
            if self.proc is None:
                self.proc=subprocess.Popen(self.argv(),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                self.count=0;self.started=time.monotonic();self.stderr_bytes=0
            if self.proc.poll() is not None:
                raise RuntimeError('inventory_session_exited')
            request=json.dumps({'id':self.count+1,'args':list(args)},separators=(',',':')).encode()+b'\n'
            if len(request)>4096:
                raise ValueError('inventory_request_limit')
            self.proc.stdin.write(request);self.proc.stdin.flush()
            deadline=time.monotonic()+SESSION_REPLY_SECONDS;output=bytearray()
            with selectors.DefaultSelector() as selector:
                selector.register(self.proc.stdout,selectors.EVENT_READ,'stdout')
                selector.register(self.proc.stderr,selectors.EVENT_READ,'stderr')
                while True:
                    remaining=deadline-time.monotonic()
                    if remaining<=0:
                        raise TimeoutError('inventory_reply_timeout')
                    for key,_ in selector.select(remaining):
                        data=os.read(key.fileobj.fileno(),65536)
                        if key.data=='stderr':
                            self.stderr_bytes+=len(data)
                            if self.stderr_bytes>16384:
                                raise ValueError('inventory_stderr_limit')
                            if not data:selector.unregister(key.fileobj)
                        else:
                            if not data:
                                raise RuntimeError('inventory_reply_eof')
                            output.extend(data)
                            if len(output)>1048576:
                                raise ValueError('inventory_reply_limit')
                    if b'\n' in output:
                        line,extra=output.split(b'\n',1)
                        if extra:
                            raise ValueError('inventory_extra_output')
                        response=json.loads(line)
                        if (not isinstance(response,dict) or set(response)!={'id','result'}
                                or type(response['id']) is not int or response['id']!=self.count+1
                                or not isinstance(response['result'],dict)):
                            raise ValueError('inventory_reply_invalid')
                        self.count+=1
                        return response['result']
        except BaseException as exc:
            self.failed=True
            proc=self.proc
            self.close()
            if (isinstance(exc,RuntimeError) and str(exc) in {'inventory_reply_eof','inventory_session_exited'}
                    and proc is not None and proc.returncode in (-signal.SIGALRM,128+signal.SIGALRM)):
                raise subprocess.TimeoutExpired('point_archive_inventory',85) from None
            raise

    def close(self):
        proc=self.proc
        if proc is None:return
        self.proc=None
        try:
            try:proc.stdin.close()
            except BrokenPipeError:pass
            try:
                proc.wait(timeout=95)
            except subprocess.TimeoutExpired:
                proc.kill();proc.wait(timeout=5)
                self.failed=True
                raise RuntimeError('inventory_remote_closure_unknown')
            if proc.returncode!=0 and not self.failed:
                self.failed=True
                raise RuntimeError('inventory_session_exit_failed')
        finally:
            for pipe in (proc.stdin,proc.stdout,proc.stderr):
                pipe.close()


def failure_detail(exc, stage):
    """Persist only allowlisted diagnostics; stderr may contain credentials or data."""
    raw = getattr(exc, 'stderr', '') or ''
    if isinstance(raw, bytes):
        raw = raw.decode('utf-8', errors='replace')
    raw = raw[:8192]
    code = getattr(exc, 'returncode', None)
    reason = 'command_failed' if isinstance(exc, subprocess.CalledProcessError) else type(exc).__name__
    transient = isinstance(exc, (subprocess.TimeoutExpired,TimeoutError)) or code == 124
    if transient:
        reason = 'command_timeout'
    elif isinstance(exc, subprocess.CalledProcessError):
        for marker, label in (
            ('canceling statement due to statement timeout', 'database_timeout'),
            ('is not running', 'worker_unavailable'),
            ('max connections (2) reached', 'nas_busy'),
            ('Connection timed out', 'connection_timeout'),
            ('Connection reset by peer', 'connection_reset'),
        ):
            if marker in raw:
                reason, transient = label, True
                break
    # A lost Telegram response must not cause an automatic duplicate request.
    if stage == 'request':
        transient = False
    return dict(stage=stage, error=type(exc).__name__, returncode=code,
                reason=reason, transient=transient,
                stderr_sha256=hashlib.sha256(raw.encode()).hexdigest() if raw else None)


class ObjectNAS(prep.NAS):
    def path_for(self,entry):
        validate_entry(entry)
        return self.guarded_path(entry['sha256'])

    def guarded_path(self,digest):
        if not re.fullmatch('[0-9a-f]{64}',digest):
            raise ValueError('invalid_object_digest')
        return prep.NAS_ROOT+'/point-archive/objects/'+digest+'.xls'

    def provision(self):
        # Parent is the already authorized backup share. Never follow newly introduced links.
        parent=prep.NAS_ROOT+'/point-archive';objects=parent+'/objects'
        self.command(f'umask 077; test ! -L {parent} && mkdir -p {parent} && test ! -L {objects} && mkdir -p {objects}')

    def put(self,entry,source):
        dest=self.path_for(entry);folder=dest.rsplit('/',1)[0]
        if int(self.command('df -Pk '+folder).splitlines()[-1].split()[3])*1024 < prep.RESERVE+entry['bytes']:
            raise ValueError('nas_capacity')
        command=(f'umask 077; test ! -L {folder} && test ! -L {folder.rsplit(chr(47),1)[0]} || exit 1; tmp=$(mktemp {folder}/.incoming.XXXXXX) || exit 1; '
                 f'trap \'rm -f "$tmp"\' EXIT; cat > "$tmp" && '
                 f'test "$(sha256sum "$tmp" | cut -d " " -f1)" = {entry["sha256"]} && sync && '
                 f'if test -e {dest}; then test ! -L {dest} && test "$(sha256sum {dest} | cut -d " " -f1)" = {entry["sha256"]}; '
                 f'else mv -n "$tmp" {dest} && sync; fi')
        with prep.ssh_stream(self.guarded(command),stdin=subprocess.PIPE) as proc:
            with Path(source).open('rb') as stream:
                shutil.copyfileobj(stream,proc.stdin,1024*1024)
            proc.stdin.close()
        metadata=json.dumps({k:v for k,v in entry.items() if k!='verified_at'},sort_keys=True,separators=(',',':')).encode()
        encoded=base64.b64encode(metadata).decode();checksum=hashlib.sha256(metadata).hexdigest()
        index=dest+'.'+entry['id']+'.json'
        self.command(f'umask 077; test ! -L {folder} && test ! -L {folder.rsplit(chr(47),1)[0]} || exit 1; '
                     f'tmp=$(mktemp {folder}/.index.XXXXXX) || exit 1; trap \'rm -f "$tmp"\' EXIT; '
                     f'printf %s {encoded} | base64 -d > "$tmp"; '
                     f'test "$(sha256sum "$tmp" | cut -d " " -f1)" = {checksum} && sync && '
                     f'if test -e {index}; then test ! -L {index} && test "$(sha256sum {index} | cut -d " " -f1)" = {checksum}; '
                     f'else mv -n "$tmp" {index} && sync; fi')
        self.verify_index(entry)

    def verify_index(self,entry):
        source=self.path_for(entry)+'.'+entry['id']+'.json'
        folder=source.rsplit('/',1)[0];parent=folder.rsplit('/',1)[0]
        value=self.command(f'test ! -L {parent} && test ! -L {folder} && test ! -L {source} && test -f {source} && head -c 8193 {source}')
        if len(value)>8192 or json.loads(value)!={k:v for k,v in entry.items() if k!='verified_at'}:
            raise ValueError('nas_index_mismatch')

    def get(self,entry,destination):
        self.verify_index(entry)
        source=self.path_for(entry)
        folder=source.rsplit('/',1)[0];parent=folder.rsplit('/',1)[0]
        digest=hashlib.sha256();copied=0
        with prep.ssh_stream(self.guarded(f'test ! -L {parent} && test ! -L {folder} && test ! -L {source} && test -f {source} && cat {source}'),stdout=subprocess.PIPE) as proc:
            with destination.open('xb') as target:
                while chunk:=proc.stdout.read(1024*1024):
                    copied+=len(chunk)
                    if copied>entry['bytes']:
                        raise ValueError('object_size_limit')
                    digest.update(chunk);target.write(chunk)
                target.flush();os.fsync(target.fileno())
        if copied!=entry['bytes'] or digest.hexdigest()!=entry['sha256']:
            raise ValueError('object_hash_mismatch')


class Broker:
    def __init__(self,store=None,nas=None,state=STATE,inventory=None,parse=None):
        self.store=store or ArchiveStore(APP/'storage/pos_bridge/archive',APP/'storage/pos_bridge/raw_exports')
        self.nas=nas or ObjectNAS();self.state=Path(state)
        self.inventory=inventory or self._inventory;self.parse=parse or self._parse
        self.session=None

    def __enter__(self):
        return self

    def __exit__(self,*exc):
        self.close()

    def close(self):
        if self.session is not None:self.session.close()

    def command(self,*args):
        if self.session is None:self.session=InventorySession()
        return self.session.request(args)

    def _inventory(self,name=None,after_id=0):
        return self.command('--name',name) if name else self.command('--after-id',str(after_id))

    def _parse(self,path):
        relative=Path(path).relative_to(APP)
        return self.command('--parse','/app/'+str(relative))

    def matches_original(self,entry):
        try:
            return self.store.snapshot(self.store.sources_root/entry['relative_path'])=={k:entry[k] for k in ('sha256','bytes')}
        except (OSError,RuntimeError):
            return False

    def eligible(self,entry):
        result=self.inventory(name=entry['relative_path'])
        return any(x['sha256']==entry['sha256'] and (x['bytes'] is None or x['bytes']==entry['bytes']) for x in result['entries'])

    def staging(self):
        path=self.store.root/'staging';path.mkdir(mode=0o755,parents=True,exist_ok=True)
        if path.is_symlink() or shutil.disk_usage(path).free<prep.RESERVE+2*MAX_PILOT_BYTES:
            raise ValueError('recovery_capacity')
        return path

    def readback(self,entry,expected_semantic=None):
        # Staging is on the existing shared ERP bind, read-only to the parser operation.
        root=self.staging();path=root/(os.urandom(16).hex()+'.xls')
        try:
            self.nas.get(entry,path);path.chmod(0o644)
            if self.store.snapshot(path)!={k:entry[k] for k in ('sha256','bytes')}:
                raise ValueError('readback_hash')
            semantic=self.parse(path)
            if expected_semantic and semantic!=expected_semantic:
                raise ValueError('semantic_recovery_mismatch')
            return semantic
        finally:
            path.unlink(missing_ok=True)

    def prepare_entry(self,candidate):
        entry=dict(schema=1,id=entry_id(candidate['relative_path'],candidate['sha256']),relative_path=candidate['relative_path'],sha256=candidate['sha256'],bytes=candidate['bytes'],nas_key='point-archive/objects/'+candidate['sha256']+'.xls',verified_at=int(time.time()),consumers=['category_report'])
        validate_entry(entry)
        semantic={k:candidate[k] for k in ('semantic_sha256','rows')}
        with self.store.path_lock(entry['relative_path'],exclusive=True) as fd:
            # Keep the same inode: archived locks cannot be removed by an app-owned directory entry.
            if os.geteuid()==0:
                os.fchown(fd,0,0)
            os.fchmod(fd,0o666)
            if not self.matches_original(entry):
                raise ValueError('original_changed')
            self.nas.put(entry,self.store.sources_root/entry['relative_path'])
            self.readback(entry,semantic)
            if not self.matches_original(entry):
                raise ValueError('original_changed')
            self.store.register(entry)
        return entry

    def verify_batch(self,entries):
        for entry in entries:
            if self.store.catalog_entry(entry_id=entry['id'])!=entry or not self.eligible(entry):
                raise ValueError('point_scope_changed')
            self.readback(entry)

    def recover_requests(self,now=None):
        now=int(time.time()) if now is None else now
        results=[];attempts_path=self.state/'recovery-attempts.json'
        attempts=json.loads(attempts_path.read_text()) if attempts_path.exists() else {}
        for request in sorted((self.store.root/'requests').glob('*.json'))[:10]:
            if not re.fullmatch('[0-9a-f]{64}',request.stem) or request.is_symlink():
                continue
            item=attempts.get(request.stem,{'count':0,'next_at':0})
            if item['count']>=3:
                results.append({'id':request.stem,'state':'blocked'})
                continue
            if item['next_at']>now:
                continue
            path=None
            try:
                entry=self.store.catalog_entry(entry_id=request.stem)
                if self.store._read_json(request)!={'id':entry['id']}:
                    raise ValueError('invalid_request')
                path=self.staging()/(os.urandom(16).hex()+'.xls')
                self.nas.get(entry,path)
                self.store.publish_restored(entry,path)
                request.unlink()
                attempts.pop(request.stem,None);results.append({'id':entry['id'],'state':'recovered'})
            except Exception as exc:
                count=item['count']+1
                attempts[request.stem]={'count':count,'next_at':now+[60,300,900][count-1],'error':type(exc).__name__}
                results.append({'id':request.stem,'state':'blocked' if count>=3 else 'retry'})
            finally:
                if path is not None:
                    path.unlink(missing_ok=True)
                prep.atomic_json(attempts_path,attempts)
        return results

    def prepare(self,ledger,config,send=telegram):
        if not callable(getattr(cleanup,'require_recovery',None)) or not hasattr(cleanup,'RecoveryPending'):
            return 'activation_pending'
        if not approval_window()[0]:
            return 'outside_approval_hours'
        self.state.mkdir(mode=0o700,parents=True,exist_ok=True)
        state_path=self.state/'status.json';status=json.loads(state_path.read_text()) if state_path.exists() else {}
        if prep.disk_percent()<=75 or status.get('next_at',0)>time.time() or prep.blocked(ledger,None,int(time.time())):
            return 'waiting'
        if status.get('failures',0)>=3:
            return 'attempt_limit'
        entries=[];total=0
        stage='recovery_gate'
        try:
            cleanup.require_recovery()
            stage='inventory'
            scan=self.inventory(after_id=status.get('cursor',0))
            stage='nas_provision'
            self.nas.provision()
            catalog=list((self.store.root/'catalog/objects').glob('*.json'))
            decisions={}
            for file in ledger.directory.glob('*.json'):
                row=ledger.read(file.stem);frozen=ledger.directory/'proposals'/file.name
                if not frozen.exists():
                    continue
                previous,_=cleanup.read_proposal(frozen)
                if previous['delete_candidates'][0]['type']!='point_archive_manifest':
                    continue
                for old_entry in previous['delete_candidates'][0]['files']:
                    decisions.setdefault(old_entry['id'],[]).append(row)
            reserved = set()
            queued = Ledger(ledger.directory/'scheduled', validator=cleanup.validate_intent)
            for pending in queued.directory.glob('*.json'):
                intent = queued.read(pending.stem)
                if intent['kind'] != 'cleanup':
                    continue
                frozen = ledger.directory/'scheduled-proposals'/(intent['binding']['digest']+'.json')
                previous, digest = cleanup.read_proposal(frozen)
                if digest != intent['binding']['digest']:
                    raise ValueError('scheduled_proposal_changed')
                old_item = previous['delete_candidates'][0]
                if old_item['type'] == 'point_archive_manifest':
                    reserved.update(entry['id'] for entry in old_item['files'])
            stage='verify_catalog'
            for file in sorted(catalog):
                entry=self.store.catalog_entry(entry_id=file.stem)
                prior=decisions.get(entry['id'],[])
                # The shared daily scheduler renews exact undecided lots; never invent another scope.
                if prior or entry['id'] in reserved:
                    continue
                if len(entries)>=MAX_PILOT_FILES or total+entry['bytes']>MAX_PILOT_BYTES:
                    break
                if self.matches_original(entry) and self.eligible(entry):
                    self.readback(entry)
                    entries.append(entry);total+=entry['bytes']
            next_cursor=scan['next_id'] if scan['has_more'] else 0
            for candidate in scan['entries']:
                if len(entries)>=MAX_PILOT_FILES or total+candidate['bytes']>MAX_PILOT_BYTES:
                    next_cursor=candidate['task_id']-1
                    break
                try:
                    self.store.catalog_entry(relative_path=candidate['relative_path'])
                    continue
                except FileNotFoundError:
                    pass
                stage='copy_and_readback'
                entry=self.prepare_entry(candidate)
                entries.append(entry);total+=candidate['bytes'];catalog.append(entry)
            status.update(cursor=next_cursor,next_at=int(time.time())+300)
            prep.atomic_json(state_path,status)
            if not entries:
                return 'no_eligible_point_sources'
            day=time.strftime('%Y%m%d',time.gmtime());known={ledger.read(p.stem)['proposal_id'] for p in ledger.directory.glob('*.json')}
            proposal_id=next((f'VPS-LIM-{day}-{i:02d}' for i in range(1,100) if f'VPS-LIM-{day}-{i:02d}' not in known and not (self.state/f'VPS-LIM-{day}-{i:02d}.proposal').exists()),None)
            if proposal_id is None:
                raise ValueError('daily_id_limit')
            item=dict(type='point_archive_manifest',id=cleanup.point_manifest_id(entries),files=entries,recovery=dict(files_verified=len(entries),verified_at=int(time.time())))
            proposal=dict(schema=2,executor='resident-v1',id=proposal_id,host='68.183.165.47',status='awaiting_explicit_approval',delete_candidates=[item],execution='Retiro residente tras aprobacion vigente del lote exacto.',expected_recovery='Copia NAS leida de regreso, hash y parser equivalentes.',expected_usage='Ahorro real por medir; no garantiza bajar de 75%.',risk='NAS necesario para recuperar fuentes que no esten locales; fallo no equivale a reporte vacio.',preconditions=['Revalidar evidencia, referencias, lease y vigencia antes de retirar.'],preserve=['Todos los respaldos, bases, ventas, servicios y fuentes ajenas al lote.'])
            path=self.state/(proposal_id+'.proposal');prep.atomic_json(path,proposal)
            cleanup.read_proposal(path)
            if prep.blocked(ledger,None,int(time.time())):
                raise ValueError('concurrent_request')
            stage='request'
            row=cleanup.request(path,ledger,config,send)
            status.update(proposal=proposal_id,nonce=row['nonce']);prep.atomic_json(state_path,status)
            return 'deferred' if row['state']=='deferred' else 'requested'
        except cleanup.RecoveryPending:
            return 'recovery_pending'
        except Exception as exc:
            detail=failure_detail(exc,stage)
            count=status.get('failures',0)+1
            delay=(60 if count==1 else 300) if detail['transient'] and count<3 else 21600
            detail.update(at=int(time.time()),attempt=count,cursor=status.get('cursor',0))
            history=status.get('failure_history',[])
            history.append(detail)
            status.update(next_at=int(time.time())+delay,error=type(exc).__name__,failures=count,
                          failure_history=history[-3:],last_failure=detail)
            prep.atomic_json(state_path,status)
            followup=('Límite de tres fallas: requiere revisión del operador.' if count>=3 else
                      f'Reintento automático en {delay//60} minutos.' if detail['transient'] else
                      'No se aplicará una reparación a ciegas; revisión del operador en 6 horas.')
            send(config,'sendMessage',{'chat_id':config['chat_id'],'text':
                 f'Preparación Point bloqueada en {stage}: {detail["reason"]} '
                 f'(salida {detail["returncode"]}, intento {count}/3). Originales conservados. '+followup})
            return 'blocked'


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--once',action='store_true',required=True);parser.parse_args()
    if os.geteuid()!=0:
        parser.error('root required')
    os.umask(0o077);STATE.mkdir(mode=0o700,parents=True,exist_ok=True)
    config=json.loads((Path(os.environ['CREDENTIALS_DIRECTORY'])/'telegram.json').read_text());ledger=Ledger(cleanup.STATE,validator=cleanup.validate)
    with Ledger(STATE).lock(worker=True), Broker() as broker:
        recoveries=broker.recover_requests()
        for result in recoveries:
            if result['state']=='blocked':
                cleanup.enqueue(ledger,{'nonce':result['id'][:24],'proposal_id':'Recuperación Point '+result['id'][:12]},'recovery-blocked','Tres intentos fallaron. La fuente sigue pendiente; revisar NAS y espacio. No se devolverán datos vacíos.')
        cleanup.flush(ledger,config)
        print(json.dumps({'recoveries':recoveries,'prepare':broker.prepare(ledger,config)}))


if __name__=='__main__':
    main()
