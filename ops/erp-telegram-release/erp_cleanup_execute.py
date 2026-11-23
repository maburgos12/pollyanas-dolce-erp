"""Execute only new resident-v1 proposals with a current authenticated decision."""
import argparse
from contextlib import contextmanager
import fcntl
import json
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import time

import erp_telegram_cleanup as cleanup
from erp_telegram_release import Ledger, telegram
from erp_cleanup_prepare import atomic_json


def execution_path(ledger, nonce):
    ledger.path(nonce)  # Strict nonce validation before constructing paths.
    root = ledger.directory / 'executions'
    root.mkdir(mode=0o700, exist_ok=True)
    return root / (nonce + '.json')


@contextmanager
def deployment_lock(path=Path('/run/lock/erp-deploy.lock')):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def execute(nonce, ledger, adapter, now=time.time):
    """Adapter owns bounded recovery/leases. Decisions and execution evidence stay separate."""
    proposal_path = ledger.directory / 'proposals' / (nonce + '.json')
    proposal, digest = cleanup.read_proposal(proposal_path)
    if proposal.get('schema') != 2 or proposal.get('executor') != 'resident-v1':
        return 'legacy_manual_only'
    item = proposal['delete_candidates'][0]
    record_path = execution_path(ledger, nonce)
    with deployment_lock(adapter.deploy_lock), ledger.lock(worker=True):
        old = cleanup.execution_state(ledger, nonce)
        if old and old['state'] != 'executing':
            notify_result(ledger, ledger.read(nonce), old)
            return old['state']
        approved = cleanup.status(proposal_path, nonce, ledger, now=int(now()))
        if not approved['approved'] and not old:
            return 'not_approved'
        retry_path = record_path.with_suffix('.attempts')
        retry = json.loads(retry_path.read_text()) if retry_path.exists() else {'count':0,'next_at':0}
        if retry['next_at']>now():
            return 'backoff'
        try:
            adapter.preflight(item)
            before = adapter.measure()
            adapter.verify_preserved(before,before)
        except cleanup.RecoveryPending:
            return 'recovery_pending'
        except Exception as exc:
            retry.update(count=retry['count']+1,next_at=int(now())+[60,300,900][min(retry['count'],2)],reason=type(exc).__name__)
            atomic_json(retry_path,retry)
            if retry['count']>=3:
                record = dict(old) if old else dict(nonce=nonce,proposal_sha256=digest,removed=[],skipped=[])
                record.update(state='uncertain' if old else 'blocked',reason='preflight_attempt_limit')
                atomic_json(record_path,record)
                notify_result(ledger, ledger.read(nonce), record)
                return record['state']
            return 'retry'
        with ledger.lock():
            row = ledger.read(nonce)
            if not old and (row['state'] != 'approved' or row['expires'] <= now() or row['proposal_sha256'] != digest
                    or row['image_id'] != item['id'] or row['proposal_id'] != proposal['id']):
                return 'not_approved'
            if old:
                record=old
                adapter.verify_preserved(record['before'],before)
                if record['intent']:
                    entry=next(e for e in item['files'] if adapter.identity(e)==record['intent'])
                    with adapter.lease(entry):
                        state=adapter.reconcile(entry)
                    if state=='different':
                        record.update(state='uncertain',reason='intent_source_changed')
                        atomic_json(record_path,record)
                        return 'uncertain'
                    if state=='absent' and record['intent'] not in record['removed']:
                        record['removed'].append(record['intent'])
                    record['intent']=None
                before=record['before']
            else:
                record = dict(nonce=nonce, proposal_sha256=digest, state='executing', started_at=int(now()),
                              before=before, removed=[], skipped=[], intent=None)
            atomic_json(record_path, record)
        try:
            cleanup.enqueue(ledger, row, 'execution-started', 'Autorización confirmada. Comienza el lote exacto; ya no admite revocación durante esta ejecución.')
        except Exception as exc:
            record['notification_error']=type(exc).__name__
            atomic_json(record_path,record)
        try:
            for entry in item['files']:
                if adapter.identity(entry) in record['removed'] or adapter.identity(entry) in record['skipped']:
                    continue
                if now() >= row['expires']:
                    record['reason'] = 'authorization_expired'
                    break
                with adapter.lease(entry):
                    if not adapter.verify(entry):
                        record['skipped'].append(adapter.identity(entry))
                        atomic_json(record_path, record)
                        continue
                    if now() >= row['expires']:
                        record['reason'] = 'authorization_expired'
                        break
                    record['intent'] = adapter.identity(entry)
                    atomic_json(record_path, record)
                    adapter.remove(entry)
                    record['removed'].append(record['intent'])
                    record['intent'] = None
                    atomic_json(record_path, record)
            after = adapter.measure()
            adapter.verify_preserved(before, after)
            record.update(after=after, finished_at=int(now()), state='completed' if len(record['removed']) == len(item['files']) else 'partial',
                          freed_bytes=after['free_bytes'] - before['free_bytes'])
        except Exception as exc:
            record.update(state='uncertain' if record['intent'] else 'partial' if record['removed'] else 'blocked',
                          reason=type(exc).__name__, finished_at=int(now()))
        atomic_json(record_path, record)
        notify_result(ledger, row, record)
        return record['state']


def notify_result(ledger, row, record):
    message = {'completed': 'Limpieza terminada.', 'partial': 'Limpieza parcial; los archivos restantes se conservan.',
               'blocked': 'Limpieza bloqueada antes del retiro; revisar la evidencia.',
               'uncertain': 'Ejecución interrumpida; se conserva el registro de retiros e intención pendiente. Requiere revisión.'}[record['state']]
    if 'after' in record:
        message += ' Espacio neto recuperado: %.3f GB. Disco: %s%%. Respaldos y servicios conservados.' % (record['freed_bytes'] / 1e9, record['after']['percent'])
    cleanup.enqueue(ledger, row, 'execution-result', message)


class HostAdapter:
    deploy_lock = Path('/run/lock/erp-deploy.lock')

    def __init__(self, item):
        self.item = item
        if item['type'] != 'point_archive_manifest':
            raise ValueError('unsupported_resident_scope')
        from erp_point_archive import Broker
        self.point = Broker()

    @staticmethod
    def command(*args):
        return subprocess.check_output(args, text=True, timeout=30).strip()

    def preflight(self, item):
        health = json.loads(Path('/var/lib/erp-backup-restore/health-latest.json').read_text())
        if not health.get('success') or not 0 <= time.time() - health['finished_epoch'] < 36 * 3600:
            raise cleanup.RecoveryPending('backup_recovery_not_current')
        latest = max(p.name[:-9] for p in Path('/opt/backups/erp').glob('backup_*.manifest'))
        if latest != health['retained']:
            raise cleanup.RecoveryPending('latest_backup_not_verified')
        digest=hashlib.sha256((Path('/opt/backups/erp')/(latest+'.manifest')).read_bytes()).hexdigest()
        if not any(p['point']==latest and p['manifest_sha256']==digest for p in health['verified']):
            raise ValueError('backup_manifest_changed')
        self.point.verify_batch(item['files'])

    def reconcile(self,entry):
        path=self.point.store.sources_root/entry['relative_path']
        if not path.exists() and not path.is_symlink():
            return 'absent'
        return 'same' if self.point.matches_original(entry) else 'different'

    def identity(self, entry):
        return entry['id']

    @contextmanager
    def lease(self, entry):
        with self.point.store.path_lock(entry['relative_path'], exclusive=True):
            yield

    def verify(self, entry):
        return self.point.eligible(entry) and self.point.matches_original(entry)

    def remove(self, entry):
        path = self.point.store.sources_root / entry['relative_path']
        # verify again at the point of use, under the Point exclusive lease.
        if not self.verify(entry):
            raise ValueError('candidate_changed')
        path.unlink()
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def measure(self):
        ids = self.command('docker','ps','-aq').split()
        containers = json.loads(self.command('docker','inspect',*ids)) if ids else []
        return dict(free_bytes=shutil.disk_usage('/').free,
            percent=int(self.command('df','-P','/').splitlines()[-1].split()[4].rstrip('%')),
            containers={x['Id']:[x['State']['Running'],x['State']['StartedAt'],x['Image']] for x in containers},
            images=sorted(set(self.command('docker','image','ls','-aq','--no-trunc').split())),
            volumes=sorted(self.command('docker','volume','ls','-q').split()),
            backups={p.name:[p.stat().st_ino,p.stat().st_size,p.stat().st_mtime_ns] for p in Path('/opt/backups/erp').iterdir() if p.is_file()},
            monitors='\n'.join(self.command('systemctl','show','--property=ActiveState','--value',unit)
                for unit in ('erp-backup-alerts.timer','erp-point-decision.service','erp-telegram-release.timer')),
            http=self.command('curl','--max-time','15','-sSL','-o','/dev/null','-w','%{http_code}','https://erp.pollyanasdolce.com/login/'))

    @staticmethod
    def verify_preserved(before, after):
        # The publisher may be deliberately paused; recovery and the decision receiver must be active.
        if before['monitors'].splitlines() not in (['active','active','active'], ['active','active','inactive']) or before['http'] != '200' or after['http'] != '200' or any(after[k] != before[k] for k in ('containers','images','volumes','monitors')):
            raise ValueError('service_state_changed')
        if any(after['backups'].get(k) != v for k,v in before['backups'].items()):
            raise ValueError('backup_state_changed')


def once(ledger, config):
    cleanup.flush(ledger, config)
    outcomes = {}
    for path in sorted(ledger.directory.glob('*.json')):
        row = ledger.read(path.stem)
        frozen = ledger.directory / 'proposals' / path.name
        if row['state'] != 'approved' or not frozen.is_file():
            continue
        proposal, _ = cleanup.read_proposal(frozen)
        if proposal.get('executor') != 'resident-v1':
            continue
        if row['expires'] <= time.time() and cleanup.execution_state(ledger, row['nonce']) is None:
            cleanup.enqueue(ledger, row, 'expired', 'La autorización venció antes de ejecutar. Nada retirado; requiere una propuesta revalidada.')
            continue
        try:
            result = execute(row['nonce'], ledger, HostAdapter(proposal['delete_candidates'][0]))
            outcomes[row['nonce']] = result
            if result in {'completed','partial','blocked','uncertain'}:
                telegram(config, 'editMessageReplyMarkup', {'chat_id':row['chat_id'], 'message_id':row['message_id'], 'reply_markup':{'inline_keyboard':[]}})
        except BlockingIOError:
            outcomes[row['nonce']] = 'busy'
        except (ValueError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
            outcomes[row['nonce']] = type(exc).__name__
    cleanup.flush(ledger, config)
    return outcomes


def reissue_expired(nonce, ledger, config):
    ledger.path(nonce)
    path = ledger.directory / 'proposals' / (nonce + '.json')
    proposal, digest = cleanup.read_proposal(path)
    item = proposal['delete_candidates'][0]
    adapter = HostAdapter(item)
    with deployment_lock(adapter.deploy_lock), ledger.lock(worker=True):
        cleanup.check_replacement(ledger, nonce, proposal, digest)
        adapter.preflight(item)
        if not all(adapter.point.matches_original(entry) for entry in item['files']):
            raise ValueError('candidate_changed')
        return cleanup._request(path, ledger, config, replaces=nonce)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--once', action='store_true')
    mode.add_argument('--reissue-expired', metavar='NONCE')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('root required')
    os.umask(0o077)
    config = json.loads((Path(os.environ['CREDENTIALS_DIRECTORY'])/'telegram.json').read_text())
    ledger = Ledger(cleanup.STATE, validator=cleanup.validate)
    if args.reissue_expired:
        row = reissue_expired(args.reissue_expired, ledger, config)
        print(json.dumps({key: row[key] for key in ('nonce', 'state', 'message_id', 'expires')}))
    else:
        print(json.dumps(once(ledger, config)))


if __name__ == '__main__':
    main()
