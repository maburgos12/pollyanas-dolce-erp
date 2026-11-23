"""Authenticated cleanup decisions; deletion belongs to the opt-in resident executor."""
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import time
import tempfile
import shutil
import stat
import subprocess

from erp_telegram_release import (Ledger, NONCE, telegram, verify_message, schedule_request,
                                  approval_window, validate_intent, operational_module, RecoveryPending, APPROVAL_TTL_SECONDS)

STATE = Path('/var/lib/erp-point-decision/cleanups')
DIGEST = re.compile(r'[0-9a-f]{64}')
IMAGE = re.compile(r'sha256:[0-9a-f]{64}')
PROPOSAL_ID = re.compile(r'VPS-LIM-[0-9]{8}-[0-9]{2}')
CALLBACK = re.compile(r'c:([ar]):([0-9a-f]{24}):([0-9a-f]{24})')
FIELDS = {'proposal_id', 'proposal_sha256', 'image_id', 'nonce', 'expires', 'chat_id',
          'message_id', 'state', 'decision_at'}


# Closed scopes only. This validator never authorizes arbitrary paths or commands.
FILE_PATH = re.compile(
    r'(?:/var/cache/apt/(?:srcpkgcache|pkgcache)\.bin'
    r'|/var/cache/apt/archives/[A-Za-z0-9_+.~%:-]+\.deb'
    r'|/var/log/(?:[A-Za-z0-9_-]+/)?[A-Za-z0-9_.-]+\.(?:[0-9]+\.gz|1)'
    r'|/var/log/journal/[0-9a-f]{32}/(?:system|user-[0-9]+)@[0-9a-f-]+\.journal)')


def validate_manifest(item, proposal_id):
    if set(item) != {'type', 'id', 'files', 'build_cache', 'recovery'}:
        raise ValueError('invalid_manifest_schema')
    files, cache = item['files'], item['build_cache']
    if (not isinstance(files, list) or not 1 <= len(files) <= 256
            or not isinstance(cache, list) or len(cache) > 128):
        raise ValueError('invalid_manifest_count')
    identities = set()
    for entry in files:
        if (not isinstance(entry, dict) or set(entry) != {'path', 'bytes', 'sha256'}
                or not isinstance(entry['path'], str) or not FILE_PATH.fullmatch(entry['path'])
                or '..' in entry['path'] or 'backup' in entry['path'].lower()
                or not isinstance(entry['sha256'], str) or not DIGEST.fullmatch(entry['sha256'])
                or type(entry['bytes']) is not int or not 0 <= entry['bytes'] < 10_000_000_000
                or entry['path'] in identities):
            raise ValueError('invalid_file_candidate')
        identities.add(entry['path'])
    for entry in cache:
        if (not isinstance(entry, dict) or set(entry) != {'id', 'bytes'}
                or not isinstance(entry['id'], str) or not re.fullmatch(r'[a-z0-9]{25}', entry['id'])
                or type(entry['bytes']) is not int or not 0 <= entry['bytes'] < 10_000_000_000
                or entry['id'] in identities):
            raise ValueError('invalid_cache_candidate')
        identities.add(entry['id'])
    recovery = item['recovery']
    if (not isinstance(recovery, dict) or set(recovery) != {'archive', 'sha256', 'files_verified'}
            or recovery['archive'] != f'/share/ERP_BACKUPS/vps-cleanup/{proposal_id}/files-recovery.tar.gz'
            or not isinstance(recovery['sha256'], str) or not DIGEST.fullmatch(recovery['sha256'])
            or type(recovery['files_verified']) is not int or recovery['files_verified'] != len(files)):
        raise ValueError('invalid_recovery_evidence')
    content = json.dumps(dict(files=files, build_cache=cache), sort_keys=True, separators=(',', ':')).encode()
    if item['id'] != 'sha256:' + hashlib.sha256(content).hexdigest():
        raise ValueError('manifest_digest_mismatch')


def point_manifest_id(files):
    raw = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
    return 'sha256:' + hashlib.sha256(raw).hexdigest()


def validate_point_manifest(item):
    if set(item) != {'type', 'id', 'files', 'recovery'} or not isinstance(item['files'], list) or not 1 <= len(item['files']) <= 100:
        raise ValueError('invalid_point_manifest')
    seen = set()
    for entry in item['files']:
        fields = {'schema', 'id', 'relative_path', 'sha256', 'bytes', 'nas_key', 'verified_at', 'consumers'}
        if not isinstance(entry, dict) or set(entry) != fields:
            raise ValueError('invalid_point_entry')
        name, digest = entry['relative_path'], entry['sha256']
        if (entry['schema'] != 1 or not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9_-]+\.xls', name)
                or not isinstance(digest, str) or not DIGEST.fullmatch(digest)
                or type(entry['bytes']) is not int or not 0 < entry['bytes'] <= 100_000_000
                or type(entry['verified_at']) is not int or entry['verified_at'] <= 0
                or entry['consumers'] != ['category_report'] or name in seen
                or entry['nas_key'] != 'point-archive/objects/' + digest + '.xls'
                or entry['id'] != hashlib.sha256((name + '\0' + digest).encode()).hexdigest()):
            raise ValueError('invalid_point_entry')
        seen.add(name)
    if sum(e['bytes'] for e in item['files']) > 100_000_000:
        raise ValueError('point_pilot_limit')
    recovery = item['recovery']
    if (set(recovery) != {'files_verified', 'verified_at'} or recovery['files_verified'] != len(item['files'])
            or type(recovery['verified_at']) is not int or recovery['verified_at'] <= 0
            or item['id'] != point_manifest_id(item['files'])):
        raise ValueError('invalid_point_recovery')


def read_proposal(path):
    with Path(path).open('rb') as stream:
        raw = stream.read(131073)
    if len(raw) > 131072:
        raise ValueError('proposal_too_large')
    proposal = json.loads(raw)
    fields = {'delete_candidates', 'execution', 'expected_recovery', 'expected_usage', 'host',
              'id', 'preconditions', 'preserve', 'risk', 'status'}
    if not isinstance(proposal, dict) or set(proposal) not in (fields, fields | {'schema', 'executor'}, fields | {'schema', 'executor', 'predecessor'}):
        raise ValueError('invalid_proposal_schema')
    if (proposal['host'] != '68.183.165.47' or not isinstance(proposal['id'], str)
            or not PROPOSAL_ID.fullmatch(proposal['id']) or proposal['status'] != 'awaiting_explicit_approval'):
        raise ValueError('invalid_proposal')
    resident = proposal.get('schema') == 2 and proposal.get('executor') == 'resident-v1'
    if ('schema' in proposal or 'executor' in proposal) and not resident:
        raise ValueError('invalid_executor_opt_in')
    candidates = proposal['delete_candidates']
    if not isinstance(candidates, list) or len(candidates) != 1:
        raise ValueError('one_scope_only')
    item = candidates[0]
    if isinstance(item, dict) and item.get('type') == 'point_archive_manifest':
        if not resident:
            raise ValueError('point_requires_resident_contract')
        validate_point_manifest(item)
        if 'predecessor' in proposal:
            link = proposal['predecessor']
            if (not isinstance(link, dict) or set(link) != {'nonce', 'proposal_sha256', 'execution_sha256', 'attempts_sha256'}
                    or not isinstance(link['nonce'], str) or not NONCE.fullmatch(link['nonce'])
                    or any(not isinstance(link[k], str) or not DIGEST.fullmatch(link[k]) for k in link if k != 'nonce')):
                raise ValueError('invalid_predecessor')
    elif isinstance(item, dict) and item.get('type') == 'cache_log_manifest':
        validate_manifest(item, proposal['id'])
        if resident:
            raise ValueError('apt_logs_require_producer_coordination')
    else:
        if (not isinstance(item, dict) or set(item) != {'bytes_logical', 'container_references', 'id', 'tag', 'type', 'unique_size_docker_report'}
                or item['type'] != 'docker_image' or not isinstance(item['id'], str) or not IMAGE.fullmatch(item['id'])
                or not isinstance(item['tag'], str) or not re.fullmatch(r'erp-autofix-deps:[0-9a-f]{12}', item['tag'])
                or item['tag'].split(':')[1] != item['id'][7:19]
                or type(item['bytes_logical']) is not int or not 0 < item['bytes_logical'] < 100_000_000_000
                or type(item['container_references']) is not int or item['container_references'] != 0):
            raise ValueError('invalid_image_candidate')
    if 'predecessor' in proposal and item['type'] != 'point_archive_manifest':
        raise ValueError('point_predecessor_only')
    texts = [proposal[k] for k in ('execution', 'expected_recovery', 'expected_usage', 'risk')]
    if item['type'] == 'docker_image':
        texts.append(item['unique_size_docker_report'])
    for key in ('preconditions', 'preserve'):
        if not isinstance(proposal[key], list) or not 1 <= len(proposal[key]) <= 20:
            raise ValueError('invalid_conditions')
        texts.extend(proposal[key])
    if any(not isinstance(s, str) or not 1 <= len(s) <= 600 or any(ord(c) < 32 for c in s) for s in texts):
        raise ValueError('invalid_proposal_text')
    if resident and item['type'] == 'docker_image':
        raise ValueError('image_requires_manual_execution')
    return proposal, hashlib.sha256(raw).hexdigest()


def validate(row):
    if not isinstance(row, dict) or set(row) != FIELDS:
        raise ValueError('invalid_cleanup_ledger')
    for key, pattern in [('proposal_id', PROPOSAL_ID), ('proposal_sha256', DIGEST), ('image_id', IMAGE), ('nonce', NONCE)]:
        if not isinstance(row[key], str) or not pattern.fullmatch(row[key]):
            raise ValueError('invalid_cleanup_binding')
    if row['state'] not in {'notifying', 'deferred', 'pending', 'approved', 'rejected'}:
        raise ValueError('invalid_cleanup_state')
    for key in ('expires', 'chat_id'):
        if type(row[key]) is not int or row[key] <= 0:
            raise ValueError('invalid_number')
    for key in ('message_id', 'decision_at'):
        if row[key] is not None and (type(row[key]) is not int or row[key] <= 0):
            raise ValueError('invalid_number')
    if ((row['state'] not in {'notifying', 'deferred'} and row['message_id'] is None)
            or ((row['state'] in {'approved', 'rejected'}) != (row['decision_at'] is not None))):
        raise ValueError('inconsistent_cleanup_decision')
    return row


def signed(config, row, action):
    body = f"c:{action}:{row['nonce']}"
    binding = '|'.join(str(row[k]) for k in ('proposal_id', 'proposal_sha256', 'image_id', 'expires', 'chat_id', 'message_id'))
    signature = hmac.new(config['token'].encode(), (body + '|' + binding).encode(), hashlib.sha256).hexdigest()[:24]
    return body + ':' + signature


def require_recovery():
    try:
        failures, _ = operational_module('vps_backup_alerts').collect(time.time())
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as error:
        raise RecoveryPending('backup_recovery_pending') from error
    if set(failures) - {'disk_warning', 'disk_urgent'}:
        raise RecoveryPending('backup_recovery_pending')


def request(path, ledger, config, send=telegram):
    proposal, digest = read_proposal(path)
    directory = ledger.directory / 'scheduled-proposals'
    directory.mkdir(mode=0o700, exist_ok=True)
    frozen = directory / (digest + '.json')
    with Path(path).open('rb') as stream:
        raw = stream.read(131073)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('proposal_changed')
    with ledger.lock():
        validate_predecessor(proposal, ledger)
        if not frozen.exists():
            fd = os.open(frozen, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        if read_proposal(frozen)[1] != digest:
            raise ValueError('frozen_proposal_changed')
    def check():
        current, current_digest = read_proposal(frozen)
        if current_digest != digest:
            raise ValueError('proposal_changed')
        validate_predecessor(current, ledger)
        revalidate_proposal(current, ledger.directory)
    return schedule_request('cleanup', {'digest': digest, 'proposal_id': proposal['id']}, ledger,
        lambda row: row['nonce'] != proposal.get('predecessor', {}).get('nonce') and
            (row['proposal_id'] == proposal['id'] or row['image_id'] == proposal['delete_candidates'][0]['id']),
        lambda: _request(frozen, ledger, config, send), check)


def revalidate_proposal(proposal, state):
    require_recovery()
    item = proposal['delete_candidates'][0]
    if item['type'] == 'point_archive_manifest':
        with operational_module('erp_point_archive').Broker() as broker:
            broker.verify_batch(item['files'])
        return
    # Legacy image/cache proposals lack a restorable identity contract: require operator review.
    if item['type'] != 'cache_log_manifest' or item['build_cache']:
        raise ValueError('recovery_requires_operator_review')
    prep = operational_module('erp_cleanup_prepare')
    entries = [prep.snapshot(Path(entry['path'])) for entry in item['files']]
    if [{key: entry[key] for key in ('path', 'bytes', 'sha256')} for entry in entries] != item['files']:
        raise ValueError('cleanup_scope_changed')
    prep.unchanged(entries)
    nas = prep.NAS()
    archive_path = nas.path(proposal['id'])
    if nas.digest(proposal['id']) != item['recovery']['sha256']:
        raise ValueError('nas_recovery_changed')
    size = int(nas.command(f'stat -c %s {archive_path}'))
    total = sum(entry['bytes'] for entry in entries)
    if not 0 < size <= prep.ARCHIVE_LIMIT or shutil.disk_usage(state).free < prep.RESERVE + total + size:
        raise ValueError('recovery_capacity')
    with tempfile.TemporaryDirectory(prefix='approval-recovery-', dir=state) as scratch:
        root = Path(scratch)
        archive = root / 'readback.tar.gz'
        nas.download(proposal['id'], archive, size)
        with archive.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if digest != item['recovery']['sha256']:
            raise ValueError('nas_readback_changed')
        prep.verify_archive(archive, entries, root)
    prep.unchanged(entries)


def process_scheduled(ledger, config, send=telegram):
    if not approval_window()[0]:
        return
    queue = Ledger(ledger.directory / 'scheduled', validator=validate_intent)
    for path in sorted(ledger.directory.glob('*.json')):
        row = ledger.read(path.stem)
        if row['state'] != 'pending' or row['decision_at'] is not None or row['expires'] > int(time.time()):
            continue
        candidates = [ledger.directory / 'proposals' / (row['nonce'] + '.json'),
                      ledger.directory.parent / 'cleanup-proposals' / (row['proposal_id'] + '.json'),
                      Path('/var/lib/erp-cleanup-prepare') / (row['proposal_id'] + '.proposal')]
        for candidate in candidates:
            if not candidate.is_file():
                continue
            try:
                if read_proposal(candidate)[1] == row['proposal_sha256']:
                    request(candidate, ledger, config, send)
                    break
            except (ValueError, OSError, RuntimeError, KeyError, subprocess.SubprocessError):
                break
    for path in sorted(queue.directory.glob('*.json')):
        item = queue.read(path.stem)
        if item['kind'] != 'cleanup' or item['phase'] != 'waiting':
            continue
        digest = item['binding'].get('digest', '')
        if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
            raise ValueError('invalid_cleanup_intent')
        try:
            request(ledger.directory / 'scheduled-proposals' / (digest + '.json'), ledger, config, send)
        except (ValueError, OSError, RuntimeError, KeyError, subprocess.SubprocessError):
            print('Scheduled cleanup recovery could not be confirmed; no authorization sent.', flush=True)


def _request(path, ledger, config, send=telegram):
    if not approval_window()[0]:
        raise ValueError('outside_approval_hours')
    require_recovery()
    if not approval_window()[0]:
        raise ValueError('outside_approval_hours')
    proposal, digest = read_proposal(path)
    item = proposal['delete_candidates'][0]
    row = dict(proposal_id=proposal['id'], proposal_sha256=digest, image_id=item['id'],
               nonce=secrets.token_hex(12), expires=int(time.time()) + APPROVAL_TTL_SECONDS, chat_id=config['chat_id'],
               message_id=None, state='notifying', decision_at=None)
    with ledger.lock():
        predecessor = validate_predecessor(proposal, ledger)
        for file in ledger.directory.glob('*.json'):
            old = ledger.read(file.stem)
            if predecessor and old['nonce'] == predecessor['nonce']:
                continue
            if predecessor and old['image_id'] == row['image_id']:
                raise ValueError('existing_successor_requires_resolution')
            if predecessor and old['state'] == 'approved':
                prior_execution = execution_state(ledger, old['nonce'])
                if prior_execution and prior_execution['state'] != 'completed' and not predecessor_resolved(ledger, old):
                    raise ValueError('unresolved_other_execution')
            if old['state']=='notifying' or (old['state'] in {'pending','approved'} and int(time.time())<old['expires'] and not execution_state(ledger,old['nonce'])):
                raise ValueError('existing_request_requires_resolution')
            if ((old['proposal_id'] == row['proposal_id'] or old['image_id'] == row['image_id'])
                    and (old.get('decision_at') is not None or old['state'] in {'notifying', 'approved', 'rejected'}
                         or (old['state'] == 'pending' and int(time.time()) < old['expires']))):
                raise ValueError('existing_request_requires_resolution')
        ledger.write(row)
        proposals = ledger.directory / 'proposals'
        proposals.mkdir(mode=0o700, exist_ok=True)
        with (proposals / (row['nonce'] + '.json')).open('xb') as frozen:
            frozen.write(Path(path).read_bytes())
            frozen.flush()
            os.fsync(frozen.fileno())
        fd=os.open(proposals,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
        # Only fixed, bounded operational fields are sent; arbitrary proposal prose stays local.
        if item['type'] == 'docker_image':
            text = (f"Autorizar limpieza VPS · {row['proposal_id']}\nPropuesta SHA-256: {digest}\n"
                    f"Única imagen: {item['tag']}\n{item['id']}\n"
                    f"Tamaño lógico máximo: {item['bytes_logical'] / 1e9:.2f} GB; ahorro real por medir.\n"
                    'No garantiza bajar del umbral de disco. Se conserva el respaldo completo más reciente '
                    'y cualquier respaldo posterior, todos los demás respaldos, bases, volúmenes, contenedores, '
                    'medios, logs, cachés y monitores.\n'
                    'Antes de retirar: comprobar ausencia de uso, copiar esta imagen al NAS y demostrar '
                    'recuperación aislada al mismo ID. Si falla, no se borra. Sin prune ni borrado forzado.\n'
                    'Recuperación pendiente de verificar; una tarea futura podría necesitar restaurar la imagen. '
                    'Esta aprobación no ejecuta borrados ni despliegues. Codex verificará la decisión vigente '
                    'y la recuperación antes de cualquier retiro. Vence en 12 horas; rechazar revoca la autorización.')
        elif item['type'] == 'point_archive_manifest':
            text = (f"Autorizar archivo de históricos Point · {row['proposal_id']}\nPropuesta SHA-256: {digest}\n"
                    f"{len(item['files'])} XLS verificados, {sum(e['bytes'] for e in item['files']) / 1e6:.2f} MB.\n"
                    'Copia NAS, lectura de regreso y parseo equivalentes comprobados. Se conservan registros y referencias; '
                    'el lector recupera el original si vuelve a necesitarse. Si el NAS falla, la consulta del archivo puede esperar.\n'
                    'Conservar todos los respaldos, bases, servicios y archivos fuera de este lote. '
                    'El ejecutor residente retirará sólo estos originales tras aprobación vigente y nueva comprobación. '
                    'Vence en 12 horas. Rechazar revoca hasta que comience la ejecución; al vencer se detienen los retiros pendientes.')
            if predecessor:
                text += ('\nSucesora del lote bloqueado ' + predecessor['nonce'] +
                         '. Se conservan sus tres intentos y su incidencia. Esta es una autorización nueva del mismo manifiesto exacto.')
        else:
            file_bytes = sum(entry['bytes'] for entry in item['files'])
            cache_bytes = sum(entry['bytes'] for entry in item['build_cache'])
            text = (f"Autorizar limpieza VPS · {row['proposal_id']}\nPropuesta SHA-256: {digest}\n"
                    f"Manifiesto: {item['id']}\n"
                    f"{len(item['files'])} archivos de APT/logs rotados: {file_bytes / 1e9:.2f} GB lógicos.\n"
                    f"{len(item['build_cache'])} registros de caché BuildKit: {cache_bytes / 1e9:.2f} GB estimados.\n"
                    f"Archivo NAS: {item['recovery']['archive']}\n"
                    f"SHA-256 archivo: {item['recovery']['sha256']}\n"
                    'Evidencia declarada: copia NAS y recuperación aislada de todos los archivos verificadas. '
                    'Codex debe revalidarla; la caché BuildKit es regenerable, no se afirma restaurada.\n'
                    'Antes de retirar: comprobar hashes, rutas/IDs exactos, archivos sin uso y caché no compartida '
                    'ni en uso; sin construcciones concurrentes. Si cambia algo, se excluye.\n'
                    'Conservar TODOS los respaldos, imágenes, bases, volúmenes, contenedores, medios, logs activos, '
                    'servicios y monitores. Sin prune global, reinicios ni borrado forzado. '
                    'El histórico retirado se consulta desde NAS; las próximas construcciones pueden tardar más. '
                    'Ahorro real por medir; no garantiza bajar del umbral.\n'
                    'Esta aprobación no ejecuta borrados ni despliegues. Codex verificará la decisión vigente '
                    'y la recuperación antes de cualquier retiro. Vence en 12 horas; rechazar revoca la autorización.')
        deadline = datetime.fromtimestamp(row['expires'], ZoneInfo('America/Mazatlan')).strftime('%d/%m %H:%M')
        text += f'\nVencimiento: {deadline}, hora de Mazatlán.'
        if not approval_window()[0]:
            row['state'] = 'deferred'
            ledger.write(row)
            raise ValueError('outside_approval_hours')
        result = send(config, 'sendMessage', {'chat_id': row['chat_id'], 'text': text})
        verify_message(result, row['chat_id'])
        row.update(message_id=result['message_id'], state='pending')
        ledger.write(row)
        # Persist identity before exposing buttons; ambiguous sends remain blocked, never auto-retried.
        markup = {'inline_keyboard': [[{'text': label, 'callback_data': signed(config, row, action)}
            for action, label in [('a', 'Aprobar limpieza'), ('r', 'Rechazar / revocar')]]]}
        if not approval_window()[0]:
            row['state'] = 'deferred'
            ledger.write(row)
            raise ValueError('outside_approval_hours')
        try:
            result = send(config, 'editMessageReplyMarkup', {'chat_id': row['chat_id'],
                          'message_id': row['message_id'], 'reply_markup': markup})
            verify_message(result, row['chat_id'])
            if result['message_id'] != row['message_id'] or result.get('reply_markup') != markup:
                raise ValueError('cleanup_buttons_not_verified')
        except Exception:
            row['state'] = 'notifying'
            ledger.write(row)
            raise
    return row


def decide(query, config, ledger, now=None):
    now = int(time.time()) if now is None else now
    match = CALLBACK.fullmatch(query.get('data', ''))
    if not match:
        raise ValueError('invalid_callback')
    action, nonce, _ = match.groups()
    with ledger.lock():
        row = ledger.read(nonce)
        message = query.get('message', {})
        chat = message.get('chat', {})
        if (chat.get('type') != 'private' or chat.get('id') != config['chat_id']
                or row['chat_id'] != config['chat_id'] or query.get('from', {}).get('id') != config['chat_id']
                or message.get('message_id') != row['message_id'] or row['message_id'] is None
                or not hmac.compare_digest(query['data'], signed(config, row, action))):
            raise ValueError('unauthorized_cleanup_callback')
        execution = execution_state(ledger, row['nonce'])
        if execution:
            raise RecordedDecision(row, execution['state'])
        if row['state'] == 'rejected' or (row['state'] == 'approved' and action == 'a'):
            raise RecordedDecision(row, row['state'])
        if now >= row['expires']:
            raise RecordedDecision(row, 'expired')
        if row['state'] != 'pending' and not (row['state'] == 'approved' and action == 'r'):
            raise ValueError('already_decided')
        row.update(state='approved' if action == 'a' else 'rejected', decision_at=now)
        ledger.write(row)
    return row


def status(path, nonce, ledger, now=None):
    proposal, digest = read_proposal(path)
    with ledger.lock():
        validate_predecessor(proposal, ledger)
        row = ledger.read(nonce)
        if (row['proposal_sha256'] != digest or row['proposal_id'] != proposal['id']
                or row['image_id'] != proposal['delete_candidates'][0]['id']):
            raise ValueError('proposal_changed')
        valid = row['state'] == 'approved' and (int(time.time()) if now is None else now) < row['expires']
        return {**row, 'approved': valid, 'automatic_deletion': proposal.get('executor') == 'resident-v1'}


class RecordedDecision(ValueError):
    def __init__(self, row, state):
        self.row, self.state = row, state
        super().__init__('expired' if state == 'expired' else 'already_decided:' + state)


def execution_state(ledger, nonce):
    path = ledger.directory / 'executions' / (nonce + '.json')
    if not path.exists():
        return None
    info = path.lstat()
    if path.is_symlink() or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError('unsafe_execution_state')
    value = json.loads(path.read_text())
    if value.get('nonce') != nonce or value.get('state') not in {'executing', 'completed', 'partial', 'blocked', 'uncertain'}:
        raise ValueError('invalid_execution_state')
    if value.get('proposal_sha256') != ledger.read(nonce)['proposal_sha256']:
        raise ValueError('execution_proposal_mismatch')
    return value


def recovery_evidence(path):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('unsafe_predecessor_evidence')
        raw = stream.read(131073)
    if len(raw) > 131072:
        raise ValueError('predecessor_evidence_limit')
    return raw, hashlib.sha256(raw).hexdigest()


def validate_predecessor(proposal, ledger):
    link = proposal.get('predecessor')
    if link is None:
        return None
    row = ledger.read(link['nonce'])
    frozen = ledger.directory / 'proposals' / (row['nonce'] + '.json')
    _, digest = recovery_evidence(frozen)
    parent, current = read_proposal(frozen)
    execution_raw, execution_digest = recovery_evidence(ledger.directory / 'executions' / (row['nonce'] + '.json'))
    attempts_raw, attempts_digest = recovery_evidence(ledger.directory / 'executions' / (row['nonce'] + '.attempts'))
    record, attempts = json.loads(execution_raw), json.loads(attempts_raw)
    if (row['state'] != 'approved' or row['decision_at'] is None
            or digest != current or digest != row['proposal_sha256'] or digest != link['proposal_sha256']
            or execution_digest != link['execution_sha256'] or attempts_digest != link['attempts_sha256']
            or parent.get('predecessor') is not None or parent['delete_candidates'][0]['type'] != 'point_archive_manifest'
            or parent['delete_candidates'] != proposal['delete_candidates'] or parent['id'] == proposal['id']
            or row['image_id'] != parent['delete_candidates'][0]['id'] or row['proposal_id'] != parent['id']
            or record.get('nonce') != row['nonce'] or record.get('proposal_sha256') != digest
            or record.get('state') != 'blocked' or record.get('reason') != 'preflight_attempt_limit'
            or record.get('removed') != [] or record.get('skipped') != [] or record.get('intent') is not None
            or type(attempts.get('count')) is not int or attempts['count'] != 3):
        raise ValueError('predecessor_not_recoverable')
    return row


def successor_proposal(ledger, nonce, proposal_id):
    # Explicit operator preparation only: never invoked by the periodic producer.
    if not isinstance(proposal_id, str) or not PROPOSAL_ID.fullmatch(proposal_id):
        raise ValueError('invalid_proposal_id')
    with ledger.lock():
        row = ledger.read(nonce)
        path = ledger.directory / 'proposals' / (row['nonce'] + '.json')
        _, digest = recovery_evidence(path)
        proposal, _ = read_proposal(path)
        root = ledger.directory / 'executions'
        proposal.update(id=proposal_id, predecessor=dict(nonce=row['nonce'], proposal_sha256=digest,
            execution_sha256=recovery_evidence(root / (row['nonce'] + '.json'))[1],
            attempts_sha256=recovery_evidence(root / (row['nonce'] + '.attempts'))[1]))
        validate_predecessor(proposal, ledger)
        return proposal


def predecessor_resolved(ledger, row):
    for path in (ledger.directory / 'proposals').glob('*.json'):
        child, digest = read_proposal(path)
        if child.get('predecessor', {}).get('nonce') != row['nonce']:
            continue
        successor = ledger.read(path.stem)
        if successor['proposal_sha256'] != digest or successor['image_id'] != child['delete_candidates'][0]['id']:
            raise ValueError('successor_proposal_changed')
        if successor['state'] != 'approved':
            continue
        validate_predecessor(child, ledger)
        record = execution_state(ledger, successor['nonce'])
        expected = {entry['id'] for entry in child['delete_candidates'][0]['files']}
        if (record and record['state'] == 'completed' and record.get('intent') is None
                and record.get('skipped') == [] and sorted(record.get('removed', [])) == sorted(expected)):
            return True
    return False


def decision_text(state):
    return {'approved': 'Aprobación ya registrada. Nada borrado por esta pulsación; ejecución sujeta a vigencia y comprobaciones.',
            'rejected': 'Limpieza rechazada / revocada. No se ejecutará este lote.',
            'expired': 'Solicitud vencida. No se autorizó un retiro nuevo.',
            'executing': 'La limpieza ya está en ejecución; recibirás el resultado.',
            'completed': 'La limpieza ya terminó. No se repetirá.',
            'partial': 'La limpieza terminó parcialmente. Consulta el resultado; no se repetirá.',
            'blocked': 'La limpieza está bloqueada. Se conservan los archivos pendientes.',
            'uncertain': 'La ejecución requiere revisar su registro antes de continuar.'}[state]


def _receipt(value):
    if not isinstance(value, dict) or set(value) != {'nonce', 'events'} or not NONCE.fullmatch(value['nonce']) or not isinstance(value['events'], dict):
        raise ValueError('invalid_receipt')
    for event in value['events'].values():
        if set(event) not in ({'text', 'state', 'message_id'}, {'text', 'state', 'message_id', 'attempts', 'next_at'}) or event['state'] not in {'pending', 'sending', 'sent', 'uncertain'} or not isinstance(event['text'], str) or len(event['text']) > 3500:
            raise ValueError('invalid_receipt_event')
        if any(type(event.get(k,0)) is not int or event.get(k,0)<0 for k in ('attempts','next_at')):
            raise ValueError('invalid_receipt_retry')
    return value


def enqueue(ledger, row, key, text):
    box = Ledger(ledger.directory / 'receipts', validator=_receipt)
    with box.lock():
        record = box.read(row['nonce']) if box.path(row['nonce']).exists() else {'nonce': row['nonce'], 'events': {}}
        record['events'].setdefault(key, {'text': row['proposal_id'] + ' · ' + time.strftime('%Y-%m-%d %H:%M:%S UTC',time.gmtime()) + '\n' + text, 'state': 'pending', 'message_id': None, 'attempts':0, 'next_at':0})
        box.write(record)


def flush(ledger, config, send=telegram):
    box = Ledger(ledger.directory / 'receipts', validator=_receipt)
    with box.lock(worker=True), box.lock():
        for file in sorted(box.directory.glob('*.json')):
            record = box.read(file.stem)
            for event in record['events'].values():
                if event['state'] == 'sending':
                    event['state'] = 'uncertain'
                    box.write(record)
                if event['state'] != 'pending' or event.get('next_at',0)>time.time():
                    continue
                event['state'] = 'sending'
                box.write(record)
                try:
                    result = send(config, 'sendMessage', {'chat_id': config['chat_id'], 'text': event['text']})
                    verify_message(result, config['chat_id'])
                    event.update(state='sent', message_id=result['message_id'])
                except Exception as exc:
                    from erp_telegram_release import TelegramRejected
                    if isinstance(exc,TelegramRejected) and event.get('attempts',0)<3:
                        event['attempts']=event.get('attempts',0)+1
                        event['next_at']=int(time.time())+[60,300,900][event['attempts']-1]
                        event['state']='pending'
                        box.write(record)
                        continue
                    # An ambiguous Telegram send must not repeat the cleanup or spam receipts.
                    event['state'] = 'uncertain'
                box.write(record)


def handle(query, config, state, send):
    row = None
    try:
        ledger = Ledger(Path(state) / 'cleanups', validator=validate)
        row = decide(query, config, ledger)
        text = decision_text(row['state'])
        # Durable receipts are separate from the original human decision.
        enqueue(ledger, row, 'decision-' + row['state'], text)
        flush(ledger, config, send)
    except RecordedDecision as recorded:
        text = decision_text(recorded.state)
    except (OSError, ValueError, KeyError, TypeError):
        text = ('Decisión registrada; confirmación de entrega pendiente.' if row else
                'No se pudo validar esta solicitud. No se autorizó una limpieza nueva.')
    send(config, 'answerCallbackQuery', {'callback_query_id': query['id'], 'text': text, 'show_alert': True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['request', 'status'])
    parser.add_argument('--proposal', required=True)
    parser.add_argument('--nonce')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('root required')
    ledger = Ledger(STATE, validator=validate)
    if args.command == 'request':
        config = json.loads((Path(os.environ['CREDENTIALS_DIRECTORY']) / 'telegram.json').read_text())
        row = request(args.proposal, ledger, config)
    else:
        row = status(args.proposal, args.nonce, ledger)
    print(json.dumps(row, sort_keys=True))
    if args.command == 'status' and not row['approved']:
        raise SystemExit(1)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        raise SystemExit('Cleanup authorization failed. No deletion performed; inspect private ledger.') from None
