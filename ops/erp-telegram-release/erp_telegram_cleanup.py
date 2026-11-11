"""Manual cleanup authorization only: no deletion, worker, shell or deployment."""
import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import time

from erp_telegram_release import Ledger, NONCE, telegram, verify_message

STATE = Path('/var/lib/erp-point-decision/cleanups')
DIGEST = re.compile(r'[0-9a-f]{64}')
IMAGE = re.compile(r'sha256:[0-9a-f]{64}')
PROPOSAL_ID = re.compile(r'VPS-LIM-[0-9]{8}-[0-9]{2}')
CALLBACK = re.compile(r'c:([ar]):([0-9a-f]{24}):([0-9a-f]{24})')
FIELDS = {'proposal_id', 'proposal_sha256', 'image_id', 'nonce', 'expires', 'chat_id',
          'message_id', 'state', 'decision_at'}


def read_proposal(path):
    with Path(path).open('rb') as stream:
        raw = stream.read(16385)
    if len(raw) > 16384:
        raise ValueError('proposal_too_large')
    proposal = json.loads(raw)
    fields = {'delete_candidates', 'execution', 'expected_recovery', 'expected_usage', 'host',
              'id', 'preconditions', 'preserve', 'risk', 'status'}
    if not isinstance(proposal, dict) or set(proposal) != fields:
        raise ValueError('invalid_proposal_schema')
    if (proposal['host'] != '68.183.165.47' or not isinstance(proposal['id'], str)
            or not PROPOSAL_ID.fullmatch(proposal['id']) or proposal['status'] != 'awaiting_explicit_approval'):
        raise ValueError('invalid_proposal')
    candidates = proposal['delete_candidates']
    if not isinstance(candidates, list) or len(candidates) != 1:
        raise ValueError('one_image_only')
    item = candidates[0]
    if (not isinstance(item, dict) or set(item) != {'bytes_logical', 'container_references', 'id', 'tag', 'type', 'unique_size_docker_report'}
            or item['type'] != 'docker_image' or not isinstance(item['id'], str) or not IMAGE.fullmatch(item['id'])
            or not isinstance(item['tag'], str) or not re.fullmatch(r'erp-autofix-deps:[0-9a-f]{12}', item['tag'])
            or item['tag'].split(':')[1] != item['id'][7:19]
            or type(item['bytes_logical']) is not int or not 0 < item['bytes_logical'] < 100_000_000_000
            or type(item['container_references']) is not int or item['container_references'] != 0):
        raise ValueError('invalid_image_candidate')
    texts = [proposal[k] for k in ('execution', 'expected_recovery', 'expected_usage', 'risk')]
    texts.append(item['unique_size_docker_report'])
    for key in ('preconditions', 'preserve'):
        if not isinstance(proposal[key], list) or not 1 <= len(proposal[key]) <= 20:
            raise ValueError('invalid_conditions')
        texts.extend(proposal[key])
    if any(not isinstance(s, str) or not 1 <= len(s) <= 600 or any(ord(c) < 32 for c in s) for s in texts):
        raise ValueError('invalid_proposal_text')
    return proposal, hashlib.sha256(raw).hexdigest()


def validate(row):
    if not isinstance(row, dict) or set(row) != FIELDS:
        raise ValueError('invalid_cleanup_ledger')
    for key, pattern in [('proposal_id', PROPOSAL_ID), ('proposal_sha256', DIGEST), ('image_id', IMAGE), ('nonce', NONCE)]:
        if not isinstance(row[key], str) or not pattern.fullmatch(row[key]):
            raise ValueError('invalid_cleanup_binding')
    if row['state'] not in {'notifying', 'pending', 'approved', 'rejected'}:
        raise ValueError('invalid_cleanup_state')
    for key in ('expires', 'chat_id'):
        if type(row[key]) is not int or row[key] <= 0:
            raise ValueError('invalid_number')
    for key in ('message_id', 'decision_at'):
        if row[key] is not None and (type(row[key]) is not int or row[key] <= 0):
            raise ValueError('invalid_number')
    if ((row['state'] != 'notifying' and row['message_id'] is None)
            or ((row['state'] in {'approved', 'rejected'}) != (row['decision_at'] is not None))):
        raise ValueError('inconsistent_cleanup_decision')
    return row


def signed(config, row, action):
    body = f"c:{action}:{row['nonce']}"
    binding = '|'.join(str(row[k]) for k in ('proposal_id', 'proposal_sha256', 'image_id', 'expires', 'chat_id', 'message_id'))
    signature = hmac.new(config['token'].encode(), (body + '|' + binding).encode(), hashlib.sha256).hexdigest()[:24]
    return body + ':' + signature


def request(path, ledger, config, send=telegram):
    proposal, digest = read_proposal(path)
    item = proposal['delete_candidates'][0]
    row = dict(proposal_id=proposal['id'], proposal_sha256=digest, image_id=item['id'],
               nonce=secrets.token_hex(12), expires=int(time.time()) + 3600, chat_id=config['chat_id'],
               message_id=None, state='notifying', decision_at=None)
    with ledger.lock():
        for file in ledger.directory.glob('*.json'):
            old = ledger.read(file.stem)
            if (old['proposal_id'] == row['proposal_id'] and old['state'] != 'rejected'
                    and (old['state'] == 'notifying' or int(time.time()) < old['expires'])):
                raise ValueError('existing_request_requires_resolution')
        ledger.write(row)
        # Only fixed, bounded operational fields are sent; arbitrary proposal prose stays local.
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
                'y la recuperación antes de cualquier retiro. Vence en 1 hora; rechazar revoca la autorización.')
        result = send(config, 'sendMessage', {'chat_id': row['chat_id'], 'text': text})
        verify_message(result, row['chat_id'])
        row.update(message_id=result['message_id'], state='pending')
        ledger.write(row)
        # Persist identity before exposing buttons; ambiguous sends remain blocked, never auto-retried.
        markup = {'inline_keyboard': [[{'text': label, 'callback_data': signed(config, row, action)}
            for action, label in [('a', 'Aprobar limpieza'), ('r', 'Rechazar / revocar')]]]}
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
        if now >= row['expires']:
            raise ValueError('expired')
        if row['state'] != 'pending' and not (row['state'] == 'approved' and action == 'r'):
            raise ValueError('already_decided')
        row.update(state='approved' if action == 'a' else 'rejected', decision_at=now)
        ledger.write(row)
    return row


def status(path, nonce, ledger, now=None):
    proposal, digest = read_proposal(path)
    with ledger.lock():
        row = ledger.read(nonce)
        if (row['proposal_sha256'] != digest or row['proposal_id'] != proposal['id']
                or row['image_id'] != proposal['delete_candidates'][0]['id']):
            raise ValueError('proposal_changed')
        valid = row['state'] == 'approved' and (int(time.time()) if now is None else now) < row['expires']
        return {**row, 'approved': valid, 'automatic_deletion': False}


def handle(query, config, state, send):
    try:
        row = decide(query, config, Ledger(Path(state) / 'cleanups', validator=validate))
        text = ('Autorización registrada. Nada borrado; Codex debe verificar recuperación y vigencia.'
                if row['state'] == 'approved' else 'Limpieza rechazada / revocada. Nada borrado por esta solicitud.')
    except (OSError, ValueError, KeyError, TypeError):
        text = 'Solicitud inválida, vencida o ya decidida. No se autorizó una limpieza nueva.'
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
