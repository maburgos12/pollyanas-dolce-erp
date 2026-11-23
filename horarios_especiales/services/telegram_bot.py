"""Private hours bot. Durable input precedes Telegram acknowledgement; approval is separate."""
import hashlib
import json
import os
import re
import secrets
import stat
import time
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import requests
from django.contrib.auth import get_user_model
from django.db import connection, transaction
from django.db.models import Q
from django.core.exceptions import PermissionDenied
from core.access import ROLE_ADMIN, ROLE_DG, has_any_role
from django.utils import timezone

from horarios_especiales.models import SolicitudHorarioEspecial, TelegramHoursCursor, TelegramHoursUpdate
from horarios_especiales.services.requests import approve_request, cancel_request, create_request_from_rules
from horarios_especiales.services.rules import PRECEDENCE
from horarios_especiales.services.telegram_parser import EXAMPLES, WEEKDAYS, parse_instruction


class TelegramUnavailable(Exception):
    pass


def require_approver(actor):
    if not actor or not actor.is_active or not has_any_role(actor, ROLE_DG, ROLE_ADMIN):
        raise PermissionDenied('El propietario ERP ya no tiene permiso para aprobar y ejecutar horarios.')


def owner_updates(config):
    return TelegramHoursUpdate.objects.filter(bot_id=config['bot_id'], chat_id=config['telegram_chat_id'],
        telegram_user_id=config['telegram_user_id']).filter(Q(request__isnull=True) | Q(request__requested_by_id=config['erp_user_id']))


def load_config(directory=None):
    root = Path(directory or os.environ.get('HOURS_TELEGRAM_CONFIG_DIR', '/etc/pollyanas-hours'))
    values = []
    for name in ('owner.json', 'telegram-token'):
        path = root / name
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 8192:
            raise ValueError('Configuración privada inválida; se requieren archivos regulares 0600.')
        values.append(path.read_text().strip())
    owner = json.loads(values[0])
    token = values[1]
    if not re.fullmatch(r'\d{5,20}:[A-Za-z0-9_-]{20,100}', token):
        raise ValueError('Token del bot inválido.')
    config = {key: owner[key] for key in ('telegram_user_id', 'telegram_chat_id', 'erp_user_id')}
    if any(type(value) is not int or not 0 < value < 2**63 for value in config.values()):
        raise ValueError('Propietario privado no configurado.')
    bot_id = int(token.split(':')[0])
    if not 0 < bot_id < 2**63:
        raise ValueError('Bot ID inválido.')
    config.update(bot_id=bot_id, token=token)
    return config


class TelegramClient:
    def __init__(self, config):
        self.token = config['token']

    def call(self, method, **payload):
        deadline = time.monotonic() + 8
        if method not in {'getUpdates', 'sendMessage', 'answerCallbackQuery'}:
            raise ValueError('Método no permitido.')
        try:
            with requests.Session() as session:
                session.trust_env = False
                with session.post(f'https://api.telegram.org/bot{self.token}/{method}', json=payload,
                                  timeout=(3, 5), allow_redirects=False, stream=True) as response:
                    if response.status_code != 200:
                        raise TelegramUnavailable('Telegram no disponible; intento conservado.')
                    body = bytearray()
                    for chunk in response.iter_content(8192):
                        body.extend(chunk)
                        if len(body) > 262144 or time.monotonic() > deadline:
                            raise TelegramUnavailable('Respuesta Telegram demasiado grande.')
                    result = json.loads(body)
                    if not isinstance(result, dict) or result.get('ok') is not True:
                        raise TelegramUnavailable('Telegram no confirmó la operación.')
                    return result['result']
        except (requests.RequestException, ValueError, KeyError):
            # Never expose requests exceptions: their URL contains the bot credential.
            raise TelegramUnavailable('Telegram no disponible; intento conservado.') from None


@contextmanager
def consumer_lock(bot_id):
    key = int.from_bytes(hashlib.sha256(f'hours-bot:{bot_id}'.encode()).digest()[:8], 'big', signed=True)
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_try_advisory_lock(%s)', [key])
        locked = cursor.fetchone()[0]
    try:
        yield locked
    finally:
        if locked:
            with connection.cursor() as cursor:
                cursor.execute('SELECT pg_advisory_unlock(%s)', [key])


def bounded_input(payload):
    message = payload.get('message') or payload.get('callback_query', {}).get('message', {})
    clean_message = {key: message[key] for key in ('message_id', 'date', 'chat', 'from', 'text') if key in message}
    clean_message['chat'] = {key: message['chat'][key] for key in ('id', 'type')}
    if 'from' in clean_message:
        clean_message['from'] = {'id': message['from']['id']}
    for kind in ('voice', 'audio'):
        media = message.get(kind)
        if isinstance(media, dict):
            clean_message[kind] = {key: value for key, value in media.items()
                if (key in ('file_id', 'file_unique_id') and isinstance(value, str) and len(value) <= 512)
                or (key in ('duration', 'file_size') and type(value) is int and 0 <= value <= 100_000_000)}
    if 'callback_query' in payload:
        callback = payload['callback_query']
        return {'update_id': payload['update_id'], 'callback_query': {
            'id': str(callback.get('id', ''))[:128], 'from': {'id': callback['from']['id']},
            'data': callback.get('data', '')[:64], 'message': clean_message}}
    return {'update_id': payload['update_id'], 'message': clean_message}


def authorized(payload, config):
    if not isinstance(payload, dict):
        return False
    callback = payload.get('callback_query')
    if callback is not None and not isinstance(callback, dict):
        return False
    message = callback.get('message', {}) if callback else payload.get('message', {})
    if not isinstance(message, dict) or not isinstance(message.get('chat'), dict):
        return False
    sender = callback.get('from', {}) if callback else message.get('from', {})
    if not isinstance(sender, dict) or type(message.get('message_id')) is not int or not 0 < message['message_id'] < 2**63:
        return False
    if not callback and (type(message.get('date')) is not int or not 0 < message['date'] < 4102444800
            or ('text' in message and not isinstance(message['text'], str))):
        return False
    if callback and (not isinstance(callback.get('data'), str) or len(callback['data']) > 64):
        return False
    return (sender.get('id') == config['telegram_user_id'] and
            message.get('chat', {}).get('id') == config['telegram_chat_id'] and
            message.get('chat', {}).get('type') == 'private')


@transaction.atomic
def persist_updates(config, updates):
    cursor, _ = TelegramHoursCursor.objects.get_or_create(bot_id=config['bot_id'])
    cursor = TelegramHoursCursor.objects.select_for_update().get(pk=cursor.pk)
    if not isinstance(updates, list) or len(updates) > 20:
        raise TelegramUnavailable('Lote Telegram inválido.')
    for payload in updates:
        if not isinstance(payload, dict):
            continue
        update_id = payload.get('update_id')
        if type(update_id) is not int or not 0 <= update_id < 2**63 - 1 or update_id < cursor.next_update_id:
            continue
        if authorized(payload, config):
            message = payload.get('message') or payload.get('callback_query', {}).get('message', {})
            # Telegram inputs are bounded by the transport; keep voice metadata for later transcription.
            TelegramHoursUpdate.objects.get_or_create(bot_id=config['bot_id'], update_id=update_id,
                defaults={'telegram_user_id': config['telegram_user_id'], 'chat_id': config['telegram_chat_id'],
                          'message_id': message.get('message_id', 0), 'payload': bounded_input(payload)})
        cursor.next_update_id = max(cursor.next_update_id, update_id + 1)
    cursor.save(update_fields=['next_update_id'])


def higher_priority_conflicts(branch_ids, rule):
    conflicts = []
    rows = SolicitudHorarioEspecial.objects.filter(approved_at__isnull=False, cancelled_at__isnull=True,
        status__in=['APROBADO', 'EJECUTADO', 'FALLIDO'])
    for obj in rows:
        if obj.canonical_payload.get('schema_version') != 2:
            if obj.details.filter(sucursal_id__in=branch_ids, target_date__gte=rule['start_date']).exists():
                conflicts.append(obj.request_code)
            continue
        if not set(branch_ids).intersection(row['branch_id'] for row in obj.canonical_payload['locations']):
            continue
        for old in obj.canonical_payload['rules']:
            if PRECEDENCE[old['kind']] <= PRECEDENCE[rule['kind']]:
                continue
            start = max(old['start_date'], rule['start_date'])
            end = min(old.get('end_date') or old['start_date'], rule.get('end_date') or '9999-12-31')
            if start <= end and (rule['kind'] != 'weekly' or any(
                (date.fromisoformat(start) + timedelta(days=i)).weekday() in rule['weekdays']
                for i in range(min(7, (date.fromisoformat(end) - date.fromisoformat(start)).days + 1)))):
                conflicts.append(obj.request_code)
    return sorted(set(conflicts))


def preview_text(obj):
    payload = obj.canonical_payload
    lines = ['Vista previa · NO aplicada', 'Sucursales: ' + ', '.join(row['branch_code'] for row in payload['locations'])]
    for rule in payload['rules']:
        dates = rule['start_date']
        if rule['kind'] == 'weekly':
            dates = ', '.join(WEEKDAYS[d] for d in rule['weekdays']) + ' desde ' + dates
        if rule.get('end_date'):
            dates += ' hasta ' + rule['end_date'] + ' (inclusive)'
        elif rule['kind'] == 'weekly':
            dates += ' sin fecha final'
        hours = 'CERRADO todo el día' if rule['closed_all_day'] else ', '.join(w['open'] + '–' + w['close'] for w in rule['time_windows'])
        lines.append(dates + ': ' + hours)
    lines.extend(['Zona: America/Mazatlan.', 'Aprobar actualiza el horario central para Google, tienda y Maya. Cada destino se confirma con su propia lectura.',
                  obj.execution_summary_json.get('google_preflight', 'Google: comprobación previa pendiente.'),
                  'Vence en 20 minutos. Para corregir envía la instrucción completa.', 'Revisión: ' + payload['revision']])
    return '\n'.join(lines)


def consumer_receipts(obj):
    from horarios_especiales.services.consumer_receipts import consumer_receipts as read_receipts
    return read_receipts(obj)


def status_text(obj):
    lines = [f'{obj.request_code} · ERP: {obj.status}']
    details = obj.execution_summary_json.get('details', [])
    labels = {'GOOGLE_CONFIRMED': 'confirmado por lectura Google', 'PROGRAMMED': 'programado, aún pendiente',
              'FAILED': 'falló; requiere reconciliación', 'READBACK_MISMATCH': 'no confirmado',
              'SUPERSEDED': 'solicitud sustituida por otra revisión; no se confirmó este horario',
              'PARTIALLY_SUPERSEDED': 'vigente con cambios posteriores; no se confirmó toda esta solicitud',
              'NO_APPLICABLE_DATES': 'sin fechas aplicables', 'PENDING_RECONCILE': 'pendiente de reconciliación'}
    lines += [f"Google {row.get('branch_code', '')}: {labels.get(row.get('status'), 'pendiente')}" for row in details]
    if not details:
        lines.append('Google: pendiente, sin recibo de publicación.')
    return '\n'.join(lines + consumer_receipts(obj))


def invalidate_pending(config, actor):
    for old in owner_updates(config).select_for_update(of=('self',)).filter(state__in=['PREVIEW', 'PREPARED']):
        cancel_request(request_obj=old.request, actor=actor, reason='Reemplazada o cancelada desde Telegram')
        old.state = 'CANCELLED'
        old.nonce = ''
        old.save(update_fields=['state', 'nonce'])


@transaction.atomic
def prepare_text(row, text, config, actor):
    # Invalidating first prevents old buttons from approving after an ambiguous correction.
    invalidate_pending(config, actor)
    try:
        ids, rules = parse_instruction(text, row.payload['message']['date'])
        conflicts = higher_priority_conflicts(ids, rules[0])
        if conflicts:
            raise ValueError('Hay excepciones de mayor prioridad: ' + ', '.join(conflicts) + '. Envía una instrucción con fecha exacta para cambiarlas.')
        obj, payload = create_request_from_rules(raw_text=text, branch_ids=ids, rules=rules, actor=actor)
        from horarios_especiales.services.google_projection import preview_google_conflicts
        obj.execution_summary_json['google_preflight'] = preview_google_conflicts(obj)
        obj.save(update_fields=['execution_summary_json'])
    except (ValueError, OverflowError, KeyError, TypeError) as exc:
        row.receipt = {'text': 'No preparé una aprobación. ' + (str(exc) if isinstance(exc, ValueError) else EXAMPLES)}
        row.state = 'REPLY'
    else:
        row.request = obj
        row.revision = payload['revision']
        row.nonce = secrets.token_hex(16)
        row.expires_at = timezone.now() + timedelta(minutes=20)
        row.receipt = {'text': preview_text(obj)}
        row.state = 'PREPARED'
    row.save()


@transaction.atomic
def process_callback(row, config, actor):
    actor = get_user_model().objects.get(pk=config['erp_user_id'])
    require_approver(actor)
    callback = row.payload['callback_query']
    match = re.fullmatch(r'approve:(\d+):([a-f0-9]{32})', callback.get('data', ''))
    old = TelegramHoursUpdate.objects.select_for_update().filter(pk=int(match[1])).first() if match and 0 < int(match[1]) < 2**63 else None
    valid = authorized(row.payload, config) and old and old.request_id and old.request.requested_by_id == actor.pk and old.bot_id == config['bot_id'] and old.telegram_user_id == row.telegram_user_id and old.chat_id == row.chat_id and old.preview_message_id == row.message_id and secrets.compare_digest(old.nonce, match[2])
    if valid and old.state == 'PREVIEW' and old.expires_at and old.expires_at > timezone.now():
        try:
            from horarios_especiales.services.requests import _lock_request_branches
            _lock_request_branches(old.request)
            if higher_priority_conflicts([item['branch_id'] for item in old.request.canonical_payload['locations']], old.request.canonical_payload['rules'][0]):
                raise ValueError('La prioridad cambió después de preparar la vista.')
            from horarios_especiales.services.google_projection import preview_google_conflicts
            preview_google_conflicts(old.request)
            approve_request(request_obj=old.request, actor=actor, expected_revision=old.revision)
        except ValueError as exc:
            text = 'No se aprobó: ' + str(exc) + ' Envía de nuevo la instrucción completa.'
        else:
            old.state = 'APPROVED'
            old.save(update_fields=['state'])
            text = 'Aprobación registrada en ERP. Google pendiente; tienda y Maya pendientes de verificación. Consulta /status.'
    else:
        text = 'Botón vencido, reemplazado o ya procesado. No se registró otra aprobación. Consulta /status.'
    row.state, row.receipt = 'REPLY', {'text': text}
    row.save(update_fields=['state', 'receipt'])


def process_received(row, config, actor):
    if not authorized(row.payload, config):
        row.state = 'REJECTED'
        row.save(update_fields=['state'])
        return
    if 'callback_query' in row.payload:
        process_callback(row, config, actor)
        return
    message = row.payload.get('message', {})
    text = message.get('text', '').strip()
    if text and not text.startswith('/'):
        prepare_text(row, text, config, actor)
        return
    if text in ('/start', '/help'):
        reply = 'Agente privado de horarios. Revisa y aprueba cada vista previa antes de aplicar. /status /cancel\n' + EXAMPLES
    elif text == '/cancel':
        with transaction.atomic():
            invalidate_pending(config, actor)
        reply = 'Vistas previas pendientes canceladas. Las solicitudes aprobadas se conservan; para cambiarlas envía una nueva instrucción.'
    elif text == '/status':
        last = owner_updates(config).filter(request__isnull=False).order_by('-pk').first()
        reply = status_text(last.request) if last else 'Sin solicitudes todavía.'
    elif message.get('voice') or message.get('audio'):
        reply = 'Recibí el audio. La transcripción está en preparación; todavía no creé una solicitud. Por ahora envía texto. ' + EXAMPLES
    else:
        reply = EXAMPLES
    row.state, row.receipt = 'REPLY', {'text': reply}
    row.save(update_fields=['state', 'receipt'])


def deliver(row, client, config):
    if row.telegram_user_id != config['telegram_user_id'] or row.chat_id != config['telegram_chat_id'] or row.bot_id != config['bot_id']:
        raise PermissionDenied('El propietario Telegram cambió.')
    payload = {'chat_id': row.chat_id, 'text': row.receipt['text']}
    if row.state == 'PREPARED':
        payload['reply_markup'] = {'inline_keyboard': [[{'text': 'Aprobar esta revisión', 'callback_data': f'approve:{row.pk}:{row.nonce}'}]]}
    result = client.call('sendMessage', **payload)
    if (not isinstance(result, dict) or type(result.get('message_id')) is not int or result['message_id'] <= 0
            or not isinstance(result.get('chat'), dict) or not isinstance(result.get('from'), dict)
            or result['chat'].get('id') != row.chat_id or result.get('chat', {}).get('type') != 'private'
            or result.get('from', {}).get('id') != config['bot_id'] or result.get('from', {}).get('is_bot') is not True):
        raise TelegramUnavailable('Telegram no confirmó el mensaje privado esperado.')
    if row.state == 'PREPARED':
        row.preview_message_id = result['message_id']
        row.state = 'PREVIEW'
    else:
        row.state = 'DONE'
    row.save(update_fields=['state', 'preview_message_id'])


def recover_approved(config):
    from horarios_especiales.tasks import execute_special_hours_request_task
    require_approver(get_user_model().objects.get(pk=config['erp_user_id']))
    for row in owner_updates(config).filter(state='APPROVED').select_related('request')[:20]:
        if not row.request.cancelled_at:
            # Crash after enqueue may enqueue again; the publisher locks and reconciles against readback.
            try:
                execute_special_hours_request_task.delay(request_id=row.request_id, actor_id=config['erp_user_id'])
            except Exception:
                continue
        row.state = 'QUEUED'
        row.save(update_fields=['state'])


def poll_once(config=None, client=None):
    deadline = time.monotonic() + 25
    config = config or load_config()
    actor = get_user_model().objects.get(pk=config['erp_user_id'], is_active=True)
    require_approver(actor)
    client = client or TelegramClient(config)
    with consumer_lock(config['bot_id']) as locked:
        if not locked:
            return {'busy': True}
        recover_approved(config)
        cursor, _ = TelegramHoursCursor.objects.get_or_create(bot_id=config['bot_id'])
        updates = client.call('getUpdates', offset=cursor.next_update_id, timeout=0, limit=20, allowed_updates=['message', 'callback_query'])
        persist_updates(config, updates)
        for row in owner_updates(config).filter(state__in=['RECEIVED', 'PREPARED', 'REPLY']).order_by('update_id')[:20]:
            if time.monotonic() > deadline:
                break
            if row.state == 'RECEIVED':
                process_received(row, config, actor)
            if row.state in ('PREPARED', 'REPLY'):
                deliver(row, client, config)
        recover_approved(config)
        for row in owner_updates(config).filter(state='QUEUED', created_at__gte=timezone.now() - timedelta(days=7)).select_related('request').order_by('-pk')[:20]:
            if time.monotonic() > deadline:
                break
            text = status_text(row.request)
            if row.receipt.get('last_status') != text:
                client.call('sendMessage', chat_id=row.chat_id, text=text)
                row.receipt['last_status'] = text
                row.save(update_fields=['receipt'])
    return {'polled': True}
