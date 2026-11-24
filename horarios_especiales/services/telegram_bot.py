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
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from django.contrib.auth import get_user_model
from django.db import connection, transaction
from django.db.models import F, Q
from django.core.exceptions import PermissionDenied
from core.access import ROLE_ADMIN, ROLE_DG, has_any_role
from django.utils import timezone

from horarios_especiales.models import SolicitudHorarioEspecial, TelegramHoursCursor, TelegramHoursUpdate
from horarios_especiales.services.consumer_receipts import FINALIZED_LABEL
from horarios_especiales.services.requests import approve_request, cancel_request, create_request_from_rules
from horarios_especiales.services.rules import PRECEDENCE, indexed_branch_rules, projection_rank, rule_applies, rule_priority
from horarios_especiales.services.telegram_parser import EXAMPLES, WEEKDAYS, parse_grouped_instruction, parse_instruction


class TelegramUnavailable(Exception):
    pass


IDENTITY_KEYS = ('telegram_user_id', 'telegram_chat_id', 'erp_user_id')


def members(config):
    """The original owner keeps its role requirement; delegates get only own hours."""
    yield {key: config[key] for key in IDENTITY_KEYS}
    yield from config.get('users', [])


def current_config(config):
    root = config.get('_root_config', config)
    return load_config(root['_config_dir']) if root.get('_config_dir') else root


def scoped_config(config, identity):
    return {**{key: value for key, value in config.items() if key not in ('users', '_root_config')},
            **identity, '_root_config': config}


def require_approver(actor, config=None):
    if config and 'approve_own' in config and config['approve_own'] is not True:
        raise PermissionDenied('El acceso a horarios fue deshabilitado.')
    scoped = bool(config and config.get('approve_own') is True and actor and actor.pk == config['erp_user_id'])
    if not actor or not actor.is_active or not (scoped or has_any_role(actor, ROLE_DG, ROLE_ADMIN)):
        raise PermissionDenied('El usuario ERP ya no tiene permiso para aprobar y ejecutar sus horarios.')


def actor_for(config):
    root = current_config(config)
    identity = next((member for member in members(root) if all(member[key] == config[key] for key in IDENTITY_KEYS)), None)
    if identity is None:
        raise PermissionDenied('La vinculación Telegram fue retirada o cambió.')
    actor = get_user_model().objects.filter(pk=identity['erp_user_id']).first()
    require_approver(actor, identity)
    return actor


def active_configs(config):
    root = current_config(config)
    result = []
    for identity in members(root):
        scoped = scoped_config(root, identity)
        try:
            actor_for(scoped)
        except PermissionDenied:
            continue
        result.append(scoped)
    return result


def owner_updates(config):
    return TelegramHoursUpdate.objects.filter(bot_id=config['bot_id'], chat_id=config['telegram_chat_id'],
        telegram_user_id=config['telegram_user_id']).filter(
            Q(erp_user_id=config['erp_user_id']) | Q(erp_user__isnull=True, request__requested_by_id=config['erp_user_id'])
        ).filter(Q(request__isnull=True) | Q(request__requested_by_id=config['erp_user_id']))


def active_updates(config):
    allowed = Q(pk__in=[])
    for scoped in active_configs(config):
        allowed |= Q(pk__in=owner_updates(scoped).values('pk'))
    return TelegramHoursUpdate.objects.filter(allowed)


@transaction.atomic
def discard_revoked_pending(config):
    """Drop leases/buttons of removed identities without changing approved schedules."""
    allowed = active_updates(config).values('pk')
    for row in TelegramHoursUpdate.objects.filter(bot_id=config['bot_id'],
            state__in=['RECEIVED', 'PREPARED', 'PREVIEW', 'REPLY', 'AUDIO_WAIT', 'AUDIO_LOCAL', 'AUDIO_PAID', 'IA_WAIT']
            ).exclude(pk__in=allowed).select_for_update():
        row.state, row.nonce, row.expires_at = 'CANCELLED', '', None
        row.receipt = {key: value for key, value in row.receipt.items() if key in ('reserved_cents', 'sha256')}
        row.save(update_fields=['state', 'nonce', 'expires_at', 'receipt'])


def config_for_row(config, row):
    for scoped in active_configs(config):
        if (row.bot_id == scoped['bot_id'] and row.telegram_user_id == scoped['telegram_user_id']
                and row.chat_id == scoped['telegram_chat_id'] and
                (row.erp_user_id or (row.request.requested_by_id if row.request_id else None)) == scoped['erp_user_id']
                and (not row.request_id or row.request.requested_by_id == scoped['erp_user_id'])):
            return scoped
    raise PermissionDenied('La vinculación de esta solicitud ya no está autorizada.')


def require_request_approver(request_obj):
    """Re-read the private allowlist at execution, including winning contributors."""
    try:
        config = load_config()
    except (OSError, ValueError, KeyError):
        raise PermissionDenied('Configuración Telegram no disponible; publicación detenida.') from None
    for row in request_obj.telegramhoursupdate_set.all():
        try:
            scoped = config_for_row(config, row)
        except PermissionDenied:
            continue
        actor = actor_for(scoped)
        if actor.pk == request_obj.approved_by_id == request_obj.requested_by_id:
            return actor
    raise PermissionDenied('El aprobador original ya no tiene una vinculación Telegram vigente.')


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
    if not isinstance(owner, dict) or not re.fullmatch(r'\d{5,20}:[A-Za-z0-9_-]{20,100}', token):
        raise ValueError('Configuración del bot inválida.')
    config = {key: owner[key] for key in IDENTITY_KEYS}
    users = owner.get('users', [])
    if not isinstance(users, list) or len(users) > 32:
        raise ValueError('Lista de usuarios inválida.')
    config['users'] = users
    seen = {key: set() for key in IDENTITY_KEYS}
    for index, identity in enumerate(members(config)):
        if (not isinstance(identity, dict) or
                (index and (set(identity) != set(IDENTITY_KEYS) | {'approve_own'} or type(identity['approve_own']) is not bool))):
            raise ValueError('Vinculación explícita inválida.')
        for key in IDENTITY_KEYS:
            value = identity.get(key)
            if type(value) is not int or not 0 < value < 2**63 or value in seen[key]:
                raise ValueError('Identidad inválida o duplicada.')
            seen[key].add(value)
        if identity['telegram_chat_id'] != identity['telegram_user_id']:
            raise ValueError('Se requiere el chat privado de la persona vinculada.')
    bot_id = int(token.split(':')[0])
    if not 0 < bot_id < 2**63:
        raise ValueError('Bot ID inválido.')
    config.update(bot_id=bot_id, token=token, _config_dir=str(root))
    return config


class TelegramClient:
    def __init__(self, config):
        self.token = config['token']

    def call(self, method, **payload):
        deadline = time.monotonic() + 8
        if method not in {'getUpdates', 'sendMessage', 'answerCallbackQuery', 'getFile'}:
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
        scoped = next((item for item in active_configs(config) if authorized(payload, item)), None)
        if scoped:
            message = payload.get('message') or payload.get('callback_query', {}).get('message', {})
            # Telegram inputs are bounded by the transport; keep voice metadata for later transcription.
            TelegramHoursUpdate.objects.get_or_create(bot_id=config['bot_id'], update_id=update_id,
                defaults={'telegram_user_id': scoped['telegram_user_id'], 'chat_id': scoped['telegram_chat_id'],
                          'erp_user_id': scoped['erp_user_id'],
                          'message_id': message.get('message_id', 0), 'payload': bounded_input(payload)})
        cursor.next_update_id = max(cursor.next_update_id, update_id + 1)
    cursor.save(update_fields=['next_update_id'])


def higher_priority_conflicts(branch_ids, rule):
    conflicts = []
    rows = SolicitudHorarioEspecial.objects.filter(approved_at__isnull=False, cancelled_at__isnull=True,
        status__in=['APROBADO', 'EJECUTADO', 'FALLIDO'])
    for obj in rows:
        payload = obj.canonical_payload
        if payload.get('schema_version') != 2:
            details = obj.details.filter(sucursal_id__in=branch_ids, target_date__gte=rule['start_date'])
            if rule.get('end_date'):
                details = details.filter(target_date__lte=rule['end_date'])
            if rule['kind'] == 'date':
                details = details.filter(target_date=rule['start_date'])
            if any(rule_applies(rule, day) for day in details.values_list('target_date', flat=True)):
                conflicts.append(obj.request_code)
            continue
        shared = set(branch_ids).intersection(row['branch_id'] for row in payload['locations'])
        if any(any(rule_priority(old, day) > rule_priority(rule, day) for day in overlapping_days(old, rule))
               for branch_id in shared for _, old in indexed_branch_rules(payload, branch_id)):
            conflicts.append(obj.request_code)
    return sorted(set(conflicts))


def retained_dates(branch_id, rule):
    from horarios_especiales.services.google_projection import approved_rules

    rows = approved_rules(branch_id)
    start, end = (date.fromisoformat(rule[key]) for key in ('start_date', 'end_date'))
    kept = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        if not rule_applies(rule, day) or day.isoformat() in rule.get('override_dates', ()):
            continue
        winner = max((row for row in rows if rule_applies(row[1], day)),
                     key=lambda row: projection_rank(row, day), default=None)
        if winner and (rule_priority(winner[1], day) == PRECEDENCE['date'] or winner[1]['closed_all_day']):
            kept.append(day.isoformat())
    return tuple(kept)


def preserve_approved_days(rules):
    result = []
    for rule in rules:
        groups = {}
        for branch_id in rule['branch_ids']:
            groups.setdefault(retained_dates(branch_id, rule), []).append(branch_id)
        for kept, ids in groups.items():
            scoped = {**rule, 'branch_ids': ids}
            excluded = set(rule.get('excluded_dates', ())) | set(kept)
            if excluded:
                days = (date.fromisoformat(rule['end_date']) - date.fromisoformat(rule['start_date'])).days + 1
                if len(excluded) == days:
                    raise ValueError('Una sucursal tiene horarios aprobados durante todo el periodo. Usa otra vigencia o indica las fechas concretas que quieres cambiar.')
                scoped['excluded_dates'] = sorted(excluded)
            result.append(scoped)
    return result


def request_conflicts(payload):
    conflicts = set()
    all_ids = [item['branch_id'] for item in payload['locations']]
    for rule in payload['rules']:
        ids = rule.get('branch_ids', all_ids)
        conflicts.update(higher_priority_conflicts(ids, rule))
        if 'branch_ids' in rule and any(retained_dates(branch_id, rule) for branch_id in ids):
            raise ValueError('Hay nuevos días aprobados que deben conservarse. Pide otra vista previa.')
    return sorted(conflicts)


def overlapping_days(left, right):
    start = max(date.fromisoformat(left['start_date']), date.fromisoformat(right['start_date']))
    def end(rule):
        return date.fromisoformat(rule.get('end_date') or rule['start_date']) if rule['kind'] != 'weekly' or rule.get('end_date') else date.max
    finish = min(end(left), end(right))
    limit = 31 if any(rule.get(field) for rule in (left, right)
                      for field in ('excluded_dates', 'override_dates')) else 7
    for offset in range(min(limit, (finish - start).days + 1)):
        target = start + timedelta(days=offset)
        if rule_applies(left, target) and rule_applies(right, target):
            yield target


def rules_overlap(left, right):
    return next(overlapping_days(left, right), None) is not None


def approval_snapshot(obj):
    """Relevant approved rules, not projection receipts or unrelated branches/dates."""
    target = obj.canonical_payload
    ids = {row['branch_id'] for row in target['locations']}
    snapshot = []
    for old in SolicitudHorarioEspecial.objects.filter(approved_at__isnull=False, cancelled_at__isnull=True,
            status__in=['APROBADO', 'EJECUTADO', 'FALLIDO']).exclude(pk=obj.pk).order_by('pk'):
        payload = old.canonical_payload
        if payload.get('schema_version') == 2:
            shared = ids.intersection(row['branch_id'] for row in payload['locations'])
            overlap = any(rules_overlap(new, prior) for branch_id in shared
                          for _, new in indexed_branch_rules(target, branch_id)
                          for _, prior in indexed_branch_rules(payload, branch_id))
        else:
            overlap = any(rules_overlap(new, {'kind': 'date', 'start_date': day.isoformat()})
                          for branch_id, day in old.details.filter(sucursal_id__in=ids).values_list('sucursal_id', 'target_date')
                          for _, new in indexed_branch_rules(target, branch_id))
        if overlap:
            snapshot.append({'id': old.pk, 'code': old.request_code, 'approved_at': old.approved_at.isoformat(),
                             'revision': hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()})
    return snapshot


def schedule_hours(rule):
    if rule['closed_all_day']:
        return 'Cerrado'
    return ', '.join(window['open'] + '–' + window['close'] for window in rule['time_windows'])


def schedule_date(value, *, weekday=False):
    months = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')
    day = date.fromisoformat(value)
    weekdays = ('lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo')
    prefix = weekdays[day.weekday()] + ' ' if weekday else ''
    return f'{prefix}{day.day} {months[day.month - 1]} {day.year}'


def schedule_period(rule):
    start = schedule_date(rule['start_date'])
    end = schedule_date(rule['end_date']) if rule.get('end_date') else ''
    if rule['kind'] == 'weekly':
        days = ', '.join(WEEKDAYS[day] for day in rule['weekdays'])
        return f'{days.capitalize()} · desde {start}' + (f' hasta {end}' if end else '')
    return f'{start} al {end}' if end and end != start else start


def proposed_period(rule):
    if not rule.get('excluded_dates'):
        return schedule_period(rule)
    first = date.fromisoformat(rule['start_date'])
    last = date.fromisoformat(rule['end_date'])
    spans = []
    for offset in range((last - first).days + 1):
        day = first + timedelta(days=offset)
        if not rule_applies(rule, day):
            continue
        if spans and (day - spans[-1][1]).days == 1:
            spans[-1][1] = day
        else:
            spans.append([day, day])
    return ', '.join(schedule_period({'kind': 'range', 'start_date': start.isoformat(), 'end_date': end.isoformat()})
                     for start, end in spans)


def preview_schedule(payload, *, formatted=False):
    names = {row['branch_id']: row['branch_name'] for row in payload['locations']}
    groups = {}
    for rule in payload['rules']:
        key = (proposed_period(rule), schedule_hours(rule))
        groups.setdefault(key, []).extend(names[pk] for pk in rule.get('branch_ids', names))
    periods = {period for period, hours in groups}
    lines = [next(iter(periods))] if len(periods) == 1 else []
    for (period, hours), branches in groups.items():
        when = f' · {period}' if len(periods) > 1 else ''
        names_text = ', '.join(branches)
        if formatted:
            lines.append(f'<b>{escape(hours)}</b>{escape(when)}\n{escape(names_text)}')
        else:
            lines.append(f'• {hours}{when} · {names_text}')
    return ('\n\n' if formatted else '\n').join(lines)


def preserved_schedule(payload, *, iso_dates=False):
    from horarios_especiales.services.google_projection import approved_rules

    names = {row['branch_id']: row['branch_name'] for row in payload['locations']}
    lines = []
    for rule in payload['rules']:
        if not rule.get('excluded_dates'):
            continue
        for pk in rule['branch_ids']:
            approved = approved_rules(pk)
            for value in rule['excluded_dates']:
                day = date.fromisoformat(value)
                if any(other is not rule and rule_applies(other, day)
                       for _, other in indexed_branch_rules(payload, pk)):
                    continue
                winner = max((row for row in approved if rule_applies(row[1], day)),
                             key=lambda row: projection_rank(row, day), default=None)
                hours = schedule_hours(winner[1]) if winner else 'Horario vigente'
                label = f'{value} ({WEEKDAYS[day.weekday()]})' if iso_dates else schedule_date(value, weekday=True)
                lines.append(f'• {names[pk]} · {label} · {hours}')
    return lines


def preview_text(obj):
    summary = obj.execution_summary_json
    parts = [
        '<b>Vista previa de horarios</b> · NO aplicada',
        '<b>Horarios propuestos</b>\n' + preview_schedule(obj.canonical_payload, formatted=True),
    ]
    preserved = summary.get('preserved', [])
    if preserved:
        parts.append('<b>Horarios que se conservan</b>\n' + '\n'.join(escape(line) for line in preserved))
    if summary.get('correction'):
        parts.append(escape(summary['correction']))
    parts.append('<b>Google</b> · ' + escape(summary.get(
        'google_preflight', 'Comprobación previa pendiente.').removeprefix('Google: ')))
    parts.append('Pulsa <b>Aprobar</b> para iniciar la actualización en Google, tienda y Maya. '
                 'El botón vence en 20 minutos. Puedes responder con una corrección.')
    return '\n\n'.join(parts)


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


def formatted_status(text):
    first, _, tail = text.partition('\n')
    return f'<b>{escape(first)}</b>' + ('\n\n' + escape(tail) if tail else '')


def invalidate_pending(config, actor, *, before=None, preserve_draft=False):
    rows = owner_updates(config).select_for_update(of=('self',))
    if before is not None:
        rows = rows.filter(update_id__lt=before)
    for pending in rows.filter(state__in=['AUDIO_WAIT', 'AUDIO_LOCAL', 'AUDIO_PAID', 'IA_WAIT']):
        pending.state, pending.nonce, pending.expires_at = 'CANCELLED', '', None
        pending.receipt = {key: value for key, value in pending.receipt.items() if key in ('reserved_cents', 'sha256')}
        pending.save(update_fields=['state', 'nonce', 'expires_at', 'receipt'])
    for old in rows.filter(state__in=['PREVIEW', 'PREPARED']).select_related('request'):
        if preserve_draft:
            from horarios_especiales.services.telegram_reasoning import hold
            hold(old)
            continue
        cancel_request(request_obj=old.request, actor=actor, reason='Reemplazada o cancelada desde Telegram')
        old.state = 'CANCELLED'
        old.nonce = ''
        old.save(update_fields=['state', 'nonce'])


@transaction.atomic
def build_preview(row, text, config, actor, ids, rules, *, note='', after=None):
    from horarios_especiales.services.google_projection import preview_google_conflicts
    from horarios_especiales.services.requests import _lock_request_branches

    conflicts = request_conflicts({'locations': [{'branch_id': pk} for pk in ids], 'rules': rules})
    if conflicts:
        raise ValueError('Hay excepciones de mayor prioridad: ' + ', '.join(conflicts) + '. Envía una instrucción con fecha exacta para cambiarlas.')
    obj, payload = create_request_from_rules(raw_text=text, branch_ids=ids, rules=rules, actor=actor)
    _lock_request_branches(obj)
    snapshot = approval_snapshot(obj)
    obj.execution_summary_json = {
        'google_preflight': preview_google_conflicts(obj),
        'preserved': preserved_schedule(payload),
        'correction': note,
    }
    obj.save(update_fields=['execution_summary_json'])
    invalidate_pending(config, actor, before=after)
    row.request = obj
    row.revision = payload['revision']
    row.nonce = secrets.token_hex(16)
    row.expires_at = timezone.now() + timedelta(minutes=20)
    warning = ('\n\n<b>Revisa antes de aprobar</b>\nPuede reemplazar horarios aprobados: '
               + ', '.join(escape(item['code']) for item in snapshot) + '.') if snapshot else ''
    preview = preview_text(obj) + warning
    if len(preview.encode('utf-16-le')) // 2 > 3800:
        raise ValueError('La vista previa completa no cabe en un mensaje. Envía un periodo más corto.')
    row.receipt = {'text': preview, 'parse_mode': 'HTML', 'approval_snapshot': snapshot}
    row.state = 'PREPARED'


@transaction.atomic
def prepare_text(row, text, config, actor, *, strict_branches=False):
    from horarios_especiales.services import telegram_reasoning
    from horarios_especiales.models import normalize_text
    from horarios_especiales.services.requests import lock_branch_ids
    from horarios_especiales.services.telegram_parser import GROUP_HOURS

    config = config_for_row(config, row)
    actor = actor_for(config)
    contextual = telegram_reasoning.enabled()
    invalidate_pending(config, actor, before=row.update_id, preserve_draft=contextual)
    try:
        timestamp = row.payload['message']['date']
        grouped = parse_grouped_instruction(text, timestamp)
        if grouped:
            ids, rules = grouped
        else:
            ids, rules = parse_instruction(text, timestamp, strict_branches=strict_branches)
    except (ValueError, OverflowError, KeyError, TypeError) as exc:
        draft = telegram_reasoning.draft_for(config, row) if contextual else None
        source = normalize_text(text)
        complete_notice = re.search(r'\btodas\b', source) and re.search(GROUP_HOURS, source)
        if contextual and (not draft or complete_notice):
            telegram_reasoning.queue(row, text)
            return
        if draft:
            result = telegram_reasoning.explicit_replacement(draft.request.canonical_payload, text)
            if result is None:
                telegram_reasoning.queue(row, text, draft)
                return
            try:
                telegram_reasoning.apply_correction(row, config, actor, draft, {'text': text}, result)
            except telegram_reasoning.CorrectionNeedsInterpretation:
                telegram_reasoning.queue(row, text, draft)
                return
            except ValueError as exc:
                row.receipt = {'text': telegram_reasoning.CLARIFY + '\n' + str(exc)}
                row.state = 'REPLY'
            row.save()
            return
        row.receipt = {'text': 'No preparé una aprobación. ' + (str(exc) if isinstance(exc, ValueError) else EXAMPLES)}
        row.state = 'REPLY'
    else:
        try:
            if grouped:
                lock_branch_ids(ids)
                rules = preserve_approved_days(rules)
            build_preview(row, text, config, actor, ids, rules, after=row.update_id)
        except (ValueError, OverflowError, KeyError, TypeError) as exc:
            row.receipt = {'text': 'No preparé una aprobación. ' + (str(exc) if isinstance(exc, ValueError) else EXAMPLES)}
            row.state = 'REPLY'
    row.save()


@transaction.atomic
def process_callback(row, config, actor):
    config = config_for_row(config, row)
    actor = actor_for(config)
    callback = row.payload['callback_query']
    match = re.fullmatch(r'approve:(\d+):([a-f0-9]{32})', callback.get('data', ''))
    old = TelegramHoursUpdate.objects.select_for_update().filter(pk=int(match[1])).first() if match and 0 < int(match[1]) < 2**63 else None
    valid = authorized(row.payload, config) and old and old.request_id and old.request.requested_by_id == actor.pk and old.erp_user_id in (None, actor.pk) and old.bot_id == config['bot_id'] and old.telegram_user_id == row.telegram_user_id and old.chat_id == row.chat_id and old.preview_message_id == row.message_id and secrets.compare_digest(old.nonce, match[2])
    if valid and old.state == 'PREVIEW' and old.expires_at and old.expires_at > timezone.now():
        try:
            from horarios_especiales.services.requests import _lock_request_branches
            _lock_request_branches(old.request)
            if old.receipt.get('approval_snapshot') != approval_snapshot(old.request):
                raise ValueError('El horario aprobado cambió después de preparar la vista previa.')
            if request_conflicts(old.request.canonical_payload):
                raise ValueError('La prioridad cambió después de preparar la vista.')
            from horarios_especiales.services.google_projection import preview_google_conflicts
            preview_google_conflicts(old.request)
            actor = actor_for(config)
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
    config = config_for_row(config, row)
    actor = actor_for(config)
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
    parse_mode = None
    if text in ('/start', '/help'):
        reply = ('Agente privado de horarios. Revisa y aprueba cada vista previa antes de aplicar. /status /cancel\n'
                 + EXAMPLES + '\nPara una excepción: Todas de 9 am a 7 pm excepto Bamoa de 9 am a 6 pm durante esta semana.')
    elif text == '/cancel':
        with transaction.atomic():
            invalidate_pending(config, actor)
        reply = 'Vistas previas pendientes canceladas. Las solicitudes aprobadas se conservan; para cambiarlas envía una nueva instrucción.'
    elif text == '/status':
        last = owner_updates(config).filter(request__isnull=False).order_by('-pk').first()
        reply = formatted_status(status_text(last.request)) if last else 'Sin solicitudes todavía.'
        parse_mode = 'HTML' if last else None
    elif message.get('voice') or message.get('audio'):
        from horarios_especiales.services.telegram_audio import queue
        queue(row, config)
        return
    else:
        reply = EXAMPLES
    row.state, row.receipt = 'REPLY', {'text': reply}
    if parse_mode:
        row.receipt['parse_mode'] = parse_mode
    row.save(update_fields=['state', 'receipt'])


def deliver(row, client, config):
    config = config_for_row(config, row)
    actor_for(config)
    if row.telegram_user_id != config['telegram_user_id'] or row.chat_id != config['telegram_chat_id'] or row.bot_id != config['bot_id']:
        raise PermissionDenied('El propietario Telegram cambió.')
    payload = {'chat_id': row.chat_id, 'text': row.receipt['text']}
    if row.receipt.get('parse_mode') == 'HTML':
        payload['parse_mode'] = 'HTML'
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
    elif row.state in ('AUDIO_WAIT', 'IA_WAIT'):
        row.receipt['acknowledged'] = True
    else:
        row.state = 'DONE'
    row.save(update_fields=['state', 'preview_message_id', 'receipt'])


def recover_approved(config):
    from horarios_especiales.tasks import execute_special_hours_request_task
    if not active_configs(config):
        actor_for(config)
    for row in active_updates(config).filter(state='APPROVED').select_related('request')[:20]:
        scoped = config_for_row(config, row)
        actor = actor_for(scoped)
        if not row.request.cancelled_at:
            # Crash after enqueue may enqueue again; the publisher locks and reconciles against readback.
            try:
                execute_special_hours_request_task.delay(request_id=row.request_id, actor_id=actor.pk)
            except Exception:
                continue
        row.state = 'QUEUED'
        row.save(update_fields=['state'])


def poll_once(config=None, client=None):
    deadline = time.monotonic() + 25
    config = config or load_config()
    if not active_configs(config):
        actor_for(config)
    client = client or TelegramClient(config)
    with consumer_lock(config['bot_id']) as locked:
        if not locked:
            return {'busy': True}
        discard_revoked_pending(config)
        recover_approved(config)
        cursor, _ = TelegramHoursCursor.objects.get_or_create(bot_id=config['bot_id'])
        updates = client.call('getUpdates', offset=cursor.next_update_id, timeout=0, limit=20, allowed_updates=['message', 'callback_query'])
        persist_updates(config, updates)
        for row in active_updates(config).filter(state__in=['RECEIVED', 'PREPARED', 'REPLY', 'AUDIO_WAIT', 'IA_WAIT']).order_by('update_id')[:20]:
            if time.monotonic() > deadline:
                break
            if row.state == 'RECEIVED':
                scoped = config_for_row(config, row)
                process_received(row, scoped, actor_for(scoped))
            if row.state == 'IA_WAIT' and row.receipt.get('acknowledged'):
                from horarios_especiales.services.telegram_reasoning import advance
                advance(row, config)
            if row.state in ('PREPARED', 'REPLY') or row.state in ('AUDIO_WAIT', 'IA_WAIT') and not row.receipt.get('acknowledged'):
                deliver(row, client, config)
        from horarios_especiales.services.telegram_audio import advance
        if time.monotonic() <= deadline:
            advance(config)
        recover_approved(config)
        for row in active_updates(config).filter(state='QUEUED').select_related('request').order_by(F('receipt__status_checked_at').asc(nulls_first=True), 'pk')[:20]:
            if time.monotonic() > deadline:
                break
            row.receipt['status_checked_at'] = timezone.now().isoformat()
            row.save(update_fields=['receipt'])
            scoped = config_for_row(config, row)
            actor_for(scoped)
            if row.request.cancelled_at or FINALIZED_LABEL in str(row.receipt.get('last_status', '')).splitlines():
                row.state = 'DONE'
                row.save(update_fields=['state'])
                continue
            rules = row.request.canonical_payload.get('rules', [])
            today = timezone.localdate(timezone=ZoneInfo("America/Mazatlan")).isoformat()
            expired = bool(rules) and all(not (rule['kind'] == 'weekly' and not rule.get('end_date')) and
                                         (rule.get('end_date') or rule['start_date']) < today for rule in rules)
            if expired:
                text = f'{row.request.request_code}\nEl periodo de la solicitud terminó. Consulta /status para ver el último estado.'
            else:
                text = status_text(row.request)
                if FINALIZED_LABEL not in text.splitlines():
                    continue
            # Telegram cannot deduplicate a send after an ambiguous timeout.
            # Stop automatic tracking before sending. The result is still available via /status.
            row.state = 'DONE'
            row.receipt['terminal_notice_attempted_at'] = timezone.now().isoformat()
            row.save(update_fields=['state', 'receipt'])
            actor_for(scoped)
            client.call('sendMessage', chat_id=row.chat_id, text=formatted_status(text), parse_mode='HTML',
                        disable_notification=True)
            row.receipt['last_status'] = text
            row.save(update_fields=['receipt'])
    return {'polled': True}
