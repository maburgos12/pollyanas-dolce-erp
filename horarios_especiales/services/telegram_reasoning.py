"""Interpret only edits to the sender's unapproved draft through the local router."""
import re
from datetime import date, timedelta
from uuid import NAMESPACE_URL, uuid5

from django.db import transaction
from django.utils import timezone

from horarios_especiales.models import normalize_text
from horarios_especiales.services.branch_resolution import resolve_branch_token
from horarios_especiales.services.command_parser import _parse_time_value
from horarios_especiales.services.google_projection import approved_rules
from horarios_especiales.services.rules import indexed_branch_rules, rule_applies, validate_rule_payload
from horarios_especiales.services.telegram_parser import WEEKDAYS
from orquestacion.models import AgentDefinition, AgentTask
from orquestacion.services import hours_intent, local_router

CLARIFY = (
    'Conservé tu borrador y pausé su botón. Indica la sucursal y la corrección. '
    'Por ejemplo, «El Túnel sigue cerrado los miércoles» o «Bamoa cierra a las 6 p. m.».'
)


def enabled():
    return AgentDefinition.objects.filter(code=hours_intent.AGENT, status='active').exists()


def draft_for(config, row):
    from horarios_especiales.services import telegram_bot as bot

    return (bot.owner_updates(config).select_for_update(of=('self',)).select_related('request')
            .filter(update_id__lt=row.update_id, state__in=['PREVIEW', 'PREPARED'],
                    expires_at__gt=timezone.now(), request__status='BORRADOR')
            .order_by('-update_id').first())


def hold(draft):
    if draft.nonce:
        draft.nonce = ''
        draft.save(update_fields=['nonce'])
    if draft.state == 'PREPARED':
        draft.state = 'PREVIEW'
        draft.save(update_fields=['state'])


def queue(row, text, draft):
    from horarios_especiales.services.telegram_bot import preview_schedule

    hold(draft)
    data = {'text': text, 'draft': preview_schedule(draft.request.canonical_payload)}
    try:
        result = local_router.submit(agent=hours_intent.AGENT, kind=hours_intent.KIND, data=data,
            request_id=str(uuid5(NAMESPACE_URL, f'pollyanas-hours/{row.bot_id}/{row.update_id}')))
    except ValueError:
        row.state, row.receipt = 'REPLY', {'text': CLARIFY + ' La IA local no está disponible para esta frase.'}
    else:
        row.state = 'IA_WAIT'
        row.receipt = {'task_id': result['id'], 'draft_id': draft.pk, 'draft_revision': draft.revision,
                       'text': 'Recibí tu corrección. Estoy revisándola con el borrador que ya teníamos.'}
        row.expires_at = min(draft.expires_at, timezone.now() + hours_intent.MAX_WAIT)
    row.save()


def explicit_time(span):
    norm = normalize_text(span).replace('.', '')
    if not re.fullmatch(r'(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)|(?:0\d|1[3-9]|2[0-3]):[0-5]\d)', norm):
        words = ('una', 'dos', 'tres', 'cuatro', 'cinco', 'seis', 'siete', 'ocho', 'nueve', 'diez', 'once', 'doce')
        match = re.fullmatch(r'(\d{1,2}|' + '|'.join(words)
                             + r')(?::([0-5]\d)| y (media|cuarto))? de la (manana|tarde|noche)', norm)
        if not match:
            raise ValueError('Aclara la hora con a. m. o p. m., por ejemplo 7 p. m.')
        hour = int(match[1]) if match[1].isdigit() else words.index(match[1]) + 1
        if (match[4] == 'manana' and hour == 12) or (match[4] == 'noche' and hour < 6) or (match[4] == 'tarde' and hour > 7):
            raise ValueError('Aclara la hora con a. m. o p. m.')
        minute = match[2] or {'media': '30', 'cuarto': '15', None: '00'}[match[3]]
        norm = f'{hour}:{minute} ' + ('am' if match[4] == 'manana' or hour == 12 and match[4] == 'noche' else 'pm')
    return _parse_time_value(norm)


def revise(payload, data, result):
    hours_intent.validate_output(data, result)
    if validate_rule_payload(payload):
        raise ValueError('El borrador perdió vigencia. Envía la instrucción completa.')
    action = result['action']
    source = normalize_text(data['text'])
    if action == 'clarify':
        raise ValueError('Necesito una corrección específica.')
    locations = {item['branch_id']: item for item in payload['locations']}
    all_ids = list(locations)
    reference = date.fromisoformat(payload['rules'][0]['start_date'])
    matches, errors = resolve_branch_token(result['branch'], reference_date=reference)
    if errors or len(matches) != 1 or matches[0].matched_by not in {'codigo_exacto', 'nombre_exacto', 'alias_exacto'}:
        raise ValueError('Indica el nombre exacto de la sucursal.')
    branch = matches[0].branch
    scoped = indexed_branch_rules(payload, branch.pk)
    if branch.pk not in locations or len(scoped) != 1:
        raise ValueError('La sucursal necesita una instrucción completa para este periodo.')
    index, rule = scoped[0]
    rules = list(payload['rules'])
    if action == 'keep_closed':
        weekday = normalize_text(result['detail'])
        if (weekday not in WEEKDAYS or rule['kind'] not in {'date', 'range'}
                or not re.search(r'\bcerrad[oa]s?\b', source) or re.search(r'\b(?:abre|abrir|abiert[oa]s?)\b', source)):
            raise ValueError('Indica el día de semana que quieres conservar.')
        start = date.fromisoformat(rule['start_date'])
        end = date.fromisoformat(rule.get('end_date') or rule['start_date'])
        if (end - start).days >= 31:
            raise ValueError('Indica un periodo de hasta 31 días para revisar el cierre.')
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)
                 if (start + timedelta(days=i)).weekday() == WEEKDAYS.index(weekday)]
        approved = approved_rules(branch.pk)
        for day in dates:
            winner = max((r for r in approved if rule_applies(r[1], day)), key=lambda r: r[0], default=None)
            if not winner or not winner[1]['closed_all_day'] or rule_applies(rule, day) and not rule['closed_all_day']:
                raise ValueError('No pude confirmar ese cierre previo dentro de este borrador.')
        if not dates:
            raise ValueError('El día que indicas queda fuera de este borrador.')
        return all_ids, rules, f'{branch.nombre} sigue cerrado el {result["detail"].lower()} en esta propuesta.'
    if (rule['closed_all_day'] or len(rule['time_windows']) != 1
            or re.search(r'\b(?:no|excepto|salvo|menos|sin)\b', source)):
        raise ValueError('Indica el horario completo de esta sucursal.')
    remainder = normalize_text(data['text'].replace(result['detail'], '', 1).replace(result['branch'], '', 1))
    if (re.search(r'\d|\b(?:y|hoy|manana|semana|lunes|martes|miercoles|jueves|viernes|sabado|domingo|dia|fecha)\b',
                  remainder)
            or action == 'change_open' and not re.search(r'\b(?:abre|abren|abrira|apertura|abrir)\b', remainder)
            or action == 'change_close' and not re.search(r'\b(?:cierra|cierran|cerrara|cierre|cerrar|hasta)\b', remainder)):
        raise ValueError('Indica un solo cambio para todo el periodo del borrador.')
    window = dict(rule['time_windows'][0])
    window['close' if action == 'change_close' else 'open'] = explicit_time(result['detail'])
    changed = {**rule, 'time_windows': [window]}
    targets = rule.get('branch_ids', all_ids)
    if len(targets) > 1:
        if rule['kind'] != 'range' or (date.fromisoformat(rule['end_date']) - date.fromisoformat(rule['start_date'])).days >= 31:
            raise ValueError('Indica un periodo de hasta 31 días para cambiar una sola sucursal.')
        if len(rules) != 1 and 'branch_ids' not in rule:
            raise ValueError('Este borrador tiene varios periodos. Envía la instrucción completa.')
        rules[index] = {**rule, 'branch_ids': [pk for pk in targets if pk != branch.pk]}
        rules.insert(index + 1, {**changed, 'branch_ids': [branch.pk]})
    else:
        rules[index] = changed
    return all_ids, rules, f'{branch.nombre} queda de {window["open"]} a {window["close"]} en este borrador.'


@transaction.atomic
def advance(row, config):
    from horarios_especiales.services import telegram_bot as bot
    from horarios_especiales.services.telegram_audio import stale

    config = bot.config_for_row(config, row)
    if stale(row, config):
        row.state, row.receipt = 'CANCELLED', {}
        row.save(update_fields=['state', 'receipt'])
        return
    actor = bot.actor_for(config)
    task = AgentTask.objects.filter(pk=row.receipt['task_id'], task_type=local_router.TASK,
                                   agent__code=hours_intent.AGENT, input_payload__kind=hours_intent.KIND).first()
    draft = draft_for(config, row)
    expired = not row.expires_at or row.expires_at <= timezone.now()
    if (not enabled() or expired or not draft or draft.pk != row.receipt['draft_id']
            or draft.revision != row.receipt['draft_revision'] or not task
            or task.status in ('blocked', 'cancelled') or task.output_payload.get('error')):
        row.state, row.receipt = 'REPLY', {'text': CLARIFY}
    elif task.status != 'resolved':
        return
    else:
        try:
            from horarios_especiales.services.requests import lock_branch_ids

            lock_branch_ids([item['branch_id'] for item in draft.request.canonical_payload['locations']])
            data = task.input_payload['data']
            ids, rules, note = revise(draft.request.canonical_payload, data, task.output_payload.get('result'))
            # Raw record of the revision contains the original instruction and the author's correction.
            raw = draft.request.raw_command + '\n\nCorrección: ' + data['text']
            bot.build_preview(row, raw, config, actor, ids, rules, note=note, after=row.update_id)
        except ValueError as exc:
            row.state, row.receipt = 'REPLY', {'text': CLARIFY + '\n' + str(exc)}
    row.save()
