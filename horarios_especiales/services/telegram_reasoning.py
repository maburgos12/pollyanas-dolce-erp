"""Interpret only edits to the sender's unapproved draft through the local router."""
import re
from datetime import date, timedelta
from uuid import NAMESPACE_URL, uuid5

from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from horarios_especiales.models import SucursalAlias, normalize_text
from horarios_especiales.services.branch_resolution import resolve_branch_token
from horarios_especiales.services.command_parser import MONTHS, _parse_time_value
from horarios_especiales.services.google_projection import approved_rules
from horarios_especiales.services.rules import indexed_branch_rules, projection_rank, rule_applies, validate_rule_payload
from horarios_especiales.services.telegram_parser import WEEKDAYS
from orquestacion.models import AgentDefinition, AgentTask
from orquestacion.services import hours_intent, local_router

CLARIFY = (
    'Conservé tu borrador y pausé su botón. Indica la sucursal y la corrección. '
    'Por ejemplo, «El Túnel sigue cerrado los miércoles» o «Bamoa cierra a las 6 p. m.».'
)


MONTH_LABELS = {**MONTHS, **dict(zip(
    ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'), range(1, 13)
)), 'sept': 9, 'set': 9}
WEEKDAY_PATTERN = '|'.join(WEEKDAYS)
MONTH_PATTERN = '|'.join(sorted(MONTH_LABELS, key=len, reverse=True))
DATE_REFERENCE = re.compile(
    rf'\b(?:(?P<weekday>{WEEKDAY_PATTERN})\s*,?\s+)?'
    rf'(?:(?P<iso>\d{{4}}-\d{{2}}-\d{{2}})|'
    rf'(?P<day>\d{{1,2}})\s+(?:de\s+)?(?P<month>{MONTH_PATTERN})\b\.?'
    rf'(?:\s+(?:de\s+)?(?P<year>\d{{4}}))?|(?P<bare>\d{{1,2}}))\b'
)
TIME_RANGE = r'([0-2]\d:[0-5]\d)\s*[-–—]\s*([0-2]\d:[0-5]\d)'


class CorrectionNeedsInterpretation(ValueError):
    """A correction that could not be mapped to a unique allowed scope."""


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
                       'text': 'Recibí tu corrección. La IA local está revisándola junto con tu borrador.'}
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


def replacement_mode(source):
    change = r'(?:reemplaz|sustitu|corrig|correg|ajust|modific|actualiz)\w*'
    if re.search(rf'\b(?:no\s+(?:{change}|conserv\w*|mantien\w*)|confirmar|verificar|preguntar)\b', source):
        return None
    if ((re.search(r'\b(?:solo|solamente|unic[oa])\b.{0,80}\b(?:conserv\w*|mantien\w*)\b', source))
            or (re.search(r'\bresto\b', source) and re.search(r'\b(?:conserv\w*|mantien\w*)\b', source))):
        return 'all'
    if (re.search(rf'\b{change}\b', source)
            and re.search(r'\bnuevo horario\b|\bhorario (?:del borrador|propuesto|que se propone)\b', source)
            and re.search(r'\b(?:conserv\w*|mantien\w*|sigue\s+cerrad[oa])\b', source)):
        return 'dates'
    return None


def explicit_replacement(payload, text):
    """Fast path only when the instruction identifies a branch and a single keep-day."""
    if not replacement_mode(normalize_text(text)):
        return None
    names = {}
    for location in payload['locations']:
        names[location['branch_id']] = {normalize_text(location[key]) for key in ('branch_code', 'branch_name')}
    for pk, alias in SucursalAlias.objects.filter(sucursal_id__in=names, is_active=True).values_list(
            'sucursal_id', 'alias_normalizado'):
        names[pk].add(alias)
    words = list(re.finditer(r'\w+', text))
    spans = {}
    days = {}
    for i, word in enumerate(words):
        weekday = normalize_text(word[0])
        if weekday in WEEKDAYS:
            days[weekday] = word[0]
        for j in range(i, min(i + 6, len(words))):
            span = text[word.start():words[j].end()]
            normalized = normalize_text(span)
            for pk, labels in names.items():
                if normalized in labels and len(span) > len(spans.get(pk, '')):
                    spans[pk] = span
    if len(spans) != 1 or len(days) != 1:
        return None
    return {'action': 'replace_with_draft', 'branch': next(iter(spans.values())),
            'detail': next(iter(days.values()))}


def calendar_references(source, period):
    """Bind recognizable calendar dates to the draft before checking bare day numbers."""
    retained, replaced, residual = set(), set(), []
    cursor = 0
    for match in DATE_REFERENCE.finditer(source):
        label = match['weekday']
        if match['bare'] and not label:
            continue
        if match['bare'] and re.match(r'\s+de\s+\w+', source[match.end():]):
            raise CorrectionNeedsInterpretation('No pude identificar el mes de la fecha que indicas.')
        if match['iso']:
            try:
                candidates = [date.fromisoformat(match['iso'])]
            except ValueError as exc:
                raise ValueError('La fecha indicada no existe en el calendario.') from exc
        else:
            day = int(match['bare'] or match['day'])
            month = MONTH_LABELS.get(match['month'])
            year = int(match['year']) if match['year'] else None
            candidates = [d for d in period if d.day == day
                          and (month is None or d.month == month) and (year is None or d.year == year)]
        if len(candidates) != 1 or candidates[0] not in period:
            raise ValueError('La fecha indicada no corresponde a un solo día del periodo del borrador.')
        day = candidates[0]
        if label and day.weekday() != WEEKDAYS.index(label):
            raise ValueError('El día de la semana no coincide con la fecha indicada.')
        (retained if label else replaced).add(day.isoformat())
        residual.append(source[cursor:match.start()])
        residual.append(' ')
        cursor = match.end()
    residual.append(source[cursor:])
    return retained, replaced, ''.join(residual)


def replace_dates(rule, source, weekday, approved):
    mode = replacement_mode(source)
    if not mode or rule['kind'] != 'range' or 'branch_ids' not in rule or rule['closed_all_day']:
        raise ValueError('Indica qué fechas se reemplazan con el horario de este borrador.')
    start, end = (date.fromisoformat(rule[key]) for key in ('start_date', 'end_date'))
    period = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    if {day for day in WEEKDAYS if re.search(r'\b' + day + r'\b', source)} != {weekday}:
        raise ValueError('Indica un solo día de cierre que se conserva.')
    dated_keep, dated_replace, remainder = calendar_references(source, period)
    keep = dated_keep or {day.isoformat() for day in period if day.weekday() == WEEKDAYS.index(weekday)}
    for value in keep:
        day = date.fromisoformat(value)
        winner = max((row for row in approved if rule_applies(row[1], day)),
                     key=lambda row: projection_rank(row, day), default=None)
        if not winner or not winner[1]['closed_all_day']:
            raise ValueError('Ese día no figura como cerrado en el horario previamente aprobado.')
    if not keep:
        raise ValueError('El día que quieres conservar queda fuera del periodo.')
    previous = set(rule.get('excluded_dates', ()))
    overridden = set(rule.get('override_dates', ()))
    candidates = previous - keep
    numbers = {int(raw) for raw in re.findall(r'\b\d+\b', remainder)}
    if mode == 'dates':
        mapped = {number: [day.isoformat() for day in period if day.day == number] for number in numbers}
        if any(len(values) != 1 for values in mapped.values()):
            raise CorrectionNeedsInterpretation('Indica fechas únicas dentro del periodo.')
        replace = dated_replace | {values[0] for values in mapped.values()}
        if not replace or not replace <= candidates | overridden:
            raise CorrectionNeedsInterpretation('Indica qué fechas conservadas deben tomar el horario nuevo.')
    else:
        named_days = {date.fromisoformat(value).day for value in candidates | overridden} | {start.day}
        if numbers - named_days or dated_replace - (candidates | overridden | {start.isoformat()}):
            raise CorrectionNeedsInterpretation('Identifica el cierre que se conserva y las fechas que toman el horario nuevo.')
        replace = candidates
    updated = {**rule, 'excluded_dates': sorted((previous | keep) - replace)}
    overrides = sorted((overridden | replace) - keep)
    if overrides:
        updated['override_dates'] = overrides
    else:
        updated.pop('override_dates', None)
    return updated


def copied_schedule_change(text):
    """Read preserved rows copied from Telegram and the instruction to change them."""
    head, *quotes = text.strip().split('•')
    use_proposed = re.fullmatch(
        r'(?:(?:te\s+)?falta\s+modificar|(?:modifica|cambia)(?:\s+estas\s+fechas)?)'
        r'(?:\s+horarios\s+que\s+se\s+conservan)?[:.]?',
        normalize_text(head.replace('**', '')),
    )
    if (head.strip() and not use_proposed) or not quotes:
        return None
    rows = []
    for quote in quotes:
        parts = quote.strip().split('·', 2)
        if len(parts) != 3:
            return None
        branch, date_label, hours = parts
        match = re.fullmatch(TIME_RANGE + r'\s*(.*)', normalize_text(hours.replace('**', '')))
        if not match:
            return None
        old_open, old_close, correction = match.groups()
        if correction and len(rows) + 1 != len(quotes):
            return None
        rows.append((branch.strip(), normalize_text(date_label), {'open': old_open, 'close': old_close}))
    new = re.fullmatch(
        r'(?:no,\s*)?(?:(?:esas|esos|esas fechas|esos dias)\s+)?(?:tambien\s+)?'
        r'(?:(?:se|de)\s+)?(?:modifican|cambian|ajustan)\s+(?:de|a)\s+' + TIME_RANGE + r'\.?',
        correction,
    )
    if new:
        return rows, {'open': new[1], 'close': new[2]}
    if use_proposed and not correction:
        return rows, None
    return None


def revise_copied_dates(payload, copied):
    quotes, requested_window = copied
    if validate_rule_payload(payload):
        raise ValueError('El borrador perdió vigencia. Envía la instrucción completa.')
    reference = date.fromisoformat(payload['rules'][0]['start_date'])
    branches = set()
    for branch_name, _, _ in quotes:
        matches, errors = resolve_branch_token(branch_name, reference_date=reference)
        if errors or len(matches) != 1 or matches[0].matched_by not in {'codigo_exacto', 'nombre_exacto', 'alias_exacto'}:
            raise ValueError('Indica el nombre exacto de la sucursal.')
        branches.add(matches[0].branch)
    if len(branches) != 1:
        raise ValueError('Copia las fechas de una sola sucursal por corrección.')
    branch = branches.pop()
    scoped = indexed_branch_rules(payload, branch.pk)
    if len(scoped) != 1:
        raise ValueError('La sucursal necesita una instrucción completa para este periodo.')
    index, rule = scoped[0]
    if (rule['kind'] != 'range' or 'branch_ids' not in rule or rule['closed_all_day']
            or len(rule['time_windows']) != 1
            or requested_window is not None and rule['time_windows'] != [requested_window]):
        raise ValueError('El horario nuevo debe coincidir con el horario propuesto para la sucursal.')
    start, end = (date.fromisoformat(rule[key]) for key in ('start_date', 'end_date'))
    period = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    approved = approved_rules(branch.pk)
    excluded = set(rule.get('excluded_dates', ()))
    replaced = set()
    for _, label, old_window in quotes:
        dates, unlabelled, residual = calendar_references(label, period)
        if len(dates) != 1 or unlabelled or residual.strip() or not dates <= excluded:
            raise ValueError('Una de las fechas copiadas no figura entre los horarios que se conservan.')
        day = date.fromisoformat(next(iter(dates)))
        winner = max((row for row in approved if rule_applies(row[1], day)),
                     key=lambda row: projection_rank(row, day), default=None)
        if not winner or winner[1]['closed_all_day'] or winner[1]['time_windows'] != [old_window]:
            raise ValueError('El horario copiado cambió. Revisa la vista previa vigente.')
        replaced.update(dates)
    changed = {**rule, 'excluded_dates': sorted(excluded - replaced),
               'override_dates': sorted(set(rule.get('override_dates', ())) | replaced)}
    return revised_branch(payload, index, branch, changed), f'{branch.nombre} toma el horario nuevo en las fechas copiadas.'


def revised_branch(payload, index, branch, changed):
    rules = list(payload['rules'])
    targets = rules[index]['branch_ids']
    if len(targets) > 1:
        rules[index] = {**rules[index], 'branch_ids': [pk for pk in targets if pk != branch.pk]}
        rules.insert(index + 1, {**changed, 'branch_ids': [branch.pk]})
    else:
        rules[index] = changed
    return rules


def revise(payload, data, result):
    hours_intent.validate_output(data, result)
    if validate_rule_payload(payload):
        raise ValueError('El borrador perdió vigencia. Envía la instrucción completa.')
    action = result['action']
    source = normalize_text(data['text'])
    if action == 'clarify':
        raise ValueError('Necesito una corrección específica.')
    replacement = explicit_replacement(payload, data['text'])
    if replacement and action != 'replace_with_draft':
        raise ValueError('No pude comprobar la sustitución de fechas. El borrador sigue pendiente.')
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
    if action == 'replace_with_draft':
        verified = replacement
        detail = normalize_text(result['detail'])
        labels = set(re.findall(rf'\b(?:{WEEKDAY_PATTERN})\b', detail))
        weekday = next(iter(labels)) if len(labels) == 1 else None
        if not verified or normalize_text(verified['detail']) != weekday:
            raise ValueError('Indica una sola sucursal, las fechas que cambian y el día que se conserva.')
        named, name_errors = resolve_branch_token(verified['branch'], reference_date=reference)
        if name_errors or len(named) != 1 or named[0].branch.pk != branch.pk:
            raise ValueError('La sucursal de la corrección no está confirmada.')
        if weekday not in WEEKDAYS:
            raise ValueError('Indica el día de semana que se conserva cerrado.')
        changed = replace_dates(rule, source, weekday, approved_rules(branch.pk))
        note = f'{branch.nombre} toma el horario nuevo en las fechas indicadas. Se conserva el cierre del {verified["detail"].lower()}.'
        return all_ids, revised_branch(payload, index, branch, changed), note
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
            winner = max((r for r in approved if rule_applies(r[1], day)), key=lambda r: projection_rank(r, day), default=None)
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


def apply_correction(row, config, actor, draft, data, result, *, copied=None):
    from horarios_especiales.services import telegram_bot as bot
    from horarios_especiales.services.requests import lock_branch_ids

    lock_branch_ids([item['branch_id'] for item in draft.request.canonical_payload['locations']])
    payload = draft.request.canonical_payload
    if copied:
        rules, note = revise_copied_dates(payload, copied)
        ids = [item['branch_id'] for item in payload['locations']]
    else:
        ids, rules, note = revise(payload, data, result)
    raw = draft.request.raw_command + '\n\nCorrección: ' + data['text']
    bot.build_preview(row, raw, config, actor, ids, rules, note=note, after=row.update_id)


def timely_result(task, deadline):
    if not task or task.status != 'resolved' or not deadline:
        return False
    value = task.output_payload.get('finished_at')
    try:
        finished = parse_datetime(value) if isinstance(value, str) else None
        return bool(finished and timezone.is_aware(finished) and finished <= deadline)
    except ValueError:
        return False


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
    expired = (not row.expires_at or row.expires_at <= timezone.now()) and not timely_result(task, row.expires_at)
    if (not enabled() or expired or not draft or draft.pk != row.receipt['draft_id']
            or draft.revision != row.receipt['draft_revision'] or not task
            or task.status in ('blocked', 'cancelled') or task.output_payload.get('error')):
        row.state, row.receipt = 'REPLY', {'text': CLARIFY}
    elif task.status != 'resolved':
        return
    else:
        try:
            apply_correction(row, config, actor, draft, task.input_payload['data'], task.output_payload.get('result'))
        except ValueError as exc:
            row.state, row.receipt = 'REPLY', {'text': CLARIFY + '\n' + str(exc)}
    row.save()
