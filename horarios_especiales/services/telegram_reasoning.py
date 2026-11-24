"""Interpret only edits to the sender's unapproved draft through the local router."""
import re
from datetime import date, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

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
    'Tu borrador sigue pendiente y su botón está en pausa.'
)
INITIAL_CLARIFY = 'No pude preparar la vista previa. Confirma el periodo, las sucursales y los horarios.'


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


def queue(row, text, draft=None):
    from horarios_especiales.services.telegram_bot import preserved_schedule, preview_schedule
    from horarios_especiales.services.telegram_parser import GROUP_HOURS

    now = timezone.now()
    if draft:
        hold(draft)
        payload = draft.request.canonical_payload
        schedule = preview_schedule(payload)
        exceptions = preserved_schedule(payload, iso_dates=True)
        if exceptions:
            schedule += '\nExcepciones que conserva el borrador:\n' + '\n'.join(exceptions)
        data = {'text': text, 'draft': schedule}
    else:
        reference = datetime.fromtimestamp(row.payload['message']['date'], ZoneInfo('America/Mazatlan')).date()
        windows = set()
        for match in re.finditer(GROUP_HOURS, normalize_text(text)):
            try:
                window = f'{_parse_time_value(match[1])}-{_parse_time_value(match[2])}'
                hours_intent.initial_window(window)
            except ValueError:
                continue
            windows.add(window)
        data = {'text': text, 'draft': f'Fecha de referencia en Sinaloa: {reference.isoformat()}',
                'mode': 'initial', 'windows': sorted(windows)}
    try:
        result = local_router.submit(agent=hours_intent.AGENT, kind=hours_intent.KIND, data=data,
            request_id=str(uuid5(NAMESPACE_URL, f'pollyanas-hours/{row.bot_id}/{row.update_id}')))
    except ValueError:
        row.state, row.receipt = 'REPLY', {'text': (CLARIFY if draft else INITIAL_CLARIFY) + ' La IA local no está disponible en este momento.'}
    else:
        row.state = 'IA_WAIT'
        row.receipt = {'task_id': result['id'], 'draft_id': draft.pk if draft else None,
                       'draft_revision': draft.revision if draft else '',
                       'text': ('Recibí tu corrección. La IA local está revisándola junto con tu borrador.' if draft
                                else 'Recibí tu solicitud. La IA local está preparando la vista previa.')}
        row.expires_at = min(draft.expires_at, now + hours_intent.MAX_WAIT) if draft else now + hours_intent.INITIAL_WAIT
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
    if re.search(r'\b(?:no\s+(?:reemplaz\w*|sustitu\w*|conserv\w*|mantien\w*)|confirmar|verificar|preguntar)\b', source):
        return None
    if ((re.search(r'\b(?:solo|solamente|unic[oa])\b.{0,80}\b(?:conserv\w*|mantien\w*)\b', source))
            or (re.search(r'\bresto\b', source) and re.search(r'\b(?:conserv\w*|mantien\w*)\b', source))):
        return 'all'
    if (re.search(r'\b(?:reemplaz\w*|sustitu\w*)\b', source)
            and re.search(r'\bnuevo horario\b|\bhorario (?:del borrador|propuesto|que se propone)\b', source)
            and re.search(r'\b(?:conserv\w*|mantien\w*)\b', source)):
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


def revise(payload, data, result):
    hours_intent.validate_output(data, result)
    if validate_rule_payload(payload):
        raise ValueError('El borrador perdió vigencia. Envía la instrucción completa.')
    action = result['action']
    source = normalize_text(data['text'])
    if action == 'clarify':
        raise ValueError('Necesito una corrección específica.')
    replacement = explicit_replacement(payload, data['text'])
    if replacement and action not in {'replace_with_draft', 'replace_draft_dates'}:
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
    if action in {'replace_with_draft', 'replace_draft_dates'}:
        if action == 'replace_draft_dates':
            selected = set(hours_intent.replacement_dates(result['detail']))
            if (rule['kind'] != 'range' or 'branch_ids' not in rule or rule['closed_all_day']
                    or not selected <= set(rule.get('excluded_dates', ()))):
                raise ValueError('La IA indicó fechas que no son excepciones del borrador.')
            approved = approved_rules(branch.pk)
            replaced = set()
            for value in sorted(selected):
                day = date.fromisoformat(value)
                winner = max((row for row in approved if rule_applies(row[1], day)),
                             key=lambda row: projection_rank(row, day), default=None)
                if not winner:
                    raise ValueError('La IA indicó una fecha cuyo horario previo ya no está vigente.')
                if not winner[1]['closed_all_day']:
                    replaced.add(value)
            if not replaced:
                raise ValueError('Mantengo el cierre. Para abrir ese día, envía una instrucción completa.')
            changed = {**rule, 'excluded_dates': sorted(set(rule['excluded_dates']) - replaced),
                       'override_dates': sorted(set(rule.get('override_dates', ())) | replaced)}
            note = f'{branch.nombre} toma el horario propuesto en las fechas corregidas. Las demás excepciones se conservan.'
        else:
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
        targets = rule['branch_ids']
        if len(targets) > 1:
            rules[index] = {**rule, 'branch_ids': [pk for pk in targets if pk != branch.pk]}
            rules.insert(index + 1, {**changed, 'branch_ids': [branch.pk]})
        else:
            rules[index] = changed
        return all_ids, rules, note
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


def apply_correction(row, config, actor, draft, data, result):
    from horarios_especiales.services import telegram_bot as bot
    from horarios_especiales.services.requests import lock_branch_ids

    lock_branch_ids([item['branch_id'] for item in draft.request.canonical_payload['locations']])
    ids, rules, note = revise(draft.request.canonical_payload, data, result)
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


def initial_schedule(timestamp, data, result):
    """Compile the model's branch/date assignments into the existing approval rules."""
    from horarios_especiales.services.requests import lock_branch_ids
    from horarios_especiales.services.telegram_bot import preserve_approved_days
    from horarios_especiales.services.telegram_parser import parse_instruction

    hours_intent.validate_output(data, result)
    if result['start'] is None or result['end'] is None:
        raise ValueError('Indica el periodo y el horario de cada grupo que cambia.')
    first, last = hours_intent.initial_period(result['start'], result['end'])
    source = normalize_text(data['text'])
    cited_weekdays = {index for index, label in enumerate(WEEKDAYS) if re.search(rf'\b{label}s?\b', source)}
    general = result['general']
    plan = {}
    days = [first + timedelta(days=offset) for offset in range((last - first).days + 1)]
    if general is not None:
        if not re.search(r'\btodas\b', source):
            raise ValueError('La instrucción no confirma que el horario aplique a todas las sucursales.')
        ids, _ = parse_instruction(f'todas del {first} al {last} cerradas', timestamp, strict_branches=True)
        plan = {pk: {day: (32, general) for day in days} for pk in ids}
    for group in result['groups']:
        start, end = hours_intent.initial_period(group['start'] or result['start'], group['end'] or result['end'])
        priority = (end - start).days
        requested = (priority, group['hours'])
        if group['hours'] == 'closed' and start == end and cited_weekdays and start.weekday() not in cited_weekdays:
            raise ValueError('El cierre propuesto no coincide con el día de la semana indicado.')
        for name in group['branches']:
            name = re.sub(r'^sucursal ', '', normalize_text(name))
            if name in {'todas', 'todas las sucursales'} or not re.search(rf'\b{re.escape(name)}\b', source):
                raise ValueError('Confirma el nombre de cada sucursal de la excepción.')
            ids, _ = parse_instruction(f'{name} del {start} al {end} cerrada', timestamp, strict_branches=True)
            if len(ids) != 1:
                raise ValueError('Una sucursal de la excepción es ambigua.')
            selected = plan.setdefault(ids[0], {})
            for day in days:
                if not start <= day <= end:
                    continue
                if group['hours'] == 'closed' and cited_weekdays and start != end and day.weekday() not in cited_weekdays:
                    continue
                current = selected.get(day)
                if current and current[0] == priority and current != requested:
                    raise ValueError('Hay dos horarios distintos para una misma sucursal y fecha.')
                if current is None or priority < current[0]:
                    selected[day] = requested
    lock_branch_ids(plan)
    rules = []
    for branch_id, selected in plan.items():
        approved = approved_rules(branch_id)
        by_window = {}
        for day, (_, window) in selected.items():
            winner = max((row for row in approved if rule_applies(row[1], day)),
                         key=lambda row: projection_rank(row, day), default=None)
            if window == 'closed' and winner and winner[1]['closed_all_day']:
                continue
            replace = bool(winner and not winner[1]['closed_all_day'])
            by_window.setdefault(window, {})[day] = replace
        for window, selected_dates in by_window.items():
            start, end = min(selected_dates), max(selected_dates)
            rule = {**hours_intent.initial_window(window), 'kind': 'range', 'branch_ids': [branch_id],
                    'start_date': start.isoformat(), 'end_date': end.isoformat()}
            excluded = [day.isoformat() for day in days if start <= day <= end and day not in selected_dates]
            overrides = sorted(day.isoformat() for day, replace in selected_dates.items() if replace)
            if excluded:
                rule['excluded_dates'] = excluded
            if overrides:
                rule['override_dates'] = overrides
            rules.append(rule)
    ids = sorted({pk for rule in rules for pk in rule['branch_ids']})
    if not ids:
        raise ValueError('El horario indicado ya está registrado.')
    return ids, preserve_approved_days(rules)


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
    draft_id = row.receipt.get('draft_id')
    draft = draft_for(config, row) if draft_id else None
    clarify = CLARIFY if draft else INITIAL_CLARIFY
    expired = (not row.expires_at or row.expires_at <= timezone.now()) and not timely_result(task, row.expires_at)
    if (not enabled() or expired or draft_id and (not draft or draft.pk != draft_id
            or draft.revision != row.receipt['draft_revision']) or not task
            or task.status in ('blocked', 'cancelled')):
        row.state, row.receipt = 'REPLY', {'text': clarify + ' No recibí una interpretación válida a tiempo.'}
    elif task.status != 'resolved':
        return
    else:
        try:
            data = task.input_payload['data']
            result = task.output_payload.get('result')
            if data.get('mode') == 'initial' and draft is None:
                timestamp = row.payload['message']['date']
                ids, rules = initial_schedule(timestamp, data, result)
                bot.build_preview(row, data['text'], config, actor, ids, rules, after=row.update_id)
            elif draft:
                apply_correction(row, config, actor, draft, data, result)
            else:
                raise ValueError('El borrador de la corrección perdió vigencia.')
        except ValueError as exc:
            row.state, row.receipt = 'REPLY', {'text': clarify + '\n' + str(exc)}
    row.save()
