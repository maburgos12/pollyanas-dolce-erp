"""Conservative text grammar: consume the whole instruction or ask for clarification."""
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from horarios_especiales.models import SucursalPlataformaExterna, normalize_text
from horarios_especiales.services.branch_resolution import resolve_branch_token, split_branch_tokens
from horarios_especiales.services.command_parser import MONTHS, _parse_time_value
from horarios_especiales.services.rules import validate_rules

WEEKDAYS = ('lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo')
TARGET_CODES = ('MATRIZ', 'PAYAN', 'LAS_GLORIAS', 'PLAZA_NIO', 'COLOSIO', 'LEYVA', 'EL_TUNEL', 'BAMOA', 'GUAMUCHIL', 'SINALOA_LEYVA')
DATE = r'(?:\d{4}-\d{2}-\d{2}|\d{1,2} de [a-z]+(?: de \d{4})?|hoy|manana)'
TIME = r'\d{1,2}(?::\d{2})?\s*(?:am|pm)?'
EXAMPLES = 'Ejemplos: Todas hoy cerradas; Plaza Nio mañana de 1 pm a 9 pm; Túnel todos los miércoles cerrado. Envía la instrucción completa.'


def parse_instruction(text, timestamp, *, strict_branches=False):
    if not isinstance(text, str) or not 1 <= len(text) <= 2000:
        raise ValueError(EXAMPLES)
    today = datetime.fromtimestamp(timestamp, ZoneInfo('America/Mazatlan')).date()
    remaining = normalize_text(text).strip(' .')
    remaining = re.sub(r'\b(sabado|domingo)s\b', r'\1', remaining)

    def parse_day(value):
        if value in ('hoy', 'manana'):
            return today + timedelta(days=value == 'manana')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            return date.fromisoformat(value)
        match = re.fullmatch(r'(\d{1,2}) de ([a-z]+)(?: de (\d{4}))?', value)
        return date(int(match[3] or today.year), MONTHS.get(match[2], 0), int(match[1]))

    closed = re.search(r'\s+(?:cerrad[oa]s?(?: todo el dia)?)$', remaining)
    window = re.search(rf'\s+(?:de\s+({TIME})\s+a\s+({TIME})|(?:abre|abrira|abriran)\s+(?:a las\s+)?({TIME})\s+y\s+(?:cierra|cerrara|cerraran)\s+(?:a las\s+)?({TIME}))$', remaining)
    if closed:
        rule = {'closed_all_day': True, 'time_windows': []}
        remaining = remaining[:closed.start()]
    elif window:
        rule = {'closed_all_day': False, 'time_windows': [{'open': _parse_time_value(window[1] or window[3]), 'close': _parse_time_value(window[2] or window[4])}]}
        remaining = remaining[:window.start()]
    else:
        raise ValueError('Indica cerrado todo el día o apertura y cierre con am/pm o HH:MM. ' + EXAMPLES)
    weekly = re.search(rf'\s+(?:todos los|cada)\s+((?:{"|".join(WEEKDAYS)})(?:\s*(?:,|y)\s*(?:{"|".join(WEEKDAYS)}))*)(?:\s+desde\s+({DATE}))?(?:\s+hasta\s+({DATE}))?$', remaining)
    period = re.search(rf'\s+del\s+({DATE})\s+al\s+({DATE})$', remaining)
    single = re.search(rf'\s+(?:el\s+)?(?:({"|".join(WEEKDAYS)})\s+)?({DATE})$', remaining)
    if weekly:
        start = parse_day(weekly[2]) if weekly[2] else today
        rule.update(kind='weekly', start_date=start.isoformat(), weekdays=[i for i, day in enumerate(WEEKDAYS) if re.search(rf'\b{day}\b', weekly[1])])
        if weekly[3]:
            rule['end_date'] = parse_day(weekly[3]).isoformat()
        remaining = remaining[:weekly.start()]
    elif period:
        rule.update(kind='range', start_date=parse_day(period[1]).isoformat(), end_date=parse_day(period[2]).isoformat())
        remaining = remaining[:period.start()]
    elif single:
        day = parse_day(single[2])
        if single[1] and WEEKDAYS[day.weekday()] != single[1]:
            raise ValueError('El día de semana no coincide con la fecha.')
        rule.update(kind='date', start_date=day.isoformat())
        remaining = remaining[:single.start()]
    else:
        raise ValueError('Falta una fecha o recurrencia inequívoca. ' + EXAMPLES)
    validate_rules([rule])
    configs = SucursalPlataformaExterna.objects.filter(platform=SucursalPlataformaExterna.PLATFORM_GOOGLE, is_active=True, sucursal__codigo__in=TARGET_CODES).exclude(external_location_name='')
    allowed = set(configs.values_list('sucursal_id', flat=True))
    if remaining in ('todas', 'todas las sucursales'):
        if len(allowed) != len(TARGET_CODES):
            raise ValueError('Falta configuración Google de alguna de las diez sucursales. No se preparó una selección parcial.')
        ids = sorted(allowed)
    else:
        ids = []
        for token in split_branch_tokens(remaining):
            matches, errors = resolve_branch_token(token, reference_date=date.fromisoformat(rule['start_date']))
            if errors or len(matches) != 1 or matches[0].branch.pk not in allowed or (strict_branches and matches[0].matched_by not in {'codigo_exacto', 'nombre_exacto', 'alias_exacto'}):
                raise ValueError('Sucursal no configurada o ambigua. ' + EXAMPLES)
            ids.append(matches[0].branch.pk)
    if not ids or len(ids) > 10 or len(set(ids)) != len(ids):
        raise ValueError('Indica sucursales únicas con Google configurado.')
    return ids, [rule]

GROUP_SEPARATOR = r'\b(?:excepto|salvo|(?:en|con)\s+excepcion de)\s+'
GROUP_PERIOD = rf'\bdel (?:dia )?(?P<start>{DATE}|\d{{1,2}}) al (?:dia )?(?P<end>{DATE})\b'
GROUP_THIS_WEEK = r'(?:por|durante)\s+esta semana'
GROUP_HOURS = rf'(?:abiert[oa]s?\s+)?(?:de(?:sde(?: las)?)?\s+)?({TIME})\s*(?:\ba\b|[-–—])\s*({TIME})'
NOTICE_INTRO = (
    r'^(?:aviso importante\s+)?'
    r'(?:priorizando la integridad de nuestro equipo de trabajo y de quienes nos visitan,?\s+)?'
    r'les informamos que durante esta semana contaremos con un horario temporal de atencion[.:]?\s+'
)
NOTICE_END = r'\s+seguimos atendiendo sus pedidos con el gusto de siempre dentro de este horario[.!]?\s+[¡!]?gracias por su comprension[!.]*$'


def parse_grouped_instruction(text, timestamp):
    """Return a bounded, fully consumed schedule for the general group and exceptions."""
    if not isinstance(text, str) or not 1 <= len(text) <= 2000:
        return None
    normalized = normalize_text(text).strip(' .')
    if not re.search(GROUP_SEPARATOR, normalized):
        return None
    today = datetime.fromtimestamp(timestamp, ZoneInfo('America/Mazatlan')).date()
    normalized, notice = re.subn(NOTICE_INTRO, '', normalized, count=1)
    if notice:
        normalized = re.sub(NOTICE_END, '', normalized)
    else:
        normalized = re.sub(r'^aviso importante[.:]?\s+', '', normalized)
    period = (
        re.search(rf'\s+(?:por esta semana\s+)?{GROUP_PERIOD}$', normalized)
        or re.match(rf'(?:horario temporal\s+)?{GROUP_PERIOD}[:,]?\s+', normalized)
    )
    if period:
        start = period['start']
        end = period['end']
        if start.isdecimal():
            month = re.fullmatch(r'\d{1,2} (de [a-z]+(?: de \d{4})?)', end)
            if not month:
                raise ValueError('Indica el mes de inicio del periodo.')
            start = f'{start} {month[1]}'
        normalized = (normalized[:period.start()] + normalized[period.end():]).strip()
    elif notice or re.search(rf'\s+{GROUP_THIS_WEEK}$', normalized):
        normalized = re.sub(rf'\s+{GROUP_THIS_WEEK}$', '', normalized)
        monday = today - timedelta(days=today.weekday())
        start, end = monday.isoformat(), (monday + timedelta(days=6)).isoformat()
    else:
        raise ValueError('Indica la fecha de inicio y fin, o escribe «durante esta semana» para el periodo de lunes a domingo.')
    chunks = re.split(GROUP_SEPARATOR, normalized)
    if not 2 <= len(chunks) <= 10:
        raise ValueError('Indica un horario general y hasta nueve grupos de excepciones.')

    def parse_chunk(chunk, general=False):
        match = re.fullmatch(rf'(.+?)\s+{GROUP_HOURS}', chunk.strip(' ,;:.'))
        if not match:
            raise ValueError('Indica apertura y cierre para el horario general y cada grupo de excepciones.')
        branches = match[1]
        if general and branches not in ('todas', 'todas las sucursales'):
            raise ValueError('Para usar excepciones, empieza con «Todas las sucursales» y su horario general.')
        canonical = f'{branches} del {start} al {end} de {match[2]} a {match[3]}'
        return parse_instruction(canonical, timestamp, strict_branches=True)

    all_ids, [base_rule] = parse_chunk(chunks[0], general=True)
    if (date.fromisoformat(base_rule['end_date']) - date.fromisoformat(base_rule['start_date'])).days >= 31:
        raise ValueError('Los avisos con grupos de horarios admiten hasta 31 días. Indica un periodo más corto.')
    exceptions = set()
    rules = []
    for chunk in chunks[1:]:
        ids, [rule] = parse_chunk(chunk)
        if set(ids) & exceptions:
            raise ValueError('Cada sucursal puede aparecer en un solo grupo de excepciones.')
        exceptions.update(ids)
        rules.append({**rule, 'branch_ids': sorted(ids)})
    default_ids = sorted(set(all_ids) - exceptions)
    if not default_ids:
        raise ValueError('Indica al menos una sucursal que conserve el horario general.')
    return all_ids, [{**base_rule, 'branch_ids': default_ids}, *rules]
