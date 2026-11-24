"""Schedule approved Marketing photo templates within the supported Story lifetime."""
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from horarios_especiales.services.rules import rule_applies

ZONE = ZoneInfo('America/Mazatlan')
LIFETIME = timedelta(days=1, minutes=10)
TITLE = 'Horario especial de sucursales'


def build_schedule(rules, now):
    if any(rule['kind'] == 'weekly' and not rule.get('end_date') for rule in rules):
        raise ValueError('La historia necesita una fecha final para el horario especial.')
    first = min(date.fromisoformat(rule['start_date']) for rule in rules)
    last = max(date.fromisoformat(rule.get('end_date') or rule['start_date']) for rule in rules)
    if (last - first).days >= 31:
        raise ValueError('La historia admite hasta 31 días de vigencia.')
    now = now.astimezone(ZONE)
    start = datetime.combine(first, time.min, ZONE)
    end = datetime.combine(last + timedelta(days=1), time.min, ZONE)
    review_expires = now + timedelta(minutes=20)
    windows = []
    omitted = []
    for index in range((last - first).days + 1):
        day = first + timedelta(days=index)
        if not any(rule_applies(rule, day) for rule in rules):
            continue
        begin = datetime.combine(day, time(9), ZONE)
        if begin <= review_expires + timedelta(minutes=1) or begin + LIFETIME > end:
            if day >= now.date():
                omitted.append(day.strftime('%d/%m'))
            continue
        windows.append({'scheduled_at': begin.isoformat(), 'ends_at': (begin + LIFETIME).isoformat()})
    if not windows:
        raise ValueError('La vigencia restante no permite publicar una historia de 24 horas antes de que termine el aviso.')
    note = 'Fechas sin una publicación nueva: ' + ', '.join(omitted) + '.' if omitted else ''
    return {
        'timezone': ZONE.key, 'valid_from': start.isoformat(), 'valid_until': end.isoformat(),
        'review_expires_at': review_expires.isoformat(), 'platforms': ['instagram', 'facebook'],
        'placement': 'story', 'publish_mode': 'scheduled', 'schedule_windows': windows,
    }, note
