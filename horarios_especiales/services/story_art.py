"""Create a vertical notice and schedule only periods compatible with Story lifetime."""
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont
import reportlab

from horarios_especiales.services.rules import rule_applies

ZONE = ZoneInfo('America/Mazatlan')
LIFETIME = timedelta(days=1, minutes=10)
TITLE = 'Horario especial de sucursales'
LOGO = Path(__file__).resolve().parents[2] / 'static/operacion/pollyanas-logo-transparent.png'
# ReportLab is a pinned existing dependency and ships this Unicode font on
# both macOS and the Linux worker. Pillow's default subset loses Spanish text.
FONT = Path(reportlab.__file__).resolve().parent / 'fonts/Vera.ttf'


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


def render_notice(text):
    image = Image.new('RGB', (1080, 1920), '#fff7ee')
    draw = ImageDraw.Draw(image)
    ink, accent = '#46192c', '#823750'
    title_font = ImageFont.truetype(str(FONT), size=68)
    body_font = ImageFont.truetype(str(FONT), size=37)
    footer_font = ImageFont.truetype(str(FONT), size=28)
    with Image.open(LOGO) as original:
        logo = original.convert('RGBA')
        logo.thumbnail((440, 155))
        image.paste(logo, ((1080 - logo.width) // 2, 215), logo)
    draw.text((540, 420), 'Horario especial', font=title_font, fill=ink, anchor='mt')
    draw.line((112, 532, 968, 532), fill=accent, width=4)
    y = 595
    for paragraph in text.splitlines():
        line = ''
        for word in paragraph.split():
            candidate = (line + ' ' + word).strip()
            if draw.textlength(candidate, font=body_font) > 850:
                draw.text((112, y), line, font=body_font, fill=ink)
                y += 53
                line = word
            else:
                line = candidate
        draw.text((112, y), line, font=body_font, fill=ink)
        y += 78
        if y > 1610:
            raise ValueError('El aviso completo no cabe en una historia. Reduce el alcance del aviso.')
    draw.text((540, 1690), 'pollyanasdolce.com/sucursales', font=footer_font, fill=accent, anchor='mt')
    target = BytesIO()
    image.save(target, format='JPEG', quality=90)
    return target.getvalue()
