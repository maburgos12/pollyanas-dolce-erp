"""Explicitly linked private audio. One durable reservation per job; uncertain paid attempts never retry."""
import base64
import hashlib
import io
import os
import re
import secrets
import shutil
import stat
import subprocess
import time
import wave
from datetime import timedelta
from pathlib import Path

import requests
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.utils import timezone

from horarios_especiales.models import TelegramHoursCursor
from horarios_especiales.services import telegram_bot as bot

MAX_DOWNLOAD = 8 * 1024 * 1024
MAX_PCM = 180 * 16000 * 2
AUDIO_STATES = ('AUDIO_WAIT', 'AUDIO_LOCAL', 'AUDIO_PAID')


def actor_for(config):
    return bot.actor_for(config)


def reject(row, reason):
    row.state = 'REPLY'
    row.receipt = {key: value for key, value in row.receipt.items() if key in ('reserved_cents', 'sha256')}
    row.receipt['text'] = reason + ' Envía la instrucción por texto; no se aplicó ningún horario.'
    row.nonce = ''
    row.expires_at = None
    row.save()


@transaction.atomic
def queue(row, config):
    config = bot.config_for_row(config, row)
    actor = actor_for(config)
    bot.invalidate_pending(config, actor)
    media = row.payload['message'].get('voice') or row.payload['message'].get('audio') or {}
    if (not isinstance(media.get('file_id'), str) or not media['file_id'] or
            media.get('file_size', 0) > MAX_DOWNLOAD or media.get('duration', 0) > 180):
        reject(row, 'Audio inválido: máximo 180 segundos y 8 MB.')
        return
    row.state = 'AUDIO_WAIT'
    row.expires_at = timezone.now() + timedelta(seconds=30)
    row.receipt = {'text':'Recibí el audio; la transcripción está en preparación. Después revisa y aprueba la interpretación.'}
    row.save()


def decode_pcm(data):
    if not data or len(data) > MAX_DOWNLOAD:
        raise ValueError('audio inválido')
    executable = shutil.which('ffmpeg', path='/usr/bin:/bin:/opt/homebrew/bin')
    if not executable:
        raise ValueError('decodificador no disponible')
    try:
        proc = subprocess.run([executable, '-hide_banner', '-loglevel', 'error', '-nostdin',
            '-max_alloc', '16777216', '-timelimit', '10', '-threads', '1',
            '-protocol_whitelist', 'pipe', '-format_whitelist', 'ogg,mp3,wav,flac,mov,aac,matroska,webm',
            '-i', 'pipe:0', '-map', '0:a:0', '-vn', '-sn', '-dn', '-t', '181',
            '-threads', '1', '-ac', '1', '-ar', '16000', '-f', 's16le', 'pipe:1'],
            input=data, capture_output=True, timeout=15, check=True, env={'PATH':'/usr/bin:/bin', 'LANG':'C'})
    except (OSError, subprocess.SubprocessError):
        raise ValueError('audio no decodificable') from None
    # Decode one second beyond the maximum: never accept a truncated long recording.
    if not proc.stdout or len(proc.stdout) > MAX_PCM or len(proc.stdout) % 2:
        raise ValueError('duración real fuera del límite')
    return proc.stdout


def download_pcm(row, config):
    actor_for(config)
    if not bot.authorized(row.payload, config):
        raise PermissionDenied('Propietario inválido.')
    media = row.payload['message'].get('voice') or row.payload['message'].get('audio') or {}
    info = bot.TelegramClient(config).call('getFile', file_id=media['file_id'])
    path = info.get('file_path', '') if isinstance(info, dict) else ''
    if (not re.fullmatch(r'(?:voice|music|audio|documents)/[A-Za-z0-9_-]+\.(?:oga|ogg|mp3|wav|flac|m4a|mp4|aac|opus|webm)', path)
            or type(info.get('file_size', 0)) is not int or not 0 <= info.get('file_size', 0) <= MAX_DOWNLOAD):
        raise ValueError('archivo Telegram inválido')
    actor_for(config)
    deadline = time.monotonic() + 10
    try:
        with requests.Session() as session:
            session.trust_env = False
            with session.get(f'https://api.telegram.org/file/bot{config["token"]}/{path}',
                    timeout=(3, 4), allow_redirects=False, stream=True) as response:
                if response.status_code != 200:
                    raise ValueError('descarga no disponible')
                data = bytearray()
                for chunk in response.iter_content(8192):
                    data.extend(chunk)
                    if len(data) > MAX_DOWNLOAD or time.monotonic() > deadline:
                        raise ValueError('descarga excedida')
    except requests.RequestException:
        raise ValueError('descarga no disponible') from None
    actor_for(config)
    return decode_pcm(bytes(data))


def stale(row, config):
    # Persisted corrections take precedence even before the polling loop processes them.
    for newer in bot.owner_updates(config).filter(update_id__gt=row.update_id).order_by('-update_id').iterator():
        message = newer.payload.get('message', {})
        text = message.get('text', '').strip()
        if message.get('voice') or message.get('audio') or (text and not text.startswith('/')) or text == '/cancel':
            return True
    return False


def normalize_spoken(text):
    numbers = {word: str(i) for i, word in enumerate(('una', 'dos', 'tres', 'cuatro', 'cinco', 'seis', 'siete', 'ocho', 'nueve', 'diez', 'once', 'doce'), 1)}
    pattern = r'\b(' + '|'.join(numbers) + r'|\d{1,2}) de la (mañana|tarde|noche)\b'
    def replace(match):
        hour = numbers.get(match[1].lower(), match[1])
        period = match[2].lower()
        if not 1 <= int(hour) <= 12 or (int(hour) == 12 and period == 'mañana') or (int(hour) <= 5 and period == 'noche'):
            return match[0]
        suffix = 'am' if period == 'mañana' or (int(hour) == 12 and period == 'noche') else 'pm'
        return hour + ' ' + suffix
    return re.sub(pattern, replace, text, flags=re.I)


def prepare_result(row, text, config, source):
    config = bot.config_for_row(config, row)
    actor = actor_for(config)
    if not isinstance(text, str) or not text.strip() or len(text) > 2000 or any(ord(c) < 32 and c not in '\n\t' for c in text):
        reject(row, 'No pude transcribir el audio con claridad.')
        return
    audit = {key: value for key, value in row.receipt.items() if key in ('reserved_cents', 'sha256')}
    bot.prepare_text(row, normalize_spoken(text.strip()), config, actor, strict_branches=True)
    row.receipt.update(audit, source=source)
    row.receipt['text'] = 'Escuché: ' + text.strip() + '\n\n' + row.receipt['text']
    row.save(update_fields=['receipt'])


def claim(config):
    if not bot.active_configs(config):
        actor_for(config)
    with bot.consumer_lock(config['bot_id']) as locked:
        if not locked:
            return {}
        bot.discard_revoked_pending(config)
        row = bot.active_updates(config).filter(state='AUDIO_WAIT').order_by('update_id').first()
        if not row:
            return {}
        config = bot.config_for_row(config, row)
        if stale(row, config):
            reject(row, 'Audio reemplazado por una instrucción nueva.')
            return {}
        try:
            data = download_pcm(row, config)
        except (ValueError, bot.TelegramUnavailable):
            reject(row, 'No pude validar el audio (máximo 180 segundos y 8 MB).')
            return {}
        actor_for(config)
        row.state = 'AUDIO_LOCAL'
        row.nonce = secrets.token_hex(16)
        row.expires_at = timezone.now() + timedelta(seconds=90)
        row.receipt = {'pcm': base64.b64encode(data).decode(), 'sha256': hashlib.sha256(data).hexdigest()}
        row.save()
        return {'id': row.pk, 'lease': row.nonce, 'sha256':row.receipt['sha256'], 'pcm':row.receipt['pcm']}


def finish(config, payload):
    if not bot.active_configs(config):
        actor_for(config)
    if (not isinstance(payload, dict) or set(payload) != {'id', 'lease', 'sha256', 'text'} or
            type(payload['id']) is not int or not 0 < payload['id'] < 2**63 or
            not isinstance(payload['lease'], str) or not re.fullmatch('[a-f0-9]{32}', payload['lease']) or
            not isinstance(payload['sha256'], str) or not re.fullmatch('[a-f0-9]{64}', payload['sha256'])):
        raise ValueError('resultado inválido')
    with bot.consumer_lock(config['bot_id']) as locked:
        if not locked:
            return {'accepted':False, 'busy':True}
        bot.discard_revoked_pending(config)
        row = bot.active_updates(config).filter(pk=payload['id'], state='AUDIO_LOCAL').first()
        if row:
            config = bot.config_for_row(config, row)
        if (not row or not row.expires_at or row.expires_at <= timezone.now() or
                not secrets.compare_digest(row.nonce, payload['lease']) or
                not secrets.compare_digest(row.receipt.get('sha256', ''), payload['sha256']) or stale(row, config)):
            return {'accepted':False}
        prepare_result(row, payload['text'], config, 'whisper-base-local')
        return {'accepted':True}


@transaction.atomic
def reserve(row, config):
    config = bot.config_for_row(config, row)
    actor_for(config)
    cursor = TelegramHoursCursor.objects.select_for_update().get(bot_id=config['bot_id'])
    row.refresh_from_db()
    if row.state not in ('AUDIO_WAIT', 'AUDIO_LOCAL') or stale(row, config):
        return False
    if cursor.audio_reserved_cents + 3 > 500:
        reject(row, 'Se alcanzó el presupuesto inicial de respaldo de US$5; no se renueva automáticamente.')
        return False
    cursor.audio_reserved_cents += 3
    cursor.save(update_fields=['audio_reserved_cents'])
    row.state = 'AUDIO_PAID'
    row.nonce = ''
    row.expires_at = timezone.now() + timedelta(seconds=45)
    row.receipt['reserved_cents'] = 3
    row.save()
    return True


def paid_transcribe(data):
    from openai import OpenAI
    path = Path(os.environ.get('HOURS_TELEGRAM_CONFIG_DIR', '/etc/pollyanas-hours')) / 'openai-key'
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 1 <= info.st_size <= 1024:
        raise ValueError('credencial privada inválida')
    output = io.BytesIO()
    with wave.open(output, 'wb') as wav:
        wav.setnchannels(1); wav.setsampwidth(2); wav.setframerate(16000); wav.writeframes(data)
    # A distinct key and durable budget; never uses or extends the ERP agent_audio budget.
    with OpenAI(api_key=path.read_text().strip(), base_url='https://api.openai.com/v1', max_retries=0, timeout=20) as client:
        result = client.audio.transcriptions.create(model='gpt-transcribe', language='es',
            file=('audio.wav', output.getvalue(), 'audio/wav'))
    return result.text


def advance(config):
    """Run under the polling consumer lock; at most one paid request per tick."""
    if not bot.active_configs(config):
        actor_for(config)
    row = bot.active_updates(config).filter(state__in=AUDIO_STATES, expires_at__lte=timezone.now()).order_by('update_id').first()
    if not row:
        return
    config = bot.config_for_row(config, row)
    if stale(row, config):
        reject(row, 'Audio reemplazado por una instrucción nueva.')
        return
    if row.state == 'AUDIO_PAID':
        reject(row, 'El resultado del respaldo quedó incierto; conservé su reserva y no repetiré el cargo.')
        return
    try:
        data = base64.b64decode(row.receipt['pcm'], validate=True) if row.receipt.get('pcm') else download_pcm(row, config)
        if not data or len(data) > MAX_PCM or len(data) % 2:
            raise ValueError('PCM inválido')
    except (ValueError, bot.TelegramUnavailable):
        reject(row, 'No pude validar el audio (máximo 180 segundos y 8 MB).')
        return
    row.receipt['sha256'] = hashlib.sha256(data).hexdigest()
    row.save(update_fields=['receipt'])
    if not reserve(row, config):
        return
    try:
        actor_for(config)
        text = paid_transcribe(data)
    except PermissionDenied:
        raise
    except Exception:
        # No provider exceptions: they can include credentials or the recording.
        reject(row, 'No obtuve una transcripción confirmada; no repetiré el cargo.')
        return
    actor_for(config)
    if stale(row, config):
        reject(row, 'Audio reemplazado por una instrucción nueva.')
        return
    prepare_result(row, text, config, 'openai-fallback')
