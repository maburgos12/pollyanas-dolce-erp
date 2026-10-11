"""Bounded private WAV input; transcription feeds the existing Agent Core."""
import hashlib
import io
import re
import struct
import wave
from pathlib import Path

from django.conf import settings
from django.core.files.storage import default_storage
from django.db import transaction
from django.utils import timezone

from core.models import AuditLog
from orquestacion.models import ChatConversation
from .agent_workflows import WorkflowError

MAX_SECONDS = 180
MAX_BYTES = 8 * 1024 * 1024


def validated(file):
    if Path(file.name).suffix.lower() != '.wav' or not 44 <= file.size <= MAX_BYTES:
        raise WorkflowError('audio_input_invalid', 400)
    blob = file.read(MAX_BYTES + 1); file.seek(0)
    try:
        if struct.unpack_from('<I', blob, 4)[0] + 8 != len(blob):
            raise ValueError
        with wave.open(io.BytesIO(blob), 'rb') as audio:
            channels, rate, frames = audio.getnchannels(), audio.getframerate(), audio.getnframes()
            if (channels not in (1, 2) or audio.getsampwidth() != 2 or audio.getcomptype() != 'NONE'
                    or not 8000 <= rate <= 48000 or not rate // 2 <= frames <= rate * MAX_SECONDS
                    or len(audio.readframes(frames)) != frames * channels * 2):
                raise ValueError
    except (wave.Error, EOFError, ValueError, struct.error):
        raise WorkflowError('audio_input_invalid', 400) from None
    if len(blob) != file.size:
        raise WorkflowError('audio_input_invalid', 400)
    return {'nombre':file.name, 'bytes':len(blob), 'mime':'audio/wav',
            'sha256':hashlib.sha256(blob).hexdigest(), 'duration_seconds':frames / rate, 'state':'pending'}


def read_file(message):
    from .agent_images import PRIVATE_PREFIX
    info = message.metadata_json.get('audio', {})
    path = info.get('path', '')
    if not re.fullmatch(re.escape(PRIVATE_PREFIX + message.public_id.hex + '/') + r'[0-9a-f]{32}\.wav', path):
        raise WorkflowError('audio_input_invalid', 400)
    try:
        with default_storage.open(path, 'rb') as stored:
            blob = stored.read(MAX_BYTES + 1)
    except OSError:
        raise WorkflowError('audio_input_unavailable', 409) from None
    if len(blob) != info['bytes'] or hashlib.sha256(blob).hexdigest() != info['sha256']:
        raise WorkflowError('audio_input_changed', 409)
    from django.core.files.uploadedfile import SimpleUploadedFile
    file = SimpleUploadedFile(info['nombre'], blob, content_type='audio/wav')
    validated(file)
    return file


def transcribe(*, actor, message, answer, timeout):
    from .agent_read_runtime import _fresh_access, ReadStopped, MAX_INPUT
    from .agent_pilot import reserve_audio, AUDIO_MODEL
    from openai import OpenAI
    _fresh_access(actor, message.conversation_id)
    if len(message.content) > MAX_INPUT:
        raise ReadStopped('input_limit')
    file = read_file(message)
    if not settings.OPENAI_API_KEY:
        raise ReadStopped('configuration_missing')
    # Send only validated PCM frames; extra container chunks must not change billed duration.
    pcm = io.BytesIO()
    with wave.open(file, 'rb') as source, wave.open(pcm, 'wb') as target:
        target.setparams(source.getparams())
        target.writeframes(source.readframes(source.getnframes()))
    reserve_audio(user=actor, turn_id=answer.public_id)
    _fresh_access(actor, message.conversation_id)
    try:
        result = OpenAI(api_key=settings.OPENAI_API_KEY, base_url='https://api.openai.com/v1',
            max_retries=0, timeout=min(timeout, 30)).audio.transcriptions.create(
                model=AUDIO_MODEL, file=(file.name, pcm.getvalue(), 'audio/wav'), extra_body={'languages':['es']})
    except Exception:
        raise ReadStopped('audio_transcription_failed') from None
    text = getattr(result, 'text', None)
    if not isinstance(text, str) or not text.strip() or len(text) > MAX_INPUT:
        raise ReadStopped('audio_transcription_invalid')
    text = text.strip()
    _fresh_access(actor, message.conversation_id)
    read_file(message)
    content = f"{message.content}\n\nTranscripción automática de mi nota de voz:\n{text}"
    if len(content) > MAX_INPUT:
        raise ReadStopped('input_limit')
    with transaction.atomic():
        message.content = content
        message.metadata_json = {**message.metadata_json, 'audio':{**message.metadata_json['audio'],
            'state':'complete', 'model':AUDIO_MODEL, 'transcript':text}}
        message.save(update_fields=['content', 'metadata_json', 'updated_at'])
        if message.sequence == 1 and message.conversation.title == 'Nota de voz':
            from .chat_service import _title_from_text
            ChatConversation.objects.filter(pk=message.conversation_id, owner=actor, title='Nota de voz').update(
                title=_title_from_text(text), updated_at=timezone.now())
        AuditLog.objects.create(user=actor, action='AI_CHAT_AUDIO_TRANSCRIBED', model='ChatMessage', object_id=str(message.public_id),
            payload={'conversation_id':str(message.conversation.public_id), 'sha256':message.metadata_json['audio']['sha256'],
                     'model':AUDIO_MODEL, 'turn_id':str(answer.public_id)})


def projected_audio(message):
    from .agent_read_runtime import _fresh_access
    info = message.metadata_json.get('audio')
    if not info:
        return None
    try:
        _fresh_access(message.conversation.owner, message.conversation_id)
    except Exception:
        return None
    return {'name':info['nombre'], 'state':info['state'], 'duration_seconds':info['duration_seconds'],
            'url':f'/ia-privada/api/conversations/{message.conversation.public_id}/messages/{message.public_id}/audio/'}
