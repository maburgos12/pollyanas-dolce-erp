"""Private, bounded image/PDF input to the existing Agent Core; never instructions."""
import base64
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from PIL import Image
from django.contrib.auth import get_user_model
from django.core.files.storage import default_storage
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import transaction
from django.db.models import Q

from core.models import AuditLog
from mantenimiento.evidence_validation import validate_evidence_files
from orquestacion.models import ChatMessage
from .agent_workflows import WorkflowError

MAX_TOTAL = 20 * 1024 * 1024
PRIVATE_PREFIX = 'fallas/seguimiento/agente/'  # Private staging; UUID paths fit existing 100-character evidence fields.
MAX_PDF_PAGES = 10


def pdf_pages(blob):
    # Parse untrusted PDF metadata outside the web worker, with bounded CPU/memory.
    script = '''import io, json, math, sys, resource
resource.setrlimit(resource.RLIMIT_CPU, (3, 3))
if sys.platform == "linux":
 resource.setrlimit(resource.RLIMIT_AS, (512 * 1024 * 1024, 512 * 1024 * 1024))
import pdfplumber
from itertools import islice
from pdfminer.pdfpage import PDFPage
with pdfplumber.open(io.BytesIO(sys.stdin.buffer.read())) as pdf:
 if pdf.doc.encryption: raise ValueError("encrypted")
 pages = list(islice(PDFPage.create_pages(pdf.doc), 11))
 if not 1 <= len(pages) <= 10: raise ValueError("pages")
 for page in pages:
  box = [float(x) for x in page.mediabox]
  if len(box) != 4 or any(not math.isfinite(x) or abs(x) > 10000 for x in box): raise ValueError("box")
  if not 0 < box[2]-box[0] <= 10000 or not 0 < box[3]-box[1] <= 10000: raise ValueError("size")
 print(json.dumps(len(pages)))
'''
    try:
        result = subprocess.run([sys.executable, '-c', script], input=blob, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                timeout=5, check=True, env={})
        pages = json.loads(result.stdout)
        if type(pages) is not int or not 1 <= pages <= MAX_PDF_PAGES:
            raise ValueError
        return pages
    except (OSError, ValueError, subprocess.SubprocessError):
        raise WorkflowError('pdf_input_invalid', 400,
                            'PDF no válido, cifrado o fuera del límite de diez páginas.') from None


def validated(files):
    files = validate_evidence_files(files)
    if not files or any(file.size > 10 * 1024 * 1024 for file in files) or sum(file.size for file in files) > MAX_TOTAL:
        raise WorkflowError('image_input_invalid', 400)
    infos = []
    for file in files:
        if file.content_type == 'application/pdf':
            blob = file.read(); file.seek(0)
            infos.append({'nombre':file.name, 'bytes':len(blob), 'mime':file.content_type,
                          'sha256':hashlib.sha256(blob).hexdigest(), 'pages':pdf_pages(blob)})
            continue
        try:
            with Image.open(file) as image:
                if image.width * image.height > 25000000 or image.format not in {'JPEG', 'PNG', 'WEBP'}:
                    raise ValueError
                image.verify()
        except Exception:
            raise WorkflowError('image_input_invalid', 400) from None
        finally:
            file.seek(0)
        blob = file.read(); file.seek(0)
        infos.append({'nombre': file.name, 'bytes': len(blob), 'mime': file.content_type,
                      'sha256': hashlib.sha256(blob).hexdigest()})
    if sum(info.get('pages', 0) for info in infos) > MAX_PDF_PAGES:
        raise WorkflowError('pdf_input_invalid', 400, 'Hasta diez páginas PDF en total por mensaje.')
    if len({info['sha256'] for info in infos}) != len(infos):
        raise WorkflowError('image_input_duplicate', 400)
    return files, infos


def staged_bytes(actor, *, new_message=False):
    from .agent_followups import pending_evidence
    messages = list(ChatMessage.objects.filter(Q(metadata_json__has_key='images') | Q(metadata_json__has_key='audio'), conversation__owner=actor)
                    .values_list('metadata_json', flat=True)[:101])
    drafts = pending_evidence(actor)
    if len(messages) + int(new_message) > 100 or len(drafts) > 100:
        raise WorkflowError('image_staging_limit', 409)
    files = [file for row in messages for file in row.get('images', [])]
    files += [row['audio'] for row in messages if row.get('audio')]
    files += [file for row in drafts for file in row.get('files', [])]
    # A message and its proposal reference the same original, not two uploads.
    known = {file['path']: file for file in files if file.get('path')}
    return sum(file['bytes'] for file in known.values()) + sum(file['bytes'] for file in files if not file.get('path'))


def create_image_turn(*, user, conversation, content, files, session_key='', audio=None):
    from .agent_read_runtime import _fresh_access, MAX_INPUT
    from .agent_pilot import pilot_status, MAX_TURNS
    from .chat_service import create_user_turn
    actor, _, _ = _fresh_access(user, conversation.pk)
    status = pilot_status(actor)
    if status['turns'] >= MAX_TURNS:
        raise WorkflowError('pilot_turn_limit', 403)
    files, infos = validated(files) if files else ([], [])
    from .agent_audio import validated as validate_audio
    voice = validate_audio(audio) if audio else None
    if voice and len(content) > MAX_INPUT:
        raise WorkflowError('input_limit', 400)
    if not infos and not voice:
        raise WorkflowError('image_input_invalid', 400)
    total_bytes = sum(info['bytes'] for info in infos) + (voice['bytes'] if voice else 0)
    if total_bytes > MAX_TOTAL:
        raise WorkflowError('image_input_invalid', 400)
    saved = []
    try:
        with transaction.atomic():
            get_user_model().objects.select_for_update().get(pk=actor.pk)
            _fresh_access(actor, conversation.pk)
            if staged_bytes(actor, new_message=True) + total_bytes > 100 * 1024 * 1024:
                raise WorkflowError('image_staging_limit', 409)
            if voice and ChatMessage.objects.filter(conversation=conversation, metadata_json__audio__sha256=voice['sha256']).exists():
                raise WorkflowError('audio_input_duplicate', 409)
            request, answer = create_user_turn(user=actor, conversation=conversation, content=content, session_key=session_key)
            for file, info in zip(files, infos):
                uid = uuid4()
                path = default_storage.save(f'{PRIVATE_PREFIX}{request.public_id.hex}/{uid.hex}{Path(file.name).suffix.lower()}', file)
                saved.append(path)
                info.update(id=str(uid), path=path)
            if voice:
                path = default_storage.save(f'{PRIVATE_PREFIX}{request.public_id.hex}/{uuid4().hex}.wav', audio)
                saved.append(path)
                voice.update(path=path)
            request.metadata_json = {**request.metadata_json, **({'images':infos,
                                     'expected_photo_count':sum(f['mime'].startswith('image/') for f in infos),
                                     'expected_attachment_count':len(infos)} if infos else {}),
                                     **({'audio':voice} if voice else {})}
            request.save(update_fields=['metadata_json', 'updated_at'])
            AuditLog.objects.create(user=actor, action='AI_CHAT_AUDIO_INPUT' if voice else 'AI_CHAT_IMAGE_INPUT', model='ChatMessage', object_id=str(request.public_id),
                payload={'conversation_id':str(conversation.public_id), 'images':[{'id':f['id'], 'sha256':f['sha256'], 'bytes':f['bytes'], 'mime':f['mime']} for f in infos],
                         **({'audio':{'sha256':voice['sha256'], 'bytes':voice['bytes']}} if voice else {})})
            return request, answer
    except Exception:
        for path in saved:
            default_storage.delete(path)
        raise


def read_files(message):
    infos = message.metadata_json.get('images', [])
    if not isinstance(infos, list) or len(infos) > 5:
        raise WorkflowError('image_input_invalid', 400)
    files = []
    for info in infos:
        path = info.get('path', '')
        if not re.fullmatch(re.escape(PRIVATE_PREFIX + message.public_id.hex + '/') + r'[0-9a-f]{32}\.(png|jpe?g|webp|pdf)', path):
            raise WorkflowError('image_input_invalid', 400)
        try:
            with default_storage.open(path, 'rb') as stored:
                blob = stored.read(10 * 1024 * 1024 + 1)
        except OSError:
            raise WorkflowError('image_input_unavailable', 409) from None
        if len(blob) != info['bytes'] or hashlib.sha256(blob).hexdigest() != info['sha256']:
            raise WorkflowError('image_input_changed', 409)
        files.append(SimpleUploadedFile(info['nombre'], blob, content_type=info['mime']))
    if files:
        validated(files)
    return files, infos


def source_files(actor, message_id, conversation_id):
    from .agent_read_runtime import _fresh_access
    from .chat_service import _read_message_visible
    _fresh_access(actor, conversation_id)
    message = ChatMessage.objects.select_related('conversation').filter(public_id=message_id,
        conversation_id=conversation_id, conversation__owner=actor, created_by=actor, role='user', status='complete').first()
    if not message or not _read_message_visible(message):
        raise WorkflowError('image_input_unavailable', 403)
    return read_files(message)


def projected_images(message):
    from .agent_read_runtime import _fresh_access
    if not message.metadata_json.get('images'):
        return []
    try:
        _fresh_access(message.conversation.owner, message.conversation_id)
    except Exception:
        return []
    return [{'id':file['id'], 'name':file['nombre'], 'bytes':file['bytes'], 'mime':file['mime'],
             'url':f'/ia-privada/api/conversations/{message.conversation.public_id}/messages/{message.public_id}/images/{file["id"]}/'}
            for file in message.metadata_json['images']]


def context_image_message(request):
    if request.metadata_json.get('images'):
        return request
    from .chat_service import _read_message_visible
    # Only the most recent accessible input in the bounded conversation window.
    rows = request.conversation.messages.filter(sequence__lt=request.sequence).order_by('-sequence')[:20]
    for message in rows:
        if message.role == 'user' and message.created_by_id == request.conversation.owner_id and message.metadata_json.get('images'):
            return message if _read_message_visible(message) else None
    return None


def image_parts(message):
    files, _ = read_files(message)
    parts = []
    for file in files:
        data = f'data:{file.content_type};base64,' + base64.b64encode(file.read()).decode('ascii')
        parts.append({'type':'input_file', 'filename':file.name, 'file_data':data, 'detail':'high'}
                     if file.content_type == 'application/pdf' else
                     {'type':'input_image', 'detail':'high', 'image_url':data})
    return parts


def text_size(value):
    """Bound text separately from validated inline media; provider counts its tokens."""
    def bounded(obj):
        if isinstance(obj, list):
            return [bounded(item) for item in obj]
        if isinstance(obj, dict):
            if obj.get('type') == 'input_image' and isinstance(obj.get('image_url'), str) and re.fullmatch(
                    r'data:image/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+', obj['image_url']):
                return {**obj, 'image_url':'[private image]'}
            if obj.get('type') == 'input_file' and isinstance(obj.get('file_data'), str) and re.fullmatch(
                    r'data:application/pdf;base64,[A-Za-z0-9+/=]+', obj['file_data']):
                return {**obj, 'file_data':'[private PDF]'}
            return {key:bounded(item) for key, item in obj.items()}
        return obj
    return len(json.dumps(bounded(value), ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
