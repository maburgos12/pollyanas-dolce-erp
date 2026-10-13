from __future__ import annotations

import json
import logging
import io
from uuid import UUID

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import FileResponse, Http404, JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from core.access import can_view_orquestacion
from .services.chat_service import (
    ChatConfigurationError,
    create_chat_conversation,
    create_user_turn,
    execute_chat_turn,
    get_chat_runtime_status,
    get_chat_conversation,
    list_chat_conversations,
    serialize_conversation,
    serialize_conversation_detail,
    stream_events_for_turn,
)

logger = logging.getLogger(__name__)


def _assistant_error_payload(*, assistant_message, detail: str, technical_error: str, error_code: str, retryable: bool) -> dict:
    return {
        "detail": detail,
        "assistant_message_id": str(assistant_message.public_id),
        "assistant_content": assistant_message.content,
        "error": technical_error,
        "error_code": error_code,
        "retryable": retryable,
    }


def _ensure_ai_private_access(user) -> None:
    if not user or not user.is_authenticated:
        raise PermissionDenied("La sesión no está autenticada.")
    if not can_view_orquestacion(user):
        raise PermissionDenied("No tienes permisos para consultar la IA privada.")


def _parse_json_body(request) -> dict:
    raw = (request.body or b"").decode("utf-8").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("El cuerpo JSON es inválido.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("El cuerpo debe ser un objeto JSON.")
    return parsed


@login_required
def chat_home(request):
    _ensure_ai_private_access(request.user)
    conversations = list_chat_conversations(request.user)
    selected = conversations[0] if conversations else None
    if request.GET.get("conversation"):
        try:
            selected = get_chat_conversation(request.user, request.GET.get("conversation"))
        except Exception:
            logger.warning("Conversación solicitada no disponible para el usuario.", exc_info=True)
    context = {
        "chat_api_base": "/ia-privada/api",
        "conversations": [serialize_conversation(conversation) for conversation in conversations],
        "selected_conversation": serialize_conversation_detail(selected) if selected else None,
        "runtime_status": get_chat_runtime_status(request.user),
    }
    return render(request, "orquestacion/chat.html", context)


@login_required
@require_GET
def conversations_api(request):
    _ensure_ai_private_access(request.user)
    conversations = list_chat_conversations(request.user)
    return JsonResponse({"items": [serialize_conversation(conversation) for conversation in conversations]})


@login_required
@require_POST
def create_conversation_api(request):
    _ensure_ai_private_access(request.user)
    conversation = create_chat_conversation(user=request.user, session_key=request.session.session_key or "")
    return JsonResponse({"conversation": serialize_conversation(conversation)}, status=201)


@login_required
@require_GET
def conversation_detail_api(request, conversation_id):
    _ensure_ai_private_access(request.user)
    conversation = get_chat_conversation(request.user, conversation_id)
    return JsonResponse(serialize_conversation_detail(conversation))


@login_required
@require_POST
def stream_message_api(request, conversation_id):
    _ensure_ai_private_access(request.user)
    conversation = get_chat_conversation(request.user, conversation_id)
    payload = request.POST if request.content_type == 'multipart/form-data' else _parse_json_body(request)
    client_request_id = payload.get('client_request_id')
    if client_request_id is not None:
        try:
            if not isinstance(client_request_id, str): raise ValueError
            client_request_id = str(UUID(client_request_id))
        except ValueError:
            return JsonResponse({'detail':'Referencia de envío inválida.'}, status=400)
    files = request.FILES.getlist('files')
    audios = request.FILES.getlist('audio')
    if len(audios) > 1:
        return JsonResponse({'detail':'Sólo una nota de voz por mensaje.'}, status=400)
    audio = audios[0] if audios else None
    if files or audio:
        payload = {'content':payload.get('content'), 'attachment_count':len(files)}
    count = payload.get('attachment_count', 0)
    if type(count) is not int or not 0 <= count <= 5:
        return JsonResponse({'detail':'Puedes seleccionar hasta cinco imágenes o PDFs.'}, status=400)
    content = str(payload.get("content") or "").strip()
    if audio and not content:
        content = 'Nota de voz'
    if files and not content:
        content = 'Analiza los archivos adjuntos.'
    if not content:
        return JsonResponse({"detail": "El mensaje no puede ir vacío."}, status=400)

    if files or audio:
        from mantenimiento.evidence_validation import EvidenceValidationError
        from .services.agent_images import create_image_turn
        from .services.agent_workflows import WorkflowError
        from .services.agent_pilot import PilotStopped
        from .services.agent_read_runtime import ReadStopped
        try:
            user_message, assistant_message = create_image_turn(user=request.user, conversation=conversation,
                content=content, files=files, audio=audio, session_key=request.session.session_key or '')
        except (WorkflowError, EvidenceValidationError, ReadStopped, PilotStopped) as exc:
            code = getattr(exc, 'code', 'image_input_invalid')
            detail = {'audio_input_duplicate':'La nota ya está en esta conversación. Revisa el historial antes de reenviarla.',
                      'pdf_input_invalid':'Revisa el PDF: hasta diez páginas en total, sin contraseña y 10 MB por archivo.',
                      'input_limit':'El texto que acompaña a la voz no puede exceder 6000 caracteres.'}.get(code,
                'No se admitieron los archivos. Imágenes/PDF: hasta cinco archivos, 10 MB cada uno, 20 MB en total. Voz: WAV PCM, hasta 3 minutos y 8 MB. Revisa permisos y límite del piloto.')
            return JsonResponse({'detail':detail, 'code':code}, status=getattr(exc, 'status', 400))
    else:
        user_message, assistant_message = create_user_turn(
            user=request.user, conversation=conversation, content=content,
            session_key=request.session.session_key or '',
        )

    if client_request_id:
        assistant_message.metadata_json = {**assistant_message.metadata_json, 'client_request_id':client_request_id}
        assistant_message.save(update_fields=['metadata_json', 'updated_at'])
    if count and not files:
        user_message.metadata_json = {**user_message.metadata_json, 'expected_photo_count': count}
        user_message.save(update_fields=['metadata_json', 'updated_at'])

    try:
        result = execute_chat_turn(
            user=request.user,
            conversation=conversation,
            user_message=user_message,
            assistant_message=assistant_message,
        )
    except ChatConfigurationError as exc:
        logger.exception("Falta configuración para ejecutar el chat ERP.")
        assistant_message.status = assistant_message.STATUS_ERROR
        assistant_message.content = (
            "La IA privada del ERP todavía no está configurada por completo. "
            "La conversación quedó guardada, pero no pude generar respuesta automática. "
            f"Falta resolver: {exc}"
        )
        assistant_message.save(update_fields=["status", "content", "updated_at"])
        return JsonResponse(
            _assistant_error_payload(
                assistant_message=assistant_message,
                detail="La IA privada del ERP no está configurada por completo.",
                technical_error=str(exc),
                error_code="chat_configuration_error",
                retryable=False,
            ),
            status=503,
        )
    except Exception as exc:
        logger.exception("Falló la ejecución del turno de chat ERP.")
        assistant_message.status = assistant_message.STATUS_ERROR
        assistant_message.content = (
            "No pude completar la respuesta del ERP en este momento. "
            "La conversación quedó guardada para retomar después. "
            f"Detalle técnico: {exc}"
        )
        assistant_message.save(update_fields=["status", "content", "updated_at"])
        return JsonResponse(
            _assistant_error_payload(
                assistant_message=assistant_message,
                detail="No se pudo completar la respuesta del ERP.",
                technical_error=str(exc),
                error_code="chat_execution_error",
                retryable=True,
            ),
            status=500,
        )

    response = StreamingHttpResponse(
        stream_events_for_turn(result=result, assistant_message=assistant_message),
        content_type="text/event-stream; charset=utf-8",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@login_required
@require_GET
def message_image_api(request, conversation_id, message_id, image_id):
    _ensure_ai_private_access(request.user)
    from .models import ChatConversation
    conversation = ChatConversation.objects.filter(owner=request.user, public_id=conversation_id, status='active').first()
    if not conversation:
        raise Http404
    from .services.agent_images import source_files
    try:
        files, infos = source_files(request.user, message_id, conversation.pk)
    except Exception:
        raise Http404 from None
    for file, info in zip(files, infos):
        if info['id'] == str(image_id):
            response = FileResponse(io.BytesIO(file.read()), content_type=info['mime'], filename=info['nombre'],
                                    as_attachment=info['mime'] == 'application/pdf')
            response['Cache-Control'] = 'private, no-store'
            response['X-Content-Type-Options'] = 'nosniff'
            return response
    raise Http404


@login_required
@require_GET
def message_audio_api(request, conversation_id, message_id):
    _ensure_ai_private_access(request.user)
    from .models import ChatMessage
    from .services.agent_audio import read_file
    from .services.agent_read_runtime import _fresh_access
    from .services.chat_service import _read_message_visible
    message = ChatMessage.objects.select_related('conversation').filter(public_id=message_id,
        conversation__public_id=conversation_id, conversation__owner=request.user,
        conversation__status='active', created_by=request.user, role='user').first()
    if not message or not _read_message_visible(message):
        raise Http404
    try:
        _fresh_access(request.user, message.conversation_id)
        file = read_file(message)
    except Exception:
        raise Http404 from None
    response = FileResponse(io.BytesIO(file.read()), content_type='audio/wav')
    response['Cache-Control'] = 'private, no-store'
    response['X-Content-Type-Options'] = 'nosniff'
    return response
