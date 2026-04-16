from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse, StreamingHttpResponse
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
        "runtime_status": get_chat_runtime_status(),
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
    payload = _parse_json_body(request)
    content = str(payload.get("content") or "").strip()
    if not content:
        return JsonResponse({"detail": "El mensaje no puede ir vacío."}, status=400)

    user_message, assistant_message = create_user_turn(
        user=request.user,
        conversation=conversation,
        content=content,
        session_key=request.session.session_key or "",
    )

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
