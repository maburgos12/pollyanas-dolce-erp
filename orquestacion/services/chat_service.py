from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from api.ai_gateway_services import invoke_tool, list_allowed_tools, request_tool_approval
from core.access import primary_role
from orquestacion.models import (
    ChatConversation,
    ChatConversationState,
    ChatMemoryPin,
    ChatMessage,
    ChatToolCall,
    ChatToolResult,
)
from orquestacion.services.agent_runtime import load_agent_memory

PROMPT_FILES = [
    "pos_bridge/prompts/dg_executive_response_policy.md",
    "pos_bridge/prompts/dg_executive_response_loop.md",
]
DEFAULT_CHAT_MODEL = getattr(settings, "POS_BRIDGE_AGENT_MODEL", "gpt-4o-mini")
MAX_HISTORY_MESSAGES = 24


class ChatConfigurationError(RuntimeError):
    """Raised when the native ERP chat is missing required runtime configuration."""


@dataclass(frozen=True)
class ChatTurnResult:
    assistant_text: str
    model_name: str
    tool_events: list[dict[str, Any]]


def _repo_root() -> Path:
    return Path(settings.BASE_DIR)


def _read_relative(relative_path: str) -> str:
    candidate = _repo_root() / relative_path
    if not candidate.exists():
        return ""
    return candidate.read_text(encoding="utf-8").strip()


def _safe_json(value: Any, *, max_chars: int = 12000) -> str:
    raw = json.dumps(value, ensure_ascii=False, default=str)
    if len(raw) <= max_chars:
        return raw
    return raw[:max_chars] + "…"


def _chunk_text(text: str, *, chunk_size: int = 48) -> list[str]:
    if not text:
        return []
    words = text.split()
    chunks: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= chunk_size:
            current = candidate
            continue
        if current:
            chunks.append(current)
        current = word
    if current:
        chunks.append(current)
    return chunks


def _next_sequence(conversation: ChatConversation) -> int:
    last = conversation.messages.order_by("-sequence").values_list("sequence", flat=True).first()
    return int(last or 0) + 1


def _conversation_scope_summary(user) -> str:
    profile = getattr(user, "userprofile", None)
    branch_lock = bool(profile and getattr(profile, "modo_captura_sucursal", False))
    branch_name = getattr(getattr(profile, "sucursal", None), "nombre", "")
    role = primary_role(user) or "SIN_ROL"
    if branch_lock and branch_name:
        return f"Rol primario: {role}. Alcance limitado a la sucursal: {branch_name}."
    return f"Rol primario: {role}. Alcance transversal según permisos vigentes."


def _build_system_prompt(user, conversation: ChatConversation) -> str:
    memory = load_agent_memory(base_dir=_repo_root())
    today = timezone.localdate()
    prompt_chunks = [_read_relative(path) for path in PROMPT_FILES]
    active_pins = list(
        ChatMemoryPin.objects.filter(
            conversation=conversation,
            status=ChatMemoryPin.STATUS_ACTIVE,
        ).order_by("-created_at")[:12]
    )
    pinned_facts = "\n".join(f"- {pin.label}: {pin.content}" for pin in active_pins) or "- Sin memorias fijadas."
    stable_facts = "\n".join(f"- {item}" for item in memory.stable_facts[:20]) or "- Sin hechos estables cargados."
    recurrent_errors = "\n".join(f"- {item}" for item in memory.recurrent_errors[:20]) or "- Sin errores recurrentes cargados."
    known_gaps = "\n".join(f"- {item}" for item in memory.known_gaps[:20]) or "- Sin gaps estables cargados."
    prompt_docs = "\n\n".join(chunk for chunk in prompt_chunks if chunk).strip()
    return (
        "Eres la IA operativa del ERP de Pollyana's Dolce. "
        "Tu interfaz debe sentirse como ChatGPT, pero tu comportamiento es empresarial: preciso, útil, auditable y orientado a decisión.\n\n"
        "Reglas obligatorias:\n"
        "- No inventes datos del ERP.\n"
        "- Cuando falten datos, dilo explícitamente y separa hecho auditado vs estimado operativo.\n"
        "- Usa herramientas del ERP antes de contestar si la pregunta depende de datos operativos.\n"
        f"- Fecha operativa actual: {today.isoformat()}. Si el usuario menciona un mes sin año, asume el año operativo actual ({today.year}) salvo que el historial diga otro año explícito.\n"
        "- Si preguntan por precio/costo actual de compra de un insumo o materia prima, usa erp_get_current_input_cost; no uses costo historico de receta salvo que pidan una receta.\n"
        "- Si preguntan cuántos tickets superan o igualan un monto específico, usa erp_get_ticket_amount_threshold; no infieras cero desde ventas agregadas si no existe monto individual por ticket.\n"
        "- Si preguntan si conviene una promoción, descuento, 3x2, campaña, día especial o rendimiento financiero por producto, usa erp_analyze_promotion_profitability antes de recomendar. Respeta presentaciones exactas como vaso chico/mediano/grande; si dicen revoltura/surtido de rebanadas, consérvalo como grupo de rebanadas, no como un sabor individual.\n"
        "- Si una herramienta requiere aprobación, solicita la aprobación en vez de simular la ejecución.\n"
        "- Responde siempre en español claro y ejecutivo.\n"
        "- Para preguntas de Dirección General, organiza la respuesta con: Resumen ejecutivo, Hecho auditado, Estimado operativo, Riesgo/interpretación, Siguiente acción.\n"
        "- Mantén continuidad conversacional y aprovecha el historial de la conversación.\n\n"
        f"Contexto del usuario:\n{_conversation_scope_summary(user)}\n\n"
        f"Hechos estables del ERP:\n{stable_facts}\n\n"
        f"Errores recurrentes a evitar:\n{recurrent_errors}\n\n"
        f"Gaps estables confirmados:\n{known_gaps}\n\n"
        f"Memorias fijadas en esta conversación:\n{pinned_facts}\n\n"
        f"Política y loop DG vigentes:\n{prompt_docs}\n"
    )


def list_chat_conversations(user, *, limit: int = 50) -> list[ChatConversation]:
    return list(
        ChatConversation.objects.filter(owner=user, status=ChatConversation.STATUS_ACTIVE)
        .prefetch_related("messages")
        .order_by("-last_message_at", "-updated_at")[:limit]
    )


def get_chat_conversation(user, public_id) -> ChatConversation:
    return ChatConversation.objects.prefetch_related("messages__tool_calls__result").get(owner=user, public_id=public_id)


@transaction.atomic
def create_chat_conversation(*, user, session_key: str = "") -> ChatConversation:
    conversation = ChatConversation.objects.create(
        owner=user,
        title="Nueva conversación",
        session_key=session_key or "",
        model_name=DEFAULT_CHAT_MODEL,
    )
    ChatConversationState.objects.create(conversation=conversation)
    return conversation


def _message_preview(message: ChatMessage | None) -> str:
    if not message:
        return ""
    text = (message.content or "").strip().replace("\n", " ")
    return text[:120]


def serialize_tool_call(tool_call: ChatToolCall) -> dict[str, Any]:
    result = getattr(tool_call, "result", None)
    return {
        "id": str(tool_call.public_id),
        "tool_key": tool_call.tool_key,
        "tool_name": tool_call.tool_name,
        "tool_display_name": tool_call.tool_display_name or tool_call.tool_name,
        "status": tool_call.status,
        "requires_approval": tool_call.requires_approval,
        "summary": result.summary if result else "",
        "result": result.result_json if result else {},
        "created_at": tool_call.created_at.isoformat(),
    }


def serialize_message(message: ChatMessage) -> dict[str, Any]:
    tool_calls = []
    if message.role == ChatMessage.ROLE_ASSISTANT:
        tool_calls = [serialize_tool_call(tool_call) for tool_call in message.tool_calls.select_related("result").all()]
    return {
        "id": str(message.public_id),
        "sequence": message.sequence,
        "role": message.role,
        "status": message.status,
        "content": message.content,
        "created_at": message.created_at.isoformat(),
        "tool_calls": tool_calls,
    }


def serialize_conversation(conversation: ChatConversation) -> dict[str, Any]:
    last_message = next((msg for msg in reversed(list(conversation.messages.all())) if msg.content.strip()), None)
    return {
        "id": str(conversation.public_id),
        "title": conversation.title,
        "status": conversation.status,
        "preview": _message_preview(last_message),
        "message_count": conversation.messages.count(),
        "updated_at": conversation.updated_at.isoformat(),
        "last_message_at": conversation.last_message_at.isoformat(),
    }


def serialize_conversation_detail(conversation: ChatConversation) -> dict[str, Any]:
    messages = list(
        conversation.messages.order_by("sequence", "id").prefetch_related("tool_calls__result")
    )
    return {
        "conversation": {
            "id": str(conversation.public_id),
            "title": conversation.title,
            "status": conversation.status,
            "model_name": conversation.model_name,
            "updated_at": conversation.updated_at.isoformat(),
            "last_message_at": conversation.last_message_at.isoformat(),
        },
        "messages": [serialize_message(message) for message in messages],
    }


def _build_tool_definitions(user) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    allowed_tools = list_allowed_tools(user)
    tool_map = {tool["name"]: tool for tool in allowed_tools}
    openai_tools = []
    for tool in allowed_tools:
        openai_tools.append(
            {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool.get("description") or tool.get("display_name") or tool["key"],
                    "parameters": tool.get("argument_schema") or {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": True,
                    },
                },
            }
        )
    return openai_tools, tool_map


def _build_model_history(user, conversation: ChatConversation) -> list[dict[str, Any]]:
    system_prompt = _build_system_prompt(user, conversation)
    history = [{"role": "system", "content": system_prompt}]
    queryset = (
        conversation.messages.filter(role__in=[ChatMessage.ROLE_USER, ChatMessage.ROLE_ASSISTANT])
        .exclude(content="")
        .order_by("-sequence", "-id")[:MAX_HISTORY_MESSAGES]
    )
    for message in reversed(list(queryset)):
        history.append({"role": message.role, "content": message.content})
    return history


def _tool_message_for_model(tool_call_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": _safe_json(payload),
    }


def _assistant_tool_message(tool_calls: list[Any], content: str = "") -> dict[str, Any]:
    normalized_tool_calls = []
    for tool_call in tool_calls:
        normalized_tool_calls.append(
            {
                "id": tool_call.id,
                "type": "function",
                "function": {
                    "name": tool_call.function.name,
                    "arguments": tool_call.function.arguments,
                },
            }
        )
    return {
        "role": "assistant",
        "content": content or "",
        "tool_calls": normalized_tool_calls,
    }


def _title_from_text(content: str) -> str:
    title = " ".join((content or "").strip().split())
    if not title:
        return "Nueva conversación"
    return title[:90]


def _invoke_chat_tool(*, user, conversation: ChatConversation, user_message: ChatMessage, assistant_message: ChatMessage, tool_meta: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    tool_call = ChatToolCall.objects.create(
        conversation=conversation,
        request_message=user_message,
        assistant_message=assistant_message,
        tool_key=tool_meta["key"],
        tool_name=tool_meta["name"],
        tool_display_name=tool_meta.get("display_name") or tool_meta["name"],
        arguments_json=arguments,
        requires_approval=bool(tool_meta.get("requires_approval")),
        status=ChatToolCall.STATUS_RUNNING,
        started_at=timezone.now(),
    )
    if tool_meta.get("requires_approval"):
        payload = request_tool_approval(
            user=user,
            tool_key=tool_meta["key"],
            arguments=arguments,
            summary=f"Solicitud generada desde conversación {conversation.public_id}",
            rationale="La conversación del ERP solicitó una acción con aprobación previa.",
        )
        suggestion_id = (payload.get("approval") or {}).get("suggestion_id")
        if suggestion_id:
            tool_call.approval_suggestion_id = suggestion_id
        tool_call.status = ChatToolCall.STATUS_APPROVAL_REQUESTED
        tool_call.finished_at = timezone.now()
        tool_call.save(update_fields=["approval_suggestion_id", "status", "finished_at", "updated_at"])
        summary = "Se registró la solicitud de aprobación para continuar la acción."
    else:
        payload = invoke_tool(
            user=user,
            tool_key=tool_meta["key"],
            arguments=arguments,
        )
        tool_call.status = ChatToolCall.STATUS_COMPLETE
        tool_call.finished_at = timezone.now()
        tool_call.save(update_fields=["status", "finished_at", "updated_at"])
        summary = (
            payload.get("result", {}).get("status")
            or payload.get("status")
            or payload.get("result", {}).get("summary")
            or "Herramienta ejecutada."
        )

    ChatToolResult.objects.update_or_create(
        tool_call=tool_call,
        defaults={
            "is_error": False,
            "summary": str(summary),
            "result_json": payload,
        },
    )
    return {
        "tool_call_id": tool_call.public_id,
        "tool_name": tool_call.tool_name,
        "tool_display_name": tool_call.tool_display_name,
        "status": tool_call.status,
        "payload": payload,
        "summary": summary,
    }


def _model_client():
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise ChatConfigurationError("La librería OpenAI no está instalada en este entorno.") from exc
    api_key = getattr(settings, "OPENAI_API_KEY", "")
    if not api_key:
        raise ChatConfigurationError("OPENAI_API_KEY no está configurada en el ERP.")
    return OpenAI(api_key=api_key)


def get_chat_runtime_status() -> dict[str, Any]:
    api_key = getattr(settings, "OPENAI_API_KEY", "")
    model_name = getattr(settings, "PRIVATE_AI_CHAT_MODEL", "") or DEFAULT_CHAT_MODEL
    if not api_key:
        return {
            "ready": False,
            "model_name": model_name,
            "issue": "Falta OPENAI_API_KEY en la configuración del ERP.",
        }
    return {
        "ready": True,
        "model_name": model_name,
        "issue": "",
    }


@transaction.atomic
def create_user_turn(*, user, conversation: ChatConversation, content: str, session_key: str = "") -> tuple[ChatMessage, ChatMessage]:
    text = (content or "").strip()
    if not text:
        raise ValueError("El mensaje no puede ir vacío.")
    user_message = ChatMessage.objects.create(
        conversation=conversation,
        sequence=_next_sequence(conversation),
        role=ChatMessage.ROLE_USER,
        status=ChatMessage.STATUS_COMPLETE,
        content=text,
        created_by=user,
    )
    assistant_message = ChatMessage.objects.create(
        conversation=conversation,
        sequence=_next_sequence(conversation),
        role=ChatMessage.ROLE_ASSISTANT,
        status=ChatMessage.STATUS_PENDING,
        content="",
    )
    conversation.last_message_at = timezone.now()
    conversation.session_key = session_key or conversation.session_key
    if conversation.title == "Nueva conversación":
        conversation.title = _title_from_text(text)
    conversation.save(update_fields=["last_message_at", "session_key", "title", "updated_at"])
    return user_message, assistant_message


def execute_chat_turn(*, user, conversation: ChatConversation, user_message: ChatMessage, assistant_message: ChatMessage) -> ChatTurnResult:
    client = _model_client()
    model_name = getattr(settings, "PRIVATE_AI_CHAT_MODEL", "") or DEFAULT_CHAT_MODEL
    messages = _build_model_history(user, conversation)
    tools, tool_map = _build_tool_definitions(user)
    request_kwargs = {
        "model": model_name,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 1400,
    }
    if tools:
        request_kwargs["tools"] = tools
        request_kwargs["tool_choice"] = "auto"
    first_response = client.chat.completions.create(
        **request_kwargs,
    )
    choice = first_response.choices[0].message
    tool_events: list[dict[str, Any]] = []
    final_text = choice.content or ""

    if choice.tool_calls:
        second_messages = list(messages)
        second_messages.append(_assistant_tool_message(choice.tool_calls, content=choice.content or ""))
        for tool_call in choice.tool_calls:
            tool_meta = tool_map.get(tool_call.function.name)
            if not tool_meta:
                payload = {
                    "status": "error",
                    "detail": f"La herramienta {tool_call.function.name} no está disponible para este usuario.",
                }
                tool_events.append(
                    {
                        "tool_name": tool_call.function.name,
                        "tool_display_name": tool_call.function.name,
                        "status": "error",
                        "summary": payload["detail"],
                    }
                )
                second_messages.append(_tool_message_for_model(tool_call.id, payload))
                continue
            try:
                arguments = json.loads(tool_call.function.arguments or "{}")
            except json.JSONDecodeError:
                arguments = {}
            event = _invoke_chat_tool(
                user=user,
                conversation=conversation,
                user_message=user_message,
                assistant_message=assistant_message,
                tool_meta=tool_meta,
                arguments=arguments,
            )
            tool_events.append(event)
            second_messages.append(_tool_message_for_model(tool_call.id, event["payload"]))

        second_response = client.chat.completions.create(
            model=model_name,
            messages=second_messages,
            temperature=0.2,
            max_tokens=1400,
        )
        final_text = second_response.choices[0].message.content or ""

    if not final_text.strip():
        if tool_events:
            rendered = []
            for event in tool_events:
                rendered.append(f"- {event['tool_display_name']}: {event['summary']}")
            final_text = "Ejecuté herramientas del ERP, pero el modelo no devolvió un cierre narrativo. Resultado:\n" + "\n".join(rendered)
        else:
            final_text = "No pude generar una respuesta útil con el contexto disponible."

    assistant_message.content = final_text.strip()
    assistant_message.status = ChatMessage.STATUS_COMPLETE
    assistant_message.metadata_json = {
        "model_name": model_name,
        "tool_events": [
            {
                "tool_name": event["tool_name"],
                "tool_display_name": event["tool_display_name"],
                "status": event["status"],
                "summary": event["summary"],
            }
            for event in tool_events
        ],
    }
    assistant_message.save(update_fields=["content", "status", "metadata_json", "updated_at"])
    conversation.model_name = model_name
    conversation.last_message_at = timezone.now()
    conversation.save(update_fields=["model_name", "last_message_at", "updated_at"])
    ChatConversationState.objects.update_or_create(
        conversation=conversation,
        defaults={
            "token_estimate": sum(len((msg.get("content") or "")) for msg in messages),
            "metadata_json": {
                "last_model_name": model_name,
                "last_tool_count": len(tool_events),
            },
        },
    )
    return ChatTurnResult(
        assistant_text=assistant_message.content,
        model_name=model_name,
        tool_events=tool_events,
    )


def stream_events_for_turn(*, result: ChatTurnResult, assistant_message: ChatMessage):
    yield f"event: meta\ndata: {json.dumps({'assistant_message_id': str(assistant_message.public_id), 'model_name': result.model_name}, ensure_ascii=False)}\n\n"
    for event in result.tool_events:
        yield f"event: tool\ndata: {json.dumps({'tool_name': event['tool_name'], 'tool_display_name': event['tool_display_name'], 'status': event['status'], 'summary': event['summary']}, ensure_ascii=False)}\n\n"
    rendered = ""
    for chunk in _chunk_text(result.assistant_text):
        rendered = f"{rendered} {chunk}".strip()
        yield f"event: chunk\ndata: {json.dumps({'delta': chunk + ' ', 'content': rendered}, ensure_ascii=False)}\n\n"
    yield f"event: done\ndata: {json.dumps({'assistant_message_id': str(assistant_message.public_id), 'content': result.assistant_text}, ensure_ascii=False)}\n\n"
