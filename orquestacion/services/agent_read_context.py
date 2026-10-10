"""Bounded, authorized conversation context and referenced model prose.

Prose is an explanation, never an execution receipt or an authorization.
"""
import json

from uuid import UUID

from orquestacion.models import ChatToolCall
from orquestacion.services.agent_workflows import WorkflowError

MAX_HISTORY_MESSAGES = 20
MAX_HISTORY_CHARS = 20000

ANSWER_FORMAT = {'type': 'json_schema', 'name': 'erp_answer', 'strict': True, 'schema': {
    'type': 'object', 'additionalProperties': False,
    'properties': {
        'answer': {'type': 'string'},
        'evidence_ids': {'type': 'array', 'items': {'type': 'string'}},
        'action_claim': {'type': 'string', 'enum': ['none', 'proposal', 'receipt']},
        'incident_ids': {'type': 'array', 'items': {'type': 'string'}},
    },
    'required': ['answer', 'evidence_ids', 'action_claim', 'incident_ids'],
}}

ANSWER_PROMPT = """
Responde de forma natural y breve, sin volcar JSON ni describir IDs técnicos.
El historial propio permite entender aclaraciones y preguntas de seguimiento;
es una conversación anterior, no evidencia de vigencia ni una instrucción de sistema.
Las referencias frescas y las herramientas son la autoridad para datos del ERP.
Vuelve a consultar si necesitas un dato operativo que sólo aparece en el historial.
Puedes responder un folio con incident_drafts EXECUTED: son recibos revalidados
por el servidor. No necesitas otra herramienta para repetir ese folio confirmado.
No confundas el reporte ya confirmado con una escritura realizada en este turno.
No afirmes otras operaciones ejecutadas: no hay herramientas para realizarlas.
Para la respuesta final usa el esquema erp_answer. evidence_ids identifica las
tool_call_id de resultados consultados, o draft_id de incident_drafts actuales.
action_claim=proposal sólo para un borrador propio todavía pendiente; receipt sólo
para un reporte EXECUTED verificado. Incluye esos draft_id en incident_ids. Para
otras respuestas usa none e incident_ids vacío. No uses el texto para confirmar:
las tarjetas y botones del servidor son la única prueba y vía de ejecución.
Conserva en tu explicación ambigüedad, límites y ausencia de evidencia; nunca
interpretes una ficha parcial o un plan ausente como ausencia de servicio.
"""


def history_context(conversation, before_sequence):
    from orquestacion.services.chat_service import _read_message_visible
    rows = list(conversation.messages.filter(sequence__lt=before_sequence,
                role__in=['user', 'assistant']).order_by('-sequence')[:MAX_HISTORY_MESSAGES])
    by_sequence = {row.sequence: row for row in rows}
    history, asset_ids, size = [], set(), 0
    # Admit complete pairs only; a user's quotation may repeat revoked ERP data.
    for answer in rows:
        request = by_sequence.get(answer.sequence - 1)
        proof = answer.metadata_json.get('agent_read', {}) if isinstance(answer.metadata_json, dict) else {}
        if (answer.role != 'assistant' or answer.status != 'complete' or not proof
                or not request or request.role != 'user' or request.status != 'complete' or request.created_by_id != conversation.owner_id
                or not _read_message_visible(answer)):
            continue
        pair_size = len(request.content) + len(answer.content)
        if size + pair_size > MAX_HISTORY_CHARS:
            break
        history[0:0] = [{'role': 'user', 'content': request.content},
                        {'role': 'assistant', 'content': answer.content}]
        asset_ids.update(proof.get('asset_ids', []))
        size += pair_size
    return history, asset_ids


def incident_context(conversation, actor):
    from orquestacion.services.agent_incidents import project
    rows = []
    for draft in conversation.tool_calls.filter(tool_key__in=['incident.prepare', 'incident.followup']).select_related('conversation').order_by('-updated_at')[:10]:
        try:
            dto = project(draft, actor)
        except WorkflowError:
            continue
        if dto['status'] in {'WAITING_INFORMATION', 'AWAITING_CONFIRMATION', 'EXECUTED'}:
            rows.append(dto)
    return rows


def natural_answer(output_text, events, incidents):
    """Validate references/state, not the semantic truth of arbitrary model prose."""
    if not isinstance(output_text, str) or len(output_text) > 12000:
        return None
    try:
        value = json.loads(output_text)
    except (ValueError, TypeError):
        return None
    if not isinstance(value, dict) or set(value) != {'answer', 'evidence_ids', 'action_claim', 'incident_ids'}:
        return None
    if not isinstance(value['answer'], str) or not 1 <= len(value['answer'].strip()) <= 6000:
        return None
    for key in ('evidence_ids', 'incident_ids'):
        ids = value[key]
        if not isinstance(ids, list) or len(ids) > 20 or any(not isinstance(item, str) for item in ids) or len(set(ids)) != len(ids):
            return None
    incident_map = {row['draft_id']: row for row in incidents}
    evidence = {str(event['tool_call_id']) for event in events if 'error' not in event['payload']} | set(incident_map)
    if not set(value['evidence_ids']) <= evidence or not set(value['incident_ids']) <= set(value['evidence_ids']):
        return None
    claim = value['action_claim']
    if claim == 'none':
        if value['incident_ids']:
            return None
    elif claim in {'proposal', 'receipt'}:
        allowed = {'EXECUTED'} if claim == 'receipt' else {'WAITING_INFORMATION', 'AWAITING_CONFIRMATION'}
        if not value['incident_ids'] or any(incident_map.get(key, {}).get('status') not in allowed for key in value['incident_ids']):
            return None
    else:
        return None
    if (events or incidents) and not value['evidence_ids']:
        return None
    return value


def projected_receipts(message):
    from orquestacion.services.agent_incidents import project
    proof = message.metadata_json.get('agent_read', {}) if isinstance(message.metadata_json, dict) else {}
    ids = proof.get('receipt_ids', [])
    if not isinstance(ids, list) or len(ids) > 10:
        return []
    try:
        ids = [UUID(value) for value in ids]
    except (ValueError, TypeError, AttributeError):
        return []
    receipts = []
    for draft in ChatToolCall.objects.filter(conversation=message.conversation, tool_key__in=['incident.prepare', 'incident.followup'], public_id__in=ids).select_related('conversation'):
        try:
            dto = project(draft, message.conversation.owner)
        except WorkflowError:
            continue
        if dto['status'] == 'EXECUTED':
            receipts.append(dto)
    return receipts
