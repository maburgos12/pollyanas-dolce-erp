"""One bounded READ workflow, authorized afresh on every projection and command."""
from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from uuid import UUID, uuid4

from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.utils import timezone

from activos.services_pasaporte import activos_autorizados
from api.ai_gateway_assets import _asset_choice, fresh_asset_user
from api.ai_gateway_services import invoke_read_shadow_tool
from core.models import AuditLog
from orquestacion.models import AgentWorkflow, ChatConversation, validate_workflow_state

LEASE_SECONDS = 60
TERMINAL = {'COMPLETED', 'CANCELLED', 'EXPIRED'}


class WorkflowError(Exception):
    def __init__(self, code, status=400, detail=None):
        self.code, self.status = code, status
        super().__init__(detail or code)


def _uuid(value):
    try:
        return UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        raise WorkflowError('invalid_input', detail='invalid_uuid') from None


def _access(user, conversation_id=None):
    # Reuse F3's complete ACL fingerprint, even when the origin chat is archived.
    from orquestacion.services.agent_read_runtime import _fresh_access
    actor = fresh_asset_user(user)
    if not actor or getattr(settings, 'AI_AGENT_WORKFLOWS_ENABLED', False) is not True:
        raise WorkflowError('workflow_access_denied', 403)
    if conversation_id is None:
        chat = ChatConversation.objects.filter(owner=actor, status='active').first()
        if not chat:
            # Projection has no target chat: an archived origin must remain resumable.
            from api.ai_gateway_services import list_read_shadow_tools
            from orquestacion.services.agent_read_runtime import access_fingerprint
            if not list_read_shadow_tools(user=actor) or getattr(settings, 'AI_AGENT_READ_ENABLED', False) is not True:
                raise WorkflowError('workflow_access_denied', 403)
            return actor, access_fingerprint(actor)
    else:
        chat = ChatConversation.objects.filter(public_id=_uuid(conversation_id), owner=actor, status='active').first()
        if not chat:
            raise WorkflowError('conversation_unavailable', 403)
    try:
        actor, _, fingerprint = _fresh_access(actor, chat.pk)
    except Exception:
        # Only stable codes reach the additive API, never internal exception text.
        raise WorkflowError('workflow_access_denied', 403) from None
    return actor, fingerprint


def _hash(actor, kind, payload):
    return hashlib.sha256(json.dumps({'actor':actor.pk, 'schema_version':1, 'kind':kind, 'payload':payload}, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def _payload(value, *, command=False):
    if not isinstance(value, dict):
        raise WorkflowError('invalid_input', detail='invalid_payload')
    allowed = {'query', 'asset_id', 'option_position', 'cancel', 'revalidate'} if command else {'query', 'asset_id'}
    if set(value) - allowed or len(value) > 1:
        raise WorkflowError('invalid_input', detail='invalid_fields')
    value = dict(value)
    if 'query' in value:
        if not isinstance(value['query'], str) or len(value['query']) > 180:
            raise WorkflowError('invalid_input', detail='invalid_query')
        value['query'] = value['query'].strip()
    for key, ceiling in (('asset_id', 9223372036854775807), ('option_position', 50)):
        if key in value and (type(value[key]) is not int or not 1 <= value[key] <= ceiling):
            raise WorkflowError('invalid_input', detail='invalid_' + key)
    for key in ('cancel', 'revalidate'):
        if key in value and value[key] is not True:
            raise WorkflowError('invalid_input', detail='invalid_' + key)
    return value


def _validate(wf, actor, fingerprint):
    if wf.kind != AgentWorkflow.KIND or wf.schema_version != 1 or wf.status not in AgentWorkflow.STATUSES:
        raise WorkflowError('unsupported_schema', 409)
    try:
        validate_workflow_state(wf.state_json)
    except ValidationError:
        raise WorkflowError('unsupported_schema', 409) from None
    if wf.state_json['provenance']['access_fingerprint'] != fingerprint:
        raise WorkflowError('workflow_access_changed', 403)
    asset_id = wf.state_json['asset_id']
    if asset_id and not activos_autorizados(actor).filter(pk=asset_id).exists():
        raise WorkflowError('resource_access_changed', 403)


def _status(wf):
    if wf.expires_at and wf.expires_at <= timezone.now():
        return 'EXPIRED'
    if wf.status == 'RUNNING' and (not wf.lease_expires_at or wf.lease_expires_at <= timezone.now()):
        return 'REVALIDATION_REQUIRED'
    return wf.status


def _dto(wf, actor):
    state = wf.state_json
    assets = {row.pk:row for row in activos_autorizados(actor).select_related('sucursal').filter(pk__in=state['option_ids'] + ([state['asset_id']] if state['asset_id'] else []))}
    options = [{'position':position, 'available':pk in assets, **({'asset':_asset_choice(assets[pk])} if pk in assets else {})} for position, pk in enumerate(state['option_ids'], 1)]
    status = _status(wf)
    next_step = {'WAITING_INFORMATION':'provide_information', 'WAITING_SELECTION':'select_asset',
                 'READY':'resume_read', 'RUNNING':'wait', 'REVALIDATION_REQUIRED':'revalidate',
                 'COMPLETED':'none', 'CANCELLED':'none', 'EXPIRED':'none'}[status]
    return {'public_id':str(wf.public_id), 'next_step':next_step, 'kind':wf.kind, 'schema_version':wf.schema_version,
            'status':status, 'version':wf.version, 'query':state['query'], 'options':options,
            'asset':_asset_choice(assets[state['asset_id']]) if state['asset_id'] in assets else None,
            'missing_fields':state['missing_fields'], 'sources':state['provenance']['sources'],
            'as_of':state['provenance']['as_of'], 'created_at':wf.created_at.isoformat(),
            'updated_at':wf.updated_at.isoformat(), 'expires_at':wf.expires_at.isoformat() if wf.expires_at else None}


def _owned(actor, public_id, *, lock=False):
    queryset = AgentWorkflow.objects.select_for_update() if lock else AgentWorkflow.objects
    wf = queryset.filter(public_id=_uuid(public_id), owner=actor).first()
    if not wf:
        raise WorkflowError('workflow_not_found', 404)
    return wf


def _audit(wf, actor, action, request_id):
    AuditLog.objects.create(user=actor, action=action, model='orquestacion.AgentWorkflow', object_id=str(wf.public_id),
                            payload={'request_id':str(request_id), 'kind':wf.kind, 'schema_version':wf.schema_version, 'status':wf.status, 'version':wf.version})


def _save(wf, actor, action, request_id, digest):
    validate_workflow_state(wf.state_json)
    wf.last_request_id, wf.last_request_hash = request_id, digest
    wf.last_response_json = {'public_id':str(wf.public_id), 'status':wf.status, 'version':wf.version}
    wf.save()
    _audit(wf, actor, action, request_id)


def get_workflow(*, user, public_id):
    actor, fingerprint = _access(user)
    wf = _owned(actor, public_id)
    _validate(wf, actor, fingerprint)
    return _dto(wf, actor)


def list_workflows(*, user, status=None, kind=None, page=1, page_size=20, pending_only=False):
    actor, fingerprint = _access(user)
    if status is not None and status not in AgentWorkflow.STATUSES or kind is not None and kind != AgentWorkflow.KIND:
        raise WorkflowError('invalid_input', detail='invalid_filter')
    if type(page) is not int or page < 1 or page > 1000 or type(page_size) is not int or not 1 <= page_size <= 20:
        raise WorkflowError('invalid_input', detail='invalid_pagination')
    queryset = AgentWorkflow.objects.filter(owner=actor).order_by('-updated_at', '-id')
    if kind:
        queryset = queryset.filter(kind=kind)
    now = timezone.now()
    expired = Q(expires_at__lte=now) | Q(status='EXPIRED')
    lost_lease = Q(status='RUNNING') & (Q(lease_expires_at__lte=now) | Q(lease_expires_at__isnull=True))
    if status == 'EXPIRED':
        queryset = queryset.filter(expired)
    elif status == 'REVALIDATION_REQUIRED':
        queryset = queryset.filter(Q(status=status) | lost_lease).exclude(expired)
    elif status:
        queryset = queryset.filter(status=status).exclude(expired)
        if status == 'RUNNING':
            queryset = queryset.exclude(lost_lease)
    if pending_only:
        queryset = queryset.exclude(status__in=TERMINAL).exclude(expired).filter(state_json__provenance__access_fingerprint=fingerprint)
    items = []
    offset = (page-1)*page_size
    valid = 0
    # Validate before pagination: revoked resources must not consume a page slot.
    for wf in queryset.iterator(chunk_size=20):
        try:
            _validate(wf, actor, fingerprint)
        except WorkflowError:
            continue
        if status and _status(wf) != status or pending_only and _status(wf) in TERMINAL:
            continue
        if valid >= offset:
            items.append(_dto(wf, actor))
        valid += 1
        if len(items) == page_size:
            break
    return {'items':items, 'page':page, 'page_size':page_size}


def _selection(actor, state, payload):
    if 'option_position' in payload:
        position = payload['option_position']
        if position > len(state['option_ids']):
            raise WorkflowError('invalid_input', detail='option_unavailable')
        payload = {'asset_id':state['option_ids'][position-1]}
    if 'asset_id' in payload:
        if not activos_autorizados(actor).filter(pk=payload['asset_id']).exists():
            raise WorkflowError('execution_interrupted', 409, detail='option_unavailable')
        return {**state, 'asset_id':payload['asset_id'], 'missing_fields':[]}, 'READY'
    query = payload.get('query', state['query'])
    state = {**state, 'query':query, 'asset_id':None, 'option_ids':[], 'missing_fields':['query_or_asset']}
    if not query:
        return state, 'WAITING_INFORMATION'
    result = invoke_read_shadow_tool(user=actor, tool_key='erp.search_assets', arguments={'q':query, 'limit':50}, mode='READ')['result']
    ids = [item['id'] for item in result['payload']['items']]
    state['option_ids'] = ids
    state['provenance'] = {**state['provenance'], 'sources':result['sources'], 'as_of':result['as_of']}
    if len(ids) == 1:
        state.update(asset_id=ids[0], missing_fields=[])
        return state, 'READY'
    state['missing_fields'] = ['asset_selection'] if ids else ['query_or_asset']
    return state, 'WAITING_SELECTION' if ids else 'WAITING_INFORMATION'


def create_workflow(*, user, kind, origin_request_id, conversation_id, payload):
    actor, fingerprint = _access(user, conversation_id)
    if kind != AgentWorkflow.KIND:
        raise WorkflowError('invalid_input', detail='invalid_kind')
    request_id, payload = _uuid(origin_request_id), _payload(payload)
    digest = _hash(actor, kind, {'action':'create', 'conversation_id':str(_uuid(conversation_id)), **payload})
    existing = AgentWorkflow.objects.filter(origin_request_id=request_id).first()
    if existing:
        if existing.owner_id != actor.pk:
            raise WorkflowError('request_payload_mismatch', 409)
        _validate(existing, actor, fingerprint)
        if existing.state_json['provenance']['origin_hash'] != digest:
            raise WorkflowError('request_payload_mismatch', 409)
        if existing.last_request_id != request_id or existing.version != 1:
            raise WorkflowError('version_conflict', 409)
        return _dto(existing, actor)
    state = {'query':'', 'option_ids':[], 'asset_id':None, 'missing_fields':['query_or_asset'],
             'provenance':{'origin_hash':digest, 'access_fingerprint':fingerprint, 'sources':[], 'as_of':timezone.now().isoformat()}}
    state, status = _selection(actor, state, payload)
    actor, current = _access(user, conversation_id)
    if current != fingerprint:
        raise WorkflowError('workflow_access_changed', 403)
    try:
        with transaction.atomic():
            wf = AgentWorkflow(owner=actor, origin_conversation=ChatConversation.objects.get(public_id=_uuid(conversation_id)),
                               origin_request_id=request_id, kind=kind, status=status, state_json=state)
            _validate(wf, actor, fingerprint)
            _save(wf, actor, 'AI_WORKFLOW_CREATE', request_id, digest)
            return _dto(wf, actor)
    except IntegrityError:
        # A competing identical create may have committed while this insert waited.
        actor, current = _access(user, conversation_id)
        existing = AgentWorkflow.objects.filter(origin_request_id=request_id).first()
        if existing is None:
            raise  # An audit/constraint failure is not a successful replay.
        if existing.owner_id != actor.pk:
            raise WorkflowError('request_payload_mismatch', 409) from None
        _validate(existing, actor, current)
        if existing.state_json['provenance']['origin_hash'] != digest:
            raise WorkflowError('request_payload_mismatch', 409)
        if existing.last_request_id != request_id or existing.version != 1:
            raise WorkflowError('version_conflict', 409) from None
        return _dto(existing, actor)


def _check_command(wf, request_id, digest, expected_version, payload):
    status = _status(wf)
    if status == 'EXPIRED':
        raise WorkflowError('workflow_expired', 409)
    if wf.last_request_id == request_id:
        if wf.last_request_hash != digest:
            raise WorkflowError('request_payload_mismatch', 409)
        if status == 'RUNNING':
            raise WorkflowError('execution_in_progress', 409)
        if status == 'REVALIDATION_REQUIRED':
            raise WorkflowError('execution_interrupted', 409)
        return True
    if wf.version != expected_version or AuditLog.objects.filter(model='orquestacion.AgentWorkflow', object_id=str(wf.public_id), payload__request_id=str(request_id)).exists():
        raise WorkflowError('version_conflict', 409)
    if status in TERMINAL:
        raise WorkflowError('invalid_input', detail='workflow_terminal')
    if status == 'RUNNING':
        raise WorkflowError('execution_in_progress', 409)
    if status == 'REVALIDATION_REQUIRED' and payload != {'revalidate':True}:
        raise WorkflowError('execution_interrupted', 409)
    return False


def command_workflow(*, user, public_id, expected_version, request_id, payload, conversation_id=None, resume=False):
    actor, fingerprint = _access(user, conversation_id if resume else None)
    request_id, payload = _uuid(request_id), _payload(payload, command=True)
    if resume and set(payload) & {'cancel', 'revalidate'}:
        raise WorkflowError('invalid_input', detail='invalid_resume_fields')
    if type(expected_version) is not int or expected_version < 1:
        raise WorkflowError('invalid_input', detail='invalid_expected_version')
    digest = _hash(actor, AgentWorkflow.KIND, {'action':'resume' if resume else 'update', 'expected_version':expected_version,
                       'conversation_id':str(_uuid(conversation_id)) if conversation_id else None, **payload})
    # Search runs before the lock; version validation below rejects concurrent edits.
    candidate = _owned(actor, public_id)
    _validate(candidate, actor, fingerprint)
    replay = _check_command(candidate, request_id, digest, expected_version, payload)
    selection = None
    if set(payload) & {'query', 'asset_id', 'option_position'} and not replay:
        selection = _selection(actor, candidate.state_json, payload)
    actor, current = _access(user, conversation_id if resume else None)
    if current != fingerprint:
        raise WorkflowError('workflow_access_changed', 403)
    with transaction.atomic():
        wf = _owned(actor, public_id, lock=True)
        actor, fingerprint = _access(user, conversation_id if resume else None)
        _validate(wf, actor, fingerprint)
        if _check_command(wf, request_id, digest, expected_version, payload):
            return {'workflow':_dto(wf, actor), 'read_result':None}
        status = _status(wf)
        if 'cancel' in payload:
            wf.status = 'CANCELLED'
        elif 'revalidate' in payload:
            if status != 'REVALIDATION_REQUIRED' or resume:
                raise WorkflowError('invalid_input', detail='invalid_revalidation')
            wf.status = 'READY' if wf.state_json['asset_id'] else 'WAITING_SELECTION' if wf.state_json['option_ids'] else 'WAITING_INFORMATION'
            wf.lease_token = wf.lease_expires_at = None
        elif selection:
            wf.state_json, wf.status = selection
            _validate(wf, actor, fingerprint)
        elif not resume:
            raise WorkflowError('invalid_input', detail='missing_update_fields')
        executing = resume and wf.status == 'READY'
        if executing:
            wf.status, wf.lease_token = 'RUNNING', uuid4()
            wf.lease_expires_at = timezone.now() + timedelta(seconds=LEASE_SECONDS)
        wf.version += 1
        _save(wf, actor, 'AI_WORKFLOW_START' if executing else 'AI_WORKFLOW_UPDATE', request_id, digest)
        token, started_version = wf.lease_token, wf.version
        if not executing:
            return {'workflow':_dto(wf, actor), 'read_result':None}
    # Gateway/provider/network work must never run while holding row locks.
    try:
        actor, current = _access(user, conversation_id)
        _validate(wf, actor, current)
        result = invoke_read_shadow_tool(user=actor, tool_key='erp.get_asset_context', arguments={'activo_id':wf.state_json['asset_id']}, mode='READ')
        actor, current = _access(user, conversation_id)
        _validate(wf, actor, current)
        if result['result']['status'] != 'ok' or result['result']['payload'].get('activo', {}).get('id') != wf.state_json['asset_id']:
            raise WorkflowError('execution_interrupted', 409)
        with transaction.atomic():
            wf = _owned(actor, public_id, lock=True)
            actor, current = _access(user, conversation_id)
            _validate(wf, actor, current)
            if wf.status != 'RUNNING' or wf.lease_token != token or wf.version != started_version or not wf.lease_expires_at or wf.lease_expires_at <= timezone.now():
                raise WorkflowError('execution_interrupted', 409)
            wf.status = 'COMPLETED'
            wf.lease_token = wf.lease_expires_at = None
            wf.state_json['provenance'].update(sources=result['result']['sources'], as_of=result['result']['as_of'])
            wf.version += 1
            _save(wf, actor, 'AI_WORKFLOW_COMPLETE', request_id, digest)
            return {'workflow':_dto(wf, actor), 'read_result':result}
    except Exception:
        with transaction.atomic():
            lost = AgentWorkflow.objects.select_for_update().filter(public_id=_uuid(public_id), owner=actor, status='RUNNING', lease_token=token).first()
            if lost:
                lost.status = 'REVALIDATION_REQUIRED'
                lost.lease_token = lost.lease_expires_at = None
                lost.version += 1
                _save(lost, actor, 'AI_WORKFLOW_REVALIDATE', request_id, digest)
        raise


def technical_serializer(key):
    from api.ai_gateway_serializers import WorkflowPendingArguments, WorkflowPrepareArguments, WorkflowResumeArguments
    return {'workflow.prepare_asset_maintenance':WorkflowPrepareArguments, 'workflow.list_pending':WorkflowPendingArguments,
            'workflow.resume_asset_maintenance':WorkflowResumeArguments}[key]


def resume_tool_arguments(value):
    """Adapt the exclusive LLM contract; REST/service payloads remain unchanged."""
    from rest_framework.exceptions import ValidationError
    if not isinstance(value, dict) or set(value) != {'workflow_id', 'expected_version', 'continuation'}:
        raise ValidationError('Invalid continuation contract.')
    action = value['continuation']
    if not isinstance(action, dict) or len(action) > 1 or set(action) - {'query', 'asset_id', 'option_position'}:
        raise ValidationError('Use exactly one continuation variant.')
    return {key:value[key] for key in ('workflow_id', 'expected_version')} | action


def technical_catalog():
    definitions = [
        ('workflow.prepare_asset_maintenance', 'erp_prepare_asset_maintenance', 'Conservar consulta de mantenimiento', 'Inicia UNA consulta NUEVA explícitamente solicitada y conserva sus faltantes. No usar para continuar, completar, identificar ni seleccionar en una consulta que ya aparece en pending_workflows; para eso usa erp_resume_asset_maintenance. Una búsqueda común no crea un pendiente.'),
        ('workflow.list_pending', 'erp_list_pending_workflows', 'Consultar consultas pendientes', 'Máximo 20 consultas propias pendientes con UUID, versión y opciones actuales; sin historial narrativo.'),
        ('workflow.resume_asset_maintenance', 'erp_resume_asset_maintenance', 'Reanudar consulta de mantenimiento', 'CONTINÚA el mismo UUID existente, también en WAITING_INFORMATION y WAITING_SELECTION: continuation.query aporta el equipo que faltaba, continuation.option_position elige su opción, continuation.asset_id identifica un equipo; continuation={} consulta y COMPLETA un READY. Guarda avances aunque falten datos, sin crear otro UUID. Exige selección del usuario entre pendientes ambiguos. Sin acciones operativas.'),
    ]
    catalog = [{'key':key, 'name':name, 'display_name':display, 'description':description, 'workflow':True,
                'argument_schema':technical_serializer(key).argument_schema()} for key, name, display, description in definitions]
    resume = catalog[-1]
    # Only the provider contract changes; backend validation still owns execution.
    resume['strict'] = True
    resume['argument_schema'] = {
        'type':'object', 'additionalProperties':False,
        'required':['workflow_id', 'expected_version', 'continuation'],
        'properties':{
            'workflow_id':{'type':'string', 'format':'uuid'},
            'expected_version':{'type':'integer', 'minimum':1},
            'continuation':{'anyOf':[
                {'type':'object', 'properties':{key:schema}, 'required':[key], 'additionalProperties':False}
                for key, schema in (
                    ('query', {'type':'string', 'description':'Fragmento literal del equipo que faltaba identificar.'}),
                    ('asset_id', {'type':'integer', 'minimum':1}),
                    ('option_position', {'type':'integer', 'minimum':1, 'maximum':50}),
                )
            ] + [{'type':'object', 'properties':{}, 'required':[], 'additionalProperties':False}]},
        },
    }
    return catalog


def invoke_workflow_tool(*, user, tool_key, arguments, conversation, user_message, call_id):
    from uuid import uuid5, NAMESPACE_URL
    if not ChatConversation.objects.filter(pk=conversation.pk, owner=user, status='active').exists() or not user_message.__class__.objects.filter(pk=user_message.pk, conversation=conversation, created_by=user, role='user', status='complete').exists():
        raise WorkflowError('conversation_unavailable', 403)
    # Deterministic per actual server message + provider call; actor cannot be supplied.
    request_id = uuid5(NAMESPACE_URL, f'erp-workflow:{user.pk}:{user_message.public_id}:{call_id}:{tool_key}')
    result = None
    if tool_key == 'workflow.prepare_asset_maintenance':
        workflow = create_workflow(user=user, kind=AgentWorkflow.KIND, origin_request_id=request_id,
                                   conversation_id=conversation.public_id, payload=arguments)
        payload = {'workflow':workflow}
        status, sources, as_of = workflow['status'], workflow['sources'], workflow['as_of']
    elif tool_key == 'workflow.list_pending':
        payload = list_workflows(user=user, pending_only=True)
        status, sources, as_of = 'ok', ['orquestacion.AgentWorkflow'], timezone.now().isoformat()
    elif tool_key == 'workflow.resume_asset_maintenance':
        command = command_workflow(user=user, public_id=arguments['workflow_id'], expected_version=arguments['expected_version'],
                           request_id=request_id, payload={key:value for key, value in arguments.items() if key in {'option_position', 'query', 'asset_id'}},
                           conversation_id=conversation.public_id, resume=True)
        workflow, result = command['workflow'], command['read_result']
        payload = {'workflow':workflow}
        if result:
            payload['consultation'] = result['result']['payload']
            payload['asset_ids'] = [result['result']['payload']['activo']['id']]
        status, sources, as_of = workflow['status'], workflow['sources'], workflow['as_of']
    else:
        raise WorkflowError('unknown_workflow_tool', 403)
    return {'result':{'status':status, 'sources':sources, 'as_of':as_of, 'payload':payload}}
