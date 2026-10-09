"""Prepare an owned incident; only an authenticated human confirmation can write."""
from datetime import timedelta
import hashlib
import json
from uuid import uuid5

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from rest_framework import serializers

from activos.services_pasaporte import activos_autorizados, puede_reportar_activo
from api.ai_gateway_assets import StrictArguments, StrictIntegerField, StrictStringField, can_read_assets, fresh_asset_user
from core.models import AuditLog
from fallas.models import CategoriaFalla, ReporteFalla
from fallas.views import _puede_reportar_fallas
from core.access import is_admin_or_dg
from operacion.services_fallas import crear_reporte_falla
from orquestacion.models import ChatConversation, ChatMessage, ChatToolCall, ChatToolResult
from orquestacion.services.agent_workflows import WorkflowError

KEY = 'incident.prepare'
FIELDS = ('activo_id', 'categoria_id', 'titulo', 'descripcion', 'prioridad', 'justificacion_sin_foto')
REQUIRED = ('activo_id', 'categoria_id', 'titulo', 'descripcion', 'justificacion_sin_foto')


class IncidentArguments(StrictArguments):
    draft_id = serializers.UUIDField(required=False)
    expected_version = StrictIntegerField(required=False, min_value=1)
    activo_id = StrictIntegerField(required=False, min_value=1, max_value=9223372036854775807)
    categoria_id = StrictIntegerField(required=False, min_value=1, max_value=9223372036854775807)
    titulo = StrictStringField(required=False, allow_blank=True, max_length=200)
    descripcion = StrictStringField(required=False, allow_blank=True, max_length=3000)
    prioridad = serializers.ChoiceField(required=False, choices=ReporteFalla.PRIORIDAD)
    justificacion_sin_foto = StrictStringField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        if ('draft_id' in attrs) != ('expected_version' in attrs):
            raise serializers.ValidationError('La continuación requiere referencia y versión.')
        return attrs


class IncidentConfirmation(StrictArguments):
    expected_version = StrictIntegerField(min_value=1)
    payload_hash = StrictStringField(min_length=64, max_length=64)
    confirm = serializers.BooleanField()

    def validate_confirm(self, value):
        if self.initial_data.get('confirm') is not True:
            raise serializers.ValidationError('Se requiere confirmación explícita.')
        return value


def _actor(user, conversation):
    actor = fresh_asset_user(user)
    if (getattr(settings, 'AI_AGENT_INCIDENTS_ENABLED', False) is not True or getattr(settings, 'AI_AGENT_READ_ENABLED', False) is not True or not can_read_assets(actor)
            or not (is_admin_or_dg(actor) or _puede_reportar_fallas(actor))
            or not ChatConversation.objects.filter(pk=conversation.pk, owner=actor, status='active').exists()):
        raise WorkflowError('incident_access_denied', 403)
    return actor


def enabled(user):
    actor = fresh_asset_user(user)
    return bool(getattr(settings, 'AI_AGENT_INCIDENTS_ENABLED', False) is True and getattr(settings, 'AI_AGENT_READ_ENABLED', False) is True and can_read_assets(actor)
                and (is_admin_or_dg(actor) or _puede_reportar_fallas(actor)))


def catalog(user):
    if not enabled(user):
        return []
    return [
        {'key':'incident.requirements', 'name':'erp_incident_requirements', 'display_name':'Requisitos de un reporte de falla',
         'description':'Consulta categorías activas, prioridades y campos requeridos para reportar una falla de equipo.',
         'argument_schema':StrictArguments.argument_schema(), 'incident':True},
        {'key':KEY, 'name':'erp_prepare_incident', 'display_name':'Preparar reporte de falla',
         'description':'Conserva un borrador propio, detecta datos faltantes y propone el reporte. Nunca crea la falla. Continúa con draft_id y expected_version del borrador existente.',
         'argument_schema':IncidentArguments.argument_schema(), 'incident':True},
    ]


def _asset(actor, arguments, *, lock=False):
    if not arguments.get('activo_id'):
        return None
    rows = activos_autorizados(actor).select_related('sucursal')
    if lock:
        rows = rows.select_for_update(of=('self',))
    asset = rows.filter(pk=arguments['activo_id']).first()
    if not asset or not puede_reportar_activo(actor, asset):
        raise WorkflowError('incident_resource_unavailable', 403)
    return asset


def _category(arguments, *, lock=False):
    if not arguments.get('categoria_id'):
        return None
    rows = CategoriaFalla.objects.filter(activo=True, tipo=CategoriaFalla.TIPO_EQUIPO)
    if lock:
        rows = rows.select_for_update()
    category = rows.filter(pk=arguments['categoria_id']).first()
    if not category:
        raise WorkflowError('incident_category_unavailable', 409)
    return category


def _hash(args, branch_id):
    return hashlib.sha256(json.dumps({'arguments':args, 'branch_id':branch_id}, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def project(draft, actor):
    _actor(actor, draft.conversation)
    asset = _asset(actor, draft.arguments_json)
    category = _category(draft.arguments_json)
    missing = [key for key in REQUIRED if not draft.arguments_json.get(key)]
    metadata = draft.metadata_json
    if asset and metadata['branch_id'] != asset.sucursal_id:
        raise WorkflowError('incident_resource_changed', 409)
    status = metadata['incident_status']
    if status == 'EXECUTED' and not ReporteFalla.objects.filter(
            pk=metadata.get('report_id'), activo_relacionado=asset,
            sucursal_id=metadata['branch_id'], reportado_por_id=metadata.get('confirmed_by')).exists():
        raise WorkflowError('incident_receipt_unavailable', 409)
    if status != 'EXECUTED' and timezone.now() >= timezone.datetime.fromisoformat(metadata['expires_at']):
        status = 'EXPIRED'
    existing = list(ReporteFalla.objects.filter(activo_relacionado=asset, sucursal_id=asset.sucursal_id,
                    estatus__in=['abierto','en_revision','en_proceso'], duplicado_de__isnull=True).values('id','titulo')[:20]) if asset and status != 'EXECUTED' else []
    if existing and status != 'EXPIRED':
        status = 'REVIEW_REQUIRED'
    return {'draft_id':str(draft.public_id), 'version':metadata['version'], 'status':status,
            'existing_reports':existing,
            'payload_hash':metadata['payload_hash'], 'missing_fields':missing,
            'fields':draft.arguments_json, 'asset':{'id':asset.pk, 'nombre':asset.nombre, 'codigo':asset.codigo, 'sucursal':asset.sucursal.nombre} if asset else None,
            'categoria':category.nombre if category else None, 'report_id':metadata.get('report_id'),
            'confirmed_at':metadata.get('confirmed_at'),
            'expires_at':metadata['expires_at']}


def _response(dto):
    return {'result':{'status':dto['status'], 'sources':['fallas.ReporteFalla', 'orquestacion.ChatToolCall'],
                      'as_of':timezone.now().isoformat(), 'payload':{'incident':dto}}}


def invoke(*, user, tool_key, arguments, conversation, user_message, assistant_message, call_id):
    actor = _actor(user, conversation)
    if tool_key == 'incident.requirements':
        return {'result':{'status':'ok', 'sources':['fallas.CategoriaFalla', 'fallas.ReporteFalla'], 'as_of':timezone.now().isoformat(),
                         'payload':{'categories':list(CategoriaFalla.objects.filter(activo=True, tipo='equipo').values('id', 'nombre')[:50]),
                                    'priorities':dict(ReporteFalla.PRIORIDAD), 'required_fields':list(REQUIRED),
                                    'evidence_rule':'Foto o justificación explícita. Este corte admite justificación; no adjunta fotos.'}}}
    if tool_key != KEY or not ChatMessage.objects.filter(pk=user_message.pk, conversation=conversation, created_by=actor, role='user', status='complete').exists():
        raise WorkflowError('invalid_incident_origin', 403)
    serializer = IncidentArguments(data=arguments)
    serializer.is_valid(raise_exception=True)
    args = serializer.validated_data
    origin = str(uuid5(user_message.public_id, call_id))
    changes = {key:args[key] for key in FIELDS if key in args}
    command_hash = hashlib.sha256(json.dumps(changes, sort_keys=True).encode()).hexdigest()
    with transaction.atomic():
        # One conversation row serializes initial inserts; the UUID is also unique.
        ChatConversation.objects.select_for_update().get(pk=conversation.pk)
        draft = ChatToolCall.objects.select_for_update().filter(public_id=args.get('draft_id', origin), conversation=conversation, tool_key=KEY).first()
        if args.get('draft_id') and not draft:
            raise WorkflowError('incident_not_found', 404)
        if draft and draft.metadata_json['last_request_id'] == origin:
            if draft.metadata_json['last_request_hash'] != command_hash:
                raise WorkflowError('request_payload_mismatch', 409)
            return _response(project(draft, actor))
        if draft:
            current = project(draft, actor)
            if current['status'] not in {'WAITING_INFORMATION', 'AWAITING_CONFIRMATION', 'REVIEW_REQUIRED'} or current['version'] != args.get('expected_version'):
                raise WorkflowError('incident_version_conflict', 409)
            data = {**draft.arguments_json, **changes}
            version = current['version'] + 1
        else:
            data = {'prioridad':ReporteFalla.PRIORIDAD_MEDIA, **changes}
            version = 1
        asset, category = _asset(actor, data), _category(data)
        missing = [key for key in REQUIRED if not data.get(key)]
        if not missing:
            report = ReporteFalla(sucursal=asset.sucursal, activo_relacionado=asset, categoria=category,
                                 titulo=data['titulo'], descripcion=data['descripcion'], prioridad=data['prioridad'],
                                 justificacion_sin_foto=data['justificacion_sin_foto'], reportado_por=actor)
            report.full_clean()
        branch_id = asset.sucursal_id if asset else None
        metadata = {'incident_status':'WAITING_INFORMATION' if missing else 'AWAITING_CONFIRMATION',
                    'version':version, 'branch_id':branch_id, 'payload_hash':_hash(data, branch_id),
                    'expires_at':(timezone.now() + timedelta(hours=24)).isoformat(),
                    'last_request_id':origin, 'last_request_hash':command_hash}
        if draft:
            draft.arguments_json, draft.metadata_json = data, {**draft.metadata_json, **metadata}
            draft.assistant_message = assistant_message
            draft.save(update_fields=['arguments_json', 'metadata_json', 'assistant_message', 'updated_at'])
        else:
            draft = ChatToolCall.objects.create(public_id=origin, conversation=conversation, request_message=user_message,
                    assistant_message=assistant_message, tool_key=KEY, tool_name='erp_prepare_incident', tool_display_name='Preparar reporte de falla',
                    arguments_json=data, metadata_json=metadata, status='complete', requires_approval=False)
        payload = _response(project(draft, actor))
        ChatToolResult.objects.update_or_create(tool_call=draft, defaults={'summary':metadata['incident_status'], 'result_json':payload})
        AuditLog.objects.create(user=actor, action='AI_INCIDENT_PREPARE', model='orquestacion.ChatToolCall', object_id=str(draft.public_id),
                                payload={'conversation_id':str(conversation.public_id), 'request_message_id':str(user_message.public_id), 'assistant_message_id':str(assistant_message.public_id), 'origin_request_id':origin, 'version':version, 'arguments':data, 'payload_hash':metadata['payload_hash']})
        return payload


def confirm(*, user, draft_id, arguments):
    serializer = IncidentConfirmation(data=arguments)
    serializer.is_valid(raise_exception=True)
    body = serializer.validated_data
    with transaction.atomic():
        draft = ChatToolCall.objects.select_for_update(of=('self',)).select_related('conversation').filter(public_id=draft_id, conversation__owner=user, tool_key=KEY).first()
        if not draft:
            raise WorkflowError('incident_not_found', 404)
        actor = _actor(user, draft.conversation)
        metadata = draft.metadata_json
        if body['expected_version'] != metadata['version'] or body['payload_hash'] != metadata['payload_hash']:
            raise WorkflowError('incident_version_conflict', 409)
        dto = project(draft, actor)
        if dto['status'] == 'EXECUTED':
            return _response(dto)
        if dto['status'] != 'AWAITING_CONFIRMATION' or dto['missing_fields']:
            raise WorkflowError('incident_not_ready', 409)
        data = draft.arguments_json
        asset, category = _asset(actor, data, lock=True), _category(data, lock=True)
        # A fresh snapshot is required if another incident appeared before confirmation.
        existing_ids = list(ReporteFalla.objects.filter(activo_relacionado=asset, estatus__in=['abierto','en_revision','en_proceso'], duplicado_de__isnull=True).values_list('pk', flat=True)[:20])
        if existing_ids:
            raise WorkflowError('incident_existing_report', 409, 'El equipo ya tiene una falla abierta. Revisa el reporte existente.')
        if _hash(data, asset.sucursal_id) != metadata['payload_hash']:
            raise WorkflowError('incident_resource_changed', 409)
        report = crear_reporte_falla(sucursal=asset.sucursal, usuario=actor, categoria=category, tipo_objetivo='EQUIPO', activo_relacionado=asset,
                    titulo=data['titulo'], descripcion=data['descripcion'], prioridad=data['prioridad'], justificacion_sin_foto=data['justificacion_sin_foto'],
                    comentario_bitacora='Reporte creado mediante el agente ERP, confirmado por el colaborador.')
        draft.metadata_json = {**metadata, 'incident_status':'EXECUTED', 'report_id':report.pk, 'confirmed_by':actor.pk, 'confirmed_at':timezone.now().isoformat()}
        draft.save(update_fields=['metadata_json', 'updated_at'])
        payload = _response(project(draft, actor))
        ChatToolResult.objects.update_or_create(tool_call=draft, defaults={'summary':'EXECUTED', 'result_json':payload})
        AuditLog.objects.create(user=actor, action='AI_INCIDENT_CREATE', model='fallas.ReporteFalla', object_id=str(report.pk),
                                payload={'draft_id':str(draft.public_id), 'conversation_id':str(draft.conversation.public_id), 'arguments':data,
                                         'version':metadata['version'], 'payload_hash':metadata['payload_hash'], 'confirmation':True})
        return payload
