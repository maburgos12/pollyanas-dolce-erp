"""Owned, confirmed follow-up of an existing failure; never a payment command."""
from datetime import timedelta
import hashlib
import json
from uuid import uuid4, uuid5

from django.core.files.storage import default_storage
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone
from rest_framework import serializers

from api.ai_gateway_assets import StrictArguments, StrictDateField, StrictIntegerField, StrictStringField, json_dto
from core.models import AuditLog
from fallas.models import ReporteFalla, BitacoraFalla, EvidenciaSeguimientoFalla
from fallas.views import _puede_cambiar_estatus_fallas
from mantenimiento.evidence_validation import validate_evidence_files
from mantenimiento.services_access import authorized_fallas, can_view_costs
from orquestacion.models import ChatConversation, ChatMessage, ChatToolCall, ChatToolResult
from orquestacion.services.agent_workflows import WorkflowError

KEY = 'incident.followup'


class ReportSearch(StrictArguments):
    q = StrictStringField(required=False, max_length=160)
    sucursal_id = StrictIntegerField(required=False, min_value=1)


class ReportDetail(StrictArguments):
    report_id = StrictIntegerField(min_value=1)


class FollowupArguments(StrictArguments):
    draft_id = serializers.UUIDField(required=False)
    expected_version = StrictIntegerField(required=False, min_value=1)
    report_id = StrictIntegerField(required=False, min_value=1)
    comentario = StrictStringField(required=False, allow_blank=True, max_length=3000)
    fecha_trabajo_finalizado = StrictDateField(required=False)
    evidence_count = StrictIntegerField(required=False, min_value=0, max_value=5)
    estatus = serializers.ChoiceField(required=False, choices=['en_proceso', 'resuelto'])

    def validate(self, attrs):
        if ('draft_id' in attrs) != ('expected_version' in attrs):
            raise serializers.ValidationError('La continuación requiere referencia y versión.')
        return attrs


SERIALIZERS = {'incident.search_reports': ReportSearch, 'incident.get_report': ReportDetail, KEY: FollowupArguments}


def actor_for(user, conversation):
    from .agent_incidents import _actor
    actor = _actor(user, conversation)
    if not _puede_cambiar_estatus_fallas(actor) or not can_view_costs(actor):
        raise WorkflowError('followup_access_denied', 403)
    return actor


def catalog(actor):
    if not _puede_cambiar_estatus_fallas(actor) or not can_view_costs(actor):
        return []
    definitions = [
        ('incident.search_reports', 'erp_search_failure_reports', 'Buscar reportes de falla', 'Busca reportes existentes por fragmento de título, proveedor o sucursal. Incluye instalaciones sin equipo; no crea reportes.'),
        ('incident.get_report', 'erp_get_failure_report', 'Consultar reporte de falla', 'Obtiene una ficha fresca, cotización, proveedor y seguimiento del reporte autorizado.'),
        (KEY, 'erp_prepare_failure_followup', 'Preparar actualización del reporte', 'Prepara seguimiento o finalización de un reporte existente sin ejecutarla. Conserva cotización, proveedor y costo real. Requiere comentario y, para finalizar, fecha real del trabajo. Las fotos se adjuntan en la interfaz; continúa el mismo draft_id y expected_version.'),
    ]
    return [{'key': key, 'name': name, 'display_name': label, 'description': description,
             'argument_schema': SERIALIZERS[key].argument_schema(), 'incident': True} for key, name, label, description in definitions]


def reports(actor):
    if not _puede_cambiar_estatus_fallas(actor):
        raise WorkflowError('followup_access_denied', 403)
    return authorized_fallas(actor).select_related('sucursal')


def get_report(actor, pk, *, lock=False):
    rows = reports(actor)
    if lock:
        rows = rows.select_for_update(of=('self',))
    report = rows.filter(pk=pk).first()
    if not report:
        raise WorkflowError('followup_resource_unavailable', 403)
    return report


def report_dto(report, actor):
    data = {'id': report.pk, 'titulo': report.titulo, 'sucursal': report.sucursal.nombre,
            'estatus': report.estatus, 'estatus_label': report.get_estatus_display(), 'proveedor': report.proveedor_servicio,
            'fecha_trabajo_finalizado': str(report.fecha_trabajo_finalizado) if report.fecha_trabajo_finalizado else None}
    if can_view_costs(actor):
        data.update(costo_estimado=str(report.costo_estimado) if report.costo_estimado is not None else None,
                    costo_real=str(report.costo_real) if report.costo_real is not None else None)
    return data


def snapshot(report):
    return json_dto({'id': report.pk, 'sucursal_id': report.sucursal_id, 'titulo': report.titulo,
        'descripcion': report.descripcion, 'estatus': report.estatus, 'proveedor': report.proveedor_servicio,
        'costo_estimado': report.costo_estimado, 'costo_real': report.costo_real,
        'fecha_trabajo_finalizado': report.fecha_trabajo_finalizado, 'fecha_resolucion': report.fecha_resolucion,
        'duplicado_de': report.duplicado_de_id,
        'bitacora_ids': list(report.bitacora.order_by('pk').values_list('pk', flat=True)),
        'duplicados': list(ReporteFalla.objects.filter(duplicado_de=report).order_by('pk').values('id', 'estatus'))})


def digest(data):
    return hashlib.sha256(json.dumps(json_dto(data), sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def validate_fields(data, report):
    missing = [key for key in ('report_id', 'comentario') if not data.get(key)]
    if data.get('estatus') == 'resuelto' and not data.get('fecha_trabajo_finalizado'):
        missing.append('fecha_trabajo_finalizado')
    if data.get('fecha_trabajo_finalizado'):
        day = timezone.datetime.strptime(data['fecha_trabajo_finalizado'], '%Y-%m-%d').date()
        if day > timezone.localdate() or (report and day < timezone.localtime(report.fecha_reporte).date()):
            raise WorkflowError('followup_invalid_date', 400, 'La fecha debe estar entre el reporte y hoy.')
        if data.get('estatus') != 'resuelto':
            raise WorkflowError('followup_invalid_date', 400, 'La fecha real corresponde a la finalización del trabajo.')
    if report and (report.estatus not in ('abierto', 'en_revision', 'en_proceso') or report.duplicado_de_id
                   or ReporteFalla.objects.filter(duplicado_de=report).exists()):
        raise WorkflowError('followup_requires_review', 409, 'Revisa el reporte finalizado o sus relaciones antes de actualizarlo.')
    return missing


def project(draft, actor):
    actor = actor_for(actor, draft.conversation)
    metadata, data = draft.metadata_json, draft.arguments_json
    report = get_report(actor, data['report_id']) if data.get('report_id') else None
    state = metadata['incident_status']
    display_report = report_dto(report, actor) if report else None
    if state == 'EXECUTED':
        if not report or not BitacoraFalla.objects.filter(pk=metadata['bitacora_id'], reporte=report, usuario_id=metadata['confirmed_by']).exists():
            raise WorkflowError('followup_receipt_unavailable', 409)
        confirmation = AuditLog.objects.filter(user_id=metadata['confirmed_by'], action='AI_FOLLOWUP_UPDATE',
            model='fallas.ReporteFalla', object_id=str(report.pk), payload__draft_id=str(draft.public_id),
            payload__payload_hash=metadata['payload_hash']).values_list('payload', flat=True).first()
        paths = [row['path'] for row in metadata.get('files', [])]
        if not confirmation or EvidenciaSeguimientoFalla.objects.filter(bitacora_id=metadata['bitacora_id'], archivo__in=paths).count() != len(paths):
            raise WorkflowError('followup_receipt_unavailable', 409)
        # A receipt describes the confirmed intervention, even if the report changes later.
        display_report.update({key: confirmation['before'][key] for key in ('titulo', 'costo_estimado', 'costo_real')})
        display_report['proveedor'] = confirmation['before']['proveedor']
    elif timezone.now() >= timezone.datetime.fromisoformat(metadata['expires_at']):
        state = 'EXPIRED'
    elif report and digest(snapshot(report)) != metadata['resource_hash']:
        state = 'REVIEW_REQUIRED'
    missing = list(metadata['missing_fields'])
    if state != 'EXECUTED' and len(metadata.get('files', [])) < metadata.get('expected_file_count', 0):
        missing.append('fotografías')
    files = [{'id': row['id'], 'nombre': row['nombre'], 'bytes': row['bytes'], 'sha256': row['sha256'],
              'url': reverse('api_ai_followup_evidence', args=[draft.public_id, row['id']])} for row in metadata.get('files', [])]
    return {'kind': 'followup', 'draft_id': str(draft.public_id), 'version': metadata['version'], 'status': state,
            'payload_hash': metadata['payload_hash'], 'fields': data, 'report': display_report,
            'missing_fields': missing, 'files': files, 'report_id': data.get('report_id'),
            'asset': None, 'confirmed_at': metadata.get('confirmed_at'), 'expires_at': metadata['expires_at']}


def response(dto):
    return {'result': {'status': dto['status'], 'sources': ['fallas.ReporteFalla', 'fallas.BitacoraFalla'],
                      'as_of': timezone.now().isoformat(), 'payload': {'incident': dto}}}


def save_result(draft, actor, action):
    payload = response(project(draft, actor))
    ChatToolResult.objects.update_or_create(tool_call=draft, defaults={'summary': payload['result']['status'], 'result_json': payload})
    AuditLog.objects.create(user=actor, action=action, model='orquestacion.ChatToolCall', object_id=str(draft.public_id),
        payload={'conversation_id': str(draft.conversation.public_id), 'version': draft.metadata_json['version'],
                 'payload_hash': draft.metadata_json['payload_hash'], 'arguments': draft.arguments_json})
    return payload


def payload_hash(data, metadata):
    return digest({'fields': data, 'resource_hash': metadata['resource_hash'],
                   'expected_file_count': metadata.get('expected_file_count', 0),
                   'files': [{k: row[k] for k in ('id', 'nombre', 'bytes', 'sha256')} for row in metadata.get('files', [])]})


def invoke(*, user, tool_key, arguments, conversation, user_message, assistant_message, call_id):
    actor = actor_for(user, conversation)
    serializer = SERIALIZERS[tool_key](data=arguments)
    serializer.is_valid(raise_exception=True)
    args = json_dto(serializer.validated_data)
    if tool_key != KEY:
        if tool_key == 'incident.get_report':
            report = get_report(actor, args['report_id'])
            dto = report_dto(report, actor)
            dto['descripcion'] = report.descripcion
            dto['bitacora'] = list(report.bitacora.order_by('-pk').values('comentario', 'timestamp')[:10])
            rows = [dto]
        else:
            qs = reports(actor)
            if args.get('q'):
                qs = qs.filter(Q(titulo__icontains=args['q']) | Q(proveedor_servicio__icontains=args['q']) | Q(sucursal__nombre__icontains=args['q']))
            if args.get('sucursal_id'):
                qs = qs.filter(sucursal_id=args['sucursal_id'])
            rows = [report_dto(r, actor) for r in qs.order_by('-fecha_reporte', '-pk')[:21]]
        payload = {'result': {'status': 'ok', 'sources': ['fallas.ReporteFalla'], 'as_of': timezone.now().isoformat(),
                    'payload': {'reports': json_dto(rows[:20]), 'truncated': len(rows) > 20}}}
        AuditLog.objects.create(user=actor, action='AI_FOLLOWUP_READ', model='fallas.ReporteFalla', object_id='', payload={'arguments': args, 'report_ids': [r['id'] for r in rows[:20]]})
        return payload
    if not ChatMessage.objects.filter(pk=user_message.pk, conversation=conversation, created_by=actor, role='user', status='complete').exists():
        raise WorkflowError('invalid_incident_origin', 403)
    origin = str(uuid5(user_message.public_id, call_id))
    changes = {k: v for k, v in args.items() if k not in ('draft_id', 'expected_version')}
    command_hash = digest(changes)
    with transaction.atomic():
        ChatConversation.objects.select_for_update().get(pk=conversation.pk)
        draft = ChatToolCall.objects.select_for_update().select_related('conversation').filter(public_id=args.get('draft_id', origin), conversation=conversation, tool_key=KEY).first()
        if args.get('draft_id') and not draft:
            raise WorkflowError('incident_not_found', 404)
        if draft and draft.metadata_json['last_request_id'] == origin:
            if draft.metadata_json['last_request_hash'] != command_hash:
                raise WorkflowError('request_payload_mismatch', 409)
            return response(project(draft, actor))
        if draft and (draft.metadata_json['version'] != args.get('expected_version') or project(draft, actor)['status'] not in ('WAITING_INFORMATION', 'AWAITING_CONFIRMATION')):
            raise WorkflowError('incident_version_conflict', 409)
        data = {**(draft.arguments_json if draft else {'estatus': 'en_proceso'}), **changes}
        if draft and draft.metadata_json.get('files') and data.get('report_id') != draft.arguments_json.get('report_id'):
            raise WorkflowError('followup_evidence_binding', 409)
        report = get_report(actor, data['report_id']) if data.get('report_id') else None
        missing = validate_fields(data, report)
        metadata = {**(draft.metadata_json if draft else {}), 'version': draft.metadata_json['version'] + 1 if draft else 1,
            'incident_status': 'WAITING_INFORMATION' if missing else 'AWAITING_CONFIRMATION', 'missing_fields': missing,
            'resource_hash': digest(snapshot(report)) if report else None,
            'expected_file_count': max((draft.metadata_json.get('expected_file_count', 0) if draft else 0), user_message.metadata_json.get('expected_photo_count', 0), data.get('evidence_count', 0)),
            'last_request_id': origin, 'last_request_hash': command_hash,
            'expires_at': (timezone.now() + timedelta(hours=24)).isoformat()}
        metadata['payload_hash'] = payload_hash(data, metadata)
        if draft:
            draft.arguments_json, draft.metadata_json, draft.assistant_message = data, metadata, assistant_message
            draft.save(update_fields=['arguments_json', 'metadata_json', 'assistant_message', 'updated_at'])
        else:
            draft = ChatToolCall.objects.create(public_id=origin, conversation=conversation, request_message=user_message,
                assistant_message=assistant_message, tool_key=KEY, tool_name='erp_prepare_failure_followup',
                tool_display_name='Actualizar reporte existente', arguments_json=data, metadata_json=metadata, status='complete')
        return save_result(draft, actor, 'AI_FOLLOWUP_PREPARE')


def owned_draft(user, pk, *, lock=False):
    qs = ChatToolCall.objects.select_related('conversation').filter(public_id=pk, conversation__owner=user, tool_key=KEY)
    if lock:
        qs = qs.select_for_update(of=('self',))
    draft = qs.first()
    if not draft:
        raise WorkflowError('incident_not_found', 404)
    return draft, actor_for(user, draft.conversation)


def attach(*, user, draft_id, arguments, files):
    from .agent_incidents import IncidentConfirmation
    serializer = IncidentConfirmation(data=arguments)
    serializer.is_valid(raise_exception=True)
    files = validate_evidence_files(files, images_only=True)
    if sum(file.size for file in files) > 20 * 1024 * 1024:
        raise WorkflowError('followup_files_too_large', 400, 'El conjunto de fotografías no puede exceder 20 MB.')
    if not files:
        raise WorkflowError('followup_no_files', 400)
    uploads = []
    for file in files:
        from PIL import Image
        try:
            with Image.open(file) as image:
                if image.width * image.height > 25000000:
                    raise ValueError('image limit')
                image.verify()
        except Exception:
            raise WorkflowError('followup_invalid_image', 400, 'La fotografía no pudo validarse.') from None
        finally:
            file.seek(0)
        data = file.read(); file.seek(0)
        uploads.append({'nombre': file.name, 'bytes': file.size, 'sha256': hashlib.sha256(data).hexdigest(), 'mime': file.content_type})
    if len({f['sha256'] for f in uploads}) != len(uploads):
        raise WorkflowError('followup_duplicate_files', 400)
    upload_hash = digest(uploads)
    saved = []
    try:
        with transaction.atomic():
            # Serialize uploads for the owner so concurrent drafts cannot bypass the staging quota.
            get_user_model().objects.select_for_update().get(pk=user.pk)
            draft, actor = owned_draft(user, draft_id, lock=True)
            dto = project(draft, actor)
            meta = draft.metadata_json
            if (meta.get('upload_hash') == upload_hash and meta.get('upload_version') == arguments['expected_version']
                    and meta.get('upload_payload_hash') == arguments['payload_hash'] and dto['status'] == 'AWAITING_CONFIRMATION'):
                return response(dto)
            if dto['status'] != 'AWAITING_CONFIRMATION' or dto['version'] != arguments['expected_version'] or dto['payload_hash'] != arguments['payload_hash']:
                raise WorkflowError('incident_version_conflict', 409)
            if meta.get('files'):
                raise WorkflowError('followup_files_already_bound', 409, 'Estas evidencias ya están asociadas; revisa la propuesta.')
            if len(files) < meta.get('expected_file_count', 0):
                raise WorkflowError('followup_files_missing', 400, 'Selecciona todas las fotografías indicadas en la propuesta en una sola carga.')
            pending = list(ChatToolCall.objects.filter(conversation__owner=actor, tool_key=KEY)
                .exclude(metadata_json__incident_status='EXECUTED').values_list('metadata_json', flat=True)[:101])
            staged_bytes = sum(row['bytes'] for metadata in pending for row in metadata.get('files', []))
            if len(pending) > 100 or staged_bytes + sum(row['bytes'] for row in uploads) > 100 * 1024 * 1024:
                raise WorkflowError('followup_staging_limit', 409, 'El espacio de propuestas pendientes está lleno. Revisa las evidencias existentes antes de adjuntar más.')
            for file, info in zip(files, uploads):
                uid = uuid4()
                name = default_storage.save(f'fallas/seguimiento/agente/{draft.public_id.hex}/{uid.hex}{file.name[file.name.rfind("."):].lower()}', file)
                saved.append(name)
                info.update(id=str(uid), path=name)
            draft.metadata_json = {**meta, 'files': uploads, 'upload_hash': upload_hash,
                'upload_version': arguments['expected_version'], 'upload_payload_hash': arguments['payload_hash'], 'version': meta['version'] + 1}
            draft.metadata_json['payload_hash'] = payload_hash(draft.arguments_json, draft.metadata_json)
            draft.save(update_fields=['metadata_json', 'updated_at'])
            return save_result(draft, actor, 'AI_FOLLOWUP_ATTACH')
    except Exception:
        for name in saved:
            default_storage.delete(name)
        raise


def confirm(*, user, draft_id, arguments):
    from .agent_incidents import IncidentConfirmation
    serializer = IncidentConfirmation(data=arguments)
    serializer.is_valid(raise_exception=True)
    with transaction.atomic():
        draft, actor = owned_draft(user, draft_id, lock=True)
        dto, meta = project(draft, actor), draft.metadata_json
        if dto['version'] != arguments['expected_version'] or dto['payload_hash'] != arguments['payload_hash']:
            raise WorkflowError('incident_version_conflict', 409)
        if dto['status'] == 'EXECUTED':
            return response(dto)
        if dto['status'] != 'AWAITING_CONFIRMATION' or dto['missing_fields']:
            raise WorkflowError('incident_not_ready', 409)
        report = get_report(actor, draft.arguments_json['report_id'], lock=True)
        before = snapshot(report)
        if digest(before) != meta['resource_hash']:
            raise WorkflowError('incident_resource_changed', 409)
        data = draft.arguments_json
        validate_fields(data, report)
        for info in meta.get('files', []):
            with default_storage.open(info['path'], 'rb') as source:
                blob = source.read(10 * 1024 * 1024 + 1)
            if len(blob) != info['bytes'] or hashlib.sha256(blob).hexdigest() != info['sha256']:
                raise WorkflowError('followup_evidence_changed', 409)
        report.estatus = data['estatus']
        fields = ['estatus']
        if data['estatus'] == 'resuelto':
            report.fecha_trabajo_finalizado = data['fecha_trabajo_finalizado']
            report.fecha_resolucion = timezone.now()
            fields += ['fecha_trabajo_finalizado', 'fecha_resolucion']
        report.save(update_fields=fields)
        log = BitacoraFalla.objects.create(reporte=report, usuario=actor,
            estatus_anterior=before['estatus'], estatus_nuevo=report.estatus,
            comentario=data['comentario'])
        for info in meta.get('files', []):
            EvidenciaSeguimientoFalla.objects.create(bitacora=log, archivo=info['path'], nombre=info['nombre'], subido_por=actor)
        draft.metadata_json = {**meta, 'incident_status': 'EXECUTED', 'bitacora_id': log.pk,
                              'confirmed_by': actor.pk, 'confirmed_at': timezone.now().isoformat()}
        draft.save(update_fields=['metadata_json', 'updated_at'])
        AuditLog.objects.create(user=actor, action='AI_FOLLOWUP_UPDATE', model='fallas.ReporteFalla', object_id=str(report.pk),
            payload={'draft_id': str(draft.public_id), 'conversation_id': str(draft.conversation.public_id),
                     'before': before, 'after': snapshot(report), 'confirmation': True, 'payload_hash': meta['payload_hash'],
                     'evidences': [{k: v[k] for k in ('id', 'sha256', 'bytes')} for v in meta.get('files', [])]})
        return save_result(draft, actor, 'AI_FOLLOWUP_CONFIRMED')
