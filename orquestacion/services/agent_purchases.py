"""Compras departamentales: leer y preparar; escribir sólo con confirmación humana."""
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
from uuid import uuid4, uuid5

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError as ModelValidationError, PermissionDenied
from django.core.files.storage import default_storage
from django.db import transaction
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone
from rest_framework import serializers

from api.ai_gateway_assets import StrictArguments, StrictDateField, StrictIntegerField, StrictStringField, fresh_asset_user, can_read_assets, json_dto
from compras.access_departamentales import _areas_usuario, _areas_lectura_solicitudes, _es_direccion, _puede_ver_solicitud, puede_gestionar_compras_departamentales
from compras.models import SolicitudCompraDepartamental, EventoCompraDepartamental
from compras.services_solicitudes import crear_solicitud
from core.models import AuditLog
from mantenimiento.evidence_validation import validate_evidence_files
from orquestacion.models import ChatConversation, ChatMessage, ChatToolCall, ChatToolResult
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable
from .agent_incidents import IncidentConfirmation
from .agent_pilot import is_pilot_participant
from .agent_workflows import WorkflowError

KEY = 'purchase.prepare'


class ExactDecimal(serializers.DecimalField):
    def to_internal_value(self, data):
        if not isinstance(data, str):
            self.fail('invalid')
        return super().to_internal_value(data)


class ItemArguments(StrictArguments):
    descripcion = StrictStringField(max_length=300)
    cantidad = ExactDecimal(max_digits=12, decimal_places=3, min_value=Decimal('0.001'))
    unidad = StrictStringField(max_length=40)
    precio_total = ExactDecimal(max_digits=14, decimal_places=2, min_value=Decimal('0'), required=False, allow_null=True)
    vendedor_reportado = StrictStringField(max_length=160, required=False, allow_blank=True)


class SearchArguments(StrictArguments):
    q = StrictStringField(required=False, max_length=160, allow_blank=True)
    area_id = StrictIntegerField(required=False, min_value=1)
    solicitante_id = StrictIntegerField(required=False, min_value=1)


class DetailArguments(StrictArguments):
    request_id = StrictIntegerField(min_value=1)


class PrepareArguments(StrictArguments):
    draft_id = serializers.UUIDField(required=False)
    expected_version = StrictIntegerField(required=False, min_value=1)
    area_id = StrictIntegerField(required=False, min_value=1)
    solicitante_id = StrictIntegerField(required=False, min_value=1)
    motivo = StrictStringField(required=False, allow_blank=True, max_length=2000)
    justificacion_extraordinaria = StrictStringField(required=False, allow_blank=True, max_length=2000)
    items = ItemArguments(many=True, required=False, min_length=1, max_length=20)
    compra_reportada = serializers.BooleanField(required=False)
    plataforma_reportada = StrictStringField(required=False, max_length=100, allow_blank=True)
    envio_global = ExactDecimal(max_digits=14, decimal_places=2, min_value=Decimal('0'), required=False, allow_null=True)
    total_reportado = ExactDecimal(max_digits=14, decimal_places=2, min_value=Decimal('0.01'), required=False, allow_null=True)
    fecha_compra_reportada = StrictDateField(required=False)
    entrega_estimada = StrictDateField(required=False)

    def validate(self, attrs):
        if ('draft_id' in attrs) != ('expected_version' in attrs):
            raise serializers.ValidationError('La continuación requiere referencia y versión.')
        return attrs


SERIALIZERS = {'purchase.requirements': SearchArguments, 'purchase.search': SearchArguments,
               'purchase.get': DetailArguments, KEY: PrepareArguments}


def enabled(user):
    actor = fresh_asset_user(user)
    return bool(actor and getattr(settings, 'AI_AGENT_READ_ENABLED', False) is True
        and getattr(settings, 'AI_AGENT_INCIDENTS_ENABLED', False) is True and can_read_assets(actor)
        and is_pilot_participant(actor)
        and (puede_gestionar_compras_departamentales(actor) or _areas_usuario(actor).exists()))


def actor_for(user, conversation):
    actor = fresh_asset_user(user)
    if not enabled(actor) or not ChatConversation.objects.filter(pk=conversation.pk, owner=actor, status='active').exists():
        raise WorkflowError('purchase_access_denied', 403)
    return actor


def access_fingerprint(user):
    actor = fresh_asset_user(user)
    return digest({'active':bool(actor), 'participant':is_pilot_participant(actor),
        'manage':puede_gestionar_compras_departamentales(actor), 'dg':_es_direccion(actor) if actor else False,
        'areas':list(AreaPresupuestoResponsable.objects.filter(usuario=actor).order_by('pk').values('area_id','puede_capturar','area__activa'))})


def catalog(user):
    if not enabled(user):
        return []
    definitions = [
        ('purchase.requirements', 'erp_purchase_requirements', 'Requisitos de compra especial', 'Obtiene áreas autorizadas y solicitantes activos para una compra extraordinaria. q busca nombre de la persona. Presupuesto y comprobante no son obligatorios; sólo DG puede representar a otra persona de su área.'),
        ('purchase.search', 'erp_search_departmental_purchases', 'Buscar solicitudes de compra', 'Busca solicitudes existentes por artículo, motivo o folio, área y solicitante. Son compras departamentales, no solicitudes de insumos; respeta el alcance del usuario.'),
        ('purchase.get', 'erp_get_departmental_purchase', 'Consultar solicitud de compra', 'Lee la solicitud y sus artículos actuales. Comprar, reportar compra y recibir son estados distintos.'),
        (KEY, 'erp_prepare_special_purchase', 'Preparar solicitud de compra especial', 'Prepara una solicitud extraordinaria sin escribirla. Continúa el mismo draft_id y expected_version. items.precio_total es el precio conjunto de esa cantidad; el servidor calcula el unitario exacto. Si ya compraron, conserva compra_reportada, vendedor y total como información reportada pendiente de regularización; nunca autoriza cotizaciones, pagos ni recepción. envio_global no se reparte por proveedor. Captura opcional en la interfaz; no la has leído. No inventes presupuesto, vendedor ni fecha. El mes de planeación es el actual.'),
    ]
    return [{'key': k, 'name': n, 'display_name': label, 'description': desc,
             'argument_schema': SERIALIZERS[k].argument_schema(), 'purchase': True} for k,n,label,desc in definitions]


def requests(actor):
    areas = _areas_lectura_solicitudes(actor)
    qs = SolicitudCompraDepartamental.objects.select_related('area', 'solicitante').prefetch_related('items')
    return qs if areas is None else qs.filter(area_id__in=areas)


def request_dto(row):
    items = sorted(row.items.all(), key=lambda item: item.pk)[:21]
    return {'id': row.pk, 'folio': row.folio, 'area': row.area.nombre,
            'solicitante': row.solicitante.get_full_name() or row.solicitante.username,
            'estado': row.get_estado_display(), 'motivo': row.motivo, 'periodo': row.periodo.isoformat(),
            'items': [{'id': i.pk, 'descripcion': i.descripcion, 'cantidad': str(i.cantidad), 'unidad': i.unidad,
                       'costo_unitario_estimado': str(i.costo_unitario_estimado) if i.costo_unitario_estimado is not None else None,
                       'estado': i.get_estado_display()} for i in items[:20]], 'truncated_items': len(items)>20}


def digest(data):
    return hashlib.sha256(json.dumps(json_dto(data), sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def identities(actor, data):
    area = AreaPresupuesto.objects.filter(pk=data.get('area_id'), activa=True).first() if data.get('area_id') else None
    if data.get('area_id') and (not area or not (puede_gestionar_compras_departamentales(actor) or _areas_usuario(actor).filter(pk=area.pk).exists())):
        raise WorkflowError('purchase_area_unavailable', 403)
    requester = get_user_model().objects.filter(pk=data.get('solicitante_id'), is_active=True).first() if data.get('solicitante_id') else None
    if data.get('solicitante_id') and (not requester or (requester.pk != actor.pk and not _es_direccion(actor))
            or (area and requester.pk != actor.pk and not _areas_usuario(requester).filter(pk=area.pk).exists())):
        raise WorkflowError('purchase_requester_unavailable', 403)
    return area, requester


def derived_items(data):
    rows = []
    for item in data.get('items', []):
        quantity = Decimal(item['cantidad'])
        total = Decimal(item['precio_total']) if item.get('precio_total') is not None else None
        unit = (total / quantity).quantize(Decimal('0.01')) if total is not None else None
        if unit is not None and unit * quantity != total:
            raise WorkflowError('purchase_price_precision', 400, 'El total no admite un precio unitario exacto a centavos. Revisa cantidad e importe.')
        rows.append({**item, 'costo_unitario_estimado': str(unit) if unit is not None else None})
    return rows


def missing_fields(data):
    rows = derived_items(data)
    missing = [k for k in ('area_id', 'solicitante_id', 'motivo', 'justificacion_extraordinaria', 'items') if not data.get(k)]
    if data.get('fecha_compra_reportada') and date.fromisoformat(data['fecha_compra_reportada']) > timezone.localdate():
        raise WorkflowError('purchase_invalid_date', 400)
    if (rows and all(i.get('precio_total') is not None for i in rows)
            and data.get('envio_global') is not None and data.get('total_reportado') is not None):
        total = sum(Decimal(i['precio_total']) for i in rows) + Decimal(data['envio_global'])
        if total != Decimal(data['total_reportado']):
            raise WorkflowError('purchase_total_mismatch', 400, 'Productos más envío no coinciden con el total reportado.')
    return missing


def signature(items):
    return sorted((i['descripcion'].strip().casefold(), str(Decimal(i['cantidad']).normalize()), i['unidad'].strip().casefold()) for i in items)


def duplicates(actor, data):
    if not data.get('area_id') or not data.get('solicitante_id') or not data.get('items'):
        return []
    candidates = list(requests(actor).filter(area_id=data['area_id'], solicitante_id=data['solicitante_id'], periodo=data['periodo'])
        .exclude(estado__in=['CANCELADA','COMPLETADA']).order_by('-pk')[:51])
    if len(candidates) > 50:
        raise WorkflowError('purchase_duplicate_search_limit', 409, 'Hay demasiadas solicitudes activas; revisa la bandeja antes de crear otra.')
    return [{'id': r.pk, 'folio': r.folio} for r in candidates
            if signature([{'descripcion':i.descripcion,'cantidad':i.cantidad,'unidad':i.unidad} for i in r.items.all()]) == signature(data['items'])]


def project(draft, user):
    actor = actor_for(user, draft.conversation)
    data, meta = draft.arguments_json, draft.metadata_json
    status = meta['purchase_status']
    row = None
    if status == 'EXECUTED':
        row = requests(actor).filter(pk=meta['request_id']).first()
        if not row or not AuditLog.objects.filter(user_id=meta['confirmed_by'], action='AI_PURCHASE_REQUEST_CREATE',
                model='compras.SolicitudCompraDepartamental', object_id=str(row.pk), payload__draft_id=str(draft.public_id),
                payload__payload_hash=meta['payload_hash']).exists():
            raise WorkflowError('purchase_receipt_unavailable', 409)
        area, requester = row.area, row.solicitante
        matches = []
    else:
        area, requester = identities(actor, data)
        matches = duplicates(actor, data)
        if timezone.now() >= timezone.datetime.fromisoformat(meta['expires_at']):
            status = 'EXPIRED'
        elif matches:
            status = 'REVIEW_REQUIRED'
    return {'kind':'purchase', 'draft_id':str(draft.public_id), 'version':meta['version'], 'status':status,
            'payload_hash':meta['payload_hash'], 'fields':data, 'items':derived_items(data),
            'area':area.nombre if area else None, 'solicitante':(requester.get_full_name() or requester.username) if requester else None,
            'missing_fields':missing_fields(data), 'existing_requests':matches,
            'request_id':row.pk if row else None, 'folio':meta.get('folio'),
            'url':reverse('compras:departamental_detalle', args=[row.pk]) if row else None,
            'confirmed_at':meta.get('confirmed_at'), 'expires_at':meta['expires_at'],
            'files':[{'id':f['id'], 'nombre':f['nombre'], 'bytes':f['bytes'], 'sha256':f['sha256'],
                      'url':reverse('api_ai_purchase_evidence',args=[draft.public_id,f['id']])} for f in meta.get('files',[])]}


def response(dto):
    return {'result':{'status':dto['status'], 'sources':['compras.SolicitudCompraDepartamental','orquestacion.ChatToolCall'],
                      'as_of':timezone.now().isoformat(), 'payload':{'incident':dto}}}


def save_result(draft, actor, action):
    value = response(project(draft, actor))
    ChatToolResult.objects.update_or_create(tool_call=draft, defaults={'summary':value['result']['status'], 'result_json':value})
    AuditLog.objects.create(user=actor, action=action, model='orquestacion.ChatToolCall', object_id=str(draft.public_id),
        payload={'conversation_id':str(draft.conversation.public_id), 'version':draft.metadata_json['version'],
                 'payload_hash':draft.metadata_json['payload_hash'], 'arguments':draft.arguments_json})
    return value


def invoke(*, user, tool_key, arguments, conversation, user_message, assistant_message, call_id):
    actor = actor_for(user, conversation)
    serializer = SERIALIZERS[tool_key](data=arguments); serializer.is_valid(raise_exception=True)
    args = json_dto(serializer.validated_data)
    if tool_key != KEY:
        if tool_key == 'purchase.requirements':
            areas = AreaPresupuesto.objects.filter(activa=True) if puede_gestionar_compras_departamentales(actor) else _areas_usuario(actor)
            people = get_user_model().objects.filter(is_active=True)
            if _es_direccion(actor):
                people = people.filter(areas_presupuesto__area__in=areas, areas_presupuesto__puede_capturar=True)
            else:
                people = people.filter(pk=actor.pk)
            if args.get('q'):
                people = people.filter(Q(first_name__icontains=args['q'])|Q(last_name__icontains=args['q'])|Q(username__icontains=args['q']))
            people = list(people.distinct().order_by('pk')[:21])
            payload = {'areas':list(areas.order_by('pk').values('id','nombre','codigo')[:50]),
                       'solicitantes':[{'id':u.pk,'nombre':u.get_full_name() or u.username} for u in people[:20]],
                       'truncated':len(people)>20, 'required_fields':['area_id','solicitante_id','motivo','justificacion_extraordinaria','items'],
                       'periodo':timezone.localdate().replace(day=1).isoformat(), 'presupuesto_requerido':False, 'evidencia_requerida':False}
        else:
            qs = requests(actor)
            if tool_key == 'purchase.get':
                qs = qs.filter(pk=args['request_id'])
                if not qs.exists():
                    raise WorkflowError('purchase_resource_unavailable', 403)
            else:
                if args.get('q'):
                    qs = qs.filter(Q(folio__icontains=args['q'])|Q(motivo__icontains=args['q'])|Q(items__descripcion__icontains=args['q'])).distinct()
                for field in ('area_id','solicitante_id'):
                    if args.get(field): qs = qs.filter(**{field:args[field]})
            rows = list(qs.order_by('-pk')[:21])
            payload = {'requests':[request_dto(r) for r in rows[:20]], 'truncated':len(rows)>20}
        AuditLog.objects.create(user=actor, action='AI_PURCHASE_READ', model='compras.SolicitudCompraDepartamental', object_id='', payload={'arguments':args})
        return {'result':{'status':'ok','sources':['compras.SolicitudCompraDepartamental','reportes.AreaPresupuestoResponsable'], 'as_of':timezone.now().isoformat(),'payload':payload}}
    if (not ChatMessage.objects.filter(pk=user_message.pk, conversation=conversation, created_by=actor, role='user', status='complete').exists()
            or not ChatMessage.objects.filter(pk=assistant_message.pk, conversation=conversation, role='assistant', created_by__isnull=True, sequence=user_message.sequence+1).exists()):
        raise WorkflowError('purchase_invalid_origin', 403)
    origin = str(uuid5(user_message.public_id,call_id))
    changes = {k:v for k,v in args.items() if k not in ('draft_id','expected_version')}
    with transaction.atomic():
        ChatConversation.objects.select_for_update().get(pk=conversation.pk)
        draft = ChatToolCall.objects.select_for_update().filter(public_id=args.get('draft_id',origin), conversation=conversation,tool_key=KEY).first()
        if args.get('draft_id') and not draft: raise WorkflowError('purchase_not_found',404)
        if draft and draft.metadata_json['last_request_id']==origin:
            if draft.metadata_json['last_request_hash']!=digest(changes): raise WorkflowError('request_payload_mismatch',409)
            return response(project(draft,actor))
        if draft and (draft.metadata_json['version']!=args.get('expected_version') or project(draft,actor)['status'] not in ('WAITING_INFORMATION','AWAITING_CONFIRMATION')):
            raise WorkflowError('purchase_version_conflict',409)
        data = {**(draft.arguments_json if draft else {'periodo':timezone.localdate().replace(day=1).isoformat(),'solicitante_id':actor.pk}), **changes}
        if draft and draft.metadata_json.get('files') and any(data.get(k)!=draft.arguments_json.get(k) for k in ('area_id','solicitante_id')):
            raise WorkflowError('purchase_evidence_binding',409, 'La evidencia está vinculada a esta persona y área; no puede trasladarse a otra solicitud.')
        identities(actor,data)
        missing = missing_fields(data)
        meta = {**(draft.metadata_json if draft else {}), 'version':draft.metadata_json['version']+1 if draft else 1,
            'purchase_status':'WAITING_INFORMATION' if missing else 'AWAITING_CONFIRMATION',
            'last_request_id':origin, 'last_request_hash':digest(changes), 'expires_at':(timezone.now()+timedelta(hours=24)).isoformat()}
        meta['payload_hash'] = digest({'fields':data,'files':meta.get('files',[])})
        if draft:
            draft.arguments_json,draft.metadata_json,draft.assistant_message = data,meta,assistant_message
            draft.save(update_fields=['arguments_json','metadata_json','assistant_message','updated_at'])
        else:
            draft = ChatToolCall.objects.create(public_id=origin, conversation=conversation,request_message=user_message,assistant_message=assistant_message,
                tool_key=KEY,tool_name='erp_prepare_special_purchase',tool_display_name='Solicitud de compra especial',arguments_json=data,metadata_json=meta,status='complete')
        return save_result(draft,actor,'AI_PURCHASE_PREPARE')


def owned_draft(user, pk, *, lock=False):
    qs = ChatToolCall.objects.select_related('conversation').filter(public_id=pk,conversation__owner=user,tool_key=KEY)
    if lock: qs=qs.select_for_update(of=('self',))
    draft=qs.first()
    if not draft: raise WorkflowError('purchase_not_found',404)
    return draft,actor_for(user,draft.conversation)


def confirm(*,user,draft_id,arguments):
    serializer=IncidentConfirmation(data=arguments);serializer.is_valid(raise_exception=True)
    with transaction.atomic():
        draft,actor=owned_draft(user,draft_id,lock=True)
        dto,meta=project(draft,actor),draft.metadata_json
        if dto['version']!=arguments['expected_version'] or dto['payload_hash']!=arguments['payload_hash']:
            raise WorkflowError('purchase_version_conflict',409)
        if dto['status']=='EXECUTED': return response(dto)
        if dto['status']!='AWAITING_CONFIRMATION' or dto['missing_fields']: raise WorkflowError('purchase_not_ready',409)
        area,requester=identities(actor,draft.arguments_json)
        # The area lock serializes confirmations across conversations before checking duplicates.
        AreaPresupuesto.objects.select_for_update().get(pk=area.pk)
        if duplicates(actor,draft.arguments_json): raise WorkflowError('purchase_duplicate',409)
        for info in meta.get('files',[]):
            with default_storage.open(info['path'],'rb') as source: blob=source.read(10*1024*1024+1)
            if len(blob)!=info['bytes'] or hashlib.sha256(blob).hexdigest()!=info['sha256']: raise WorkflowError('purchase_evidence_changed',409)
        data=draft.arguments_json
        try:
            row=crear_solicitud(actor=actor,solicitante=requester,area=area,periodo=date.fromisoformat(data['periodo']),
                tipo='EXTRAORDINARIA',motivo=data['motivo'],justificacion_extraordinaria=data['justificacion_extraordinaria'],enviar=True,
                items=[{'descripcion':i['descripcion'],'cantidad':Decimal(i['cantidad']),'unidad':i['unidad'],
                        'costo_unitario_estimado':Decimal(i['costo_unitario_estimado']) if i['costo_unitario_estimado'] is not None else None} for i in derived_items(data)])
        except (ModelValidationError,PermissionDenied) as exc:
            raise WorkflowError('purchase_validation',409,str(exc)) from exc
        if data.get('compra_reportada'):
            EventoCompraDepartamental.objects.create(solicitud=row,actor=actor,tipo='COMPRA_REPORTADA_IA',
                detalle='Compra reportada por el usuario; pendiente de regularización de cotizaciones y envío. '
                        'No registra pago, autorización ni recepción. ')
        draft.metadata_json={**meta,'purchase_status':'EXECUTED','request_id':row.pk,'folio':row.folio,'confirmed_by':actor.pk,'confirmed_at':timezone.now().isoformat()}
        draft.save(update_fields=['metadata_json','updated_at'])
        AuditLog.objects.create(user=actor,action='AI_PURCHASE_REQUEST_CREATE',model='compras.SolicitudCompraDepartamental',object_id=str(row.pk),
            payload={'draft_id':str(draft.public_id),'conversation_id':str(draft.conversation.public_id),'confirmation':True,
                     'payload_hash':meta['payload_hash'],'solicitante_id':requester.pk,'after':request_dto(row)})
        return save_result(draft,actor,'AI_PURCHASE_CONFIRMED')


def attach(*,user,draft_id,arguments,files):
    serializer=IncidentConfirmation(data=arguments);serializer.is_valid(raise_exception=True)
    files=validate_evidence_files(files,images_only=True)
    if not files or sum(f.size for f in files)>20*1024*1024: raise WorkflowError('purchase_invalid_files',400)
    from PIL import Image
    uploads=[]
    for file in files:
        try:
            with Image.open(file) as image:
                if image.width*image.height>25000000: raise ValueError
                image.verify()
        except Exception: raise WorkflowError('purchase_invalid_image',400) from None
        finally: file.seek(0)
        blob=file.read();file.seek(0)
        uploads.append({'nombre':file.name,'bytes':file.size,'sha256':hashlib.sha256(blob).hexdigest(),'mime':file.content_type})
    if len({i['sha256'] for i in uploads})!=len(uploads): raise WorkflowError('purchase_duplicate_files',400)
    upload_hash=digest(uploads)
    saved=[]
    try:
        with transaction.atomic():
            get_user_model().objects.select_for_update().get(pk=user.pk)
            draft,actor=owned_draft(user,draft_id,lock=True)
            dto,meta=project(draft,actor),draft.metadata_json
            if meta.get('upload_hash')==upload_hash and meta.get('upload_version')==arguments['expected_version'] and meta.get('upload_payload_hash')==arguments['payload_hash']:
                return response(dto)
            if dto['status'] not in ('WAITING_INFORMATION','AWAITING_CONFIRMATION') or dto['version']!=arguments['expected_version'] or dto['payload_hash']!=arguments['payload_hash']:
                raise WorkflowError('purchase_version_conflict',409)
            if meta.get('files'): raise WorkflowError('purchase_files_already_bound',409)
            pending=list(ChatToolCall.objects.filter(conversation__owner=actor,tool_key__in=['incident.followup',KEY])
                .exclude(Q(metadata_json__purchase_status='EXECUTED')|Q(metadata_json__incident_status='EXECUTED')).values_list('metadata_json',flat=True)[:101])
            if len(pending)>100 or sum(f['bytes'] for m in pending for f in m.get('files',[]))+sum(f.size for f in files)>100*1024*1024:
                raise WorkflowError('purchase_staging_limit',409)
            for file,info in zip(files,uploads):
                from pathlib import Path
                uid=uuid4();path=default_storage.save(f'compras/departamentales/agente/{draft.public_id.hex}/{uid.hex}{Path(file.name).suffix.lower()}',file)
                saved.append(path);info.update(id=str(uid),path=path)
            draft.metadata_json={**meta,'files':uploads,'upload_hash':upload_hash,'upload_version':arguments['expected_version'],
                'upload_payload_hash':arguments['payload_hash'],'version':meta['version']+1}
            draft.metadata_json['payload_hash']=digest({'fields':draft.arguments_json,'files':uploads})
            draft.save(update_fields=['metadata_json','updated_at'])
            return save_result(draft,actor,'AI_PURCHASE_ATTACH')
    except Exception:
        for path in saved: default_storage.delete(path)
        raise
