"""Audited, bounded business projection; the legacy Gateway is never invoked."""
from datetime import timedelta
import hashlib
import inspect
import json
from pathlib import Path

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.utils import timezone
from rest_framework import serializers
from rest_framework.exceptions import ValidationError

from api.ai_gateway_assets import StrictArguments, StrictDateField, StrictIntegerField, StrictStringField, can_read_assets, fresh_asset_user, json_dto
from core.audit import log_event
from core.models import Sucursal
from compras.models import SolicitudCompra, OrdenCompra
from maestros.models import CostoInsumo
from maestros.utils.canonical_catalog import canonical_insumo, canonical_insumo_by_id, canonical_member_ids
from pos_bridge.models import PointBranch, PointInventorySnapshot


class SalesArguments(StrictArguments):
    start_date = StrictDateField(required=False)
    end_date = StrictDateField(required=False)
    branch = StrictStringField(required=False, max_length=180)

    def validate(self, attrs):
        end = attrs.get('end_date', timezone.localdate())
        start = attrs.get('start_date', end - timedelta(days=29))
        if start > end or (end-start).days >= 366:
            raise ValidationError('El rango debe ser ordenado y abarcar como máximo 366 días.')
        return {**attrs, 'start_date':start, 'end_date':end}


class InventoryArguments(StrictArguments):
    branch = StrictStringField(required=False, max_length=180)
    limit = StrictIntegerField(required=False, min_value=1, max_value=100, default=20)


class CostArguments(StrictArguments):
    q = StrictStringField(required=False, max_length=180)
    insumo_id = StrictIntegerField(required=False, min_value=1, max_value=9223372036854775807)
    limit = StrictIntegerField(required=False, min_value=1, max_value=25, default=8)

    def validate(self, attrs):
        if ('q' in attrs) == ('insumo_id' in attrs):
            raise ValidationError('Indica una búsqueda o una opción de insumo, exclusivamente.')
        return attrs


class OrdersArguments(StrictArguments):
    q = StrictStringField(required=False, allow_blank=True, max_length=180, default='')
    estatus = serializers.ChoiceField(required=False, choices=OrdenCompra.STATUS_CHOICES)
    limit = StrictIntegerField(required=False, min_value=1, max_value=100, default=20)


class RequestsArguments(OrdersArguments):
    estatus = serializers.ChoiceField(required=False, choices=SolicitudCompra.STATUS_CHOICES)
    area = StrictStringField(required=False, max_length=120)


SERIALIZERS = {
    'erp.get_sales_summary': SalesArguments,
    'erp.get_inventory_low_stock': InventoryArguments,
    'erp.get_current_input_cost': CostArguments,
    'erp.get_purchase_requests': RequestsArguments,
    'erp.get_purchase_orders': OrdersArguments,
}
DISCOVERY_KEY = 'knowledge.discover'


class DiscoveryArguments(StrictArguments):
    keys = serializers.ListField(child=serializers.ChoiceField(choices=tuple(SERIALIZERS)), min_length=1, max_length=5)

    def validate_keys(self, value):
        if len(set(value)) != len(value):
            raise ValidationError('No repetir capacidades.')
        return value


UNITS = {
    'erp.get_sales_summary':'sales_range',
    'erp.get_inventory_low_stock':'branch_product_snapshot',
    'erp.get_current_input_cost':'canonical_input_purchase_cost',
    'erp.get_purchase_requests':'classic_purchase_request',
    'erp.get_purchase_orders':'classic_purchase_order',
}
RELATIONSHIPS = {
    'erp.get_sales_summary':['Venta por fecha y sucursal ERP; fuente elegida por cobertura.'],
    'erp.get_inventory_low_stock':['Producto Point y sucursal Point vinculada a sucursal ERP.'],
    'erp.get_current_input_cost':['Costo de compra e insumo canónico con unidad base y proveedor.'],
    'erp.get_purchase_requests':['Solicitud clásica e insumo; proveedor sugerido opcional.'],
    'erp.get_purchase_orders':['Orden clásica y proveedor; solicitud clásica opcional.'],
}


def _registry():
    from api.ai_gateway_services import TOOLS
    return TOOLS


def allowed_keys(actor):
    actor = fresh_asset_user(actor)
    if not actor or not can_read_assets(actor) or getattr(settings, 'AI_AGENT_READ_ENABLED', False) is not True:
        return []
    return [key for key in SERIALIZERS if _registry()[key].access_check(actor)]


def _scope_ids(actor):
    from api.ai_gateway_services import _build_scope
    profile = getattr(actor, 'userprofile', None)
    if profile and profile.modo_captura_sucursal and not profile.sucursal_id:
        raise PermissionDenied('La captura por sucursal requiere una sucursal asignada.')
    scope = _build_scope(actor, branch_scoped=True, arguments={})
    return [scope['branch_id']] if scope['branch_capture_only'] else None


def _branches(actor):
    ids = _scope_ids(actor)
    return Sucursal.objects.filter(pk__in=ids) if ids is not None else Sucursal.objects.all()


def access_fingerprint(actor):
    actor = fresh_asset_user(actor)
    if not actor:
        raise PermissionDenied('Actor no disponible.')
    profile = getattr(actor, 'userprofile', None)
    return hashlib.sha256(json.dumps({
        'allowed_keys':allowed_keys(actor), 'branch_ids':_scope_ids(actor),
        'capture':bool(profile and profile.modo_captura_sucursal),
        'permissions':sorted(actor.get_all_permissions()),
        'branches':list(_branches(actor).order_by('pk').values_list('pk','codigo','nombre')),
        'point_links':list(PointBranch.objects.filter(erp_branch_id__in=_branches(actor).values('pk')).order_by('pk').values_list('pk','erp_branch_id','external_id','name')),
    }, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def catalog(actor):
    return [{'key':key, 'name':key.replace('.','_'), 'display_name':_registry()[key].name,
             'description':_registry()[key].description, 'argument_schema':SERIALIZERS[key].argument_schema(),
             'strict':False, 'business':True} for key in allowed_keys(actor)]


def knowledge(actor):
    descriptors = []
    for tool in catalog(actor):
        key = tool['key']
        descriptors.append({'key':key, 'name':tool['display_name'], 'description':tool['description'],
            'domain':_registry()[key].data_domain,
            'required_fields':tool['argument_schema'].get('required',[]),
            'unit_of_analysis':UNITS[key], 'relationships':RELATIONSHIPS[key],
            'freshness':'Lectura nueva al consultar; fecha de captura/costo separada de la consulta.',
            'source':'Servicio canónico de ventas' if key.endswith('sales_summary') else 'Snapshots Point' if key.endswith('low_stock') else 'Catálogo y costos de compra' if key.endswith('input_cost') else 'Compras clásicas; no incluye compras departamentales ni especiales.',
            'risk':'Sólo lectura; datos parciales y límites de fuente se conservan.'})
    # Source contents version the projection independently of a deploy machine or Git HEAD.
    files = {Path(__file__), Path(inspect.getsourcefile(canonical_insumo))}
    for key in SERIALIZERS:
        for function in (_registry()[key].handler, _registry()[key].access_check):
            source = inspect.getsourcefile(function)
            if source: files.add(Path(source))
    from ventas.services import sales_read_service
    files.add(Path(sales_read_service.__file__))
    digest = hashlib.sha256(json.dumps({'schema':1,'capabilities':descriptors,
        'contracts':[tool['argument_schema'] for tool in catalog(actor)]},sort_keys=True,ensure_ascii=False).encode())
    for source in sorted(files, key=lambda path:path.name):
        digest.update(source.read_bytes())
    return {'schema_version':1, 'version':digest.hexdigest(), 'capabilities':descriptors,
            'branches':[{'code':row.codigo,'name':row.nombre} for row in _branches(actor).order_by('codigo','pk')],
            'data_is_authority':False}


DISCOVERY_TOOL = {'key':DISCOVERY_KEY, 'name':'erp_discover_capabilities', 'display_name':'Descubrir consultas autorizadas',
    'description':'Selecciona claves del conocimiento autorizado para obtener contratos y habilitar sus consultas en la siguiente ronda. No consulta cifras ni concede acceso.',
    'argument_schema':DiscoveryArguments.argument_schema(), 'strict':False, 'business':True}


def discovery_tool(actor):
    tool = json_dto(DISCOVERY_TOOL)
    tool['argument_schema']['properties']['keys']['items']['enum'] = allowed_keys(actor)
    return tool


def _audit(actor, key, arguments, scope, status):
    safe = json_dto(arguments)
    for name in ('q','area','branch'):
        if name in safe: safe[name+'_length'] = len(safe.pop(name))
    log_event(actor,'AI_AGENT_BUSINESS_READ','api.ai_gateway',key,
              {'tool_key':key,'operation_type':'read','data_domain':_registry()[key].data_domain if key in SERIALIZERS else 'knowledge',
               'arguments':safe,'scope':scope,'result_status':status})


def record_invalid(*, user, tool_key):
    _audit(fresh_asset_user(user), tool_key if tool_key in {*SERIALIZERS,DISCOVERY_KEY} else 'unregistered', {}, {}, 'invalid_arguments')


def record_denied(*, user, tool_key=DISCOVERY_KEY):
    _audit(fresh_asset_user(user), tool_key if tool_key in {*SERIALIZERS, DISCOVERY_KEY} else 'unregistered', {}, {}, 'denied')


def _resolve_branch(actor, arguments):
    from api.ai_gateway_services import _enforce_branch_scope
    assigned = _scope_ids(actor)
    token = arguments.get('branch','')
    if not token:
        scope = _enforce_branch_scope(actor,branch_scoped=True,arguments={})
        return assigned, scope, None
    point_ids = PointBranch.objects.filter(Q(name__iexact=token)|Q(external_id__iexact=token)).exclude(erp_branch_id=None).values('erp_branch_id')
    rows = list(Sucursal.objects.filter(Q(codigo__iexact=token)|Q(nombre__iexact=token)|Q(pk__in=point_ids)).order_by('codigo','pk'))
    if not rows: raise ValidationError('Sucursal desconocida; usa una opción del catálogo autorizado.')
    if assigned is not None:
        if any(row.pk not in assigned for row in rows):
            raise PermissionDenied('La consulta excede la sucursal asignada.')
    if len(rows)>1:
        return assigned, {}, {'status':'ambiguous','sources':['Catálogo de sucursales ERP'], 'payload':{'items':[{'code':row.codigo,'name':row.nombre} for row in rows], 'selection_required':True, 'truncated':False}}
    scope = _enforce_branch_scope(actor,branch_scoped=True,arguments={'branch':rows[0].codigo,'sucursal_id':rows[0].pk})
    return [rows[0].pk], scope, None


def _cost(arguments):
    from api.ai_gateway_services import _resolve_input_cost_candidates, _serialize_input_cost_candidate
    limit = arguments['limit']
    search_truncated = False
    if 'insumo_id' in arguments:
        selected = canonical_insumo_by_id(arguments['insumo_id'])
        candidates = [selected] if selected and selected.activo else []
    else:
        candidates, search_truncated = _resolve_input_cost_candidates(arguments['q'],limit=limit+1,include_search_metadata=True)
    canonical = {row.pk:row for item in candidates if (row:=canonical_insumo(item)) is not None}
    choices = [_serialize_input_cost_candidate(row) for row in canonical.values()]
    truncated = search_truncated or len(candidates)>limit
    result = {'sources':['maestros.CostoInsumo','maestros.Insumo'], 'payload':{'candidates':choices[:limit],'truncated':truncated}}
    if len(canonical)>1 or truncated:
        return {**result,'status':'ambiguous'}
    insumo = next(iter(canonical.values()),None)
    if insumo is None:
        return {**result,'status':'no_data'}
    member_ids = canonical_member_ids(insumo)
    latest = CostoInsumo.objects.select_related('proveedor').filter(insumo_id__in=member_ids,fecha__lte=timezone.localdate()).order_by('-fecha','-id').first()
    result['payload'].update(_serialize_input_cost_candidate(insumo), costo_unitario=str(latest.costo_unitario) if latest else None,
        moneda=latest.moneda if latest else None, fecha=latest.fecha.isoformat() if latest else None,
        proveedor=latest.proveedor.nombre if latest and latest.proveedor else None,
        freshness={'cost_date':latest.fecha.isoformat() if latest else None, 'future_costs_excluded':True})
    return {**result,'status':'ok' if latest else 'no_data'}


def invoke(*, user, tool_key, arguments):
    actor = fresh_asset_user(user)
    key = tool_key if tool_key in {*SERIALIZERS,DISCOVERY_KEY} else 'unregistered'
    safe, scope = {}, {}
    try:
        permitted = allowed_keys(actor)
        if not permitted or key=='unregistered' or (key!=DISCOVERY_KEY and key not in permitted):
            raise PermissionDenied('Consulta no autorizada.')
        fingerprint = access_fingerprint(actor)
        serializer = (DiscoveryArguments if key==DISCOVERY_KEY else SERIALIZERS[key])(data=arguments)
        serializer.is_valid(raise_exception=True)
        safe = serializer.validated_data
        if key==DISCOVERY_KEY:
            if not set(safe['keys'])<=set(permitted): raise PermissionDenied('Capacidad no autorizada.')
            result={'status':'ok','sources':['Catálogo autorizado ERP'],'unit_of_analysis':'capability_contract',
                    'payload':{'tools':[tool for tool in catalog(actor) if tool['key'] in safe['keys']], 'version':knowledge(actor)['version']}}
        else:
            tool = _registry()[key]
            from api import ai_gateway_services as gateway
            result = None
            if tool.branch_scoped:
                ids, scope, result = _resolve_branch(actor,safe)
                scope['resolved_branch_ids']=ids
            if result is None:
                if key=='erp.get_sales_summary':
                    result = gateway._handle_sales_summary(actor,json_dto(safe),branch_ids=ids,exact_scope=True)
                    data = result['payload']
                    # Aggregate source adapters use a ticket placeholder; it cannot support a ticket count or average.
                    data['total_tickets'] = None
                    data['limitations']['ticket_count_available'] = False
                    if data['source_status']=='EMPTY':
                        result['status']='no_data'
                        for field in ('total_sales','total_quantity','total_tickets'): data[field]=None
                    data['freshness']={'coverage_days':data.get('coverage_days'),'coverage_accepted':data.get('coverage_accepted'),'source_detail':data.get('source_detail'),'coverage_reason':data.get('coverage_reason'),'reason':data.get('reason')}
                    data['truncated']=False
                elif key=='erp.get_current_input_cost': result=_cost(safe)
                else:
                    bounded = {**safe,'limit':safe['limit']+1}
                    result = gateway._handle_inventory_low_stock(actor,bounded,branch_ids=ids,exact_scope=True) if key=='erp.get_inventory_low_stock' else tool.handler(actor,bounded)
                    data=result['payload']; rows=data['items']
                    data.update(items=rows[:safe['limit']],returned=min(len(rows),safe['limit']),truncated=len(rows)>safe['limit'])
                    result['status']='ok' if rows else 'no_data'
                    if key=='erp.get_inventory_low_stock':
                        snapshots = PointInventorySnapshot.objects.all()
                        if ids is not None:
                            snapshots = snapshots.filter(branch__erp_branch_id__in=ids)
                        # The newest capture overall is also among the newest per product.
                        latest = snapshots.order_by('-captured_at').values_list('captured_at', flat=True).first()
                        data['freshness']={'captured_at':[row['captured_at'] for row in data['items']], 'query_is_capture_time':False,
                                           'source_latest_capture':latest.isoformat() if latest else None,
                                           'source_age_seconds':int((timezone.now()-latest).total_seconds()) if latest else None,
                                           'source_snapshots_available':latest is not None}
                    else:
                        data['limitations']=['Compras clásicas únicamente; no incluye solicitudes u órdenes departamentales ni compras especiales.']
                        data['freshness']={'read_live':True,'updated_at_available':False}
            result['unit_of_analysis']=UNITS[key]
        result.update(as_of=timezone.now().isoformat(),timezone=settings.TIME_ZONE,data_is_authority=False,filters=json_dto(safe))
        # Recheck effective authorization after a read, before its evidence leaves the boundary.
        if access_fingerprint(actor)!=fingerprint: raise PermissionDenied('El acceso cambió durante la consulta.')
        response={'status':'ok','mode':'READ','scope':scope,'result':json_dto(result)}
    except Exception as exc:
        _audit(actor,key,safe,scope,'denied' if isinstance(exc,PermissionDenied) else 'invalid_arguments' if isinstance(exc,ValidationError) else 'failed')
        raise
    _audit(actor,key,safe,scope,result['status'])
    return response
