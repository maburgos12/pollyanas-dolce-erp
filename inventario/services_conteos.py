"""Atomic physical count evidence; never adjusts stock or calls Point."""
import hashlib
import json
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from uuid import UUID
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from core.models import Sucursal
from maestros.models import Insumo
from pos_bridge.models import PointProduct, PointSyncJob
from .conteos_access import autorizado_sucursal, puede_capturar, puede_coordinar, puede_revisar
from .models_conteos import ConteoSucursal, LineaConteoSucursal, LecturaConteoSucursal, EventoConteoSucursal, OperacionConteoSucursal


class ConteoError(ValidationError):
    pass


class ConteoConflict(ConteoError):
    pass


def _uuid(value):
    try: return UUID(str(value))
    except (ValueError, TypeError, AttributeError): raise ConteoError('Identificador de solicitud inválido.')


def _hash(value):
    try: return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    except (TypeError, ValueError): raise ConteoError('Datos de solicitud inválidos.')


def _text(value, limit, required=False):
    if not isinstance(value, str) or len(value) > limit or (required and not value.strip()):
        raise ConteoError('Texto faltante o demasiado largo.')
    return value.strip()


def _cantidad(value):
    if value is None or value == '': return None
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ConteoError('Cantidad inválida.')
    try: number = Decimal(value)
    except (InvalidOperation, ValueError): raise ConteoError('Cantidad inválida.')
    if not number.is_finite() or number < 0 or number >= Decimal('1000000000000') or number.as_tuple().exponent < -6:
        raise ConteoError('Cantidad debe ser no negativa, finita y tener hasta seis decimales.')
    return number


@transaction.atomic
def preparar_conteo(*, actor, sucursal, responsable, fecha, titulo, items, request_id):
    if not puede_coordinar(actor): raise ConteoError('Sin permiso para preparar conteos.')
    request_id = _uuid(request_id)
    if not isinstance(fecha, date): raise ConteoError('Fecha inválida.')
    titulo = _text(titulo,180,True)
    if not isinstance(items,list) or not items or len(items)>2000: raise ConteoError('Selecciona entre 1 y 2000 artículos.')
    fingerprint = _hash({'actor':actor.pk,'sucursal':sucursal.pk,'responsable':responsable.pk,'fecha':fecha.isoformat(),'titulo':titulo,'items':items})
    sucursal = Sucursal.objects.select_for_update(no_key=True).get(pk=sucursal.pk)
    previous = ConteoSucursal.objects.filter(request_id=request_id).first()
    if previous:
        if previous.payload_hash != fingerprint: raise ConteoConflict('La solicitud ya se usó con otros datos.')
        return previous
    if not sucursal.esta_operativa() or not sucursal.esta_operativa(fecha): raise ConteoError('Sucursal no operativa.')
    if not autorizado_sucursal(responsable,sucursal): raise ConteoError('Responsable sin acceso vigente a la sucursal.')
    frozen, seen, codes = [], set(), set()
    for item in items:
        if not isinstance(item,dict) or set(item)-{'producto_id','insumo_id','unidad','fuente_unidad'}: raise ConteoError('Artículo inválido.')
        kind = 'producto_id' if item.get('producto_id') else 'insumo_id'
        if bool(item.get('producto_id')) == bool(item.get('insumo_id')): raise ConteoError('Selecciona producto o insumo.')
        ident = item[kind]
        if not isinstance(ident,int) or isinstance(ident,bool): raise ConteoError('Artículo inválido.')
        if (kind,ident) in seen: raise ConteoError('Artículo duplicado.')
        seen.add((kind,ident))
        source = (PointProduct.objects.filter(pk=ident,active=True).first() if kind=='producto_id' else Insumo.objects.filter(pk=ident,activo=True).first())
        if not source: raise ConteoError('Artículo inexistente o inactivo.')
        code = ((source.sku if kind=='producto_id' else source.codigo_point) or '').strip()
        if not code: raise ConteoError('Artículo sin código Point comprobable.')
        if code in codes: raise ConteoError('Código físico duplicado entre artículos.')
        codes.add(code)
        frozen.append({kind:ident,'codigo':code,'nombre':source.name if kind=='producto_id' else source.nombre,'unidad':_text(item.get('unidad'),60,True),'fuente_unidad':_text(item.get('fuente_unidad'),255,True)})
    overlap = Q(producto_id__in=[x['producto_id'] for x in frozen if 'producto_id' in x])|Q(insumo_id__in=[x['insumo_id'] for x in frozen if 'insumo_id' in x])
    if LineaConteoSucursal.objects.filter(overlap | Q(codigo__in=codes), conteo__sucursal=sucursal,conteo__fecha=fecha,conteo__estado__in=['CAPTURA','RECONTEO','ENVIADO']).exists():
        raise ConteoConflict('Ya existe un conteo activo para estos artículos, sucursal y fecha.')
    try:
        # Different branches lock different rows; the UUID remains globally unique.
        # A savepoint preserves the outer transaction for the authoritative replay read.
        with transaction.atomic():
            count = ConteoSucursal.objects.create(sucursal=sucursal,responsable=responsable,creado_por=actor,fecha=fecha,titulo=titulo,request_id=request_id,payload_hash=fingerprint)
    except IntegrityError:
        previous = ConteoSucursal.objects.filter(request_id=request_id).first()
        if previous is None:
            raise
        if previous.payload_hash != fingerprint:
            raise ConteoConflict('La solicitud ya se usó con otros datos.')
        return previous
    for item in frozen:
        line = LineaConteoSucursal.objects.create(conteo=count,**item)
        LecturaConteoSucursal.objects.create(linea=line,ronda=1,actor=actor)
    EventoConteoSucursal.objects.create(conteo=count,actor=actor,action='preparar',payload={'articulos':frozen,'responsable':responsable.pk})
    return count


def lecturas_efectivas(conteo):
    return list(LecturaConteoSucursal.objects.filter(linea__conteo=conteo).order_by('linea_id','-ronda').distinct('linea_id'))


def _guardar(conteo, actor, payload):
    if set(payload)-{'lecturas','observaciones'}: raise ConteoError('Campos no admitidos.')
    values = payload.get('lecturas',{})
    if not isinstance(values,dict): raise ConteoError('Lecturas inválidas.')
    current = {str(r.linea_id):r for r in LecturaConteoSucursal.objects.filter(linea__conteo=conteo,ronda=conteo.ronda)}
    if set(values)-set(current): raise ConteoError('Artículo ajeno al conteo o a la ronda actual.')
    for key, value in values.items():
        if not isinstance(value,dict) or set(value)-{'cantidad','incidencia'}: raise ConteoError('Lectura inválida.')
        reading = current[key]
        reading.cantidad = _cantidad(value.get('cantidad'))
        reading.incidencia = _text(value.get('incidencia',''),2000)
        reading.actor = actor
        reading.save()
    if 'observaciones' in payload: conteo.observaciones = _text(payload['observaciones'],10000)


@transaction.atomic
def ejecutar_accion(*, conteo_id, actor, action, version, request_id, payload):
    if not isinstance(payload,dict): raise ConteoError('Datos inválidos.')
    if action not in {'iniciar','guardar','enviar','reconteo','aceptar','cancelar','referencia','validar_referencia'}: raise ConteoError('Acción desconocida.')
    count = ConteoSucursal.objects.select_for_update().select_related('sucursal').get(pk=conteo_id)
    permission = puede_capturar if action in {'iniciar','guardar','enviar'} else puede_revisar
    if not permission(actor,count): raise ConteoError('Sin permiso vigente para esta acción.')
    request_id = _uuid(request_id)
    fingerprint = _hash({'action':action,'version':version,'payload':payload,'actor':actor.pk})
    previous = count.operaciones.filter(request_id=request_id).first()
    if previous:
        if previous.fingerprint != fingerprint: raise ConteoConflict('La solicitud ya se usó con otros datos.')
        return previous.result
    if type(version) is not int or version != count.version: raise ConteoConflict('El conteo cambió. Actualiza antes de continuar.')
    event_payload = dict(payload)
    if action == 'iniciar':
        if payload:
            raise ConteoError('El inicio se registra con la hora del servidor, sin datos adicionales.')
        if count.estado not in {'CAPTURA', 'RECONTEO'}:
            raise ConteoConflict('El conteo no está abierto a captura.')
        if count.iniciado_en is not None or count.eventos.filter(action='iniciar', payload__ronda=count.ronda).exists():
            raise ConteoConflict('Esta ronda ya fue iniciada.')
        count.iniciado_en = timezone.now()
        event_payload = {'ronda': count.ronda, 'iniciado_en': count.iniciado_en.isoformat()}
    elif action in {'guardar','enviar'}:
        if count.estado not in {'CAPTURA','RECONTEO'}: raise ConteoConflict('El conteo no está abierto a captura.')
        if count.iniciado_en is None or not count.eventos.filter(action='iniciar', payload__ronda=count.ronda).exists():
            raise ConteoError('Inicia explícitamente esta ronda antes de registrar lecturas.')
        _guardar(count,actor,payload)
        if action=='enviar':
            effective=lecturas_efectivas(count)
            if any(r.cantidad is None and not r.incidencia.strip() for r in effective): raise ConteoError('Completa cada cantidad o registra una incidencia.')
            count.estado='ENVIADO'
            count.enviado_en=timezone.now()
            event_payload['snapshot']=[{'linea_id':r.linea_id,'ronda':r.ronda,'cantidad':str(r.cantidad) if r.cantidad is not None else None,'incidencia':r.incidencia} for r in effective]
    elif action=='reconteo':
        if count.estado!='ENVIADO': raise ConteoConflict('Solo se puede recontar un conteo enviado.')
        if set(payload)-{'linea_ids','motivo'}: raise ConteoError('Campos no admitidos.')
        _text(payload.get('motivo'),2000,True)
        ids=payload.get('linea_ids')
        if not isinstance(ids,list) or not ids or any(type(i) is not int for i in ids) or len(set(ids))!=len(ids) or count.lineas.filter(pk__in=ids).count()!=len(ids): raise ConteoError('Selecciona artículos válidos para recontar.')
        count.ronda+=1
        count.estado='RECONTEO'
        count.iniciado_en = None
        count.referencia = {}
        for ident in ids: LecturaConteoSucursal.objects.create(linea_id=ident,ronda=count.ronda,actor=actor)
    elif action in {'aceptar','cancelar'}:
        if set(payload)-{'motivo'}: raise ConteoError('Campos no admitidos.')
        _text(payload.get('motivo'),2000,True)
        if action=='aceptar':
            if count.estado!='ENVIADO': raise ConteoConflict('Solo se puede aceptar un conteo enviado.')
            count.estado='ACEPTADO'
            count.aceptado_en=timezone.now()
        else:
            if count.estado in {'ACEPTADO','CANCELADO'}: raise ConteoConflict('No se puede cancelar este conteo.')
            count.estado='CANCELADO'
    elif action=='referencia':
        if payload: raise ConteoError('La referencia se obtiene del servidor.')
        if count.estado not in {'ENVIADO','ACEPTADO'}: raise ConteoConflict('Primero envía el conteo.')
        from .conteos_point import referencia_conteo
        count.referencia=referencia_conteo(count)
        event_payload={'referencia':count.referencia}
    elif action=='validar_referencia':
        if count.estado not in {'ENVIADO','ACEPTADO'}: raise ConteoConflict('Primero envía el conteo.')
        if set(payload)-{'motivo','confirmado'} or payload.get('confirmado') is not True:
            raise ConteoError('Confirma expresamente que verificaste el corte y sus movimientos.')
        reason = _text(payload.get('motivo'),2000,True)
        reference = count.referencia
        if reference.get('estado') != 'REFERENCIA' or not isinstance(reference.get('lineas'),dict):
            raise ConteoError('No hay referencia completa para validar.')
        from .conteos_point import misma_unidad
        cutoff = count.iniciado_en
        if cutoff is None:
            raise ConteoError('No hay inicio explícito de ronda comprobable.')
        if {reading.ronda for reading in lecturas_efectivas(count)} != {count.ronda}:
            raise ConteoError('No se puede validar un corte comparativo con lecturas de rondas distintas.')
        lines = list(count.lineas.all())
        ids = [reference['lineas'].get(str(line.pk), {}).get('sync_job_id') for line in lines]
        if not ids or any(type(value) is not int for value in ids) or len(set(ids)) != 1:
            raise ConteoError('Todas las líneas deben pertenecer a un único ciclo Point comprobable.')
        cycle = PointSyncJob.objects.filter(pk=ids[0], status=PointSyncJob.STATUS_SUCCESS, job_type=PointSyncJob.JOB_TYPE_INVENTORY).first()
        if not cycle or not cycle.finished_at or not cutoff-timedelta(minutes=15) <= cycle.started_at <= cycle.finished_at <= cutoff:
            raise ConteoError('El ciclo Point completo debe estar dentro de los 15 minutos previos al inicio.')
        for line in lines:
            reading = reference['lineas'].get(str(line.pk),{})
            try:
                amount = Decimal(str(reading.get('cantidad')))
                captured = parse_datetime(reading.get('capturado_en',''))
                started = parse_datetime(reading.get('inicio_extraccion',''))
                finished = parse_datetime(reading.get('fin_extraccion',''))
            except (InvalidOperation, TypeError, ValueError):
                raise ConteoError('Referencia sin cantidad o fecha comprobable.')
            if started != cycle.started_at or finished != cycle.finished_at:
                raise ConteoError('La ventana de extracción no coincide con el ciclo Point registrado.')
            if not captured or timezone.is_naive(captured) or not cycle.started_at <= captured <= cycle.finished_at:
                raise ConteoError('La captura no corresponde a la ventana del ciclo Point.')
            if not amount.is_finite() or not misma_unidad(reading.get('unidad'),line.unidad) or not captured or timezone.is_naive(captured) or not cutoff-timedelta(minutes=15) <= captured <= cutoff:
                raise ConteoError('La referencia no corresponde a una ventana de hasta 15 minutos previa al inicio.')
        count.referencia = {**reference,'corte_verificado':True,'validado_por':actor.pk,'validado_en':timezone.now().isoformat(),'motivo_validacion':reason,'tipo_validacion':'DECLARACION_REVISOR'}
        event_payload = {'referencia':count.referencia}

    count.version+=1
    count.save()
    result={'id':count.pk,'version':count.version,'ronda':count.ronda,'estado':count.estado}
    EventoConteoSucursal.objects.create(conteo=count,actor=actor,action=action,payload={**event_payload,'resultado':result})
    OperacionConteoSucursal.objects.create(conteo=count,actor=actor,request_id=request_id,fingerprint=fingerprint,result=result)
    return result
