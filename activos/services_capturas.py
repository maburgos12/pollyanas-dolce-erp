"""Altas legadas de Activos: mismo permiso global, recibo por intento y atomicidad."""
from datetime import date
from decimal import Decimal, InvalidOperation

from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404
from django.utils import timezone

from core.access import can_manage_inventario, can_view_inventario
from maestros.models import Proveedor
from mantenimiento.services_capturas_equipos import (
    CapturaEquipoError, capturar_equipo_autorizado, guardar_factura,
)
from .models import Activo, BitacoraMantenimiento, EvidenciaOrden, OrdenMantenimiento, PlanMantenimiento, SolicitudFalla


def _fecha(value):
    if isinstance(value, date):
        return value
    try:
        return timezone.datetime.fromisoformat(str(value)).date() if value else None
    except ValueError:
        return None


def _decimal(value):
    try:
        number = Decimal(str(value or 0))
        if not number.is_finite():
            raise InvalidOperation
        return number
    except (InvalidOperation, TypeError, ValueError):
        raise CapturaEquipoError('Revisa los importes del mantenimiento.', 400)


def crear_captura_activos(*, usuario, datos, modo, archivos=None):
    """El modo viene del endpoint, nunca de un campo de usuario."""
    permiso = can_view_inventario if modo == 'reporte' else can_manage_inventario
    def validar(user, asset):
        if not user.is_active or not permiso(user):
            raise PermissionDenied('No tienes permisos para esta captura de Activos.')
        if not asset.activo:
            raise PermissionDenied('El equipo está inactivo; conserva el intento y revisa el equipo.')
    if not usuario.is_active or not permiso(usuario):
        raise PermissionDenied('No tienes permisos para esta captura de Activos.')
    if modo != 'api' and not datos.get('clave_captura'):
        raise CapturaEquipoError('Se requiere la clave del intento de captura. Revisa el formulario antes de guardar.', 400)
    try:
        asset_id = int(datos.get('activo_id', 0))
    except (TypeError, ValueError):
        raise CapturaEquipoError('Selecciona un equipo válido.', 400)
    activo = get_object_or_404(Activo, pk=asset_id)
    validar(usuario, activo)
    archivos = archivos or {}
    factura = archivos.get('factura_archivo')
    evidencias = archivos.getlist('evidencias') if hasattr(archivos, 'getlist') else archivos.get('evidencias', [])
    if len(evidencias) > 10 or any(f.size > 30 * 1024 * 1024 for f in ([factura] if factura else []) + evidencias):
        raise CapturaEquipoError('Máximo 10 evidencias y 30 MB por archivo. No se guardó la captura.', 400)
    descripcion = str(datos.get('descripcion') or '').strip()
    responsable = str(datos.get('responsable') or '').strip()
    if (modo in {'reporte', 'rapido'} and not descripcion) or len(descripcion) > 10000 or len(responsable) > 120:
        raise CapturaEquipoError('Revisa la descripción y el responsable.', 400)
    tipo = str(datos.get('tipo') or 'PREVENTIVO').upper()
    prioridad = str(datos.get('prioridad') or ('ALTA' if modo == 'rapido' else 'MEDIA')).upper()
    tipo = tipo if tipo in dict(OrdenMantenimiento.TIPO_CHOICES) else 'PREVENTIVO'
    prioridad = prioridad if prioridad in dict(OrdenMantenimiento.PRIORIDAD_CHOICES) else ('ALTA' if modo == 'rapido' else 'MEDIA')
    scheduled = None if modo == 'rapido' else _fecha(datos.get('fecha_programada'))
    fields = dict(tipo='CORRECTIVO' if modo in {'reporte', 'rapido'} else tipo,
        prioridad=prioridad, descripcion=descripcion, responsable=responsable,
        fecha_programada=scheduled, plan_ref_id=None)
    if modo in {'reporte', 'rapido'}:
        fields['responsable'] = responsable or usuario.get_full_name() or usuario.username
    if datos.get('plan_id') and modo in {'orden', 'api'}:
        try:
            fields['plan_ref_id'] = int(datos['plan_id'])
        except (TypeError, ValueError):
            raise CapturaEquipoError('Selecciona un plan válido.', 400)
    solicitud_ids = []
    if modo == 'rapido':
        raw_ids = datos.getlist('solicitud_id') if hasattr(datos, 'getlist') else datos.get('solicitud_id', [])
        try:
            solicitud_ids = sorted(set(int(s) for s in raw_ids))
        except (TypeError, ValueError):
            raise CapturaEquipoError('Revisa las solicitudes seleccionadas.', 400)
        origen = str(datos.get('origen') or 'EMERGENCIA').upper()
        fields.update(origen=origen if origen in dict(OrdenMantenimiento.ORIGEN_CHOICES) else 'EMERGENCIA',
            estatus='EN_PROCESO', fecha_inicio=None, proxima_revision=_fecha(datos.get('proxima_revision')),
            numero_factura=str(datos.get('numero_factura') or '').strip(), nota_trabajo=str(datos.get('nota_trabajo') or '').strip(),
            **{k:_decimal(datos.get(k)) for k in ('costo_repuestos', 'costo_mano_obra', 'costo_otros')})
        proveedor = datos.get('proveedor_id')
        try:
            fields['proveedor_servicio_id'] = int(proveedor) if proveedor else None
        except (TypeError, ValueError):
            raise CapturaEquipoError('Selecciona un proveedor válido.', 400)
    # Las fechas implícitas quedan fuera del digest: un reintento mañana recupera la misma orden.
    content = dict(activo_id=activo.pk, fields={**fields, 'responsable': responsable}, solicitudes=solicitud_ids, factura=factura, evidencias=evidencias)
    def crear(nuevos):
        if fields.get('plan_ref_id'):
            get_object_or_404(PlanMantenimiento, pk=fields['plan_ref_id'], activo_ref=activo)
        if fields.get('proveedor_servicio_id'):
            get_object_or_404(Proveedor, pk=fields['proveedor_servicio_id'])
        solicitudes = list(SolicitudFalla.objects.select_for_update().filter(pk__in=solicitud_ids).order_by('pk'))
        if len(solicitudes) != len(solicitud_ids) or any(s.activo_ref_id != activo.pk for s in solicitudes):
            raise CapturaEquipoError('Las solicitudes deben pertenecer al equipo seleccionado.', 400)
        if any(s.orden_atencion_id is not None for s in solicitudes):
            raise CapturaEquipoError('Una solicitud ya tiene otra orden de atención. No se reasignó ninguna solicitud.')
        values = fields.copy()
        values['fecha_programada'] = scheduled or timezone.localdate()
        if modo == 'rapido':
            values['fecha_inicio'] = timezone.localdate()
        order = OrdenMantenimiento.objects.create(activo_ref=activo, creado_por=usuario, **values)
        if factura:
            guardar_factura(order, factura, nuevos)
        comment = 'Orden creada desde API' if modo == 'api' else 'Orden creada desde UI'
        if modo == 'rapido':
            comment = f'Orden de emergencia registrada desde dispositivo móvil. Origen: {order.get_origen_display()}'
        if modo == 'reporte':
            profile = getattr(usuario, 'userprofile', None)
            context = []
            if profile and profile.departamento_id:
                context.append(f'Área: {profile.departamento.nombre}')
            if profile and profile.sucursal_id:
                context.append(f'Sucursal: {profile.sucursal.nombre}')
            comment = ' · '.join(context) or 'Reporte desde módulo Activos'
        BitacoraMantenimiento.objects.create(orden=order, accion='REPORTE_FALLA' if modo == 'reporte' else 'CREADA', comentario=comment, usuario=usuario)
        for solicitud in solicitudes:
            solicitud.orden_atencion = order
            solicitud.estatus = SolicitudFalla.ESTATUS_EN_PROCESO
            solicitud.save(update_fields=['orden_atencion', 'estatus', 'actualizado_en'])
        for file in evidencias:
            ext = file.name.rsplit('.', 1)[-1].lower()
            kind = EvidenciaOrden.TIPO_VIDEO if ext in {'mp4','mov','avi','mkv'} else EvidenciaOrden.TIPO_DOCUMENTO if ext in {'pdf','doc','docx','xls','xlsx'} else EvidenciaOrden.TIPO_FOTO
            evidence = EvidenciaOrden(orden=order, tipo=kind, descripcion='Evidencia del mantenimiento', subido_por=usuario)
            evidence.archivo.save(file.name, file, save=False)
            nuevos.append((evidence.archivo.storage, evidence.archivo.name))
            evidence.save()
        return order
    def metadata(order):
        payload = {'captura_origen': 'activos_' + modo, 'folio': order.folio,
            'activo_id': order.activo_ref_id, 'plan_id': order.plan_ref_id, 'tipo': order.tipo,
            'prioridad': order.prioridad, 'estatus': order.estatus, 'source': modo}
        if modo == 'rapido':
            payload.update(activo=order.activo_ref.nombre, origen=order.origen)
        if modo == 'reporte':
            payload.update(tipo='REPORTE_FALLA', tipo_orden=order.tipo)
        return payload

    return capturar_equipo_autorizado(usuario=usuario, activo=activo, operacion='activos_' + modo,
        clave=datos.get('clave_captura') or None, contenido=content, crear=crear, validar=validar,
        ordenes=lambda user: OrdenMantenimiento.objects.all(), audit_metadata=metadata)
