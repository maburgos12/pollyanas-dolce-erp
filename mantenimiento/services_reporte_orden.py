"""Intervención explícita desde un reporte, sin mover sus importes o evidencias."""
from collections.abc import Mapping
from datetime import date

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.shortcuts import get_object_or_404
from django.utils import timezone

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento
from mantenimiento.services_access import authorized_fallas, can_write_mantenimiento
from mantenimiento.services_capturas_equipos import CapturaEquipoError, capturar_equipo, validar_equipo
from mantenimiento.services_vinculos import guardar_vinculo


ABIERTOS = {'abierto', 'en_revision', 'en_proceso'}
ACTIVAS = {'PENDIENTE', 'EN_PROCESO'}


def motivo_no_elegible(reporte):
    if reporte.tipo_objetivo != 'EQUIPO' or not reporte.activo_relacionado_id:
        return 'El reporte necesita un equipo registrado.'
    if reporte.duplicado_de_id:
        return 'Este reporte es repetido. Revisa el reporte principal.'
    if not reporte.activo_relacionado.activo:
        return 'El equipo está inactivo.'
    if reporte.activo_relacionado.sucursal_id != reporte.sucursal_id:
        return 'El equipo y el reporte pertenecen a distintas sucursales.'
    if reporte.estatus not in ABIERTOS:
        return 'El reporte está finalizado; no admite nuevas intervenciones.'
    return ''


def contexto_orden_desde_reporte(usuario, reporte_id):
    reporte = get_object_or_404(authorized_fallas(usuario).select_related('activo_relacionado', 'sucursal'), pk=reporte_id)
    links = reporte.vinculos_atencion.select_related('orden').order_by('-creado_en', '-pk')
    from mantenimiento.services_access import authorized_orders
    links = links.filter(orden_id__in=authorized_orders(usuario).values('pk'))
    orders = [{'id':v.orden_id, 'folio':v.orden.folio, 'uid':f'orden:{v.orden_id}',
               'estatus':v.orden.estatus, 'estado':v.orden.get_estatus_display()} for v in links]
    active = [o for o in orders if o['estatus'] in ACTIVAS]
    reason = motivo_no_elegible(reporte)
    if not reason and active:
        reason = 'Ya existe una orden pendiente o en proceso para este reporte.'
    if not reason and not can_write_mantenimiento(usuario):
        reason = 'No tienes permiso para crear órdenes.'
    if not reason:
        try:
            validar_equipo(usuario, reporte.activo_relacionado)
        except PermissionDenied:
            reason = 'El equipo no está disponible en tu ámbito autorizado.'
    return {'puede_crear':not reason, 'motivo':reason, 'activas':active, 'ordenes':orders,
            'activo':{'id':reporte.activo_relacionado_id, 'nombre':str(reporte.activo_relacionado) if reporte.activo_relacionado_id else ''},
            'sucursal':{'id':reporte.sucursal_id, 'nombre':reporte.sucursal.nombre},
            'descripcion':reporte.descripcion, 'prioridad':reporte.prioridad.upper(),
            'fecha_programada':timezone.localdate().isoformat(), 'reporte_id':reporte.pk}


def _datos(datos):
    if not isinstance(datos, Mapping):
        raise CapturaEquipoError('Datos de orden no válidos.', 400)
    allowed = {'descripcion', 'prioridad', 'fecha_programada', 'responsable', 'clave_captura', 'csrfmiddlewaretoken'}
    if set(datos) - allowed:
        raise CapturaEquipoError('El equipo y la sucursal son fijos; no se permiten importes ni otros campos.', 400)
    description = datos.get('descripcion')
    responsible = datos.get('responsable', '')
    priority = datos.get('prioridad')
    if (not isinstance(description, str) or not description.strip() or len(description) > 10000
            or not isinstance(responsible, str) or len(responsible) > 120
            or not isinstance(priority, str) or priority not in dict(OrdenMantenimiento.PRIORIDAD_CHOICES)):
        raise CapturaEquipoError('Revisa descripción, prioridad y responsable.', 400)
    try:
        scheduled = date.fromisoformat(datos.get('fecha_programada', ''))
    except (TypeError, ValueError):
        raise CapturaEquipoError('Confirma una fecha programada válida.', 400)
    if not datos.get('clave_captura'):
        raise CapturaEquipoError('Se requiere la clave del intento de captura.', 400)
    return {'descripcion':description.strip(), 'prioridad':priority, 'fecha_programada':scheduled, 'responsable':responsible.strip()}


@transaction.atomic
def crear_orden_desde_reporte(*, usuario, reporte_id, datos):
    if not usuario.is_active or not can_write_mantenimiento(usuario):
        raise PermissionDenied('No tienes permisos para crear órdenes.')
    content = _datos(datos)
    # Serializa este acceso contextual para distintas claves y usuarios. Los flujos
    # de vinculación bloquean orden→reporte; aquí nunca bloqueamos una orden existente.
    reporte = get_object_or_404(authorized_fallas(usuario).select_for_update(of=('self',)), pk=reporte_id)
    if not reporte.activo_relacionado_id:
        raise CapturaEquipoError('El reporte necesita un equipo registrado.', 400)
    activo = get_object_or_404(Activo.objects.select_for_update(), pk=reporte.activo_relacionado_id)
    reporte.activo_relacionado = activo
    validar_equipo(usuario, activo)
    content['reporte_id'] = reporte.pk

    def crear(archivos):
        reason = motivo_no_elegible(reporte)
        if reason:
            raise CapturaEquipoError(reason, 400)
        active = reporte.vinculos_atencion.filter(orden__estatus__in=ACTIVAS).select_related('orden').first()
        if active:
            raise CapturaEquipoError(f'Ya existe la orden {active.orden.folio} pendiente o en proceso. Abre los trabajos vinculados.')
        order = OrdenMantenimiento.objects.create(activo_ref=activo, tipo='CORRECTIVO', estatus='PENDIENTE',
            origen='SOLICITUD', creado_por=usuario, **{k:v for k,v in content.items() if k != 'reporte_id'})
        BitacoraMantenimiento.objects.create(orden=order, usuario=usuario, accion='CREATE',
            comentario=f'Orden creada desde reporte #{reporte.pk}. Estados e importes del reporte conservados.')
        guardar_vinculo(usuario, order, reporte, f'Orden creada para atender reporte #{reporte.pk}.')
        return order

    return capturar_equipo(usuario=usuario, activo=activo, operacion='orden_desde_reporte',
        clave=datos['clave_captura'], contenido=content, crear=crear)
