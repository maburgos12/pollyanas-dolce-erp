"""Vínculos explícitos, con autorización de ambos documentos y auditoría atómica."""
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.shortcuts import get_object_or_404

from core.audit import log_event
from mantenimiento.models import VinculoAtencionEquipo
from mantenimiento.services_access import authorized_fallas, authorized_orders, can_write_mantenimiento

PROTECTED_MESSAGE = 'Este documento tiene trabajos o reportes vinculados. Revisa los vínculos antes de eliminarlo.'


def validar_motivo(motivo):
    if not isinstance(motivo, str) or not motivo.strip() or len(motivo.strip()) > 2000:
        raise ValidationError('Indica un motivo de hasta 2000 caracteres.')
    return motivo.strip()


def _require_write(user):
    if not can_write_mantenimiento(user):
        raise PermissionDenied('No tienes permiso para administrar vínculos.')


def authorized_vinculos(user):
    return VinculoAtencionEquipo.objects.filter(
        orden_id__in=authorized_orders(user).values('pk'),
        reporte_id__in=authorized_fallas(user).values('pk'),
    )


def validar_par(orden, reporte):
    if (not reporte.activo_relacionado_id or reporte.tipo_objetivo != reporte.OBJETIVO_EQUIPO
            or reporte.activo_relacionado_id != orden.activo_ref_id
            or not orden.activo_ref.sucursal_id or reporte.sucursal_id != orden.activo_ref.sucursal_id):
        raise ValidationError('Selecciona documentos del mismo equipo registrado y sucursal.')
    if reporte.duplicado_de_id:
        raise ValidationError('El reporte está marcado como repetido. Vincula su reporte principal.')


@transaction.atomic
def crear_vinculo(user, orden_id, reporte_id, motivo):
    _require_write(user)
    motivo = validar_motivo(motivo)
    # Todos los pares bloquean primero orden, después reporte; serializa reintentos.
    orden = get_object_or_404(authorized_orders(user).select_for_update(of=('self',)).select_related('activo_ref'), pk=orden_id)
    reporte = get_object_or_404(authorized_fallas(user).select_for_update(), pk=reporte_id)
    validar_par(orden, reporte)
    vinculo, creado = VinculoAtencionEquipo.objects.get_or_create(
        orden=orden, reporte=reporte, defaults={'creador':user, 'motivo':motivo},
    )
    if creado:
        log_event(user, 'CREATE', 'mantenimiento.VinculoAtencionEquipo', str(vinculo.pk),
                  {'orden_id':orden.pk, 'reporte_id':reporte.pk, 'motivo':motivo})
    return vinculo, creado


@transaction.atomic
def retirar_vinculo(user, pk, motivo):
    _require_write(user)
    motivo = validar_motivo(motivo)
    vinculo = get_object_or_404(authorized_vinculos(user), pk=pk)
    get_object_or_404(authorized_orders(user).select_for_update(), pk=vinculo.orden_id)
    get_object_or_404(authorized_fallas(user).select_for_update(), pk=vinculo.reporte_id)
    vinculo = get_object_or_404(authorized_vinculos(user).select_for_update(), pk=pk)
    log_event(user, 'DELETE', 'mantenimiento.VinculoAtencionEquipo', str(vinculo.pk),
              {'orden_id':vinculo.orden_id, 'reporte_id':vinculo.reporte_id, 'motivo':motivo})
    vinculo.delete()
