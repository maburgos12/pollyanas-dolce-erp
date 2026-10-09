"""Destinos documentales explícitos, sin efectos de adquisición o presupuesto."""
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Q

from activos.models import Activo, OrdenMantenimiento
from core.access import can_manage_inventario, can_view_inventario
from core.models import AuditLog
from mantenimiento.services_access import authorized_orders, can_access_mantenimiento, can_write_mantenimiento
from mantenimiento.services_vinculos_proveedores import actor_actual
from .access_departamentales import _areas_lectura_solicitudes, _puede_enviar_solicitud, _puede_ver_solicitud
from .models import DestinoCompraDocumental, ItemCompraDepartamental


def destinos_autorizados(actor, tipo, *, escritura=False):
    """Cada puerta conserva la capacidad y alcance de su fuente original."""
    inventario = can_manage_inventario(actor) if escritura else can_view_inventario(actor)
    if tipo == 'ACTIVO':
        return Activo.objects.all() if inventario else Activo.objects.none()
    if tipo != 'ORDEN':
        raise ValidationError('Selecciona un equipo o un trabajo existente.')
    if inventario:
        return OrdenMantenimiento.objects.all()
    # Restricción propia de la relación: los grupos no reviven permisos revocados.
    explicit = dict(actor.module_access.filter(module__in=[
        'mantenimiento','mantenimiento.app','mantenimiento.bandeja','mantenimiento.dashboard',
    ]).values_list('module','access'))
    limite = 'manage' if actor.is_superuser else explicit.get('mantenimiento')
    if limite is None and explicit:
        values = set(explicit.values())
        limite = 'manage' if 'manage' in values else 'view' if 'view' in values else 'none'

    mantenimiento = can_access_mantenimiento(actor) and limite != 'none'
    if escritura:
        mantenimiento = mantenimiento and limite != 'view' and can_write_mantenimiento(actor)
    return authorized_orders(actor) if mantenimiento else OrdenMantenimiento.objects.none()


def vinculos_visibles(actor, *, item=None, tipo=None, destino_id=None):
    vinculos = DestinoCompraDocumental.objects.select_related('item__solicitud__area', 'activo', 'orden__activo_ref', 'autor')
    vinculos = vinculos.filter(
        Q(tipo='ACTIVO', activo__in=destinos_autorizados(actor,'ACTIVO'))
        | Q(tipo='ORDEN', orden__in=destinos_autorizados(actor,'ORDEN')),
        item__isnull=False,
    )
    areas = _areas_lectura_solicitudes(actor)
    if areas is not None:
        vinculos = vinculos.filter(item__solicitud__area_id__in=areas)
    if item is not None:
        vinculos = vinculos.filter(item=item)
    if tipo is not None:
        vinculos = vinculos.filter(tipo=tipo,destino_original_id=destino_id)
    return list(vinculos)


@transaction.atomic
def confirmar_destino(*, user, item_id, tipo, destino_id, motivo, evidencia, confirmado):
    actor = actor_actual(user)
    motivo, evidencia = str(motivo or '').strip(), str(evidencia or '').strip()
    if confirmado is not True or not motivo or not evidencia:
        raise ValidationError('Confirma el destino e indica motivo y evidencia documental.')
    if tipo not in ('ACTIVO','ORDEN'):
        raise ValidationError('Selecciona un equipo o un trabajo existente.')
    try:
        item_id, destino_id = int(item_id), int(destino_id)
    except (ValueError, TypeError):
        raise ValidationError('Selecciona los registros existentes.')
    # La fuente serializa el par sin bloquear las FK compartidas con otros flujos.
    item = ItemCompraDepartamental.objects.select_for_update(no_key=True).select_related('solicitud').filter(pk=item_id).first()
    if not item or not _puede_ver_solicitud(actor,item.solicitud) or not _puede_enviar_solicitud(actor,item.solicitud):
        raise PermissionDenied('Necesitas gestionar la solicitud de origen.')
    destino = destinos_autorizados(actor,tipo,escritura=True).select_for_update(no_key=True).filter(pk=destino_id).first()
    if destino is None:
        raise PermissionDenied('Necesitas gestionar el destino dentro de tu alcance actual.')
    vinculo, creado = DestinoCompraDocumental.objects.get_or_create(
        item_original_id=item.pk,tipo=tipo,destino_original_id=destino.pk,
        defaults={'item':item, 'activo':destino if tipo=='ACTIVO' else None,
                  'orden':destino if tipo=='ORDEN' else None,
                  'motivo':motivo,'evidencia':evidencia,'autor':actor,'autor_original_id':actor.pk},
    )
    if not creado:
        if vinculo.item_id != item.pk or (vinculo.activo_id if tipo=='ACTIVO' else vinculo.orden_id) != destino.pk:
            raise ValidationError('La fuente original fue eliminada. No se restaura el vínculo.',code='conflict')
        if vinculo.motivo != motivo or vinculo.evidencia != evidencia:
            raise ValidationError('Este destino ya fue confirmado con otro contenido. Se conserva el registro original.',code='conflict')
        return vinculo,False
    AuditLog.objects.create(user=actor,action='CREATE',model='compras.DestinoCompraDocumental',object_id=str(vinculo.pk),
                            payload={'item_id':item.pk,'tipo':tipo,'destino_id':destino.pk,'motivo':motivo,'evidencia':evidencia})
    return vinculo,True
