"""Captura única de solicitudes; la identidad de quien interviene queda auditada."""
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import connection, transaction
from django.utils import timezone

from core.models import AuditLog
from reportes.models import AreaPresupuesto
from .access_departamentales import _areas_usuario, _es_direccion, puede_gestionar_compras_departamentales
from .models import SolicitudCompraDepartamental, ItemCompraDepartamental


@transaction.atomic
def crear_solicitud(*, actor, solicitante, area, periodo, tipo, motivo,
                    justificacion_extraordinaria, items, enviar=False):
    if not actor.is_active or not solicitante.is_active:
        raise PermissionDenied('Usuario inactivo.')
    area = AreaPresupuesto.objects.select_for_update().get(pk=area.pk, activa=True)
    if not (puede_gestionar_compras_departamentales(actor) or _areas_usuario(actor).filter(pk=area.pk).exists()):
        raise PermissionDenied('Solo puedes solicitar compras para un área autorizada.')
    if solicitante.pk != actor.pk and not (_es_direccion(actor) and _areas_usuario(solicitante).filter(pk=area.pk).exists()):
        raise PermissionDenied('No puedes representar a ese solicitante en esta área.')
    if not items:
        raise ValidationError('Agrega al menos un artículo.')
    solicitud = SolicitudCompraDepartamental(area=area, solicitante=solicitante, periodo=periodo,
        tipo=tipo, motivo=motivo, justificacion_extraordinaria=justificacion_extraordinaria,
        estado='ENVIADA' if enviar else 'BORRADOR', enviada_en=timezone.now() if enviar else None)
    solicitud.full_clean()
    # El folio legado usa count+1; serializar también a los capturistas anteriores.
    with connection.cursor() as cursor:
        cursor.execute('LOCK TABLE compras_solicitudcompradepartamental IN SHARE ROW EXCLUSIVE MODE')
    solicitud.save()
    for fields in items:
        item = ItemCompraDepartamental(solicitud=solicitud, estado='POR_REVISAR', **fields)
        item.full_clean()
        if item.cantidad <= 0 or (item.costo_unitario_estimado is not None and item.costo_unitario_estimado < 0):
            raise ValidationError('Cantidad o costo estimado inválido.')
        item.save()
    AuditLog.objects.create(user=actor, action='COMPRA_SOLICITUD_CREADA', model='compras.SolicitudCompraDepartamental',
        object_id=str(solicitud.pk), payload={'solicitante_id': solicitante.pk, 'area_id': area.pk,
                                            'estado': solicitud.estado, 'item_ids': list(solicitud.items.values_list('pk', flat=True))})
    return solicitud
