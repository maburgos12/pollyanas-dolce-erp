"""Captura única de solicitudes; la identidad de quien interviene queda auditada."""
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import connection, transaction
from django.db.models import Q
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


ELIMINACION_BORRADOR = "SOLICITUD_BORRADOR_ELIMINADA"
RETIRO_SOLICITUD = "SOLICITUD_RETIRADA"
BLOQUEO_SOLICITUD = "La solicitud ya tiene atención de Compras o está cerrada."


def puede_modificar_solicitud(user, solicitud):
    if not (user.is_authenticated and user.is_active):
        return False
    if user.is_superuser:
        return True
    registro = AuditLog.objects.filter(
        model="compras.SolicitudCompraDepartamental", object_id=str(solicitud.pk),
        action="COMPRA_SOLICITUD_CREADA",
    ).order_by("pk").values_list("user_id")[:1]
    # Los folios anteriores a la bitácora de captura guardaban al autor como solicitante.
    autor = next(iter(registro), (solicitud.solicitante_id,))[0]
    return autor == user.pk and (
        puede_gestionar_compras_departamentales(user)
        or _areas_usuario(user).filter(pk=solicitud.area_id).exists()
    )


def solicitud_sin_atencion(solicitud):
    if solicitud.estado not in (solicitud.ESTADO_BORRADOR, solicitud.ESTADO_ENVIADA) or solicitud.comprador_asignado_id:
        return False
    if solicitud.eventos.exclude(tipo="ENVIO_SOLICITUD").exists():
        return False
    return not solicitud.items.filter(
        ~Q(estado=ItemCompraDepartamental.ESTADO_POR_REVISAR)
        | ~Q(monto_gastado=0)
        | Q(cotizaciones__isnull=False)
        | Q(lineas_orden__isnull=False)
        | Q(intentos_compra__isnull=False)
        | Q(compras_realizadas__isnull=False)
        | Q(compromisos__isnull=False)
        | Q(destinos_documentales__isnull=False)
    ).exists()


def bloquear_items_solicitud(pk):
    # Las operaciones de cotización y recepción bloquean los artículos antes que el folio.
    return list(
        ItemCompraDepartamental.objects.select_for_update()
        .filter(solicitud_id=pk).order_by("pk").values_list("pk", flat=True)
    )


def retirar_solicitud(solicitud, *, actor, motivo):
    borrador = solicitud.estado == solicitud.ESTADO_BORRADOR
    solicitud.estado = solicitud.ESTADO_CANCELADA
    solicitud.save(update_fields=["estado", "actualizado_en"])
    solicitud.items.update(
        estado=ItemCompraDepartamental.ESTADO_CANCELADO,
        siguiente_responsable=ItemCompraDepartamental.RESPONSABLE_NADIE,
        actualizado_en=solicitud.actualizado_en,
    )
    # Mantener reservado el folio evita reutilizarlo y conserva su trazabilidad.
    solicitud.eventos.create(
        actor=actor, tipo=ELIMINACION_BORRADOR if borrador else RETIRO_SOLICITUD, detalle=motivo,
    )
    return borrador
