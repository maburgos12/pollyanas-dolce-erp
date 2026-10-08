from datetime import datetime
import logging
from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from crm.models import PedidoCliente, PointOrderLink
from crm.services.point_order_link import _note_snapshot
from pos_bridge.services.point_note_detail_service import PointNoteContractError, PointNoteDetailService
from pos_bridge.services.point_special_order_service import PointSpecialOrderService


logger = logging.getLogger(__name__)


def point_document(order):
    return getattr(order, 'point_order_link', None)


def point_snapshot(order):
    link = point_document(order)
    return order.point_note_snapshot or (link.snapshot if link else {})


def has_verified_point(order):
    link = point_document(order)
    return bool(link.is_active and not link.last_error) if link else bool(order.point_note_id)


def _set_final_note(order, snapshot):
    if not snapshot:
        return
    if order.point_note_id:
        if order.point_note_id != snapshot['pk_nota']:
            raise ValidationError('El pedido ya tiene otra nota de venta final.')
        return
    if PedidoCliente.objects.filter(point_note_id=snapshot['pk_nota']).exclude(pk=order.pk).exists():
        raise ValidationError('La nota final ya respalda otro pedido. Requiere revisión; no se fusionará automáticamente.')
    order.backfill_point_note_provenance(
        point_note_id=snapshot['pk_nota'], point_note_folio=snapshot['folio'],
        point_note_snapshot=snapshot, point_note_fetched_at=timezone.now())


def _confirm_final_note_delivery(order, actor):
    from logistica.models import SolicitudDomicilio
    from logistica.services_domicilio_status import transition_domicilio_status

    if not order.point_note_id or not has_verified_point(order):
        return
    with transaction.atomic():
        delivery = SolicitudDomicilio.objects.select_for_update().filter(pedido_cliente=order).first()
        if delivery and delivery.estatus == SolicitudDomicilio.ESTATUS_PENDIENTE_POINT:
            transition_domicilio_status(solicitud_id=delivery.pk,
                requested_status=SolicitudDomicilio.ESTATUS_CONFIRMADO, audit_user=actor)


def link_web_point(*, order, kind, point_id, delivery_date, actor, snapshot=None):
    if order.external_source != 'POLLYANAS_ECOMMERCE' or order.canal != 'WEB':
        raise ValidationError('Este vínculo requiere el pedido WEB original.')
    if snapshot is None:
        if kind == 'SPECIAL':
            snapshot = PointSpecialOrderService().fetch(pk_pedido=point_id, delivery_date=delivery_date)
        else:
            snapshot = _note_snapshot(PointNoteDetailService().fetch(pk_nota=point_id))
    if Decimal(snapshot['total']) != order.monto_estimado:
        raise ValidationError('El total Point difiere del total WEB. Revisa el documento seleccionado.')
    if kind == 'SPECIAL':
        if snapshot['pk_pedido'] != point_id or snapshot['cancelado'] or Decimal(snapshot['restante']) != 0:
            raise ValidationError('El pedido especial debe estar liquidado y vigente.')
        committed = timezone.localtime(datetime.fromisoformat(snapshot['fecha_entrega'])).date()
    else:
        if snapshot['pk_nota'] != point_id:
            raise ValidationError('La nota no coincide con la identidad seleccionada.')
        committed = timezone.localtime(datetime.fromisoformat(snapshot['sold_at'])).date()
    if delivery_date != committed or not order.fecha_compromiso or committed != order.fecha_compromiso:
        raise ValidationError('La fecha Point difiere de la entrega WEB. Revisa el documento seleccionado.')
    with transaction.atomic():
        order = PedidoCliente.objects.select_for_update().get(pk=order.pk)
        link = PointOrderLink.objects.filter(order=order).first()
        if link:
            if (link.kind, link.point_id) != (kind, point_id):
                raise ValidationError('El pedido WEB ya tiene otro documento Point vinculado.')
            _confirm_final_note_delivery(order, actor)
            return link
        if PointOrderLink.objects.filter(kind=kind, point_id=point_id).exists():
            raise ValidationError('El documento Point ya respalda otro pedido WEB.')
        link = PointOrderLink.objects.create(order=order, kind=kind, point_id=point_id,
                                             snapshot=snapshot, checked_at=timezone.now())
        final = snapshot.get('nota_final') if kind == 'SPECIAL' else snapshot
        _set_final_note(order, final)
        _confirm_final_note_delivery(order, actor)
        if kind == 'SPECIAL':
            from logistica.models import SolicitudDomicilio
            delivery = SolicitudDomicilio.objects.get(pedido_cliente=order)
            scheduled = datetime.fromisoformat(snapshot['fecha_entrega'])
            delivery.ventana_inicio = scheduled
            delivery.ventana_fin = scheduled
            delivery.save(update_fields=['ventana_inicio', 'ventana_fin'])
        log_event(actor, 'link_point_document', 'crm.PedidoCliente', str(order.pk),
                  {'kind': kind, 'point_id': point_id, 'folio': snapshot['folio']})
        return link


def refresh_special_links():
    """Run before automatic note intake so final tickets keep their WEB identity."""
    errors = 0
    links = PointOrderLink.objects.filter(kind='SPECIAL', order__point_note_id='').select_related('order')
    for link in links:
        try:
            day = datetime.fromisoformat(link.snapshot['fecha_entrega']).date()
            fresh = PointSpecialOrderService().fetch(pk_pedido=link.point_id, delivery_date=day)
            if Decimal(fresh['total']) != link.order.monto_estimado:
                raise ValidationError('El total Point cambió; requiere revisión.')
            with transaction.atomic():
                order = PedidoCliente.objects.select_for_update().get(pk=link.order_id)
                _set_final_note(order, fresh.get('nota_final'))
                link.is_active = not fresh['cancelado'] and Decimal(fresh['restante']) == 0
                link.last_error = ''
                link.checked_at = timezone.now()
                link.save(update_fields=['is_active', 'last_error', 'checked_at'])
                _confirm_final_note_delivery(order, None)
        except Exception as exc:
            # Preserve the source snapshot; any uncertain document blocks intake.
            logger.warning('Special Point verification failed link=%s error=%s', link.pk, type(exc).__name__)
            link.last_error = 'No se pudo verificar el pedido especial; requiere revisión en Point.'
            errors += 1
            link.checked_at = timezone.now()
            link.save(update_fields=['last_error', 'checked_at'])
    return errors
