"""Transiciones auditables de cancelación y reembolso de intentos de compra."""

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import logging

from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import UploadedFile
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from .models import (
    CompraRealizadaDepartamental,
    CompromisoCompraDepartamental,
    CotizacionCompraDepartamental,
    EventoCompraDepartamental,
    IntentoCompraDepartamental,
    ItemCompraDepartamental,
    RecepcionItemDepartamental,
    ReembolsoCompraDepartamental,
)

logger = logging.getLogger(__name__)


def _importe_positivo(value, *, nombre):
    try:
        importe = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValidationError({nombre: "Captura un importe válido."}) from None
    if not importe.is_finite() or importe <= 0:
        raise ValidationError({nombre: "El importe debe ser mayor que cero."})
    if importe.as_tuple().exponent < -2:
        raise ValidationError({nombre: "El importe debe tener como máximo dos decimales."})
    return importe


@contextmanager
def _transaccion_con_archivos():
    """Revierte archivos nuevos si la operación o el commit de BD fallan."""
    nuevos = []
    try:
        with transaction.atomic():
            yield nuevos
    except Exception:
        for storage, nombre in reversed(nuevos):
            try:
                storage.delete(nombre)
            except Exception:
                logger.exception("No se pudo limpiar el archivo de compra %s", nombre)
        raise


def _guardar_archivo_nuevo(modelo, campo_nombre, instancia, valor, nuevos):
    """El nombre devuelto por storage.save identifica solo el archivo creado aquí."""
    if not isinstance(valor, UploadedFile):
        return valor
    campo = modelo._meta.get_field(campo_nombre)
    candidato = campo.generate_filename(instancia, valor.name)
    nombre = campo.storage.save(candidato, valor, max_length=campo.max_length)
    nuevos.append((campo.storage, nombre))
    return nombre


def _fecha_valida(value, *, nombre):
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValidationError({nombre: "Captura una fecha válida."})
    if value > timezone.localdate():
        raise ValidationError({nombre: "La fecha no puede ser futura."})
    return value


def _bloquear_item_e_intento(intento):
    """Usa el mismo orden de bloqueos que orden/compra/recepción."""
    item_id = IntentoCompraDepartamental.objects.values_list("item_id", flat=True).get(pk=intento.pk)
    item = ItemCompraDepartamental.objects.select_for_update().select_related("solicitud").get(pk=item_id)
    bloqueado = IntentoCompraDepartamental.objects.select_for_update().get(pk=intento.pk)
    if bloqueado.item_id != item.pk:
        raise ValidationError("El intento cambió de artículo. Recarga y revisa el historial.")
    return item, bloqueado


def _compromiso(intento):
    return CompromisoCompraDepartamental.objects.select_for_update().filter(intento=intento).first()


def _liberar_compromiso(compromiso):
    if compromiso and compromiso.activo:
        compromiso.activo = False
        compromiso.liberado_en = timezone.now()
        compromiso.save(update_fields=["activo", "liberado_en"])


def cancelar_intento_compra(
    intento, *, version, motivo, detalle, actor,
    reembolso_solicitado_en=None, reembolso_solicitado=None,
    evidencia_solicitud_reembolso=None,
):
    with _transaccion_con_archivos() as nuevos:
        return _cancelar_intento_compra(
            intento, version=version, motivo=motivo, detalle=detalle, actor=actor,
            reembolso_solicitado_en=reembolso_solicitado_en,
            reembolso_solicitado=reembolso_solicitado,
            evidencia_solicitud_reembolso=evidencia_solicitud_reembolso,
            nuevos=nuevos,
        )


def _cancelar_intento_compra(
    intento, *, version, motivo, detalle, actor,
    reembolso_solicitado_en, reembolso_solicitado,
    evidencia_solicitud_reembolso, nuevos,
):
    item, intento = _bloquear_item_e_intento(intento)
    if intento.version != version:
        raise ValidationError("Otra persona actualizó este intento. Recarga y revisa los cambios.")
    if intento.estado != IntentoCompraDepartamental.ESTADO_VIGENTE:
        raise ValidationError("Solo se puede cancelar el intento vigente.")
    if motivo not in dict(IntentoCompraDepartamental.MOTIVO_CHOICES):
        raise ValidationError({"motivo": "Selecciona un motivo de cancelación válido."})
    if not isinstance(detalle, str) or not detalle.strip():
        raise ValidationError({"detalle": "Describe por qué se cancela el intento."})
    if RecepcionItemDepartamental.objects.filter(
        linea_orden__intento=intento, cantidad_recibida__gt=0,
    ).exists():
        raise ValidationError("Este intento tiene una recepción registrada; revisa la entrega antes de cancelarlo.")

    compra = CompraRealizadaDepartamental.objects.select_for_update().filter(intento=intento).first()
    compromiso = _compromiso(intento)
    if compra:
        fecha = _fecha_valida(reembolso_solicitado_en, nombre="reembolso_solicitado_en")
        importe = _importe_positivo(reembolso_solicitado, nombre="reembolso_solicitado")
        if importe > compra.importe_final:
            raise ValidationError({"reembolso_solicitado": "El reembolso no puede superar la compra pagada."})
        if compromiso is None or not compromiso.activo:
            raise ValidationError("La compra pagada no tiene un compromiso activo. Revisa su registro financiero antes de cancelar.")
        intento.estado = IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO
        intento.reembolso_solicitado_en = fecha
        intento.reembolso_solicitado = importe
        intento.evidencia_solicitud_reembolso = _guardar_archivo_nuevo(
            IntentoCompraDepartamental, "evidencia_solicitud_reembolso",
            intento, evidencia_solicitud_reembolso, nuevos,
        )
        campos = ["reembolso_solicitado_en", "reembolso_solicitado", "evidencia_solicitud_reembolso"]
    else:
        if any(value is not None for value in (
            reembolso_solicitado_en, reembolso_solicitado, evidencia_solicitud_reembolso,
        )):
            raise ValidationError("Un intento sin compra pagada no requiere solicitud de reembolso.")
        intento.estado = IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO
        _liberar_compromiso(compromiso)
        campos = []

    intento.motivo_cancelacion = motivo
    intento.detalle_cancelacion = detalle.strip()
    intento.cancelado_en = timezone.now()
    intento.cancelado_por = actor
    intento.version += 1
    intento.save(update_fields=[
        "estado", "motivo_cancelacion", "detalle_cancelacion", "cancelado_en",
        "cancelado_por", "version", "actualizado_en", *campos,
    ])
    CotizacionCompraDepartamental.objects.filter(pk=intento.cotizacion_id).update(seleccionada=False)
    item.estado = ItemCompraDepartamental.ESTADO_POR_COTIZAR
    item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
    item.comentario_reciente = (
        f"{intento.get_motivo_cancelacion_display()}: {intento.detalle_cancelacion}"
    )
    item.save(update_fields=["estado", "siguiente_responsable", "comentario_reciente", "actualizado_en"])
    item.solicitud.actualizar_estado_desde_items()
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo="INTENTO_CANCELADO",
        detalle=(f"Intento #{intento.numero}: {intento.get_motivo_cancelacion_display()}. "
                 f"{intento.detalle_cancelacion} Estado: {intento.get_estado_display()}."),
    )
    if compra:
        EventoCompraDepartamental.objects.create(
            solicitud=item.solicitud, item=item, actor=actor, tipo="REEMBOLSO_SOLICITADO",
            detalle=(f"Intento #{intento.numero}: solicitud del "
                     f"{intento.reembolso_solicitado_en:%Y-%m-%d} por "
                     f"${intento.reembolso_solicitado:.2f}."),
        )
    return intento


def registrar_reembolso_compra(
    intento, *, version, fecha, importe, actor, referencia="", comprobante=None,
):
    with _transaccion_con_archivos() as nuevos:
        return _registrar_reembolso_compra(
            intento, version=version, fecha=fecha, importe=importe, actor=actor,
            referencia=referencia, comprobante=comprobante, nuevos=nuevos,
        )


def _registrar_reembolso_compra(
    intento, *, version, fecha, importe, actor, referencia, comprobante, nuevos,
):
    item, intento = _bloquear_item_e_intento(intento)
    if intento.version != version:
        raise ValidationError("Otra persona actualizó este intento. Recarga y revisa el saldo.")
    if intento.estado != IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO:
        raise ValidationError("El intento no tiene un reembolso pendiente.")
    fecha = _fecha_valida(fecha, nombre="fecha")
    importe = _importe_positivo(importe, nombre="importe")
    recibido = intento.reembolsos.aggregate(total=Sum("importe"))["total"] or Decimal("0")
    saldo = (intento.reembolso_solicitado or Decimal("0")) - recibido
    if importe > saldo:
        raise ValidationError({"importe": "El reembolso supera el saldo solicitado."})
    compromiso = _compromiso(intento)
    if compromiso is None or not compromiso.activo:
        raise ValidationError("El intento no tiene un compromiso activo. Revisa su registro financiero.")
    comprobante = _guardar_archivo_nuevo(
        ReembolsoCompraDepartamental, "comprobante",
        ReembolsoCompraDepartamental(intento=intento), comprobante, nuevos,
    )
    reembolso = ReembolsoCompraDepartamental.objects.create(
        intento=intento, fecha=fecha, importe=importe,
        referencia=(referencia or "").strip(), comprobante=comprobante, registrado_por=actor,
    )
    intento.version += 1
    campos = ["version", "actualizado_en"]
    if importe == saldo:
        intento.estado = IntentoCompraDepartamental.ESTADO_REEMBOLSADO
        campos.append("estado")
        _liberar_compromiso(compromiso)
    intento.save(update_fields=campos)
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo="REEMBOLSO_RECIBIDO",
        detalle=f"Intento #{intento.numero}: reembolso ${importe}; saldo ${(saldo - importe)}.",
    )
    return reembolso


@transaction.atomic
def cancelar_articulo_definitivamente(item, *, motivo, actor):
    item = ItemCompraDepartamental.objects.select_for_update().select_related("solicitud").get(pk=item.pk)
    if not isinstance(motivo, str) or not motivo.strip():
        raise ValidationError({"motivo": "Explica por qué ya no se comprará el artículo."})
    if item.estado == ItemCompraDepartamental.ESTADO_CANCELADO:
        raise ValidationError("El artículo ya está cancelado.")
    intentos = list(IntentoCompraDepartamental.objects.select_for_update().filter(item=item))
    if any(intento.estado == IntentoCompraDepartamental.ESTADO_VIGENTE for intento in intentos):
        raise ValidationError("Cancela primero el intento de compra vigente.")
    if RecepcionItemDepartamental.objects.filter(
        linea_orden__item=item, cantidad_recibida__gt=0,
    ).exists():
        raise ValidationError("El artículo tiene una recepción registrada y no puede cancelarse.")
    if any(intento.saldo_reembolso > 0 for intento in intentos):
        raise ValidationError("Hay un reembolso pendiente; regístralo antes de cancelar el artículo.")
    item.estado = ItemCompraDepartamental.ESTADO_CANCELADO
    item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_NADIE
    item.comentario_reciente = motivo.strip()
    item.save(update_fields=["estado", "siguiente_responsable", "comentario_reciente", "actualizado_en"])
    item.solicitud.actualizar_estado_desde_items()
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo="ARTICULO_CANCELADO",
        detalle=motivo.strip(),
    )
    return item
