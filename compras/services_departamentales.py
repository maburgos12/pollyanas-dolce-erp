from dataclasses import dataclass
from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q, Sum
from django.utils import timezone

from reportes.models import LineaPresupuestoMensual

from .models import (
    CompromisoCompraDepartamental,
    CotizacionCompraDepartamental,
    EventoCompraDepartamental,
    ItemCompraDepartamental,
    LineaOrdenCompraDepartamental,
    OrdenCompraDepartamental,
)


@dataclass(frozen=True)
class EvaluacionPresupuesto:
    presupuesto: Decimal | None
    gasto_real: Decimal | None
    compromisos_previos: Decimal | None
    disponible_antes: Decimal | None
    costo_seleccionado: Decimal
    disponible_despues: Decimal | None
    exceso: Decimal | None

    @property
    def presupuesto_asignado(self):
        return self.presupuesto is not None

    @property
    def calculable(self):
        return self.disponible_antes is not None

    @property
    def requiere_dg(self):
        return not self.calculable or self.exceso > 0


def _lineas_presupuesto(item):
    if item.rubro_id is None:
        return LineaPresupuestoMensual.objects.none()
    qs = LineaPresupuestoMensual.objects.filter(
        rubro_id=item.rubro_id,
        rubro__activo=True,
        rubro__area=item.solicitud.area,
        periodo=item.solicitud.periodo,
    )
    rubros_revisados = qs.filter(version=LineaPresupuestoMensual.VERSION_REVISADO).values("rubro_id")
    return qs.filter(
        Q(version=LineaPresupuestoMensual.VERSION_REVISADO)
        | (Q(version=LineaPresupuestoMensual.VERSION_ORIGINAL) & ~Q(rubro_id__in=rubros_revisados))
    )


def evaluar_presupuesto_item(item: ItemCompraDepartamental, costo: Decimal) -> EvaluacionPresupuesto:
    lineas = _lineas_presupuesto(item)
    totales = lineas.aggregate(presupuesto=Sum("monto_presupuesto"), real=Sum("monto_real"))
    # Ausencia de partida y gasto real desconocido no equivalen a cero.
    presupuesto = totales["presupuesto"]
    gasto_real = totales["real"]
    compromisos = None
    if presupuesto is not None:
        compromisos = (
            CompromisoCompraDepartamental.objects.filter(
                activo=True,
                item__rubro_id=item.rubro_id,
                item__solicitud__area=item.solicitud.area,
                item__solicitud__periodo=item.solicitud.periodo,
            )
            .exclude(item=item)
            .aggregate(total=Sum("monto"))["total"]
            or Decimal("0")
        )
    calculable = presupuesto is not None and gasto_real is not None
    disponible_antes = presupuesto - gasto_real - compromisos if calculable else None
    disponible_despues = disponible_antes - costo if calculable else None
    return EvaluacionPresupuesto(
        presupuesto=presupuesto,
        gasto_real=gasto_real,
        compromisos_previos=compromisos,
        disponible_antes=disponible_antes,
        costo_seleccionado=costo,
        disponible_despues=disponible_despues,
        exceso=max(-disponible_despues, Decimal("0")) if calculable else None,
    )


@transaction.atomic
def seleccionar_cotizacion(cotizacion: CotizacionCompraDepartamental, *, actor):
    item = ItemCompraDepartamental.objects.select_for_update().select_related("solicitud__area").get(pk=cotizacion.item_id)
    cotizacion = CotizacionCompraDepartamental.objects.select_for_update().get(pk=cotizacion.pk)
    from .services_edicion_compra import tiene_compra_o_recepcion
    if item.estado in ('ORDENADO', 'COMPRADO', 'RECIBIDO_PARCIAL', 'PENDIENTE_CONFIRMACION', 'RECIBIDO_CONFORME', 'RECHAZADO', 'CANCELADO') or tiene_compra_o_recepcion(item) or LineaOrdenCompraDepartamental.objects.filter(item=item).exists():
        raise ValidationError("Este artículo ya no admite cambiar la cotización seleccionada.")
    revision_pendiente = (item.estado in ('ESPERANDO_DG', 'POSPUESTO', 'FINANCIAMIENTO')
        and item.cotizaciones.filter(historial__isnull=False).exists())
    CotizacionCompraDepartamental.objects.filter(item=item).update(seleccionada=False)
    cotizacion.seleccionada = True
    cotizacion.save(update_fields=["seleccionada"])
    resultado = evaluar_presupuesto_item(item, cotizacion.total_adquisicion)
    if resultado.requiere_dg or revision_pendiente:
        CompromisoCompraDepartamental.objects.filter(item=item).update(activo=False, liberado_en=timezone.now())
        item.estado = ItemCompraDepartamental.ESTADO_ESPERANDO_DG
        item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_DG
    else:
        CompromisoCompraDepartamental.objects.update_or_create(
            item=item,
            defaults={
                "cotizacion": cotizacion,
                "monto": cotizacion.total_adquisicion,
                "activo": True,
                "formalizado_en": None,
                "liberado_en": None,
            },
        )
        item.estado = ItemCompraDepartamental.ESTADO_AUTORIZADO
        item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
    item.save(update_fields=["estado", "siguiente_responsable", "actualizado_en"])
    item.solicitud.actualizar_estado_desde_items()
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud,
        item=item,
        actor=actor,
        tipo="COTIZACION_SELECCIONADA",
        detalle=f"{cotizacion.proveedor}: ${cotizacion.total_adquisicion}",
    )
    return resultado


@transaction.atomic
def decidir_exceso(item: ItemCompraDepartamental, *, decision: str, comentario: str, actor, cotizacion_id=None, version=None):
    from .services_edicion_compra import tiene_compra_o_recepcion, sincronizar_linea_orden
    item = ItemCompraDepartamental.objects.select_for_update().get(pk=item.pk)
    if tiene_compra_o_recepcion(item) or item.estado not in ('ESPERANDO_DG', 'POSPUESTO', 'FINANCIAMIENTO'):
        raise ValidationError("El artículo no tiene una decisión de Dirección General pendiente.")
    decisiones = {
        "AUTORIZAR": (ItemCompraDepartamental.ESTADO_AUTORIZADO, ItemCompraDepartamental.RESPONSABLE_COMPRAS),
        "POSPONER": (ItemCompraDepartamental.ESTADO_POSPUESTO, ItemCompraDepartamental.RESPONSABLE_DG),
        "FINANCIAR": (ItemCompraDepartamental.ESTADO_FINANCIAMIENTO, ItemCompraDepartamental.RESPONSABLE_DG),
        "RECHAZAR": (ItemCompraDepartamental.ESTADO_RECHAZADO, ItemCompraDepartamental.RESPONSABLE_NADIE),
    }
    if decision not in decisiones:
        raise ValidationError("Decisión de Dirección General inválida.")
    if decision != "AUTORIZAR" and not comentario.strip():
        raise ValidationError("La decisión requiere un comentario.")
    cotizacion = item.cotizaciones.filter(seleccionada=True).first()
    if not cotizacion:
        raise ValidationError("Selecciona una cotización antes de decidir.")
    if (cotizacion_id is not None or version is not None) and (cotizacion.pk != cotizacion_id or cotizacion.version != version):
        raise ValidationError("La cotización cambió desde que abriste la decisión. Recarga y revisa la versión actual antes de autorizar.")
    item.estado, item.siguiente_responsable = decisiones[decision]
    linea = None
    if decision == "AUTORIZAR":
        linea = sincronizar_linea_orden(item, cotizacion, actor=actor)
        if linea:
            item.estado = ItemCompraDepartamental.ESTADO_ORDENADO
    item.comentario_reciente = comentario
    item.save(update_fields=["estado", "siguiente_responsable", "comentario_reciente", "actualizado_en"])
    item.solicitud.actualizar_estado_desde_items()
    if decision == "AUTORIZAR":
        CompromisoCompraDepartamental.objects.update_or_create(
            item=item,
            defaults={
                "cotizacion": cotizacion,
                "monto": cotizacion.total_adquisicion,
                "activo": True,
                "formalizado_en": timezone.now() if linea else None,
                "liberado_en": None,
            },
        )
    else:
        CompromisoCompraDepartamental.objects.filter(item=item).update(activo=False, liberado_en=timezone.now())
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo=f"DG_{decision}", detalle=comentario
    )


@transaction.atomic
def generar_ordenes_departamentales(items, *, actor):
    grupos = {}
    for original in items:
        item = ItemCompraDepartamental.objects.select_for_update().select_related("solicitud").get(pk=original.pk)
        from .services_edicion_compra import tiene_compra_o_recepcion
        if tiene_compra_o_recepcion(item) or LineaOrdenCompraDepartamental.objects.filter(item=item).exists():
            raise ValidationError(f"{item.descripcion} ya tiene una orden, compra o entrega registrada.")
        cotizacion = item.cotizaciones.select_related("proveedor").filter(seleccionada=True).first()
        if not cotizacion:
            raise ValidationError(f"{item.descripcion} no tiene cotización seleccionada.")
        if item.estado != ItemCompraDepartamental.ESTADO_AUTORIZADO:
            raise ValidationError(f"{item.descripcion} aún no está autorizado.")
        grupos.setdefault(cotizacion.proveedor_id, []).append((item, cotizacion, original))

    ordenes = []
    for proveedor_id, lineas in grupos.items():
        orden = OrdenCompraDepartamental.objects.create(proveedor_id=proveedor_id, creado_por=actor)
        for item, cotizacion, original in lineas:
            LineaOrdenCompraDepartamental.objects.create(
                orden=orden,
                item=item,
                cotizacion=cotizacion,
                cantidad=item.cantidad,
                costo_unitario=cotizacion.costo_unitario,
                total=cotizacion.total_adquisicion,
            )
            CompromisoCompraDepartamental.objects.filter(item=item, activo=True).update(
                formalizado_en=timezone.now()
            )
            item.estado = ItemCompraDepartamental.ESTADO_ORDENADO
            item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
            item.save(update_fields=["estado", "siguiente_responsable", "actualizado_en"])
            item.solicitud.actualizar_estado_desde_items()
            original.estado = item.estado
            original.siguiente_responsable = item.siguiente_responsable
            EventoCompraDepartamental.objects.create(
                solicitud=item.solicitud, item=item, actor=actor, tipo="ORDEN_GENERADA", detalle=orden.folio
            )
        ordenes.append(orden)
    return ordenes


@transaction.atomic
def confirmar_recepcion_departamental(item: ItemCompraDepartamental, *, conforme: bool, actor, comentario: str = ""):
    item = ItemCompraDepartamental.objects.select_for_update().get(pk=item.pk)
    if item.estado != ItemCompraDepartamental.ESTADO_PENDIENTE_CONFIRMACION:
        raise ValidationError("El artículo aún no está listo para confirmación del área.")
    if conforme:
        item.estado = ItemCompraDepartamental.ESTADO_RECIBIDO_CONFORME
        item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_NADIE
        tipo = "RECEPCION_CONFORME"
    else:
        if not comentario.strip():
            raise ValidationError("Describe la diferencia encontrada.")
        item.estado = ItemCompraDepartamental.ESTADO_ESPERANDO_AREA
        item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
        tipo = "RECEPCION_CON_DIFERENCIAS"
    item.comentario_reciente = comentario
    item.save(update_fields=["estado", "siguiente_responsable", "comentario_reciente", "actualizado_en"])
    item.solicitud.actualizar_estado_desde_items()
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo=tipo, detalle=comentario
    )
