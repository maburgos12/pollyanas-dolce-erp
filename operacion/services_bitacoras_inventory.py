from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import NamedTuple

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.utils import timezone

from core.access import can_manage_module
from inventario.models import (
    ALMACEN_LABELS,
    ExistenciaInsumo,
    LoteProduccion,
    MovimientoInventario,
    normalizar_codigo_lote,
    UBICACION_CFP_1_1,
)
from inventario.services_existencias import aplicar_delta
from maestros.models import Insumo, UnidadMedida
from recetas.models import Receta
from recetas.utils.costeo_snapshot import preparation_recipe_matches_insumo
from operacion.models import BitacoraOperativa


class CierreHornosResult(NamedTuple):
    lotes_creados: int


def _cantidad_positiva(cantidad) -> Decimal:
    try:
        value = Decimal(str(cantidad))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("La cantidad debe ser numérica.") from exc
    if not value.is_finite() or value <= 0:
        raise ValidationError("La cantidad debe ser finita y mayor que cero.")
    return value


def _fecha_operativa(fecha_elaboracion: date | datetime | None) -> tuple[datetime, str | None]:
    if fecha_elaboracion is None:
        return timezone.now(), None
    if isinstance(fecha_elaboracion, datetime):
        value = timezone.make_aware(fecha_elaboracion) if timezone.is_naive(fecha_elaboracion) else fecha_elaboracion
        normalized = value.astimezone(timezone.get_current_timezone())
        if normalized.date() > timezone.localdate():
            raise ValidationError("La fecha de elaboración no puede ser futura.")
        return normalized, normalized.isoformat()
    if not isinstance(fecha_elaboracion, date):
        raise ValidationError("La fecha de elaboración no es válida.")
    if fecha_elaboracion > timezone.localdate():
        raise ValidationError("La fecha de elaboración no puede ser futura.")
    value = timezone.make_aware(datetime.combine(fecha_elaboracion, time(hour=12)))
    return value, value.isoformat()


def _source_hash_apertura(insumo: Insumo, ubicacion: str) -> str:
    raw = f"bitacoras:apertura:{insumo.pk}:{ubicacion}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _validar_apertura_existente(
    movimiento: MovimientoInventario,
    *,
    receta: Receta,
    insumo: Insumo,
    cantidad: Decimal,
    unidad: UnidadMedida,
    observaciones: str,
    fecha_normalizada: str | None,
) -> LoteProduccion:
    lote = movimiento.lote
    if not lote or any(
        (
            not lote.es_apertura,
            lote.insumo_id != insumo.pk,
            movimiento.insumo_id != insumo.pk,
            movimiento.tipo != MovimientoInventario.TIPO_ENTRADA,
            movimiento.almacen != movimiento.trazabilidad.get("ubicacion"),
            lote.receta_id != receta.pk,
            lote.cantidad_inicial != cantidad,
            lote.unidad_id != unidad.pk,
            lote.observaciones != observaciones,
            movimiento.trazabilidad.get("fecha_elaboracion") != fecha_normalizada,
        )
    ):
        raise ValidationError(
            "La apertura de este producto y ubicación ya fue aplicada y no puede editarse."
        )
    return lote


def registrar_apertura_inicial(
    *,
    receta: Receta,
    insumo: Insumo,
    cantidad,
    unidad: UnidadMedida,
    ubicacion: str,
    fecha_elaboracion: date | datetime | None,
    actor,
    observaciones: str,
) -> LoteProduccion:
    if not can_manage_module(actor, "produccion"):
        raise PermissionDenied("Se requiere permiso de gestión de Producción.")
    cantidad_decimal = _cantidad_positiva(cantidad)
    observaciones_limpias = (observaciones or "").strip()
    if not observaciones_limpias:
        raise ValidationError("La observación de apertura es obligatoria.")
    if ubicacion not in ALMACEN_LABELS:
        raise ValidationError("Selecciona una ubicación válida.")
    if not normalizar_codigo_lote(insumo.codigo_point) or not normalizar_codigo_lote(receta.codigo_point):
        raise ValidationError("El producto requiere identidad canónica de Point.")
    if receta.tipo != Receta.TIPO_PREPARACION or not preparation_recipe_matches_insumo(receta, insumo):
        raise ValidationError("El producto y su receta canónica no corresponden.")
    if not insumo.unidad_base_id or unidad.pk != insumo.unidad_base_id:
        raise ValidationError("La unidad debe coincidir con la unidad base del producto.")

    producido_en, fecha_normalizada = _fecha_operativa(fecha_elaboracion)
    source_hash = _source_hash_apertura(insumo, ubicacion)

    movimiento_existente = MovimientoInventario.objects.select_related("lote").filter(source_hash=source_hash).first()
    if movimiento_existente:
        return _validar_apertura_existente(
            movimiento_existente,
            receta=receta,
            insumo=insumo,
            cantidad=cantidad_decimal,
            unidad=unidad,
            observaciones=observaciones_limpias,
            fecha_normalizada=fecha_normalizada,
        )

    try:
        with transaction.atomic():
            movimiento_existente = (
                MovimientoInventario.objects.select_for_update()
                .filter(source_hash=source_hash)
                .first()
            )
            if movimiento_existente:
                return _validar_apertura_existente(
                    movimiento_existente,
                    receta=receta,
                    insumo=insumo,
                    cantidad=cantidad_decimal,
                    unidad=unidad,
                    observaciones=observaciones_limpias,
                    fecha_normalizada=fecha_normalizada,
                )
            existencia, _ = ExistenciaInsumo.objects.select_for_update().get_or_create(
                insumo=insumo,
                almacen=ubicacion,
            )
            if Decimal(str(existencia.stock_actual or 0)) != Decimal("0"):
                raise ValidationError(
                    "La ubicación ya tiene saldo; primero debes conciliarlo o registrar un ajuste autorizado."
                )
            lote = LoteProduccion.objects.create(
                insumo=insumo,
                receta=receta,
                cantidad_inicial=cantidad_decimal,
                unidad=unidad,
                producido_en=producido_en,
                linea_origen=None,
                creado_por=actor,
                es_apertura=True,
                observaciones=observaciones_limpias,
            )
            MovimientoInventario.objects.create(
                fecha=timezone.now(),
                tipo=MovimientoInventario.TIPO_ENTRADA,
                insumo=insumo,
                cantidad=cantidad_decimal,
                referencia=lote.codigo,
                almacen=ubicacion,
                notas=observaciones_limpias,
                registrado_por=actor.get_username(),
                source_hash=source_hash,
                lote=lote,
                linea_bitacora=None,
                registrado_por_usuario=actor,
                trazabilidad={
                    "evento": "apertura_inicial",
                    "lote": lote.codigo,
                    "ubicacion": ubicacion,
                    "fecha_elaboracion": fecha_normalizada,
                    "sin_fuente_historica": True,
                },
            )
            aplicar_delta(insumo, ubicacion, cantidad_decimal)
            return lote
    except IntegrityError as original_error:
        movimiento = MovimientoInventario.objects.select_related("lote").filter(source_hash=source_hash).first()
        if movimiento is None:
            raise
        try:
            return _validar_apertura_existente(
                movimiento,
                receta=receta,
                insumo=insumo,
                cantidad=cantidad_decimal,
                unidad=unidad,
                observaciones=observaciones_limpias,
                fecha_normalizada=fecha_normalizada,
            )
        except ValidationError:
            raise original_error


def _resolve_insumo_from_receta(receta: Receta) -> Insumo | None:
    """Resolver el insumo derivado desde una receta PREPARACION por codigo_point."""
    if not receta or receta.tipo != Receta.TIPO_PREPARACION:
        return None
    punto = (receta.codigo_point or "").strip().upper()
    if not punto:
        return None
    return Insumo.objects.filter(
        tipo_item=Insumo.TIPO_INTERNO,
        codigo_point__iexact=punto,
    ).first()


def cerrar_hornos(bitacora: BitacoraOperativa, actor) -> CierreHornosResult:
    if bitacora.tipo != BitacoraOperativa.TIPO_HORNOS:
        raise ValidationError("La bitacora no corresponde a Hornos.")

    with transaction.atomic():
        bitacora_locked = BitacoraOperativa.objects.select_for_update().get(pk=bitacora.pk)
        lineas = bitacora_locked.lineas.select_related("receta").all()

        lotes_creados = 0
        for linea in lineas:
            if linea.receta is None or not normalizar_codigo_lote(linea.receta.codigo_point):
                raise ValidationError("La linea requiere una receta con codigo Point.")
            if linea.receta.rendimiento_unidad_id is None:
                raise ValidationError("La preparacion requiere unidad de rendimiento.")

            insumo = _resolve_insumo_from_receta(linea.receta)
            if not insumo:
                raise ValidationError(f"No se encontró el insumo derivado de {linea.receta.nombre}.")

            cantidad = Decimal(str(linea.datos.get("existencia") or 0))
            source_hash = sha256(
                f"BITACORA:HORNOS:{linea.id}:ENTRADA:{UBICACION_CFP_1_1}".encode()
            ).hexdigest()

            movimiento, movimiento_nuevo = MovimientoInventario.objects.select_for_update().get_or_create(
                linea_bitacora=linea,
                almacen=UBICACION_CFP_1_1,
                defaults={
                    "fecha": timezone.now(),
                    "tipo": MovimientoInventario.TIPO_ENTRADA,
                    "insumo": insumo,
                    "cantidad": cantidad,
                    "referencia": "",
                    "notas": linea.observaciones,
                    "registrado_por": actor.get_username(),
                    "source_hash": source_hash,
                    "registrado_por_usuario": actor,
                    "trazabilidad": {
                        "evento": "cierre_hornos",
                        "bitacora_id": bitacora_locked.pk,
                        "linea_id": linea.id,
                    },
                },
            )

            if movimiento_nuevo:
                lote, lote_nuevo = LoteProduccion.objects.select_for_update().get_or_create(
                    linea_origen=linea,
                    defaults={
                        "insumo": insumo,
                        "receta": linea.receta,
                        "cantidad_inicial": cantidad,
                        "unidad": linea.receta.rendimiento_unidad,
                        "producido_en": timezone.make_aware(
                            datetime.combine(bitacora_locked.fecha, time(12, 0))
                        ),
                        "creado_por": actor,
                        "estado": LoteProduccion.DISPONIBLE,
                    },
                )
                movimiento.lote = lote
                movimiento.referencia = lote.codigo
                movimiento.save(update_fields=["lote", "referencia"])

                if lote_nuevo:
                    aplicar_delta(insumo, UBICACION_CFP_1_1, cantidad)
                    lotes_creados += 1

        bitacora_locked.estatus = BitacoraOperativa.ESTATUS_CERRADA
        bitacora_locked.cerrado_en = timezone.now()
        bitacora_locked.actualizado_en = timezone.now()
        bitacora_locked.save(update_fields=["estatus", "cerrado_en", "actualizado_en"])

        return CierreHornosResult(lotes_creados=lotes_creados)
