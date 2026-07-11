from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.utils import timezone

from core.access import can_manage_module
from inventario.models import ALMACEN_LABELS, LoteProduccion, MovimientoInventario, normalizar_codigo_lote
from inventario.services_existencias import aplicar_delta
from maestros.models import Insumo, UnidadMedida
from recetas.models import Receta
from recetas.utils.costeo_snapshot import preparation_recipe_matches_insumo


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
        return normalized, normalized.isoformat()
    if not isinstance(fecha_elaboracion, date):
        raise ValidationError("La fecha de elaboración no es válida.")
    value = timezone.make_aware(datetime.combine(fecha_elaboracion, time(hour=12)))
    return value, value.isoformat()


def _source_hash_apertura(insumo: Insumo, ubicacion: str) -> str:
    raw = f"bitacoras:apertura:{insumo.pk}:{ubicacion}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _validar_apertura_existente(
    movimiento: MovimientoInventario,
    *,
    receta: Receta,
    cantidad: Decimal,
    unidad: UnidadMedida,
    observaciones: str,
    fecha_normalizada: str | None,
) -> LoteProduccion:
    lote = movimiento.lote
    if not lote or any(
        (
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
            cantidad=cantidad_decimal,
            unidad=unidad,
            observaciones=observaciones_limpias,
            fecha_normalizada=fecha_normalizada,
        )

    try:
        with transaction.atomic():
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
    except IntegrityError:
        movimiento = MovimientoInventario.objects.select_related("lote").get(source_hash=source_hash)
        return _validar_apertura_existente(
            movimiento,
            receta=receta,
            cantidad=cantidad_decimal,
            unidad=unidad,
            observaciones=observaciones_limpias,
            fecha_normalizada=fecha_normalizada,
        )
