from decimal import Decimal, InvalidOperation

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from maestros.models import Insumo

from .models import ExistenciaInsumo


ALMACEN_DEFAULT = "ALMACEN_1"


def get_or_create_existencia(insumo: Insumo, almacen: str = ALMACEN_DEFAULT):
    return ExistenciaInsumo.objects.get_or_create(insumo=insumo, almacen=almacen)[0]


@transaction.atomic
def establecer_stock(insumo: Insumo, almacen: str, cantidad) -> ExistenciaInsumo:
    try:
        cantidad_decimal = Decimal(str(cantidad))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError(
            f"El stock de {almacen} debe ser una cantidad válida.",
            code="stock_invalido",
        ) from exc
    if not cantidad_decimal.is_finite() or cantidad_decimal < 0:
        raise ValidationError(
            f"El stock de {almacen} debe ser una cantidad finita mayor o igual a cero.",
            code="stock_invalido",
        )

    existencia, _ = ExistenciaInsumo.objects.select_for_update().get_or_create(
        insumo=insumo,
        almacen=almacen,
    )
    existencia.stock_actual = cantidad_decimal
    existencia.actualizado_en = timezone.now()
    existencia.save(update_fields=["stock_actual", "actualizado_en"])
    return existencia


@transaction.atomic
def aplicar_delta(insumo: Insumo, almacen: str, delta, *, permitir_negativo: bool = False) -> ExistenciaInsumo:
    try:
        delta_decimal = Decimal(str(delta))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError(
            f"El delta de {almacen} debe ser una cantidad válida.",
            code="delta_invalido",
        ) from exc
    if not delta_decimal.is_finite():
        raise ValidationError(
            f"El delta de {almacen} debe ser una cantidad finita.",
            code="delta_invalido",
        )
    existencia, _ = ExistenciaInsumo.objects.select_for_update().get_or_create(
        insumo=insumo,
        almacen=almacen,
    )
    nuevo_saldo = Decimal(str(existencia.stock_actual or 0)) + delta_decimal
    # permitir_negativo: el consumo automático por venta Point puede correr antes
    # de la apertura de stock del insumo; los flujos manuales siguen estrictos.
    if nuevo_saldo < 0 and not permitir_negativo:
        raise ValidationError(
            f"Stock insuficiente en {almacen}: saldo resultante {nuevo_saldo}.",
            code="stock_negativo",
        )

    existencia.stock_actual = nuevo_saldo
    existencia.actualizado_en = timezone.now()
    existencia.save(update_fields=["stock_actual", "actualizado_en"])
    return existencia


def stock_ubicacion(insumo: Insumo, almacen: str) -> Decimal:
    stock = (
        ExistenciaInsumo.objects.filter(insumo=insumo, almacen=almacen)
        .values_list("stock_actual", flat=True)
        .first()
    )
    return Decimal(str(stock or 0))
