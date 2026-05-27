from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP

from maestros.models import Insumo, UnidadMedida
from recetas.utils.costeo_snapshot import convert_unit_cost, resolve_insumo_unit_cost

Q2 = Decimal("0.01")
Q6 = Decimal("0.000001")


@dataclass(frozen=True)
class SimulatedLineCost:
    insumo: Insumo
    cantidad: Decimal
    unit: UnidadMedida | None
    unit_cost: Decimal
    line_cost: Decimal
    source: str
    unresolved: bool = False


@dataclass(frozen=True)
class SalePriceSuggestion:
    unit_cost: Decimal
    target_margin_pct: Decimal
    raw_price: Decimal
    suggested_price: Decimal


def q2(value: Decimal | int | float | str | None) -> Decimal:
    return Decimal(str(value or 0)).quantize(Q2, rounding=ROUND_HALF_UP)


def q6(value: Decimal | int | float | str | None) -> Decimal:
    return Decimal(str(value or 0)).quantize(Q6, rounding=ROUND_HALF_UP)


def suggest_sale_price(
    *,
    unit_cost: Decimal | int | float | str | None,
    target_margin_pct: Decimal | int | float | str | None = Decimal("55"),
    rounding_increment: Decimal | int | float | str | None = Decimal("5"),
) -> SalePriceSuggestion:
    cost = q6(unit_cost)
    margin_pct = q2(target_margin_pct)
    if margin_pct < Decimal("1"):
        margin_pct = Decimal("1.00")
    if margin_pct > Decimal("90"):
        margin_pct = Decimal("90.00")

    increment = q2(rounding_increment)
    if increment <= 0:
        increment = Decimal("1.00")

    if cost <= 0:
        return SalePriceSuggestion(
            unit_cost=Decimal("0.000000"),
            target_margin_pct=margin_pct,
            raw_price=Decimal("0.000000"),
            suggested_price=Decimal("0.00"),
        )

    raw_price = (cost / (Decimal("1") - (margin_pct / Decimal("100")))).quantize(
        Q6,
        rounding=ROUND_HALF_UP,
    )
    suggested = (raw_price / increment).to_integral_value(rounding=ROUND_CEILING) * increment
    return SalePriceSuggestion(
        unit_cost=cost,
        target_margin_pct=margin_pct,
        raw_price=raw_price,
        suggested_price=q2(suggested),
    )


def calculate_line_cost(
    *,
    insumo: Insumo,
    cantidad: Decimal | int | float | str | None,
    unidad: UnidadMedida | None = None,
) -> SimulatedLineCost:
    qty = q6(cantidad)
    source_cost, source_unit, source_label = resolve_insumo_unit_cost(insumo)
    target_unit = unidad or source_unit or insumo.unidad_base
    unit_cost = Decimal("0.000000")
    unresolved = False

    if source_cost is None or source_cost <= 0:
        unresolved = True
    elif source_unit is None or target_unit is None:
        unresolved = True
    else:
        converted = convert_unit_cost(source_cost, source_unit=source_unit, target_unit=target_unit)
        if converted is not None and converted > 0:
            unit_cost = q6(converted)
        elif source_unit.id == target_unit.id:
            unit_cost = q6(source_cost)
        else:
            unresolved = True

    return SimulatedLineCost(
        insumo=insumo,
        cantidad=qty,
        unit=target_unit,
        unit_cost=unit_cost,
        line_cost=q6(unit_cost * qty),
        source=source_label,
        unresolved=unresolved,
    )
