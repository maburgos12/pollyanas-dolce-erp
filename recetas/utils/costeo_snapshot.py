from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from django.db.models import Q

from maestros.models import Insumo, UnidadMedida
from maestros.utils.canonical_catalog import latest_costo_canonico
from recetas.models import LineaReceta, Receta
from recetas.utils.normalizacion import normalizar_nombre


Q6 = Decimal("0.000001")


def _q6(value: Decimal | int | float | str | None) -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(Q6, rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.000000")


def _unit_factor(unit: UnidadMedida | None) -> Decimal | None:
    if unit is None:
        return None
    try:
        return Decimal(str(unit.factor_to_base or 0))
    except Exception:
        return None


def _compatible_units(source_unit: UnidadMedida | None, target_unit: UnidadMedida | None) -> bool:
    if source_unit is None or target_unit is None:
        return False
    return (source_unit.tipo or "").strip().upper() == (target_unit.tipo or "").strip().upper()


def convert_unit_cost(
    unit_cost: Decimal | int | float | str | None,
    *,
    source_unit: UnidadMedida | None,
    target_unit: UnidadMedida | None,
) -> Decimal | None:
    cost = _q6(unit_cost)
    if cost <= 0:
        return None
    if source_unit is None or target_unit is None:
        return None
    if source_unit.id == target_unit.id:
        return cost
    if not _compatible_units(source_unit, target_unit):
        return None

    source_factor = _unit_factor(source_unit)
    target_factor = _unit_factor(target_unit)
    if not source_factor or not target_factor or source_factor <= 0 or target_factor <= 0:
        return None

    return _q6(cost * (target_factor / source_factor))


def resolve_preparation_recipe_for_insumo(insumo: Insumo | None) -> Receta | None:
    if not insumo:
        return None

    qs = Receta.objects.filter(tipo=Receta.TIPO_PREPARACION)
    if (insumo.codigo_point or "").strip():
        receta = qs.filter(codigo_point__iexact=(insumo.codigo_point or "").strip()).order_by("id").first()
        if receta:
            return receta

    normalized_name = normalizar_nombre(insumo.nombre_point or insumo.nombre or "")
    if not normalized_name:
        return None
    return qs.filter(nombre_normalizado=normalized_name).order_by("id").first()


def resolve_insumo_unit_cost(insumo: Insumo | None) -> tuple[Decimal | None, UnidadMedida | None, str]:
    if not insumo:
        return None, None, "NO_INSUMO"

    prep_recipe = resolve_preparation_recipe_for_insumo(insumo)
    if prep_recipe and prep_recipe.costo_por_unidad_rendimiento and prep_recipe.costo_por_unidad_rendimiento > 0:
        return _q6(prep_recipe.costo_por_unidad_rendimiento), prep_recipe.rendimiento_unidad, "RECETA_PREPARACION"

    latest = latest_costo_canonico(insumo)
    if latest is not None and latest > 0:
        return _q6(latest), insumo.unidad_base, "COSTO_CANONICO"

    return None, None, "SIN_COSTO"


def resolve_line_snapshot_cost(linea: LineaReceta) -> tuple[Decimal | None, str]:
    if not linea.insumo_id or linea.insumo is None:
        return None, "NO_INSUMO"

    unit_cost, source_unit, source_label = resolve_insumo_unit_cost(linea.insumo)
    if unit_cost is None or unit_cost <= 0:
        return None, source_label

    target_unit = linea.unidad or linea.insumo.unidad_base or source_unit
    if target_unit is None or source_unit is None:
        return None, f"{source_label}_SIN_UNIDAD"

    converted = convert_unit_cost(unit_cost, source_unit=source_unit, target_unit=target_unit)
    if converted is not None and converted > 0:
        return converted, source_label

    if source_unit.id == target_unit.id:
        return unit_cost, source_label

    return None, f"{source_label}_UNIDAD_INCOMPATIBLE"
