from __future__ import annotations

from contextvars import ContextVar
from decimal import Decimal, ROUND_HALF_UP
import re

from django.db.models import Q

from maestros.models import CostoInsumo, Insumo, UnidadMedida
from maestros.utils.canonical_catalog import canonical_member_ids
from recetas.models import LineaReceta, Receta, RecetaCostoVersion
from recetas.utils.normalizacion import normalizar_nombre


Q6 = Decimal("0.000001")
POINT_UNIT_ALIASES = {
    "g": "g",
    "gr": "g",
    "gramo": "g",
    "gramos": "g",
    "kg": "kg",
    "kilogramo": "kg",
    "kilogramos": "kg",
    "ml": "ml",
    "mililitro": "ml",
    "mililitros": "ml",
    "l": "lt",
    "lt": "lt",
    "lts": "lt",
    "litro": "lt",
    "litros": "lt",
    "pza": "pza",
    "pz": "pza",
    "pieza": "pza",
    "piezas": "pza",
    "unidad": "unidad",
    "unidades": "unidad",
    "u": "unidad",
    "gli": "GLI",
    "galon": "GLI",
    "galón": "GLI",
    "galones": "GLI",
    "gfn": "Gfn",
    "garafon": "Gfn",
    "garrafon": "Gfn",
    "garafón": "Gfn",
    "garrafón": "Gfn",
    "cja": "CJA",
    "caja": "CJA",
    "cajas": "CJA",
}
_ACTIVE_PREPARATION_RECIPE_IDS: ContextVar[tuple[int, ...]] = ContextVar(
    "_ACTIVE_PREPARATION_RECIPE_IDS",
    default=(),
)


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


def _normalize_point_unit(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _unit_from_point_raw(raw: dict | None, fallback_unit: UnidadMedida | None) -> UnidadMedida | None:
    if not isinstance(raw, dict):
        return fallback_unit
    raw_unit = _normalize_point_unit(raw.get("unit") or raw.get("unidad") or raw.get("rendimiento_unidad"))
    code = POINT_UNIT_ALIASES.get(raw_unit)
    if not code:
        return fallback_unit
    unit = UnidadMedida.objects.filter(codigo__iexact=code).first()
    if unit is None:
        return fallback_unit
    if fallback_unit is not None and not _compatible_units(unit, fallback_unit):
        return fallback_unit
    return unit


def _latest_canonical_cost_with_unit(insumo: Insumo) -> tuple[Decimal | None, UnidadMedida | None]:
    member_ids = canonical_member_ids(insumo)
    if not member_ids:
        return None, insumo.unidad_base
    latest = (
        CostoInsumo.objects.filter(insumo_id__in=member_ids)
        .order_by("-fecha", "-id")
        .first()
    )
    if latest is None or latest.costo_unitario is None:
        return None, insumo.unidad_base
    source_unit = _unit_from_point_raw(latest.raw, insumo.unidad_base)
    return _q6(latest.costo_unitario), source_unit


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


def preparation_recipe_matches_insumo(receta: Receta | None, insumo: Insumo | None) -> bool:
    if receta is None or insumo is None:
        return False

    point_code = (insumo.codigo_point or "").strip()
    recipe_point_code = (receta.codigo_point or "").strip()
    if point_code and recipe_point_code and point_code.upper() == recipe_point_code.upper():
        return True

    recipe_name = (receta.nombre_normalizado or normalizar_nombre(receta.nombre or "")).strip()
    insumo_names = {
        normalizar_nombre(raw or "")
        for raw in (insumo.nombre, insumo.nombre_point, insumo.nombre_normalizado)
        if normalizar_nombre(raw or "")
    }
    return bool(recipe_name and recipe_name in insumo_names)


def resolve_preparation_recipe_for_insumo(insumo: Insumo | None) -> Receta | None:
    if not insumo:
        return None

    qs = Receta.objects.filter(tipo=Receta.TIPO_PREPARACION)
    point_code = (insumo.codigo_point or "").strip()
    if point_code:
        receta = qs.filter(codigo_point__iexact=point_code).order_by("id").first()
        if receta:
            return receta

    derived_code = (insumo.codigo or "").strip()
    derived_match = re.match(r"^DERIVADO:RECETA:(\d+):PREPARACION$", derived_code)
    if derived_match:
        receta_id = int(derived_match.group(1))
        receta = qs.filter(id=receta_id).order_by("id").first()
        if preparation_recipe_matches_insumo(receta, insumo):
            return receta

    for raw_name in [insumo.nombre, insumo.nombre_point]:
        normalized_name = normalizar_nombre(raw_name or "")
        if not normalized_name:
            continue
        receta = qs.filter(nombre_normalizado=normalized_name).order_by("id").first()
        if receta:
            return receta
    return None


def resolve_preparation_recipe_unit_cost(prep_recipe: Receta | None) -> tuple[Decimal | None, UnidadMedida | None, str]:
    if prep_recipe is None:
        return None, None, "NO_PREPARACION"

    recipe_id = int(prep_recipe.id or 0)
    active_recipe_ids = _ACTIVE_PREPARATION_RECIPE_IDS.get()
    if recipe_id and recipe_id in active_recipe_ids:
        return None, prep_recipe.rendimiento_unidad, "RECETA_PREPARACION_CYCLE"

    token = _ACTIVE_PREPARATION_RECIPE_IDS.set(
        (*active_recipe_ids, recipe_id) if recipe_id else active_recipe_ids
    )
    try:
        unit_cost = prep_recipe.costo_por_unidad_rendimiento
    except RecursionError:
        return None, prep_recipe.rendimiento_unidad, "RECETA_PREPARACION_CYCLE"
    finally:
        _ACTIVE_PREPARATION_RECIPE_IDS.reset(token)

    quantized_cost = _q6(unit_cost)
    if quantized_cost > 0:
        return quantized_cost, prep_recipe.rendimiento_unidad, "RECETA_PREPARACION"

    # BOM sin costo — usar POINT_PRODUCTION_REPORT como respaldo
    production_report_cost = (
        RecetaCostoVersion.objects.filter(
            receta=prep_recipe,
            fuente="POINT_PRODUCTION_REPORT",
            costo_total__gt=0,
        )
        .order_by("-version_num", "-creado_en", "-id")
        .values_list("costo_total", flat=True)
        .first()
    )
    if production_report_cost is not None:
        return _q6(production_report_cost), prep_recipe.rendimiento_unidad, "POINT_PRODUCTION_REPORT"

    return None, prep_recipe.rendimiento_unidad, "RECETA_PREPARACION_SIN_COSTO"


def resolve_insumo_unit_cost(insumo: Insumo | None) -> tuple[Decimal | None, UnidadMedida | None, str]:
    if not insumo:
        return None, None, "NO_INSUMO"

    # Preparaciones internas: el costo BOM (o POINT_PRODUCTION_REPORT como respaldo) es correcto.
    # Insumos de compra: no tendrán prep_recipe, van directamente a CostoInsumo.
    prep_recipe = resolve_preparation_recipe_for_insumo(insumo)
    prep_cost, prep_unit, prep_label = resolve_preparation_recipe_unit_cost(prep_recipe)
    if prep_cost is not None and prep_cost > 0:
        return prep_cost, prep_unit, prep_label

    latest, source_unit = _latest_canonical_cost_with_unit(insumo)
    if latest is not None and latest > 0:
        return _q6(latest), source_unit, "COSTO_CANONICO"

    if prep_recipe is not None:
        return None, prep_unit, prep_label

    return None, None, "SIN_COSTO"


def resolve_line_snapshot_cost(linea: LineaReceta) -> tuple[Decimal | None, str]:
    if not linea.insumo_id or linea.insumo is None:
        return None, "NO_INSUMO"

    line_snapshot_cost = _q6(getattr(linea, "costo_unitario_snapshot", None))
    unit_cost, source_unit, source_label = resolve_insumo_unit_cost(linea.insumo)
    if unit_cost is None or unit_cost <= 0:
        if line_snapshot_cost > 0:
            return line_snapshot_cost, "LINEA_SNAPSHOT"
        return None, source_label

    target_unit = linea.unidad
    if target_unit is None and source_label == "RECETA_PREPARACION":
        # En líneas históricas sin unidad explícita de preparaciones internas,
        # la cantidad suele venir en la unidad de rendimiento de la receta base.
        target_unit = source_unit
    if target_unit is None:
        target_unit = linea.insumo.unidad_base or source_unit
    if target_unit is None or source_unit is None:
        if line_snapshot_cost > 0:
            return line_snapshot_cost, f"{source_label}_SIN_UNIDAD_LINEA_SNAPSHOT"
        return None, f"{source_label}_SIN_UNIDAD"

    converted = convert_unit_cost(unit_cost, source_unit=source_unit, target_unit=target_unit)
    if converted is not None and converted > 0:
        return converted, source_label

    if source_unit.id == target_unit.id:
        return unit_cost, source_label

    if line_snapshot_cost > 0:
        return line_snapshot_cost, f"{source_label}_UNIDAD_INCOMPATIBLE_LINEA_SNAPSHOT"

    return None, f"{source_label}_UNIDAD_INCOMPATIBLE"
