from __future__ import annotations

from decimal import Decimal

from django.db.models import Q
from django.db.models import DecimalField

from recetas.models import LineaReceta, Receta, RecetaCostoVersion, RecetaPresentacionDerivada
from recetas.utils.costeo_snapshot import (
    convert_unit_cost,
    preparation_recipe_matches_insumo,
    resolve_insumo_unit_cost,
    resolve_preparation_recipe_unit_cost,
)
from recetas.utils.normalizacion import normalizar_nombre


ZERO = Decimal("0")
LINE_COST_OUTPUT_FIELD = DecimalField(max_digits=24, decimal_places=6)


def _prioritized_version_cost_map(recipe_ids: set[int]) -> dict[int, Decimal]:
    if not recipe_ids:
        return {}

    costs: dict[int, Decimal] = {}
    # Los reportes de produccion de Point son costo unitario de producto terminado.
    # Otros snapshots pueden representar capturas intermedias y no siempre son
    # equivalentes para dividir presentaciones derivadas.
    for version in (
        RecetaCostoVersion.objects.filter(
            receta_id__in=recipe_ids,
            fuente="POINT_PRODUCTION_REPORT",
            costo_total__gt=0,
        ).order_by("receta_id", "-version_num", "-creado_en", "-id")
    ):
        if version.receta_id not in costs:
            costs[int(version.receta_id)] = Decimal(str(version.costo_total or 0))
    return costs


def _prioritized_version_cost(receta: Receta) -> Decimal | None:
    return _prioritized_version_cost_map({int(receta.id)}).get(int(receta.id))


def _resolve_line_total_cost(linea: LineaReceta) -> Decimal:
    if linea.tipo_linea == LineaReceta.TIPO_SUBSECCION:
        return ZERO

    if linea.insumo_id:
        quantity = Decimal(str(linea.cantidad or 0))
        if quantity <= 0:
            return ZERO

        unit_cost = Decimal(str(linea.costo_unitario_snapshot or 0))
        if unit_cost <= 0:
            from recetas.utils.costeo_snapshot import resolve_line_snapshot_cost

            resolved_cost, _ = resolve_line_snapshot_cost(linea)
            unit_cost = Decimal(str(resolved_cost or 0))
        if unit_cost <= 0:
            return ZERO
        return quantity * unit_cost

    return Decimal(str(linea.costo_linea_excel or 0))


def get_active_derived_relation(receta: Receta) -> RecetaPresentacionDerivada | None:
    cached = getattr(receta, "_active_derived_relation_cache", None)
    if cached is not None:
        return cached
    relation = (
        RecetaPresentacionDerivada.objects.select_related("receta_padre", "receta_derivada")
        .filter(receta_derivada=receta, activo=True)
        .order_by("id")
        .first()
    )
    setattr(receta, "_active_derived_relation_cache", relation)
    return relation


def get_direct_components_cost(receta: Receta) -> Decimal:
    total = ZERO
    for linea in receta.lineas.exclude(match_status=LineaReceta.STATUS_REJECTED).all():
        total += _resolve_line_total_cost(linea)
    return total


def get_parent_unit_cost(receta: Receta) -> Decimal | None:
    relation = get_active_derived_relation(receta)
    if relation is None:
        return None
    units = Decimal(str(relation.unidades_por_padre or 0))
    if units <= 0:
        return None
    parent_total = _prioritized_version_cost(relation.receta_padre)
    if parent_total is None or parent_total <= 0:
        parent_total = relation.receta_padre.costo_total_estimado_decimal
    if parent_total <= 0:
        return None
    return parent_total / units


def get_total_cost(receta: Receta) -> Decimal:
    direct_cost = get_direct_components_cost(receta)
    parent_unit_cost = get_parent_unit_cost(receta)
    if parent_unit_cost is None:
        return direct_cost
    return direct_cost + parent_unit_cost


def build_upstream_snapshot(receta: Receta) -> dict[str, object] | None:
    relation = get_active_derived_relation(receta)
    if relation is None:
        return None
    parent_unit_cost = get_parent_unit_cost(receta) or ZERO
    direct_cost = get_direct_components_cost(receta)
    return {
        "relation_id": relation.id,
        "tipo": relation.tipo_derivado,
        "parent_recipe_id": relation.receta_padre_id,
        "parent_recipe_name": relation.receta_padre.nombre,
        "parent_recipe_code": relation.receta_padre.codigo_point,
        "units_per_parent": Decimal(str(relation.unidades_por_padre or 0)),
        "requires_direct_components": bool(relation.requiere_componentes_directos),
        "parent_unit_cost": parent_unit_cost,
        "direct_components_cost": direct_cost,
        "notes": relation.notas,
    }


def get_total_cost_map(recipe_ids: list[int] | set[int] | tuple[int, ...]) -> dict[int, Decimal]:
    requested_ids = {int(recipe_id) for recipe_id in recipe_ids if int(recipe_id or 0) > 0}
    if not requested_ids:
        return {}

    discovered_ids = set(requested_ids)
    frontier = set(requested_ids)
    direct_cost_by_recipe: dict[int, Decimal] = {}
    relation_by_recipe: dict[int, tuple[int, Decimal]] = {}
    insumo_cost_cache: dict[int, tuple[Decimal | None, object | None, str]] = {}

    def prime_insumo_cost_cache(lineas: list[LineaReceta]) -> None:
        missing_insumos = {
            int(linea.insumo_id): linea.insumo
            for linea in lineas
            if linea.insumo_id and int(linea.insumo_id) not in insumo_cost_cache and linea.insumo is not None
        }
        if not missing_insumos:
            return

        derived_recipe_ids: set[int] = set()
        point_codes: set[str] = set()
        normalized_names: set[str] = set()
        for insumo in missing_insumos.values():
            derived_code = (insumo.codigo or "").strip()
            if derived_code.startswith("DERIVADO:RECETA:") and derived_code.endswith(":PREPARACION"):
                parts = derived_code.split(":")
                if len(parts) >= 3 and parts[2].isdigit():
                    derived_recipe_ids.add(int(parts[2]))
            if (insumo.codigo_point or "").strip():
                point_codes.add((insumo.codigo_point or "").strip())
            for raw_name in (insumo.nombre, insumo.nombre_point):
                normalized_name = normalizar_nombre(raw_name or "")
                if normalized_name:
                    normalized_names.add(normalized_name)

        prep_q = Q()
        if derived_recipe_ids:
            prep_q |= Q(id__in=sorted(derived_recipe_ids))
        if point_codes:
            prep_q |= Q(codigo_point__in=sorted(point_codes))
        if normalized_names:
            prep_q |= Q(nombre_normalizado__in=sorted(normalized_names))
        prep_recipes = (
            Receta.objects.filter(tipo=Receta.TIPO_PREPARACION)
            .filter(prep_q)
            .select_related("rendimiento_unidad")
            if prep_q
            else Receta.objects.none()
        )
        prep_by_id = {recipe.id: recipe for recipe in prep_recipes}
        prep_by_code = {
            (recipe.codigo_point or "").strip().upper(): recipe
            for recipe in prep_by_id.values()
            if (recipe.codigo_point or "").strip()
        }
        prep_by_name = {
            (recipe.nombre_normalizado or "").strip(): recipe
            for recipe in prep_by_id.values()
            if (recipe.nombre_normalizado or "").strip()
        }

        for insumo_id, insumo in missing_insumos.items():
            prep_recipe = None
            if (insumo.codigo_point or "").strip():
                prep_recipe = prep_by_code.get((insumo.codigo_point or "").strip().upper())
            if prep_recipe is None:
                for raw_name in (insumo.nombre, insumo.nombre_point):
                    normalized_name = normalizar_nombre(raw_name or "")
                    if normalized_name:
                        prep_recipe = prep_by_name.get(normalized_name)
                    if prep_recipe is not None:
                        break
            if prep_recipe is None:
                derived_code = (insumo.codigo or "").strip()
                if derived_code.startswith("DERIVADO:RECETA:") and derived_code.endswith(":PREPARACION"):
                    parts = derived_code.split(":")
                    if len(parts) >= 3 and parts[2].isdigit():
                        candidate = prep_by_id.get(int(parts[2]))
                        if preparation_recipe_matches_insumo(candidate, insumo):
                            prep_recipe = candidate
            prep_cost, prep_unit, prep_label = resolve_preparation_recipe_unit_cost(prep_recipe)
            if prep_cost is not None and prep_cost > 0:
                insumo_cost_cache[insumo_id] = (
                    Decimal(str(prep_cost)),
                    prep_unit,
                    prep_label,
                )
                continue
            insumo_cost_cache[insumo_id] = resolve_insumo_unit_cost(insumo)

    def resolve_line_total_cost_cached(linea: LineaReceta) -> Decimal:
        if linea.tipo_linea == LineaReceta.TIPO_SUBSECCION:
            return ZERO
        if not linea.insumo_id:
            return Decimal(str(linea.costo_linea_excel or 0))

        quantity = Decimal(str(linea.cantidad or 0))
        if quantity <= 0:
            return ZERO

        unit_cost = Decimal(str(linea.costo_unitario_snapshot or 0))
        if unit_cost <= 0:
            cached = insumo_cost_cache.get(int(linea.insumo_id))
            if cached is None:
                cached = resolve_insumo_unit_cost(linea.insumo)
                insumo_cost_cache[int(linea.insumo_id)] = cached
            resolved_cost, source_unit, _source_label = cached
            if resolved_cost is None or resolved_cost <= 0:
                return ZERO
            target_unit = linea.unidad or linea.insumo.unidad_base or source_unit
            if target_unit is None or source_unit is None:
                return ZERO
            converted = convert_unit_cost(
                resolved_cost,
                source_unit=source_unit,
                target_unit=target_unit,
            )
            if converted is not None and converted > 0:
                unit_cost = Decimal(str(converted))
            elif getattr(source_unit, "id", None) == getattr(target_unit, "id", None):
                unit_cost = Decimal(str(resolved_cost))
        if unit_cost <= 0:
            return ZERO
        return quantity * unit_cost

    while frontier:
        lineas = list(
            LineaReceta.objects.filter(receta_id__in=frontier)
            .exclude(match_status=LineaReceta.STATUS_REJECTED)
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .select_related("insumo", "unidad", "insumo__unidad_base")
        )
        prime_insumo_cost_cache(lineas)
        line_totals: dict[int, Decimal] = {}
        for linea in lineas:
            recipe_id = int(linea.receta_id)
            line_totals.setdefault(recipe_id, ZERO)
            line_totals[recipe_id] += resolve_line_total_cost_cached(linea)
        direct_cost_by_recipe.update(line_totals)

        next_frontier: set[int] = set()
        for row in (
            RecetaPresentacionDerivada.objects.filter(receta_derivada_id__in=frontier, activo=True)
            .order_by("receta_derivada_id", "id")
            .values("receta_derivada_id", "receta_padre_id", "unidades_por_padre")
        ):
            recipe_id = int(row["receta_derivada_id"])
            if recipe_id in relation_by_recipe:
                continue
            parent_id = int(row["receta_padre_id"])
            units = Decimal(str(row["unidades_por_padre"] or 0))
            relation_by_recipe[recipe_id] = (parent_id, units)
            if parent_id not in discovered_ids:
                discovered_ids.add(parent_id)
                next_frontier.add(parent_id)
        frontier = next_frontier

    memo: dict[int, Decimal] = {}
    visiting: set[int] = set()
    version_cost_by_parent = _prioritized_version_cost_map({parent_id for parent_id, _units in relation_by_recipe.values()})

    def resolve(recipe_id: int) -> Decimal:
        if recipe_id in memo:
            return memo[recipe_id]
        if recipe_id in visiting:
            return direct_cost_by_recipe.get(recipe_id, ZERO)
        visiting.add(recipe_id)
        total = direct_cost_by_recipe.get(recipe_id, ZERO)
        relation = relation_by_recipe.get(recipe_id)
        if relation:
            parent_id, units = relation
            if units > ZERO:
                parent_total = version_cost_by_parent.get(parent_id)
                if parent_total is None or parent_total <= ZERO:
                    parent_total = resolve(parent_id)
                if parent_total > ZERO:
                    total += parent_total / units
        visiting.discard(recipe_id)
        memo[recipe_id] = total
        return total

    return {recipe_id: resolve(recipe_id) for recipe_id in requested_ids}
