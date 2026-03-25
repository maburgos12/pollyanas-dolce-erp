from __future__ import annotations

from decimal import Decimal

from recetas.models import Receta, RecetaPresentacionDerivada


ZERO = Decimal("0")


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
    for linea in receta.lineas.all():
        total += Decimal(str(linea.costo_total_estimado or 0))
    return total


def get_parent_unit_cost(receta: Receta) -> Decimal | None:
    relation = get_active_derived_relation(receta)
    if relation is None:
        return None
    units = Decimal(str(relation.unidades_por_padre or 0))
    if units <= 0:
        return None
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
