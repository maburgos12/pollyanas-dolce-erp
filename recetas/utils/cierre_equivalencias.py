from __future__ import annotations

from decimal import Decimal

from recetas.models import Receta, RecetaEquivalencia
from recetas.utils.derived_product_presentations import get_active_derived_relation


ZERO = Decimal("0")


def get_active_closure_equivalence(receta: Receta) -> RecetaEquivalencia | None:
    cached = getattr(receta, "_active_closure_equivalence_cache", None)
    if cached is not None:
        return cached
    equivalence = (
        RecetaEquivalencia.objects.select_related("receta_porcion", "receta_padre")
        .filter(receta_porcion=receta, activo=True)
        .order_by("id")
        .first()
    )
    setattr(receta, "_active_closure_equivalence_cache", equivalence)
    return equivalence


def resolve_closure_recipe_quantity(receta: Receta, quantity: Decimal):
    if receta.excluir_cierre:
        return None, ZERO, "", False, "EXCLUIDA"

    equivalence = get_active_closure_equivalence(receta)
    if equivalence is not None:
        factor = Decimal(str(equivalence.factor_conversion or 0))
        if factor <= 0:
            return receta, quantity, f"Equivalencia de cierre sin factor valido para {receta.nombre}", False, "DIRECTA"
        return equivalence.receta_padre, quantity / factor, "", equivalence.receta_padre_id != receta.id, "EQUIVALENCIA"

    relation = get_active_derived_relation(receta)
    if relation is None:
        return receta, quantity, "", False, "DIRECTA"
    units_per_parent = Decimal(str(relation.unidades_por_padre or 0))
    if units_per_parent <= 0:
        return receta, quantity, f"Relacion derivada sin unidades_por_padre para {receta.nombre}", False, "DIRECTA"
    return relation.receta_padre, quantity / units_per_parent, "", True, "PRESENTACION_DERIVADA"
