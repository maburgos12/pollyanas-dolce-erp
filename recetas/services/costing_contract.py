from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
from typing import Any

from recetas.models import LineaReceta, Receta, RecetaCostoSemanal, RecetaCostoVersion
from recetas.utils.costeo_snapshot import resolve_line_snapshot_cost
from recetas.utils.derived_product_presentations import get_total_cost, get_total_cost_map


Q6 = Decimal("0.000001")


class CostContext(str, Enum):
    CURRENT_LIVE = "CURRENT_LIVE"
    WEEKLY_SNAPSHOT = "WEEKLY_SNAPSHOT"
    MONTHLY_CLOSE = "MONTHLY_CLOSE"


@dataclass(frozen=True)
class CostResolution:
    context: CostContext
    total_cost: Decimal
    unit_cost: Decimal | None
    source: str
    source_period: str = ""
    unresolved: bool = False
    unresolved_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def effective_unit_cost(self) -> Decimal:
        return self.unit_cost if self.unit_cost is not None else self.total_cost


def _q6(value: Decimal | int | float | str | None) -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(Q6, rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0.000000")


def _cost_context(context: CostContext | str) -> CostContext:
    if isinstance(context, CostContext):
        return context
    return CostContext(str(context))


def _recipe_unit_cost(receta: Receta, total_cost: Decimal) -> Decimal:
    yield_qty = _q6(getattr(receta, "rendimiento_cantidad", None))
    if yield_qty > 0:
        return _q6(total_cost / yield_qty)
    return _q6(total_cost)


def resolve_line_cost(
    linea: LineaReceta,
    *,
    context: CostContext | str = CostContext.CURRENT_LIVE,
) -> CostResolution:
    """Resolve a recipe line cost through the single ERP/VPS costing contract."""
    resolved_context = _cost_context(context)
    if resolved_context != CostContext.CURRENT_LIVE:
        return CostResolution(
            context=resolved_context,
            total_cost=Decimal("0.000000"),
            unit_cost=None,
            source="UNSUPPORTED_LINE_CONTEXT",
            unresolved=True,
            unresolved_reason="Las lineas de receta solo se resuelven contra costo vivo VPS.",
        )

    unit_cost, source = resolve_line_snapshot_cost(linea)
    unit_cost_q = _q6(unit_cost)
    quantity = _q6(getattr(linea, "cantidad", None))
    if unit_cost_q <= 0:
        return CostResolution(
            context=resolved_context,
            total_cost=Decimal("0.000000"),
            unit_cost=None,
            source=source,
            unresolved=True,
            unresolved_reason=source,
        )

    return CostResolution(
        context=resolved_context,
        total_cost=_q6(unit_cost_q * quantity),
        unit_cost=unit_cost_q,
        source=source,
    )


def resolve_recipe_cost(
    receta: Receta,
    *,
    context: CostContext | str = CostContext.CURRENT_LIVE,
    period_start: date | None = None,
    week_start: date | None = None,
) -> CostResolution:
    resolved_context = _cost_context(context)
    if resolved_context == CostContext.CURRENT_LIVE:
        total_cost = _q6(get_total_cost(receta))
        if total_cost <= 0:
            return CostResolution(
                context=resolved_context,
                total_cost=Decimal("0.000000"),
                unit_cost=None,
                source="CURRENT_LIVE",
                unresolved=True,
                unresolved_reason="SIN_COSTO_VIVO",
            )
        return CostResolution(
            context=resolved_context,
            total_cost=total_cost,
            unit_cost=_recipe_unit_cost(receta, total_cost),
            source="CURRENT_LIVE",
        )

    if resolved_context == CostContext.WEEKLY_SNAPSHOT:
        filters: dict[str, Any] = {
            "scope_type": RecetaCostoSemanal.SCOPE_RECIPE,
            "receta": receta,
        }
        if week_start is not None:
            filters["week_start"] = week_start
        row = RecetaCostoSemanal.objects.filter(**filters).order_by("-week_start", "-id").first()
        if row is not None and _q6(row.costo_mp) > 0:
            return CostResolution(
                context=resolved_context,
                total_cost=_q6(row.costo_total),
                unit_cost=_q6(row.costo_mp),
                source="WEEKLY_SNAPSHOT",
                source_period=row.week_start.isoformat(),
            )

        version = (
            RecetaCostoVersion.objects.filter(
                receta=receta,
                fuente="WEEKLY_SNAPSHOT",
                costo_total__gt=0,
            )
            .order_by("-version_num", "-creado_en", "-id")
            .first()
        )
        if version is not None:
            unit = _q6(version.costo_por_unidad_rendimiento or version.costo_total)
            return CostResolution(
                context=resolved_context,
                total_cost=_q6(version.costo_total),
                unit_cost=unit,
                source="WEEKLY_SNAPSHOT_VERSION",
                source_period=version.creado_en.date().isoformat() if version.creado_en else "",
            )

        return CostResolution(
            context=resolved_context,
            total_cost=Decimal("0.000000"),
            unit_cost=None,
            source="WEEKLY_SNAPSHOT",
            unresolved=True,
            unresolved_reason="SIN_SNAPSHOT_SEMANAL",
        )

    if resolved_context == CostContext.MONTHLY_CLOSE:
        if period_start is None:
            raise ValueError("period_start es obligatorio para MONTHLY_CLOSE")
        from reportes.models import ProductoCostoOperativoMensual, RecetaCostoHistoricoMensual

        product_cost = ProductoCostoOperativoMensual.objects.filter(
            periodo=period_start,
            receta=receta,
        ).first()
        if product_cost is not None and _q6(product_cost.costo_fabricacion_unit) > 0:
            return CostResolution(
                context=resolved_context,
                total_cost=_q6(product_cost.costo_fabricacion_unit),
                unit_cost=_q6(product_cost.costo_fabricacion_unit),
                source="MONTHLY_OPERATING_CLOSE",
                source_period=period_start.isoformat(),
                metadata={"row_id": product_cost.id},
            )

        monthly_cost = RecetaCostoHistoricoMensual.objects.filter(
            periodo=period_start,
            receta=receta,
        ).first()
        if monthly_cost is not None:
            unit_cost = _q6(monthly_cost.costo_por_unidad_rendimiento or monthly_cost.costo_total)
            if unit_cost > 0:
                return CostResolution(
                    context=resolved_context,
                    total_cost=_q6(monthly_cost.costo_total),
                    unit_cost=unit_cost,
                    source="MONTHLY_HISTORICAL",
                    source_period=period_start.isoformat(),
                    metadata={"row_id": monthly_cost.id},
                )

        return CostResolution(
            context=resolved_context,
            total_cost=Decimal("0.000000"),
            unit_cost=None,
            source="MONTHLY_CLOSE",
            source_period=period_start.isoformat(),
            unresolved=True,
            unresolved_reason="SIN_CIERRE_MENSUAL",
        )

    raise ValueError(f"Contexto de costo no soportado: {resolved_context}")


def resolve_recipe_cost_map(
    recipe_ids: list[int] | set[int] | tuple[int, ...],
    *,
    context: CostContext | str = CostContext.CURRENT_LIVE,
) -> dict[int, CostResolution]:
    resolved_context = _cost_context(context)
    requested_ids = {int(recipe_id) for recipe_id in recipe_ids if int(recipe_id or 0) > 0}
    if not requested_ids:
        return {}
    if resolved_context != CostContext.CURRENT_LIVE:
        recetas = Receta.objects.filter(id__in=requested_ids).only("id")
        return {receta.id: resolve_recipe_cost(receta, context=resolved_context) for receta in recetas}

    costs = get_total_cost_map(requested_ids)
    result: dict[int, CostResolution] = {}
    for receta in Receta.objects.filter(id__in=requested_ids).only("id", "rendimiento_cantidad"):
        total_cost = _q6(costs.get(receta.id))
        if total_cost > 0:
            result[int(receta.id)] = CostResolution(
                context=resolved_context,
                total_cost=total_cost,
                unit_cost=_recipe_unit_cost(receta, total_cost),
                source="CURRENT_LIVE",
            )
        else:
            result[int(receta.id)] = CostResolution(
                context=resolved_context,
                total_cost=Decimal("0.000000"),
                unit_cost=None,
                source="CURRENT_LIVE",
                unresolved=True,
                unresolved_reason="SIN_COSTO_VIVO",
            )
    return result
