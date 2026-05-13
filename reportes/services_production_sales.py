from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from django.db.models import Sum

from pos_bridge.models import PointDailySale, PointSalesDailyProductFact
from reportes.models import FactProduccionDiaria

ZERO = Decimal("0")


class SalesPeriod(Protocol):
    month_start: object
    month_end: object


def _decimal(value) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value or 0))


def _aggregate_by_recipe(queryset, field_name: str, recipe_field: str = "receta_id") -> dict[int, Decimal]:
    rows = queryset.values(recipe_field).annotate(total=Sum(field_name))
    return {
        int(row[recipe_field]): _decimal(row["total"])
        for row in rows
        if row.get(recipe_field)
    }


def build_sales_map(period: SalesPeriod, sucursal_id: int | None) -> tuple[dict[int, Decimal], str]:
    sales = PointDailySale.objects.filter(
        sale_date__gte=period.month_start,
        sale_date__lte=period.month_end,
        receta_id__isnull=False,
    )
    if sucursal_id:
        sales = sales.filter(branch__erp_branch_id=sucursal_id)
    if sales.exists():
        sales_map = _aggregate_by_recipe(sales, "quantity")
        source = "PointDailySale"
        if not sucursal_id:
            source = _fill_global_fallbacks(
                sales_map,
                period,
                primary_source=source,
                fallbacks=[
                    (
                        FactProduccionDiaria.objects.filter(
                            fecha__gte=period.month_start,
                            fecha__lte=period.month_end,
                            receta_id__isnull=False,
                            vendido__gt=0,
                        ),
                        "vendido",
                        "FactProduccionDiaria",
                    ),
                    (
                        PointSalesDailyProductFact.objects.filter(
                            sale_date__gte=period.month_start,
                            sale_date__lte=period.month_end,
                            receta_id__isnull=False,
                        ),
                        "total_cantidad",
                        "PointSalesDailyProductFact",
                    ),
                ],
            )
        return sales_map, source

    facts = PointSalesDailyProductFact.objects.filter(
        sale_date__gte=period.month_start,
        sale_date__lte=period.month_end,
        receta_id__isnull=False,
    )
    if sucursal_id:
        facts = facts.filter(branch__erp_branch_id=sucursal_id)
    if facts.exists():
        sales_map = _aggregate_by_recipe(facts, "total_cantidad")
        source = "PointSalesDailyProductFact"
        if not sucursal_id:
            source = _fill_global_fallbacks(
                sales_map,
                period,
                primary_source=source,
                fallbacks=[
                    (
                        FactProduccionDiaria.objects.filter(
                            fecha__gte=period.month_start,
                            fecha__lte=period.month_end,
                            receta_id__isnull=False,
                            vendido__gt=0,
                        ),
                        "vendido",
                        "FactProduccionDiaria",
                    ),
                    (
                        PointDailySale.objects.filter(
                            sale_date__gte=period.month_start,
                            sale_date__lte=period.month_end,
                            receta_id__isnull=False,
                        ),
                        "quantity",
                        "PointDailySale",
                    ),
                ],
            )
        return sales_map, source

    production = FactProduccionDiaria.objects.filter(
        fecha__gte=period.month_start,
        fecha__lte=period.month_end,
        receta_id__isnull=False,
        vendido__gt=0,
    )
    if sucursal_id:
        production = production.filter(sucursal_id=sucursal_id)
    if production.exists():
        return _aggregate_by_recipe(production, "vendido"), "FactProduccionDiaria"

    return {}, "Sin ventas"


def _fill_global_fallbacks(
    sales_map: dict[int, Decimal],
    period: SalesPeriod,
    *,
    primary_source: str,
    fallbacks,
) -> str:
    source_parts = [primary_source]
    for fallback_qs, field_name, source_name in fallbacks:
        fallback_map = _aggregate_by_recipe(fallback_qs, field_name)
        filled = 0
        for receta_id, value in fallback_map.items():
            if receta_id not in sales_map or sales_map[receta_id] == ZERO:
                sales_map[receta_id] = value
                filled += 1
        if filled:
            source_parts.append(f"{source_name}({filled})")
    return "+".join(source_parts)
