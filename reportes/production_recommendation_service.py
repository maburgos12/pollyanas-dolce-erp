from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation

from django.db.models import OuterRef, Subquery, Sum
from django.utils import timezone

from pos_bridge.models import PointInventorySnapshot
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import Receta
from reportes.forecast_service import build_daily_forecast_context
from reportes.models import FactProduccionDiaria, FactVentaDiaria


ZERO = Decimal("0")
SHELF_LIFE_DAYS = 2


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _normalize_top_n(top_n: int | None, *, default: int = 24) -> int | None:
    if top_n is None:
        return None
    if top_n <= 0:
        return None
    return max(top_n, default)


def _load_latest_stock_by_recipe_branch(keys: set[tuple[int, int]]) -> dict[tuple[int, int], Decimal]:
    if not keys:
        return {}
    branch_ids = sorted({branch_id for branch_id, _ in keys})
    latest_snapshot_id = (
        PointInventorySnapshot.objects.filter(
            branch_id=OuterRef("branch_id"),
            product_id=OuterRef("product_id"),
        )
        .order_by("-captured_at", "-id")
        .values("id")[:1]
    )
    snapshots = list(
        PointInventorySnapshot.objects.select_related("product", "branch__erp_branch")
        .filter(branch_id__in=branch_ids, id=Subquery(latest_snapshot_id))
        .order_by("branch_id", "product_id")
    )
    if not snapshots:
        return {}

    product_ids = sorted({snapshot.product_id for snapshot in snapshots})
    product_recipe_map = dict(
        FactVentaDiaria.objects.filter(point_product_id__in=product_ids, receta_id__isnull=False)
        .order_by("point_product_id", "-fecha")
        .distinct("point_product_id")
        .values_list("point_product_id", "receta_id")
    )
    missing_product_ids = [product_id for product_id in product_ids if product_id not in product_recipe_map]
    if missing_product_ids:
        matcher = PointSalesMatchingService()
        for snapshot in snapshots:
            if snapshot.product_id not in missing_product_ids:
                continue
            receta = matcher.resolve_receta(
                codigo_point=snapshot.product.sku,
                point_name=snapshot.product.name,
            )
            if receta is not None:
                product_recipe_map[snapshot.product_id] = receta.id

    stock_map: dict[tuple[int, int], Decimal] = defaultdict(lambda: ZERO)
    for snapshot in snapshots:
        branch_id = getattr(snapshot.branch, "erp_branch_id", None)
        recipe_id = product_recipe_map.get(snapshot.product_id)
        if not branch_id or not recipe_id:
            continue
        key = (int(branch_id), int(recipe_id))
        if key not in keys:
            continue
        stock_map[key] += _to_decimal(snapshot.stock)
    return dict(stock_map)


def _load_production_history(keys: set[tuple[int, int]], target_date: date) -> dict[tuple[int, int], dict[str, Decimal]]:
    if not keys:
        return {}
    branch_ids = sorted({branch_id for branch_id, _ in keys})
    recipe_ids = sorted({recipe_id for _, recipe_id in keys})
    rows = (
        FactProduccionDiaria.objects.filter(
            fecha__lt=target_date,
            fecha__gte=target_date.fromordinal(target_date.toordinal() - 28),
            sucursal_id__in=branch_ids,
            receta_id__in=recipe_ids,
        )
        .values("sucursal_id", "receta_id")
        .annotate(
            produced=Sum("producido"),
            sold=Sum("vendido"),
            waste=Sum("merma"),
        )
    )
    return {
        (int(row["sucursal_id"]), int(row["receta_id"])): {
            "produced": _to_decimal(row.get("produced")),
            "sold": _to_decimal(row.get("sold")),
            "waste": _to_decimal(row.get("waste")),
        }
        for row in rows
    }


def build_production_recommendation_context(
    *,
    target_date: date | None = None,
    forecast_context: dict[str, object] | None = None,
    top_n: int | None = 12,
) -> dict[str, object]:
    target_date = target_date or timezone.localdate()
    if target_date.weekday() == 6:
        return {
            "target_date": target_date.isoformat(),
            "target_label": target_date.strftime("%d %b %Y"),
            "shelf_life_days": SHELF_LIFE_DAYS,
            "rows": [],
            "summary": {
                "recommended_units": ZERO,
                "high_risk_rows": 0,
            },
            "note": "Domingo se mantiene sin produccion programada; solo se monitorea venta y stock remanente.",
        }

    requested_top_n = _normalize_top_n(top_n)
    forecast_context = forecast_context or build_daily_forecast_context(target_date=target_date, top_n=requested_top_n)
    forecast_rows = list(forecast_context.get("rows") or [])
    keys = {(int(row["branch_id"]), int(row["recipe_id"])) for row in forecast_rows}
    stock_map = _load_latest_stock_by_recipe_branch(keys)
    production_map = _load_production_history(keys, target_date)

    recommendation_rows: list[dict[str, object]] = []
    high_risk_rows = 0
    total_units = ZERO
    for row in forecast_rows:
        key = (int(row["branch_id"]), int(row["recipe_id"]))
        forecast_qty = _to_decimal(row.get("forecast_qty"))
        forecast_min = _to_decimal(row.get("forecast_min_qty"))
        buffer_units = _to_decimal(row.get("buffer_units"))
        stock_units = stock_map.get(key, ZERO)
        production_history = production_map.get(key, {})
        produced_28 = _to_decimal(production_history.get("produced"))
        waste_28 = _to_decimal(production_history.get("waste"))
        waste_rate = (waste_28 / produced_28) if produced_28 > ZERO else ZERO
        stock_cover_days = (stock_units / forecast_qty) if forecast_qty > ZERO else ZERO
        waste_guard_units = max(stock_units - (forecast_qty * Decimal(SHELF_LIFE_DAYS)), ZERO)
        waste_guard_units += forecast_qty * min(waste_rate, Decimal("0.15"))
        suggested_units = max(forecast_qty + buffer_units - stock_units - waste_guard_units, ZERO)
        if stock_cover_days >= Decimal("2") or waste_rate >= Decimal("0.10"):
            risk_level = "ALTO"
            high_risk_rows += 1
        elif stock_cover_days >= Decimal("1") or waste_rate >= Decimal("0.05"):
            risk_level = "MEDIO"
        else:
            risk_level = "BAJO"
        why = (
            f"Forecast {forecast_qty.quantize(Decimal('0.01'))} pzs, "
            f"stock visible {stock_units.quantize(Decimal('0.01'))}, "
            f"buffer {buffer_units.quantize(Decimal('0.01'))}, "
            f"merma 28d {(waste_rate * Decimal('100')).quantize(Decimal('0.01'))}%."
        )
        recommendation_rows.append(
            {
                **row,
                "stock_units": _quantize_units(stock_units),
                "stock_cover_days": stock_cover_days.quantize(Decimal("0.01")),
                "waste_rate_pct": (waste_rate * Decimal("100")).quantize(Decimal("0.01")),
                "suggested_units": _quantize_units(suggested_units),
                "risk_level": risk_level,
                "coverage_gap_units": _quantize_units(max(forecast_min - stock_units, ZERO)),
                "why": why,
            }
        )
        total_units += suggested_units

    recommendation_rows.sort(
        key=lambda item: (
            _to_decimal(item.get("suggested_units")),
            _to_decimal(item.get("forecast_qty")),
        ),
        reverse=True,
    )
    return {
        "target_date": target_date.isoformat(),
        "target_label": target_date.strftime("%d %b %Y"),
        "shelf_life_days": SHELF_LIFE_DAYS,
        "rows": recommendation_rows if top_n is None or top_n <= 0 else recommendation_rows[:top_n],
        "summary": {
            "recommended_units": _quantize_units(total_units),
            "high_risk_rows": high_risk_rows,
            "rows_considered": len(recommendation_rows),
        },
        "note": "Formula: forecast + buffer - stock visible - guardia por merma/vida de anaquel.",
    }
