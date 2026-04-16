from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from django.utils import timezone

from reportes.forecast_service import build_daily_forecast_context
from reportes.production_recommendation_service import build_production_recommendation_context
from reportes.waste_detection_service import build_waste_detection_context


ZERO = Decimal("0")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def build_opportunity_context(
    *,
    target_date: date | None = None,
    forecast_context: dict[str, object] | None = None,
    production_context: dict[str, object] | None = None,
    waste_context: dict[str, object] | None = None,
    top_n: int | None = 12,
) -> dict[str, object]:
    target_date = target_date or timezone.localdate()
    requested_top_n = None if top_n is None or top_n <= 0 else max(top_n * 2, 24)
    forecast_context = forecast_context or build_daily_forecast_context(target_date=target_date, top_n=requested_top_n)
    production_context = production_context or build_production_recommendation_context(
        target_date=target_date,
        forecast_context=forecast_context,
        top_n=requested_top_n,
    )
    waste_context = waste_context or build_waste_detection_context(top_n=requested_top_n)

    waste_map = {
        (int(row["branch_id"]), int(row["recipe_id"])): row
        for row in (waste_context.get("rows") or [])
    }
    production_map = {
        (int(row["branch_id"]), int(row["recipe_id"])): row
        for row in (production_context.get("rows") or [])
    }

    opportunity_rows: list[dict[str, object]] = []
    for row in forecast_context.get("rows") or []:
        key = (int(row["branch_id"]), int(row["recipe_id"]))
        production_row = production_map.get(key, {})
        waste_row = waste_map.get(key, {})
        trend_pct = _to_decimal(row.get("trend_pct"))
        margin_pct = _to_decimal(row.get("margin_pct"))
        stock_cover_days = _to_decimal(production_row.get("stock_cover_days"))
        suggested_units = _to_decimal(production_row.get("suggested_units"))
        recent_avg_7 = _to_decimal(row.get("recent_avg_7"))
        recent_avg_28 = _to_decimal(row.get("recent_avg_28"))
        waste_rate_pct = _to_decimal(waste_row.get("waste_rate_pct"))

        action = None
        why = ""
        priority = "MEDIA"
        if suggested_units > ZERO and (trend_pct >= Decimal("8") or stock_cover_days < Decimal("0.75")):
            action = "PRODUCIR_MAS"
            priority = "ALTA"
            why = (
                f"Demanda en crecimiento {trend_pct.quantize(Decimal('0.01'))}% "
                f"y cobertura de stock {stock_cover_days.quantize(Decimal('0.01'))} dias."
            )
        elif waste_rate_pct >= Decimal("6") and margin_pct > ZERO:
            action = "PROMOCIONAR"
            priority = "MEDIA"
            why = (
                f"Merma historica {waste_rate_pct.quantize(Decimal('0.01'))}% con margen "
                f"{margin_pct.quantize(Decimal('0.01'))}%."
            )
        elif recent_avg_28 > Decimal("1") and recent_avg_7 < (recent_avg_28 * Decimal("0.45")) and margin_pct >= ZERO:
            action = "REACTIVAR"
            priority = "MEDIA"
            why = (
                f"Promedio 28d {recent_avg_28.quantize(Decimal('0.01'))} pzs vs "
                f"7d {recent_avg_7.quantize(Decimal('0.01'))}; la demanda se enfrió."
            )
        if not action:
            continue
        opportunity_rows.append(
            {
                "action": action,
                "priority": priority,
                "branch_id": int(row["branch_id"]),
                "branch_code": row["branch_code"],
                "branch_name": row["branch_name"],
                "recipe_id": int(row["recipe_id"]),
                "recipe_name": row["recipe_name"],
                "category": row["category"],
                "family": row["family"],
                "forecast_qty": row["forecast_qty"],
                "forecast_amount": row["forecast_amount"],
                "suggested_units": production_row.get("suggested_units") or ZERO,
                "margin_pct": row.get("margin_pct") or ZERO,
                "why": why,
            }
        )

    opportunity_rows.sort(
        key=lambda item: (
            2 if item["priority"] == "ALTA" else 1,
            _to_decimal(item.get("forecast_amount")),
        ),
        reverse=True,
    )
    return {
        "target_date": target_date.isoformat(),
        "rows": opportunity_rows if top_n is None or top_n <= 0 else opportunity_rows[:top_n],
        "summary": {
            "rows": len(opportunity_rows),
            "high_priority": sum(1 for row in opportunity_rows if row["priority"] == "ALTA"),
        },
        "limitations": {
            "cross_sell_available": False,
            "reason": "No hay canasta transaccional por ticket en la capa analitica actual; solo venta agregada por dia/producto/sucursal.",
        },
    }
