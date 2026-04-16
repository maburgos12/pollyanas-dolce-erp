from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from django.utils import timezone

from reportes.forecast_service import build_daily_forecast_context
from reportes.opportunity_service import build_opportunity_context
from reportes.production_recommendation_service import build_production_recommendation_context
from reportes.waste_detection_service import build_waste_detection_context


ZERO = Decimal("0")
HUNDRED = Decimal("100")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _safe_score(value: Decimal, max_value: Decimal) -> Decimal:
    if max_value <= ZERO:
        return ZERO
    return min((value / max_value) * HUNDRED, HUNDRED)


def build_decision_score_context(
    *,
    target_date: date | None = None,
    forecast_context: dict[str, object] | None = None,
    production_context: dict[str, object] | None = None,
    waste_context: dict[str, object] | None = None,
    opportunity_context: dict[str, object] | None = None,
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
    opportunity_context = opportunity_context or build_opportunity_context(
        target_date=target_date,
        forecast_context=forecast_context,
        production_context=production_context,
        waste_context=waste_context,
        top_n=requested_top_n,
    )

    production_map = {
        (int(row["branch_id"]), int(row["recipe_id"])): row
        for row in (production_context.get("rows") or [])
    }
    waste_map = {
        (int(row["branch_id"]), int(row["recipe_id"])): row
        for row in (waste_context.get("rows") or [])
    }
    opportunity_map = {
        (int(row["branch_id"]), int(row["recipe_id"])): row
        for row in (opportunity_context.get("rows") or [])
    }

    forecast_rows = list(forecast_context.get("rows") or [])
    max_forecast = max((_to_decimal(row.get("forecast_qty")) for row in forecast_rows), default=ZERO)
    max_contribution = max((_to_decimal(row.get("contribution_unit")) for row in forecast_rows), default=ZERO)

    score_rows: list[dict[str, object]] = []
    for row in forecast_rows:
        key = (int(row["branch_id"]), int(row["recipe_id"]))
        production_row = production_map.get(key, {})
        waste_row = waste_map.get(key, {})
        opportunity_row = opportunity_map.get(key, {})
        demand_score = _safe_score(_to_decimal(row.get("forecast_qty")), max_forecast)
        profitability_score = _safe_score(max(_to_decimal(row.get("contribution_unit")), ZERO), max_contribution if max_contribution > ZERO else Decimal("1"))
        stock_cover_days = _to_decimal(production_row.get("stock_cover_days"))
        rotation_pct = _to_decimal(waste_row.get("rotation_pct"))
        waste_rate_pct = _to_decimal(waste_row.get("waste_rate_pct"))
        rotation_score = min(rotation_pct, HUNDRED) if rotation_pct > ZERO else (HUNDRED if stock_cover_days < Decimal("1") else Decimal("50"))
        waste_penalty = min(waste_rate_pct * Decimal("1.5"), Decimal("35"))
        score = (demand_score * Decimal("0.40")) + (profitability_score * Decimal("0.30")) + (rotation_score * Decimal("0.20")) - (waste_penalty * Decimal("0.10"))
        if opportunity_row.get("priority") == "ALTA":
            score += Decimal("8")
        elif opportunity_row.get("priority") == "MEDIA":
            score += Decimal("3")
        score = max(min(score, HUNDRED), ZERO).quantize(Decimal("0.01"))
        if score >= Decimal("70"):
            priority = "ALTA"
        elif score >= Decimal("45"):
            priority = "MEDIA"
        else:
            priority = "BAJA"
        why = (
            f"Demanda {demand_score.quantize(Decimal('0.1'))}, "
            f"rentabilidad {profitability_score.quantize(Decimal('0.1'))}, "
            f"rotacion {rotation_score.quantize(Decimal('0.1'))}, "
            f"penalizacion merma {waste_penalty.quantize(Decimal('0.1'))}."
        )
        score_rows.append(
            {
                "priority": priority,
                "score": score,
                "branch_id": int(row["branch_id"]),
                "branch_code": row["branch_code"],
                "branch_name": row["branch_name"],
                "recipe_id": int(row["recipe_id"]),
                "recipe_name": row["recipe_name"],
                "category": row["category"],
                "family": row["family"],
                "recommended_action": opportunity_row.get("action") or ("PRODUCIR_MAS" if _to_decimal(production_row.get("suggested_units")) > ZERO else "MONITOREAR"),
                "forecast_qty": row["forecast_qty"],
                "margin_pct": row.get("margin_pct") or ZERO,
                "waste_rate_pct": waste_rate_pct,
                "why": why,
            }
        )

    score_rows.sort(key=lambda row: (_to_decimal(row.get("score")), _to_decimal(row.get("forecast_qty"))), reverse=True)
    return {
        "target_date": target_date.isoformat(),
        "rows": score_rows if top_n is None or top_n <= 0 else score_rows[:top_n],
        "summary": {
            "high_priority": sum(1 for row in score_rows if row["priority"] == "ALTA"),
            "medium_priority": sum(1 for row in score_rows if row["priority"] == "MEDIA"),
            "rows": len(score_rows),
        },
        "formula": {
            "demand_weight": "40%",
            "profitability_weight": "30%",
            "rotation_weight": "20%",
            "waste_penalty": "10%",
        },
    }
