from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.db.models import Max, Sum
from django.utils import timezone

from recetas.utils.derived_product_presentations import get_total_cost_map
from reportes.models import FactProduccionDiaria, ProductoCostoOperativoMensual


ZERO = Decimal("0")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _latest_unit_cost_map(recipe_ids: set[int]) -> dict[int, Decimal]:
    if not recipe_ids:
        return {}
    latest_period = ProductoCostoOperativoMensual.objects.aggregate(v=Max("periodo")).get("v")
    if not latest_period:
        return get_total_cost_map(recipe_ids)
    rows = ProductoCostoOperativoMensual.objects.filter(periodo=latest_period, receta_id__in=recipe_ids).values_list(
        "receta_id",
        "costo_fabricacion_unit",
    )
    cost_map = {int(recipe_id): _to_decimal(cost) for recipe_id, cost in rows}
    missing = sorted(recipe_ids - set(cost_map))
    if missing:
        cost_map.update(get_total_cost_map(missing))
    return cost_map


def build_waste_detection_context(*, reference_date: date | None = None, lookback_days: int = 28, top_n: int = 12) -> dict[str, object]:
    reference_date = reference_date or timezone.localdate()
    start_date = reference_date - timedelta(days=lookback_days)
    rows = list(
        FactProduccionDiaria.objects.filter(fecha__gte=start_date, fecha__lt=reference_date, receta_id__isnull=False, sucursal_id__isnull=False)
        .values(
            "sucursal_id",
            "sucursal__codigo",
            "sucursal__nombre",
            "receta_id",
            "receta__nombre",
            "receta__familia",
            "receta__categoria",
        )
        .annotate(
            produced=Sum("producido"),
            sold=Sum("vendido"),
            waste=Sum("merma"),
        )
    )
    cost_map = _latest_unit_cost_map({int(row["receta_id"]) for row in rows})
    risk_rows: list[dict[str, object]] = []
    total_realized = ZERO
    total_exposed = ZERO
    for row in rows:
        produced = _to_decimal(row.get("produced"))
        sold = _to_decimal(row.get("sold"))
        waste = _to_decimal(row.get("waste"))
        if produced <= ZERO and waste <= ZERO:
            continue
        reference_units = max(produced, sold, waste)
        rotation_pct = (sold / reference_units * Decimal("100")) if reference_units > ZERO else ZERO
        waste_rate_pct = (waste / reference_units * Decimal("100")) if reference_units > ZERO else ZERO
        excess_units = max(produced - sold, ZERO)
        unit_cost = _to_decimal(cost_map.get(int(row["receta_id"])))
        realized_impact = waste * unit_cost
        exposed_impact = excess_units * unit_cost
        if waste_rate_pct >= Decimal("8") or rotation_pct <= Decimal("75"):
            risk_level = "ALTO"
        elif waste_rate_pct >= Decimal("4") or rotation_pct <= Decimal("90"):
            risk_level = "MEDIO"
        else:
            risk_level = "BAJO"
        why = (
            f"Produccion {produced.quantize(Decimal('0.01'))} pzs, venta {sold.quantize(Decimal('0.01'))}, "
            f"merma {waste.quantize(Decimal('0.01'))} ({waste_rate_pct.quantize(Decimal('0.01'))}%), "
            f"rotacion {rotation_pct.quantize(Decimal('0.01'))}%."
        )
        risk_rows.append(
            {
                "branch_id": int(row["sucursal_id"]),
                "branch_code": row.get("sucursal__codigo") or "",
                "branch_name": row.get("sucursal__nombre") or "",
                "recipe_id": int(row["receta_id"]),
                "recipe_name": row.get("receta__nombre") or "",
                "family": row.get("receta__familia") or "",
                "category": row.get("receta__categoria") or "",
                "produced_units": _quantize_units(produced),
                "sold_units": _quantize_units(sold),
                "waste_units": _quantize_units(waste),
                "excess_units": _quantize_units(excess_units),
                "rotation_pct": rotation_pct.quantize(Decimal("0.01")),
                "waste_rate_pct": waste_rate_pct.quantize(Decimal("0.01")),
                "risk_level": risk_level,
                "realized_impact": _quantize_money(realized_impact),
                "exposed_impact": _quantize_money(exposed_impact),
                "why": why,
            }
        )
        total_realized += realized_impact
        total_exposed += exposed_impact
    risk_rows.sort(
        key=lambda item: (
            _to_decimal(item.get("realized_impact")) + _to_decimal(item.get("exposed_impact")),
            _to_decimal(item.get("waste_units")),
        ),
        reverse=True,
    )
    return {
        "lookback_days": lookback_days,
        "rows": risk_rows if top_n is None or top_n <= 0 else risk_rows[:top_n],
        "summary": {
            "products": len(risk_rows),
            "realized_impact": _quantize_money(total_realized),
            "exposed_impact": _quantize_money(total_exposed),
            "high_risk_rows": sum(1 for row in risk_rows if row["risk_level"] == "ALTO"),
        },
        "note": "Se marca riesgo por sobreproduccion persistente, baja rotacion y merma historica sobre los ultimos 28 dias.",
    }
