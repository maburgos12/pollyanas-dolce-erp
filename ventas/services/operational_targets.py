from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ventas.models import EventoVenta, EventoVentaForecast
from recetas.utils.commercial_composition import (
    CommercialRecipeLookupContext,
    get_commercial_total_cost_map,
)


ZERO = Decimal("0")
ONE = Decimal("1")

SERVICE_LEVEL_Z = {
    Decimal("0.55"): Decimal("0.126"),
    Decimal("0.60"): Decimal("0.253"),
    Decimal("0.65"): Decimal("0.385"),
    Decimal("0.70"): Decimal("0.524"),
    Decimal("0.75"): Decimal("0.674"),
    Decimal("0.80"): Decimal("0.842"),
    Decimal("0.85"): Decimal("1.036"),
}


@dataclass(frozen=True, slots=True)
class OperationalTarget:
    forecast_id: int
    demand_qty: Decimal
    target_qty: Decimal
    service_level: Decimal
    uncertainty_qty: Decimal
    overage_cost: Decimal
    underage_cost: Decimal
    policy_band: str
    reason: str


def _to_decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _keyword_bucket(product_name: str) -> str:
    name = (product_name or "").strip().lower()
    if any(token in name for token in ("vaso", "rebanada")):
        return "MUY_PERECEDERO"
    if any(token in name for token in ("pay", "bollo", "mini", "empanada", "mediano", "chico")):
        return "PERECEDERO_MEDIO"
    return "ESTABLE"


def _policy_limits(bucket: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    if bucket == "MUY_PERECEDERO":
        return Decimal("0.58"), Decimal("0.70"), Decimal("0.05"), Decimal("1.20")
    if bucket == "PERECEDERO_MEDIO":
        return Decimal("0.62"), Decimal("0.76"), Decimal("0.18"), Decimal("1.45")
    return Decimal("0.68"), Decimal("0.82"), Decimal("0.30"), Decimal("1.75")


def _nearest_service_level(value: Decimal) -> Decimal:
    ordered = sorted(SERVICE_LEVEL_Z.keys())
    return min(ordered, key=lambda candidate: abs(candidate - value))


def _compute_operational_target(
    *,
    forecast: EventoVentaForecast,
    avg_cost: Decimal,
) -> OperationalTarget:
    demand_qty = _to_decimal(forecast.final_forecast)
    if demand_qty <= ZERO or _to_decimal(forecast.confidence_score) <= ZERO:
        return OperationalTarget(
            forecast_id=int(forecast.id),
            demand_qty=_quantize_units(demand_qty),
            target_qty=_quantize_units(demand_qty),
            service_level=Decimal("0.50"),
            uncertainty_qty=ZERO,
            overage_cost=ZERO,
            underage_cost=ZERO,
            policy_band="SIN_AJUSTE",
            reason="Sin señal estadística suficiente; el objetivo operativo permanece igual a la demanda esperada.",
        )

    bucket = _keyword_bucket(forecast.product.nombre or "")
    floor_service, ceiling_service, salvage_pct, underage_multiplier = _policy_limits(bucket)
    underage_cost = max(avg_cost * underage_multiplier, avg_cost * Decimal("0.25"), Decimal("1"))
    salvage_value = avg_cost * salvage_pct
    overage_cost = max(avg_cost - salvage_value, avg_cost * Decimal("0.35"), Decimal("1"))
    raw_service = underage_cost / (underage_cost + overage_cost) if (underage_cost + overage_cost) > ZERO else floor_service
    service_level = max(floor_service, min(ceiling_service, raw_service))
    service_level = _nearest_service_level(service_level)
    z_value = SERVICE_LEVEL_Z[service_level]

    uncertainty_qty = max(
        abs(_to_decimal(forecast.aggressive_forecast) - demand_qty),
        abs(demand_qty - _to_decimal(forecast.conservative_forecast)),
        demand_qty * Decimal("0.06"),
    )
    target_qty = max(demand_qty + (uncertainty_qty * z_value), demand_qty)
    return OperationalTarget(
        forecast_id=int(forecast.id),
        demand_qty=_quantize_units(demand_qty),
        target_qty=_quantize_units(target_qty),
        service_level=service_level,
        uncertainty_qty=_quantize_units(uncertainty_qty),
        overage_cost=overage_cost.quantize(Decimal("0.01")),
        underage_cost=underage_cost.quantize(Decimal("0.01")),
        policy_band=bucket,
        reason=(
            f"Objetivo operativo separado de forecast con service level {service_level} "
            f"según banda {bucket.lower().replace('_', ' ')}, merma/quiebre y dispersión del forecast."
        ),
    )


def build_operational_targets(
    event: EventoVenta,
    *,
    commercial_context: CommercialRecipeLookupContext | None = None,
) -> dict[int, OperationalTarget]:
    forecasts = list(
        EventoVentaForecast.objects.filter(sales_event=event).select_related("product")
    )
    if not forecasts:
        return {}

    cost_map = get_commercial_total_cost_map(
        {forecast.product_id for forecast in forecasts},
        context=commercial_context,
    )
    targets: dict[int, OperationalTarget] = {}
    for forecast in forecasts:
        avg_cost = cost_map.get(forecast.product_id, ZERO)
        targets[int(forecast.id)] = _compute_operational_target(
            forecast=forecast,
            avg_cost=avg_cost,
        )
    return targets
