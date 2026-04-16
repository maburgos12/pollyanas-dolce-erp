from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from django.conf import settings
from django.db.models import Count, Sum, Q
from django.utils import timezone

from recetas.utils.commercial_composition import get_commercial_total_cost_map
from ventas.models import (
    EventoVenta,
    EventoVentaFinancial,
    EventoVentaForecast,
    EventoVentaInputRequirement,
    EventoVentaProductionPlan,
    EventoVentaProjectionArtifact,
    EventoVentaPurchaseRequirement,
)
from ventas.services.financials import resolve_unit_price


ZERO = Decimal("0")
PRICE_COVERAGE_MIN = Decimal("85.00")
COST_COVERAGE_MIN = Decimal("85.00")
ACTIVE_REPROCESS_STATUSES = {
    EventoVenta.STATUS_BORRADOR,
    EventoVenta.STATUS_MODELADO,
    EventoVenta.STATUS_PENDIENTE_DG,
    EventoVenta.STATUS_APROBADO,
    EventoVenta.STATUS_APROBADO_AJUSTES,
    EventoVenta.STATUS_ENVIADO_PROD,
    EventoVenta.STATUS_VALIDADO_PROD,
    EventoVenta.STATUS_ENVIADO_COMPRAS,
    EventoVenta.STATUS_EN_EJECUCION,
    EventoVenta.STATUS_CERRADO,
}


def _as_decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def identify_priority_events(*, event_ids: list[int] | None = None, lookback_days: int = 60) -> list[EventoVenta]:
    if event_ids:
        ordering = {int(event_id): idx for idx, event_id in enumerate(event_ids)}
        events = list(EventoVenta.objects.filter(id__in=event_ids).order_by("-updated_at", "-id"))
        events.sort(key=lambda event: ordering.get(event.id, 999999))
        return events

    today = timezone.localdate()
    updated_cutoff = timezone.now() - timedelta(days=max(int(lookback_days or 0), 1))
    event_ids_qs = (
        EventoVenta.objects.filter(
            Q(status__in=ACTIVE_REPROCESS_STATUSES)
            | Q(updated_at__gte=updated_cutoff)
            | Q(analysis_end_date__gte=today - timedelta(days=14))
        )
        .annotate(forecast_count=Count("forecasts"))
        .filter(Q(forecast_count__gt=0) | Q(status__in=ACTIVE_REPROCESS_STATUSES))
        .order_by("-updated_at", "-id")
        .values_list("id", flat=True)
    )
    return list(EventoVenta.objects.filter(id__in=list(event_ids_qs)).order_by("-updated_at", "-id"))


def collect_event_pipeline_snapshot(event: EventoVenta) -> dict[str, Any]:
    forecast_qs = EventoVentaForecast.objects.filter(sales_event=event).select_related("product", "branch")
    forecasts = list(forecast_qs)
    forecast_total = _as_decimal(forecast_qs.aggregate(total=Sum("final_forecast")).get("total"))
    forecast_by_day = [
        {
            "date": row["forecast_date"].isoformat(),
            "qty": str(_as_decimal(row["total"]).quantize(Decimal("0.001"))),
        }
        for row in forecast_qs.values("forecast_date").annotate(total=Sum("final_forecast")).order_by("forecast_date")
    ]

    product_ids = {row.product_id for row in forecasts}
    price_cache: dict[tuple[int, int], Decimal] = {}
    cost_map = get_commercial_total_cost_map(product_ids)
    qty_total = ZERO
    qty_with_price = ZERO
    qty_with_cost = ZERO
    missing_price: dict[int, dict[str, Any]] = {}
    missing_cost: dict[int, dict[str, Any]] = {}

    for row in forecasts:
        qty = _as_decimal(row.final_forecast)
        if qty <= 0:
            continue
        qty_total += qty
        price_key = (row.product_id, row.branch_id)
        if price_key not in price_cache:
            price_cache[price_key] = resolve_unit_price(
                row.product_id,
                event.analysis_start_date,
                event.analysis_end_date,
                branch_id=row.branch_id,
            )
        unit_price = price_cache[price_key]
        unit_cost = _as_decimal(cost_map.get(row.product_id))
        if unit_price > 0:
            qty_with_price += qty
        else:
            item = missing_price.setdefault(
                row.product_id,
                {"product": row.product.nombre, "qty": ZERO, "unit_price": "0.0000"},
            )
            item["qty"] += qty
        if unit_cost > 0:
            qty_with_cost += qty
        else:
            item = missing_cost.setdefault(
                row.product_id,
                {"product": row.product.nombre, "qty": ZERO, "unit_cost": "0.000000"},
            )
            item["qty"] += qty

    def _pct(part: Decimal, total: Decimal) -> Decimal:
        if total <= 0:
            return Decimal("0.00")
        return (part / total * Decimal("100")).quantize(Decimal("0.01"))

    price_qty_pct = _pct(qty_with_price, qty_total)
    cost_qty_pct = _pct(qty_with_cost, qty_total)

    finance_rows = [
        {
            "scenario": row.scenario,
            "estimated_sales": str(_as_decimal(row.estimated_sales).quantize(Decimal("0.01"))),
            "estimated_cogs": str(_as_decimal(row.estimated_cogs).quantize(Decimal("0.01"))),
            "estimated_gross_profit": str(_as_decimal(row.estimated_gross_profit).quantize(Decimal("0.01"))),
            "estimated_margin": str(_as_decimal(row.estimated_margin).quantize(Decimal("0.01"))),
            "expected_roi": str(_as_decimal(row.expected_roi).quantize(Decimal("0.01"))),
        }
        for row in EventoVentaFinancial.objects.filter(sales_event=event).order_by("scenario")
    ]

    input_rows = [
        {
            "input": row.input_item.nombre,
            "required_qty": str(_as_decimal(row.required_qty).quantize(Decimal("0.001"))),
            "unit": row.input_item.unidad_base.codigo if row.input_item.unidad_base_id else "",
            "shortage_qty": str(_as_decimal(row.net_shortage_qty).quantize(Decimal("0.001"))),
            "risk": row.risk_level,
        }
        for row in EventoVentaInputRequirement.objects.filter(sales_event=event)
        .select_related("input_item", "input_item__unidad_base")
        .order_by("-required_qty", "input_item__nombre")[:10]
    ]
    absurd_inputs = [
        row
        for row in input_rows
        if (
            row["unit"] in {"kg", "lt"} and Decimal(row["required_qty"]) >= Decimal("1000")
        ) or (
            row["unit"] == "pza" and Decimal(row["required_qty"]) >= Decimal("10000")
        )
    ]

    production_plan_count = EventoVentaProductionPlan.objects.filter(sales_event=event).count()
    production_line_total = (
        EventoVentaProductionPlan.objects.filter(sales_event=event).aggregate(total=Count("lines")).get("total") or 0
    )
    artifact_rows = [
        {
            "export_type": row.export_type,
            "forecast_version": row.forecast_version,
            "file_name": row.file_name,
            "size_bytes": row.size_bytes,
            "created_at": row.created_at.isoformat(),
        }
        for row in EventoVentaProjectionArtifact.objects.filter(sales_event=event).order_by("export_type", "-created_at")[:20]
    ]

    blockers: list[str] = []
    if price_qty_pct < PRICE_COVERAGE_MIN:
        blockers.append(f"Cobertura de precio insuficiente: {price_qty_pct}%.")
    if cost_qty_pct < COST_COVERAGE_MIN:
        blockers.append(f"Cobertura de costo insuficiente: {cost_qty_pct}%.")
    if absurd_inputs:
        blockers.append(f"Se detectaron {len(absurd_inputs)} insumos con magnitud absurda aparente.")

    return {
        "event": {
            "id": event.id,
            "code": event.code,
            "name": event.name,
            "status": event.status,
            "version": event.version,
            "main_date": event.main_date.isoformat() if event.main_date else None,
            "analysis_start_date": event.analysis_start_date.isoformat() if event.analysis_start_date else None,
            "analysis_end_date": event.analysis_end_date.isoformat() if event.analysis_end_date else None,
            "updated_at": event.updated_at.isoformat() if event.updated_at else None,
        },
        "forecast": {
            "rows": len(forecasts),
            "total_qty": str(forecast_total.quantize(Decimal("0.001"))),
            "by_day": forecast_by_day,
        },
        "coverage": {
            "price_qty_pct": str(price_qty_pct),
            "cost_qty_pct": str(cost_qty_pct),
            "missing_price_products": [
                {
                    "product": item["product"],
                    "qty": str(_as_decimal(item["qty"]).quantize(Decimal("0.001"))),
                }
                for item in sorted(missing_price.values(), key=lambda row: (-row["qty"], row["product"]))
            ],
            "missing_cost_products": [
                {
                    "product": item["product"],
                    "qty": str(_as_decimal(item["qty"]).quantize(Decimal("0.001"))),
                }
                for item in sorted(missing_cost.values(), key=lambda row: (-row["qty"], row["product"]))
            ],
        },
        "financials": finance_rows,
        "production": {
            "plan_count": production_plan_count,
            "line_count": production_line_total,
        },
        "inputs": {
            "top_requirements": input_rows,
            "absurd_requirements": absurd_inputs,
        },
        "purchases": {
            "rows": EventoVentaPurchaseRequirement.objects.filter(sales_event=event).count(),
        },
        "artifacts": artifact_rows,
        "blockers": blockers,
    }


def audit_output_dir(event: EventoVenta) -> Path:
    return Path(settings.BASE_DIR) / "output" / "spreadsheet" / "ventas_eventos" / event.code.lower() / "audit"


def write_event_pipeline_audit_report(
    event: EventoVenta,
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    steps: list[dict[str, Any]],
    selection_reason: str,
) -> Path:
    output_dir = audit_output_dir(event)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = timezone.localtime().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"{event.code.lower()}_pipeline_audit_{timestamp}.json"
    payload = {
        "generated_at": timezone.localtime().isoformat(),
        "event": after.get("event") or before.get("event"),
        "selection_reason": selection_reason,
        "before": before,
        "after": after,
        "steps": steps,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
