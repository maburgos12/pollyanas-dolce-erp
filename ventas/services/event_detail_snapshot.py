from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from typing import Any

from django.db.models import Count, Max

from ventas.models import EventoVenta, EventoVentaDetailSnapshot


def _freeze_value(value: Any):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_freeze_value(item) for item in value]
    if isinstance(value, tuple):
        return [_freeze_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _freeze_value(item) for key, item in value.items()}
    return value


def _event_detail_source_hash(event: EventoVenta) -> str:
    aggregates = {
        "event_version": int(event.version or 0),
        "event_status": event.status or "",
        "scenario_focus": event.scenario_focus or "",
        "objective_notes": event.objective_notes or "",
        "approved_at": event.approved_at.isoformat() if event.approved_at else "",
        "rejected_at": event.rejected_at.isoformat() if event.rejected_at else "",
        "capacity_rule_count": event.capacity_rules.count(),
        "capacity_rule_latest": event.capacity_rules.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "forecast_count": event.forecasts.count(),
        "forecast_latest": event.forecasts.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "production_plan_count": event.production_plans.count(),
        "production_line_count": event.production_plans.aggregate(total=Count("lines")).get("total") or 0,
        "production_latest": event.production_plans.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "input_count": event.input_requirements.count(),
        "input_latest": event.input_requirements.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "purchase_count": event.purchase_requirements.count(),
        "purchase_latest": event.purchase_requirements.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "financial_count": event.financials.count(),
        "financial_latest": event.financials.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "adjustment_count": event.adjustments.count(),
        "adjustment_latest": event.adjustments.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "draft_latest": event.adjustment_drafts.aggregate(max_ts=Max("updated_at")).get("max_ts"),
        "approval_latest": event.approvals.aggregate(max_ts=Max("created_at")).get("max_ts"),
        "artifact_latest": event.projection_artifacts.aggregate(max_ts=Max("created_at")).get("max_ts"),
    }
    frozen = _freeze_value(aggregates)
    return hashlib.sha256(json.dumps(frozen, sort_keys=True).encode("utf-8")).hexdigest()


def _serialize_executive_dataset(dataset: dict[str, Any]) -> dict[str, Any]:
    payload = _freeze_value(dataset)
    daily_rows = payload.get("daily_rows") or []
    for row in daily_rows:
        raw_date = row.get("date") or ""
        if isinstance(raw_date, str) and raw_date:
            try:
                row["date_label"] = date.fromisoformat(raw_date).strftime("%m-%d")
            except ValueError:
                row["date_label"] = raw_date
    return payload


def _serialize_production_dataset(dataset: dict[str, Any]) -> dict[str, Any]:
    payload = _freeze_value(dataset)
    payload["high_risk_inputs"] = [
        {
            "input_item_name": row.input_item.nombre,
            "net_shortage_qty": str(row.net_shortage_qty or 0),
            "risk_level_display": row.get_risk_level_display(),
            "required_by_date": row.required_by_date.isoformat() if row.required_by_date else "",
        }
        for row in dataset.get("high_risk_inputs", [])
    ]
    payload["purchase_rows"] = [
        {
            "input_item_name": row.input_requirement.input_item.nombre,
            "purchase_deadline": row.purchase_deadline.isoformat() if row.purchase_deadline else "",
            "supplier_name": row.supplier.nombre if row.supplier_id and row.supplier else "Por definir",
            "estimated_cost": str(row.estimated_cost or 0),
            "status_display": row.get_status_display(),
        }
        for row in dataset.get("purchase_rows", [])
    ]
    return payload


def _serialize_focused_financial(row) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "estimated_sales": str(row.estimated_sales or 0),
        "estimated_gross_profit": str(row.estimated_gross_profit or 0),
        "expected_roi": str(row.expected_roi or 0),
    }


def build_event_detail_snapshot_payload(event: EventoVenta) -> dict[str, Any]:
    from ventas import views as ventas_views

    forecast_qs = ventas_views._event_forecast_qs(event)
    week_forecast_qs = ventas_views._week_scope_qs(event, forecast_qs)
    week_start, week_end = ventas_views._event_projection_window(event)

    executive_dataset = ventas_views._event_financial_dataset(
        event,
        forecast_qs,
        start_date=week_start,
        end_date=week_end,
    )
    focused_financial = (
        ventas_views._focused_financial_row(event)
        if executive_dataset["financial_trusted"]
        else None
    )
    production_dataset = ventas_views._production_operational_dataset(
        event,
        forecast_qs,
        start_date=week_start,
        end_date=week_end,
    )

    main_day_row = next(
        (
            row
            for row in executive_dataset.get("daily_rows", [])
            if str(row.get("date") or "") == event.main_date.isoformat()
        ),
        None,
    )
    payload = {
        "week_total_qty": str(
            week_forecast_qs.aggregate(total=ventas_views.Sum("final_forecast")).get("total") or 0
        ),
        "main_day_total_qty": str(
            forecast_qs.filter(forecast_date=event.main_date)
            .aggregate(total=ventas_views.Sum("final_forecast"))
            .get("total")
            or 0
        ),
        "week_projected_revenue": (
            str(executive_dataset["summary"]["sales"])
            if executive_dataset["financial_trusted"]
            else None
        ),
        "main_day_projected_revenue": (
            str((main_day_row or {}).get("sales") or 0)
            if executive_dataset["financial_trusted"]
            else None
        ),
        "input_investment_required": str(ventas_views._input_investment_amount(event)),
        "week_branch_breakdown": _freeze_value(ventas_views._branch_projection_rows(week_forecast_qs)),
        "week_product_projection": _freeze_value(ventas_views._product_projection_rows(week_forecast_qs)),
        "week_scope_label": ventas_views._week_scope_label(event),
        "week_trend_note": ventas_views._projection_trend_note(
            ventas_views._product_projection_rows(week_forecast_qs),
            label="la semana del evento",
        ),
        "executive_dataset": _serialize_executive_dataset(executive_dataset),
        "focused_financial": _serialize_focused_financial(focused_financial),
        "production_dataset": _serialize_production_dataset(production_dataset),
        "purchase_summary": _freeze_value(
            {
                "total_shortage": ventas_views._round_money(
                    event.input_requirements.aggregate(total=ventas_views.Sum("net_shortage_qty")).get("total") or 0
                ),
                "high_risk": event.input_requirements.filter(
                    risk_level=ventas_views.EventoVentaInputRequirement.RISK_HIGH
                ).count(),
                "pending_purchases": event.purchase_requirements.filter(
                    status=ventas_views.EventoVentaPurchaseRequirement.STATUS_PENDIENTE
                ).count(),
                "estimated_purchase_cost": ventas_views._round_money(
                    event.purchase_requirements.aggregate(total=ventas_views.Sum("estimated_cost")).get("total") or 0
                ),
            }
        ),
        "considered_product_count": week_forecast_qs.values("product_id").distinct().count(),
    }
    return payload


def refresh_event_detail_snapshot(event: EventoVenta, generated_by=None) -> EventoVentaDetailSnapshot:
    source_hash = _event_detail_source_hash(event)
    payload = build_event_detail_snapshot_payload(event)
    snapshot, _created = EventoVentaDetailSnapshot.objects.update_or_create(
        sales_event=event,
        defaults={
            "snapshot_version": int(event.version or 0),
            "source_hash": source_hash,
            "payload_json": payload,
            "generated_by": generated_by,
        },
    )
    return snapshot


def get_event_detail_snapshot_payload(
    event: EventoVenta,
    *,
    generated_by=None,
    allow_refresh: bool = True,
) -> dict[str, Any] | None:
    current_hash = _event_detail_source_hash(event)
    try:
        snapshot = event.detail_snapshot
    except EventoVentaDetailSnapshot.DoesNotExist:
        snapshot = None
    if snapshot and snapshot.source_hash == current_hash and snapshot.payload_json:
        return snapshot.payload_json
    if not allow_refresh:
        return snapshot.payload_json if snapshot else None
    snapshot = refresh_event_detail_snapshot(event, generated_by=generated_by)
    return snapshot.payload_json
