from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.db import OperationalError, ProgrammingError, connection
from django.utils import timezone

from reportes.decision_score_service import build_decision_score_context
from reportes.dashboard_daily_ops_dataset import get_dashboard_daily_ops_dataset
from reportes.dashboard_production_dataset import get_dashboard_production_dataset
from reportes.dashboard_sales_dataset import get_dashboard_sales_dataset
from reportes.executive_panels import build_executive_bi_panels
from reportes.forecast_service import build_daily_forecast_context
from reportes.opportunity_service import build_opportunity_context
from reportes.production_recommendation_service import build_production_recommendation_context
from reportes.waste_detection_service import build_waste_detection_context


ALLOWED_MONTH_WINDOWS = (6, 9, 12)


def normalize_dashboard_months_window(value: object) -> int:
    try:
        months_window = int(value or 6)
    except (TypeError, ValueError):
        months_window = 6
    return months_window if months_window in ALLOWED_MONTH_WINDOWS else 6


def _json_ready(value: Any):
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def serialize_dashboard_full_payload(payload: dict[str, object]) -> dict[str, object]:
    return _json_ready(payload)


def _coerce_json(value):
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _parse_iso_date(value):
    if not value or not isinstance(value, str):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError:
        return value


def _hydrate_dashboard_full_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    hydrated = dict(payload)
    inventory_ledger_panel = dict(hydrated.get("inventory_ledger_panel") or {})
    inventory_ledger_panel["cutoff_date"] = _parse_iso_date(inventory_ledger_panel.get("cutoff_date"))
    inventory_ledger_panel["flow_coverage_start"] = _parse_iso_date(inventory_ledger_panel.get("flow_coverage_start"))
    inventory_ledger_panel["snapshot_coverage_start"] = _parse_iso_date(inventory_ledger_panel.get("snapshot_coverage_start"))
    hydrated["inventory_ledger_panel"] = inventory_ledger_panel
    return hydrated


def build_dashboard_full_payload(*, months_window: int) -> dict[str, object]:
    months_window = normalize_dashboard_months_window(months_window)

    # Import lazily to avoid an import cycle with core.views while preserving
    # the exact business-safe builders already validated in production.
    from core.views import (  # noqa: PLC0415
        _build_canonical_inventory_dashboard_metrics,
        _build_dashboard_purchase_snapshot,
    )

    sales_dataset = get_dashboard_sales_dataset(months=months_window)
    daily_ops_dataset = get_dashboard_daily_ops_dataset()
    executive_panels = build_executive_bi_panels(months=months_window)
    production_panel = dict(executive_panels.get("production_sales_panel") or {})
    production_cutoff = production_panel.get("cutoff_date")
    if isinstance(production_cutoff, str):
        production_cutoff = _parse_iso_date(production_cutoff)
    dataset_production = get_dashboard_production_dataset(
        latest_date=production_cutoff or timezone.localdate(),
    )
    inventory_metrics = _build_canonical_inventory_dashboard_metrics()
    purchase_snapshot = _build_dashboard_purchase_snapshot()
    daily_sales_snapshot = dict(sales_dataset.get("daily_sales_snapshot") or {})
    decision_target_date = timezone.localdate()
    forecast_recommendation = build_daily_forecast_context(target_date=decision_target_date, top_n=24)
    production_recommendation = build_production_recommendation_context(
        target_date=decision_target_date,
        forecast_context=forecast_recommendation,
        top_n=12,
    )
    waste_detection = build_waste_detection_context(reference_date=decision_target_date, top_n=12)
    opportunity_detection = build_opportunity_context(
        target_date=decision_target_date,
        forecast_context=forecast_recommendation,
        production_context=production_recommendation,
        waste_context=waste_detection,
        top_n=12,
    )
    decision_score = build_decision_score_context(
        target_date=decision_target_date,
        forecast_context=forecast_recommendation,
        production_context=production_recommendation,
        waste_context=waste_detection,
        opportunity_context=opportunity_detection,
        top_n=12,
    )

    return {
        "months_window": months_window,
        "dashboard_exec_ready": True,
        "daily_sales_snapshot": daily_sales_snapshot,
        "purchase_snapshot": purchase_snapshot,
        "criticos_count": int(inventory_metrics.get("criticos_count") or 0),
        "bajo_reorden_count": int(inventory_metrics.get("bajo_reorden_count") or 0),
        "dataset_sales": sales_dataset,
        "dataset_daily_ops": daily_ops_dataset,
        "dataset_production": dataset_production,
        "executive_panels": executive_panels,
        "forecast_panel": executive_panels.get("forecast_panel") or {},
        "yoy_panel": executive_panels.get("yoy_panel") or {},
        "profitability_panel": executive_panels.get("profitability_panel") or {},
        "production_sales_panel": production_panel,
        "central_flow_panel": executive_panels.get("central_flow_panel") or {},
        "inventory_ledger_panel": executive_panels.get("inventory_ledger_panel") or {},
        "decision_support": {
            "forecast": forecast_recommendation,
            "production": production_recommendation,
            "waste": waste_detection,
            "opportunities": opportunity_detection,
            "score": decision_score,
        },
        "kpi_summary": {
            "sales_amount": daily_sales_snapshot.get("total_amount") or "0",
            "sales_tickets": daily_sales_snapshot.get("total_tickets") or 0,
            "forecast_amount": (executive_panels.get("forecast_panel") or {}).get("forecast_amount") or "0",
            "yoy_delta_pct": ((executive_panels.get("yoy_panel") or {}).get("hero_row") or {}).get("amount_delta_pct"),
            "criticos_count": int(inventory_metrics.get("criticos_count") or 0),
            "bajo_reorden_count": int(inventory_metrics.get("bajo_reorden_count") or 0),
            "ordenes_abiertas": int(purchase_snapshot.get("ordenes_abiertas") or 0),
        },
    }


def get_materialized_dashboard_full_payload(*, months_window: int) -> dict[str, object] | None:
    normalized_months = normalize_dashboard_months_window(months_window)
    sql = """
    SELECT payload
    FROM mv_dashboard_full
    WHERE months_window = %s
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, [normalized_months])
            row = cursor.fetchone()
    except (OperationalError, ProgrammingError):
        return None
    if not row:
        return None
    return _hydrate_dashboard_full_payload(_coerce_json(row[0]))
