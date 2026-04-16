from __future__ import annotations

from datetime import date

from core.audit import log_event
from pos_bridge.models import PointSyncJob
from reportes.analytics_service import refresh_incremental
from reportes.sales_dashboard_freshness import ensure_sales_dashboard_freshness
from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync
from pos_bridge.utils.dates import resolve_incremental_window


def _analytics_lookback_for_window(*, start_date: date, end_date: date) -> int:
    return max((end_date - start_date).days, 1)


def run_daily_sales_sync(
    *,
    triggered_by=None,
    branch_filter: str | None = None,
    lookback_days: int = 3,
    lag_days: int = 1,
    anchor_date: date | None = None,
    excluded_ranges: list[tuple[date, date]] | None = None,
    source_mode: str | None = None,
    credito_scopes: list[str] | None = None,
    publish_analytics: bool = True,
):
    start_date, end_date = resolve_incremental_window(
        anchor_date=anchor_date,
        lookback_days=lookback_days,
        lag_days=lag_days,
    )
    sync_job = run_sales_history_sync(
        start_date=start_date,
        end_date=end_date,
        excluded_ranges=excluded_ranges,
        triggered_by=triggered_by,
        branch_filter=branch_filter,
        source_mode=source_mode,
        credito_scopes=credito_scopes,
    )
    if publish_analytics and sync_job.status in {PointSyncJob.STATUS_SUCCESS, PointSyncJob.STATUS_PARTIAL}:
        analytics_lookback_days = _analytics_lookback_for_window(start_date=start_date, end_date=end_date)
        payload = {
            "reference_date": end_date.isoformat(),
            "lookback_days": analytics_lookback_days,
            "trigger": "point_daily_sales_sync",
            "sync_job_id": sync_job.id,
        }
        try:
            summary = refresh_incremental(reference_date=end_date, lookback_days=analytics_lookback_days)
        except Exception as exc:
            log_event(
                triggered_by,
                "INTEGRATIONS_ANALYTICS_REFRESH_FAILED",
                "reportes.AnalyticRefreshWindow",
                end_date.isoformat(),
                payload={**payload, "error": str(exc)},
            )
            raise
        result_summary = dict(sync_job.result_summary or {})
        result_summary["analytics_refresh"] = {
            "reference_date": end_date.isoformat(),
            "lookback_days": analytics_lookback_days,
            "sales_rows": int(summary.sales_rows or 0),
            "inventory_rows": int(summary.inventory_rows or 0),
            "production_rows": int(summary.production_rows or 0),
            "forecast_rows": int(summary.forecast_rows or 0),
            "calibration_rows": int(summary.calibration_rows or 0),
            "trigger": "point_daily_sales_sync",
        }
        freshness = ensure_sales_dashboard_freshness(
            reference_date=end_date,
            lookback_days=analytics_lookback_days,
            triggered_by=triggered_by,
            trigger="point_daily_sales_sync",
        )
        result_summary["sales_dashboard_freshness"] = {
            "target_date": freshness.target_date.isoformat() if freshness.target_date else "",
            "point_latest_date": freshness.point_latest_date.isoformat() if freshness.point_latest_date else "",
            "fact_latest_date_before": freshness.fact_latest_date_before.isoformat()
            if freshness.fact_latest_date_before
            else "",
            "fact_latest_date_after": freshness.fact_latest_date_after.isoformat()
            if freshness.fact_latest_date_after
            else "",
            "visible_cut_date_before": freshness.visible_cut_date_before.isoformat()
            if freshness.visible_cut_date_before
            else "",
            "visible_cut_date_after": freshness.visible_cut_date_after.isoformat()
            if freshness.visible_cut_date_after
            else "",
            "catchup_attempted": bool(freshness.catchup_attempted),
            "catchup_succeeded": bool(freshness.catchup_succeeded),
            "lag_days_before": int(freshness.lag_days_before or 0),
            "lag_days_after": int(freshness.lag_days_after or 0),
        }
        sync_job.result_summary = result_summary
        sync_job.save(update_fields=["result_summary", "updated_at"])
        log_event(
            triggered_by,
            "INTEGRATIONS_ANALYTICS_REFRESH_COMPLETED",
            "reportes.AnalyticRefreshWindow",
            end_date.isoformat(),
            payload=payload,
        )
    return sync_job
