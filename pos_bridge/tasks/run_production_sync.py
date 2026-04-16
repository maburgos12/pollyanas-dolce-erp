from __future__ import annotations

from datetime import date

from core.audit import log_event
from pos_bridge.models import PointSyncJob
from reportes.analytics_service import refresh_incremental
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.utils.dates import resolve_incremental_window


def _analytics_lookback_for_window(*, start_date: date, end_date: date) -> int:
    return max((end_date - start_date).days, 1)


def run_production_sync(
    *,
    triggered_by=None,
    branch_filter: str | None = None,
    lookback_days: int = 1,
    lag_days: int = 1,
    anchor_date: date | None = None,
    publish_analytics: bool = True,
):
    start_date, end_date = resolve_incremental_window(
        anchor_date=anchor_date,
        lookback_days=lookback_days,
        lag_days=lag_days,
    )
    service = PointMovementSyncService()
    sync_job = service.run_production_sync(
        start_date=start_date,
        end_date=end_date,
        branch_filter=branch_filter,
        triggered_by=triggered_by,
    )
    if publish_analytics and sync_job.status == PointSyncJob.STATUS_SUCCESS:
        analytics_lookback_days = _analytics_lookback_for_window(start_date=start_date, end_date=end_date)
        payload = {
            "reference_date": end_date.isoformat(),
            "lookback_days": analytics_lookback_days,
            "trigger": "point_production_sync",
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
            "trigger": "point_production_sync",
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
