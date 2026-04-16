from __future__ import annotations

from datetime import date

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.official_sales_backfill_service import OfficialSalesBackfillService
from pos_bridge.services.sync_service import PointSyncService


def run_sales_history_sync(
    *,
    start_date: date,
    end_date: date,
    excluded_ranges: list[tuple[date, date]] | None = None,
    triggered_by=None,
    branch_filter: str | None = None,
    max_days: int | None = None,
    source_mode: str | None = None,
    credito_scopes: list[str] | None = None,
):
    settings = load_point_bridge_settings()
    resolved_mode = str(source_mode or settings.sales_sync_source_mode or "OFFICIAL").strip().upper()
    if resolved_mode == "LEGACY":
        service = PointSyncService()
        return service.run_sales_sync(
            start_date=start_date,
            end_date=end_date,
            excluded_ranges=excluded_ranges,
            triggered_by=triggered_by,
            branch_filter=branch_filter,
            max_days=max_days,
        )

    service = OfficialSalesBackfillService()
    return service.run(
        start_date=start_date,
        end_date=end_date,
        branch_filter=branch_filter,
        credito_scopes=credito_scopes or settings.sales_sync_credito_scopes,
        excluded_ranges=excluded_ranges,
        max_days=max_days,
        triggered_by=triggered_by,
    )
