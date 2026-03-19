from __future__ import annotations

from datetime import date

from pos_bridge.services.sync_service import PointSyncService


def run_sales_history_sync(
    *,
    start_date: date,
    end_date: date,
    excluded_ranges: list[tuple[date, date]] | None = None,
    triggered_by=None,
    branch_filter: str | None = None,
    max_days: int | None = None,
):
    service = PointSyncService()
    return service.run_sales_sync(
        start_date=start_date,
        end_date=end_date,
        excluded_ranges=excluded_ranges,
        triggered_by=triggered_by,
        branch_filter=branch_filter,
        max_days=max_days,
    )
