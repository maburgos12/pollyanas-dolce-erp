from __future__ import annotations

from datetime import date

from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync
from pos_bridge.utils.dates import resolve_incremental_window


def run_daily_sales_sync(
    *,
    triggered_by=None,
    branch_filter: str | None = None,
    lookback_days: int = 3,
    lag_days: int = 1,
    anchor_date: date | None = None,
    excluded_ranges: list[tuple[date, date]] | None = None,
):
    start_date, end_date = resolve_incremental_window(
        anchor_date=anchor_date,
        lookback_days=lookback_days,
        lag_days=lag_days,
    )
    return run_sales_history_sync(
        start_date=start_date,
        end_date=end_date,
        excluded_ranges=excluded_ranges,
        triggered_by=triggered_by,
        branch_filter=branch_filter,
    )
