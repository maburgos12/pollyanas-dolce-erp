from __future__ import annotations

from datetime import date

from pos_bridge.services.attendance_sync_service import PointAttendanceSyncService
from pos_bridge.utils.dates import resolve_incremental_window


def run_attendance_sync(
    *,
    triggered_by=None,
    branch_filter: str | None = None,
    lookback_days: int = 2,
    lag_days: int = 0,
    anchor_date: date | None = None,
):
    start_date, end_date = resolve_incremental_window(
        anchor_date=anchor_date,
        lookback_days=lookback_days,
        lag_days=lag_days,
    )
    return PointAttendanceSyncService().run_sync(
        start_date=start_date,
        end_date=end_date,
        branch_filter=branch_filter,
        triggered_by=triggered_by,
    )
