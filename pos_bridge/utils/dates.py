from __future__ import annotations

from datetime import date, timedelta

from django.utils import timezone


def local_now():
    return timezone.localtime(timezone.now())


def timestamp_token() -> str:
    return local_now().strftime("%Y%m%d_%H%M%S")


def iter_business_dates(
    start_date: date,
    end_date: date,
    *,
    excluded_ranges: list[tuple[date, date]] | None = None,
) -> list[date]:
    excluded_ranges = excluded_ranges or []
    cursor = start_date
    dates: list[date] = []
    while cursor <= end_date:
        if not any(excluded_start <= cursor <= excluded_end for excluded_start, excluded_end in excluded_ranges):
            dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def resolve_incremental_window(
    *,
    anchor_date: date | None = None,
    lookback_days: int = 3,
    lag_days: int = 1,
) -> tuple[date, date]:
    current_date = anchor_date or timezone.localdate()
    effective_lookback_days = max(int(lookback_days or 1), 1)
    effective_lag_days = max(int(lag_days or 0), 0)
    end_date = current_date - timedelta(days=effective_lag_days)
    start_date = end_date - timedelta(days=effective_lookback_days - 1)
    return start_date, end_date
