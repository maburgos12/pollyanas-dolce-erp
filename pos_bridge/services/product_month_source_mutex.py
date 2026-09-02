from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from django.conf import settings
from django.db import connection
from django.utils import timezone

PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE = 1_347_901_004
POINT_BUSINESS_TIMEZONE = ZoneInfo("America/Mazatlan")


def _business_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        if timezone.is_aware(value):
            value = timezone.localtime(value, POINT_BUSINESS_TIMEZONE)
        return value.date()
    return value


def month_start(value: date | datetime) -> date:
    value = _business_date(value)
    return date(value.year, value.month, 1)


def next_month(value: date) -> date:
    value = month_start(value)
    return date(value.year + (value.month == 12), 1 if value.month == 12 else value.month + 1, 1)


def months_in_range(start: date, end: date) -> list[date]:
    cursor, last, result = month_start(start), month_start(end), []
    while cursor <= last:
        result.append(cursor)
        cursor = next_month(cursor)
    return result


def lock_product_month_sources(months) -> tuple[date, ...]:
    """Lock authoritative monthly facts in deterministic order until commit."""
    if connection.vendor != "postgresql":
        raise RuntimeError("El mutex mensual de fuentes requiere PostgreSQL.")
    if not connection.in_atomic_block:
        raise RuntimeError("El mutex mensual de fuentes requiere transaction.atomic().")
    ordered = tuple(sorted({month_start(value) for value in months}))
    with connection.cursor() as cursor:
        for value in ordered:
            cursor.execute("SELECT pg_advisory_xact_lock(%s, %s)", [
                PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE, value.year * 100 + value.month,
            ])
    return ordered


def snapshot_affected_months(captured_at: date | datetime) -> tuple[date, ...]:
    """Coordinate every month-end whose canonical window accepts this capture."""
    captured_date = _business_date(captured_at)
    tolerance = max(0, int(getattr(settings, "PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS", 3)))
    earliest = captured_date - timedelta(days=tolerance)
    latest = captured_date + timedelta(days=tolerance)
    affected = set()
    for candidate in months_in_range(earliest, latest):
        target = date(candidate.year, candidate.month, monthrange(candidate.year, candidate.month)[1])
        if earliest <= target <= latest:
            affected.update((candidate, next_month(candidate)))
    return tuple(sorted(affected))
