from __future__ import annotations

from datetime import date, datetime

from django.db import connection

PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE = 1_347_901_004


def month_start(value: date | datetime) -> date:
    if isinstance(value, datetime):
        value = value.date()
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


def snapshot_affected_months(captured_at: date | datetime) -> tuple[date, date]:
    current = month_start(captured_at)
    return current, next_month(current)
