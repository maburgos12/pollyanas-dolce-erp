from __future__ import annotations

from calendar import monthrange
from contextlib import contextmanager
from datetime import date, timedelta

from django.db import connection
from django.utils import timezone

from pos_bridge.models import PointSyncJob
from pos_bridge.services.conversion_sync_service import sync_conversion_lines
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.product_month_closure_service import ProductMonthClosureError, ProductMonthClosureService
from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync
from recetas.models import ProductoMonthClosure


MONTHLY_PRODUCT_CLOSURE_LOCK_NAMESPACE = 1_347_901_003


@contextmanager
def _monthly_product_closure_mutex(month_start: date):
    """Hold one PostgreSQL session lock for a specific closure month."""
    if connection.vendor != "postgresql":
        raise RuntimeError("La orquestación mensual de Point requiere PostgreSQL.")
    month_key = month_start.year * 100 + month_start.month
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_try_advisory_lock(%s, %s)",
            [MONTHLY_PRODUCT_CLOSURE_LOCK_NAMESPACE, month_key],
        )
        acquired = bool(cursor.fetchone()[0])
    try:
        yield acquired
    finally:
        if acquired:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    [MONTHLY_PRODUCT_CLOSURE_LOCK_NAMESPACE, month_key],
                )


def _resolve_target_month(*, month: str | date | None = None, anchor_date: date | None = None) -> date:
    if isinstance(month, date):
        return date(month.year, month.month, 1)
    if isinstance(month, str) and month.strip():
        year_text, month_text = month.strip().split("-", 1)
        return date(int(year_text), int(month_text), 1)

    anchor = anchor_date or timezone.localdate()
    current_month_start = date(anchor.year, anchor.month, 1)
    previous_month_end = current_month_start - timedelta(days=1)
    return date(previous_month_end.year, previous_month_end.month, 1)


def _serialize_source_step(name: str, result) -> dict[str, object]:
    if isinstance(result, dict):
        status = str(result.get("status") or "")
        issues = list(result.get("issues") or [])
        if not status:
            status = PointSyncJob.STATUS_PARTIAL if issues else PointSyncJob.STATUS_SUCCESS
        return {
            "name": name,
            "job_id": result.get("job_id"),
            "status": status,
            "summary": result,
            "error": str(result.get("error") or ""),
            "retryable": bool(result.get("retryable", False)),
        }
    summary = dict(getattr(result, "result_summary", {}) or {})
    return {
        "name": name,
        "job_id": getattr(result, "id", None),
        "status": str(getattr(result, "status", "") or PointSyncJob.STATUS_FAILED),
        "summary": summary,
        "error": str(getattr(result, "error_message", "") or ""),
        "retryable": bool(summary.get("retryable", False)),
    }


def _is_transient_source_error(exc: Exception) -> bool:
    return isinstance(exc, (TimeoutError, ConnectionError, OSError))


def _refresh_month_sources(*, month_start: date, month_end: date, triggered_by=None) -> list[dict[str, object]]:
    """Refresh the four range-capable Point authorities for one exact month.

    Point inventory has no historical range endpoint: opening and closing are
    therefore verified from the persisted Point snapshots by the canonical
    balance build instead of relabelling a current capture as a past one.
    """
    steps: list[dict[str, object]] = []
    movement_service = PointMovementSyncService()
    operations = (
        (
            "sales",
            lambda: run_sales_history_sync(
                start_date=month_start,
                end_date=month_end,
                excluded_ranges=None,
                triggered_by=triggered_by,
                branch_filter=None,
                max_days=None,
                source_mode="OFFICIAL",
            ),
        ),
        (
            "production",
            lambda: movement_service.run_production_sync(
                start_date=month_start,
                end_date=month_end,
                branch_filter=None,
                triggered_by=triggered_by,
            ),
        ),
        (
            "waste",
            lambda: movement_service.run_waste_sync(
                start_date=month_start,
                end_date=month_end,
                branch_filter=None,
                triggered_by=triggered_by,
            ),
        ),
        (
            "conversions",
            lambda: sync_conversion_lines(
                date_from=month_start,
                date_to=month_end,
                branch_filter=None,
            ),
        ),
    )
    # One Point account invalidates its prior session on a new login. Keep the
    # complete multi-report refresh inside the shared cross-service mutex so
    # Domicilios cannot interrupt conversion polling or report downloads.
    with point_account_session_lock(wait=True):
        for name, operation in operations:
            try:
                steps.append(_serialize_source_step(name, operation()))
            except Exception as exc:  # each official service persists its own failed job when available
                steps.append(
                    {
                        "name": name,
                        "job_id": None,
                        "status": PointSyncJob.STATUS_FAILED,
                        "summary": {},
                        "error": str(exc),
                        "retryable": _is_transient_source_error(exc),
                    }
                )
    return steps


def _inventory_authority(
    metadata: dict[str, object],
    *,
    month_start: date,
    month_end: date,
) -> dict[str, object]:
    opening = dict(metadata.get("opening_meta") or {})
    closing = dict(metadata.get("closing_inventory_meta") or {})
    opening.setdefault("target_date", (month_start - timedelta(days=1)).isoformat())
    closing.setdefault("target_date", month_end.isoformat())
    return {
        "opening": opening,
        "closing": closing,
    }


def _run_monthly_product_closure_locked(
    *,
    month: str | date | None = None,
    anchor_date: date | None = None,
    triggered_by=None,
    rebuild: bool = False,
    lock_after_build: bool = False,
    sync_inventory_before_build: bool = False,
) -> dict[str, object]:
    target_month = _resolve_target_month(month=month, anchor_date=anchor_date)
    month_end = date(target_month.year, target_month.month, monthrange(target_month.year, target_month.month)[1])
    existing = ProductoMonthClosure.objects.filter(month_start=target_month).order_by("-id").first()
    if existing is not None and existing.is_locked:
        validation = dict((existing.metadata or {}).get("validation") or {})
        return {
            "action": "skipped_locked",
            "action_label": "Cierre bloqueado; sin cambios",
            "month": target_month.strftime("%Y-%m"),
            "closure_id": existing.id,
            "closure_status": existing.status,
            "closure_status_label": existing.get_status_display(),
            "is_locked": existing.is_locked,
            "lock_ready": bool(validation.get("lock_ready")),
            "inventory_sync": None,
            "inventory_authority": _inventory_authority(
                dict(existing.metadata or {}),
                month_start=target_month,
                month_end=month_end,
            ),
            "source_refresh": [],
            "failed_or_partial_sources": [],
            "automation_status": "LOCKED",
            "automation_reviews": list(validation.get("automation_reviews") or []),
            "retryable": False,
        }

    source_refresh = _refresh_month_sources(
        month_start=target_month,
        month_end=month_end,
        triggered_by=triggered_by,
    )
    failed_or_partial_sources = [
        str(step["name"])
        for step in source_refresh
        if step["status"] != PointSyncJob.STATUS_SUCCESS
    ]
    retryable = any(
        bool(step.get("retryable")) and step.get("status") == PointSyncJob.STATUS_FAILED
        for step in source_refresh
    )

    service = ProductMonthClosureService()
    closure = service.build(
        month=target_month,
        rebuild=bool(existing is not None or rebuild),
        lock_after_build=False,
        built_by=triggered_by,
        approval_reason="scheduled_monthly_automation",
        approval_channel="celery_monthly_product_closure",
    )
    validation = dict((closure.metadata or {}).get("validation") or {})
    automation_reviews = list(validation.get("automation_reviews") or [])
    can_lock = (
        lock_after_build
        and not failed_or_partial_sources
        and bool(validation.get("lock_ready"))
        and not closure.is_locked
    )
    if can_lock:
        try:
            closure = service.lock(
                closure=closure,
                locked_by=triggered_by,
                reason="scheduled_monthly_automation",
                note="",
                channel="celery_monthly_product_closure",
            )
            validation = dict((closure.metadata or {}).get("validation") or validation)
        except ProductMonthClosureError as exc:
            failed_or_partial_sources.append("lock")
            automation_reviews.append(str(exc))
    metadata = dict(closure.metadata or {})
    if closure.is_locked:
        automation_status = "LOCKED"
    elif failed_or_partial_sources or not validation.get("lock_ready"):
        automation_status = "REVIEW"
    else:
        automation_status = "READY"
    return {
        "action": "rebuilt" if existing is not None else "built",
        "action_label": "Cierre reconstruido" if existing is not None else "Cierre construido",
        "month": target_month.strftime("%Y-%m"),
        "closure_id": closure.id,
        "closure_status": closure.status,
        "closure_status_label": closure.get_status_display(),
        "is_locked": closure.is_locked,
        "lock_ready": bool(validation.get("lock_ready")),
        "inventory_sync": None,
        "inventory_authority": _inventory_authority(
            metadata,
            month_start=target_month,
            month_end=month_end,
        ),
        "source_refresh": source_refresh,
        "failed_or_partial_sources": failed_or_partial_sources,
        "automation_status": automation_status,
        "automation_reviews": list(dict.fromkeys(automation_reviews)),
        "retryable": retryable,
    }


def run_monthly_product_closure(
    *,
    month: str | date | None = None,
    anchor_date: date | None = None,
    triggered_by=None,
    rebuild: bool = False,
    lock_after_build: bool = False,
    sync_inventory_before_build: bool = False,
) -> dict[str, object]:
    target_month = _resolve_target_month(month=month, anchor_date=anchor_date)
    with _monthly_product_closure_mutex(target_month) as acquired:
        if not acquired:
            return {
                "action": "already_running",
                "action_label": "Cierre mensual ya en ejecución",
                "month": target_month.strftime("%Y-%m"),
                "closure_id": None,
                "closure_status": None,
                "closure_status_label": "En ejecución",
                "is_locked": False,
                "lock_ready": False,
                "inventory_sync": None,
                "inventory_authority": {},
                "source_refresh": [],
                "failed_or_partial_sources": [],
                "automation_status": "ALREADY_RUNNING",
                "automation_reviews": ["Ya existe una orquestación activa para este mes."],
                "retryable": False,
            }
        return _run_monthly_product_closure_locked(
            month=target_month,
            anchor_date=anchor_date,
            triggered_by=triggered_by,
            rebuild=rebuild,
            lock_after_build=lock_after_build,
            sync_inventory_before_build=sync_inventory_before_build,
        )
