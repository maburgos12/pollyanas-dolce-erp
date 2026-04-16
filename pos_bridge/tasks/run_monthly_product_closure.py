from __future__ import annotations

from datetime import date, timedelta

from django.utils import timezone

from pos_bridge.services.product_month_closure_service import ProductMonthClosureService
from recetas.models import ProductoMonthClosure


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


def run_monthly_product_closure(
    *,
    month: str | date | None = None,
    anchor_date: date | None = None,
    triggered_by=None,
    rebuild: bool = False,
    lock_after_build: bool = False,
) -> dict[str, object]:
    target_month = _resolve_target_month(month=month, anchor_date=anchor_date)
    existing = ProductoMonthClosure.objects.filter(month_start=target_month).order_by("-id").first()
    if existing is not None and not rebuild:
        validation = dict((existing.metadata or {}).get("validation") or {})
        return {
            "action": "skipped_existing",
            "month": target_month.strftime("%Y-%m"),
            "closure_id": existing.id,
            "closure_status": existing.status,
            "is_locked": existing.is_locked,
            "lock_ready": bool(validation.get("lock_ready")),
        }

    closure = ProductMonthClosureService().build(
        month=target_month,
        rebuild=rebuild,
        lock_after_build=lock_after_build,
        built_by=triggered_by,
        approval_reason="scheduled_monthly_automation",
        approval_channel="celery_monthly_product_closure",
    )
    validation = dict((closure.metadata or {}).get("validation") or {})
    return {
        "action": "built",
        "month": target_month.strftime("%Y-%m"),
        "closure_id": closure.id,
        "closure_status": closure.status,
        "is_locked": closure.is_locked,
        "lock_ready": bool(validation.get("lock_ready")),
    }
