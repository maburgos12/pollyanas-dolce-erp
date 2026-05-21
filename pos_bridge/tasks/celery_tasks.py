from __future__ import annotations

from datetime import date
from decimal import Decimal

from celery import shared_task
from django.core.cache import cache
from django.core.management import call_command
from django.db.models import Sum

from core.audit import log_event
from pos_bridge.models import PointDailyBranchIndicator, PointSyncJob
from pos_bridge.services.open_transfer_sync_service import OpenTransferSyncService
from pos_bridge.services.realtime_inventory_service import deliver_ecommerce_webhook, run_realtime_inventory_sync
from pos_bridge.tasks.retry_failed_jobs import retry_failed_jobs
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync
from pos_bridge.tasks.run_inventory_sync import run_inventory_sync
from pos_bridge.tasks.run_monthly_product_closure import run_monthly_product_closure
from pos_bridge.tasks.run_production_sync import run_production_sync
from pos_bridge.tasks.run_product_recipe_sync import run_product_recipe_sync
from pos_bridge.tasks.run_recipe_gap_audit import run_recipe_gap_audit
from pos_bridge.tasks.run_transfer_sync import run_transfer_sync
from pos_bridge.tasks.run_waste_sync import run_waste_sync
from pos_bridge.tasks.run_weekly_cost_snapshot import run_weekly_cost_snapshot
from reportes.analytics_service import refresh_dashboard_full_materialized_view
from reportes.dashboard_full_dataset import get_materialized_dashboard_full_payload
from reportes.models import AnalyticAuditLog, FactVentaDiaria

BI_FORCE_REFRESH_LOCK_KEY = "reportes:bi-force-refresh-lock"
INTEGRATIONS_ANALYTICS_REFRESH_LOCK_KEY = "integraciones:analytics-refresh-lock"
INTEGRATIONS_OPERATIONAL_REFRESH_LOCK_KEY = "integraciones:operational-refresh-lock"
VISIBLE_CUT_REFRESH_TOLERANCE = Decimal("0.01")


def _decimal_total(value) -> Decimal:
    return Decimal(str(value or 0)).quantize(VISIBLE_CUT_REFRESH_TOLERANCE)


def _totals_match(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= VISIBLE_CUT_REFRESH_TOLERANCE


def _coerce_snapshot_date(value):
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _read_materialized_visible_cut(*, months_window: int = 6) -> dict[str, object]:
    payload = get_materialized_dashboard_full_payload(months_window=months_window) or {}
    snapshot = dict(payload.get("daily_sales_snapshot") or {})
    return {
        "date": _coerce_snapshot_date(snapshot.get("date")),
        "total_amount": _decimal_total(snapshot.get("total_amount")),
    }


def _validate_visible_cut_refresh(*, reference_date: date) -> dict[str, object]:
    fact_total = _decimal_total(
        FactVentaDiaria.objects.filter(fecha=reference_date).aggregate(total=Sum("venta_total")).get("total")
    )
    indicator_total = _decimal_total(
        PointDailyBranchIndicator.objects.filter(indicator_date=reference_date).aggregate(total=Sum("total_amount")).get("total")
    )

    snapshot = _read_materialized_visible_cut()
    if snapshot["date"] != reference_date or not _totals_match(snapshot["total_amount"], fact_total):
        refresh_dashboard_full_materialized_view(months_windows=(6,), concurrently=False)
        snapshot = _read_materialized_visible_cut()

    if indicator_total > 0 and not _totals_match(fact_total, indicator_total):
        raise RuntimeError(
            "Visible cut mismatch after sync: "
            f"fact={fact_total} indicator={indicator_total} date={reference_date.isoformat()}"
        )
    if snapshot["date"] != reference_date:
        raise RuntimeError(
            "Visible cut snapshot date mismatch after sync: "
            f"expected={reference_date.isoformat()} got={snapshot['date']}"
        )
    if not _totals_match(snapshot["total_amount"], fact_total):
        raise RuntimeError(
            "Visible cut snapshot total mismatch after sync: "
            f"fact={fact_total} snapshot={snapshot['total_amount']} date={reference_date.isoformat()}"
        )

    return {
        "fact_total": f"{fact_total:.2f}",
        "indicator_total": f"{indicator_total:.2f}",
        "materialized_total": f"{snapshot['total_amount']:.2f}",
        "materialized_date": snapshot["date"].isoformat() if snapshot["date"] else "",
    }


def _record_visible_cut_audit(
    *,
    reference_date: date,
    status: str,
    message: str,
    payload: dict[str, object],
    discrepancy_count: int = 0,
) -> None:
    AnalyticAuditLog.objects.create(
        audit_type="VISIBLE_CUT_REFRESH",
        status=status,
        date_from=reference_date,
        date_to=reference_date,
        discrepancy_count=max(int(discrepancy_count or 0), 0),
        message=message,
        payload=payload,
    )


@shared_task(name="pos_bridge.daily_sales_sync", bind=True, max_retries=2, default_retry_delay=300, acks_late=True)
def task_daily_sales_sync(self, *, days: int = 3, lag_days: int = 1, branch_filter: str | None = None, triggered_by_id: int | None = None):
    return _run_with_optional_user(
        run_daily_sales_sync,
        triggered_by_id=triggered_by_id,
        branch_filter=branch_filter,
        lookback_days=days,
        lag_days=lag_days,
    )


@shared_task(name="pos_bridge.inventory_sync", bind=True, max_retries=2, default_retry_delay=300, acks_late=True)
def task_inventory_sync(
    self,
    *,
    branch_filter: str | None = None,
    limit_branches: int | None = None,
    capture_costs: bool | None = None,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_inventory_sync,
        triggered_by_id=triggered_by_id,
        branch_filter=branch_filter,
        limit_branches=limit_branches,
        capture_costs=capture_costs,
    )


@shared_task(name="pos_bridge.purchase_resale_cost_sync", bind=True, max_retries=1, default_retry_delay=600, acks_late=True)
def task_purchase_resale_cost_sync(
    self,
    *,
    dias: int = 365,
    max_compras: int = 900,
):
    from pos_bridge.services.point_purchase_resale_cost_service import PointPurchaseResaleCostSyncService

    result = PointPurchaseResaleCostSyncService().sync_from_point(
        dias=dias,
        max_compras=max_compras,
        apply=True,
    )
    return {
        "purchases_seen": result.purchases_seen,
        "details_seen": result.details_seen,
        "matched_products": result.matched_products,
        "created": result.created,
        "existing": result.existing,
        "zero_or_invalid_cost": result.zero_or_invalid_cost,
        "unresolved": result.unresolved,
        "products": sorted(result.imported_products)[:80],
    }


@shared_task(
    name="pos_bridge.realtime_inventory_sync",
    bind=True,
    max_retries=1,
    default_retry_delay=120,
    acks_late=True,
    time_limit=600,
    soft_time_limit=540,
)
def task_realtime_inventory_sync(self, *, force: bool = False, triggered_by_id: int | None = None):
    jobs = _run_with_optional_user(
        run_realtime_inventory_sync,
        triggered_by_id=triggered_by_id,
        force=force,
        return_jobs=True,
    )
    return {
        "jobs": [
            {"job_id": job.id, "status": job.status, "summary": job.result_summary}
            for job in jobs
        ]
    }


@shared_task(
    name="pos_bridge.ecommerce_webhook_delivery",
    bind=True,
    max_retries=3,
    default_retry_delay=15,
    acks_late=True,
    time_limit=30,
    soft_time_limit=20,
)
def task_ecommerce_webhook_delivery(self, *, webhook_url: str, payload: dict):
    try:
        deliver_ecommerce_webhook(webhook_url=webhook_url, payload=payload, timeout_seconds=5)
    except Exception as exc:
        raise self.retry(exc=exc)
    return {"delivered": True, "event": payload.get("event", "")}


@shared_task(
    name="pos_bridge.monthly_product_closure",
    bind=True,
    max_retries=1,
    default_retry_delay=900,
    acks_late=True,
    time_limit=1800,
)
def task_monthly_product_closure(
    self,
    *,
    month: str | None = None,
    rebuild: bool = False,
    lock_after_build: bool = False,
    sync_inventory_before_build: bool = False,
    triggered_by_id: int | None = None,
):
    user = _resolve_user(triggered_by_id)
    return run_monthly_product_closure(
        month=month,
        triggered_by=user,
        rebuild=rebuild,
        lock_after_build=lock_after_build,
        sync_inventory_before_build=sync_inventory_before_build,
    )


@shared_task(
    name="pos_bridge.product_recipe_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=600,
    acks_late=True,
    time_limit=1800,
)
def task_product_recipe_sync(
    self,
    *,
    branch_hint: str | None = None,
    product_codes: list[str] | None = None,
    include_without_recipe: bool = False,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_product_recipe_sync,
        triggered_by_id=triggered_by_id,
        branch_hint=branch_hint,
        product_codes=product_codes,
        include_without_recipe=include_without_recipe,
    )


@shared_task(name="pos_bridge.retry_failed_jobs", acks_late=True)
def task_retry_failed_jobs(*, limit: int = 3):
    jobs = retry_failed_jobs(limit=limit)
    return {
        "retried_count": len(jobs),
        "jobs": [{"job_id": job.id, "status": job.status} for job in jobs],
    }


@shared_task(
    name="pos_bridge.sync_product_prices_task",
    bind=True,
    max_retries=1,
    default_retry_delay=600,
    acks_late=True,
    time_limit=1800,
)
def task_sync_product_prices(self):
    call_command("sync_product_prices")
    return {"status": "ok"}


@shared_task(
    name="pos_bridge.recipe_gap_audit",
    bind=True,
    max_retries=1,
    default_retry_delay=600,
    acks_late=True,
    time_limit=1800,
)
def task_recipe_gap_audit(self, *, branch_hint: str | None = None, product_codes: list[str] | None = None, limit: int | None = None, triggered_by_id: int | None = None):
    return _run_with_optional_user(
        run_recipe_gap_audit,
        triggered_by_id=triggered_by_id,
        branch_hint=branch_hint,
        product_codes=product_codes,
        limit=limit,
    )




@shared_task(
    name="pos_bridge.weekly_cost_snapshot",
    bind=True,
    max_retries=1,
    default_retry_delay=600,
    acks_late=True,
    time_limit=1800,
)
def task_weekly_cost_snapshot(
    self,
    *,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_weekly_cost_snapshot,
        triggered_by_id=triggered_by_id,
    )


@shared_task(
    name="pos_bridge.waste_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    acks_late=True,
    time_limit=1800,
)
def task_waste_sync(
    self,
    *,
    days: int = 1,
    lag_days: int = 1,
    branch_filter: str | None = None,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_waste_sync,
        triggered_by_id=triggered_by_id,
        branch_filter=branch_filter,
        lookback_days=days,
        lag_days=lag_days,
    )


@shared_task(
    name="pos_bridge.production_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    acks_late=True,
    time_limit=1800,
)
def task_production_sync(
    self,
    *,
    days: int = 1,
    lag_days: int = 1,
    branch_filter: str | None = None,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_production_sync,
        triggered_by_id=triggered_by_id,
        branch_filter=branch_filter,
        lookback_days=days,
        lag_days=lag_days,
    )


@shared_task(
    name="pos_bridge.transfer_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    acks_late=True,
    time_limit=1800,
)
def task_transfer_sync(
    self,
    *,
    days: int = 1,
    lag_days: int = 1,
    branch_filter: str | None = None,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_transfer_sync,
        triggered_by_id=triggered_by_id,
        branch_filter=branch_filter,
        lookback_days=days,
        lag_days=lag_days,
    )


@shared_task(
    name="pos_bridge.open_transfer_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    acks_late=True,
    time_limit=1800,
)
def task_open_transfer_sync(
    self,
    *,
    fecha_operacion: str | None = None,
    branch_filter: str | None = None,
    triggered_by_id: int | None = None,
):
    fecha = date.fromisoformat(fecha_operacion) if fecha_operacion else date.today()
    user = _resolve_user(triggered_by_id)
    job = OpenTransferSyncService().sync_open_transfers(
        fecha=fecha,
        branch_filter=branch_filter,
        triggered_by=user,
    )
    return _serialize_job(job)


@shared_task(
    name="reportes.analytics_refresh_cycle",
    bind=True,
    max_retries=1,
    default_retry_delay=600,
    acks_late=True,
    time_limit=2400,
    soft_time_limit=2100,
)
def task_analytics_refresh_cycle(
    self,
    *,
    reference_date_iso: str | None = None,
    lookback_days: int = 7,
    months: int = 6,
    triggered_by_id: int | None = None,
):
    reference_date = reference_date_iso or date.today().isoformat()
    triggered_by = _resolve_user(triggered_by_id)
    payload = {
        "reference_date": reference_date,
        "lookback_days": int(lookback_days or 7),
        "months": int(months or 6),
        "triggered_by_id": triggered_by_id,
    }
    try:
        call_command(
            "refresh_analytics_layer",
            date=reference_date,
            lookback_days=int(lookback_days or 7),
            months=int(months or 6),
        )
        log_event(
            triggered_by,
            "INTEGRATIONS_ANALYTICS_REFRESH_COMPLETED",
            "reportes.AnalyticRefreshWindow",
            reference_date,
            payload=payload,
        )
    except Exception as exc:
        log_event(
            triggered_by,
            "INTEGRATIONS_ANALYTICS_REFRESH_FAILED",
            "reportes.AnalyticRefreshWindow",
            reference_date,
            payload={**payload, "error": str(exc)},
        )
        raise
    finally:
        cache.delete(INTEGRATIONS_ANALYTICS_REFRESH_LOCK_KEY)
    return payload


@shared_task(
    name="reportes.visible_cut_refresh_cycle",
    bind=True,
    max_retries=1,
    default_retry_delay=600,
    acks_late=True,
    time_limit=2400,
    soft_time_limit=2100,
)
def task_visible_cut_refresh_cycle(
    self,
    *,
    reference_date_iso: str | None = None,
    triggered_by_id: int | None = None,
):
    reference_date = date.fromisoformat(reference_date_iso) if reference_date_iso else date.today()
    triggered_by = _resolve_user(triggered_by_id)
    payload = {
        "reference_date": reference_date.isoformat(),
        "lookback_days": 1,
        "lag_days": 0,
        "scope": "visible_cut",
        "triggered_by_id": triggered_by_id,
    }
    try:
        sync_job = run_daily_sales_sync(
            triggered_by=triggered_by,
            lookback_days=1,
            lag_days=0,
            anchor_date=reference_date,
            publish_analytics=True,
        )
        payload["sync_job_id"] = getattr(sync_job, "id", None)
        payload["sync_status"] = getattr(sync_job, "status", "")
        if getattr(sync_job, "status", "") not in {PointSyncJob.STATUS_SUCCESS, PointSyncJob.STATUS_PARTIAL}:
            raise RuntimeError(
                f"Visible cut sync finished with unexpected status {getattr(sync_job, 'status', '') or 'UNKNOWN'}."
            )
        payload.update(_validate_visible_cut_refresh(reference_date=reference_date))
        _record_visible_cut_audit(
            reference_date=reference_date,
            status=AnalyticAuditLog.STATUS_OK,
            message="Visible cut refresh validated against facts, indicators, and dashboard snapshot.",
            payload=payload,
        )
        log_event(
            triggered_by,
            "INTEGRATIONS_OPERATIONAL_REFRESH_COMPLETED",
            "reportes.AnalyticRefreshWindow",
            reference_date.isoformat(),
            payload=payload,
        )
    except Exception as exc:
        _record_visible_cut_audit(
            reference_date=reference_date,
            status=AnalyticAuditLog.STATUS_ERROR,
            message=f"Visible cut refresh failed: {exc}",
            payload={**payload, "error": str(exc)},
            discrepancy_count=1,
        )
        log_event(
            triggered_by,
            "INTEGRATIONS_OPERATIONAL_REFRESH_FAILED",
            "reportes.AnalyticRefreshWindow",
            reference_date.isoformat(),
            payload={**payload, "error": str(exc)},
        )
        raise
    finally:
        cache.delete(BI_FORCE_REFRESH_LOCK_KEY)
        cache.delete(INTEGRATIONS_OPERATIONAL_REFRESH_LOCK_KEY)
    return payload


@shared_task(
    name="reportes.operations_automation_cycle",
    bind=True,
    max_retries=1,
    default_retry_delay=600,
    acks_late=True,
    time_limit=3600,
    soft_time_limit=3300,
)
def task_operations_automation_cycle(
    self,
    *,
    reference_date_iso: str | None = None,
    lookback_days: int = 7,
    sucursal_id: int | None = None,
    skip_refresh: bool = False,
    triggered_by_id: int | None = None,
):
    reference_date = reference_date_iso or date.today().isoformat()
    triggered_by = _resolve_user(triggered_by_id)
    payload = {
        "reference_date": reference_date,
        "lookback_days": int(lookback_days or 7),
        "sucursal_id": sucursal_id,
        "skip_refresh": bool(skip_refresh),
        "triggered_by_id": triggered_by_id,
    }
    try:
        call_command(
            "run_operations_automation",
            fecha=reference_date,
            lookback_days=int(lookback_days or 7),
            sucursal_id=sucursal_id,
            skip_refresh=bool(skip_refresh),
        )
        log_event(
            triggered_by,
            "INTEGRATIONS_OPERATIONAL_REFRESH_COMPLETED",
            "reportes.AnalyticRefreshWindow",
            reference_date,
            payload=payload,
        )
    except Exception as exc:
        log_event(
            triggered_by,
            "INTEGRATIONS_OPERATIONAL_REFRESH_FAILED",
            "reportes.AnalyticRefreshWindow",
            reference_date,
            payload={**payload, "error": str(exc)},
        )
        raise
    finally:
        cache.delete(BI_FORCE_REFRESH_LOCK_KEY)
        cache.delete(INTEGRATIONS_OPERATIONAL_REFRESH_LOCK_KEY)
    return payload


@shared_task(
    name="pos_bridge.conversion_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    acks_late=True,
    time_limit=600,
    soft_time_limit=540,
)
def task_conversion_sync(
    self,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
):
    from datetime import timedelta

    from pos_bridge.services.conversion_sync_service import sync_conversion_lines

    today = date.today()
    df = date.fromisoformat(date_from) if date_from else today.replace(day=1)
    dt = date.fromisoformat(date_to) if date_to else today
    if date_to is None and date_from is None and today.day == 1:
        previous_month_end = today - timedelta(days=1)
        dt = previous_month_end
        df = previous_month_end.replace(day=1)

    try:
        return sync_conversion_lines(date_from=df, date_to=dt)
    except Exception as exc:
        raise self.retry(exc=exc)


def _resolve_user(user_id: int | None):
    if not user_id:
        return None
    from django.contrib.auth import get_user_model

    User = get_user_model()
    try:
        return User.objects.get(id=user_id)
    except User.DoesNotExist:
        return None


def _serialize_job(job):
    return {
        "job_id": job.id,
        "status": job.status,
        "summary": job.result_summary,
        "error_message": job.error_message,
    }


def _run_with_optional_user(func, *, triggered_by_id: int | None = None, return_jobs: bool = False, **kwargs):
    user = _resolve_user(triggered_by_id)
    result = func(triggered_by=user, **kwargs)
    if return_jobs:
        return result or []
    return _serialize_job(result)
