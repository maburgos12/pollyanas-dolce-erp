from __future__ import annotations

from celery import shared_task

from pos_bridge.services.realtime_inventory_service import run_realtime_inventory_sync
from pos_bridge.tasks.retry_failed_jobs import retry_failed_jobs
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync
from pos_bridge.tasks.run_inventory_sync import run_inventory_sync
from pos_bridge.tasks.run_production_sync import run_production_sync
from pos_bridge.tasks.run_product_recipe_sync import run_product_recipe_sync
from pos_bridge.tasks.run_recipe_gap_audit import run_recipe_gap_audit
from pos_bridge.tasks.run_transfer_sync import run_transfer_sync
from pos_bridge.tasks.run_waste_sync import run_waste_sync
from pos_bridge.tasks.run_weekly_cost_snapshot import run_weekly_cost_snapshot


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
def task_inventory_sync(self, *, branch_filter: str | None = None, limit_branches: int | None = None, triggered_by_id: int | None = None):
    return _run_with_optional_user(
        run_inventory_sync,
        triggered_by_id=triggered_by_id,
        branch_filter=branch_filter,
        limit_branches=limit_branches,
    )


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
