from __future__ import annotations

from datetime import date
from decimal import Decimal

from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.core.management import call_command
from django.db.models import Sum
from django.utils import timezone

from core.audit import log_event
from pos_bridge.models import PointDailyBranchIndicator, PointSyncJob
from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.canonical_insumo_inventory_capture_service import CanonicalInsumoInventoryCaptureService
from pos_bridge.services.open_transfer_sync_service import OpenTransferSyncService
from pos_bridge.services.product_recipe_sync_service import PointProductRecipeSyncService
from pos_bridge.services.realtime_inventory_service import deliver_ecommerce_webhook, run_realtime_inventory_sync
from pos_bridge.tasks.retry_failed_jobs import retry_failed_jobs
from pos_bridge.tasks.run_attendance_sync import run_attendance_sync
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


@shared_task(name="pos_bridge.canonical_insumo_inventory_sync", acks_late=True)
def task_canonical_insumo_inventory_sync():
    now = timezone.now()
    parameters = {
        "canonical_insumo_inventory": True,
        "locations": ["ALMACEN", "CEDIS"],
    }
    job = PointSyncJob.objects.create(
        job_type=PointSyncJob.JOB_TYPE_INVENTORY,
        status=PointSyncJob.STATUS_RUNNING,
        started_at=now,
        parameters=parameters,
        attempt_count=1,
    )
    try:
        with point_account_session_lock(wait=True):
            result = CanonicalInsumoInventoryCaptureService().capture(sync_job=job, captured_at=now)
        job.status = PointSyncJob.STATUS_SUCCESS if result["complete"] else PointSyncJob.STATUS_PARTIAL
        job.result_summary = result
    except Exception as exc:
        job.status = PointSyncJob.STATUS_FAILED
        job.error_message = str(exc)
        job.result_summary = {"complete": False, "blockers": [{"reason": str(exc)}]}
    job.finished_at = timezone.now()
    job.save(update_fields=["status", "result_summary", "error_message", "finished_at", "updated_at"])
    return {"job_id": job.id, "status": job.status, **job.result_summary}


@shared_task(name="pos_bridge.delivery_note_sync", acks_late=True)
def delivery_note_sync(*, lookback_days: int = 7):
    if not bool(getattr(settings, "POINT_DELIVERY_SYNC_ENABLED", False)):
        return {
            "status": "NEVER_RUN",
            "counts": {"seen": 0, "created": 0, "existing": 0, "failed": 0},
            "error_code": "SYNC_DISABLED",
            "job_id": None,
        }
    from crm.services.point_delivery_auto_sync import PointDeliveryAutoSyncService

    return PointDeliveryAutoSyncService().run(lookback_days=lookback_days)

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


@shared_task(
    name="pos_bridge.attendance_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    acks_late=True,
)
def task_attendance_sync(
    self,
    *,
    days: int = 2,
    lag_days: int = 0,
    branch_filter: str | None = None,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_attendance_sync,
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
    time_limit=7200,
    soft_time_limit=6900,
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
    result = run_monthly_product_closure(
        month=month,
        triggered_by=user,
        rebuild=rebuild,
        lock_after_build=lock_after_build,
        sync_inventory_before_build=sync_inventory_before_build,
    )
    if result.get("retryable"):
        if self.request.retries < self.max_retries:
            raise self.retry(
                exc=RuntimeError(
                    f"Fallo transitorio al refrescar fuentes Point del cierre {result.get('month') or month or ''}."
                )
            )
        result = {**result, "retry_exhausted": True}
    return result


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
    articulo_codes: list[str] | None = None,
    include_without_recipe: bool = False,
    triggered_by_id: int | None = None,
):
    return _run_with_optional_user(
        run_product_recipe_sync,
        triggered_by_id=triggered_by_id,
        branch_hint=branch_hint,
        product_codes=product_codes,
        articulo_codes=articulo_codes,
        include_without_recipe=include_without_recipe,
    )


@shared_task(
    name="pos_bridge.catalog_recipe_sync",
    bind=True,
    max_retries=2,
    default_retry_delay=600,
    acks_late=True,
    soft_time_limit=14100,
    time_limit=14400,
)
def task_catalog_recipe_sync(self, *, job_id: int):
    job = PointSyncJob.objects.select_related("triggered_by").get(id=job_id)
    if job.status == PointSyncJob.STATUS_SUCCESS:
        return _serialize_job(job)
    parameters = dict(job.parameters or {})
    action = parameters.get("action")
    branch_hint = parameters.get("branch_hint") or "MATRIZ"
    include_without_recipe = bool(parameters.get("include_without_recipe"))
    discovery = None

    initial_stage = "DISCOVERING" if action == "SYNC_ONLY_NEW_PRODUCTS" else "IMPORTING"
    initial_detail = (
        "Revisando el catálogo de Point para detectar productos nuevos con receta/BOM."
        if action == "SYNC_ONLY_NEW_PRODUCTS"
        else "Descargando y materializando las recetas/BOM vigentes de Point."
    )
    parameters["progress"] = {"stage": initial_stage, "detail": initial_detail}
    job.parameters = parameters
    job.status = PointSyncJob.STATUS_RUNNING
    job.started_at = job.started_at or timezone.now()
    job.attempt_count = int(job.attempt_count or 0) + 1
    job.error_message = ""
    job.save(
        update_fields=[
            "parameters",
            "status",
            "started_at",
            "attempt_count",
            "error_message",
            "updated_at",
        ]
    )

    service = PointProductRecipeSyncService()
    try:
        product_codes = None
        if action == "SYNC_ONLY_NEW_PRODUCTS":
            discovery = service.discover_new_product_codes(branch_hint=branch_hint)
            product_codes = list(discovery.get("new_codes") or [])
            if not product_codes:
                summary = {
                    "products_selected": 0,
                    "recipes_completed_successfully": 0,
                    "recipes_with_unresolved_inputs": 0,
                    "new_products_imported": 0,
                    "new_preparations_imported": 0,
                    "unresolved_inputs_count": 0,
                    "discovery": discovery,
                }
                parameters["progress"] = {
                    "stage": "COMPLETED",
                    "detail": "La revisión terminó; Point no reportó productos nuevos importables.",
                }
                job.parameters = parameters
                job.status = PointSyncJob.STATUS_SUCCESS
                job.finished_at = timezone.now()
                job.result_summary = summary
                job.save(
                    update_fields=[
                        "parameters",
                        "status",
                        "finished_at",
                        "result_summary",
                        "updated_at",
                    ]
                )
                log_event(
                    job.triggered_by,
                    "SYNC_POINT_RECIPES",
                    "pos_bridge.PointSyncJob",
                    job.id,
                    {"action": action, "product_codes": [], "summary": summary},
                )
                return _serialize_job(job)
            parameters["progress"] = {
                "stage": "IMPORTING",
                "detail": f"Importando receta/BOM de {len(product_codes)} producto(s) nuevo(s).",
                "products_found": len(product_codes),
            }
            job.parameters = parameters
            job.save(update_fields=["parameters", "updated_at"])
        elif action != "SYNC_ALL_RECIPES":
            raise ValueError(f"Unsupported catalog recipe sync action: {action}")

        result = service.sync(
            branch_hint=branch_hint,
            product_codes=product_codes,
            include_without_recipe=include_without_recipe,
            sync_job=job,
        )
        parameters["progress"] = {
            "stage": "COSTING",
            "detail": "Las recetas ya se incorporaron; recalculando el corte semanal de costos.",
            "products_processed": int((result.summary or {}).get("products_selected") or 0),
        }
        job.parameters = parameters
        job.save(update_fields=["parameters", "updated_at"])
        snapshot = run_weekly_cost_snapshot(triggered_by=job.triggered_by)
        summary = dict(result.summary or {})
        if discovery is not None:
            summary["discovery"] = discovery
        summary["weekly_cost_snapshot"] = snapshot
        parameters["progress"] = {
            "stage": "COMPLETED",
            "detail": "Catálogo, recetas/BOM y corte semanal de costos actualizados.",
            "products_processed": int(summary.get("products_selected") or 0),
        }
        job.parameters = parameters
        job.status = PointSyncJob.STATUS_SUCCESS
        job.finished_at = timezone.now()
        job.result_summary = summary
        job.artifacts = {"raw_export_path": result.raw_export_path}
        job.save(
            update_fields=[
                "status",
                "parameters",
                "finished_at",
                "result_summary",
                "artifacts",
                "updated_at",
            ]
        )
        log_event(
            job.triggered_by,
            "SYNC_POINT_RECIPES",
            "pos_bridge.PointSyncJob",
            job.id,
            {
                "action": action,
                "product_codes": product_codes or [],
                "summary": summary,
                "raw_export_path": result.raw_export_path,
            },
        )
        return _serialize_job(job)
    except Exception as exc:
        parameters["progress"] = {
            "stage": "FAILED",
            "detail": "La actualización se detuvo; revisa el error antes de volver a intentarlo.",
        }
        job.parameters = parameters
        job.status = PointSyncJob.STATUS_FAILED
        job.finished_at = timezone.now()
        job.error_message = str(exc)
        job.save(update_fields=["parameters", "status", "finished_at", "error_message", "updated_at"])
        log_event(
            job.triggered_by,
            "SYNC_POINT_RECIPES_FAILED",
            "pos_bridge.PointSyncJob",
            job.id,
            {"action": action, "error": str(exc)},
        )
        raise


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

    today = timezone.localdate()
    df = date.fromisoformat(date_from) if date_from else today.replace(day=1)
    dt = date.fromisoformat(date_to) if date_to else today - timedelta(days=1)
    if date_to is None and date_from is None and today.day == 1:
        df = dt.replace(day=1)

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
