from __future__ import annotations

from calendar import monthrange
from contextlib import contextmanager
from datetime import date, timedelta
from decimal import Decimal

from django.db import connection, transaction
from django.utils import timezone

from core.branch_catalog import canonical_point_network_branch_qs
from inventario.conteos_nocturna import conciliar_conteos_nocturnos
from inventario.models_conteos import ConteoSucursal
from pos_bridge.management.commands import capture_point_historical_closing as historical_closing
from pos_bridge.models import PointHistoricalInventoryClosing, PointSyncJob
from pos_bridge.utils.source_retry import source_error_metadata
from pos_bridge.services.conversion_sync_service import sync_conversion_lines
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.product_month_closure_service import ProductMonthClosureError, ProductMonthClosureService
from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync
from recetas.models import ProductoMonthClosure
from reportes.models import ProductInventoryAuditRun
from reportes.services_inventory_audit_refresh import refresh_inventory_audit_month


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
    return bool(source_error_metadata(exc)["retryable"])


def _capture_historical_closing(*, operational_date: date) -> dict[str, object]:
    existing = (
        PointHistoricalInventoryClosing.objects.filter(
            operational_date=operational_date,
            status=PointHistoricalInventoryClosing.STATUS_VERIFIED,
        )
        .order_by("-id")
        .first()
    )
    if existing is not None:
        return {
            "status": PointSyncJob.STATUS_SUCCESS,
            "closing_id": existing.id,
            "resolved": existing.lines.count(),
            "unresolved": 0,
            "reused": True,
        }

    result = historical_closing.HistoricalPointInventoryClosingCapture(
        client=historical_closing.PointHttpSessionClient(
            historical_closing.load_point_bridge_settings()
        )
    ).capture(
        operational_date=operational_date,
        branches=historical_closing.select_default_branches(operational_date),
        products=historical_closing.select_default_products(),
    )
    verified = (
        result.closing.status == PointHistoricalInventoryClosing.STATUS_VERIFIED
        and result.unresolved_count == 0
    )
    return {
        "status": PointSyncJob.STATUS_SUCCESS if verified else PointSyncJob.STATUS_PARTIAL,
        "closing_id": result.closing.id,
        "resolved": result.resolved_count,
        "unresolved": result.unresolved_count,
        "reused": False,
        "issues": [] if verified else [f"Cierre histórico con {result.unresolved_count} pendientes."],
    }


def _refresh_month_sources(*, month_start: date, month_end: date, triggered_by=None) -> list[dict[str, object]]:
    """Refresh the range-capable Point authorities and the exact final stock.

    Point inventory has no historical range report, so the final balance is
    reconstructed from Point's official stock history instead of relabelling a
    current snapshot as a past closing.
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
            "transfers",
            lambda: movement_service.run_transfer_sync(
                start_date=month_start,
                end_date=month_end,
                branch_filter=None,
                triggered_by=triggered_by,
                apply_inventory=False,
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
        (
            "historical_closing",
            lambda: _capture_historical_closing(operational_date=month_end),
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


def _physical_count_gate(month_end: date) -> dict[str, object]:
    branches = list(canonical_point_network_branch_qs(month_end).only("id", "codigo", "nombre"))
    counts = list(
        ConteoSucursal.objects.filter(
            fecha=month_end,
            sucursal_id__in=[branch.id for branch in branches],
            lineas__producto_id__isnull=False,
        )
        .exclude(estado="CANCELADO")
        .select_related("sucursal")
        .prefetch_related("lineas")
        .distinct()
    )
    by_branch: dict[int, list[ConteoSucursal]] = {}
    for count in counts:
        by_branch.setdefault(count.sucursal_id, []).append(count)

    issues: list[str] = []
    branch_rows: list[dict[str, object]] = []
    for branch in branches:
        branch_counts = by_branch.get(branch.id, [])
        if not branch_counts:
            issues.append(f"{branch.codigo}: falta conteo físico de productos del {month_end:%d/%m/%Y}.")
            branch_rows.append({"code": branch.codigo, "ready": False, "count_ids": [], "product_lines": 0})
            continue

        product_lines = 0
        ready = True
        reasons: list[str] = []
        for count in branch_counts:
            line_ids = [line.id for line in count.lineas.all() if line.producto_id]
            product_lines += len(line_ids)
            lines = dict((count.conciliacion or {}).get("lineas") or {})
            if count.estado not in {"ENVIADO", "ACEPTADO"}:
                ready = False
                reasons.append(f"conteo {count.pk} está {count.estado}")
                continue
            for line_id in line_ids:
                result = dict(lines.get(str(line_id)) or {})
                if result.get("estado") != "CALCULADA" or result.get("diferencia") in (None, ""):
                    ready = False
                    reasons.append(f"conteo {count.pk} tiene líneas pendientes")
                    break
                try:
                    difference = Decimal(str(result["diferencia"]))
                except (ArithmeticError, TypeError, ValueError):
                    ready = False
                    reasons.append(f"conteo {count.pk} tiene una diferencia inválida")
                    break
                if difference != 0:
                    ready = False
                    reasons.append(f"conteo {count.pk} requiere reconteo")
                    break
        if product_lines <= 0:
            ready = False
            reasons.append("no contiene productos")
        if not ready:
            issues.append(f"{branch.codigo}: {'; '.join(dict.fromkeys(reasons))}.")
        branch_rows.append(
            {
                "code": branch.codigo,
                "ready": ready,
                "count_ids": [count.pk for count in branch_counts],
                "product_lines": product_lines,
            }
        )

    return {
        "ready": bool(branches) and not issues,
        "operational_date": month_end.isoformat(),
        "required_branch_count": len(branches),
        "ready_branch_count": sum(bool(row["ready"]) for row in branch_rows),
        "branches": branch_rows,
        "issues": issues,
    }


def _audit_materialization_gate(month_start: date) -> dict[str, object]:
    try:
        refresh = refresh_inventory_audit_month(month_start)
    except Exception as exc:  # the monthly run remains reviewable and retries on the next schedule
        return {"ready": False, "status": "error", "run_status": "", "error": str(exc)}
    run = ProductInventoryAuditRun.objects.filter(month=month_start).first()
    run_status = run.status if run is not None else ""
    ready = run_status == ProductInventoryAuditRun.Status.READY
    return {
        "ready": ready,
        "status": str(refresh.get("status") or ""),
        "run_status": run_status,
        "run_id": run.pk if run is not None else None,
        "error": "" if ready else "La auditoría visible todavía no está lista.",
    }


def _apply_monthly_automation_gates(
    closure: ProductoMonthClosure,
    *,
    month_end: date,
) -> tuple[ProductoMonthClosure, dict[str, object]]:
    physical = _physical_count_gate(month_end)
    audit = _audit_materialization_gate(closure.month_start)
    with transaction.atomic():
        closure = ProductoMonthClosure.objects.select_for_update().get(pk=closure.pk)
        metadata = dict(closure.metadata or {})
        validation = dict(metadata.get("validation") or {})
        blocking = list(validation.get("blocking_issues") or [])
        reviews = [item if isinstance(item, str) else str(item) for item in validation.get("automation_reviews") or []]
        if not physical["ready"]:
            blocking.extend(f"Conteos físicos: {issue}" for issue in physical["issues"])
            reviews.extend(physical["issues"])
        if not audit["ready"]:
            blocking.append("La auditoría materializada de Producido vs Vendido no está lista.")
            reviews.append(str(audit.get("error") or "Auditoría mensual pendiente."))
        validation.update(
            {
                "blocking_issues": list(dict.fromkeys(blocking)),
                "lock_ready": not blocking,
                "physical_counts": physical,
                "audit_materialization": audit,
                "automation_reviews": list(dict.fromkeys(reviews)),
            }
        )
        metadata["validation"] = validation
        metadata["reservation_policy"] = {
            "source": "crm.PickupReservation",
            "treatment": "availability_only_not_sale",
            "detail": "Las reservas reducen disponibilidad; sólo Point acredita la venta mensual.",
        }
        closure.metadata = metadata
        closure.save(update_fields=["metadata", "updated_at"])
    return closure, {"physical_counts": physical, "audit_refresh": audit}


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

    try:
        count_reconciliation = conciliar_conteos_nocturnos()
    except Exception as exc:
        count_reconciliation = {"procesados": 0, "calculados": 0, "pendientes": 0, "reintentar": True, "error": str(exc)}

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
        bool(step.get("retryable"))
        and step.get("status") in (PointSyncJob.STATUS_FAILED, PointSyncJob.STATUS_PARTIAL)
        for step in source_refresh
    )
    if retryable:
        return {
            "action": "source_refresh_failed",
            "action_label": "Fuentes Point con fallo transitorio; build diferido",
            "month": target_month.strftime("%Y-%m"),
            "closure_id": getattr(existing, "id", None),
            "closure_status": getattr(existing, "status", None),
            "closure_status_label": (
                existing.get_status_display() if existing is not None else "Sin construir"
            ),
            "is_locked": False,
            "lock_ready": False,
            "inventory_sync": None,
            "inventory_authority": {},
            "source_refresh": source_refresh,
            "failed_or_partial_sources": failed_or_partial_sources,
            "automation_status": "RETRY",
            "automation_reviews": [
                str(step.get("error") or f"{step['name']}: fallo transitorio")
                for step in source_refresh
                if step.get("retryable")
            ],
            "count_reconciliation": count_reconciliation,
            "retryable": True,
        }

    service = ProductMonthClosureService()
    closure = service.build(
        month=target_month,
        rebuild=bool(existing is not None or rebuild),
        lock_after_build=False,
        built_by=triggered_by,
        approval_reason="scheduled_monthly_automation",
        approval_channel="celery_monthly_product_closure",
    )
    closure, automation_evidence = _apply_monthly_automation_gates(closure, month_end=month_end)
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
        "count_reconciliation": count_reconciliation,
        **automation_evidence,
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
