"""Refresh persisted audits from local evidence only, never import Point."""
import hashlib
import json
import logging
from datetime import date, datetime, time, timedelta
from uuid import uuid4

from django.apps import apps
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import Count, Max, Sum, Q
from django.utils import timezone

from pos_bridge.services.product_month_source_mutex import lock_product_month_sources, next_month, month_start, POINT_BUSINESS_TIMEZONE
from recetas.models import ProductoMonthClosure
from reportes.models import ProductInventoryAuditRun
from reportes.services_inventory_traceability import InventoryAuditMaterializer

logger = logging.getLogger(__name__)


def _month(value):
    return month_start(date.fromisoformat(value + "-01") if isinstance(value, str) else value)


def inventory_audit_review_months():
    current = timezone.localdate().replace(day=1)
    months = {current, (current - timedelta(days=1)).replace(day=1)}
    months.update(ProductInventoryAuditRun.objects.values_list("month", flat=True))
    locked = set(ProductoMonthClosure.objects.filter(is_locked=True).values_list("month_start", flat=True))
    return sorted(months - locked)


def _source_signature(month):
    end = next_month(month)
    try:
        revision = cache.get(f"inventory-audit:dirty:{month}")
    except Exception:
        revision = None  # Cache is optional; a missing revision never proves freshness.
    evidence = [revision]
    # Aggregate bounded source records, including deletes and quantity changes.
    for app, name, field, quantity in (
        ("pos_bridge", "PointProductionLine", "production_date", "produced_quantity"),
        ("pos_bridge", "PointWasteLine", "movement_at", "quantity"),
        ("pos_bridge", "PointConversionLine", "movement_at", "quantity"),
        ("pos_bridge", "PointSalesDailyProductFact", "sale_date", "total_cantidad"),
        ("pos_bridge", "PointDailySale", "sale_date", "quantity"),
        ("ventas", "VentaAutoritativaPoint", "sale_date", "quantity"),
        ("pos_bridge", "PointProductHistoryRow", "movement_at", "quantity"),
    ):
        model = apps.get_model(app, name)
        fields = {f.name for f in model._meta.fields}
        stamp = next((f for f in ("updated_at", "imported_at", "created_at") if f in fields), "id")
        is_datetime = model._meta.get_field(field).get_internal_type() == "DateTimeField"
        lower, upper = (datetime.combine(value, time.min, POINT_BUSINESS_TIMEZONE) for value in (month, end)) if is_datetime else (month, end)
        evidence.append(model.objects.filter(**{field + "__gte": lower, field + "__lt": upper}).aggregate(
            count=Count("id"), latest=Max(stamp), last_id=Max("id"), quantity=Sum(quantity)))
    # These catalogs/manifests also change authority, aliases and historical returns.
    # ponytail: small catalog/global transfer aggregates; partition if measured latency grows.
    for app, name in (
        ("pos_bridge", "PointTransferLine"),
        ("pos_bridge", "PointSyncJob"), ("pos_bridge", "PointHistoricalInventoryClosing"),
        ("pos_bridge", "PointProductHistoryImport"), ("pos_bridge", "PointBranch"),
        ("recetas", "RecetaCodigoPointAlias"), ("logistica", "DiscrepanciaLogistica"),
        ("logistica", "ParadaRuta"),
        ("logistica", "RutaCargaChecklistLinea"),
    ):
        model = apps.get_model(app, name)
        fields = {f.name for f in model._meta.fields}
        stamp = next((f for f in ("updated_at", "actualizado_en", "revisado_en", "captured_at", "id") if f in fields))
        sums = {f: Sum(f) for f in ("stock", "sent_quantity", "received_quantity") if f in fields}
        evidence.append(model.objects.aggregate(count=Count("id"), latest=Max(stamp), last_id=Max("id"), **sums))
    for app, name, fields in (
        ("pos_bridge", "PointBranch", ("id", "erp_branch_id", "external_id")),
        ("recetas", "RecetaCodigoPointAlias", ("id", "receta_id", "codigo_point", "activo")),
    ):
        evidence.append(list(apps.get_model(app, name).objects.order_by("id").values_list(*fields)))
    snapshot_scope = Q()
    window = max(0, int(getattr(settings, "PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS", 3))) + 1
    for boundary in (month, end):
        lower = datetime.combine(boundary - timedelta(days=window), time.min, POINT_BUSINESS_TIMEZONE)
        upper = datetime.combine(boundary + timedelta(days=window), time.min, POINT_BUSINESS_TIMEZONE)
        snapshot_scope |= Q(captured_at__gte=lower, captured_at__lt=upper)
    snapshots = apps.get_model("pos_bridge", "PointInventorySnapshot")
    evidence.append(list(snapshots.objects.filter(snapshot_scope).order_by("id").values_list("id", "branch_id", "product_id", "stock", "sync_job_id")))
    closings = apps.get_model("pos_bridge", "PointHistoricalInventoryClosingLine")
    evidence.append(list(closings.objects.filter(closing__operational_date__in=[month - timedelta(days=1), end - timedelta(days=1)]).order_by("id").values_list("id", "branch_id", "product_id", "stock")))
    return hashlib.sha256(json.dumps(evidence, default=str, sort_keys=True).encode()).hexdigest()


def refresh_inventory_audit_month(month):
    from pos_bridge.tasks.retry_failed_jobs import MAX_RUNNING_HOURS
    month = _month(month)
    key = f"inventory-audit:signature:{month}"
    with transaction.atomic():
        lock_product_month_sources([(month - timedelta(days=1)).replace(day=1), month])
        if ProductoMonthClosure.objects.filter(month_start=month, is_locked=True).exists():
            return {"month": str(month), "status": "locked"}
        jobs = apps.get_model("pos_bridge", "PointSyncJob").objects.filter(
            Q(status="PENDING") | Q(status="RUNNING", updated_at__gte=timezone.now() - timedelta(hours=MAX_RUNNING_HOURS)),
            job_type__in=["inventory", "sales", "production", "waste", "transfers", "conversions"])
        for params in jobs.values_list("parameters", flat=True):
            if not isinstance(params, dict):
                continue
            try:
                first = date.fromisoformat(str(params["start_date"])[:10])
                last = date.fromisoformat(str(params.get("end_date") or params["start_date"])[:10])
            except (KeyError, TypeError, ValueError):
                continue
            if first < next_month(month) and last >= month:
                return {"month": str(month), "status": "deferred"}
        signature = _source_signature(month)
        try:
            previous = cache.get(key)
        except Exception:
            previous = None
        if previous == signature:
            return {"month": str(month), "status": "unchanged"}
        result = InventoryAuditMaterializer().rebuild(month=month)
        if result.required_sources_available:
            def save_revision():
                try:
                    cache.set(key, signature, timeout=86400)
                except Exception:
                    logger.exception("No se pudo guardar vigencia de auditoría %s", month)
            transaction.on_commit(save_revision)
        return {"month": str(month), "status": "updated" if result.required_sources_available else "incomplete", "counts": dict(result)}


def enqueue_inventory_audit_months(months):
    months = sorted({_month(m) for m in months})

    def publish():
        from reportes.tasks import refresh_inventory_audit_month_task
        for month in months:
            key = f"inventory-audit:queued:{month}"
            token = uuid4().hex
            try:
                cache.set(f"inventory-audit:dirty:{month}", token, timeout=86400)
                if cache.add(key, token, timeout=300):
                    # Kombu also applies this policy to the initial connection;
                    # retry=False alone still waits for broker connection retries.
                    refresh_inventory_audit_month_task.apply_async(kwargs={"month": month.strftime("%Y-%m"), "token": token}, countdown=15, retry=True, retry_policy={"max_retries": 0})
            except Exception:
                try:
                    if cache.get(key) == token:
                        cache.delete(key)
                except Exception:
                    pass
                logger.exception("No se pudo programar auditoría %s; el respaldo diario la recuperará", month)
    transaction.on_commit(publish)
