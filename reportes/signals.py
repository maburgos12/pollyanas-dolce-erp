from __future__ import annotations

from datetime import date, datetime

from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.db.models import Min, Max
from django.dispatch import receiver
from django.utils import timezone

from inventario.models import AjusteInventario, ExistenciaInsumo, MovimientoInventario
from logistica.models import ParadaRuta, DiscrepanciaLogistica, RutaCargaChecklistLinea
from maestros.models import CostoInsumo, Insumo
from pos_bridge.historical_freeze import is_frozen
from pos_bridge.models import (
    PointDailyBranchIndicator,
    PointDailySale,
    PointInventorySnapshot,
    PointMonthlySalesOfficial,
    PointProductionLine,
    PointSalesDailyCategoryFact,
    PointSalesDailyProductFact,
    PointTransferLine,
    PointWasteLine,
    PointConversionLine,
    PointSyncJob,
    PointHistoricalInventoryClosing,
    PointProductHistoryImport,
)
from reportes.analytics_service import mark_analytics_dirty_for_range
from ventas.models import VentaAutoritativaPoint
from reportes.services_inventory_audit_refresh import enqueue_inventory_audit_months
from pos_bridge.services.product_month_source_mutex import months_in_range, snapshot_affected_months, next_month, month_start


def _local_day(value) -> date:
    if value is None:
        return timezone.localdate()
    if isinstance(value, datetime):
        if timezone.is_aware(value):
            return timezone.localtime(value).date()
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        # Django acepta strings ISO al asignar DateField; la instancia conserva
        # el string hasta refrescarse, así que el signal debe tolerarlo.
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return timezone.localdate()
    return value.date()


def _mark_after_commit(*, start_date: date, end_date: date, **flags) -> None:
    if flags.get("include_production"):
        enqueue_inventory_audit_months(months_in_range(start_date, end_date))
    transaction.on_commit(
        lambda: mark_analytics_dirty_for_range(
            start_date=start_date,
            end_date=end_date,
            **flags,
        )
    )


@receiver(post_save, sender=Insumo)
@receiver(post_delete, sender=Insumo)
@receiver(post_save, sender=CostoInsumo)
@receiver(post_delete, sender=CostoInsumo)
@receiver(post_save, sender=ExistenciaInsumo)
@receiver(post_delete, sender=ExistenciaInsumo)
@receiver(post_save, sender=AjusteInventario)
@receiver(post_delete, sender=AjusteInventario)
def _mark_inventory_master_refresh(instance, **_kwargs) -> None:
    day = timezone.localdate()
    _mark_after_commit(
        start_date=day,
        end_date=day,
        include_inventory=True,
        reason=f"{instance.__class__.__name__} changed",
    )


@receiver(post_save, sender=MovimientoInventario)
@receiver(post_delete, sender=MovimientoInventario)
def _mark_inventory_refresh(instance, **_kwargs) -> None:
    day = _local_day(getattr(instance, "fecha", None))
    _mark_after_commit(
        start_date=day,
        end_date=day,
        include_inventory=True,
        reason="MovimientoInventario changed",
    )


@receiver(post_save, sender=VentaAutoritativaPoint)
@receiver(post_delete, sender=VentaAutoritativaPoint)
@receiver(post_save, sender=PointDailySale)
@receiver(post_delete, sender=PointDailySale)
@receiver(post_save, sender=PointSalesDailyProductFact)
@receiver(post_delete, sender=PointSalesDailyProductFact)
@receiver(post_save, sender=PointSalesDailyCategoryFact)
@receiver(post_delete, sender=PointSalesDailyCategoryFact)
@receiver(post_save, sender=PointDailyBranchIndicator)
@receiver(post_delete, sender=PointDailyBranchIndicator)
def _mark_sales_refresh(instance, **_kwargs) -> None:
    day = _local_day(getattr(instance, "sale_date", None) or getattr(instance, "indicator_date", None))
    if isinstance(instance, PointDailySale) and is_frozen(day):
        return
    _mark_after_commit(
        start_date=day,
        end_date=day,
        include_sales=True,
        include_production=True,
        include_forecast=True,
        reason=f"{instance.__class__.__name__} changed",
    )


@receiver(post_save, sender=PointMonthlySalesOfficial)
@receiver(post_delete, sender=PointMonthlySalesOfficial)
def _mark_monthly_sales_refresh(instance, **_kwargs) -> None:
    start_date = getattr(instance, "month_start", None) or timezone.localdate()
    end_date = getattr(instance, "month_end", None) or start_date
    _mark_after_commit(
        start_date=start_date,
        end_date=end_date,
        include_sales=True,
        include_production=True,
        include_forecast=True,
        reason="PointMonthlySalesOfficial changed",
    )


@receiver(post_save, sender=PointProductionLine)
@receiver(post_delete, sender=PointProductionLine)
@receiver(post_save, sender=PointWasteLine)
@receiver(post_delete, sender=PointWasteLine)
@receiver(post_save, sender=PointConversionLine)
@receiver(post_delete, sender=PointConversionLine)
@receiver(post_save, sender=PointTransferLine)
@receiver(post_delete, sender=PointTransferLine)
@receiver(post_save, sender=PointInventorySnapshot)
@receiver(post_delete, sender=PointInventorySnapshot)
def _mark_flow_refresh(instance, **_kwargs) -> None:
    if isinstance(instance, PointInventorySnapshot):
        enqueue_inventory_audit_months(snapshot_affected_months(instance.captured_at))
    elif isinstance(instance, PointTransferLine):
        enqueue_inventory_audit_months(month_start(v) for v in (instance.registered_at, instance.sent_at, instance.received_at) if v)
    day = _local_day(
        getattr(instance, "production_date", None)
        or getattr(instance, "movement_at", None)
        or getattr(instance, "received_at", None)
        or getattr(instance, "registered_at", None)
        or getattr(instance, "captured_at", None)
    )
    _mark_after_commit(
        start_date=day,
        end_date=day,
        include_production=True,
        reason=f"{instance.__class__.__name__} changed",
    )


@receiver(post_save, sender=PointSyncJob)
def _audit_sync_finished(instance, update_fields=None, **kwargs):
    if instance.status not in {"SUCCESS", "PARTIAL", "FAILED"} or (update_fields and "status" not in update_fields):
        return
    params = instance.parameters or {}
    try:
        start = date.fromisoformat(str(params["start_date"])[:10])
        end = date.fromisoformat(str(params.get("end_date") or params["start_date"])[:10])
    except (KeyError, TypeError, ValueError):
        return  # Unknown scope is recovered by daily local review, not guessed.
    enqueue_inventory_audit_months(months_in_range(start, end))


@receiver(post_save, sender=PointHistoricalInventoryClosing)
@receiver(post_delete, sender=PointHistoricalInventoryClosing)
def _audit_historical_closing(instance, **kwargs):
    month = month_start(instance.operational_date)
    enqueue_inventory_audit_months([month, next_month(month)])


@receiver(post_save, sender=PointProductHistoryImport)
@receiver(post_delete, sender=PointProductHistoryImport)
def _audit_history_finished(instance, **kwargs):
    from reportes.models import ProductInventoryAuditRun
    span = instance.rows.aggregate(first=Min("movement_at"), last=Max("movement_at"))
    months = {month_start(instance.report_date)} if instance.report_date else set()
    if span["first"] and span["last"]:
        months.update(ProductInventoryAuditRun.objects.filter(
            month__gte=month_start(span["first"]), month__lte=month_start(span["last"])).values_list("month", flat=True))
    enqueue_inventory_audit_months(months)


@receiver(post_save, sender=ParadaRuta)
@receiver(post_delete, sender=ParadaRuta)
@receiver(post_save, sender=DiscrepanciaLogistica)
@receiver(post_delete, sender=DiscrepanciaLogistica)
@receiver(post_save, sender=RutaCargaChecklistLinea)
@receiver(post_delete, sender=RutaCargaChecklistLinea)
def _audit_logistics_changed(instance, **kwargs):
    # Checklist writers already carry this parent; do not discover its route per row.
    route = instance.checklist.ruta if isinstance(instance, RutaCargaChecklistLinea) else (getattr(instance, "ruta", None) or instance.parada.ruta)
    day = route._meta.get_field("fecha_ruta").to_python(route.fecha_ruta)
    enqueue_inventory_audit_months([month_start(day)])
