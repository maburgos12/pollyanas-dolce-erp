from __future__ import annotations

from datetime import date

from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver
from django.utils import timezone

from inventario.models import AjusteInventario, ExistenciaInsumo, MovimientoInventario
from maestros.models import CostoInsumo, Insumo
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
)
from reportes.analytics_service import mark_analytics_dirty_for_range
from ventas.models import VentaAutoritativaPoint


def _local_day(value) -> date:
    if value is None:
        return timezone.localdate()
    if isinstance(value, date):
        return value
    if timezone.is_aware(value):
        return timezone.localtime(value).date()
    return value.date()


def _mark_after_commit(*, start_date: date, end_date: date, **flags) -> None:
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
@receiver(post_save, sender=PointTransferLine)
@receiver(post_delete, sender=PointTransferLine)
@receiver(post_save, sender=PointInventorySnapshot)
@receiver(post_delete, sender=PointInventorySnapshot)
def _mark_flow_refresh(instance, **_kwargs) -> None:
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
