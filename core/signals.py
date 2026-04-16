from __future__ import annotations

from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from compras.models import OrdenCompra, PresupuestoCompraPeriodo, RecepcionCompra, SolicitudCompra
from control.models import MermaPOS
from inventario.models import AjusteInventario, AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
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
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, RecetaPresentacionDerivada, SolicitudVenta, VentaHistorica

from core.cache_versions import bump_cache_scopes


def _bump_on_commit(*scopes: str) -> None:
    transaction.on_commit(lambda: bump_cache_scopes(*scopes))


@receiver(post_save, sender=Insumo)
@receiver(post_delete, sender=Insumo)
@receiver(post_save, sender=CostoInsumo)
@receiver(post_delete, sender=CostoInsumo)
def _invalidate_insumos_scope(**_kwargs) -> None:
    _bump_on_commit("insumos", "inventario", "dashboard")


@receiver(post_save, sender=MovimientoInventario)
@receiver(post_delete, sender=MovimientoInventario)
@receiver(post_save, sender=ExistenciaInsumo)
@receiver(post_delete, sender=ExistenciaInsumo)
@receiver(post_save, sender=AjusteInventario)
@receiver(post_delete, sender=AjusteInventario)
@receiver(post_save, sender=AlmacenSyncRun)
@receiver(post_delete, sender=AlmacenSyncRun)
def _invalidate_inventory_scope(**_kwargs) -> None:
    _bump_on_commit("inventario", "dashboard")


@receiver(post_save, sender=SolicitudCompra)
@receiver(post_delete, sender=SolicitudCompra)
@receiver(post_save, sender=OrdenCompra)
@receiver(post_delete, sender=OrdenCompra)
@receiver(post_save, sender=RecepcionCompra)
@receiver(post_delete, sender=RecepcionCompra)
@receiver(post_save, sender=PresupuestoCompraPeriodo)
@receiver(post_delete, sender=PresupuestoCompraPeriodo)
def _invalidate_purchase_scope(**_kwargs) -> None:
    _bump_on_commit("dashboard")


@receiver(post_save, sender=VentaHistorica)
@receiver(post_delete, sender=VentaHistorica)
@receiver(post_save, sender=SolicitudVenta)
@receiver(post_delete, sender=SolicitudVenta)
@receiver(post_save, sender=MermaPOS)
@receiver(post_delete, sender=MermaPOS)
@receiver(post_save, sender=PointDailySale)
@receiver(post_delete, sender=PointDailySale)
@receiver(post_save, sender=PointMonthlySalesOfficial)
@receiver(post_delete, sender=PointMonthlySalesOfficial)
@receiver(post_save, sender=PointDailyBranchIndicator)
@receiver(post_delete, sender=PointDailyBranchIndicator)
@receiver(post_save, sender=PointSalesDailyCategoryFact)
@receiver(post_delete, sender=PointSalesDailyCategoryFact)
@receiver(post_save, sender=PointSalesDailyProductFact)
@receiver(post_delete, sender=PointSalesDailyProductFact)
@receiver(post_save, sender=PointInventorySnapshot)
@receiver(post_delete, sender=PointInventorySnapshot)
@receiver(post_save, sender=PointProductionLine)
@receiver(post_delete, sender=PointProductionLine)
@receiver(post_save, sender=PointTransferLine)
@receiver(post_delete, sender=PointTransferLine)
@receiver(post_save, sender=PointWasteLine)
@receiver(post_delete, sender=PointWasteLine)
def _invalidate_sales_scope(**_kwargs) -> None:
    _bump_on_commit("ventas", "dashboard")


@receiver(post_save, sender=PlanProduccion)
@receiver(post_delete, sender=PlanProduccion)
@receiver(post_save, sender=PlanProduccionItem)
@receiver(post_delete, sender=PlanProduccionItem)
@receiver(post_save, sender=LineaReceta)
@receiver(post_delete, sender=LineaReceta)
@receiver(post_save, sender=RecetaPresentacionDerivada)
@receiver(post_delete, sender=RecetaPresentacionDerivada)
def _invalidate_recipe_operational_scope(**_kwargs) -> None:
    _bump_on_commit("dashboard")
