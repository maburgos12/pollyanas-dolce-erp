from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from django.db.models import Sum

from pos_bridge.models import PointDailySale

from .models import ConfigBonoVentasPeriodo, VentaCategoriaSucursal


MAPEO_CATEGORIAS = {
    "Grande": "GRANDE",
    "Mediano": "MEDIANO",
    "Chico": "CHICO",
    "Mini": "MINI",
    "Velas Sparklers": "VELAS_ACCESORIOS",
    "Viva Party": "VELAS_ACCESORIOS",
    "Xtudio": "VELAS_ACCESORIOS",
    "Accesorios de Repostería": "VELAS_ACCESORIOS",
    "Vaso Preparado Mini": "VASOS",
    "Vasos Preparados Grande": "VASOS",
}


def _month_range(anio: int, mes: int) -> tuple[date, date]:
    start = date(anio, mes, 1)
    end = date(anio + (mes // 12), (mes % 12) + 1, 1)
    return start, end


def _ventas_por_sucursal_categoria(start: date, end: date, sucursal_id: int | None = None) -> dict[tuple[int, str], Decimal]:
    qs = PointDailySale.objects.filter(
        sale_date__gte=start,
        sale_date__lt=end,
        branch__erp_branch__isnull=False,
    )
    if sucursal_id:
        qs = qs.filter(branch__erp_branch_id=sucursal_id)
    qs = qs.values("branch__erp_branch_id", "product__category").annotate(total=Sum("quantity"))

    totals = defaultdict(Decimal)
    for row in qs:
        categoria = MAPEO_CATEGORIAS.get(row["product__category"])
        if not categoria:
            continue
        totals[(row["branch__erp_branch_id"], categoria)] += row["total"] or Decimal("0")
    return totals


def sync_ventas_categorias(periodo: ConfigBonoVentasPeriodo, sucursal_id: int | None = None) -> int:
    start, end = _month_range(periodo.anio, periodo.mes)
    prev_start, prev_end = _month_range(periodo.anio - 1, periodo.mes)
    actuales = _ventas_por_sucursal_categoria(start, end, sucursal_id=sucursal_id)
    anteriores = _ventas_por_sucursal_categoria(prev_start, prev_end, sucursal_id=sucursal_id)
    keys = sorted(set(actuales) | set(anteriores))

    updated = 0
    for sucursal_pk, categoria in keys:
        VentaCategoriaSucursal.objects.update_or_create(
            periodo=periodo,
            sucursal_id=sucursal_pk,
            categoria=categoria,
            defaults={
                "cantidad_actual": actuales.get((sucursal_pk, categoria), Decimal("0.000")),
                "cantidad_anterior": anteriores.get((sucursal_pk, categoria), Decimal("0.000")),
                "fuente": VentaCategoriaSucursal.FUENTE_POS_BRIDGE,
            },
        )
        updated += 1
    return updated
