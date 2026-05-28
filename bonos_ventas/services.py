from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from ventas.services.sales_read_service import get_point_sales_category_totals

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, VentaCategoriaSucursal


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
    totals = defaultdict(Decimal)
    for row in get_point_sales_category_totals(start_date=start, end_date=end, sucursal_id=sucursal_id):
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


def sync_dias_repartidor(periodo: ConfigBonoVentasPeriodo) -> dict[str, int]:
    """
    Para cada BonoVentasEmpleado del periodo donde el empleado es REPARTIDOR,
    cuenta los días con BitacoraRepartidor registrada ese mes y actualiza
    dias_con_bitacora + recalcula el bono.

    Requiere que el empleado tenga usuario_erp ligado al mismo User
    que el Repartidor en logística (via Repartidor.user.empleado_rrhh).
    """
    from logistica.models import BitacoraRepartidor

    bonos = (
        BonoVentasEmpleado.objects
        .filter(periodo=periodo)
        .select_related("empleado__usuario_erp")
    )

    actualizados = 0
    sin_repartidor = 0

    for bono in bonos:
        if (bono.empleado.puesto_operativo or "").strip().upper() != "REPARTIDOR":
            continue
        usuario_erp = bono.empleado.usuario_erp
        if not usuario_erp:
            sin_repartidor += 1
            continue
        try:
            repartidor = usuario_erp.repartidor_logistica
        except Exception:
            sin_repartidor += 1
            continue

        dias = BitacoraRepartidor.objects.filter(
            repartidor=repartidor,
            fecha__year=periodo.anio,
            fecha__month=periodo.mes,
        ).count()
        bono.dias_con_bitacora = dias
        bono.recalcular()
        bono.save(update_fields=["dias_con_bitacora", "pct_efectividad_entrega", "monto_bono_entregas", "bono_ventas", "pasa_bono_ventas", "total_a_pagar", "actualizado_en"])
        actualizados += 1

    return {"actualizados": actualizados, "sin_repartidor_vinculado": sin_repartidor}
