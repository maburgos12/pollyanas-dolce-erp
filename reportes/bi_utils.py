from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Count, F, Sum
from django.utils import timezone

from compras.models import OrdenCompra
from crm.models import PedidoCliente
from inventario.models import ExistenciaInsumo, MovimientoInventario
from logistica.models import EntregaRuta, RutaEntrega
from rrhh.models import Empleado, NominaPeriodo


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return start, end


def _recent_month_pairs(window: int) -> list[tuple[int, int]]:
    today = timezone.localdate()
    cursor_y, cursor_m = today.year, today.month
    pairs: list[tuple[int, int]] = []
    for _ in range(max(window, 1)):
        pairs.append((cursor_y, cursor_m))
        cursor_m -= 1
        if cursor_m == 0:
            cursor_m = 12
            cursor_y -= 1
    pairs.reverse()
    return pairs


def compute_bi_snapshot(period_days: int = 90, months_window: int = 6) -> dict:
    period_days = max(7, min(int(period_days or 90), 365))
    months_window = max(3, min(int(months_window or 6), 24))

    today = timezone.localdate()
    date_from = today - timedelta(days=period_days - 1)

    compras_qs = OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_BORRADOR).filter(
        fecha_emision__gte=date_from,
        fecha_emision__lte=today,
    )
    ventas_qs = PedidoCliente.objects.exclude(estatus=PedidoCliente.ESTATUS_CANCELADO).filter(
        created_at__date__gte=date_from,
        created_at__date__lte=today,
    )
    nomina_qs = NominaPeriodo.objects.filter(
        fecha_fin__gte=date_from,
        fecha_fin__lte=today,
    )
    rutas_qs = RutaEntrega.objects.exclude(estatus=RutaEntrega.ESTATUS_CANCELADA).filter(
        fecha_ruta__gte=date_from,
        fecha_ruta__lte=today,
    )
    entregas_qs = EntregaRuta.objects.exclude(estatus=EntregaRuta.ESTATUS_CANCELADA).filter(
        ruta__fecha_ruta__gte=date_from,
        ruta__fecha_ruta__lte=today,
    )

    compras_total = compras_qs.aggregate(total=Sum("monto_estimado")).get("total") or Decimal("0")
    ventas_total = ventas_qs.aggregate(total=Sum("monto_estimado")).get("total") or Decimal("0")
    nomina_total = nomina_qs.aggregate(total=Sum("total_neto")).get("total") or Decimal("0")

    margen_bruto = ventas_total - compras_total
    margen_operativo = ventas_total - compras_total - nomina_total
    margen_operativo_pct = (margen_operativo * Decimal("100") / ventas_total) if ventas_total > 0 else None

    monthly_rows = []
    for y, m in _recent_month_pairs(months_window):
        m_from, m_to = _month_bounds(y, m)
        compras_m = (
            OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_BORRADOR)
            .filter(fecha_emision__gte=m_from, fecha_emision__lte=m_to)
            .aggregate(total=Sum("monto_estimado"))
            .get("total")
            or Decimal("0")
        )
        ventas_m = (
            PedidoCliente.objects.exclude(estatus=PedidoCliente.ESTATUS_CANCELADO)
            .filter(created_at__date__gte=m_from, created_at__date__lte=m_to)
            .aggregate(total=Sum("monto_estimado"))
            .get("total")
            or Decimal("0")
        )
        nomina_m = (
            NominaPeriodo.objects.filter(fecha_fin__gte=m_from, fecha_fin__lte=m_to)
            .aggregate(total=Sum("total_neto"))
            .get("total")
            or Decimal("0")
        )
        entregas_m = EntregaRuta.objects.filter(
            ruta__fecha_ruta__gte=m_from,
            ruta__fecha_ruta__lte=m_to,
            estatus=EntregaRuta.ESTATUS_ENTREGADA,
        ).count()

        monthly_rows.append(
            {
                "periodo": f"{y:04d}-{m:02d}",
                "compras": compras_m,
                "ventas": ventas_m,
                "nomina": nomina_m,
                "margen": ventas_m - compras_m - nomina_m,
                "entregas": entregas_m,
            }
        )

    top_proveedores = list(
        compras_qs.values("proveedor__nombre")
        .annotate(total=Sum("monto_estimado"), ordenes=Count("id"))
        .order_by("-total", "proveedor__nombre")[:10]
    )

    top_insumos_consumo = list(
        MovimientoInventario.objects.filter(
            fecha__date__gte=date_from,
            fecha__date__lte=today,
            tipo__in=[MovimientoInventario.TIPO_CONSUMO, MovimientoInventario.TIPO_SALIDA],
        )
        .values("insumo__nombre")
        .annotate(total=Sum("cantidad"), movimientos=Count("id"))
        .order_by("-total", "insumo__nombre")[:10]
    )

    alertas_stock = ExistenciaInsumo.objects.filter(stock_actual__lt=F("punto_reorden")).count()
    criticos_stock = ExistenciaInsumo.objects.filter(stock_actual__lte=0).count()
    bajo_reorden_calc = ExistenciaInsumo.objects.filter(stock_actual__gt=0, stock_actual__lt=F("punto_reorden")).count()

    return {
        "range": {
            "from": date_from,
            "to": today,
            "days": period_days,
            "months_window": months_window,
        },
        "kpis": {
            "compras_total": compras_total,
            "ventas_total": ventas_total,
            "nomina_total": nomina_total,
            "margen_bruto": margen_bruto,
            "margen_operativo": margen_operativo,
            "margen_operativo_pct": margen_operativo_pct,
            "ordenes_compra": compras_qs.count(),
            "pedidos_venta": ventas_qs.count(),
            "rutas": rutas_qs.count(),
            "entregas": entregas_qs.count(),
            "entregas_completadas": entregas_qs.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count(),
            "empleados_activos": Empleado.objects.filter(activo=True).count(),
            "alertas_stock": int(alertas_stock),
            "criticos_stock": criticos_stock,
            "bajo_reorden_stock": bajo_reorden_calc,
        },
        "series_mensual": monthly_rows,
        "top_proveedores": top_proveedores,
        "top_insumos_consumo": top_insumos_consumo,
    }


def serialize_bi_for_api(data):
    from decimal import Decimal
    from datetime import date, datetime

    if isinstance(data, dict):
        return {k: serialize_bi_for_api(v) for k, v in data.items()}
    if isinstance(data, list):
        return [serialize_bi_for_api(v) for v in data]
    if isinstance(data, Decimal):
        return str(data)
    if isinstance(data, (date, datetime)):
        return data.isoformat()
    return data
