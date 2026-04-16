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
from reportes.analytics_service import get_sales_fact_range_summary
from reportes.models import FactInventarioDiario, FactProduccionDiaria, FactVentaDiaria
from rrhh.models import Empleado, NominaPeriodo
from ventas.services.sales_read_service import get_sales_range


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


def _sales_range_summary(start_date: date, end_date: date) -> dict:
    fact_summary = get_sales_fact_range_summary(start_date=start_date, end_date=end_date)
    if fact_summary is not None:
        return {
            "cantidad": fact_summary["cantidad"],
            "monto": fact_summary["monto"],
            "source": "analytic_fact",
            "source_detail": "reportes.fact_venta_diaria",
            "start_date": start_date,
            "end_date": end_date,
            "rows": fact_summary["rows"],
            "coverage_days": fact_summary["coverage_days"],
            "coverage_branches": fact_summary["coverage_branches"],
            "fallback_legacy_used": False,
            "coverage_accepted": True,
            "coverage_reason": "analytic_fact",
        }
    return get_sales_range(
        start_date=start_date,
        end_date=end_date,
        coverage_policy="prefer_complete",
    )


def _period_key(value: date | None) -> str | None:
    if value is None:
        return None
    return f"{value.year:04d}-{value.month:02d}"


def _monthly_sum_map(queryset, *, date_field: str, total_field: str) -> dict[str, Decimal]:
    rows = queryset.values(f"{date_field}__year", f"{date_field}__month").annotate(total=Sum(total_field))
    return {
        f"{int(row[f'{date_field}__year']):04d}-{int(row[f'{date_field}__month']):02d}": row.get("total") or Decimal("0")
        for row in rows
    }


def _monthly_count_map(queryset, *, date_field: str) -> dict[str, int]:
    rows = queryset.values(f"{date_field}__year", f"{date_field}__month").annotate(total=Count("id"))
    return {
        f"{int(row[f'{date_field}__year']):04d}-{int(row[f'{date_field}__month']):02d}": int(row.get("total") or 0)
        for row in rows
    }


def compute_bi_snapshot(period_days: int = 90, months_window: int = 6) -> dict:
    period_days = max(7, min(int(period_days or 90), 365))
    months_window = max(3, min(int(months_window or 6), 24))

    today = timezone.localdate()
    date_from = today - timedelta(days=period_days - 1)

    compras_qs = OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_BORRADOR).filter(
        fecha_emision__gte=date_from,
        fecha_emision__lte=today,
    )
    ventas_summary = _sales_range_summary(date_from, today)
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
    ventas_total = ventas_summary.get("monto") or Decimal("0")
    nomina_total = nomina_qs.aggregate(total=Sum("total_neto")).get("total") or Decimal("0")
    compras_ready = compras_qs.exists()
    nomina_ready = nomina_qs.exists()

    margen_bruto = ventas_total - compras_total
    margen_operativo = ventas_total - compras_total - nomina_total
    margin_ready = bool(ventas_total > 0 and compras_ready and nomina_ready)
    margen_operativo_pct = (margen_operativo * Decimal("100") / ventas_total) if (margin_ready and ventas_total > 0) else None

    monthly_pairs = _recent_month_pairs(months_window)
    monthly_keys = [f"{y:04d}-{m:02d}" for y, m in monthly_pairs]
    compras_month_map = _monthly_sum_map(compras_qs, date_field="fecha_emision", total_field="monto_estimado")
    nomina_month_map = _monthly_sum_map(nomina_qs, date_field="fecha_fin", total_field="total_neto")
    entregas_month_map = _monthly_count_map(
        entregas_qs.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA),
        date_field="ruta__fecha_ruta",
    )
    ventas_month_rows = (
        FactVentaDiaria.objects.filter(fecha__gte=min(_month_bounds(y, m)[0] for y, m in monthly_pairs), fecha__lte=today)
        .values("fecha__year", "fecha__month")
        .annotate(total=Sum("venta_total"))
    )
    ventas_month_map = {
        f"{int(row['fecha__year']):04d}-{int(row['fecha__month']):02d}": row.get("total") or Decimal("0")
        for row in ventas_month_rows
    }

    monthly_rows = []
    for (y, m), period_key in zip(monthly_pairs, monthly_keys, strict=False):
        compras_m = compras_month_map.get(period_key, Decimal("0"))
        ventas_m = ventas_month_map.get(period_key, Decimal("0"))
        nomina_m = nomina_month_map.get(period_key, Decimal("0"))
        entregas_m = entregas_month_map.get(period_key, 0)

        monthly_rows.append(
            {
                "periodo": period_key,
                "compras": compras_m,
                "ventas": ventas_m,
                "nomina": nomina_m,
                "margen": (ventas_m - compras_m - nomina_m) if margin_ready else None,
                "entregas": entregas_m,
            }
        )

    closed_months = [row for row in monthly_rows if row["periodo"] != f"{today.year:04d}-{today.month:02d}"]
    official_sales_series_ready = bool(closed_months) and all(_row.get("ventas", Decimal("0")) > 0 for _row in closed_months)

    top_proveedores = list(
        compras_qs.values("proveedor__nombre")
        .annotate(total=Sum("monto_estimado"), ordenes=Count("id"))
        .order_by("-total", "proveedor__nombre")[:10]
    )

    inventory_fact_top = list(
        FactInventarioDiario.objects.filter(fecha__gte=date_from, fecha__lte=today)
        .values("insumo__nombre")
        .annotate(total=Sum("salidas"), movimientos=Count("id"))
        .order_by("-total", "insumo__nombre")[:10]
    )
    top_insumos_consumo = (
        inventory_fact_top
        if inventory_fact_top
        else list(
            MovimientoInventario.objects.filter(
                fecha__date__gte=date_from,
                fecha__date__lte=today,
                tipo__in=[MovimientoInventario.TIPO_CONSUMO, MovimientoInventario.TIPO_SALIDA],
            )
            .values("insumo__nombre")
            .annotate(total=Sum("cantidad"), movimientos=Count("id"))
            .order_by("-total", "insumo__nombre")[:10]
        )
    )

    alertas_stock = ExistenciaInsumo.objects.filter(stock_actual__lt=F("punto_reorden")).count()
    criticos_stock = ExistenciaInsumo.objects.filter(stock_actual__lte=0).count()
    bajo_reorden_calc = ExistenciaInsumo.objects.filter(stock_actual__gt=0, stock_actual__lt=F("punto_reorden")).count()
    production_fact_total = (
        FactProduccionDiaria.objects.filter(fecha__gte=date_from, fecha__lte=today).aggregate(total=Sum("producido")).get("total")
        or Decimal("0")
    )
    fact_sales_total = (
        FactVentaDiaria.objects.filter(fecha__gte=date_from, fecha__lte=today).aggregate(total=Sum("cantidad")).get("total")
        or Decimal("0")
    )

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
            "margin_ready": margin_ready,
            "compras_ready": compras_ready,
            "nomina_ready": nomina_ready,
            "official_sales_series_ready": official_sales_series_ready,
            "ordenes_compra": compras_qs.count(),
            "pedidos_venta": ventas_summary.get("coverage_days", 0),
            "rutas": rutas_qs.count(),
            "entregas": entregas_qs.count(),
            "entregas_completadas": entregas_qs.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count(),
            "empleados_activos": Empleado.objects.filter(activo=True).count(),
            "alertas_stock": int(alertas_stock),
            "criticos_stock": criticos_stock,
            "bajo_reorden_stock": bajo_reorden_calc,
            "produccion_total_fact": production_fact_total,
            "ventas_total_fact": fact_sales_total,
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
