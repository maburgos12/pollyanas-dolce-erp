from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from django.db.models import Sum

from recetas.models import VentaHistorica
from ventas.models import EventoVenta, EventoVentaExecutionMetric, EventoVentaForecast, EventoVentaNotification
from ventas.services.notifications import create_unique_notification
from ventas.services.sales_read_service import get_daily_sales


def _as_decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def _avg_unit_price(product_id: int, branch_id: int | None, start_date, end_date) -> Decimal:
    qs = VentaHistorica.objects.filter(receta_id=product_id, fecha__range=(start_date, end_date))
    if branch_id:
        qs = qs.filter(sucursal_id=branch_id)
    aggregate = qs.aggregate(qty=Sum("cantidad"), sales=Sum("monto_total"))
    qty = _as_decimal(aggregate.get("qty"))
    sales = _as_decimal(aggregate.get("sales"))
    if qty > 0:
        return sales / qty
    return Decimal("0")


def build_postmortem(event: EventoVenta) -> dict:
    forecasts = list(
        EventoVentaForecast.objects.filter(sales_event=event)
        .select_related("branch", "product")
        .order_by("forecast_date", "branch__codigo", "product__nombre")
    )
    if not forecasts:
        return {"created": 0, "warnings": ["No hay forecast del evento."]}

    forecast_map: dict[tuple, dict] = {}
    for row in forecasts:
        key = (row.forecast_date, row.branch_id, row.product_id)
        forecast_map[key] = {
            "forecast_qty": _as_decimal(row.final_forecast),
            "forecast_sales": Decimal("0"),
            "branch_id": row.branch_id,
            "product_id": row.product_id,
            "metric_date": row.forecast_date,
        }

    historico = (
        VentaHistorica.objects.filter(
            fecha__range=(event.analysis_start_date, event.analysis_end_date),
            receta_id__in=[row.product_id for row in forecasts],
            sucursal_id__in=[row.branch_id for row in forecasts],
        )
        .values("fecha", "sucursal_id", "receta_id")
        .annotate(actual_qty=Sum("cantidad"), actual_sales=Sum("monto_total"))
    )
    actual_map: dict[tuple, dict] = defaultdict(lambda: {"actual_qty": Decimal("0"), "actual_sales": Decimal("0")})
    for row in historico:
        key = (row["fecha"], row["sucursal_id"], row["receta_id"])
        actual_map[key]["actual_qty"] += _as_decimal(row["actual_qty"])
        actual_map[key]["actual_sales"] += _as_decimal(row["actual_sales"])
    for key, forecast_row in forecast_map.items():
        if key in actual_map:
            continue
        canonical = get_daily_sales(
            forecast_row["branch_id"],
            forecast_row["metric_date"],
            forecast_row["product_id"],
        )
        if _as_decimal(canonical.get("cantidad")) <= 0 and _as_decimal(canonical.get("monto")) <= 0:
            continue
        actual_map[key]["actual_qty"] += _as_decimal(canonical.get("cantidad"))
        actual_map[key]["actual_sales"] += _as_decimal(canonical.get("monto"))

    EventoVentaExecutionMetric.objects.filter(sales_event=event).delete()

    created = 0
    total_forecast = Decimal("0")
    total_actual = Decimal("0")
    high_variance = 0
    for key, forecast_row in forecast_map.items():
        actual_row = actual_map.get(key, {})
        forecast_qty = forecast_row["forecast_qty"]
        actual_qty = _as_decimal(actual_row.get("actual_qty"))
        unit_price = _avg_unit_price(
            forecast_row["product_id"],
            forecast_row["branch_id"],
            event.analysis_start_date,
            event.analysis_end_date,
        )
        forecast_sales = forecast_qty * unit_price
        actual_sales = _as_decimal(actual_row.get("actual_sales"))
        variance_qty = actual_qty - forecast_qty
        variance_sales = actual_sales - forecast_sales

        EventoVentaExecutionMetric.objects.create(
            sales_event=event,
            metric_date=forecast_row["metric_date"],
            branch_id=forecast_row["branch_id"],
            product_id=forecast_row["product_id"],
            forecast_qty=forecast_qty,
            actual_qty=actual_qty,
            forecast_sales=forecast_sales,
            actual_sales=actual_sales,
            variance_qty=variance_qty,
            variance_sales=variance_sales,
        )
        created += 1
        total_forecast += forecast_qty
        total_actual += actual_qty
        if forecast_qty > 0:
            delta_pct = abs((actual_qty - forecast_qty) / forecast_qty)
            if delta_pct >= Decimal("0.25"):
                high_variance += 1

    if high_variance:
        create_unique_notification(
            event,
            f"Postmortem detecto {high_variance} combinaciones fecha-sucursal-producto con desviacion >= 25%.",
            EventoVentaNotification.SEVERITY_WARN,
        )
    else:
        create_unique_notification(event, "Postmortem actualizado sin desviaciones criticas.")

    event.status = EventoVenta.STATUS_EVALUADO if event.status == EventoVenta.STATUS_CERRADO else event.status
    event.save(update_fields=["status", "updated_at"])

    bias = total_actual - total_forecast
    return {
        "created": created,
        "summary": {
            "forecast_total_qty": total_forecast,
            "actual_total_qty": total_actual,
            "bias_qty": bias,
            "high_variance_rows": high_variance,
        },
    }
