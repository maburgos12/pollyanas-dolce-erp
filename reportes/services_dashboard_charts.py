from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from django.db.models import Sum

from control.models import MermaMensualSucursal
from reportes.models import EmpresaResultadoMensual, PresupuestoResumenMensual
from reportes.services_budget_vs_actual import BUDGET_VS_ACTUAL_SOURCE

ZERO = Decimal("0")
SHORT_MONTH_LABELS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


def _decimal(value) -> Decimal:
    if value in (None, ""):
        return ZERO
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return ZERO


def _money(value) -> str:
    return str(_decimal(value).quantize(Decimal("0.01")))


def _pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return ZERO
    return (numerator / denominator * Decimal("100")).quantize(Decimal("0.01"))


def _snapshot_rows(snapshot: PresupuestoResumenMensual | None) -> dict[str, dict[str, Decimal]]:
    rows: dict[str, dict[str, Decimal]] = {}
    if snapshot is None:
        return rows
    for row in (snapshot.metadata or {}).get("rows") or []:
        concept = str(row.get("concept") or "").strip()
        if not concept:
            continue
        rows[concept] = {
            "budget": _decimal(row.get("budget")),
            "actual": _decimal(row.get("actual")),
            "variance": _decimal(row.get("variance")),
            "variance_pct": _decimal(row.get("variance_pct")),
        }
    return rows


def build_dashboard_charts_payload(*, year: int) -> dict[str, object]:
    finance_by_month = {
        row.periodo.month: row
        for row in EmpresaResultadoMensual.objects.filter(periodo__year=year).order_by("periodo")
    }
    snapshots_by_month = {
        row.period.month: row
        for row in PresupuestoResumenMensual.objects.filter(
            period__year=year,
            tipo=PresupuestoResumenMensual.TIPO_FUENTE,
            fuente_nombre=BUDGET_VS_ACTUAL_SOURCE,
        ).order_by("period")
    }
    snapshot_rows_by_month = {month: _snapshot_rows(snapshot) for month, snapshot in snapshots_by_month.items()}
    months = sorted(set(finance_by_month) | set(snapshots_by_month))
    if not months:
        months = list(range(1, 13))

    labels = [SHORT_MONTH_LABELS[month - 1] for month in months]
    periods = [date(year, month, 1).isoformat() for month in months]

    ventas_real: list[str] = []
    ventas_budget: list[str] = []
    ventas_attainment: list[str] = []
    utilidad_real: list[str] = []
    utilidad_budget: list[str] = []
    costs = {"mp": [], "reventa": [], "nomina": [], "gasto_fijo": [], "logistica": []}
    ventas_context: list[str] = []

    for month in months:
        finance = finance_by_month.get(month)
        rows = snapshot_rows_by_month.get(month, {})
        ventas = _decimal(getattr(finance, "venta_total", ZERO))
        ventas_presupuesto = rows.get("ventas", {}).get("budget", ZERO)
        utilidad = _decimal(getattr(finance, "utilidad_operativa_total", ZERO))
        utilidad_presupuesto = rows.get("utilidad_operativa", {}).get("budget", ZERO)
        costo_mp = _decimal(getattr(finance, "costo_materia_prima_total", ZERO))
        costo_reventa = _decimal(getattr(finance, "costo_reventa_total", ZERO))
        nomina = _decimal(getattr(finance, "mano_obra_prod_total", ZERO))
        gasto_fijo = _decimal(getattr(finance, "gasto_comercial_total", ZERO)) + _decimal(
            getattr(finance, "gasto_corporativo_total", ZERO)
        )
        logistica = rows.get("logistica", {}).get("actual", ZERO)

        ventas_real.append(_money(ventas))
        ventas_budget.append(_money(ventas_presupuesto))
        ventas_attainment.append(_money(_pct(ventas, ventas_presupuesto)))
        utilidad_real.append(_money(utilidad))
        utilidad_budget.append(_money(utilidad_presupuesto))
        costs["mp"].append(_money(costo_mp))
        costs["reventa"].append(_money(costo_reventa))
        costs["nomina"].append(_money(nomina))
        costs["gasto_fijo"].append(_money(gasto_fijo))
        costs["logistica"].append(_money(logistica))
        ventas_context.append(_money(ventas))

    waste_rows = (
        MermaMensualSucursal.objects.filter(periodo__year=year)
        .values("periodo")
        .annotate(costo=Sum("costo_merma"))
        .order_by("periodo")
    )
    waste_labels: list[str] = []
    waste_periods: list[str] = []
    waste_cost: list[str] = []
    waste_pct: list[str] = []
    for row in waste_rows:
        period = row["periodo"]
        month = period.month
        costo = _decimal(row.get("costo"))
        sales = _decimal(getattr(finance_by_month.get(month), "venta_total", ZERO))
        waste_labels.append(SHORT_MONTH_LABELS[month - 1])
        waste_periods.append(period.isoformat())
        waste_cost.append(_money(costo))
        waste_pct.append(_money(_pct(costo, sales)))

    return {
        "year": year,
        "periods": periods,
        "labels": labels,
        "sources": {
            "actual": "reportes.EmpresaResultadoMensual",
            "budget": "reportes.PresupuestoResumenMensual",
            "waste": "control.MermaMensualSucursal",
        },
        "charts": {
            "ventas_vs_presupuesto": {
                "labels": labels,
                "real": ventas_real,
                "presupuesto": ventas_budget,
                "cumplimiento_pct": ventas_attainment,
            },
            "utilidad_operativa": {
                "labels": labels,
                "real": utilidad_real,
                "presupuesto": utilidad_budget,
            },
            "desglose_costos": {
                "labels": labels,
                "mp": costs["mp"],
                "reventa": costs["reventa"],
                "nomina": costs["nomina"],
                "gasto_fijo": costs["gasto_fijo"],
                "logistica": costs["logistica"],
                "ventas": ventas_context,
            },
            "merma_mensual": {
                "labels": waste_labels,
                "periods": waste_periods,
                "costo_merma": waste_cost,
                "pct_sobre_ventas": waste_pct,
            },
        },
    }
