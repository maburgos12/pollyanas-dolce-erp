from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.db.models import DecimalField, ExpressionWrapper, F, Sum
from django.utils import timezone

from inventario.models import ExistenciaInsumo
from recetas.models import PlanProduccionItem

from .models import MermaPOS, VentaPOS


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    end = next_month - timedelta(days=1)
    return start, end


def resolve_period_range(
    *,
    period_raw: str | None,
    date_from_raw: str | None = None,
    date_to_raw: str | None = None,
) -> tuple[date, date, str]:
    if date_from_raw:
        try:
            date_from = date.fromisoformat(str(date_from_raw))
        except ValueError:
            date_from = None
    else:
        date_from = None

    if date_to_raw:
        try:
            date_to = date.fromisoformat(str(date_to_raw))
        except ValueError:
            date_to = None
    else:
        date_to = None

    if date_from and date_to:
        if date_to < date_from:
            date_to = date_from
        return date_from, date_to, f"{date_from}..{date_to}"

    period = (period_raw or "").strip()
    if period:
        parts = period.split("-")
        if len(parts) == 2:
            try:
                year = int(parts[0])
                month = int(parts[1])
                if 2000 <= year <= 2200 and 1 <= month <= 12:
                    start, end = _month_bounds(year, month)
                    return start, end, f"{year:04d}-{month:02d}"
            except ValueError:
                pass

    today = timezone.localdate()
    start, end = _month_bounds(today.year, today.month)
    return start, end, f"{today.year:04d}-{today.month:02d}"


def _aggregate_plan_to_insumo(date_from: date, date_to: date) -> dict[int, Decimal]:
    amount_expr = ExpressionWrapper(
        F("cantidad") * F("receta__lineas__cantidad"),
        output_field=DecimalField(max_digits=24, decimal_places=6),
    )
    rows = (
        PlanProduccionItem.objects.filter(
            plan__fecha_produccion__gte=date_from,
            plan__fecha_produccion__lte=date_to,
            receta__lineas__insumo_id__isnull=False,
            receta__lineas__cantidad__isnull=False,
        )
        .values("receta__lineas__insumo_id")
        .annotate(total=Sum(amount_expr))
    )
    result: dict[int, Decimal] = {}
    for row in rows:
        insumo_id = int(row["receta__lineas__insumo_id"])
        result[insumo_id] = Decimal(str(row.get("total") or 0))
    return result


def _aggregate_ventas_to_insumo(date_from: date, date_to: date, sucursal_id: int | None) -> dict[int, Decimal]:
    amount_expr = ExpressionWrapper(
        F("cantidad") * F("receta__lineas__cantidad"),
        output_field=DecimalField(max_digits=24, decimal_places=6),
    )
    qs = VentaPOS.objects.filter(
        fecha__gte=date_from,
        fecha__lte=date_to,
        receta__isnull=False,
        receta__lineas__insumo_id__isnull=False,
        receta__lineas__cantidad__isnull=False,
    )
    if sucursal_id:
        qs = qs.filter(sucursal_id=sucursal_id)
    rows = qs.values("receta__lineas__insumo_id").annotate(total=Sum(amount_expr))

    result: dict[int, Decimal] = {}
    for row in rows:
        insumo_id = int(row["receta__lineas__insumo_id"])
        result[insumo_id] = Decimal(str(row.get("total") or 0))
    return result


def _aggregate_mermas_to_insumo(date_from: date, date_to: date, sucursal_id: int | None) -> dict[int, Decimal]:
    amount_expr = ExpressionWrapper(
        F("cantidad") * F("receta__lineas__cantidad"),
        output_field=DecimalField(max_digits=24, decimal_places=6),
    )
    qs = MermaPOS.objects.filter(
        fecha__gte=date_from,
        fecha__lte=date_to,
        receta__isnull=False,
        receta__lineas__insumo_id__isnull=False,
        receta__lineas__cantidad__isnull=False,
    )
    if sucursal_id:
        qs = qs.filter(sucursal_id=sucursal_id)
    rows = qs.values("receta__lineas__insumo_id").annotate(total=Sum(amount_expr))

    result: dict[int, Decimal] = {}
    for row in rows:
        insumo_id = int(row["receta__lineas__insumo_id"])
        result[insumo_id] = Decimal(str(row.get("total") or 0))
    return result


def build_discrepancias_report(
    *,
    date_from: date,
    date_to: date,
    sucursal_id: int | None = None,
    threshold_pct: Decimal | float = Decimal("10"),
    top: int = 300,
) -> dict[str, Any]:
    try:
        threshold = Decimal(str(threshold_pct or 0))
    except Exception:
        threshold = Decimal("10")
    if threshold < 0:
        threshold = Decimal("0")

    plan_map = _aggregate_plan_to_insumo(date_from, date_to)
    ventas_map = _aggregate_ventas_to_insumo(date_from, date_to, sucursal_id)
    mermas_map = _aggregate_mermas_to_insumo(date_from, date_to, sucursal_id)

    insumo_ids = set(plan_map.keys()) | set(ventas_map.keys()) | set(mermas_map.keys())
    existencia_qs = ExistenciaInsumo.objects.select_related("insumo")
    if insumo_ids:
        existencia_qs = existencia_qs.filter(insumo_id__in=insumo_ids)
    existencia_map = {e.insumo_id: e for e in existencia_qs}
    insumo_ids |= set(existencia_map.keys())

    rows = []
    total_alertas = 0
    total_observar = 0

    for insumo_id in sorted(insumo_ids):
        ex = existencia_map.get(insumo_id)
        nombre = ex.insumo.nombre if ex else f"Insumo #{insumo_id}"
        unidad = ex.insumo.unidad_base.codigo if ex and ex.insumo.unidad_base_id else ""
        produccion = plan_map.get(insumo_id, Decimal("0"))
        ventas = ventas_map.get(insumo_id, Decimal("0"))
        merma = mermas_map.get(insumo_id, Decimal("0"))
        teorico = produccion - ventas - merma
        real = Decimal(str(ex.stock_actual if ex else 0))
        discrepancia = real - teorico
        base = abs(teorico) if abs(teorico) > Decimal("1") else Decimal("1")
        variacion_pct = (abs(discrepancia) * Decimal("100")) / base

        if variacion_pct > threshold:
            status = "ALERTA"
            semaforo = "ROJO"
            total_alertas += 1
        elif abs(discrepancia) > 0:
            status = "OBSERVAR"
            semaforo = "AMARILLO"
            total_observar += 1
        else:
            status = "OK"
            semaforo = "VERDE"

        rows.append(
            {
                "insumo_id": insumo_id,
                "insumo": nombre,
                "unidad": unidad,
                "produccion": float(produccion),
                "ventas_pos": float(ventas),
                "mermas_pos": float(merma),
                "inventario_teorico": float(teorico),
                "inventario_real": float(real),
                "discrepancia": float(discrepancia),
                "variacion_pct": float(variacion_pct),
                "status": status,
                "semaforo": semaforo,
            }
        )

    rows.sort(key=lambda item: abs(item["discrepancia"]), reverse=True)
    rows = rows[: max(1, min(int(top or 300), 1000))]

    totals = {
        "insumos": len(insumo_ids),
        "alertas": total_alertas,
        "observar": total_observar,
        "ok": max(len(insumo_ids) - total_alertas - total_observar, 0),
        "produccion": float(sum(Decimal(str(r["produccion"])) for r in rows)),
        "ventas_pos": float(sum(Decimal(str(r["ventas_pos"])) for r in rows)),
        "mermas_pos": float(sum(Decimal(str(r["mermas_pos"])) for r in rows)),
    }

    if totals["alertas"] > 0:
        semaforo_global = "ROJO"
    elif totals["observar"] > 0:
        semaforo_global = "AMARILLO"
    else:
        semaforo_global = "VERDE"

    return {
        "range": {
            "from": str(date_from),
            "to": str(date_to),
        },
        "threshold_pct": float(threshold),
        "totals": totals,
        "semaforo_global": semaforo_global,
        "rows": rows,
    }
