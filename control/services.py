from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.db.models import DecimalField, ExpressionWrapper, F, Sum
from django.utils import timezone

from inventario.models import ExistenciaInsumo
from inventario.services_documentary_audit import convert
from maestros.models import Insumo
from maestros.utils.canonical_catalog import canonicalized_active_insumos
from recetas.models import PlanProduccionItem

from .models import MermaPOS, VentaPOS


def _canonical_member_maps(limit: int = 2000) -> tuple[dict[int, int], dict[int, Any]]:
    member_to_canonical: dict[int, int] = {}
    canonical_rows: dict[int, Any] = {}
    for row in canonicalized_active_insumos(limit=limit):
        canonical = row["canonical"]
        canonical_rows[canonical.id] = row
        for member_id in row["member_ids"]:
            member_to_canonical[member_id] = canonical.id
    return member_to_canonical, canonical_rows


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


def _ingredient_totals(qs, member_to_canonical, canonical_rows, missing_units):
    line_prefix = "receta__lineas__"
    insumo_field = line_prefix + "insumo_id"
    base_unit_field = line_prefix + "insumo__unidad_base__codigo"
    unit_field = line_prefix + "unidad__codigo"
    unit_text_field = line_prefix + "unidad_texto"
    amount = ExpressionWrapper(
        F("cantidad") * F(line_prefix + "cantidad"),
        output_field=DecimalField(max_digits=24, decimal_places=6),
    )
    grouped = (
        qs.filter(**{insumo_field + "__isnull": False, line_prefix + "cantidad__isnull": False})
        .values(insumo_field, base_unit_field, unit_field, unit_text_field)
        .annotate(total=Sum(amount))
    )
    totals: dict[int, Decimal] = {}
    for row in grouped:
        insumo_id = row[insumo_field]
        canonical_id = member_to_canonical.get(insumo_id, insumo_id)
        canonical = canonical_rows.get(canonical_id, {}).get("canonical")
        if canonical:
            target = canonical.unidad_base.codigo if canonical.unidad_base_id else None
        else:
            target = row[base_unit_field]
        converted = convert(row["total"], row[unit_field] or row[unit_text_field], target)
        if converted is None:
            missing_units.add(canonical_id)
            continue
        totals[canonical_id] = totals.get(canonical_id, Decimal("0")) + converted
    return totals


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

    member_to_canonical, canonical_rows = _canonical_member_maps()
    missing_units: set[int] = set()
    plan_qs = PlanProduccionItem.objects.filter(plan__fecha_produccion__range=(date_from, date_to))
    sales_qs = VentaPOS.objects.filter(fecha__range=(date_from, date_to))
    waste_qs = MermaPOS.objects.filter(fecha__range=(date_from, date_to))
    if sucursal_id:
        sales_qs = sales_qs.filter(sucursal_id=sucursal_id)
        waste_qs = waste_qs.filter(sucursal_id=sucursal_id)

    plan_map, ventas_map, mermas_map = (
        _ingredient_totals(qs, member_to_canonical, canonical_rows, missing_units)
        for qs in (plan_qs, sales_qs, waste_qs)
    )
    insumo_ids = set(plan_map) | set(ventas_map) | set(mermas_map)
    existencia_raw = {}
    for e in ExistenciaInsumo.objects.select_related("insumo__unidad_base"):
        canonical_id = member_to_canonical.get(e.insumo_id, e.insumo_id)
        canonical_insumo = canonical_rows.get(canonical_id, {}).get("canonical", e.insumo)
        stock = e.stock_actual
        if canonical_id != e.insumo_id:
            source_unit = e.insumo.unidad_base.codigo if e.insumo.unidad_base_id else None
            target_unit = canonical_insumo.unidad_base.codigo if canonical_insumo.unidad_base_id else None
            stock = convert(stock, source_unit, target_unit)
            if stock is None:
                missing_units.add(canonical_id)
                continue
        bucket = existencia_raw.setdefault(canonical_id, {"insumo": canonical_insumo, "stock_actual": Decimal("0")})
        bucket["stock_actual"] += stock
    existencia_map = existencia_raw
    insumo_ids = (insumo_ids | set(existencia_map)) - missing_units
    unmapped_insumos = Insumo.objects.select_related("unidad_base").in_bulk(
        insumo_ids.difference(existencia_map, canonical_rows)
    )

    rows = []
    total_alertas = 0
    total_observar = 0

    for insumo_id in sorted(insumo_ids):
        ex = existencia_map.get(insumo_id)
        insumo = ex["insumo"] if ex else (
            canonical_rows.get(insumo_id, {}).get("canonical") or unmapped_insumos.get(insumo_id)
        )
        nombre = insumo.nombre if insumo else f"Insumo #{insumo_id}"
        unidad = insumo.unidad_base.codigo if insumo and insumo.unidad_base_id else ""
        produccion = plan_map.get(insumo_id, Decimal("0"))
        ventas = ventas_map.get(insumo_id, Decimal("0"))
        merma = mermas_map.get(insumo_id, Decimal("0"))
        teorico = produccion - ventas - merma
        real = Decimal(str(ex["stock_actual"] if ex else 0))
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
        "insumos": len(insumo_ids) + len(missing_units),
        "sin_unidad_compatible": len(missing_units),
        "alertas": total_alertas,
        "observar": total_observar + len(missing_units),
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
        "insumos_sin_unidad_compatible": list(
            Insumo.objects.filter(pk__in=missing_units).order_by("nombre", "pk").values("id", "nombre")
        ),
        "rows": rows,
    }
