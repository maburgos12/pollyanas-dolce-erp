from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import json
from pathlib import Path

from django.conf import settings
from django.core.cache import cache
from django.db.models import Count, Max, Min, Q, Sum
from django.utils import timezone
from unidecode import unidecode

from core.cache_versions import get_or_set_versioned_cache
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import (
    PointDailyBranchIndicator,
    PointDailySale,
    PointInventorySnapshot,
    PointMonthlySalesOfficial,
    PointProductionLine,
    PointTransferLine,
    PointWasteLine,
)
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import RecetaCostoSemanal
from reportes.models import FactVentaDiaria, SnapshotFlujoCentralMensual, SnapshotLedgerInventarioMensual
from reportes.dashboard_production_dataset import get_dashboard_production_dataset
from ventas.services.sales_read_service import get_sales_range


ZERO = Decimal("0")
ONE = Decimal("1")
Q2 = Decimal("0.01")
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
RECENT_POINT_SOURCE = "/Report/VentasCategorias"
OFFICIAL_PARTIAL_CACHE_PATH = Path("storage/pos_bridge/reports/official_partial_sales_cache.json")
DASHBOARD_CACHE_TTL_SECONDS = int(getattr(settings, "ERP_DASHBOARD_CACHE_TTL_SECONDS", 900) or 900)


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _normalize_text(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


def _dashboard_cache_key(*, panel: str, latest_date: date, months: int | None = None) -> str:
    suffix = f":m{int(months)}" if months is not None else ""
    return f"erp:dashboard:{panel}:{latest_date.year}:{latest_date.month:02d}{suffix}"


def _cache_get(key: str):
    try:
        return cache.get(key)
    except Exception:
        return None


def _cache_set(key: str, value, ttl: int | None = None) -> None:
    try:
        cache.set(key, value, timeout=ttl or DASHBOARD_CACHE_TTL_SECONDS)
    except Exception:
        return None


def _persist_inventory_ledger_snapshot_rows(
    *,
    rows: list[dict[str, object]],
    cutoff_date: date,
    flow_coverage_start: date | None,
    snapshot_coverage_start: date | None,
) -> None:
    if not rows:
        return
    persisted_rows: list[SnapshotLedgerInventarioMensual] = []
    for row in rows:
        month_start = date.fromisoformat(str(row["month_label"]) + "-01")
        persisted_rows.append(
            SnapshotLedgerInventarioMensual(
                month_start=month_start,
                month_end=_month_end(month_start),
                is_partial=bool(row.get("is_partial")),
                opening_job_id=row.get("opening_job_id"),
                closing_job_id=row.get("closing_job_id"),
                opening_units=_to_decimal(row.get("opening_units")),
                production_units=_to_decimal(row.get("production_units")),
                sold_units=_to_decimal(row.get("sold_units")),
                waste_units=_to_decimal(row.get("waste_units")),
                theoretical_closing=_to_decimal(row.get("theoretical_closing")),
                actual_closing=_to_decimal(row.get("actual_closing")),
                variance_units=_to_decimal(row.get("variance_units")),
                metadata={
                    "cutoff_date": cutoff_date.isoformat(),
                    "flow_coverage_start": flow_coverage_start.isoformat() if flow_coverage_start else "",
                    "snapshot_coverage_start": snapshot_coverage_start.isoformat() if snapshot_coverage_start else "",
                },
            )
        )
    SnapshotLedgerInventarioMensual.objects.bulk_create(
        persisted_rows,
        update_conflicts=True,
        unique_fields=["month_start"],
        update_fields=[
            "month_end",
            "is_partial",
            "opening_job_id",
            "closing_job_id",
            "opening_units",
            "production_units",
            "sold_units",
            "waste_units",
            "theoretical_closing",
            "actual_closing",
            "variance_units",
            "metadata",
            "actualizado_en",
        ],
    )


def _inventory_ledger_panel_from_snapshot(*, latest_date: date, months: int) -> dict[str, object] | None:
    month_windows = _month_windows(latest_date, months)
    month_starts = [window[0] for window in month_windows]
    snapshot_rows = {
        row.month_start: row
        for row in SnapshotLedgerInventarioMensual.objects.filter(month_start__in=month_starts).order_by("month_start")
    }
    if not snapshot_rows:
        return None

    rows: list[dict[str, object]] = []
    omitted_months: list[str] = []
    flow_coverage_start = None
    snapshot_coverage_start = None
    for month_start, month_end, is_partial in month_windows:
        snapshot = snapshot_rows.get(month_start)
        if snapshot is None:
            omitted_months.append(month_start.strftime("%Y-%m"))
            continue
        metadata = snapshot.metadata or {}
        if not flow_coverage_start and metadata.get("flow_coverage_start"):
            flow_coverage_start = date.fromisoformat(metadata["flow_coverage_start"])
        if not snapshot_coverage_start and metadata.get("snapshot_coverage_start"):
            snapshot_coverage_start = date.fromisoformat(metadata["snapshot_coverage_start"])
        rows.append(
            {
                "month_label": month_start.strftime("%Y-%m"),
                "is_partial": bool(snapshot.is_partial or is_partial),
                "opening_units": _to_decimal(snapshot.opening_units).quantize(Q2),
                "production_units": _to_decimal(snapshot.production_units).quantize(Q2),
                "sold_units": _to_decimal(snapshot.sold_units).quantize(Q2),
                "waste_units": _to_decimal(snapshot.waste_units).quantize(Q2),
                "theoretical_closing": _to_decimal(snapshot.theoretical_closing).quantize(Q2),
                "actual_closing": _to_decimal(snapshot.actual_closing).quantize(Q2),
                "variance_units": _to_decimal(snapshot.variance_units).quantize(Q2),
                "opening_job_id": snapshot.opening_job_id,
                "closing_job_id": snapshot.closing_job_id,
            }
        )

    for index, row in enumerate(rows[:-1]):
        next_row = rows[index + 1]
        row["next_opening_units"] = next_row["opening_units"]
        row["rollover_gap"] = (_to_decimal(next_row["opening_units"]) - _to_decimal(row["actual_closing"])).quantize(Q2)
    if rows:
        rows[-1]["next_opening_units"] = None
        rows[-1]["rollover_gap"] = None

    if not rows:
        return None
    basis_note = (
        "Puente mensual de inventario de red: inventario inicial + producción - venta - merma = cierre teórico. "
        "Las transferencias internas no se suman porque no cambian el inventario total de la red."
    )
    if snapshot_coverage_start:
        basis_note += f" Cobertura real de snapshots desde {snapshot_coverage_start.strftime('%Y-%m')}."
    return {
        "cutoff_date": latest_date,
        "rows": rows,
        "latest_row": rows[-1] if rows else None,
        "basis_note": basis_note,
        "flow_coverage_start": flow_coverage_start,
        "snapshot_coverage_start": snapshot_coverage_start,
        "omitted_months": omitted_months,
        "visible_months_count": len(rows),
    }


def _persist_central_flow_snapshot_rows(*, rows: list[dict[str, object]], cutoff_date: date) -> None:
    if not rows:
        return
    persisted_rows: list[SnapshotFlujoCentralMensual] = []
    for row in rows:
        month_start = date.fromisoformat(str(row["month_label"]) + "-01")
        persisted_rows.append(
            SnapshotFlujoCentralMensual(
                month_start=month_start,
                month_end=_month_end(month_start),
                is_partial=bool(row.get("is_partial")),
                central_source=str(row.get("central_source") or ""),
                production_units=_to_decimal(row.get("production_units")),
                transfer_units=_to_decimal(row.get("transfer_units")),
                sold_units=_to_decimal(row.get("sold_units")),
                waste_units=_to_decimal(row.get("waste_units")),
                supply_units=_to_decimal(row.get("supply_units")),
                net_units=_to_decimal(row.get("net_units")),
                actual_inventory_closing=_to_decimal(row.get("actual_inventory_closing")),
                inventory_variance_units=_to_decimal(row.get("inventory_variance_units")),
                metadata={"cutoff_date": cutoff_date.isoformat()},
            )
        )
    SnapshotFlujoCentralMensual.objects.bulk_create(
        persisted_rows,
        update_conflicts=True,
        unique_fields=["month_start"],
        update_fields=[
            "month_end",
            "is_partial",
            "central_source",
            "production_units",
            "transfer_units",
            "sold_units",
            "waste_units",
            "supply_units",
            "net_units",
            "actual_inventory_closing",
            "inventory_variance_units",
            "metadata",
            "actualizado_en",
        ],
    )


def _central_flow_panel_from_snapshot(*, latest_date: date, months: int) -> dict[str, object] | None:
    month_windows = _month_windows(latest_date, months)
    month_starts = [window[0] for window in month_windows]
    snapshot_rows = {
        row.month_start: row
        for row in SnapshotFlujoCentralMensual.objects.filter(month_start__in=month_starts).order_by("month_start")
    }
    if not snapshot_rows:
        return None

    rows: list[dict[str, object]] = []
    for month_start, month_end, is_partial in month_windows:
        snapshot = snapshot_rows.get(month_start)
        if snapshot is None:
            continue
        rows.append(
            {
                "month_label": month_start.strftime("%Y-%m"),
                "is_partial": bool(snapshot.is_partial or is_partial),
                "central_source": snapshot.central_source,
                "production_units": _to_decimal(snapshot.production_units).quantize(Q2),
                "transfer_units": _to_decimal(snapshot.transfer_units).quantize(Q2),
                "sold_units": _to_decimal(snapshot.sold_units).quantize(Q2),
                "waste_units": _to_decimal(snapshot.waste_units).quantize(Q2),
                "supply_units": _to_decimal(snapshot.supply_units).quantize(Q2),
                "net_units": _to_decimal(snapshot.net_units).quantize(Q2),
                "actual_inventory_closing": _to_decimal(snapshot.actual_inventory_closing).quantize(Q2),
                "inventory_variance_units": _to_decimal(snapshot.inventory_variance_units).quantize(Q2),
            }
        )
    if not rows:
        return None
    latest_row = rows[-1]
    return {
        "cutoff_date": latest_date,
        "rows": rows,
        "latest_row": latest_row,
        "basis_note": (
            "Flujo central mensual materializado: producción central + transferencias a CEDIS - venta - merma, "
            "con cierre de inventario tomado del snapshot mensual."
        ),
        "visible_months_count": len(rows),
    }


def _month_windows(latest_date: date, months: int) -> list[tuple[date, date, bool]]:
    windows: list[tuple[date, date, bool]] = []
    for offset in range(months - 1, -1, -1):
        month_anchor = _shift_month(_month_start(latest_date), -offset)
        month_end = _month_end(month_anchor)
        effective_end = min(
            month_end,
            latest_date if month_anchor.year == latest_date.year and month_anchor.month == latest_date.month else month_end,
        )
        windows.append((month_anchor, effective_end, effective_end != month_end))
    return windows


def _aware_range(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date + timedelta(days=1), time.min)
    if timezone.is_aware(timezone.now()):
        start_dt = timezone.make_aware(start_dt)
        end_dt = timezone.make_aware(end_dt)
    return start_dt, end_dt


@lru_cache(maxsize=1)
def _official_partial_sales_cache() -> dict[str, dict]:
    try:
        if not OFFICIAL_PARTIAL_CACHE_PATH.exists():
            return {}
        return json.loads(OFFICIAL_PARTIAL_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _exact_partial_cache_payload(start_date: date, end_date: date) -> dict | None:
    return _official_partial_sales_cache().get(f"{start_date.isoformat()}_{end_date.isoformat()}")


def _best_partial_cache_payload(start_date: date, end_date: date) -> dict | None:
    cache = _official_partial_sales_cache()
    exact = cache.get(f"{start_date.isoformat()}_{end_date.isoformat()}")
    if exact:
        return exact
    best_payload = None
    best_end = None
    for payload in cache.values():
        try:
            period_start = date.fromisoformat(payload["period_start"])
            period_end = date.fromisoformat(payload["period_end"])
        except Exception:
            continue
        if period_start != start_date:
            continue
        if period_end > end_date:
            continue
        if best_end is None or period_end > best_end:
            best_end = period_end
            best_payload = payload
    return best_payload


def _canonical_sales_range_summary(*, start_date: date, end_date: date) -> dict[str, object]:
    return get_sales_range(
        start_date=start_date,
        end_date=end_date,
        coverage_policy="prefer_complete",
    )


def _partial_month_amount_quantity(*, start_date: date, end_date: date) -> tuple[Decimal, Decimal]:
    payload = _best_partial_cache_payload(start_date, end_date)
    if payload is None:
        aggregate = _canonical_sales_range_summary(start_date=start_date, end_date=end_date)
        amount = _to_decimal(aggregate.get("monto"))
        quantity = _to_decimal(aggregate.get("cantidad"))
        return amount, quantity

    amount = _to_decimal(payload.get("total_amount"))
    quantity = _to_decimal(payload.get("total_quantity"))
    cached_end = date.fromisoformat(payload["period_end"])
    if cached_end >= end_date:
        return amount, quantity

    supplement_start = cached_end + timedelta(days=1)
    supplement = _canonical_sales_range_summary(start_date=supplement_start, end_date=end_date)
    amount += _to_decimal(supplement.get("monto"))
    quantity += _to_decimal(supplement.get("cantidad"))
    return amount, quantity


def _partial_sales_cache_latest_end() -> date | None:
    latest = None
    for payload in _official_partial_sales_cache().values():
        try:
            period_end = date.fromisoformat(payload["period_end"])
        except Exception:
            continue
        if latest is None or period_end > latest:
            latest = period_end
    return latest


def _recipe_cost_map_for_sales_lens(
    *,
    latest_week: date | None,
    familia: str | None = None,
    categoria: str | None = None,
    q: str = "",
) -> dict[int, Decimal]:
    if not latest_week:
        return {}

    cost_qs = RecetaCostoSemanal.objects.filter(
        week_start=latest_week,
        scope_type=RecetaCostoSemanal.SCOPE_RECIPE,
        receta__tipo="PRODUCTO_FINAL",
        receta_id__isnull=False,
    )
    if familia:
        cost_qs = cost_qs.filter(familia=familia)
    if categoria:
        cost_qs = cost_qs.filter(categoria=categoria)
    if q:
        cost_qs = cost_qs.filter(Q(label__icontains=q) | Q(receta__nombre__icontains=q))

    cost_map = {
        int(row["receta_id"]): _to_decimal(row["costo_total"])
        for row in cost_qs.values("receta_id", "costo_total")
    }

    grouped_qs = RecetaCostoSemanal.objects.filter(
        week_start=latest_week,
        scope_type=RecetaCostoSemanal.SCOPE_GROUPED_ADDON,
        base_receta_id__isnull=False,
    )
    if familia:
        grouped_qs = grouped_qs.filter(familia=familia)
    if categoria:
        grouped_qs = grouped_qs.filter(categoria=categoria)
    if q:
        grouped_qs = grouped_qs.filter(Q(label__icontains=q) | Q(base_receta__nombre__icontains=q))

    grouped_by_base: dict[int, list[RecetaCostoSemanal]] = defaultdict(list)
    for row in grouped_qs.select_related("base_receta"):
        grouped_by_base[int(row.base_receta_id)].append(row)

    # Regla segura: solo sustituimos costo base cuando existe exactamente un
    # add-on aprobado activo para esa receta base en la semana visible.
    for base_receta_id, rows in grouped_by_base.items():
        if len(rows) != 1:
            continue
        cost_map[base_receta_id] = _to_decimal(rows[0].costo_total)

    return cost_map


def _month_start(day: date) -> date:
    return date(day.year, day.month, 1)


def _month_end(day: date) -> date:
    return date(day.year, day.month, monthrange(day.year, day.month)[1])


def _shift_month(day: date, offset: int) -> date:
    year = day.year
    month = day.month + offset
    while month > 12:
        year += 1
        month -= 12
    while month < 1:
        year -= 1
        month += 12
    return date(year, month, 1)


def _week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _week_end(day: date) -> date:
    return _week_start(day) + timedelta(days=6)


def _decimal_avg(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    return sum(values, ZERO) / Decimal(str(len(values)))


def _decimal_median(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _safe_pct(delta: Decimal, base: Decimal) -> Decimal | None:
    if base == 0:
        return None
    return (delta / base) * Decimal("100")


def _cost_ratio_signal(cost_pct: Decimal | None) -> dict[str, str | None]:
    if cost_pct is None:
        return {"tone": "neutral", "label": "Sin lectura", "detail": None}
    if cost_pct > Decimal("50"):
        return {"tone": "critical", "label": "Accionar pronto", "detail": "Costo MP arriba de 50% sobre venta."}
    if cost_pct > Decimal("45"):
        return {"tone": "danger", "label": "Rojo", "detail": "Costo MP entre 45% y 50% sobre venta."}
    if cost_pct > Decimal("40"):
        return {"tone": "warning", "label": "Amarillo", "detail": "Costo MP entre 40% y 45% sobre venta."}
    if cost_pct >= Decimal("35"):
        return {"tone": "success", "label": "Verde", "detail": "Costo MP entre 35% y 40% sobre venta."}
    return {"tone": "success", "label": "Óptimo", "detail": "Costo MP por debajo de 35% sobre venta."}


def _expense_ratio_signal(expense_pct: Decimal | None) -> dict[str, str | None]:
    if expense_pct is None:
        return {"tone": "neutral", "label": "Sin lectura", "detail": None}
    if expense_pct > Decimal("40"):
        return {"tone": "critical", "label": "Accionar pronto", "detail": "Gasto recurrente arriba de 40% sobre venta."}
    if expense_pct > Decimal("32"):
        return {"tone": "danger", "label": "Rojo", "detail": "Gasto recurrente entre 32% y 40% sobre venta."}
    if expense_pct > Decimal("25"):
        return {"tone": "warning", "label": "Amarillo", "detail": "Gasto recurrente entre 25% y 32% sobre venta."}
    return {"tone": "success", "label": "Sano", "detail": "Gasto recurrente por debajo de 25% sobre venta."}


def _margin_ratio_signal(margin_pct: Decimal | None) -> dict[str, str | None]:
    if margin_pct is None:
        return {"tone": "neutral", "label": "Sin lectura", "detail": None}
    if margin_pct < Decimal("0"):
        return {"tone": "danger", "label": "Rojo", "detail": "Resultado negativo sobre venta."}
    if margin_pct < Decimal("8"):
        return {"tone": "warning", "label": "Amarillo", "detail": "Margen menor a 8% sobre venta."}
    if margin_pct < Decimal("15"):
        return {"tone": "success", "label": "Verde", "detail": "Margen entre 8% y 15% sobre venta."}
    return {"tone": "success", "label": "Óptimo", "detail": "Margen arriba de 15% sobre venta."}


def _gross_margin_signal(gross_margin_pct: Decimal | None) -> dict[str, str | None]:
    if gross_margin_pct is None:
        return {"tone": "neutral", "label": "Sin lectura", "detail": None}
    if gross_margin_pct < Decimal("45"):
        return {"tone": "danger", "label": "Presión", "detail": "El margen bruto real está por debajo de 45% de la venta."}
    if gross_margin_pct < Decimal("55"):
        return {"tone": "warning", "label": "Vigilado", "detail": "El margen bruto real está entre 45% y 55% de la venta."}
    if gross_margin_pct < Decimal("65"):
        return {"tone": "success", "label": "Sano", "detail": "El margen bruto real está entre 55% y 65% de la venta."}
    return {"tone": "success", "label": "Fuerte", "detail": "El margen bruto real está arriba de 65% de la venta."}


def _pending_signal(label: str, detail: str) -> dict[str, str | None]:
    return {"tone": "warning", "label": label, "detail": detail}


def _health_priority(tone: str | None) -> int:
    mapping = {
        "critical": 4,
        "danger": 3,
        "warning": 2,
        "success": 1,
        "neutral": 0,
    }
    return mapping.get(str(tone or "neutral"), 0)


def _combined_health_signal(*signals: dict[str, str | None]) -> dict[str, str | None]:
    worst = max(signals, key=lambda item: _health_priority(item.get("tone")), default={"tone": "neutral", "label": "Sin lectura", "detail": None})
    tone = str(worst.get("tone") or "neutral")
    if tone == "critical":
        return {"tone": "critical", "label": "Mes crítico", "detail": "Costo o gasto están fuera de rango y el margen está bajo fuerte presión."}
    if tone == "danger":
        return {"tone": "danger", "label": "Bajo presión", "detail": "Hay señales rojas en costo, gasto o margen que requieren corrección."}
    if tone == "warning":
        return {"tone": "warning", "label": "Vigilado", "detail": "El margen todavía es positivo, pero ya muestra presión."}
    if tone == "success":
        return {"tone": "success", "label": "Sano", "detail": "Costo, gasto y margen están dentro de rangos aceptables."}
    return {"tone": "neutral", "label": "Sin lectura", "detail": None}


def _attainment_signal(attainment_pct: Decimal | None) -> dict[str, str | None]:
    if attainment_pct is None:
        return {"tone": "neutral", "label": "Sin comparativo", "detail": None}
    if attainment_pct >= Decimal("100"):
        return {"tone": "success", "label": "Meta cumplida", "detail": "La venta real alcanzó o superó la meta."}
    if attainment_pct >= Decimal("90"):
        return {"tone": "warning", "label": "Cerca de meta", "detail": "La venta real está cerca de la meta."}
    return {"tone": "danger", "label": "Debajo de meta", "detail": "La venta real sigue debajo de la meta planeada."}


@lru_cache(maxsize=1)
def _official_sales_stage_max_date() -> date | None:
    return (
        PointDailySale.objects.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
        .aggregate(v=Max("sale_date"))
        .get("v")
    )


@lru_cache(maxsize=1)
def _recent_sales_stage_max_date() -> date | None:
    return (
        PointDailySale.objects.filter(source_endpoint=RECENT_POINT_SOURCE)
        .aggregate(v=Max("sale_date"))
        .get("v")
    )


@lru_cache(maxsize=1)
def _sales_cutoff_date() -> date | None:
    sale_date = max(
        [value for value in [_official_sales_stage_max_date(), _recent_sales_stage_max_date()] if value],
        default=None,
    )
    indicator_date = PointDailyBranchIndicator.objects.aggregate(v=Max("indicator_date")).get("v")
    if sale_date and indicator_date:
        return min(sale_date, indicator_date)
    return sale_date or indicator_date


def _operational_sales_filters(*, start_date: date, end_date: date) -> Q:
    official_max = _official_sales_stage_max_date()
    q = Q()
    if official_max:
        official_end = min(end_date, official_max)
        if start_date <= official_end:
            q |= Q(source_endpoint=OFFICIAL_POINT_SOURCE, sale_date__gte=start_date, sale_date__lte=official_end)
        recent_start = max(start_date, official_max + timedelta(days=1))
    else:
        recent_start = start_date
    if recent_start <= end_date:
        q |= Q(source_endpoint=RECENT_POINT_SOURCE, sale_date__gte=recent_start, sale_date__lte=end_date)
    return q


@lru_cache(maxsize=1)
def _production_cutoff_date() -> date | None:
    return PointProductionLine.objects.aggregate(v=Max("production_date")).get("v")


@lru_cache(maxsize=1)
def _waste_cutoff_date() -> date | None:
    value = PointWasteLine.objects.aggregate(v=Max("movement_at")).get("v")
    if value is None:
        return None
    return timezone.localtime(value).date() if timezone.is_aware(value) else value.date()


@lru_cache(maxsize=1)
def _common_flow_cutoff_date() -> date | None:
    candidates = [value for value in [_sales_cutoff_date(), _production_cutoff_date(), _waste_cutoff_date()] if value]
    if not candidates:
        return None
    return min(candidates)


@lru_cache(maxsize=1)
def _first_central_cedis_production_date() -> date | None:
    return (
        PointProductionLine.objects.filter(is_insumo=False, branch__name__iexact="CEDIS")
        .aggregate(v=Min("production_date"))
        .get("v")
    )


def _central_production_branch_for_day(work_date: date, *, first_cedis_date: date | None = None) -> str | None:
    if first_cedis_date is None:
        first_cedis_date = _first_central_cedis_production_date()
    if first_cedis_date is None:
        return "matriz"
    return "cedis" if work_date >= first_cedis_date else "matriz"


def _active_sales_queryset(*, start_date: date, end_date: date):
    return PointDailySale.objects.filter(
        branch__erp_branch_id__isnull=False,
        branch__erp_branch__activa=True,
    ).filter(
        _operational_sales_filters(start_date=start_date, end_date=end_date)
    )


def _active_indicator_queryset(*, start_date: date, end_date: date):
    return PointDailyBranchIndicator.objects.filter(
        indicator_date__gte=start_date,
        indicator_date__lte=end_date,
        branch__erp_branch_id__isnull=False,
        branch__erp_branch__activa=True,
    )


def _sales_fact_daily_map(*, start_date: date, end_date: date) -> dict[date, tuple[Decimal, Decimal]]:
    rows = (
        FactVentaDiaria.objects.filter(
            fecha__gte=start_date,
            fecha__lte=end_date,
            sucursal_id__isnull=False,
            sucursal__activa=True,
        )
        .values("fecha")
        .annotate(
            amount=Sum("venta_total"),
            quantity=Sum("cantidad"),
        )
    )
    return {
        row["fecha"]: (
            _to_decimal(row.get("amount")),
            _to_decimal(row.get("quantity")),
        )
        for row in rows
    }


def _indicator_daily_ticket_map(*, start_date: date, end_date: date) -> dict[date, int]:
    rows = (
        PointDailyBranchIndicator.objects.filter(
            indicator_date__gte=start_date,
            indicator_date__lte=end_date,
            branch__erp_branch_id__isnull=False,
            branch__erp_branch__activa=True,
        )
        .values("indicator_date")
        .annotate(total_tickets=Sum("total_tickets"))
    )
    return {
        row["indicator_date"]: int(row.get("total_tickets") or 0)
        for row in rows
    }


def _sum_sales_daily_map(
    sales_daily_map: dict[date, tuple[Decimal, Decimal]],
    *,
    start_date: date,
    end_date: date,
) -> tuple[Decimal, Decimal]:
    amount = ZERO
    quantity = ZERO
    cursor = start_date
    while cursor <= end_date:
        row = sales_daily_map.get(cursor)
        if row:
            amount += row[0]
            quantity += row[1]
        cursor += timedelta(days=1)
    return amount, quantity


def _sum_ticket_daily_map(
    ticket_daily_map: dict[date, int],
    *,
    start_date: date,
    end_date: date,
) -> int:
    total = 0
    cursor = start_date
    while cursor <= end_date:
        total += int(ticket_daily_map.get(cursor) or 0)
        cursor += timedelta(days=1)
    return total


def _monthly_official_sales_cache_map(*, month_starts: list[date]) -> dict[date, PointMonthlySalesOfficial]:
    if not month_starts:
        return {}
    rows = PointMonthlySalesOfficial.objects.filter(month_start__in=month_starts)
    return {row.month_start: row for row in rows}


def _operational_category(*, category: str = "", family: str = "", item_name: str = "") -> str:
    raw = (category or family or item_name or "").strip()
    if not raw:
        return "Sin categoría"
    lowered = _normalize_text(raw)
    if "pastel mediano" in lowered:
        return "Pastel Mediano"
    if "pastel grande" in lowered:
        return "Pastel Grande"
    if "pastel chico" in lowered:
        return "Pastel Chico"
    if "pay grande" in lowered:
        return "Pay Grande"
    if "pay mediano" in lowered:
        return "Pay Mediano"
    if "reban" in lowered:
        return "Rebanada"
    if "individual" in lowered:
        return "Individual"
    if "mini" in lowered:
        return "Mini"
    if "vaso" in lowered:
        return "Vasos"
    if "bollo" in lowered:
        return "Bollo"
    if "galleta" in lowered:
        return "Galletas"
    if "empanad" in lowered:
        return "Empanadas"
    return raw[:60]


def _is_network_inventory_branch(*, point_name: str = "", erp_active: bool | None = None) -> bool:
    normalized = _normalize_text(point_name)
    return _is_network_inventory_branch_normalized(normalized_point_name=normalized, erp_active=erp_active)


def _is_network_inventory_branch_normalized(*, normalized_point_name: str = "", erp_active: bool | None = None) -> bool:
    normalized = (normalized_point_name or "").strip()
    if normalized in {"almacen", "devoluciones"}:
        return False
    if normalized in {"cedis", "produccion crucero"}:
        return True
    return bool(erp_active)


@dataclass(slots=True)
class ForecastWeek:
    week_start: date
    week_end: date
    amount: Decimal
    quantity: Decimal
    tickets: int
    avg_ticket: Decimal
    atypical: bool
    atypical_reason: str


def build_sales_forecast_panel(*, latest_date: date | None = None, lookback_weeks: int = 8, baseline_weeks: int = 3) -> dict[str, object]:
    latest_date = latest_date or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    min_high_uplift_pct = _to_decimal(
        getattr(settings, "ERP_DASHBOARD_FORECAST_HIGH_SCENARIO_MIN_UPLIFT_PCT", "5"),
        "5",
    )
    if min_high_uplift_pct < ZERO:
        min_high_uplift_pct = ZERO
    min_high_uplift_factor = ONE + (min_high_uplift_pct / Decimal("100"))
    current_week_start = _week_start(latest_date)
    week_starts = [current_week_start - timedelta(days=7 * idx) for idx in range(max(lookback_weeks, baseline_weeks))]
    week_starts.reverse()
    range_start = week_starts[0]
    range_end = week_starts[-1] + timedelta(days=6)
    sales_daily_map = _sales_fact_daily_map(start_date=range_start, end_date=range_end)
    ticket_daily_map = _indicator_daily_ticket_map(start_date=range_start, end_date=range_end)

    weekly_rows: list[ForecastWeek] = []
    for week_start in week_starts:
        week_end = week_start + timedelta(days=6)
        amount, quantity = _sum_sales_daily_map(
            sales_daily_map,
            start_date=week_start,
            end_date=week_end,
        )
        tickets = _sum_ticket_daily_map(
            ticket_daily_map,
            start_date=week_start,
            end_date=week_end,
        )
        avg_ticket = (amount / Decimal(str(tickets))) if tickets > 0 else ZERO
        weekly_rows.append(
            ForecastWeek(
                week_start=week_start,
                week_end=week_end,
                amount=amount,
                quantity=quantity,
                tickets=tickets,
                avg_ticket=avg_ticket,
                atypical=False,
                atypical_reason="",
            )
        )

    historical_rows = [row for row in weekly_rows[:-1] if row.amount > 0]
    amount_median = _decimal_median([row.amount for row in historical_rows])
    qty_median = _decimal_median([row.quantity for row in historical_rows])
    for row in historical_rows:
        reasons: list[str] = []
        if amount_median > 0 and row.amount >= amount_median * Decimal("1.35"):
            reasons.append("pico de $")
        if amount_median > 0 and row.amount <= amount_median * Decimal("0.65"):
            reasons.append("bache de $")
        if qty_median > 0 and row.quantity >= qty_median * Decimal("1.35"):
            reasons.append("pico de piezas")
        if qty_median > 0 and row.quantity <= qty_median * Decimal("0.65"):
            reasons.append("bache de piezas")
        row.atypical = bool(reasons)
        row.atypical_reason = ", ".join(reasons)

    ordered_recent = list(reversed(weekly_rows))
    baseline_candidates = [row for row in ordered_recent if not row.atypical and row.amount > 0][:baseline_weeks]
    if len(baseline_candidates) < min(2, baseline_weeks):
        baseline_candidates = [row for row in ordered_recent if row.amount > 0][:baseline_weeks]
    base_amount = _decimal_avg([row.amount for row in baseline_candidates])
    base_quantity = _decimal_avg([row.quantity for row in baseline_candidates])
    base_tickets = int(_decimal_avg([Decimal(str(row.tickets)) for row in baseline_candidates])) if baseline_candidates else 0
    base_avg_ticket = (base_amount / Decimal(str(base_tickets))) if base_tickets > 0 else ZERO

    atypical_recent = [row for row in ordered_recent[:lookback_weeks] if row.atypical]
    high_floor_amount = base_amount * min_high_uplift_factor if base_amount > 0 else ZERO
    high_floor_quantity = base_quantity * min_high_uplift_factor if base_quantity > 0 else ZERO
    high_scenario_amount = max([high_floor_amount] + [row.amount for row in atypical_recent]) if atypical_recent else high_floor_amount
    high_scenario_quantity = (
        max([high_floor_quantity] + [row.quantity for row in atypical_recent]) if atypical_recent else high_floor_quantity
    )
    if atypical_recent:
        high_scenario_note = "Escenario alto toma el mayor pico atípico reciente y nunca baja del colchón mínimo configurado."
    else:
        high_scenario_note = (
            "No se detectaron picos atípicos recientes; el escenario alto usa un colchón mínimo "
            f"de {min_high_uplift_pct.quantize(Q2)}% sobre la base."
        )
    next_week_start = current_week_start + timedelta(days=7)
    next_week_end = next_week_start + timedelta(days=6)

    return {
        "latest_date": latest_date,
        "basis_note": (
            "Pronóstico inferido con las últimas 2-3 semanas normales. "
            "No existe calendario oficial de fechas atípicas parametrizado; los picos se detectan por anomalía histórica. "
            f"Cuando no hay picos, el escenario alto conserva un colchón mínimo de {min_high_uplift_pct.quantize(Q2)}%."
        ),
        "baseline_label": f"{len(baseline_candidates)} semana(s) base",
        "forecast_amount": base_amount.quantize(Q2),
        "forecast_quantity": base_quantity.quantize(Q2),
        "forecast_tickets": base_tickets,
        "forecast_avg_ticket": base_avg_ticket.quantize(Q2),
        "high_scenario_amount": high_scenario_amount.quantize(Q2),
        "high_scenario_quantity": high_scenario_quantity.quantize(Q2),
        "high_scenario_note": high_scenario_note,
        "has_atypical_history": bool(atypical_recent),
        "atypical_rows": [
            {
                "label": f"{row.week_start.strftime('%d %b')} → {row.week_end.strftime('%d %b')}",
                "amount": row.amount.quantize(Q2),
                "quantity": row.quantity.quantize(Q2),
                "reason": row.atypical_reason,
            }
            for row in atypical_recent[:4]
        ],
        "target_week_label": f"{next_week_start.strftime('%d %b')} → {next_week_end.strftime('%d %b')}",
        "weekly_rows": [
            {
                "week_label": f"{row.week_start.strftime('%d %b')} → {row.week_end.strftime('%d %b')}",
                "amount": row.amount.quantize(Q2),
                "quantity": row.quantity.quantize(Q2),
                "tickets": row.tickets,
                "avg_ticket": row.avg_ticket.quantize(Q2),
                "atypical": row.atypical,
                "atypical_reason": row.atypical_reason,
            }
            for row in ordered_recent[:6]
        ],
    }


def build_monthly_yoy_panel(*, latest_date: date | None = None, months: int = 6) -> dict[str, object]:
    latest_date = latest_date or _sales_cutoff_date() or _partial_sales_cache_latest_end() or (timezone.localdate() - timedelta(days=1))
    partial_cache = _official_partial_sales_cache()
    current_year = latest_date.year
    year_start = date(current_year, 1, 1)
    month_starts = []
    cursor = year_start
    while cursor <= latest_date:
        month_starts.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    earliest_current_start = month_starts[0] if month_starts else _month_start(latest_date)
    earliest_prev_start = date(earliest_current_start.year - 1, earliest_current_start.month, 1)
    sales_daily_map = _sales_fact_daily_map(start_date=earliest_prev_start, end_date=latest_date)
    ticket_daily_map = _indicator_daily_ticket_map(start_date=earliest_prev_start, end_date=latest_date)
    official_cache_map = _monthly_official_sales_cache_map(
        month_starts=month_starts + [date(month_start.year - 1, month_start.month, 1) for month_start in month_starts]
    )
    rows: list[dict[str, object]] = []
    for month_anchor in month_starts:
        current_start = month_anchor
        current_end = _month_end(month_anchor)
        partial_cutoff = latest_date if month_anchor.year == latest_date.year and month_anchor.month == latest_date.month else current_end
        current_end = min(current_end, partial_cutoff)
        current_partial_payload = None

        prev_year_start = date(current_start.year - 1, current_start.month, 1)
        prev_year_limit_day = min(current_end.day, monthrange(prev_year_start.year, prev_year_start.month)[1])
        prev_year_end = date(prev_year_start.year, prev_year_start.month, prev_year_limit_day)

        current_month_cache = official_cache_map.get(current_start)
        prev_month_cache = official_cache_map.get(prev_year_start)
        current_is_full_month = current_end == _month_end(month_anchor)
        prev_is_full_month = prev_year_end == _month_end(prev_year_start)
        if not current_is_full_month:
            current_partial_payload = _best_partial_cache_payload(current_start, current_end)
        prev_partial_payload = None
        if prev_month_cache and not prev_is_full_month:
            partial_ranges = (prev_month_cache.raw_payload or {}).get("partial_ranges") or {}
            prev_partial_payload = partial_ranges.get(f"{prev_year_start.isoformat()}_{prev_year_end.isoformat()}")
        if prev_partial_payload is None and not prev_is_full_month:
            prev_partial_payload = partial_cache.get(f"{prev_year_start.isoformat()}_{prev_year_end.isoformat()}")

        if current_month_cache and current_is_full_month:
            amount = _to_decimal(current_month_cache.total_amount)
            qty = _to_decimal(current_month_cache.total_quantity)
            tickets = _sum_ticket_daily_map(
                ticket_daily_map,
                start_date=current_start,
                end_date=current_end,
            )
            avg_ticket = (amount / Decimal(str(tickets))) if tickets > 0 else ZERO
        elif current_partial_payload is not None:
            amount = _to_decimal(current_partial_payload.get("total_amount"))
            qty = _to_decimal(current_partial_payload.get("total_quantity"))
            cached_end = date.fromisoformat(current_partial_payload["period_end"])
            if cached_end < current_end:
                supplement_amount, supplement_qty = _sum_sales_daily_map(
                    sales_daily_map,
                    start_date=cached_end + timedelta(days=1),
                    end_date=current_end,
                )
                amount += supplement_amount
                qty += supplement_qty
            tickets = _sum_ticket_daily_map(
                ticket_daily_map,
                start_date=current_start,
                end_date=current_end,
            )
            avg_ticket = (amount / Decimal(str(tickets))) if tickets > 0 else ZERO
        else:
            amount, qty = _sum_sales_daily_map(
                sales_daily_map,
                start_date=current_start,
                end_date=current_end,
            )
            tickets = _sum_ticket_daily_map(
                ticket_daily_map,
                start_date=current_start,
                end_date=current_end,
            )
            avg_ticket = (amount / Decimal(str(tickets))) if tickets > 0 else ZERO

        prev_official_available = bool(prev_month_cache and prev_is_full_month)
        if prev_official_available:
            prev_amount = _to_decimal(prev_month_cache.total_amount)
            prev_qty = _to_decimal(prev_month_cache.total_quantity)
            prev_tickets = _sum_ticket_daily_map(
                ticket_daily_map,
                start_date=prev_year_start,
                end_date=prev_year_end,
            )
            prev_avg_ticket = (prev_amount / Decimal(str(prev_tickets))) if prev_tickets > 0 else ZERO
        elif prev_partial_payload is not None:
            prev_amount = _to_decimal(prev_partial_payload.get("total_amount"))
            prev_qty = _to_decimal(prev_partial_payload.get("total_quantity"))
            prev_tickets = 0
            prev_avg_ticket = None
        elif not prev_is_full_month:
            # Fallback: calcular desde sales_daily_map aunque sea mes parcial del año previo
            prev_amount, prev_qty = _sum_sales_daily_map(
                sales_daily_map,
                start_date=prev_year_start,
                end_date=prev_year_end,
            )
            prev_tickets = _sum_ticket_daily_map(
                ticket_daily_map,
                start_date=prev_year_start,
                end_date=prev_year_end,
            )
            prev_avg_ticket = (prev_amount / Decimal(str(prev_tickets))) if prev_tickets > 0 else ZERO
            if prev_amount == ZERO:
                prev_amount = None
                prev_qty = None
                prev_avg_ticket = None
        else:
            prev_amount, prev_qty = _sum_sales_daily_map(
                sales_daily_map,
                start_date=prev_year_start,
                end_date=prev_year_end,
            )
            prev_tickets = _sum_ticket_daily_map(
                ticket_daily_map,
                start_date=prev_year_start,
                end_date=prev_year_end,
            )
            prev_avg_ticket = (prev_amount / Decimal(str(prev_tickets))) if prev_tickets > 0 else ZERO

        amount_delta = (amount - prev_amount) if prev_amount is not None else None
        qty_delta = (qty - prev_qty) if prev_qty is not None else None
        rows.append(
            {
                "month_label": current_start.strftime("%Y-%m"),
                "is_partial": current_end != _month_end(month_anchor),
                "amount": amount.quantize(Q2),
                "quantity": qty.quantize(Q2),
                "tickets": tickets,
                "avg_ticket": avg_ticket.quantize(Q2),
                "prev_amount": prev_amount.quantize(Q2) if prev_amount is not None else None,
                "prev_quantity": prev_qty.quantize(Q2) if prev_qty is not None else None,
                "prev_tickets": prev_tickets,
                "prev_avg_ticket": prev_avg_ticket.quantize(Q2) if prev_avg_ticket is not None else None,
                "prev_official_available": prev_official_available or prev_partial_payload is not None,
                "amount_delta": amount_delta.quantize(Q2) if amount_delta is not None else None,
                "qty_delta": qty_delta.quantize(Q2) if qty_delta is not None else None,
                "amount_delta_pct": _safe_pct(amount_delta, prev_amount) if amount_delta is not None and prev_amount is not None else None,
                "qty_delta_pct": _safe_pct(qty_delta, prev_qty) if qty_delta is not None and prev_qty is not None else None,
            }
        )

    latest_row = rows[-1] if rows else None
    latest_comparable_row = next(
        (row for row in reversed(rows) if row.get("amount_delta_pct") is not None),
        None,
    )
    hero_row = latest_row
    hero_note = f"{latest_row['month_label']} · comparación al mismo corte del año previo" if latest_row else "Sin mes"
    hero_mode = "current"
    if latest_row and latest_row.get("amount_delta_pct") is None and latest_comparable_row is not None:
        hero_row = latest_comparable_row
        hero_mode = "last_closed"
        hero_note = (
            f"{hero_row['month_label']} último mes cerrado comparable · "
            f"{latest_row['month_label']} sigue parcial"
        )
    return {
        "rows": rows,
        "latest_row": latest_row,
        "latest_comparable_row": latest_comparable_row,
        "hero_row": hero_row,
        "hero_mode": hero_mode,
        "hero_note": hero_note,
        "basis_note": "Mes contra mismo mes del año anterior. Los meses cerrados usan cache oficial mensual Point. El mes parcial actual usa rango oficial equivalente cuando ya existe cacheado; si no, se deja sin comparativo.",
    }


def build_profitability_panel(
    *,
    latest_date: date | None = None,
    lookback_days: int = 28,
    familia: str | None = None,
    categoria: str | None = None,
    bucket: str | None = None,
    q: str = "",
) -> dict[str, object]:
    latest_date = latest_date or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    window_start = latest_date - timedelta(days=max(lookback_days - 1, 0))
    prev_start = window_start - timedelta(days=lookback_days)
    prev_end = window_start - timedelta(days=1)
    latest_week = RecetaCostoSemanal.objects.aggregate(v=Max("week_start")).get("v")
    q = (q or "").strip()

    cost_map = _recipe_cost_map_for_sales_lens(
        latest_week=latest_week,
        familia=familia,
        categoria=categoria,
        q=q,
    )

    current_rows = _active_sales_queryset(start_date=window_start, end_date=latest_date).filter(
        total_amount__gt=0,
        receta_id__isnull=False,
    )
    if familia:
        current_rows = current_rows.filter(receta__familia=familia)
    if categoria:
        current_rows = current_rows.filter(receta__categoria=categoria)
    if q:
        current_rows = current_rows.filter(receta__nombre__icontains=q)
    current_rows = current_rows.values("receta_id", "receta__nombre", "receta__familia", "receta__categoria").annotate(
        revenue=Sum("total_amount"),
        quantity=Sum("quantity"),
    )

    prev_rows = _active_sales_queryset(start_date=prev_start, end_date=prev_end).filter(
        total_amount__gt=0,
        receta_id__isnull=False,
    )
    if familia:
        prev_rows = prev_rows.filter(receta__familia=familia)
    if categoria:
        prev_rows = prev_rows.filter(receta__categoria=categoria)
    if q:
        prev_rows = prev_rows.filter(receta__nombre__icontains=q)
    prev_map = {
        int(row["receta_id"]): _to_decimal(row["quantity"])
        for row in prev_rows.values("receta_id").annotate(quantity=Sum("quantity"))
    }

    material_rows: list[dict[str, object]] = []
    qty_values: list[Decimal] = []
    margin_values: list[Decimal] = []
    cost_pct_values: list[Decimal] = []
    for row in current_rows:
        receta_id = int(row["receta_id"])
        unit_cost = cost_map.get(receta_id)
        if unit_cost is None:
            continue
        qty = _to_decimal(row["quantity"])
        revenue = _to_decimal(row["revenue"])
        if qty <= 0 or revenue <= 0:
            continue
        asp = revenue / qty
        cost_total = qty * unit_cost
        margin = revenue - cost_total
        margin_pct = _safe_pct(margin, revenue)
        cost_pct = _safe_pct(cost_total, revenue)
        cost_signal = _cost_ratio_signal(cost_pct)
        prev_qty = prev_map.get(receta_id, ZERO)
        trend_pct = _safe_pct(qty - prev_qty, prev_qty)
        qty_values.append(qty)
        if margin_pct is not None:
            margin_values.append(margin_pct)
        if cost_pct is not None:
            cost_pct_values.append(cost_pct)
        material_rows.append(
            {
                "receta_id": receta_id,
                "label": row["receta__nombre"],
                "familia": row["receta__familia"] or "",
                "categoria": row["receta__categoria"] or "",
                "revenue": revenue.quantize(Q2),
                "quantity": qty.quantize(Q2),
                "asp": asp.quantize(Q2),
                "unit_cost": unit_cost.quantize(Q2),
                "cost_total": cost_total.quantize(Q2),
                "margin": margin.quantize(Q2),
                "margin_pct": margin_pct.quantize(Q2) if margin_pct is not None else None,
                "cost_pct": cost_pct.quantize(Q2) if cost_pct is not None else None,
                "cost_signal_tone": cost_signal["tone"],
                "cost_signal_label": cost_signal["label"],
                "cost_signal_detail": cost_signal["detail"],
                "prev_quantity": prev_qty.quantize(Q2),
                "trend_pct": trend_pct.quantize(Q2) if trend_pct is not None else None,
            }
        )

    qty_median = _decimal_median(qty_values) if qty_values else ZERO
    margin_median = _decimal_median(margin_values) if margin_values else ZERO
    promo_candidates: list[dict[str, object]] = []
    selected_bucket = bucket
    for row in material_rows:
        qty = _to_decimal(row["quantity"])
        margin_pct = _to_decimal(row["margin_pct"])
        high_volume = qty >= qty_median if qty_median > 0 else True
        healthy_margin = margin_pct >= margin_median if margin_median > 0 else True
        trend_pct = row["trend_pct"]
        if high_volume and healthy_margin:
            recommendation = "Defender precio y disponibilidad"
            row_bucket = "Defender"
        elif high_volume and not healthy_margin:
            recommendation = "Revisar costo o subir precio"
            row_bucket = "Ajustar margen"
        elif (not high_volume) and healthy_margin:
            recommendation = "Promoción táctica"
            row_bucket = "Promocionar"
            promo_candidates.append(row)
        else:
            recommendation = "Depurar, reformular o sacar de foco"
            row_bucket = "Revisar portafolio"
        if trend_pct is not None and _to_decimal(trend_pct) < Decimal("-8") and healthy_margin:
            recommendation = "Promoción táctica inmediata"
            row_bucket = "Promocionar"
            if row not in promo_candidates:
                promo_candidates.append(row)
        row["bucket"] = row_bucket
        row["recommendation"] = recommendation

    if selected_bucket:
        material_rows = [row for row in material_rows if str(row.get("bucket") or "") == selected_bucket]
        promo_candidates = [row for row in promo_candidates if str(row.get("bucket") or "") == selected_bucket]

    material_rows.sort(key=lambda item: (-_to_decimal(item["margin"]), -_to_decimal(item["revenue"]), item["label"]))
    promo_candidates.sort(key=lambda item: (_to_decimal(item.get("trend_pct") or 0), -_to_decimal(item["margin_pct"])))
    avg_cost_pct = _decimal_avg(cost_pct_values).quantize(Q2) if cost_pct_values else None
    signal_counts = {"success": 0, "warning": 0, "danger": 0, "critical": 0}
    for row in material_rows:
        tone = str(row.get("cost_signal_tone") or "")
        if tone in signal_counts:
            signal_counts[tone] += 1

    return {
        "lookback_days": lookback_days,
        "latest_week": latest_week,
        "rows": material_rows[:18],
        "promo_candidates": promo_candidates[:4],
        "avg_cost_pct": avg_cost_pct,
        "cost_signal_counts": signal_counts,
        "basis_note": (
            "Margen calculado solo con materia prima costada en la última semana disponible. "
            "Mano de obra e indirectos siguen fuera del modelo."
        ),
    }


def build_production_vs_sales_panel(*, latest_date: date | None = None, lookback_weeks: int = 4) -> dict[str, object]:
    latest_date = latest_date or _common_flow_cutoff_date() or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    current_week_start = _week_start(latest_date)
    current_week_end = _week_end(latest_date)
    settings = load_point_bridge_settings()
    production_allowed = {_normalize_text(value) for value in settings.production_storage_branches if value}
    dataset = get_dashboard_production_dataset(latest_date=latest_date, lookback_weeks=lookback_weeks)
    sales_by_category: dict[str, Decimal] = defaultdict(lambda: ZERO)
    production_by_category: dict[str, Decimal] = defaultdict(lambda: ZERO)
    weekly_rows: list[dict[str, object]] = []
    for row in list(dataset.get("weekly_rows") or []):
        week_start = row.get("week_start")
        if isinstance(week_start, str):
            week_start = date.fromisoformat(week_start)
        week_end = week_start + timedelta(days=6)
        sold_units = _to_decimal(row.get("sold_units"))
        produced_units = _to_decimal(row.get("produced_units"))
        weekly_rows.append(
            {
                "week_label": f"{week_start.strftime('%d %b')} → {week_end.strftime('%d %b')}",
                "sold_units": sold_units.quantize(Q2),
                "produced_units": produced_units.quantize(Q2),
                "delta_units": (produced_units - sold_units).quantize(Q2),
            }
        )

    for row in list(dataset.get("sales_category_rows") or []):
        label = _operational_category(
            category=str(row.get("category") or ""),
            family=str(row.get("family") or ""),
            item_name=str(row.get("item_name") or ""),
        )
        sales_by_category[label] += _to_decimal(row.get("units"))

    for row in list(dataset.get("production_category_rows") or []):
        central_branch = _normalize_text(str(row.get("central_branch") or ""))
        if central_branch not in production_allowed:
            continue
        label = _operational_category(
            category=str(row.get("category") or ""),
            family=str(row.get("family") or ""),
            item_name=str(row.get("item_name") or ""),
        )
        production_by_category[label] += _to_decimal(row.get("units"))

    category_rows = []
    labels = sorted(set(sales_by_category.keys()) | set(production_by_category.keys()))
    for label in labels:
        produced = production_by_category.get(label, ZERO)
        sold = sales_by_category.get(label, ZERO)
        delta = produced - sold
        if delta >= Decimal("20"):
            status = "Sobreproducción"
            tone = "warning"
        elif delta <= Decimal("-20"):
            status = "Déficit"
            tone = "danger"
        else:
            status = "Balanceado"
            tone = "success"
        category_rows.append(
            {
                "label": label,
                "produced_units": produced.quantize(Q2),
                "sold_units": sold.quantize(Q2),
                "delta_units": delta.quantize(Q2),
                "status": status,
                "tone": tone,
            }
        )
    category_rows.sort(key=lambda row: (-abs(_to_decimal(row["delta_units"])), row["label"]))
    return {
        "week_label": f"{current_week_start.strftime('%d %b')} → {current_week_end.strftime('%d %b')}",
        "cutoff_date": latest_date,
        "weekly_rows": weekly_rows,
        "category_rows": category_rows[:12],
        "basis_note": (
            "Compara producción central directa Point contra venta Point de la semana. "
            "Antes de la separación operativa de CEDIS, la fuente central histórica se toma desde MATRIZ; "
            "después, desde CEDIS. Transferencias internas se analizan aparte."
        ),
    }


def build_central_flow_panel(*, latest_date: date | None = None, months: int = 6) -> dict[str, object]:
    latest_date = latest_date or _common_flow_cutoff_date() or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    return get_or_set_versioned_cache(
        key_parts=(_dashboard_cache_key(panel="central-flow", latest_date=latest_date, months=months),),
        scopes=("dashboard",),
        timeout=DASHBOARD_CACHE_TTL_SECONDS,
        builder=lambda: _build_central_flow_panel_materialized(latest_date=latest_date, months=months),
    )


def build_monthly_inventory_ledger_panel(*, latest_date: date | None = None, months: int = 6) -> dict[str, object]:
    latest_date = latest_date or _common_flow_cutoff_date() or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    return get_or_set_versioned_cache(
        key_parts=(_dashboard_cache_key(panel="inventory-ledger", latest_date=latest_date, months=months),),
        scopes=("dashboard",),
        timeout=DASHBOARD_CACHE_TTL_SECONDS,
        builder=lambda: _build_monthly_inventory_ledger_panel_materialized(latest_date=latest_date, months=months),
    )


def _build_monthly_inventory_ledger_panel_materialized(*, latest_date: date, months: int) -> dict[str, object]:
    snapshot_payload = _inventory_ledger_panel_from_snapshot(latest_date=latest_date, months=months)
    if snapshot_payload is not None:
        return snapshot_payload
    payload = _build_monthly_inventory_ledger_panel_uncached(latest_date=latest_date, months=months)
    _persist_inventory_ledger_snapshot_rows(
        rows=list(payload.get("rows") or []),
        cutoff_date=latest_date,
        flow_coverage_start=payload.get("flow_coverage_start"),
        snapshot_coverage_start=payload.get("snapshot_coverage_start"),
    )
    return payload


def _build_central_flow_panel_materialized(*, latest_date: date, months: int) -> dict[str, object]:
    snapshot_payload = _central_flow_panel_from_snapshot(latest_date=latest_date, months=months)
    if snapshot_payload is not None:
        return snapshot_payload
    payload = _build_central_flow_panel_uncached(latest_date=latest_date, months=months)
    _persist_central_flow_snapshot_rows(rows=list(payload.get("rows") or []), cutoff_date=latest_date)
    return payload


def _build_monthly_inventory_ledger_panel_uncached(*, latest_date: date, months: int) -> dict[str, object]:
    production_min = PointProductionLine.objects.aggregate(v=Min("production_date")).get("v")
    transfer_min_raw = PointTransferLine.objects.aggregate(v=Min("received_at")).get("v")
    sales_min = PointDailySale.objects.aggregate(v=Min("sale_date")).get("v")
    waste_min_raw = PointWasteLine.objects.aggregate(v=Min("movement_at")).get("v")
    transfer_min = (
        timezone.localtime(transfer_min_raw).date()
        if transfer_min_raw is not None and timezone.is_aware(transfer_min_raw)
        else transfer_min_raw.date() if transfer_min_raw is not None else None
    )
    waste_min = (
        timezone.localtime(waste_min_raw).date()
        if waste_min_raw is not None and timezone.is_aware(waste_min_raw)
        else waste_min_raw.date() if waste_min_raw is not None else None
    )
    flow_coverage_start = min([value for value in [production_min, transfer_min, sales_min, waste_min] if value], default=None)
    first_snapshot_at = PointInventorySnapshot.objects.aggregate(v=Min("captured_at")).get("v")
    snapshot_coverage_start = None
    if first_snapshot_at is not None:
        snapshot_local = timezone.localtime(first_snapshot_at) if timezone.is_aware(first_snapshot_at) else first_snapshot_at
        snapshot_coverage_start = _month_start(snapshot_local.date())

    month_windows = _month_windows(latest_date, months)
    earliest_requested_month = month_windows[0][0] if month_windows else _month_start(latest_date)
    effective_start_month = max(earliest_requested_month, snapshot_coverage_start) if snapshot_coverage_start else None
    visible_month_windows = [
        (month_start, month_end, is_partial)
        for month_start, month_end, is_partial in month_windows
        if not effective_start_month or month_start >= effective_start_month
    ]
    range_start = visible_month_windows[0][0] if visible_month_windows else latest_date
    range_start_dt, range_end_dt = _aware_range(range_start, latest_date)

    snapshot_rows_by_month: dict[str, dict[str, object]] = {}
    snapshot_values = (
        PointInventorySnapshot.objects.filter(
            captured_at__gte=range_start_dt,
            captured_at__lt=range_end_dt,
        )
        .order_by("captured_at", "id")
        .values_list("captured_at__date", "stock", "sync_job_id", "branch__normalized_name", "branch__erp_branch__activa")
        .iterator(chunk_size=2000)
    )
    for captured_day, stock, sync_job_id, branch_normalized_name, erp_active in snapshot_values:
        month_label = captured_day.strftime("%Y-%m")
        if not _is_network_inventory_branch_normalized(
            normalized_point_name=branch_normalized_name or "",
            erp_active=erp_active,
        ):
            continue
        bucket = snapshot_rows_by_month.setdefault(
            month_label,
            {
                "opening_job_id": sync_job_id,
                "closing_job_id": sync_job_id,
                "opening_units": ZERO,
                "closing_units": ZERO,
            },
        )
        stock_decimal = _to_decimal(stock)
        if bucket["opening_job_id"] == sync_job_id:
            bucket["opening_units"] += stock_decimal
        if bucket["closing_job_id"] != sync_job_id:
            bucket["closing_job_id"] = sync_job_id
            bucket["closing_units"] = stock_decimal
        else:
            bucket["closing_units"] += stock_decimal

    production_by_month: dict[str, Decimal] = defaultdict(lambda: ZERO)
    production_values = (
        PointProductionLine.objects.filter(
            production_date__gte=range_start,
            production_date__lte=latest_date,
            is_insumo=False,
        )
        .values_list("production_date", "produced_quantity", "branch__normalized_name", "erp_branch__activa")
        .iterator(chunk_size=2000)
    )
    for production_date, produced_quantity, branch_normalized_name, erp_active in production_values:
        if _is_network_inventory_branch_normalized(
            normalized_point_name=branch_normalized_name or "",
            erp_active=erp_active,
        ):
            production_by_month[production_date.strftime("%Y-%m")] += _to_decimal(produced_quantity)

    sales_by_month = {
        f"{year:04d}-{month:02d}": _to_decimal(total_quantity)
        for year, month, total_quantity in _active_sales_queryset(start_date=range_start, end_date=latest_date)
        .values("sale_date__year", "sale_date__month")
        .annotate(total_quantity=Sum("quantity"))
        .values_list("sale_date__year", "sale_date__month", "total_quantity")
    }

    waste_by_month: dict[str, Decimal] = defaultdict(lambda: ZERO)
    waste_values = (
        PointWasteLine.objects.filter(
            movement_at__gte=range_start_dt,
            movement_at__lt=range_end_dt,
            receta_id__isnull=False,
        )
        .values_list("movement_at__date", "quantity", "branch__normalized_name", "erp_branch__activa")
        .iterator(chunk_size=2000)
    )
    for movement_day, quantity, branch_normalized_name, erp_active in waste_values:
        if _is_network_inventory_branch_normalized(
            normalized_point_name=branch_normalized_name or "",
            erp_active=erp_active,
        ):
            waste_by_month[movement_day.strftime("%Y-%m")] += _to_decimal(quantity)

    rows: list[dict[str, object]] = []
    omitted_months: list[str] = []
    for month_anchor, month_end, is_partial in month_windows:
        if effective_start_month and month_anchor < effective_start_month:
            omitted_months.append(month_anchor.strftime("%Y-%m"))
            continue
        month_start = month_anchor
        month_label = month_start.strftime("%Y-%m")
        snapshot_bucket = snapshot_rows_by_month.get(month_label)
        if not snapshot_bucket:
            omitted_months.append(month_anchor.strftime("%Y-%m"))
            continue

        opening_job_id = snapshot_bucket["opening_job_id"]
        closing_job_id = snapshot_bucket["closing_job_id"]
        opening_units = snapshot_bucket["opening_units"]
        closing_units = snapshot_bucket["closing_units"]
        produced_units = production_by_month.get(month_label, ZERO)
        sold_units = sales_by_month.get(month_label, ZERO)
        waste_units = waste_by_month.get(month_label, ZERO)
        theoretical_closing = opening_units + produced_units - sold_units - waste_units
        variance_units = closing_units - theoretical_closing
        rows.append(
            {
                "month_label": month_label,
                "is_partial": is_partial,
                "opening_units": opening_units.quantize(Q2),
                "production_units": produced_units.quantize(Q2),
                "sold_units": sold_units.quantize(Q2),
                "waste_units": waste_units.quantize(Q2),
                "theoretical_closing": theoretical_closing.quantize(Q2),
                "actual_closing": closing_units.quantize(Q2),
                "variance_units": variance_units.quantize(Q2),
                "opening_job_id": opening_job_id,
                "closing_job_id": closing_job_id,
            }
        )

    for index, row in enumerate(rows[:-1]):
        next_row = rows[index + 1]
        row["next_opening_units"] = next_row["opening_units"]
        row["rollover_gap"] = (_to_decimal(next_row["opening_units"]) - _to_decimal(row["actual_closing"])).quantize(Q2)
    if rows:
        rows[-1]["next_opening_units"] = None
        rows[-1]["rollover_gap"] = None

    latest_row = rows[-1] if rows else None
    omitted_months = list(dict.fromkeys(omitted_months))
    if rows:
        basis_note = (
            "Puente mensual de inventario de red: inventario inicial + producción - venta - merma = cierre teórico. "
            "Las transferencias internas no se suman porque no cambian el inventario total de la red."
        )
        if snapshot_coverage_start:
            basis_note += f" Cobertura real de snapshots desde {snapshot_coverage_start.strftime('%Y-%m')}."
    else:
        basis_note = "Aún no existe cobertura suficiente de snapshots de inventario para construir un puente mensual confiable."

    return {
        "cutoff_date": latest_date,
        "rows": rows,
        "latest_row": latest_row,
        "basis_note": basis_note,
        "flow_coverage_start": flow_coverage_start,
        "snapshot_coverage_start": snapshot_coverage_start,
        "omitted_months": omitted_months,
        "visible_months_count": len(rows),
    }


def _build_central_flow_panel_uncached(*, latest_date: date, months: int) -> dict[str, object]:
    first_cedis_date = _first_central_cedis_production_date()
    partial_cache = _official_partial_sales_cache()
    production_min = PointProductionLine.objects.aggregate(v=Min("production_date")).get("v")
    transfer_min_raw = PointTransferLine.objects.aggregate(v=Min("received_at")).get("v")
    sales_min = PointDailySale.objects.aggregate(v=Min("sale_date")).get("v")
    waste_min_raw = PointWasteLine.objects.aggregate(v=Min("movement_at")).get("v")
    transfer_min = (
        timezone.localtime(transfer_min_raw).date()
        if transfer_min_raw is not None and timezone.is_aware(transfer_min_raw)
        else transfer_min_raw.date() if transfer_min_raw is not None else None
    )
    waste_min = (
        timezone.localtime(waste_min_raw).date()
        if waste_min_raw is not None and timezone.is_aware(waste_min_raw)
        else waste_min_raw.date() if waste_min_raw is not None else None
    )
    flow_coverage_start = min([value for value in [production_min, transfer_min, sales_min, waste_min] if value], default=None)

    snapshot_month_map: dict[str, dict[str, Decimal | date | None]] = {}
    for row in build_monthly_inventory_ledger_panel(latest_date=latest_date, months=months).get("rows", []):
        snapshot_month_map[str(row["month_label"])] = {
            "actual_closing": row["actual_closing"],
            "variance_units": row["variance_units"],
            "is_partial": row["is_partial"],
        }

    month_windows = _month_windows(latest_date, months)
    range_start = month_windows[0][0] if month_windows else latest_date
    range_start_dt, range_end_dt = _aware_range(range_start, latest_date)
    production_by_month: dict[str, Decimal] = defaultdict(lambda: ZERO)
    production_values = (
        PointProductionLine.objects.filter(
            production_date__gte=range_start,
            production_date__lte=latest_date,
            is_insumo=False,
        )
        .values_list("production_date", "produced_quantity", "branch__normalized_name", "erp_branch__nombre")
        .iterator(chunk_size=2000)
    )
    for production_date, produced_quantity, branch_normalized_name, erp_branch_name in production_values:
        branch_label = _normalize_text(erp_branch_name or "") or (branch_normalized_name or "")
        central_branch = _central_production_branch_for_day(production_date, first_cedis_date=first_cedis_date)
        if branch_label == central_branch:
            production_by_month[production_date.strftime("%Y-%m")] += _to_decimal(produced_quantity)

    transfer_by_month: dict[str, Decimal] = defaultdict(lambda: ZERO)
    transfer_values = (
        PointTransferLine.objects.filter(
            is_insumo=False,
            destination_branch__name__iexact="CEDIS",
            received_at__isnull=False,
            received_at__gte=range_start_dt,
            received_at__lt=range_end_dt,
        )
        .values_list("received_at__date", "received_quantity")
        .iterator(chunk_size=2000)
    )
    for transfer_date, received_quantity in transfer_values:
        transfer_by_month[transfer_date.strftime("%Y-%m")] += _to_decimal(received_quantity)

    official_sales_by_month = {
        month_start.strftime("%Y-%m"): _to_decimal(total_quantity)
        for month_start, total_quantity in PointMonthlySalesOfficial.objects.filter(
            month_start__gte=range_start,
            month_start__lte=_month_start(latest_date),
        ).values_list("month_start", "total_quantity")
    }

    waste_by_month: dict[str, Decimal] = defaultdict(lambda: ZERO)
    waste_values = (
        PointWasteLine.objects.filter(
            movement_at__gte=range_start_dt,
            movement_at__lt=range_end_dt,
            receta_id__isnull=False,
        )
        .values_list("movement_at__date", "quantity", "branch__normalized_name", "erp_branch__activa")
        .iterator(chunk_size=2000)
    )
    for movement_date, quantity, branch_normalized_name, erp_active in waste_values:
        if _is_network_inventory_branch_normalized(
            normalized_point_name=branch_normalized_name or "",
            erp_active=erp_active,
        ):
            waste_by_month[movement_date.strftime("%Y-%m")] += _to_decimal(quantity)

    rows: list[dict[str, object]] = []
    for month_start, month_end, is_partial in month_windows:
        month_label = month_start.strftime("%Y-%m")
        production_units = production_by_month.get(month_label, ZERO)
        transfer_units = transfer_by_month.get(month_label, ZERO)
        current_month_cache = official_sales_by_month.get(month_label)
        current_is_full_month = month_end == _month_end(month_start)
        current_partial_payload = None
        if not current_is_full_month:
            current_partial_payload = partial_cache.get(f"{month_start.isoformat()}_{month_end.isoformat()}")

        if current_month_cache is not None and current_is_full_month:
            sold_units = _to_decimal(current_month_cache)
        elif current_partial_payload is not None:
            sold_units = _to_decimal(current_partial_payload.get("total_quantity"))
        else:
            sold_units = _to_decimal(
                _canonical_sales_range_summary(start_date=month_start, end_date=month_end).get("cantidad")
            )
        waste_units = waste_by_month.get(month_label, ZERO)

        supply_units = production_units + transfer_units
        net_units = supply_units - sold_units - waste_units
        snapshot_row = snapshot_month_map.get(month_label) or {}
        central_source = "CEDIS" if (first_cedis_date and month_end >= first_cedis_date) else "MATRIZ"
        rows.append(
            {
                "month_label": month_label,
                "is_partial": is_partial,
                "central_source": central_source,
                "production_units": production_units.quantize(Q2),
                "transfer_units": transfer_units.quantize(Q2),
                "supply_units": supply_units.quantize(Q2),
                "sold_units": sold_units.quantize(Q2),
                "waste_units": waste_units.quantize(Q2),
                "net_units": net_units.quantize(Q2),
                "actual_closing": snapshot_row.get("actual_closing"),
                "variance_units": snapshot_row.get("variance_units"),
                "has_snapshot": bool(snapshot_row),
            }
        )

    complete_months = sum(1 for row in rows if row.get("has_snapshot"))
    flow_only_months = sum(1 for row in rows if not row.get("has_snapshot"))

    return {
        "cutoff_date": latest_date,
        "rows": rows,
        "first_cedis_date": first_cedis_date,
        "flow_coverage_start": flow_coverage_start,
        "complete_months": complete_months,
        "flow_only_months": flow_only_months,
        "basis_note": (
            "Flujo central histórico calculado desde registros diarios disponibles: producción central "
            "(MATRIZ antes de CEDIS, CEDIS después), transferencias recibidas a CEDIS, venta Point y merma Point. "
            "Solo cuando existe snapshot real se muestra cierre de inventario."
        ),
    }


def build_budget_operating_panel(*, year: int | None = None, selected_month: int | None = None) -> dict[str, object]:
    from reportes.models import (
        EmpresaResultadoMensual,
        PresupuestoImport,
        PresupuestoLineaMensual,
        PresupuestoResumenMensual,
        ProductoPricingDecisionMensual,
    )

    target_year = int(year or timezone.localdate().year)
    today = timezone.localdate()
    month_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    ytd_cutoff_month = today.month if target_year == today.year else 12
    budget_excluded_concepts = {
        "INGRESOS",
        "EGRESOS",
        "COSTOS",
        "UTILIDAD BRUTA",
        "UTILIDAD O PERDIDA",
        "UTILIDAD O PÉRDIDA",
        "VENTA COMPLEMENTOS",
        "VENTA POSTRES",
        "PRODUCCIÓN",
        "TOTAL GASTOS",
        "TOTAL GASTOS VENTAS",
        "TOTAL POR MES",
        "TOTALES",
    }
    budget_non_recurrent_concepts = {
        "APERTURA SUCURSAL",
        "ADQUISICIÓN DE EQUIPO/MAQUINARIA",
        "ADQUISICION DE EQUIPO/MAQUINARIA",
    }
    budget_sales_concepts = {"INGRESOS"}
    budget_sales_fallback_concepts = {"VENTA POSTRES", "VENTA COMPLEMENTOS"}
    budget_product_cost_concepts = {
        "COSTOS INSUMOS/PRODUCTOS",
        "COSTOS COMPLEMENTOS",
        "MERMA",
        "COSTO DE PRODUCCION",
        "COSTO DE PRODUCCIÓN",
    }
    detail_kind_labels = {
        "admin_recurrente": "Administración recurrente",
        "branch_sales": "Comercial sucursales",
        "payroll_area": "Nómina por área",
        "production_budget": "Producción",
        "logistics_budget": "Logística",
    }
    budget_review_import_exists = PresupuestoLineaMensual.objects.filter(
        importacion__fuente_nombre="PRESUPUESTO ADMINISTRACIÓN 2026.xlsx",
        importacion__sheet_name="GENERAL",
        period__year=target_year,
    ).exists()
    trusted_detail_qs = (
        PresupuestoLineaMensual.objects.filter(
            importacion__tipo=PresupuestoImport.TIPO_DETALLE,
            period__year=target_year,
        )
        .select_related("importacion")
        .order_by("period", "importacion__fuente_nombre", "importacion__sheet_name", "row_index")
    )
    trusted_detail_lines = list(trusted_detail_qs)
    trusted_detail_exists = bool(trusted_detail_lines)
    admin_general_lines = list(
        PresupuestoLineaMensual.objects.filter(
            importacion__fuente_nombre="PRESUPUESTO ADMINISTRACIÓN 2026.xlsx",
            importacion__sheet_name="GENERAL",
            period__year=target_year,
        ).order_by("period", "row_index")
    )

    budget_rows = {
        row.period.month: row
        for row in PresupuestoResumenMensual.objects.filter(
            period__year=target_year,
            tipo=PresupuestoResumenMensual.TIPO_GLOBAL,
        )
    }
    finance_rows = {
        row.periodo.month: row
        for row in EmpresaResultadoMensual.objects.filter(periodo__year=target_year)
    }
    monthly_official_rows = {
        row.month_start.month: row
        for row in PointMonthlySalesOfficial.objects.filter(month_start__year=target_year)
    }

    latest_pricing_period = (
        ProductoPricingDecisionMensual.objects.filter(periodo__year=target_year)
        .order_by("-periodo")
        .values_list("periodo", flat=True)
        .first()
    )
    pricing_counts = defaultdict(int)
    if latest_pricing_period:
        for row in (
            ProductoPricingDecisionMensual.objects.filter(periodo=latest_pricing_period)
            .values("accion_sugerida")
            .annotate(total=Count("id"))
        ):
            pricing_counts[str(row["accion_sugerida"] or "")] = int(row["total"] or 0)

    action_labels = {
        "DEFENDER": "Defender",
        "PROMOVER": "Promover",
        "CORREGIR_COSTO": "Corregir costo",
        "SUBIR_PRECIO": "Subir precio",
        "GANCHO": "Gancho",
        "REFORMULAR": "Reformular",
    }
    pricing_action_rows = [
        {
            "code": code,
            "label": action_labels.get(code, code.title()),
            "count": pricing_counts.get(code, 0),
        }
        for code in ["SUBIR_PRECIO", "CORREGIR_COSTO", "PROMOVER", "DEFENDER", "GANCHO", "REFORMULAR"]
        if pricing_counts.get(code, 0) > 0
    ]

    def _kind_for_line(line: PresupuestoLineaMensual) -> str:
        line_meta = getattr(line, "metadata", {}) or {}
        import_meta = getattr(getattr(line, "importacion", None), "metadata", {}) or {}
        return str(line_meta.get("kind") or import_meta.get("kind") or "").strip()

    def _normalized_concept(concept: str) -> str:
        return _normalize_text(concept).upper()

    def _is_verified_operational_line(line: PresupuestoLineaMensual) -> bool:
        if getattr(line, "audit_status", PresupuestoLineaMensual.AUDIT_PENDING) != PresupuestoLineaMensual.AUDIT_OK:
            return False
        concept_upper = _normalized_concept(str(line.concept or ""))
        if not concept_upper:
            return False
        if concept_upper in budget_excluded_concepts:
            return False
        if concept_upper in budget_non_recurrent_concepts:
            return False
        if concept_upper.startswith("TOTAL "):
            return False
        return True

    general_monthly_budget_by_concept: dict[str, dict[int, Decimal]] = defaultdict(lambda: defaultdict(lambda: ZERO))
    for line in admin_general_lines:
        general_monthly_budget_by_concept[_normalized_concept(str(line.concept or ""))][line.period.month] += _to_decimal(
            line.monthly_budget
        )

    def _general_budget_total(month: int, *concepts: str) -> Decimal:
        return sum(
            (
                general_monthly_budget_by_concept[_normalized_concept(concept)].get(month, ZERO)
                for concept in concepts
            ),
            ZERO,
        )

    source_groups_map: dict[str, dict[str, object]] = {}
    source_group_months: dict[str, dict[int, tuple[Decimal, Decimal]]] = defaultdict(lambda: defaultdict(lambda: (ZERO, ZERO)))
    monthly_operating_budget: dict[int, Decimal] = defaultdict(lambda: ZERO)
    monthly_operating_actual: dict[int, Decimal] = defaultdict(lambda: ZERO)
    monthly_recurrent_budget: dict[int, Decimal] = defaultdict(lambda: ZERO)
    monthly_recurrent_actual: dict[int, Decimal] = defaultdict(lambda: ZERO)
    branch_monthly_budget: dict[int, Decimal] = defaultdict(lambda: ZERO)
    branch_monthly_actual: dict[int, Decimal] = defaultdict(lambda: ZERO)
    branch_sheet_names: set[str] = set()
    for line in trusted_detail_lines:
        if not _is_verified_operational_line(line):
            continue
        concept_upper = _normalized_concept(str(line.concept or ""))
        kind = _kind_for_line(line)
        label = detail_kind_labels.get(kind, kind or "Otra fuente")
        order = list(detail_kind_labels).index(kind) if kind in detail_kind_labels else 50
        month = line.period.month
        budget_value = _to_decimal(line.monthly_budget)
        actual_value = _to_decimal(line.monthly_actual)
        monthly_operating_budget[month] += budget_value
        monthly_operating_actual[month] += actual_value
        if concept_upper not in budget_product_cost_concepts and concept_upper not in budget_sales_fallback_concepts and concept_upper not in budget_sales_concepts:
            monthly_recurrent_budget[month] += budget_value
            monthly_recurrent_actual[month] += actual_value
        payload = source_groups_map.setdefault(
            kind or label,
            {
                "code": kind or "other",
                "label": label,
                "source_name": kind or label,
                "order": order,
                "ytd_budget": ZERO,
                "ytd_actual": ZERO,
                "latest_budget": ZERO,
                "latest_actual": ZERO,
                "latest_month_label": "Sin dato",
            },
        )
        if month <= ytd_cutoff_month:
            payload["ytd_budget"] += budget_value
            payload["ytd_actual"] += actual_value
        current_budget, current_actual = source_group_months[kind or label][month]
        source_group_months[kind or label][month] = (current_budget + budget_value, current_actual + actual_value)
        if kind == "branch_sales":
            branch_monthly_budget[month] += budget_value
            branch_monthly_actual[month] += actual_value
            payload_sheet = getattr(line.importacion, "sheet_name", "") or ""
            if payload_sheet:
                branch_sheet_names.add(payload_sheet)

    source_groups = []
    for source_key, payload in source_groups_map.items():
        month_map = source_group_months.get(source_key, {})
        if month_map:
            latest_month = max(month_map.keys())
            payload["latest_budget"], payload["latest_actual"] = month_map[latest_month]
            payload["latest_month_label"] = month_labels[latest_month - 1]
        payload["variance_pct"] = _safe_pct(payload["ytd_actual"] - payload["ytd_budget"], payload["ytd_budget"])
        source_groups.append(payload)
    source_groups.sort(key=lambda item: (item["order"], item["label"]))

    rows: list[dict[str, object]] = []
    ytd_budget = ZERO
    ytd_actual = ZERO
    ytd_budget_master = ZERO
    ytd_actual_master = ZERO
    ytd_sales_budget = ZERO
    ytd_product_cost_budget = ZERO
    ytd_recurrent_expense_budget = ZERO
    ytd_budget_result = ZERO
    ytd_actual_expense = ZERO
    ytd_sales = ZERO
    ytd_costed_sales = ZERO
    ytd_non_recipe = ZERO
    ytd_manufacturing = ZERO
    ytd_commercial = ZERO
    ytd_corporate = ZERO
    ytd_profit = ZERO
    months_with_budget = 0
    months_with_finance = 0

    operating_budget_map: dict[int, tuple[Decimal, Decimal]] = {
        month: (
            monthly_operating_budget.get(month, ZERO),
            monthly_operating_actual.get(month, ZERO),
        )
        for month in range(1, 13)
    }

    for month in range(1, 13):
        period = date(target_year, month, 1)
        budget = budget_rows.get(month)
        finance = finance_rows.get(month)
        budget_master_total = _to_decimal(getattr(budget, "total_budget", ZERO))
        actual_master_total = _to_decimal(getattr(budget, "total_actual", ZERO))
        budget_total, actual_total = operating_budget_map.get(month, (ZERO, ZERO))
        variance_pct = _safe_pct(actual_total - budget_total, budget_total)
        sales_budget_total = _general_budget_total(month, *budget_sales_concepts)
        if sales_budget_total == 0:
            sales_budget_total = _general_budget_total(month, *budget_sales_fallback_concepts)
        product_cost_budget_total = _general_budget_total(month, *budget_product_cost_concepts)
        recurrent_expense_budget_total = monthly_recurrent_budget.get(month, ZERO)
        budget_result_total = sales_budget_total - product_cost_budget_total - recurrent_expense_budget_total
        monthly_official = monthly_official_rows.get(month)
        sales_total = _to_decimal(getattr(finance, "venta_total", ZERO))
        if monthly_official is not None:
            sales_total = _to_decimal(monthly_official.total_amount)
        manufacturing_total = _to_decimal(getattr(finance, "costo_fabricacion_total", ZERO))
        commercial_total = _to_decimal(getattr(finance, "gasto_comercial_total", ZERO))
        corporate_total = _to_decimal(getattr(finance, "gasto_corporativo_total", ZERO))
        operating_expense_total = commercial_total + corporate_total
        operating_profit_total = _to_decimal(getattr(finance, "utilidad_operativa_total", ZERO))
        finance_meta = getattr(finance, "metadata", {}) or {}
        costed_sales_total = _to_decimal(finance_meta.get("venta_costeada_total", ZERO))
        non_recipe_total = _to_decimal(finance_meta.get("venta_no_receta_total", ZERO))
        costed_coverage_pct = _safe_pct(costed_sales_total, sales_total)
        finance_close_complete = (
            finance is not None
            and sales_total > 0
            and costed_sales_total > 0
            and operating_expense_total > 0
        )
        gross_margin_total = sales_total - manufacturing_total
        gross_margin_pct = _safe_pct(gross_margin_total, sales_total)

        if budget is not None:
            if month <= ytd_cutoff_month:
                ytd_budget_master += budget_master_total
                ytd_actual_master += actual_master_total
        if budget_total > 0 or actual_total > 0:
            months_with_budget += 1
            if month <= ytd_cutoff_month:
                ytd_budget += budget_total
                ytd_actual += actual_total
                ytd_sales_budget += sales_budget_total
                ytd_product_cost_budget += product_cost_budget_total
                ytd_recurrent_expense_budget += recurrent_expense_budget_total
                ytd_budget_result += budget_result_total
        if finance is not None:
            months_with_finance += 1
            if month <= ytd_cutoff_month:
                ytd_sales += sales_total
                ytd_costed_sales += costed_sales_total
                ytd_non_recipe += non_recipe_total
                ytd_manufacturing += manufacturing_total
                ytd_commercial += commercial_total
                ytd_corporate += corporate_total
                ytd_actual_expense += operating_expense_total
                ytd_profit += operating_profit_total

        budget_attainment_pct = _safe_pct(actual_total, budget_total)
        sales_budget_attainment_pct = _safe_pct(sales_total, sales_budget_total)
        sales_gap_total = sales_total - sales_budget_total
        sales_gap_pct = _safe_pct(sales_gap_total, sales_budget_total)
        budget_result_variance_pct = _safe_pct(operating_profit_total - budget_result_total, budget_result_total)
        product_cost_budget_mix_pct = _safe_pct(product_cost_budget_total, sales_budget_total)
        recurrent_expense_budget_mix_pct = _safe_pct(recurrent_expense_budget_total, sales_budget_total)
        budget_result_margin_pct = _safe_pct(budget_result_total, sales_budget_total)
        actual_product_cost_mix_pct = _safe_pct(manufacturing_total, sales_total)
        actual_expense_mix_pct = _safe_pct(operating_expense_total, sales_total)
        operating_margin_pct = _safe_pct(operating_profit_total, sales_total)
        sales_signal = _attainment_signal(sales_budget_attainment_pct)
        product_cost_signal = (
            _cost_ratio_signal(actual_product_cost_mix_pct)
            if finance is not None and sales_total > 0 and manufacturing_total > 0
            else _pending_signal("Sin lectura", "Todavía no hay costo real suficiente para comparar contra la venta.")
        )
        expense_signal = (
            _expense_ratio_signal(actual_expense_mix_pct)
            if finance_close_complete
            else _pending_signal("Pendiente", "Falta gasto recurrente real completo para cerrar el mes.")
        )
        margin_signal = (
            _margin_ratio_signal(operating_margin_pct)
            if finance_close_complete
            else _pending_signal("Pendiente", "El margen real queda pendiente hasta cargar el gasto completo del mes.")
        )
        gross_margin_signal = (
            _gross_margin_signal(gross_margin_pct)
            if finance is not None and sales_total > 0 and manufacturing_total > 0
            else _pending_signal("Sin lectura", "Todavía no hay costo real suficiente para calcular margen bruto real.")
        )
        health_signal = (
            _combined_health_signal(product_cost_signal, expense_signal, margin_signal)
            if finance_close_complete
            else _pending_signal("Lectura parcial", "Hay venta real, pero el mes todavía no tiene cierre financiero completo.")
        )
        rows.append(
            {
                "period": period,
                "month_label": month_labels[month - 1],
                "budget_master_total": budget_master_total,
                "actual_master_total": actual_master_total,
                "budget_total": budget_total,
                "actual_total": actual_total,
                "variance_pct": variance_pct,
                "sales_budget_total": sales_budget_total,
                "sales_total": sales_total,
                "product_cost_budget_total": product_cost_budget_total,
                "costed_sales_total": costed_sales_total,
                "recurrent_expense_budget_total": recurrent_expense_budget_total,
                "operating_expense_total": operating_expense_total,
                "budget_result_total": budget_result_total,
                "non_recipe_total": non_recipe_total,
                "costed_coverage_pct": costed_coverage_pct,
                "finance_close_complete": finance_close_complete,
                "manufacturing_total": manufacturing_total,
                "gross_margin_total": gross_margin_total,
                "gross_margin_pct": gross_margin_pct,
                "commercial_total": commercial_total,
                "corporate_total": corporate_total,
                "operating_profit_total": operating_profit_total,
                "budget_attainment_pct": budget_attainment_pct,
                "sales_budget_attainment_pct": sales_budget_attainment_pct,
                "sales_gap_total": sales_gap_total,
                "sales_gap_pct": sales_gap_pct,
                "sales_signal": sales_signal,
                "budget_result_variance_pct": budget_result_variance_pct,
                "product_cost_budget_mix_pct": product_cost_budget_mix_pct,
                "recurrent_expense_budget_mix_pct": recurrent_expense_budget_mix_pct,
                "budget_result_margin_pct": budget_result_margin_pct,
                "actual_product_cost_mix_pct": actual_product_cost_mix_pct,
                "actual_expense_mix_pct": actual_expense_mix_pct,
                "operating_margin_pct": operating_margin_pct,
                "product_cost_signal": product_cost_signal,
                "gross_margin_signal": gross_margin_signal,
                "expense_signal": expense_signal,
                "margin_signal": margin_signal,
                "health_signal": health_signal,
                "has_budget": budget_total > 0 or actual_total > 0 or sales_budget_total > 0,
                "has_finance": finance is not None,
            }
        )

    latest_finance_month = max(finance_rows.keys(), default=None)
    latest_budget_month = max(budget_rows.keys(), default=None)
    rows_operated = [row for row in rows if row["has_finance"]]
    rows_budget_only = [row for row in rows if row["has_budget"] and not row["has_finance"]]
    rows_summary = [row for row in rows if row["has_budget"] or row["has_finance"]]
    selectable_months = [row["period"].month for row in rows_summary]
    default_selected_month = latest_finance_month or latest_budget_month or (rows_summary[-1]["period"].month if rows_summary else 1)
    selected_month = int(selected_month or default_selected_month)
    if selectable_months and selected_month not in selectable_months:
        selected_month = default_selected_month
    selected_row = next((row for row in rows_summary if row["period"].month == selected_month), rows_summary[-1] if rows_summary else None)
    month_options = [
        {
            "value": row["period"].month,
            "label": row["month_label"],
            "has_budget": row["has_budget"],
            "has_finance": row["has_finance"],
            "finance_close_complete": row["finance_close_complete"],
        }
        for row in rows_summary
    ]
    coverage_note = "Presupuesto visible desde detalle verificado en base: administración recurrente, sucursales, nómina por área, producción y logística. La hoja GENERAL queda solo para conciliación."
    if months_with_finance == 0:
        coverage_note = "Todavía no hay snapshots financieros mensuales para el año visible."
    budget_confidence_status = "verified_detail" if trusted_detail_exists else "under_review"
    budget_confidence_label = "Presupuesto verificado en base" if trusted_detail_exists else "Presupuesto en revisión"
    budget_confidence_note = (
        "El KPI principal usa detalle confiable cargado en base. La hoja GENERAL de administración queda como hoja de control y auditoría."
        if trusted_detail_exists
        else "La hoja GENERAL de administración mezcla ingresos, costos, utilidad y egresos. El BI principal usa operación real; el presupuesto queda en auditoría hasta reconstruirlo desde hojas detalladas."
    )
    global_mode = ""
    if budget_rows:
        latest_budget_row = next(iter(sorted(budget_rows.items(), reverse=True)))[1]
        global_mode = str((latest_budget_row.metadata or {}).get("global_mode") or "")

    audit_map = {
        "Imss": "IMSS",
        "Infonavit-RCV": "INFONAVIT",
        "Aguinaldo": "AGUINALDO",
        "Utilidades": "UTILIDADES",
        "Playera": "PLAYERAS",
        "Mandil": "MANDIL",
        "Camisa mujer": "CAMISA MUJER",
        "Camisa hombre": "CAMISA HOMBRE",
        "Gorra": "GORRA",
    }
    detail_lookup: dict[tuple[int, str], Decimal] = defaultdict(lambda: ZERO)
    alias_map = { _normalize_text(k): _normalize_text(v) for k, v in audit_map.items() }
    for line in trusted_detail_lines:
        if _kind_for_line(line) != "payroll_area":
            continue
        normalized = _normalize_text(str(line.concept or ""))
        detail_lookup[(line.period.month, normalized)] += _to_decimal(line.monthly_budget)
    audit_exceptions = []
    for concept, detail_concept in audit_map.items():
        monthly_rows = [line for line in admin_general_lines if line.concept == concept]
        if not monthly_rows:
            continue
        total_gap = ZERO
        months_affected = []
        sample_current = ZERO
        sample_expected = ZERO
        for line in monthly_rows:
            expected = detail_lookup[(line.period.month, _normalize_text(detail_concept))]
            current = _to_decimal(line.monthly_budget)
            diff = expected - current
            total_gap += diff
            if diff != 0:
                months_affected.append(month_labels[line.period.month - 1])
                if sample_current == ZERO and sample_expected == ZERO:
                    sample_current = current
                    sample_expected = expected
        if total_gap != 0:
            audit_exceptions.append(
                {
                    "concept": concept,
                    "detail_concept": detail_concept,
                    "months": months_affected,
                    "sample_current": sample_current,
                    "sample_expected": sample_expected,
                    "ytd_gap": total_gap,
                }
            )
    audit_exceptions.sort(key=lambda item: abs(_to_decimal(item["ytd_gap"])), reverse=True)

    branch_budget_panel = {
        "available": bool(branch_sheet_names),
        "title": "Presupuesto por sucursal",
        "note": (
            f"Detalle por sucursal cargado en base: {len(branch_sheet_names)} hojas. YTD ${sum((branch_monthly_budget.get(m, ZERO) for m in range(1, ytd_cutoff_month + 1)), ZERO):,.2f}."
            if branch_sheet_names
            else "Aún no está importado el detalle por sucursal del presupuesto."
        ),
    }

    return {
        "year": target_year,
        "rows": rows,
        "rows_operated": rows_operated,
        "rows_budget_only": rows_budget_only,
        "rows_summary": rows_summary,
        "selected_month": selected_month,
        "selected_row": selected_row,
        "month_options": month_options,
        "months_with_budget": months_with_budget,
        "months_with_finance": months_with_finance,
        "ytd_cutoff_label": month_labels[ytd_cutoff_month - 1],
        "latest_finance_label": month_labels[latest_finance_month - 1] if latest_finance_month else "Sin snapshot",
        "latest_budget_label": month_labels[latest_budget_month - 1] if latest_budget_month else "Sin presupuesto",
        "ytd_budget_master": ytd_budget_master,
        "ytd_actual_master": ytd_actual_master,
        "ytd_budget": ytd_budget,
        "ytd_actual": ytd_actual,
        "ytd_sales_budget": ytd_sales_budget,
        "ytd_product_cost_budget": ytd_product_cost_budget,
        "ytd_recurrent_expense_budget": ytd_recurrent_expense_budget,
        "ytd_budget_result": ytd_budget_result,
        "ytd_actual_expense": ytd_actual_expense,
        "ytd_structured_budget_total": ytd_product_cost_budget + ytd_recurrent_expense_budget,
        "ytd_sales": ytd_sales,
        "ytd_costed_sales": ytd_costed_sales,
        "ytd_non_recipe": ytd_non_recipe,
        "ytd_manufacturing": ytd_manufacturing,
        "ytd_commercial": ytd_commercial,
        "ytd_corporate": ytd_corporate,
        "ytd_operating_profit": ytd_profit,
        "ytd_budget_master_variance_pct": _safe_pct(ytd_actual_master - ytd_budget_master, ytd_budget_master),
        "ytd_budget_variance_pct": _safe_pct(ytd_actual - ytd_budget, ytd_budget),
        "ytd_sales_budget_variance_pct": _safe_pct(ytd_sales - ytd_sales_budget, ytd_sales_budget),
        "ytd_budget_result_variance_pct": _safe_pct(ytd_profit - ytd_budget_result, ytd_budget_result),
        "ytd_product_cost_budget_mix_pct": _safe_pct(ytd_product_cost_budget, ytd_sales_budget),
        "ytd_recurrent_expense_budget_mix_pct": _safe_pct(ytd_recurrent_expense_budget, ytd_sales_budget),
        "ytd_budget_result_margin_pct": _safe_pct(ytd_budget_result, ytd_sales_budget),
        "ytd_actual_product_cost_mix_pct": _safe_pct(ytd_manufacturing, ytd_sales),
        "ytd_actual_expense_mix_pct": _safe_pct(ytd_actual_expense, ytd_sales),
        "ytd_actual_result_margin_pct": _safe_pct(ytd_profit, ytd_sales),
        "ytd_product_cost_signal": _cost_ratio_signal(_safe_pct(ytd_manufacturing, ytd_sales)),
        "ytd_expense_signal": _expense_ratio_signal(_safe_pct(ytd_actual_expense, ytd_sales)),
        "ytd_margin_signal": _margin_ratio_signal(_safe_pct(ytd_profit, ytd_sales)),
        "ytd_health_signal": _combined_health_signal(
            _cost_ratio_signal(_safe_pct(ytd_manufacturing, ytd_sales)),
            _expense_ratio_signal(_safe_pct(ytd_actual_expense, ytd_sales)),
            _margin_ratio_signal(_safe_pct(ytd_profit, ytd_sales)),
        ),
        "ytd_costed_coverage_pct": _safe_pct(ytd_costed_sales, ytd_sales),
        "coverage_note": coverage_note,
        "budget_confidence_status": budget_confidence_status,
        "budget_confidence_label": budget_confidence_label,
        "budget_confidence_note": budget_confidence_note,
        "show_budget_primary": trusted_detail_exists,
        "global_mode": global_mode,
        "source_groups": source_groups,
        "branch_budget_panel": branch_budget_panel,
        "pricing_action_rows": pricing_action_rows,
        "pricing_period_label": latest_pricing_period.strftime("%b %Y") if latest_pricing_period else "Sin pricing mensual",
        "audit_exceptions": audit_exceptions[:8],
    }


def build_branch_contribution_panel(*, year: int | None = None) -> dict[str, object]:
    from reportes.models import ProductoSucursalContribucionMensual

    target_year = int(year or timezone.localdate().year)
    today = timezone.localdate()
    ytd_cutoff_month = today.month if target_year == today.year else 12
    month_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    months_in_scope = Decimal(str(ytd_cutoff_month))

    contrib_qs = ProductoSucursalContribucionMensual.objects.filter(
        periodo__year=target_year,
        periodo__month__lte=ytd_cutoff_month,
    ).select_related("receta", "sucursal")

    latest_period = contrib_qs.order_by("-periodo").values_list("periodo", flat=True).first()

    branch_rows: dict[int, dict[str, object]] = {}
    for row in contrib_qs.iterator():
        branch_id = int(row.sucursal_id)
        payload = branch_rows.setdefault(
            branch_id,
            {
                "branch_id": branch_id,
                "branch_label": row.sucursal.nombre,
                "branch_code": row.sucursal.codigo,
                "sales_total": ZERO,
                "cost_total": ZERO,
                "commercial_total": ZERO,
                "contribution_total": ZERO,
                "units_total": ZERO,
                "latest_month_sales_total": ZERO,
                "latest_month_contribution_total": ZERO,
                "fabricated_sales_total": ZERO,
                "resale_sales_total": ZERO,
                "non_recipe_total": ZERO,
                "non_recipe_resale_total": ZERO,
                "non_recipe_accessory_total": ZERO,
                "non_recipe_service_total": ZERO,
                "top_support": [],
                "top_drag": [],
            },
        )
        sale_amount = _to_decimal(row.venta_total)
        contribution_amount = _to_decimal(row.contribucion_total)
        payload["sales_total"] += sale_amount
        payload["cost_total"] += _to_decimal(row.costo_producto_total)
        payload["commercial_total"] += _to_decimal(row.gasto_comercial_total)
        payload["contribution_total"] += contribution_amount
        payload["units_total"] += _to_decimal(row.unidades_vendidas)
        if latest_period and row.periodo == latest_period:
            payload["latest_month_sales_total"] += sale_amount
            payload["latest_month_contribution_total"] += contribution_amount
        if row.receta.modo_costeo == "REVENTA":
            payload["resale_sales_total"] += sale_amount
        else:
            payload["fabricated_sales_total"] += sale_amount

    sales_matcher = PointSalesMatchingService()
    non_recipe_qs = PointDailySale.objects.select_related("branch__erp_branch", "product").filter(
        sale_date__year=target_year,
        sale_date__month__lte=ytd_cutoff_month,
        receta__isnull=True,
        branch__erp_branch_id__isnull=False,
        branch__erp_branch__activa=True,
    )
    for row in non_recipe_qs.iterator():
        branch = getattr(row.branch, "erp_branch", None)
        if branch is None:
            continue
        mode = sales_matcher.infer_cost_mode(
            {
                "family": (row.product.metadata or {}).get("family", ""),
                "category": row.product.category,
                "name": row.product.name,
                "sku": row.product.sku,
            }
        )
        if mode == "FABRICADO":
            continue
        payload = branch_rows.setdefault(
            int(branch.id),
            {
                "branch_id": int(branch.id),
                "branch_label": branch.nombre,
                "branch_code": branch.codigo,
                "sales_total": ZERO,
                "cost_total": ZERO,
                "commercial_total": ZERO,
                "contribution_total": ZERO,
                "units_total": ZERO,
                "latest_month_sales_total": ZERO,
                "latest_month_contribution_total": ZERO,
                "fabricated_sales_total": ZERO,
                "resale_sales_total": ZERO,
                "non_recipe_total": ZERO,
                "non_recipe_resale_total": ZERO,
                "non_recipe_accessory_total": ZERO,
                "non_recipe_service_total": ZERO,
                "top_support": [],
                "top_drag": [],
            },
        )
        amount = _to_decimal(row.total_amount)
        payload["non_recipe_total"] += amount
        bucket = sales_matcher.infer_non_recipe_bucket(
            {
                "family": (row.product.metadata or {}).get("family", ""),
                "category": row.product.category,
                "name": row.product.name,
                "sku": row.product.sku,
            }
        )
        if bucket == "REVENTA":
            payload["non_recipe_resale_total"] += amount
        elif bucket == "SERVICIO":
            payload["non_recipe_service_total"] += amount
        else:
            payload["non_recipe_accessory_total"] += amount

    if latest_period:
        latest_branch_qs = (
            ProductoSucursalContribucionMensual.objects.filter(periodo=latest_period)
            .select_related("receta", "sucursal")
            .order_by("sucursal__codigo", "-contribucion_total", "receta__nombre")
        )
        latest_grouped: dict[int, list[ProductoSucursalContribucionMensual]] = defaultdict(list)
        for row in latest_branch_qs:
            latest_grouped[int(row.sucursal_id)].append(row)
        for branch_id, items in latest_grouped.items():
            support = sorted(items, key=lambda item: (_to_decimal(item.contribucion_total), _to_decimal(item.venta_total)), reverse=True)[:2]
            drag = [
                item
                for item in sorted(items, key=lambda item: (_to_decimal(item.contribucion_total), _to_decimal(item.venta_total)))
                if _to_decimal(item.contribucion_total) < 0
            ][:2]
            if branch_id not in branch_rows:
                continue
            branch_rows[branch_id]["top_support"] = [
                {
                    "label": item.receta.nombre,
                    "contribution_total": _to_decimal(item.contribucion_total),
                    "sales_total": _to_decimal(item.venta_total),
                }
                for item in support
            ]
            branch_rows[branch_id]["top_drag"] = [
                {
                    "label": item.receta.nombre,
                    "contribution_total": _to_decimal(item.contribucion_total),
                    "sales_total": _to_decimal(item.venta_total),
                }
                for item in drag
            ]

    rows: list[dict[str, object]] = []
    for payload in branch_rows.values():
        sales_total = _to_decimal(payload["sales_total"])
        contribution_total = _to_decimal(payload["contribution_total"])
        latest_month_sales_total = _to_decimal(payload["latest_month_sales_total"])
        latest_month_contribution_total = _to_decimal(payload["latest_month_contribution_total"])
        ytd_pct = _safe_pct(contribution_total, sales_total)
        latest_month_pct = _safe_pct(latest_month_contribution_total, latest_month_sales_total)
        avg_monthly_sales = sales_total / months_in_scope if months_in_scope > 0 else ZERO
        avg_monthly_contribution = contribution_total / months_in_scope if months_in_scope > 0 else ZERO
        if latest_month_pct is not None and latest_month_pct < Decimal("10"):
            traffic_tone = "danger"
        elif ytd_pct is not None and ytd_pct < Decimal("15"):
            traffic_tone = "warning"
        else:
            traffic_tone = "success"
        rows.append(
            {
                **payload,
                "contribution_pct": ytd_pct,
                "latest_month_contribution_pct": latest_month_pct,
                "avg_monthly_sales_total": avg_monthly_sales,
                "avg_monthly_contribution_total": avg_monthly_contribution,
                "latest_month_sales_vs_ytd_avg_pct": _safe_pct(latest_month_sales_total - avg_monthly_sales, avg_monthly_sales),
                "latest_month_contribution_vs_ytd_avg_pct": _safe_pct(
                    latest_month_contribution_total - avg_monthly_contribution,
                    avg_monthly_contribution,
                ),
                "latest_month_margin_delta_pp": (latest_month_pct - ytd_pct) if latest_month_pct is not None and ytd_pct is not None else None,
                "fabricated_mix_pct": _safe_pct(_to_decimal(payload["fabricated_sales_total"]), sales_total),
                "resale_mix_pct": _safe_pct(_to_decimal(payload["resale_sales_total"]), sales_total),
                "rank_tone": traffic_tone,
            }
        )

    rows.sort(key=lambda item: (_to_decimal(item["contribution_total"]), _to_decimal(item["sales_total"])), reverse=True)
    max_sales_total = max((_to_decimal(row["sales_total"]) for row in rows), default=ZERO)
    max_contribution_total = max((_to_decimal(row["contribution_total"]) for row in rows), default=ZERO)
    for row in rows:
        sales_total = _to_decimal(row["sales_total"])
        contribution_total = _to_decimal(row["contribution_total"])
        row["sales_share_pct"] = _safe_pct(sales_total, max_sales_total) if max_sales_total > 0 else ZERO
        row["contribution_share_pct"] = _safe_pct(contribution_total, max_contribution_total) if max_contribution_total > 0 else ZERO
        row["non_recipe_mix_pct"] = _safe_pct(_to_decimal(row["non_recipe_total"]), sales_total)
    top_rows = rows[:4]
    alert_rows = [
        item
        for item in sorted(rows, key=lambda row: (_to_decimal(row["contribution_total"]), _to_decimal(row["sales_total"])))
        if _to_decimal(item["contribution_total"]) < 0
    ][:4]
    improving_rows = sorted(
        rows,
        key=lambda item: _to_decimal(item.get("latest_month_contribution_vs_ytd_avg_pct")),
        reverse=True,
    )[:4]
    deteriorating_rows = sorted(
        rows,
        key=lambda item: _to_decimal(item.get("latest_month_contribution_vs_ytd_avg_pct")),
    )[:4]

    branch_options = [
        {
            "branch_id": row["branch_id"],
            "branch_label": row["branch_label"],
            "branch_code": row["branch_code"],
        }
        for row in rows
    ]

    return {
        "year": target_year,
        "ytd_cutoff_label": month_labels[ytd_cutoff_month - 1],
        "latest_period_label": latest_period.strftime("%b %Y") if latest_period else "Sin snapshot",
        "rows": rows,
        "branch_options": branch_options,
        "top_rows": top_rows,
        "alert_rows": alert_rows,
        "improving_rows": improving_rows,
        "deteriorating_rows": deteriorating_rows,
        "has_data": bool(rows),
        "basis_note": "Contribución sucursal = venta costeada - costo del producto - gasto comercial de sucursal. No incluye reparto corporativo. El comparativo del último mes se mide contra el promedio mensual YTD del año visible. No receta se separa entre reventa residual, accesorio comercial y servicio.",
    }


def build_branch_pricing_panel(*, year: int | None = None, branch_id: int | None = None, action_filter: str | None = None) -> dict[str, object]:
    from core.models import Sucursal
    from reportes.models import ProductoSucursalContribucionMensual

    target_year = int(year or timezone.localdate().year)
    today = timezone.localdate()
    ytd_cutoff_month = today.month if target_year == today.year else 12
    latest_period = (
        ProductoSucursalContribucionMensual.objects.filter(periodo__year=target_year, periodo__month__lte=ytd_cutoff_month)
        .order_by("-periodo")
        .values_list("periodo", flat=True)
        .first()
    )

    available_branch_ids = list(
        ProductoSucursalContribucionMensual.objects.filter(periodo__year=target_year, periodo__month__lte=ytd_cutoff_month)
        .values_list("sucursal_id", flat=True)
        .distinct()
    )
    if branch_id is None and available_branch_ids:
        top_branch = (
            ProductoSucursalContribucionMensual.objects.filter(
                periodo__year=target_year,
                periodo__month__lte=ytd_cutoff_month,
            )
            .values("sucursal_id", "sucursal__nombre", "sucursal__codigo")
            .annotate(contrib=Sum("contribucion_total"))
            .order_by("-contrib", "sucursal__nombre")
            .first()
        )
        branch_id = int(top_branch["sucursal_id"]) if top_branch else int(available_branch_ids[0])

    selected_branch = Sucursal.objects.filter(id=branch_id).first() if branch_id else None
    if latest_period is None or selected_branch is None:
        return {
            "year": target_year,
            "selected_branch_id": branch_id,
            "selected_branch_label": selected_branch.nombre if selected_branch else "Sin sucursal",
            "latest_period_label": "Sin snapshot",
            "rows": [],
            "top_actions": [],
            "available_actions": [],
            "selected_action": action_filter or "",
            "basis_note": "Pricing por tienda usando el último mes visible de la sucursal, con costo de producto y contribución ya materializados.",
        }

    qs = (
        ProductoSucursalContribucionMensual.objects.filter(periodo=latest_period, sucursal_id=selected_branch.id)
        .select_related("receta")
        .order_by("receta__nombre")
    )
    qty_values: list[Decimal] = []
    margin_values: list[Decimal] = []
    rows: list[dict[str, object]] = []
    for row in qs:
        qty = _to_decimal(row.unidades_vendidas)
        sales_total = _to_decimal(row.venta_total)
        contribution_total = _to_decimal(row.contribucion_total)
        cost_total = _to_decimal(row.costo_producto_total)
        cost_pct = _safe_pct(cost_total, sales_total)
        margin_pct = _safe_pct(contribution_total, sales_total)
        if qty > 0:
            qty_values.append(qty)
        if margin_pct is not None:
            margin_values.append(margin_pct)
        rows.append(
            {
                "receta_id": int(row.receta_id),
                "label": row.receta.nombre,
                "familia": row.receta.familia or "",
                "categoria": row.receta.categoria or "",
                "mode": row.receta.modo_costeo,
                "quantity": qty,
                "sales_total": sales_total,
                "asp": _to_decimal(row.asp),
                "cost_total": cost_total,
                "cost_pct": cost_pct,
                "commercial_total": _to_decimal(row.gasto_comercial_total),
                "contribution_total": contribution_total,
                "contribution_pct": margin_pct,
            }
        )

    qty_median = _decimal_median(qty_values) if qty_values else ZERO
    margin_median = _decimal_median(margin_values) if margin_values else ZERO
    for row in rows:
        qty = _to_decimal(row["quantity"])
        margin_pct = _to_decimal(row["contribution_pct"])
        high_volume = qty >= qty_median if qty_median > 0 else True
        healthy_margin = margin_pct >= margin_median if margin_median > 0 else True
        if margin_pct < Decimal("0.10"):
            bucket = "Reformular"
            recommendation = "Revisar precio, costo o continuidad en esta sucursal."
        elif (not healthy_margin) and high_volume:
            bucket = "Subir precio"
            recommendation = "Tiene tracción local, pero la contribución se quedó corta."
        elif (not high_volume) and healthy_margin:
            bucket = "Promover"
            recommendation = "Tiene margen sano en la sucursal; conviene empujarlo."
        elif healthy_margin and high_volume:
            bucket = "Defender"
            recommendation = "Sostiene la sucursal; mantener precio y disponibilidad."
        else:
            bucket = "Corregir costo"
            recommendation = "No logra margen ni volumen; revisar receta, empaque o merma."
        row["bucket"] = bucket
        row["recommendation"] = recommendation

    bucket_priority = {"Subir precio": 0, "Corregir costo": 1, "Promover": 2, "Defender": 3, "Reformular": 4}
    available_actions = ["Defender", "Promover", "Subir precio", "Corregir costo", "Reformular"]
    filtered_rows = rows
    if action_filter:
        filtered_rows = [row for row in rows if row["bucket"] == action_filter]
    top_actions = sorted(
        filtered_rows,
        key=lambda item: (bucket_priority.get(str(item["bucket"]), 99), _to_decimal(item["contribution_total"]), -_to_decimal(item["sales_total"])),
    )[:8]
    filtered_rows.sort(key=lambda item: (-_to_decimal(item["contribution_total"]), -_to_decimal(item["sales_total"]), str(item["label"])))

    return {
        "year": target_year,
        "selected_branch_id": int(selected_branch.id),
        "selected_branch_label": selected_branch.nombre,
        "selected_branch_code": selected_branch.codigo,
        "latest_period_label": latest_period.strftime("%b %Y"),
        "rows": filtered_rows,
        "top_actions": top_actions,
        "available_actions": available_actions,
        "selected_action": action_filter or "",
        "basis_note": "Pricing por tienda usando el último mes visible de la sucursal, con costo de producto y contribución ya materializados.",
    }


def build_executive_bi_panels(
    *,
    latest_date: date | None = None,
    months: int = 6,
    branch_id: int | None = None,
    action_filter: str | None = None,
    budget_month: int | None = None,
) -> dict[str, object]:
    trusted_sales_latest = latest_date or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    yoy_latest_date = max(
        trusted_sales_latest,
        _partial_sales_cache_latest_end() or trusted_sales_latest,
    )
    common_flow_date = _common_flow_cutoff_date() or trusted_sales_latest
    return {
        "latest_cutoff_date": trusted_sales_latest,
        "forecast_panel": build_sales_forecast_panel(latest_date=trusted_sales_latest),
        "yoy_panel": build_monthly_yoy_panel(latest_date=yoy_latest_date, months=months),
        "profitability_panel": build_profitability_panel(latest_date=trusted_sales_latest),
        "branch_contribution_panel": build_branch_contribution_panel(year=trusted_sales_latest.year),
        "branch_pricing_panel": build_branch_pricing_panel(
            year=trusted_sales_latest.year,
            branch_id=branch_id,
            action_filter=action_filter,
        ),
        "production_sales_panel": build_production_vs_sales_panel(latest_date=common_flow_date),
        "central_flow_panel": build_central_flow_panel(latest_date=common_flow_date, months=months),
        "inventory_ledger_panel": build_monthly_inventory_ledger_panel(latest_date=common_flow_date, months=months),
        "budget_operating_panel": build_budget_operating_panel(year=trusted_sales_latest.year, selected_month=budget_month),
    }
