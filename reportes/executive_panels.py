from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path

from django.db.models import Max, Min, Q, Sum
from django.utils import timezone
from unidecode import unidecode

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
from recetas.models import RecetaCostoSemanal


ZERO = Decimal("0")
ONE = Decimal("1")
Q2 = Decimal("0.01")
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
RECENT_POINT_SOURCE = "/Report/VentasCategorias"
OFFICIAL_PARTIAL_CACHE_PATH = Path("storage/pos_bridge/reports/official_partial_sales_cache.json")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _normalize_text(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


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


def _partial_month_amount_quantity(*, start_date: date, end_date: date) -> tuple[Decimal, Decimal]:
    payload = _best_partial_cache_payload(start_date, end_date)
    if payload is None:
        sales_qs = _active_sales_queryset(start_date=start_date, end_date=end_date)
        amount = _to_decimal(sales_qs.aggregate(v=Sum("total_amount")).get("v"))
        quantity = _to_decimal(sales_qs.aggregate(v=Sum("quantity")).get("v"))
        return amount, quantity

    amount = _to_decimal(payload.get("total_amount"))
    quantity = _to_decimal(payload.get("total_quantity"))
    cached_end = date.fromisoformat(payload["period_end"])
    if cached_end >= end_date:
        return amount, quantity

    supplement_start = cached_end + timedelta(days=1)
    supplement_qs = _active_sales_queryset(start_date=supplement_start, end_date=end_date)
    amount += _to_decimal(supplement_qs.aggregate(v=Sum("total_amount")).get("v"))
    quantity += _to_decimal(supplement_qs.aggregate(v=Sum("quantity")).get("v"))
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


def _official_sales_stage_max_date() -> date | None:
    return (
        PointDailySale.objects.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
        .aggregate(v=Max("sale_date"))
        .get("v")
    )


def _recent_sales_stage_max_date() -> date | None:
    return (
        PointDailySale.objects.filter(source_endpoint=RECENT_POINT_SOURCE)
        .aggregate(v=Max("sale_date"))
        .get("v")
    )


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


def _production_cutoff_date() -> date | None:
    return PointProductionLine.objects.aggregate(v=Max("production_date")).get("v")


def _waste_cutoff_date() -> date | None:
    value = PointWasteLine.objects.aggregate(v=Max("movement_at")).get("v")
    if value is None:
        return None
    return timezone.localtime(value).date() if timezone.is_aware(value) else value.date()


def _common_flow_cutoff_date() -> date | None:
    candidates = [value for value in [_sales_cutoff_date(), _production_cutoff_date(), _waste_cutoff_date()] if value]
    if not candidates:
        return None
    return min(candidates)


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
    current_week_start = _week_start(latest_date)
    week_starts = [current_week_start - timedelta(days=7 * idx) for idx in range(max(lookback_weeks, baseline_weeks))]
    week_starts.reverse()

    weekly_rows: list[ForecastWeek] = []
    for week_start in week_starts:
        week_end = week_start + timedelta(days=6)
        sales_qs = _active_sales_queryset(start_date=week_start, end_date=week_end)
        indicator_qs = _active_indicator_queryset(start_date=week_start, end_date=week_end)
        amount = _to_decimal(sales_qs.aggregate(v=Sum("total_amount")).get("v"))
        quantity = _to_decimal(sales_qs.aggregate(v=Sum("quantity")).get("v"))
        tickets = int(indicator_qs.aggregate(v=Sum("total_tickets")).get("v") or 0)
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
    high_scenario_amount = max([base_amount] + [row.amount for row in atypical_recent]) if atypical_recent else base_amount
    high_scenario_quantity = max([base_quantity] + [row.quantity for row in atypical_recent]) if atypical_recent else base_quantity
    next_week_start = current_week_start + timedelta(days=7)
    next_week_end = next_week_start + timedelta(days=6)

    return {
        "latest_date": latest_date,
        "basis_note": (
            "Pronóstico inferido con las últimas 2-3 semanas normales. "
            "No existe calendario oficial de fechas atípicas parametrizado; los picos se detectan por anomalía histórica."
        ),
        "baseline_label": f"{len(baseline_candidates)} semana(s) base",
        "forecast_amount": base_amount.quantize(Q2),
        "forecast_quantity": base_quantity.quantize(Q2),
        "forecast_tickets": base_tickets,
        "forecast_avg_ticket": base_avg_ticket.quantize(Q2),
        "high_scenario_amount": high_scenario_amount.quantize(Q2),
        "high_scenario_quantity": high_scenario_quantity.quantize(Q2),
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
    rows: list[dict[str, object]] = []
    for offset in range(months - 1, -1, -1):
        month_anchor = _shift_month(_month_start(latest_date), -offset)
        current_start = month_anchor
        current_end = _month_end(month_anchor)
        partial_cutoff = latest_date if month_anchor.year == latest_date.year and month_anchor.month == latest_date.month else current_end
        current_end = min(current_end, partial_cutoff)
        current_partial_payload = None

        prev_year_start = date(current_start.year - 1, current_start.month, 1)
        prev_year_limit_day = min(current_end.day, monthrange(prev_year_start.year, prev_year_start.month)[1])
        prev_year_end = date(prev_year_start.year, prev_year_start.month, prev_year_limit_day)

        current_month_cache = PointMonthlySalesOfficial.objects.filter(month_start=current_start).first()
        prev_month_cache = PointMonthlySalesOfficial.objects.filter(month_start=prev_year_start).first()
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

        current_sales = _active_sales_queryset(start_date=current_start, end_date=current_end)
        prev_sales = _active_sales_queryset(start_date=prev_year_start, end_date=prev_year_end)
        current_ind = _active_indicator_queryset(start_date=current_start, end_date=current_end)
        prev_ind = _active_indicator_queryset(start_date=prev_year_start, end_date=prev_year_end)

        if current_month_cache and current_is_full_month:
            amount = _to_decimal(current_month_cache.total_amount)
            qty = _to_decimal(current_month_cache.total_quantity)
            tickets = int(current_ind.aggregate(v=Sum("total_tickets")).get("v") or 0)
            avg_ticket = (amount / Decimal(str(tickets))) if tickets > 0 else ZERO
        elif current_partial_payload is not None:
            amount, qty = _partial_month_amount_quantity(start_date=current_start, end_date=current_end)
            tickets = int(current_ind.aggregate(v=Sum("total_tickets")).get("v") or 0)
            avg_ticket = (amount / Decimal(str(tickets))) if tickets > 0 else ZERO
        else:
            amount = _to_decimal(current_sales.aggregate(v=Sum("total_amount")).get("v"))
            qty = _to_decimal(current_sales.aggregate(v=Sum("quantity")).get("v"))
            tickets = int(current_ind.aggregate(v=Sum("total_tickets")).get("v") or 0)
            avg_ticket = (amount / Decimal(str(tickets))) if tickets > 0 else ZERO

        prev_official_available = bool(prev_month_cache and prev_is_full_month)
        if prev_official_available:
            prev_amount = _to_decimal(prev_month_cache.total_amount)
            prev_qty = _to_decimal(prev_month_cache.total_quantity)
            prev_tickets = int(prev_ind.aggregate(v=Sum("total_tickets")).get("v") or 0)
            prev_avg_ticket = (prev_amount / Decimal(str(prev_tickets))) if prev_tickets > 0 else ZERO
        elif prev_partial_payload is not None:
            prev_amount = _to_decimal(prev_partial_payload.get("total_amount"))
            prev_qty = _to_decimal(prev_partial_payload.get("total_quantity"))
            prev_tickets = 0
            prev_avg_ticket = None
        elif not prev_is_full_month:
            prev_amount = None
            prev_qty = None
            prev_tickets = 0
            prev_avg_ticket = None
        else:
            prev_amount = _to_decimal(prev_sales.aggregate(v=Sum("total_amount")).get("v"))
            prev_qty = _to_decimal(prev_sales.aggregate(v=Sum("quantity")).get("v"))
            prev_tickets = int(prev_ind.aggregate(v=Sum("total_tickets")).get("v") or 0)
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


def build_profitability_panel(*, latest_date: date | None = None, lookback_days: int = 28) -> dict[str, object]:
    latest_date = latest_date or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    window_start = latest_date - timedelta(days=max(lookback_days - 1, 0))
    prev_start = window_start - timedelta(days=lookback_days)
    prev_end = window_start - timedelta(days=1)
    latest_week = RecetaCostoSemanal.objects.aggregate(v=Max("week_start")).get("v")

    cost_map: dict[int, Decimal] = {}
    if latest_week:
        for row in RecetaCostoSemanal.objects.filter(
            week_start=latest_week,
            scope_type=RecetaCostoSemanal.SCOPE_RECIPE,
            receta__tipo="PRODUCTO_FINAL",
            receta_id__isnull=False,
        ).values("receta_id", "costo_total"):
            cost_map[int(row["receta_id"])] = _to_decimal(row["costo_total"])

    current_rows = (
        _active_sales_queryset(start_date=window_start, end_date=latest_date)
        .filter(total_amount__gt=0, receta_id__isnull=False)
        .values("receta_id", "receta__nombre", "receta__familia", "receta__categoria")
        .annotate(revenue=Sum("total_amount"), quantity=Sum("quantity"))
    )
    prev_map = {
        int(row["receta_id"]): _to_decimal(row["quantity"])
        for row in (
            _active_sales_queryset(start_date=prev_start, end_date=prev_end)
            .filter(total_amount__gt=0, receta_id__isnull=False)
            .values("receta_id")
            .annotate(quantity=Sum("quantity"))
        )
    }

    material_rows: list[dict[str, object]] = []
    qty_values: list[Decimal] = []
    margin_values: list[Decimal] = []
    for row in current_rows:
        receta_id = int(row["receta_id"])
        unit_cost = cost_map.get(receta_id)
        if unit_cost is None:
            continue
        qty = _to_decimal(row["quantity"])
        revenue = _to_decimal(row["revenue"])
        if qty <= 0 or revenue <= 0:
            continue
        cost_total = qty * unit_cost
        margin = revenue - cost_total
        margin_pct = _safe_pct(margin, revenue)
        prev_qty = prev_map.get(receta_id, ZERO)
        trend_pct = _safe_pct(qty - prev_qty, prev_qty)
        qty_values.append(qty)
        if margin_pct is not None:
            margin_values.append(margin_pct)
        material_rows.append(
            {
                "receta_id": receta_id,
                "label": row["receta__nombre"],
                "familia": row["receta__familia"] or "",
                "categoria": row["receta__categoria"] or "",
                "revenue": revenue.quantize(Q2),
                "quantity": qty.quantize(Q2),
                "unit_cost": unit_cost.quantize(Q2),
                "cost_total": cost_total.quantize(Q2),
                "margin": margin.quantize(Q2),
                "margin_pct": margin_pct.quantize(Q2) if margin_pct is not None else None,
                "prev_quantity": prev_qty.quantize(Q2),
                "trend_pct": trend_pct.quantize(Q2) if trend_pct is not None else None,
            }
        )

    qty_median = _decimal_median(qty_values) if qty_values else ZERO
    margin_median = _decimal_median(margin_values) if margin_values else ZERO
    promo_candidates: list[dict[str, object]] = []
    for row in material_rows:
        qty = _to_decimal(row["quantity"])
        margin_pct = _to_decimal(row["margin_pct"])
        high_volume = qty >= qty_median if qty_median > 0 else True
        healthy_margin = margin_pct >= margin_median if margin_median > 0 else True
        trend_pct = row["trend_pct"]
        if high_volume and healthy_margin:
            recommendation = "Defender precio y disponibilidad"
            bucket = "Defender"
        elif high_volume and not healthy_margin:
            recommendation = "Revisar costo o subir precio"
            bucket = "Ajustar margen"
        elif (not high_volume) and healthy_margin:
            recommendation = "Promoción táctica"
            bucket = "Promocionar"
            promo_candidates.append(row)
        else:
            recommendation = "Depurar, reformular o sacar de foco"
            bucket = "Revisar portafolio"
        if trend_pct is not None and _to_decimal(trend_pct) < Decimal("-8") and healthy_margin:
            recommendation = "Promoción táctica inmediata"
            bucket = "Promocionar"
            if row not in promo_candidates:
                promo_candidates.append(row)
        row["bucket"] = bucket
        row["recommendation"] = recommendation

    material_rows.sort(key=lambda item: (-_to_decimal(item["margin"]), -_to_decimal(item["revenue"]), item["label"]))
    promo_candidates.sort(key=lambda item: (_to_decimal(item.get("trend_pct") or 0), -_to_decimal(item["margin_pct"])))

    return {
        "lookback_days": lookback_days,
        "latest_week": latest_week,
        "rows": material_rows[:18],
        "promo_candidates": promo_candidates[:4],
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
    first_cedis_date = _first_central_cedis_production_date()

    sales_by_category: dict[str, Decimal] = defaultdict(lambda: ZERO)
    production_by_category: dict[str, Decimal] = defaultdict(lambda: ZERO)
    weekly_rows: list[dict[str, object]] = []
    for idx in range(lookback_weeks - 1, -1, -1):
        week_start = current_week_start - timedelta(days=7 * idx)
        week_end = week_start + timedelta(days=6)
        sales_week = (
            _active_sales_queryset(start_date=week_start, end_date=week_end)
            .filter(receta_id__isnull=False, total_amount__gt=0)
            .select_related("receta")
        )
        prod_week = (
            PointProductionLine.objects.filter(
                production_date__gte=week_start,
                production_date__lte=week_end,
                is_insumo=False,
            )
            .select_related("erp_branch", "branch", "receta")
        )
        sold_units = ZERO
        produced_units = ZERO
        for sale in sales_week:
            qty = _to_decimal(sale.quantity)
            sold_units += qty
            if week_start == current_week_start:
                label = _operational_category(
                    category=getattr(sale.receta, "categoria", ""),
                    family=getattr(sale.receta, "familia", ""),
                    item_name=sale.product.name,
                )
                sales_by_category[label] += qty
        for prod in prod_week:
            branch_label = _normalize_text(getattr(prod.erp_branch, "nombre", "") or prod.branch.name)
            central_branch = _central_production_branch_for_day(prod.production_date, first_cedis_date=first_cedis_date)
            if branch_label != central_branch:
                continue
            if central_branch not in production_allowed and not (
                central_branch == "matriz" and first_cedis_date and prod.production_date < first_cedis_date
            ):
                continue
            qty = _to_decimal(prod.produced_quantity)
            produced_units += qty
            if week_start == current_week_start:
                label = _operational_category(
                    category=getattr(prod.receta, "categoria", ""),
                    family=getattr(prod.receta, "familia", ""),
                    item_name=prod.item_name,
                )
                production_by_category[label] += qty
        weekly_rows.append(
            {
                "week_label": f"{week_start.strftime('%d %b')} → {week_end.strftime('%d %b')}",
                "sold_units": sold_units.quantize(Q2),
                "produced_units": produced_units.quantize(Q2),
                "delta_units": (produced_units - sold_units).quantize(Q2),
            }
        )

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
    first_cedis_date = _first_central_cedis_production_date()
    partial_cache = _official_partial_sales_cache()

    snapshot_month_map: dict[str, dict[str, Decimal | date | None]] = {}
    for row in build_monthly_inventory_ledger_panel(latest_date=latest_date, months=months).get("rows", []):
        snapshot_month_map[str(row["month_label"])] = {
            "actual_closing": row["actual_closing"],
            "variance_units": row["variance_units"],
            "is_partial": row["is_partial"],
        }

    rows: list[dict[str, object]] = []
    for offset in range(months - 1, -1, -1):
        month_anchor = _shift_month(_month_start(latest_date), -offset)
        month_start = month_anchor
        month_end = min(
            _month_end(month_anchor),
            latest_date if month_anchor.year == latest_date.year and month_anchor.month == latest_date.month else _month_end(month_anchor),
        )

        production_rows = (
            PointProductionLine.objects.filter(
                production_date__gte=month_start,
                production_date__lte=month_end,
                is_insumo=False,
            )
            .select_related("branch", "erp_branch")
        )
        production_units = ZERO
        for row in production_rows:
            branch_label = _normalize_text(getattr(row.erp_branch, "nombre", "") or row.branch.name)
            central_branch = _central_production_branch_for_day(row.production_date, first_cedis_date=first_cedis_date)
            if branch_label == central_branch:
                production_units += _to_decimal(row.produced_quantity)

        transfer_units = ZERO
        transfer_rows = PointTransferLine.objects.filter(
            is_insumo=False,
            destination_branch__name__iexact="CEDIS",
            received_at__isnull=False,
            received_at__date__gte=month_start,
            received_at__date__lte=month_end,
        )
        transfer_units = sum((_to_decimal(row.received_quantity) for row in transfer_rows), ZERO)

        current_month_cache = PointMonthlySalesOfficial.objects.filter(month_start=month_start).first()
        current_is_full_month = month_end == _month_end(month_anchor)
        current_partial_payload = None
        if not current_is_full_month:
            current_partial_payload = partial_cache.get(f"{month_start.isoformat()}_{month_end.isoformat()}")

        if current_month_cache and current_is_full_month:
            sold_units = _to_decimal(current_month_cache.total_quantity)
        elif current_partial_payload is not None:
            sold_units = _to_decimal(current_partial_payload.get("total_quantity"))
        else:
            sold_units = _to_decimal(
                _active_sales_queryset(start_date=month_start, end_date=month_end).aggregate(v=Sum("quantity")).get("v")
            )

        waste_rows = (
            PointWasteLine.objects.filter(
                movement_at__date__gte=month_start,
                movement_at__date__lte=month_end,
                receta_id__isnull=False,
            )
            .select_related("branch", "erp_branch")
        )
        waste_units = sum(
            (
                _to_decimal(row.quantity)
                for row in waste_rows
                if _is_network_inventory_branch(
                    point_name=getattr(row.branch, "name", ""),
                    erp_active=getattr(getattr(row, "erp_branch", None), "activa", None),
                )
            ),
            ZERO,
        )

        supply_units = production_units + transfer_units
        net_units = supply_units - sold_units - waste_units
        snapshot_row = snapshot_month_map.get(month_start.strftime("%Y-%m")) or {}
        central_source = "CEDIS" if (first_cedis_date and month_end >= first_cedis_date) else "MATRIZ"
        rows.append(
            {
                "month_label": month_start.strftime("%Y-%m"),
                "is_partial": month_end != _month_end(month_anchor),
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

    return {
        "cutoff_date": latest_date,
        "rows": rows,
        "first_cedis_date": first_cedis_date,
        "basis_note": (
            "Flujo central histórico calculado desde registros diarios disponibles: producción central "
            "(MATRIZ antes de CEDIS, CEDIS después), transferencias recibidas a CEDIS, venta Point y merma Point. "
            "Solo cuando existe snapshot real se muestra cierre de inventario."
        ),
    }


def build_monthly_inventory_ledger_panel(*, latest_date: date | None = None, months: int = 6) -> dict[str, object]:
    latest_date = latest_date or _common_flow_cutoff_date() or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    first_snapshot_at = PointInventorySnapshot.objects.aggregate(v=Min("captured_at")).get("v")
    snapshot_coverage_start = None
    if first_snapshot_at is not None:
        snapshot_local = timezone.localtime(first_snapshot_at) if timezone.is_aware(first_snapshot_at) else first_snapshot_at
        snapshot_coverage_start = _month_start(snapshot_local.date())

    earliest_requested_month = _shift_month(_month_start(latest_date), -(months - 1))
    effective_start_month = max(earliest_requested_month, snapshot_coverage_start) if snapshot_coverage_start else None

    rows: list[dict[str, object]] = []
    omitted_months: list[str] = []
    for offset in range(months - 1, -1, -1):
        month_anchor = _shift_month(_month_start(latest_date), -offset)
        if effective_start_month and month_anchor < effective_start_month:
            omitted_months.append(month_anchor.strftime("%Y-%m"))
            continue
        month_start = month_anchor
        month_end = min(_month_end(month_anchor), latest_date if month_anchor.year == latest_date.year and month_anchor.month == latest_date.month else _month_end(month_anchor))
        base_snapshot_qs = PointInventorySnapshot.objects.filter(
            captured_at__date__gte=month_start,
            captured_at__date__lte=month_end,
        ).select_related("branch", "branch__erp_branch")
        filtered_snapshots = [
            row
            for row in base_snapshot_qs.order_by("captured_at", "id")
            if _is_network_inventory_branch(
                point_name=getattr(row.branch, "name", ""),
                erp_active=getattr(getattr(row.branch, "erp_branch", None), "activa", None),
            )
        ]
        if not filtered_snapshots:
            omitted_months.append(month_anchor.strftime("%Y-%m"))
            continue

        opening_job_id = filtered_snapshots[0].sync_job_id
        closing_job_id = filtered_snapshots[-1].sync_job_id
        opening_units = sum((_to_decimal(row.stock) for row in filtered_snapshots if row.sync_job_id == opening_job_id), ZERO)
        closing_units = sum((_to_decimal(row.stock) for row in filtered_snapshots if row.sync_job_id == closing_job_id), ZERO)

        production_rows = PointProductionLine.objects.filter(
            production_date__gte=month_start,
            production_date__lte=month_end,
            is_insumo=False,
        ).select_related("branch", "erp_branch")
        produced_units = sum(
            (
                _to_decimal(row.produced_quantity)
                for row in production_rows
                if _is_network_inventory_branch(
                    point_name=getattr(row.branch, "name", ""),
                    erp_active=getattr(getattr(row, "erp_branch", None), "activa", None),
                )
            ),
            ZERO,
        )
        sold_units = _to_decimal(
            _active_sales_queryset(start_date=month_start, end_date=month_end).aggregate(v=Sum("quantity")).get("v")
        )
        waste_rows = PointWasteLine.objects.filter(
            movement_at__date__gte=month_start,
            movement_at__date__lte=month_end,
            receta_id__isnull=False,
        ).select_related("branch", "erp_branch")
        waste_units = sum(
            (
                _to_decimal(row.quantity)
                for row in waste_rows
                if _is_network_inventory_branch(
                    point_name=getattr(row.branch, "name", ""),
                    erp_active=getattr(getattr(row, "erp_branch", None), "activa", None),
                )
            ),
            ZERO,
        )
        theoretical_closing = opening_units + produced_units - sold_units - waste_units
        variance_units = closing_units - theoretical_closing
        rows.append(
            {
                "month_label": month_start.strftime("%Y-%m"),
                "is_partial": month_end != _month_end(month_anchor),
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
        "snapshot_coverage_start": snapshot_coverage_start,
        "omitted_months": omitted_months,
    }


def build_executive_bi_panels(*, latest_date: date | None = None) -> dict[str, object]:
    trusted_sales_latest = latest_date or _sales_cutoff_date() or (timezone.localdate() - timedelta(days=1))
    yoy_latest_date = max(
        trusted_sales_latest,
        _partial_sales_cache_latest_end() or trusted_sales_latest,
    )
    common_flow_date = _common_flow_cutoff_date() or trusted_sales_latest
    return {
        "latest_cutoff_date": trusted_sales_latest,
        "forecast_panel": build_sales_forecast_panel(latest_date=trusted_sales_latest),
        "yoy_panel": build_monthly_yoy_panel(latest_date=yoy_latest_date),
        "profitability_panel": build_profitability_panel(latest_date=trusted_sales_latest),
        "production_sales_panel": build_production_vs_sales_panel(latest_date=common_flow_date),
        "central_flow_panel": build_central_flow_panel(latest_date=common_flow_date),
        "inventory_ledger_panel": build_monthly_inventory_ledger_panel(latest_date=common_flow_date),
    }
