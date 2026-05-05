from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache
from typing import Any

from django.db.models import Q, Sum
from django.utils import timezone

from core.branch_catalog import EXCLUDED_BRANCH_CODES
from core.models import Sucursal
from pos_bridge.models import PointDailySale
from recetas.models import Receta
from ventas.models import EventoVenta, EventoVentaForecast, VentaAutoritativaPoint
from ventas.services.sales_truth import recipe_point_codes

ZERO = Decimal("0")
ONE = Decimal("1")
ALPHA_ROTACION = Decimal("1.0")
ROTACION_MIN = Decimal("0.50")
ROTACION_MAX = Decimal("2.00")
SUCURSAL_MIN = Decimal("0.70")
SUCURSAL_MAX = Decimal("1.40")
FACTOR_CALIBRACION_DIA_NINO = Decimal("0.98143")
FACTOR_EVENTO_SUAVE = ONE + ((FACTOR_CALIBRACION_DIA_NINO - ONE) * Decimal("0.35"))
V9_MODEL_VERSION = "v9-producto-sucursal-explicito"
LOOKBACK_DAYS = 30

SIZE_TOKENS = ("grande", "mediano", "chico", "individual", "mini", "rebanada", "r")


@dataclass(frozen=True)
class V9ProductBranchContext:
    product: Receta
    branch: Sucursal
    rotacion_2026: Decimal
    rotacion_2025: Decimal
    factor_rotacion: Decimal
    factor_sucursal: Decimal
    factor_rotacion_cap_aplicado: bool
    factor_sucursal_cap_aplicado: bool


def _decimal(value: Any) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value or 0))


def _ceil_decimal(value: Decimal) -> Decimal:
    return Decimal(math.ceil(float(value or ZERO)))


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> tuple[Decimal, bool]:
    if value < low:
        return low, True
    if value > high:
        return high, True
    return value, False


def _date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _same_day_previous_year(target: date) -> date:
    try:
        return target.replace(year=target.year - 1)
    except ValueError:
        return target.replace(year=target.year - 1, day=28)


def _event_dates(event: EventoVenta) -> list[date]:
    start = event.analysis_start_date or event.main_date
    end = event.analysis_end_date or event.main_date
    if end < start:
        start, end = end, start
    return _date_range(start, end)


def _reference_window(event: EventoVenta) -> tuple[date, date, date, date]:
    today = timezone.localdate()
    anchor = min(today, event.main_date or today) - timedelta(days=1)
    start_2026 = anchor - timedelta(days=LOOKBACK_DAYS - 1)
    end_2026 = anchor
    return start_2026, end_2026, _same_day_previous_year(start_2026), _same_day_previous_year(end_2026)


def _active_event_products(event: EventoVenta) -> list[Receta]:
    return list(
        Receta.objects.filter(sales_event_products__sales_event=event, sales_event_products__is_active=True)
        .distinct()
        .order_by("familia", "categoria", "nombre")
    )


def _active_event_branches(event: EventoVenta) -> list[Sucursal]:
    linked_branches = list(
        Sucursal.objects.filter(sales_events__sales_event=event, sales_events__is_active=True, activa=True)
        .exclude(codigo__in=EXCLUDED_BRANCH_CODES)
        .distinct()
        .order_by("codigo")
    )
    if linked_branches:
        return linked_branches
    return list(
        Sucursal.objects.filter(sales_event_forecasts__sales_event=event, activa=True)
        .exclude(codigo__in=EXCLUDED_BRANCH_CODES)
        .distinct()
        .order_by("codigo")
    )


def _product_code_filter(product_id: int) -> Q:
    codes = recipe_point_codes(product_id)
    filters = Q(receta_id=product_id)
    if codes:
        filters |= Q(product__sku__in=codes)
        for code in codes:
            filters |= Q(raw_payload__Codigo=code)
    return filters


@lru_cache(maxsize=20000)
def _point_sales_qty_cached(product_id: int, branch_id: int | None, start: date, end: date) -> Decimal:
    qs = PointDailySale.objects.filter(sale_date__range=(start, end)).filter(_product_code_filter(product_id))
    if branch_id:
        qs = qs.filter(branch__erp_branch_id=branch_id)
    total = qs.aggregate(total=Sum("quantity")).get("total")
    qty = _decimal(total)
    if qty:
        return qty

    auth_qs = VentaAutoritativaPoint.objects.filter(sale_date__range=(start, end)).filter(
        Q(product_id=product_id) | Q(product_code__in=recipe_point_codes(product_id))
    )
    if branch_id:
        auth_qs = auth_qs.filter(branch_id=branch_id)
    return _decimal(auth_qs.aggregate(total=Sum("quantity")).get("total"))


def _point_sales_qty(product: Receta, branch: Sucursal | None, start: date, end: date) -> Decimal:
    return _point_sales_qty_cached(product.id, branch.id if branch else None, start, end)


def _point_sales_qty_for_products(products: list[Receta], branch: Sucursal | None, start: date, end: date) -> Decimal:
    total = ZERO
    for product in products:
        total += _point_sales_qty(product, branch, start, end)
    return total


def _product_size(product: Receta) -> str:
    category = (product.categoria or "").strip().lower()
    if category:
        return category
    name = (product.nombre or "").strip().lower()
    for token in SIZE_TOKENS:
        if re.search(rf"(^|\s){re.escape(token)}$", name):
            return token
    return ""


def _family(product: Receta) -> str:
    return (product.familia or "Sin familia").strip() or "Sin familia"


def _event_2025_window(event_dates: list[date]) -> tuple[date, date]:
    homologue_dates = [_same_day_previous_year(day) for day in event_dates]
    return min(homologue_dates), max(homologue_dates)


def _family_share(
    *,
    family_products: list[Receta],
    event_dates: list[date],
    branch: Sucursal,
) -> dict[date, Decimal]:
    totals_by_day: dict[date, Decimal] = {}
    range_total = ZERO
    for day in event_dates:
        homologue_day = _same_day_previous_year(day)
        day_total = _point_sales_qty_for_products(family_products, branch, homologue_day, homologue_day)
        totals_by_day[day] = day_total
        range_total += day_total
    if not range_total:
        equal_share = ONE / Decimal(len(event_dates))
        return {day: equal_share for day in event_dates}
    return {day: totals_by_day[day] / range_total for day in event_dates}


def _product_day_shares(
    *,
    product: Receta,
    family_products: list[Receta],
    branch: Sucursal,
    event_dates: list[date],
) -> tuple[dict[date, Decimal], Decimal]:
    day_totals: dict[date, Decimal] = {}
    product_range_total = ZERO
    for day in event_dates:
        homologue_day = _same_day_previous_year(day)
        qty = _point_sales_qty(product, branch, homologue_day, homologue_day)
        day_totals[day] = qty
        product_range_total += qty

    if product_range_total:
        product_share = {day: day_totals[day] / product_range_total for day in event_dates}
    else:
        equal_share = ONE / Decimal(len(event_dates))
        product_share = {day: equal_share for day in event_dates}

    family_share = _family_share(family_products=family_products, event_dates=event_dates, branch=branch)
    product_weight = Decimal("0.70") if product_range_total >= Decimal("10") else Decimal("0.30")
    family_weight = ONE - product_weight
    shares = {day: (product_weight * product_share[day]) + (family_weight * family_share[day]) for day in event_dates}
    return shares, product_range_total


def _fallback_base(
    *,
    product: Receta,
    branch: Sucursal,
    target_day: date,
    active_products: list[Receta],
    rotacion_2026: Decimal,
    event_dates_2025: tuple[date, date],
) -> tuple[Decimal, int]:
    family = _family(product)
    size = _product_size(product)
    peer_same_size = [p for p in active_products if _family(p) == family and _product_size(p) == size and p.id != product.id]
    if peer_same_size:
        total = _point_sales_qty_for_products(peer_same_size, branch, target_day, target_day)
        if total:
            return total / Decimal(len(peer_same_size)), 1

    peer_family = [p for p in active_products if _family(p) == family and p.id != product.id]
    if peer_family:
        total = _point_sales_qty_for_products(peer_family, branch, target_day, target_day)
        if total:
            return total / Decimal(len(peer_family)), 2

    family_products = [p for p in active_products if _family(p) == family]
    family_event_2025 = _point_sales_qty_for_products(family_products, branch, event_dates_2025[0], event_dates_2025[1])
    april_start = date(event_dates_2025[0].year, 4, 1)
    april_end = date(event_dates_2025[0].year, 4, 30)
    april_daily_avg = _point_sales_qty_for_products(family_products, branch, april_start, april_end) / Decimal("30")
    if april_daily_avg:
        multiplier = family_event_2025 / april_daily_avg
        return rotacion_2026 * multiplier, 3

    return rotacion_2026 * Decimal("1.5"), 4


def _branch_factor(branch: Sucursal, products: list[Receta], start_2026: date, end_2026: date, start_2025: date, end_2025: date) -> tuple[Decimal, bool]:
    qty_2026 = _point_sales_qty_for_products(products, branch, start_2026, end_2026)
    qty_2025 = _point_sales_qty_for_products(products, branch, start_2025, end_2025)
    raw = (qty_2026 / qty_2025) if qty_2025 else ONE
    return _clamp(raw, SUCURSAL_MIN, SUCURSAL_MAX)


def _rotation_context(
    product: Receta,
    branch: Sucursal,
    factor_sucursal: Decimal,
    factor_sucursal_cap: bool,
    start_2026: date,
    end_2026: date,
    start_2025: date,
    end_2025: date,
) -> V9ProductBranchContext:
    rotacion_2026 = _point_sales_qty(product, branch, start_2026, end_2026) / Decimal(LOOKBACK_DAYS)
    rotacion_2025 = _point_sales_qty(product, branch, start_2025, end_2025) / Decimal(LOOKBACK_DAYS)
    raw_factor = (rotacion_2026 + ALPHA_ROTACION) / (rotacion_2025 + ALPHA_ROTACION)
    factor_rotacion, cap_rotacion = _clamp(raw_factor, ROTACION_MIN, ROTACION_MAX)
    return V9ProductBranchContext(
        product=product,
        branch=branch,
        rotacion_2026=rotacion_2026,
        rotacion_2025=rotacion_2025,
        factor_rotacion=factor_rotacion,
        factor_sucursal=factor_sucursal,
        factor_rotacion_cap_aplicado=cap_rotacion,
        factor_sucursal_cap_aplicado=factor_sucursal_cap,
    )


def _forecast_v8_total(event: EventoVenta, product: Receta, branch: Sucursal, forecast_date: date) -> Decimal:
    return _decimal(
        EventoVentaForecast.objects.filter(
            sales_event=event,
            product=product,
            branch=branch,
            forecast_date=forecast_date,
        )
        .exclude(model_version=V9_MODEL_VERSION)
        .aggregate(total=Sum("final_forecast"))
        .get("total")
    )


def _category_average_rotation(products: list[Receta], start_2026: date, end_2026: date) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(Decimal)
    counts: dict[str, int] = defaultdict(int)
    for product in products:
        category = (product.categoria or _family(product) or "Sin categoria").strip()
        totals[category] += _point_sales_qty(product, None, start_2026, end_2026) / Decimal(LOOKBACK_DAYS)
        counts[category] += 1
    return {category: (total / Decimal(counts[category])) for category, total in totals.items() if counts[category]}


def _product_alerts(rows: list[dict[str, Any]], category_avg_rotation: dict[str, Decimal]) -> dict[int, str | None]:
    by_product: dict[int, dict[str, Any]] = defaultdict(lambda: {"forecast": ZERO, "real": ZERO, "rotacion": ZERO, "count": 0, "category": ""})
    for row in rows:
        bucket = by_product[row["product"].id]
        bucket["forecast"] += row["forecast_decimal"]
        bucket["real"] += row["real_2025"]
        bucket["rotacion"] += row["rotacion_2026"]
        bucket["count"] += 1
        bucket["category"] = row["product"].categoria or _family(row["product"])

    alerts: dict[int, str | None] = {}
    for product_id, values in by_product.items():
        forecast = values["forecast"]
        real = values["real"]
        avg_rot = values["rotacion"] / Decimal(values["count"] or 1)
        category_avg = category_avg_rotation.get(values["category"], ZERO)
        diff_abs = abs(forecast - real)
        diff_pct = (diff_abs / real * Decimal("100")) if real else Decimal("100") if forecast else ZERO
        alert = None
        if forecast < (real * Decimal("0.80")) and avg_rot >= Decimal("0.95"):
            alert = "probable_subestimacion"
        elif forecast > (real * Decimal("1.30")) and avg_rot <= Decimal("1.05"):
            alert = "probable_sobreestimacion"
        elif not real and category_avg and avg_rot > (category_avg * Decimal("1.20")):
            alert = "producto_nuevo_alta_rotacion"
        elif diff_abs > Decimal("50") or diff_pct > Decimal("40"):
            alert = "revisar_manual"
        alerts[product_id] = alert
    return alerts


def generate_event_forecast_v9(event: EventoVenta) -> list[dict[str, Any]]:
    products = _active_event_products(event)
    branches = _active_event_branches(event)
    event_dates = _event_dates(event)
    event_dates_2025 = _event_2025_window(event_dates)
    start_2026, end_2026, start_2025, end_2025 = _reference_window(event)
    family_products: dict[str, list[Receta]] = defaultdict(list)
    for product in products:
        family_products[_family(product)].append(product)

    branch_factors = {
        branch.id: _branch_factor(branch, products, start_2026, end_2026, start_2025, end_2025)
        for branch in branches
    }
    rows: list[dict[str, Any]] = []

    for branch in branches:
        factor_sucursal, factor_sucursal_cap = branch_factors[branch.id]
        for product in products:
            context = _rotation_context(
                product,
                branch,
                factor_sucursal,
                factor_sucursal_cap,
                start_2026,
                end_2026,
                start_2025,
                end_2025,
            )
            day_shares, real_2025_range = _product_day_shares(
                product=product,
                family_products=family_products[_family(product)],
                branch=branch,
                event_dates=event_dates,
            )

            bases_by_day: dict[date, Decimal] = {}
            real_by_day: dict[date, Decimal] = {}
            fallback_by_day: dict[date, int] = {}
            base_total = ZERO
            for forecast_date in event_dates:
                homologue_day = _same_day_previous_year(forecast_date)
                base = _point_sales_qty(product, branch, homologue_day, homologue_day)
                real_by_day[forecast_date] = base
                fallback_level = 0
                if not base:
                    base, fallback_level = _fallback_base(
                        product=product,
                        branch=branch,
                        target_day=homologue_day,
                        active_products=products,
                        rotacion_2026=context.rotacion_2026,
                        event_dates_2025=event_dates_2025,
                    )
                bases_by_day[forecast_date] = base
                fallback_by_day[forecast_date] = fallback_level
                base_total += base

            forecast_total = base_total * context.factor_rotacion * context.factor_sucursal * FACTOR_EVENTO_SUAVE
            for forecast_date in event_dates:
                share = day_shares[forecast_date] if len(event_dates) > 1 else ONE
                forecast_decimal = forecast_total * share
                forecast_v8 = _forecast_v8_total(event, product, branch, forecast_date)
                real_2025 = real_by_day[forecast_date]
                diff = forecast_decimal - forecast_v8
                diff_pct = (diff / forecast_v8 * Decimal("100")) if forecast_v8 else ZERO
                rows.append(
                    {
                        "event": event,
                        "branch": branch,
                        "product": product,
                        "forecast_date": forecast_date,
                        "base_demand": bases_by_day[forecast_date],
                        "forecast_decimal": forecast_decimal,
                        "conservative_forecast": _ceil_decimal(forecast_decimal * Decimal("0.90")),
                        "final_forecast": _ceil_decimal(forecast_decimal),
                        "aggressive_forecast": _ceil_decimal(forecast_decimal * Decimal("1.12")),
                        "event_uplift": context.factor_rotacion - ONE,
                        "trend_adjustment": context.factor_sucursal - ONE,
                        "real_2025": real_2025,
                        "real_2025_range": real_2025_range,
                        "rotacion_2026": context.rotacion_2026,
                        "rotacion_2025": context.rotacion_2025,
                        "factor_rotacion": context.factor_rotacion,
                        "factor_sucursal": context.factor_sucursal,
                        "factor_evento_suave": FACTOR_EVENTO_SUAVE,
                        "share_dia": share,
                        "fallback_level": fallback_by_day[forecast_date],
                        "forecast_v8": forecast_v8,
                        "forecast_v9": _ceil_decimal(forecast_decimal),
                        "diferencia": diff,
                        "diferencia_pct": diff_pct,
                        "cap_aplicado": context.factor_rotacion_cap_aplicado or context.factor_sucursal_cap_aplicado,
                    }
                )

    product_totals: dict[int, Decimal] = defaultdict(Decimal)
    for row in rows:
        product_totals[row["product"].id] += row["forecast_decimal"]
    top_product_ids = {product_id for product_id, _qty in sorted(product_totals.items(), key=lambda item: item[1], reverse=True)[:20]}

    category_avg = _category_average_rotation(products, start_2026, end_2026)
    alerts = _product_alerts(rows, category_avg)
    for row in rows:
        if row["product"].id in top_product_ids:
            row["aggressive_forecast"] = _ceil_decimal(row["forecast_decimal"] * Decimal("1.15"))
        row["alerta"] = alerts.get(row["product"].id)
        row["explanation_json"] = {
            "base_2025": float(row["base_demand"]),
            "factor_rotacion": float(row["factor_rotacion"]),
            "factor_sucursal": float(row["factor_sucursal"]),
            "factor_evento_suave": float(row["factor_evento_suave"]),
            "share_dia": float(row["share_dia"]),
            "fallback_level": row["fallback_level"],
            "alerta": row["alerta"],
            "rotacion_2026": float(row["rotacion_2026"]),
            "rotacion_2025": float(row["rotacion_2025"]),
            "cap_aplicado": bool(row["cap_aplicado"]),
            "real_2025": float(row["real_2025"]),
            "real_2025_rango_producto_sucursal": float(row["real_2025_range"]),
            "forecast_v8": float(row["forecast_v8"]),
            "forecast_v9": float(row["forecast_v9"]),
            "diferencia": float(row["diferencia"]),
            "diferencia_pct": float(row["diferencia_pct"]),
        }
    return rows


def summarize_v9_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    products = {row["product"].id for row in rows}
    fallback_counts = Counter(row["fallback_level"] for row in rows)
    alert_counts = Counter(row["alerta"] for row in rows if row.get("alerta"))
    total_units = sum((row["final_forecast"] for row in rows), ZERO)
    by_family: dict[str, dict[str, Decimal]] = defaultdict(lambda: {"v8": ZERO, "v9": ZERO})
    for row in rows:
        family = _family(row["product"])
        by_family[family]["v8"] += row["forecast_v8"]
        by_family[family]["v9"] += row["final_forecast"]
    return {
        "products_processed": len(products),
        "fallback_counts": dict(sorted(fallback_counts.items())),
        "alert_counts": dict(sorted(alert_counts.items())),
        "total_units": total_units,
        "family_comparison": dict(sorted(by_family.items())),
    }
