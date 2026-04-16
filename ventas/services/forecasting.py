from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache
import calendar
import re
import unicodedata

from django.db import transaction
from django.db.models import Avg, Min, Q, Sum
from django.utils import timezone

from core.branch_catalog import EXCLUDED_BRANCH_CODES, POINT_MATURE_BRANCH_CODES, POINT_NETWORK_BRANCH_CODES
from core.models import Sucursal
from pos_bridge.models import PointDailyBranchIndicator, PointDailySale, PointSalesDailyCategoryFact, PointSalesDailyProductFact
from recetas.models import InventarioCedisProducto, Receta, VentaHistorica
from recetas.utils.commercial_composition import RULE_BLOQUEADO_POR_AMBIGUEDAD, classify_commercial_recipe, get_legacy_history_spec
from reportes.models import FactVentaDiaria
from ventas.models import (
    EventoVenta,
    EventoVentaForecast,
    EventoVentaNotification,
    EventoVentaProducto,
    EventoVentaSubstitutionWeight,
    EventoVentaSucursal,
    VentaAutoritativaPoint,
)
from ventas.services.notifications import create_unique_notification
from ventas.services.sales_read_service import get_daily_sales
from ventas.services.sales_truth import authoritative_daily_total, authoritative_day_loaded, recipe_point_codes
from ventas.services.substitution_learning import preload_learned_substitution_weights, resolve_learned_substitution_weight


@dataclass
class ForecastInputs:
    event: EventoVenta
    branches: list[Sucursal]
    products: list[Receta]
    branch_comparables: dict[int, Sucursal]
    blocked_products: list[Receta] = field(default_factory=list)
    excluded_products: list[tuple[Receta, str]] = field(default_factory=list)


@dataclass
class ForecastRuntimeCache:
    daily_sales: dict[tuple[int, int, date], tuple[str, Decimal]] = field(default_factory=dict)
    branch_daily_totals: dict[tuple[int, date], Decimal] = field(default_factory=dict)
    branch_window_totals: dict[tuple[int, date, int, int], Decimal] = field(default_factory=dict)
    branch_indicator_totals: dict[tuple[int, date, int], Decimal] = field(default_factory=dict)
    point_daily_series: dict[tuple[int, int | None, date, date], list[Decimal]] = field(default_factory=dict)
    historical_series: dict[tuple[int, int | None, date, date], list[Decimal]] = field(default_factory=dict)
    daily_series: dict[tuple[int, int | None, date, date], list[Decimal]] = field(default_factory=dict)
    daily_series_prefer_point: dict[tuple[int, int | None, date, date], list[Decimal]] = field(default_factory=dict)
    weekday_series: dict[tuple[int, int | None, date, int], list[Decimal]] = field(default_factory=dict)
    recent_window_series: dict[tuple[int, int | None, date, int, int], list[Decimal]] = field(default_factory=dict)
    recent_sales_velocity: dict[tuple[int, int | None, date], Decimal] = field(default_factory=dict)
    recent_product_share_shift: dict[tuple[int, int, date], Decimal] = field(default_factory=dict)
    portfolio_share_shift: dict[tuple[int, int | None, date], Decimal] = field(default_factory=dict)
    category_trend_signal: dict[tuple[str, str, int | None, date], Decimal] = field(default_factory=dict)
    branch_growth_signal: dict[tuple[int, date], Decimal] = field(default_factory=dict)
    comparable_branch_resolution: dict[tuple[int, int | None, date], tuple[Sucursal | None, Decimal]] = field(default_factory=dict)
    uplift_factor: dict[tuple[int, int, int | None], Decimal] = field(default_factory=dict)
    event_historical_anchor: dict[tuple[int, int, int | None, date], tuple[Decimal, str]] = field(default_factory=dict)
    fallback_categoria_avg: dict[tuple[str, int | None, date], Decimal] = field(default_factory=dict)
    product_group_window_totals: dict[tuple[tuple[int, ...], int | None, date, int, int], Decimal] = field(default_factory=dict)
    family_category_groups: dict[tuple[str, str], tuple[int, ...]] = field(default_factory=dict)
    family_groups: dict[str, tuple[int, ...]] = field(default_factory=dict)
    stock_policy_caps: dict[tuple[int, int], Decimal] = field(default_factory=dict)
    cedis_available: dict[int, Decimal] = field(default_factory=dict)
    ytd_recipe_factor: dict[tuple[int, int | None, date], Decimal] = field(default_factory=dict)
    same_period_baseline: dict[tuple[int, int | None, date], Decimal] = field(default_factory=dict)
    recent_series_stddev: dict[tuple[int, int | None, date], Decimal] = field(default_factory=dict)


ZERO = Decimal("0")
ONE = Decimal("1")
EVENT_EXECUTIVE_EXCLUDED_FAMILY_KEYS = {
    "accesorios",
    "bebidas",
}


def _ascii_norm(value: str) -> str:
    raw = (value or "").strip().lower()
    return "".join(ch for ch in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(ch))


def _infer_projection_labels(*, product_name: str, family: str, category: str) -> tuple[str, str]:
    name_norm = _ascii_norm(product_name)
    family_clean = (family or "").strip()
    category_clean = (category or "").strip()

    if not family_clean:
        if "bollo" in name_norm:
            family_clean = "Bollo"
        elif "pay" in name_norm:
            family_clean = "Pay"
        elif "cheesecake" in name_norm:
            family_clean = "Cheesecakes"
        elif "vaso" in name_norm:
            family_clean = "Vasos Preparados"
        elif "galleta" in name_norm:
            family_clean = "Galletas"
        elif "pastel" in name_norm:
            family_clean = "Pastel"

    if not category_clean:
        if "rebanada" in name_norm or name_norm.endswith(" r") or " reb" in name_norm:
            category_clean = "Rebanada"
        elif "individual" in name_norm:
            category_clean = "Individual"
        elif "mini" in name_norm:
            category_clean = "Pastel Mini" if family_clean == "Pastel" else "Mini"
        elif family_clean == "Bollo":
            category_clean = "Bollo"
        elif family_clean == "Vasos Preparados":
            if "grande" in name_norm:
                category_clean = "Vasos Grande"
            elif "mediano" in name_norm:
                category_clean = "Vasos Mediano"
            elif "chico" in name_norm:
                category_clean = "Vasos Chico"
            else:
                category_clean = "Vasos Mediano"
        elif family_clean == "Pay":
            if "grande" in name_norm:
                category_clean = "Pay Grande"
            elif "mediano" in name_norm:
                category_clean = "Pay Mediano"
            else:
                category_clean = "Rebanada" if "rebanada" in name_norm else "Pay Mediano"
        elif family_clean == "Cheesecakes":
            category_clean = "Individual"
    else:
        category_norm = _ascii_norm(category_clean)
        if family_clean == "Pastel":
            if category_norm == "chico":
                category_clean = "Pastel Chico"
            elif category_norm == "mediano":
                category_clean = "Pastel Mediano"
            elif category_norm == "grande":
                category_clean = "Pastel Grande"
            elif category_norm == "mini":
                category_clean = "Pastel Mini"
        elif family_clean == "Pay":
            if category_norm == "mediano":
                category_clean = "Pay Mediano"
            elif category_norm == "grande":
                category_clean = "Pay Grande"

    return family_clean or "Sin familia", category_clean or "Sin categoria"


def executive_event_product_scope(product: Receta) -> tuple[bool, str]:
    family, _category = _infer_projection_labels(
        product_name=product.nombre or "",
        family=getattr(product, "familia", "") or "",
        category=getattr(product, "categoria", "") or "",
    )
    if getattr(product, "modo_costeo", "") == Receta.MODO_COSTEO_SERVICIO:
        return False, "modo_costeo_servicio_accesorio"
    if _ascii_norm(family) in EVENT_EXECUTIVE_EXCLUDED_FAMILY_KEYS:
        return False, f"familia_excluida:{family}"
    return True, "eligible"


def _date_range(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _replace_year_safe(value: date, year: int) -> date:
    day = min(value.day, calendar.monthrange(year, value.month)[1])
    return value.replace(year=year, day=day)


def _event_executive_benchmark_sales(event: EventoVenta) -> Decimal:
    notes = (event.objective_notes or "").strip()
    if not notes:
        return ZERO
    for line in notes.splitlines():
        normalized = _ascii_norm(line)
        if "benchmark" not in normalized or "dg" not in normalized:
            continue
        if "dia principal" in normalized or "dia fuerte" in normalized or "main day" in normalized:
            continue
        match = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", line)
        if not match:
            continue
        raw = match.group(1).replace(",", "").strip()
        try:
            return Decimal(raw).quantize(Decimal("0.01"))
        except Exception:
            continue
    return ZERO


def _event_executive_main_day_benchmark_sales(event: EventoVenta) -> Decimal:
    notes = (event.objective_notes or "").strip()
    if not notes:
        return ZERO
    for line in notes.splitlines():
        normalized = _ascii_norm(line)
        if "benchmark" not in normalized or "dg" not in normalized:
            continue
        if "dia principal" not in normalized and "dia fuerte" not in normalized and "main day" not in normalized:
            continue
        match = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", line)
        if not match:
            continue
        raw = match.group(1).replace(",", "").strip()
        try:
            return Decimal(raw).quantize(Decimal("0.01"))
        except Exception:
            continue
    return ZERO


def _event_growth_anchor_factor(model: dict[str, object]) -> Decimal:
    same_store_factor = Decimal(str(model.get("same_store_factor") or ONE))
    expansion_factor = Decimal(str(model.get("expansion_factor") or ZERO))
    contraction_factor = Decimal(str(model.get("contraction_factor") or ONE))
    base_factor = same_store_factor + expansion_factor
    if base_factor <= ZERO:
        base_factor = same_store_factor if same_store_factor > ZERO else ONE
    return (base_factor * contraction_factor).quantize(Decimal("0.0001"))


def _observed_anchor(anchor: date) -> date:
    """
    Forecasts for future dates must use the last available observed date,
    not the event date itself, otherwise recent-signal windows fall into
    dates that do not exist yet and comparable-branch logic collapses.
    """
    return min(anchor, timezone.localdate())


def _weighted_avg(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    total_weight = Decimal("0")
    weighted_sum = Decimal("0")
    for idx, val in enumerate(values, start=1):
        weight = Decimal(idx)
        total_weight += weight
        weighted_sum += Decimal(val) * weight
    return weighted_sum / total_weight if total_weight else Decimal("0")


def _avg(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    return sum(values, ZERO) / Decimal(len(values))


def _stddev(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    mean_value = _avg(values)
    variance = sum((value - mean_value) ** 2 for value in values) / Decimal(len(values))
    if variance <= ZERO:
        return ZERO
    return Decimal(str(float(variance) ** 0.5))


def _clamp(value: Decimal, *, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(high, value))


def _series_cache_key(receta: Receta, sucursal: Sucursal | None, start: date, end: date) -> tuple[int, int | None, date, date]:
    return (int(receta.id), int(sucursal.id) if sucursal else None, start, end)


def _select_source_value(
    bucket: dict[tuple[int, int, date], dict[str, Decimal]],
    key: tuple[int, int, date],
    priorities: tuple[str, ...],
) -> Decimal:
    source_map = bucket.get(key)
    if not source_map:
        return ZERO
    for source in priorities:
        if source in source_map:
            return source_map[source]
    for value in source_map.values():
        return value
    return ZERO


def _required_forecast_dates(event: EventoVenta, dates: list[date]) -> set[date]:
    required = set(dates)
    observed_anchors = {_observed_anchor(day) for day in dates}
    for day in dates:
        for lag in range(1, 9):
            required.add(day - timedelta(days=7 * lag))
        required.add(_replace_year_safe(day, day.year - 1))

    last_year_start = _replace_year_safe(event.analysis_start_date, event.analysis_start_date.year - 1)
    last_year_end = _replace_year_safe(event.analysis_end_date, event.analysis_end_date.year - 1)
    required.update(_date_range(last_year_start, last_year_end))
    baseline_start = last_year_start - timedelta(days=28)
    baseline_end = last_year_start - timedelta(days=7)
    required.update(_date_range(baseline_start, baseline_end))

    for observed_anchor in observed_anchors:
        recent_start = observed_anchor - timedelta(days=42)
        recent_end = observed_anchor - timedelta(days=1)
        if recent_end >= recent_start:
            required.update(_date_range(recent_start, recent_end))
    return required


def _populate_fact_sales_cache(
    *,
    cache: ForecastRuntimeCache,
    branch_ids: list[int],
    product_ids: list[int],
    required_dates: set[date],
) -> set[date]:
    if not branch_ids or not product_ids or not required_dates:
        return set()
    clean_days = {
        day
        for day in required_dates
        if FactVentaDiaria.objects.filter(fecha=day).exists()
    }
    if not clean_days:
        return set()

    product_bucket: dict[tuple[int, int, date], dict[str, Decimal]] = {}
    fact_rows = (
        FactVentaDiaria.objects.filter(
            fecha__in=clean_days,
            sucursal_id__in=branch_ids,
            receta_id__in=product_ids,
        )
        .values("fecha", "sucursal_id", "receta_id", "source_kind")
        .annotate(qty=Sum("cantidad"))
    )
    fact_priority = ("AUTHORITATIVE", "V2_FACT", "LEGACY")
    for row in fact_rows:
        key = (int(row["sucursal_id"]), int(row["receta_id"]), row["fecha"])
        product_bucket.setdefault(key, {})[str(row["source_kind"])] = Decimal(str(row["qty"] or 0))
    for key in product_bucket:
        cache.daily_sales[key] = ("fact", _select_source_value(product_bucket, key, fact_priority))

    branch_bucket: dict[tuple[int, date], dict[str, Decimal]] = {}
    branch_rows = (
        FactVentaDiaria.objects.filter(
            fecha__in=clean_days,
            sucursal_id__in=branch_ids,
        )
        .values("fecha", "sucursal_id", "source_kind")
        .annotate(qty=Sum("cantidad"))
    )
    for row in branch_rows:
        key = (int(row["sucursal_id"]), row["fecha"])
        branch_bucket.setdefault(key, {})[str(row["source_kind"])] = Decimal(str(row["qty"] or 0))
    for key in branch_bucket:
        cache.branch_daily_totals[key] = _select_source_value(
            {(key[0], 0, key[1]): branch_bucket[key]},
            (key[0], 0, key[1]),
            fact_priority,
        )
    return clean_days


def _populate_authoritative_sales_cache(
    *,
    cache: ForecastRuntimeCache,
    branch_ids: list[int],
    product_ids: list[int],
    required_dates: set[date],
    skip_days: set[date],
) -> None:
    target_days = required_dates
    if not branch_ids or not product_ids or not target_days:
        return

    rows = (
        VentaAutoritativaPoint.objects.filter(
            sale_date__in=target_days,
            branch_id__in=branch_ids,
            product_id__in=product_ids,
        )
        .values("sale_date", "branch_id", "product_id")
        .annotate(qty=Sum("quantity"))
    )
    for row in rows:
        key = (int(row["branch_id"]), int(row["product_id"]), row["sale_date"])
        cache.daily_sales.setdefault(key, ("authoritative", Decimal(str(row["qty"] or 0))))

    branch_rows = (
        VentaAutoritativaPoint.objects.filter(
            sale_date__in=target_days,
            branch_id__in=branch_ids,
        )
        .values("sale_date", "branch_id")
        .annotate(qty=Sum("quantity"))
    )
    for row in branch_rows:
        key = (int(row["branch_id"]), row["sale_date"])
        cache.branch_daily_totals.setdefault(key, Decimal(str(row["qty"] or 0)))


def _populate_v2_sales_cache(
    *,
    cache: ForecastRuntimeCache,
    branch_ids: list[int],
    product_ids: list[int],
    required_dates: set[date],
    skip_days: set[date],
) -> None:
    target_days = required_dates
    if not branch_ids or not product_ids or not target_days:
        return

    rows = (
        PointSalesDailyProductFact.objects.filter(
            sale_date__in=target_days,
            branch__erp_branch_id__in=branch_ids,
            receta_id__in=product_ids,
        )
        .values("sale_date", "branch__erp_branch_id", "receta_id")
        .annotate(qty=Sum("total_cantidad"))
    )
    for row in rows:
        key = (int(row["branch__erp_branch_id"]), int(row["receta_id"]), row["sale_date"])
        cache.daily_sales.setdefault(key, ("v2_fact", Decimal(str(row["qty"] or 0))))

    branch_rows = (
        PointSalesDailyCategoryFact.objects.filter(
            sale_date__in=target_days,
            branch__erp_branch_id__in=branch_ids,
        )
        .values("sale_date", "branch__erp_branch_id")
        .annotate(qty=Sum("total_cantidad"))
    )
    for row in branch_rows:
        key = (int(row["branch__erp_branch_id"]), row["sale_date"])
        cache.branch_daily_totals.setdefault(key, Decimal(str(row["qty"] or 0)))


def _populate_legacy_sales_cache(
    *,
    cache: ForecastRuntimeCache,
    branch_ids: list[int],
    product_ids: list[int],
    required_dates: set[date],
    skip_days: set[date],
) -> None:
    target_days = required_dates
    if not branch_ids or not product_ids or not target_days:
        return

    code_to_recipe: dict[str, int] = {}
    for product_id in product_ids:
        for code in recipe_point_codes(product_id):
            normalized = (code or "").strip()
            if not normalized:
                continue
            if normalized in code_to_recipe and code_to_recipe[normalized] != product_id:
                code_to_recipe.pop(normalized, None)
                continue
            code_to_recipe[normalized] = product_id

    product_filters = Q(receta_id__in=product_ids)
    if code_to_recipe:
        product_filters |= Q(product__sku__in=list(code_to_recipe.keys()))

    endpoint_priority = (
        "/Report/PrintReportes?idreporte=3",
        "/Report/VentasCategorias",
    )
    bucket: dict[tuple[int, int, date], dict[str, Decimal]] = {}
    rows = (
        PointDailySale.objects.filter(
            sale_date__in=target_days,
            branch__erp_branch_id__in=branch_ids,
        )
        .filter(product_filters)
        .values("sale_date", "branch__erp_branch_id", "receta_id", "product__sku", "source_endpoint")
        .annotate(qty=Sum("quantity"))
    )
    for row in rows:
        recipe_id = row["receta_id"]
        if recipe_id is None:
            recipe_id = code_to_recipe.get((row.get("product__sku") or "").strip())
        if not recipe_id:
            continue
        key = (int(row["branch__erp_branch_id"]), int(recipe_id), row["sale_date"])
        bucket.setdefault(key, {})[str(row.get("source_endpoint") or "")] = (
            bucket.setdefault(key, {}).get(str(row.get("source_endpoint") or ""), ZERO)
            + Decimal(str(row["qty"] or 0))
        )
    for key in bucket:
        cache.daily_sales.setdefault(key, ("legacy", _select_source_value(bucket, key, endpoint_priority)))

    branch_bucket: dict[tuple[int, date], dict[str, Decimal]] = {}
    branch_rows = (
        PointDailySale.objects.filter(
            sale_date__in=target_days,
            branch__erp_branch_id__in=branch_ids,
        )
        .values("sale_date", "branch__erp_branch_id", "source_endpoint")
        .annotate(qty=Sum("quantity"))
    )
    for row in branch_rows:
        key = (int(row["branch__erp_branch_id"]), row["sale_date"])
        endpoint = str(row.get("source_endpoint") or "")
        branch_bucket.setdefault(key, {})[endpoint] = Decimal(str(row["qty"] or 0))
    for key in branch_bucket:
        cache.branch_daily_totals.setdefault(
            key,
            _select_source_value({(key[0], 0, key[1]): branch_bucket[key]}, (key[0], 0, key[1]), endpoint_priority),
        )


def _prime_forecast_runtime_cache(
    *,
    event: EventoVenta,
    inputs: ForecastInputs,
    dates: list[date],
    cache: ForecastRuntimeCache,
) -> None:
    branch_ids = [int(branch.id) for branch in inputs.branches]
    product_ids = [int(product.id) for product in inputs.products]
    required_dates = _required_forecast_dates(event, dates)
    clean_days = _populate_fact_sales_cache(
        cache=cache,
        branch_ids=branch_ids,
        product_ids=product_ids,
        required_dates=required_dates,
    )
    _populate_authoritative_sales_cache(
        cache=cache,
        branch_ids=branch_ids,
        product_ids=product_ids,
        required_dates=required_dates,
        skip_days=clean_days,
    )
    _populate_v2_sales_cache(
        cache=cache,
        branch_ids=branch_ids,
        product_ids=product_ids,
        required_dates=required_dates,
        skip_days=clean_days,
    )
    _populate_legacy_sales_cache(
        cache=cache,
        branch_ids=branch_ids,
        product_ids=product_ids,
        required_dates=required_dates,
        skip_days=clean_days,
    )
    for branch_id in branch_ids:
        for day in required_dates:
            cache.branch_daily_totals.setdefault((branch_id, day), ZERO)
            for product_id in product_ids:
                cache.daily_sales.setdefault((branch_id, product_id, day), ("none", ZERO))


def _daily_sale_quantity(
    receta: Receta,
    sucursal: Sucursal,
    target_day: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> tuple[str, Decimal]:
    cache_key = (int(sucursal.id), int(receta.id), target_day)
    if cache and cache_key in cache.daily_sales:
        return cache.daily_sales[cache_key]

    day_sales = get_daily_sales(sucursal=sucursal, fecha=target_day, producto=receta)
    payload = (str(day_sales["source"] or "none"), Decimal(str(day_sales["cantidad"] or 0)))
    if cache:
        cache.daily_sales[cache_key] = payload
    return payload


@lru_cache(maxsize=256)
def _recipe_by_name(name: str) -> Receta | None:
    return Receta.objects.filter(nombre=name).first()


@lru_cache(maxsize=256)
def _recipe_by_code(code: str) -> Receta | None:
    return Receta.objects.filter(codigo_point__iexact=(code or "").strip()).first()


@lru_cache(maxsize=1024)
def _first_point_sale_for_recipe(recipe_id: int) -> date | None:
    authoritative_first = (
        VentaAutoritativaPoint.objects.filter(product_id=recipe_id)
        .aggregate(first=Min("sale_date"))
        .get("first")
    )
    if authoritative_first:
        return authoritative_first

    rebuilt_first = (
        PointSalesDailyProductFact.objects.filter(receta_id=recipe_id)
        .aggregate(first=Min("sale_date"))
        .get("first")
    )
    if rebuilt_first:
        return rebuilt_first

    official_first = (
        PointDailySale.objects.filter(
            receta_id=recipe_id,
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )
        .aggregate(first=Min("sale_date"))
        .get("first")
    )
    if official_first:
        return official_first

    return (
        PointDailySale.objects.filter(receta_id=recipe_id)
        .aggregate(first=Min("sale_date"))
        .get("first")
    )


def _legacy_pay_recipe_segments(receta: Receta, start: date, end: date) -> list[tuple[Receta, date, date]]:
    legacy_spec = get_legacy_history_spec(receta)
    if not legacy_spec:
        return []
    current_start = _first_point_sale_for_recipe(receta.id)
    legacy_recipe = None
    legacy_code = (legacy_spec.legacy_code or "").strip()
    legacy_name = (legacy_spec.legacy_name or "").strip()
    if legacy_code:
        legacy_recipe = _recipe_by_code(legacy_code)
    if legacy_recipe is None and legacy_name:
        legacy_recipe = _recipe_by_name(legacy_name)
    if not current_start or not legacy_recipe:
        return []
    legacy_end = min(end, current_start - timedelta(days=1))
    if legacy_end < start:
        return []
    return [(legacy_recipe, start, legacy_end)]


def _load_historial(
    receta: Receta,
    sucursal: Sucursal | None,
    start: date,
    end: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> list[Decimal]:
    cache_key = _series_cache_key(receta, sucursal, start, end)
    if cache and cache_key in cache.historical_series:
        return list(cache.historical_series[cache_key])
    if start > timezone.localdate():
        if cache:
            cache.historical_series[cache_key] = []
        return []

    qs = VentaHistorica.objects.filter(receta=receta, fecha__range=(start, end))
    if sucursal:
        qs = qs.filter(sucursal=sucursal)
    values = [Decimal(str(x["cantidad"])) for x in qs.values("cantidad")]
    for legacy_recipe, legacy_start, legacy_end in _legacy_pay_recipe_segments(receta, start, end):
        legacy_qs = VentaHistorica.objects.filter(receta=legacy_recipe, fecha__range=(legacy_start, legacy_end))
        if sucursal:
            legacy_qs = legacy_qs.filter(sucursal=sucursal)
        values.extend(Decimal(str(x["cantidad"])) for x in legacy_qs.values("cantidad"))
    if cache:
        cache.historical_series[cache_key] = list(values)
    return values


def _load_point_daily(
    receta: Receta,
    sucursal: Sucursal | None,
    start: date,
    end: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> list[Decimal]:
    cache_key = _series_cache_key(receta, sucursal, start, end)
    if cache and cache_key in cache.point_daily_series:
        return list(cache.point_daily_series[cache_key])
    if start > timezone.localdate():
        if cache:
            cache.point_daily_series[cache_key] = []
        return []

    values: list[Decimal] = []
    if sucursal:
        for target_day in _date_range(start, end):
            source, quantity = _daily_sale_quantity(receta, sucursal, target_day, cache=cache)
            if source == "none":
                continue
            values.append(quantity)
    for legacy_recipe, legacy_start, legacy_end in _legacy_pay_recipe_segments(receta, start, end):
        values.extend(_load_point_daily(legacy_recipe, sucursal, legacy_start, legacy_end, cache=cache))
    if cache:
        cache.point_daily_series[cache_key] = list(values)
    return values


def _daily_series(
    receta: Receta,
    sucursal: Sucursal | None,
    start: date,
    end: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> list[Decimal]:
    cache_key = _series_cache_key(receta, sucursal, start, end)
    if cache and cache_key in cache.daily_series:
        return list(cache.daily_series[cache_key])
    if start > timezone.localdate():
        if cache:
            cache.daily_series[cache_key] = []
        return []

    values = _load_point_daily(receta, sucursal, start, end, cache=cache)
    if values:
        if cache:
            cache.daily_series[cache_key] = list(values)
        return values
    branch_id = sucursal.id if sucursal else None
    authoritative_values: list[Decimal] = []
    for target_day in _date_range(start, end):
        if authoritative_day_loaded(branch_id, target_day):
            qty = authoritative_daily_total(receta.id, branch_id, target_day)
            authoritative_values.append(qty)
    if authoritative_values:
        return authoritative_values
    values = _load_historial(receta, sucursal, start, end, cache=cache)
    if cache:
        cache.daily_series[cache_key] = list(values)
    return values


def _daily_series_prefer_point(
    receta: Receta,
    sucursal: Sucursal | None,
    start: date,
    end: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> list[Decimal]:
    cache_key = _series_cache_key(receta, sucursal, start, end)
    if cache and cache_key in cache.daily_series_prefer_point:
        return list(cache.daily_series_prefer_point[cache_key])
    if start > timezone.localdate():
        if cache:
            cache.daily_series_prefer_point[cache_key] = []
        return []

    values = _load_point_daily(receta, sucursal, start, end, cache=cache)
    if values:
        if cache:
            cache.daily_series_prefer_point[cache_key] = list(values)
        return values
    values = _load_historial(receta, sucursal, start, end, cache=cache)
    if cache:
        cache.daily_series_prefer_point[cache_key] = list(values)
    return values


def _weekday_series(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    weeks: int = 8,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> list[Decimal]:
    cache_key = (int(receta.id), int(sucursal.id) if sucursal else None, anchor, weeks)
    if cache and cache_key in cache.weekday_series:
        return list(cache.weekday_series[cache_key])

    values: list[Decimal] = []
    observed_anchor = _observed_anchor(anchor)
    target = observed_anchor - timedelta(days=1)
    for _ in range(weeks):
        while target.weekday() != anchor.weekday():
            target -= timedelta(days=1)
        values.extend(_daily_series(receta, sucursal, target, target, cache=cache))
        target -= timedelta(days=7)
    if cache:
        cache.weekday_series[cache_key] = list(values)
    return values


def _recent_window_series(
    receta: Receta,
    sucursal: Sucursal | None,
    *,
    anchor: date,
    days_back: int,
    skip_recent_days: int = 0,
    cache: ForecastRuntimeCache | None = None,
) -> list[Decimal]:
    cache_key = (int(receta.id), int(sucursal.id) if sucursal else None, anchor, days_back, skip_recent_days)
    if cache and cache_key in cache.recent_window_series:
        return list(cache.recent_window_series[cache_key])

    observed_anchor = _observed_anchor(anchor)
    end = observed_anchor - timedelta(days=1 + skip_recent_days)
    start = end - timedelta(days=days_back - 1)
    if end < start:
        return []
    values = _daily_series(receta, sucursal, start, end, cache=cache)
    if cache:
        cache.recent_window_series[cache_key] = list(values)
    return values


def _window_total(series: list[Decimal]) -> Decimal:
    return sum(series, ZERO)


def _window_avg(series: list[Decimal], expected_days: int) -> Decimal:
    if expected_days <= 0:
        return ZERO
    return _window_total(series) / Decimal(expected_days)


def _family_category_key(receta: Receta) -> tuple[str, str]:
    return (
        (receta.familia or "").strip() or "SIN_FAMILIA",
        (receta.categoria or "").strip() or "SIN_CATEGORIA",
    )


def _family_key(receta: Receta) -> str:
    return (receta.familia or "").strip() or "SIN_FAMILIA"


def _prime_product_group_maps(*, cache: ForecastRuntimeCache, products: list[Receta]) -> None:
    family_category_groups: defaultdict[tuple[str, str], list[int]] = defaultdict(list)
    family_groups: defaultdict[str, list[int]] = defaultdict(list)
    for product in products:
        family_category_groups[_family_category_key(product)].append(int(product.id))
        family_groups[_family_key(product)].append(int(product.id))
    cache.family_category_groups = {
        key: tuple(sorted(product_ids))
        for key, product_ids in family_category_groups.items()
    }
    cache.family_groups = {
        key: tuple(sorted(product_ids))
        for key, product_ids in family_groups.items()
    }


def _competitive_group_spec(receta: Receta, *, cache: ForecastRuntimeCache | None = None) -> tuple[str, tuple[int, ...], str]:
    family, category = _family_category_key(receta)
    family_category_ids = (cache.family_category_groups.get((family, category)) if cache else None) or ()
    if len(family_category_ids) >= 2:
        return (f"familia_categoria::{family}::{category}", family_category_ids, "familia_categoria")

    family_ids = (cache.family_groups.get(_family_key(receta)) if cache else None) or ()
    if len(family_ids) >= 2:
        return (f"familia::{family}", family_ids, "familia")

    return (f"producto::{int(receta.id)}", (int(receta.id),), "producto")


def _pct_delta(current: Decimal, previous: Decimal) -> Decimal:
    if previous <= 0:
        return ZERO
    return (current - previous) / previous


def _nonzero_days(series: list[Decimal]) -> int:
    return sum(1 for value in series if value > ZERO)


def _croston_estimate(series: list[Decimal], *, alpha: Decimal = Decimal("0.20")) -> Decimal:
    nonzero_positions = [(idx + 1, value) for idx, value in enumerate(series) if value > ZERO]
    if not nonzero_positions:
        return ZERO
    z_hat = Decimal(str(nonzero_positions[0][1]))
    p_hat = Decimal(str(nonzero_positions[0][0]))
    previous_pos = nonzero_positions[0][0]
    for position, value in nonzero_positions[1:]:
        interval = Decimal(str(position - previous_pos))
        z_hat = (alpha * Decimal(str(value))) + ((ONE - alpha) * z_hat)
        p_hat = (alpha * interval) + ((ONE - alpha) * p_hat)
        previous_pos = position
    if p_hat <= ZERO:
        return ZERO
    return z_hat / p_hat


def _same_period_baseline(
    receta: Receta,
    sucursal: Sucursal | None,
    target_day: date,
    *,
    cache: ForecastRuntimeCache | None = None,
    years_back: int = 3,
    window_radius: int = 1,
) -> Decimal:
    cache_key = (int(receta.id), int(sucursal.id) if sucursal else None, target_day)
    if cache and cache_key in cache.same_period_baseline:
        return cache.same_period_baseline[cache_key]

    observations: list[Decimal] = []
    for years in range(1, years_back + 1):
        anchor = _replace_year_safe(target_day, target_day.year - years)
        start = anchor - timedelta(days=window_radius)
        end = anchor + timedelta(days=window_radius)
        values = _daily_series_prefer_point(receta, sucursal, start, end, cache=cache)
        if values:
            observations.append(_avg(values))
    value = _avg(observations)
    if cache:
        cache.same_period_baseline[cache_key] = value
    return value


def _ytd_recipe_factor(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (int(receta.id), int(sucursal.id) if sucursal else None, anchor)
    if cache and cache_key in cache.ytd_recipe_factor:
        return cache.ytd_recipe_factor[cache_key]

    observed_anchor = _observed_anchor(anchor)
    if observed_anchor.month == 1 and observed_anchor.day < 10:
        value = ONE
        if cache:
            cache.ytd_recipe_factor[cache_key] = value
        return value
    current_start = date(observed_anchor.year, 1, 1)
    last_year_end = _replace_year_safe(observed_anchor, observed_anchor.year - 1)
    last_year_start = date(last_year_end.year, 1, 1)
    current_total = _window_total(_daily_series_prefer_point(receta, sucursal, current_start, observed_anchor, cache=cache))
    prior_total = _window_total(_daily_series_prefer_point(receta, sucursal, last_year_start, last_year_end, cache=cache))
    if prior_total <= ZERO:
        value = ONE
    else:
        raw_factor = current_total / prior_total
        value = _clamp(ONE + ((raw_factor - ONE) * Decimal("0.70")), low=Decimal("0.86"), high=Decimal("1.03"))
    if cache:
        cache.ytd_recipe_factor[cache_key] = value
    return value


def _recent_sales_velocity(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (int(receta.id), int(sucursal.id) if sucursal else None, anchor)
    if cache and cache_key in cache.recent_sales_velocity:
        return cache.recent_sales_velocity[cache_key]

    recent = _recent_window_series(receta, sucursal, anchor=anchor, days_back=14, cache=cache)
    prior = _recent_window_series(receta, sucursal, anchor=anchor, days_back=28, skip_recent_days=14, cache=cache)
    recent_avg = _window_avg(recent, 14)
    prior_avg = _window_avg(prior, 28)
    value = _clamp(_pct_delta(recent_avg, prior_avg), low=Decimal("-0.30"), high=Decimal("0.40"))
    if cache:
        cache.recent_sales_velocity[cache_key] = value
    return value


def _recent_product_share_shift(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    if not sucursal:
        return ZERO
    cache_key = (int(receta.id), int(sucursal.id), anchor)
    if cache and cache_key in cache.recent_product_share_shift:
        return cache.recent_product_share_shift[cache_key]

    recent_product = _window_total(_recent_window_series(receta, sucursal, anchor=anchor, days_back=14, cache=cache))
    prior_product = _window_total(
        _recent_window_series(receta, sucursal, anchor=anchor, days_back=28, skip_recent_days=14, cache=cache)
    )
    recent_branch_total = _recent_branch_quantity_total_cached(sucursal.id, anchor=anchor, days_back=14, cache=cache)
    prior_branch_total = _recent_branch_quantity_total_cached(
        sucursal.id,
        anchor=anchor,
        days_back=28,
        skip_recent_days=14,
        cache=cache,
    )
    if recent_branch_total <= 0 or prior_branch_total <= 0 or prior_product <= 0:
        return ZERO
    recent_share = recent_product / recent_branch_total
    prior_share = prior_product / prior_branch_total
    value = _clamp(_pct_delta(recent_share, prior_share), low=Decimal("-0.25"), high=Decimal("0.35"))
    if cache:
        cache.recent_product_share_shift[cache_key] = value
    return value


def _product_group_window_total(
    product_ids: tuple[int, ...],
    sucursal: Sucursal | None,
    anchor: date,
    *,
    days_back: int,
    skip_recent_days: int = 0,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (tuple(product_ids), int(sucursal.id) if sucursal else None, anchor, int(days_back), int(skip_recent_days))
    if cache and cache_key in cache.product_group_window_totals:
        return cache.product_group_window_totals[cache_key]

    observed_anchor = _observed_anchor(anchor)
    end = observed_anchor - timedelta(days=1 + skip_recent_days)
    start = end - timedelta(days=days_back - 1)
    if end < start:
        return ZERO

    total = ZERO
    if cache and sucursal and cache.daily_sales:
        for target_day in _date_range(start, end):
            for product_id in product_ids:
                total += cache.daily_sales.get((int(sucursal.id), int(product_id), target_day), ("none", ZERO))[1]
        cache.product_group_window_totals[cache_key] = total
        return total

    qs = VentaHistorica.objects.filter(receta_id__in=product_ids, fecha__range=(start, end))
    if sucursal:
        qs = qs.filter(sucursal=sucursal)
    total = Decimal(str(qs.aggregate(total=Sum("cantidad")).get("total") or 0))
    if cache:
        cache.product_group_window_totals[cache_key] = total
    return total


def _portfolio_peer_group_ids(receta: Receta, *, cache: ForecastRuntimeCache | None = None) -> tuple[int, ...]:
    family_category_ids = (cache.family_category_groups.get(_family_category_key(receta)) if cache else None) or ()
    if len(family_category_ids) > 1:
        return family_category_ids
    family_ids = (cache.family_groups.get(_family_key(receta)) if cache else None) or ()
    if len(family_ids) > 1:
        return family_ids
    return family_category_ids or family_ids or (int(receta.id),)


def _portfolio_share_shift(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (int(receta.id), int(sucursal.id) if sucursal else None, anchor)
    if cache and cache_key in cache.portfolio_share_shift:
        return cache.portfolio_share_shift[cache_key]

    peer_group_ids = _portfolio_peer_group_ids(receta, cache=cache)
    if len(peer_group_ids) <= 1:
        return ZERO

    recent_product = _window_total(_recent_window_series(receta, sucursal, anchor=anchor, days_back=14, cache=cache))
    prior_product = _window_total(
        _recent_window_series(receta, sucursal, anchor=anchor, days_back=28, skip_recent_days=14, cache=cache)
    )
    recent_group = _product_group_window_total(peer_group_ids, sucursal, anchor, days_back=14, cache=cache)
    prior_group = _product_group_window_total(peer_group_ids, sucursal, anchor, days_back=28, skip_recent_days=14, cache=cache)
    if recent_group <= 0 or prior_group <= 0 or prior_product <= 0:
        return ZERO

    recent_share = recent_product / recent_group
    prior_share = prior_product / prior_group
    value = _clamp(_pct_delta(recent_share, prior_share), low=Decimal("-0.35"), high=Decimal("0.45"))
    if cache:
        cache.portfolio_share_shift[cache_key] = value
    return value


def _category_trend_signal(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    family, category = _family_category_key(receta)
    cache_key = (family, category, int(sucursal.id) if sucursal else None, anchor)
    if cache and cache_key in cache.category_trend_signal:
        return cache.category_trend_signal[cache_key]

    group_ids = (cache.family_category_groups.get((family, category)) if cache else None) or (int(receta.id),)
    recent_total = _product_group_window_total(group_ids, sucursal, anchor, days_back=14, cache=cache)
    prior_total = _product_group_window_total(group_ids, sucursal, anchor, days_back=28, skip_recent_days=14, cache=cache)
    recent_avg = recent_total / Decimal("14")
    prior_avg = prior_total / Decimal("28")
    if prior_avg <= ZERO:
        return ZERO

    value = _clamp(_pct_delta(recent_avg, prior_avg), low=Decimal("-0.35"), high=Decimal("0.45"))
    if cache:
        cache.category_trend_signal[cache_key] = value
    return value


def _competitive_group_metrics(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    *,
    product_velocity: Decimal,
    ytd_factor: Decimal,
    cache: ForecastRuntimeCache | None = None,
) -> tuple[str, str, tuple[int, ...], Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
    group_key, group_ids, group_scope = _competitive_group_spec(receta, cache=cache)
    recent_product = _window_total(_recent_window_series(receta, sucursal, anchor=anchor, days_back=14, cache=cache))
    prior_product = _window_total(
        _recent_window_series(receta, sucursal, anchor=anchor, days_back=28, skip_recent_days=14, cache=cache)
    )
    recent_group = _product_group_window_total(group_ids, sucursal, anchor, days_back=14, cache=cache)
    prior_group = _product_group_window_total(group_ids, sucursal, anchor, days_back=28, skip_recent_days=14, cache=cache)
    share_recent = (recent_product / recent_group) if recent_group > ZERO else ZERO
    share_prior = (prior_product / prior_group) if prior_group > ZERO else ZERO
    share_delta = share_recent - share_prior if share_prior > ZERO else ZERO
    ytd_delta = ytd_factor - ONE
    preference_score = _clamp(
        (share_delta * Decimal("0.50")) + (product_velocity * Decimal("0.30")) + (ytd_delta * Decimal("0.20")),
        low=Decimal("-0.30"),
        high=Decimal("0.30"),
    )
    return (
        group_key,
        group_scope,
        group_ids,
        share_recent,
        share_prior,
        share_delta,
        ytd_delta,
        preference_score,
        recent_group,
        prior_group,
    )


def _uplift_factor(
    event: EventoVenta,
    receta: Receta,
    sucursal: Sucursal | None,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (int(event.id), int(receta.id), int(sucursal.id) if sucursal else None)
    if cache and cache_key in cache.uplift_factor:
        return cache.uplift_factor[cache_key]

    last_year_start = _replace_year_safe(event.analysis_start_date, event.analysis_start_date.year - 1)
    last_year_end = _replace_year_safe(event.analysis_end_date, event.analysis_end_date.year - 1)
    last_year_values = _daily_series_prefer_point(receta, sucursal, last_year_start, last_year_end, cache=cache)
    if not last_year_values:
        return Decimal("0")
    last_year_avg = sum(last_year_values) / Decimal(len(last_year_values))

    baseline_window_start = last_year_start - timedelta(days=28)
    baseline_window_end = last_year_start - timedelta(days=7)
    baseline_values = _daily_series_prefer_point(
        receta,
        sucursal,
        baseline_window_start,
        baseline_window_end,
        cache=cache,
    )
    if not baseline_values:
        return Decimal("0")
    baseline_avg = sum(baseline_values) / Decimal(len(baseline_values))
    if baseline_avg <= 0:
        return Decimal("0")
    raw_uplift = (last_year_avg - baseline_avg) / baseline_avg
    value = _clamp(raw_uplift, low=Decimal("-0.06"), high=Decimal("0.06"))
    if cache:
        cache.uplift_factor[cache_key] = value
    return value


def _event_historical_anchor(
    event: EventoVenta,
    receta: Receta,
    sucursal: Sucursal | None,
    day: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> tuple[Decimal, str]:
    cache_key = (int(event.id), int(receta.id), int(sucursal.id) if sucursal else None, day)
    if cache and cache_key in cache.event_historical_anchor:
        return cache.event_historical_anchor[cache_key]

    last_year_day = _replace_year_safe(day, day.year - 1)
    same_day_values = _daily_series_prefer_point(receta, sucursal, last_year_day, last_year_day, cache=cache)

    last_year_start = _replace_year_safe(event.analysis_start_date, event.analysis_start_date.year - 1)
    last_year_end = _replace_year_safe(event.analysis_end_date, event.analysis_end_date.year - 1)
    window_values = _daily_series_prefer_point(receta, sucursal, last_year_start, last_year_end, cache=cache)
    window_avg = _avg(window_values)

    if same_day_values and window_avg > 0:
        same_day_qty = _avg(same_day_values)
        # Event homologue is the primary anchor; the week average smooths noisy daily spikes.
        value = ((same_day_qty * Decimal("0.55")) + (window_avg * Decimal("0.45"))), "same_day_plus_window_last_year"
        if cache:
            cache.event_historical_anchor[cache_key] = value
        return value
    if same_day_values:
        value = _avg(same_day_values), "same_day_last_year"
        if cache:
            cache.event_historical_anchor[cache_key] = value
        return value
    if window_avg > 0:
        value = window_avg, "event_window_last_year_avg"
        if cache:
            cache.event_historical_anchor[cache_key] = value
        return value
    value = ZERO, "no_event_anchor"
    if cache:
        cache.event_historical_anchor[cache_key] = value
    return value


def _fallback_categoria_avg(receta: Receta, sucursal: Sucursal | None, anchor: date) -> Decimal:
    return _fallback_categoria_avg_cached(receta, sucursal, anchor, cache=None)


def _fallback_categoria_avg_cached(
    receta: Receta,
    sucursal: Sucursal | None,
    anchor: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    categoria = (receta.categoria or "").strip()
    if not categoria:
        return Decimal("0")
    cache_key = (categoria, int(sucursal.id) if sucursal else None, anchor)
    if cache and cache_key in cache.fallback_categoria_avg:
        return cache.fallback_categoria_avg[cache_key]
    qs = VentaHistorica.objects.filter(
        receta__categoria=categoria,
        fecha__range=(anchor - timedelta(days=56), anchor - timedelta(days=1)),
    )
    if sucursal:
        qs = qs.filter(sucursal=sucursal)
    avg = qs.aggregate(avg=Avg("cantidad")).get("avg")
    value = Decimal(str(avg or 0))
    if cache:
        cache.fallback_categoria_avg[cache_key] = value
    return value


@lru_cache(maxsize=2048)
def _recent_branch_quantity_total(branch_id: int, anchor: date, days_back: int = 28, skip_recent_days: int = 0) -> Decimal:
    return _recent_branch_quantity_total_cached(
        branch_id,
        anchor,
        days_back=days_back,
        skip_recent_days=skip_recent_days,
        cache=None,
    )


def _recent_branch_quantity_total_cached(
    branch_id: int,
    anchor: date,
    *,
    days_back: int = 28,
    skip_recent_days: int = 0,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (int(branch_id), anchor, int(days_back), int(skip_recent_days))
    if cache and cache_key in cache.branch_window_totals:
        return cache.branch_window_totals[cache_key]
    observed_anchor = _observed_anchor(anchor)
    end = observed_anchor - timedelta(days=1 + skip_recent_days)
    start = end - timedelta(days=days_back - 1)
    if cache and cache.branch_daily_totals:
        total = ZERO
        for target_day in _date_range(start, end):
            total += cache.branch_daily_totals.get((int(branch_id), target_day), ZERO)
        cache.branch_window_totals[cache_key] = total
        return total
    branch = Sucursal.objects.filter(pk=branch_id).first()
    if not branch:
        return ZERO
    total = ZERO
    for target_day in _date_range(start, end):
        day_sales = get_daily_sales(sucursal=branch, fecha=target_day)
        total += Decimal(str(day_sales["cantidad"] or 0))
    if cache:
        cache.branch_window_totals[cache_key] = total
    return total


def _branch_growth_signal(branch: Sucursal, anchor: date) -> Decimal:
    return _branch_growth_signal_cached(branch, anchor, cache=None)


def _branch_growth_signal_cached(
    branch: Sucursal,
    anchor: date,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (int(branch.id), anchor)
    if cache and cache_key in cache.branch_growth_signal:
        return cache.branch_growth_signal[cache_key]

    recent_amount = _recent_branch_indicator_total(branch, anchor, lookback_days=14, cache=cache)
    prior_end_anchor = anchor - timedelta(days=14)
    prior_amount = _recent_branch_indicator_total(branch, prior_end_anchor, lookback_days=28, cache=cache)
    if recent_amount <= 0:
        recent_amount = _recent_branch_quantity_total_cached(branch.id, anchor=anchor, days_back=14, cache=cache)
    if prior_amount <= 0:
        prior_amount = _recent_branch_quantity_total_cached(
            branch.id,
            anchor=anchor,
            days_back=28,
            skip_recent_days=14,
            cache=cache,
        )
    if prior_amount <= 0:
        return ZERO
    value = _clamp(_pct_delta(recent_amount, prior_amount), low=Decimal("-0.12"), high=Decimal("0.10"))
    if cache:
        cache.branch_growth_signal[cache_key] = value
    return value


def _product_temporality_signal(event: EventoVenta, receta: Receta) -> Decimal:
    if receta.temporalidad == Receta.TEMPORALIDAD_PERMANENTE:
        return ZERO
    detail = (receta.temporalidad_detalle or "").strip().lower()
    event_type = (event.event_type or "").strip().lower()
    event_name = (event.name or "").strip().lower()
    if receta.temporalidad == Receta.TEMPORALIDAD_FECHA_ESPECIAL:
        if detail and (detail in event_type or detail in event_name):
            return Decimal("0.04")
        if detail and detail not in event_type and detail not in event_name:
            return Decimal("-0.12")
        return Decimal("0.01")
    if receta.temporalidad == Receta.TEMPORALIDAD_TEMPORAL:
        return Decimal("0.01")
    return ZERO


def _operational_cap_multiplier(receta: Receta, sucursal: Sucursal, base_projection: Decimal) -> tuple[Decimal, dict]:
    return _operational_cap_multiplier_cached(receta, sucursal, base_projection, cache=None)


def _operational_cap_multiplier_cached(
    receta: Receta,
    sucursal: Sucursal,
    base_projection: Decimal,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> tuple[Decimal, dict]:
    cedis_key = int(receta.id)
    if cache and cedis_key in cache.cedis_available:
        available_cedis = cache.cedis_available[cedis_key]
    else:
        cedis_inventory = InventarioCedisProducto.objects.filter(receta=receta).first()
        available_cedis = Decimal(str(cedis_inventory.disponible if cedis_inventory else 0))
        if cache:
            cache.cedis_available[cedis_key] = available_cedis
    details = {
        "policy_cap_qty": 0.0,
        "policy_reason": "forecast_is_not_stock",
        "cedis_available_qty": float(available_cedis),
    }
    return ONE, details


def _starter_branch_floor(
    *,
    sucursal: Sucursal,
    base_method: str,
    fallback_used: bool,
    comparable_scale: Decimal,
    final_projection: Decimal,
) -> tuple[Decimal, dict]:
    details = {
        "starter_floor_applied": False,
        "starter_floor_qty": 0.0,
        "starter_floor_reason": "",
    }
    if sucursal.codigo in POINT_MATURE_BRANCH_CODES:
        return final_projection, details
    comparable_backed = comparable_scale > ZERO or base_method in {
        "weekday_comparable_branch",
        "recent_comparable_branch",
        "fallback_categoria_comparable",
    }
    if not comparable_backed:
        return final_projection, details
    if final_projection < ZERO or final_projection >= ONE:
        return final_projection, details
    details["starter_floor_applied"] = True
    details["starter_floor_qty"] = 1.0
    details["starter_floor_reason"] = "new_branch_minimum_visible_proposal"
    return ONE, details


def _recent_branch_indicator_total(
    branch: Sucursal,
    anchor: date,
    lookback_days: int = 28,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> Decimal:
    cache_key = (int(branch.id), anchor, int(lookback_days))
    if cache and cache_key in cache.branch_indicator_totals:
        return cache.branch_indicator_totals[cache_key]
    observed_anchor = _observed_anchor(anchor)
    start = observed_anchor - timedelta(days=lookback_days)
    end = observed_anchor - timedelta(days=1)
    total = (
        PointDailyBranchIndicator.objects.filter(
            branch__erp_branch=branch,
            indicator_date__range=(start, end),
        ).aggregate(total=Sum("total_amount")).get("total")
        or 0
    )
    value = Decimal(str(total))
    if cache:
        cache.branch_indicator_totals[cache_key] = value
    return value


def _branch_has_operational_signal(branch: Sucursal, anchor: date, lookback_days: int = 45) -> bool:
    observed_anchor = _observed_anchor(anchor)
    start = observed_anchor - timedelta(days=lookback_days)
    if PointDailyBranchIndicator.objects.filter(
        branch__erp_branch=branch,
        indicator_date__range=(start, observed_anchor),
    ).exists():
        return True
    return PointDailySale.objects.filter(
        branch__erp_branch=branch,
        sale_date__range=(start, observed_anchor),
    ).exists()


def _resolve_comparable_branch(
    target_branch: Sucursal,
    candidate_branches: list[Sucursal],
    anchor: date,
    configured_branch: Sucursal | None = None,
    *,
    cache: ForecastRuntimeCache | None = None,
) -> tuple[Sucursal | None, Decimal]:
    cache_key = (int(target_branch.id), int(configured_branch.id) if configured_branch else None, anchor)
    if cache and cache_key in cache.comparable_branch_resolution:
        return cache.comparable_branch_resolution[cache_key]

    target_total = _recent_branch_indicator_total(target_branch, anchor, cache=cache)
    donor_rows: list[tuple[Decimal, Sucursal]] = []
    for branch in candidate_branches:
        if branch.id == target_branch.id:
            continue
        if branch.codigo not in POINT_MATURE_BRANCH_CODES:
            continue
        donor_total = _recent_branch_indicator_total(branch, anchor, cache=cache)
        if donor_total > 0:
            donor_rows.append((donor_total, branch))
    if not donor_rows:
        value = (None, Decimal("0"))
        if cache:
            cache.comparable_branch_resolution[cache_key] = value
        return value
    if configured_branch and configured_branch.codigo in POINT_MATURE_BRANCH_CODES:
        configured_total = _recent_branch_indicator_total(configured_branch, anchor, cache=cache)
        if configured_total > 0 and target_total > 0:
            value = (configured_branch, (target_total / configured_total))
            if cache:
                cache.comparable_branch_resolution[cache_key] = value
            return value
        if configured_total > 0:
            value = (configured_branch, Decimal("0"))
            if cache:
                cache.comparable_branch_resolution[cache_key] = value
            return value
    if target_total > 0:
        donor_total, donor_branch = min(donor_rows, key=lambda row: abs(row[0] - target_total))
        value = (donor_branch, (target_total / donor_total if donor_total > 0 else Decimal("0")))
        if cache:
            cache.comparable_branch_resolution[cache_key] = value
        return value
    donor_total, donor_branch = min(donor_rows, key=lambda row: row[0])
    value = (donor_branch, Decimal("0"))
    if cache:
        cache.comparable_branch_resolution[cache_key] = value
    return value


def _build_explanation(
    base_values: list[Decimal],
    uplift: Decimal,
    trend: Decimal,
    fallback_used: bool,
    *,
    event: EventoVenta,
    receta: Receta,
    sucursal: Sucursal,
    base_method: str,
    branch_growth: Decimal,
    product_velocity: Decimal,
    share_shift: Decimal,
    portfolio_share_shift: Decimal,
    category_trend_signal: Decimal,
    portfolio_preference_pct: Decimal,
    group_key: str,
    group_scope: str,
    share_recent: Decimal,
    share_prior: Decimal,
    share_delta: Decimal,
    ytd_delta: Decimal,
    preference_score: Decimal,
    group_recent_total: Decimal,
    group_prior_total: Decimal,
    operational_multiplier: Decimal,
    operational_details: dict,
    event_anchor_qty: Decimal,
    event_anchor_method: str,
    ytd_factor: Decimal,
    same_period_baseline: Decimal,
    recent_stddev: Decimal,
    intermittent_detected: bool,
    interval_buffer_qty: Decimal,
    scenario_method: str,
    comparable_branch: Sucursal | None = None,
    comparable_factor: Decimal | None = None,
) -> dict:
    legacy_spec = get_legacy_history_spec(receta)
    legacy_pay_equivalent = legacy_spec.legacy_name if legacy_spec else ""
    legacy_pay_cutoff = _first_point_sale_for_recipe(receta.id) if legacy_pay_equivalent else None
    return {
        "base_points": len(base_values),
        "base_method": base_method,
        "analysis_window_start": event.analysis_start_date.isoformat(),
        "analysis_window_end": event.analysis_end_date.isoformat(),
        "event_main_date": event.main_date.isoformat(),
        "recipe_temporality": receta.temporalidad,
        "recipe_temporality_detail": receta.temporalidad_detalle,
        "branch_code": sucursal.codigo,
        "uplift_pct": float(uplift),
        "trend_pct": float(trend),
        "branch_growth_pct": float(branch_growth),
        "product_velocity_pct": float(product_velocity),
        "share_shift_pct": float(share_shift),
        "portfolio_share_shift_pct": float(portfolio_share_shift),
        "category_trend_pct": float(category_trend_signal),
        "portfolio_preference_pct": float(portfolio_preference_pct),
        "group_key": group_key,
        "group_scope": group_scope,
        "share_recent_pct": float(share_recent),
        "share_prior_pct": float(share_prior),
        "share_delta_pct": float(share_delta),
        "ytd_delta_pct": float(ytd_delta),
        "preference_score": float(preference_score),
        "group_recent_total_qty": float(group_recent_total),
        "group_prior_total_qty": float(group_prior_total),
        "substitution_boost_pct": 0.0,
        "substitution_drag_pct": 0.0,
        "group_growth_pct": 0.0,
        "substitution_confidence": "low",
        "substitution_weight_source": "heuristic",
        "substitution_source_level": "",
        "substitution_sample_size": 0,
        "substitution_weight_applied": 0.0,
        "substitution_lambda_branch": 0.0,
        "group_normalization_applied": False,
        "group_normalization_factor": 1.0,
        "group_base_total_qty": 0.0,
        "group_pre_normalization_total_qty": 0.0,
        "group_target_total_qty": 0.0,
        "group_member_count": 1,
        "event_anchor_qty": float(event_anchor_qty),
        "event_anchor_method": event_anchor_method,
        "same_period_baseline_qty": float(same_period_baseline),
        "ytd_factor": float(ytd_factor),
        "recent_stddev_qty": float(recent_stddev),
        "intermittent_detected": intermittent_detected,
        "interval_buffer_qty": float(interval_buffer_qty),
        "scenario_method": scenario_method,
        "legacy_pay_equivalent": legacy_pay_equivalent,
        "legacy_pay_cutoff": legacy_pay_cutoff.isoformat() if legacy_pay_cutoff else "",
        "calibration_applied": False,
        "calibration_scope": "",
        "calibration_factor": 1.0,
        "operational_multiplier": float(operational_multiplier),
        "fallback_used": fallback_used,
        "comparable_branch_code": comparable_branch.codigo if comparable_branch else "",
        "comparable_scale_factor": float(comparable_factor or 0),
        **operational_details,
    }


def _json_decimal(payload: dict, key: str) -> Decimal:
    return Decimal(str(payload.get(key) or 0))


def _substitution_attribute_similarity(product_a: Receta, product_b: Receta) -> Decimal:
    same_family = (_family_key(product_a) == _family_key(product_b))
    same_family_category = (_family_category_key(product_a) == _family_category_key(product_b))
    score = Decimal("0.40")
    if same_family:
        score = Decimal("0.75")
    if same_family_category:
        score = Decimal("1.00")
    if (product_a.temporalidad or "") == (product_b.temporalidad or ""):
        score = min(ONE, score + Decimal("0.05"))
    return score


def _share_overlap_score(explanation_a: dict, explanation_b: dict) -> Decimal:
    recent_a = _json_decimal(explanation_a, "share_recent_pct")
    recent_b = _json_decimal(explanation_b, "share_recent_pct")
    if recent_a > ZERO and recent_b > ZERO:
        return min(recent_a, recent_b) / max(recent_a, recent_b)
    prior_a = _json_decimal(explanation_a, "share_prior_pct")
    prior_b = _json_decimal(explanation_b, "share_prior_pct")
    if prior_a > ZERO and prior_b > ZERO:
        return min(prior_a, prior_b) / max(prior_a, prior_b)
    return ZERO


def _inverse_share_pressure(winner_explanation: dict, loser_explanation: dict) -> Decimal:
    winner_gain = max(_json_decimal(winner_explanation, "share_delta_pct"), ZERO)
    loser_drop = max(-_json_decimal(loser_explanation, "share_delta_pct"), ZERO)
    if winner_gain <= ZERO or loser_drop <= ZERO:
        return ZERO
    return _clamp((winner_gain + loser_drop) / Decimal("0.25"), low=ZERO, high=ONE)


def _group_substitution_confidence(rows: list[EventoVentaForecast]) -> tuple[str, Decimal]:
    if len(rows) < 2:
        return ("low", ZERO)
    sample = rows[0].explanation_json or {}
    recent_total = _json_decimal(sample, "group_recent_total_qty")
    prior_total = _json_decimal(sample, "group_prior_total_qty")
    if recent_total < Decimal("8") or prior_total < Decimal("8"):
        return ("low", ZERO)

    level = "medium"
    strength = Decimal("0.55")
    if recent_total >= Decimal("20") and prior_total >= Decimal("20"):
        level = "high"
        strength = ONE

    if any((row.explanation_json or {}).get("comparable_branch_code") for row in rows):
        if level == "high":
            return ("medium", Decimal("0.55"))
        return ("low", ZERO)
    return (level, strength)


def _apply_group_substitution_adjustments(forecast_rows: list[EventoVentaForecast]) -> None:
    grouped_rows: defaultdict[tuple[int, date, str], list[EventoVentaForecast]] = defaultdict(list)
    for row in forecast_rows:
        explanation = row.explanation_json or {}
        group_key = str(explanation.get("group_key") or f"producto::{row.product_id}")
        grouped_rows[(int(row.branch_id), row.forecast_date, group_key)].append(row)

    learned_weights = preload_learned_substitution_weights(
        group_keys={group_key for _branch_id, _forecast_date, group_key in grouped_rows.keys()},
        branch_ids={branch_id for branch_id, _forecast_date, _group_key in grouped_rows.keys()},
    )

    for rows in grouped_rows.values():
        if len(rows) < 2:
            continue
        confidence_label, confidence_strength = _group_substitution_confidence(rows)
        if confidence_strength <= ZERO:
            for row in rows:
                explanation = dict(row.explanation_json or {})
                explanation["substitution_confidence"] = confidence_label
                explanation["group_member_count"] = len(rows)
                row.explanation_json = explanation
            continue

        base_total = sum((Decimal(str(row.final_forecast or 0)) for row in rows), ZERO)
        if base_total <= ZERO:
            continue

        weighted_category_trend = sum(
            Decimal(str(row.final_forecast or 0)) * _json_decimal(row.explanation_json or {}, "category_trend_pct")
            for row in rows
        ) / base_total

        candidate_winners = [
            row for row in rows
            if _json_decimal(row.explanation_json or {}, "preference_score") > ZERO
        ]
        candidate_losers = [
            row for row in rows
            if _json_decimal(row.explanation_json or {}, "share_delta_pct") < ZERO
        ]

        drag_pressure: dict[int, Decimal] = defaultdict(lambda: ZERO)
        boost_pct_map: dict[int, Decimal] = defaultdict(lambda: ZERO)
        row_substitution_meta: dict[int, dict[str, object]] = defaultdict(dict)

        for winner in candidate_winners:
            winner_explanation = winner.explanation_json or {}
            winner_push = max(_json_decimal(winner_explanation, "preference_score"), ZERO)
            if winner_push <= ZERO:
                continue

            raw_weights: dict[int, Decimal] = {}
            for loser in candidate_losers:
                if loser.product_id == winner.product_id:
                    continue
                loser_explanation = loser.explanation_json or {}
                inverse_pressure = _inverse_share_pressure(winner_explanation, loser_explanation)
                if inverse_pressure <= ZERO:
                    continue
                learned_resolution = resolve_learned_substitution_weight(
                    learned_weights=learned_weights,
                    group_key=str(winner_explanation.get("group_key") or f"producto::{winner.product_id}"),
                    winner_product_id=int(winner.product_id),
                    loser_product_id=int(loser.product_id),
                    branch_id=int(winner.branch_id),
                )
                if learned_resolution:
                    raw_weight = learned_resolution.weight
                    source_weight = row_substitution_meta[int(winner.product_id)].get("substitution_weight_applied", ZERO)
                    if raw_weight >= Decimal(str(source_weight or 0)):
                        row_substitution_meta[int(winner.product_id)] = {
                            "substitution_weight_source": "learned",
                            "substitution_source_level": learned_resolution.source_level,
                            "substitution_sample_size": learned_resolution.sample_size,
                            "substitution_weight_applied": float(raw_weight),
                            "substitution_lambda_branch": float(learned_resolution.lambda_branch),
                        }
                        row_substitution_meta[int(loser.product_id)] = {
                            "substitution_weight_source": "learned",
                            "substitution_source_level": learned_resolution.source_level,
                            "substitution_sample_size": learned_resolution.sample_size,
                            "substitution_weight_applied": float(raw_weight),
                            "substitution_lambda_branch": float(learned_resolution.lambda_branch),
                        }
                else:
                    attribute_similarity = _substitution_attribute_similarity(winner.product, loser.product)
                    overlap_score = _share_overlap_score(winner_explanation, loser_explanation)
                    raw_weight = (
                        inverse_pressure * Decimal("0.45")
                        + attribute_similarity * Decimal("0.35")
                        + overlap_score * Decimal("0.20")
                    )
                    source_weight = row_substitution_meta[int(winner.product_id)].get("substitution_weight_applied", ZERO)
                    if raw_weight >= Decimal(str(source_weight or 0)):
                        heuristic_meta = {
                            "substitution_weight_source": "heuristic",
                            "substitution_source_level": "",
                            "substitution_sample_size": 0,
                            "substitution_weight_applied": float(raw_weight),
                            "substitution_lambda_branch": 0.0,
                        }
                        row_substitution_meta[int(winner.product_id)] = heuristic_meta
                        row_substitution_meta[int(loser.product_id)] = heuristic_meta
                if raw_weight > ZERO:
                    raw_weights[int(loser.product_id)] = raw_weight

            if raw_weights:
                raw_total = sum(raw_weights.values(), ZERO)
                for loser_product_id, raw_weight in raw_weights.items():
                    normalized_weight = raw_weight / raw_total if raw_total > ZERO else ZERO
                    drag_pressure[loser_product_id] += winner_push * normalized_weight

            boost_pct_map[int(winner.product_id)] = min(
                Decimal("0.12"),
                winner_push * Decimal("0.18") * confidence_strength,
            )

        pre_normalization_total = ZERO
        target_total = base_total * (
            ONE + _clamp(weighted_category_trend * Decimal("0.35") * confidence_strength, low=Decimal("-0.05"), high=Decimal("0.08"))
        )

        adjusted_finals: dict[int, Decimal] = {}
        for row in rows:
            explanation = dict(row.explanation_json or {})
            boost_pct = boost_pct_map.get(int(row.product_id), ZERO)
            drag_pct = min(
                Decimal("0.10"),
                drag_pressure.get(int(row.product_id), ZERO) * Decimal("0.14") * confidence_strength,
            )
            original_final = Decimal(str(row.final_forecast or 0))
            adjusted_final = max(ZERO, original_final * (ONE + boost_pct - drag_pct))
            adjusted_finals[int(row.product_id)] = adjusted_final
            pre_normalization_total += adjusted_final
            explanation["substitution_boost_pct"] = float(boost_pct)
            explanation["substitution_drag_pct"] = float(drag_pct)
            explanation["substitution_confidence"] = confidence_label
            explanation["group_growth_pct"] = float((target_total / base_total) - ONE if base_total > ZERO else ZERO)
            explanation["group_member_count"] = len(rows)
            explanation["group_base_total_qty"] = float(base_total)
            explanation.update(row_substitution_meta.get(int(row.product_id), {}))
            row.explanation_json = explanation

        normalization_factor = target_total / pre_normalization_total if pre_normalization_total > ZERO else ONE
        group_normalization_applied = abs(normalization_factor - ONE) > Decimal("0.0001")

        for row in rows:
            explanation = dict(row.explanation_json or {})
            original_final = Decimal(str(row.final_forecast or 0))
            adjusted_final = adjusted_finals.get(int(row.product_id), original_final)
            normalized_final = adjusted_final * normalization_factor
            ratio = (normalized_final / original_final) if original_final > ZERO else ONE
            row.final_forecast = normalized_final
            row.conservative_forecast = Decimal(str(row.conservative_forecast or 0)) * ratio
            row.aggressive_forecast = Decimal(str(row.aggressive_forecast or 0)) * ratio
            row.trend_adjustment = row.final_forecast - Decimal(str(row.base_demand or 0)) - Decimal(str(row.event_uplift or 0))
            explanation["group_normalization_applied"] = group_normalization_applied
            explanation["group_normalization_factor"] = float(normalization_factor)
            explanation["group_pre_normalization_total_qty"] = float(pre_normalization_total)
            explanation["group_target_total_qty"] = float(target_total)
            row.explanation_json = explanation


def _confidence_score(points: int, uplift: Decimal, *, fallback_used: bool, product_velocity: Decimal) -> Decimal:
    score = Decimal("0.40") + Decimal("0.05") * Decimal(min(points, 8))
    if uplift != 0:
        score += Decimal("0.1")
    if product_velocity != 0:
        score += Decimal("0.05")
    if fallback_used:
        score -= Decimal("0.15")
    return max(Decimal("0.10"), min(Decimal("0.95"), score))


def _event_window_last_year(event: EventoVenta) -> tuple[date, date]:
    return (
        _replace_year_safe(event.analysis_start_date, event.analysis_start_date.year - 1),
        _replace_year_safe(event.analysis_end_date, event.analysis_end_date.year - 1),
    )


def _weekday_occurrence_in_month(value: date) -> int:
    return ((value.day - 1) // 7) + 1


def _same_weekday_occurrence_date(value: date, year: int) -> date:
    occurrence = _weekday_occurrence_in_month(value)
    target_weekday = value.weekday()
    matches = [
        day
        for day in range(1, calendar.monthrange(year, value.month)[1] + 1)
        if date(year, value.month, day).weekday() == target_weekday
    ]
    if not matches:
        return _replace_year_safe(value, year)
    index = min(occurrence - 1, len(matches) - 1)
    return date(year, value.month, matches[index])


def _event_homologue_window_candidates(event: EventoVenta) -> list[tuple[str, date, date, date]]:
    calendar_main = _replace_year_safe(event.main_date, event.main_date.year - 1)
    start_offset = (event.analysis_start_date - event.main_date).days
    end_offset = (event.analysis_end_date - event.main_date).days
    candidates = [
        (
            "calendar",
            calendar_main + timedelta(days=start_offset),
            calendar_main + timedelta(days=end_offset),
            calendar_main,
        )
    ]

    weekday_main = _same_weekday_occurrence_date(event.main_date, event.main_date.year - 1)
    if weekday_main != calendar_main:
        candidates.append(
            (
                "weekday_occurrence",
                weekday_main + timedelta(days=start_offset),
                weekday_main + timedelta(days=end_offset),
                weekday_main,
            )
        )
    return candidates


def _aggregate_historical_quantity(
    *,
    start: date,
    end: date,
    product_ids: set[int],
    branch_ids: set[int],
) -> Decimal:
    if not product_ids or not branch_ids:
        return ZERO
    total = (
        VentaHistorica.objects.filter(
            fecha__range=(start, end),
            receta_id__in=product_ids,
            sucursal_id__in=branch_ids,
        ).aggregate(total=Sum("cantidad"))["total"]
        or ZERO
    )
    return Decimal(str(total))


def _aggregate_historical_sales(
    *,
    start: date,
    end: date,
    product_ids: set[int],
    branch_ids: set[int],
) -> Decimal:
    if not product_ids or not branch_ids:
        return ZERO
    total = (
        VentaHistorica.objects.filter(
            fecha__range=(start, end),
            receta_id__in=product_ids,
            sucursal_id__in=branch_ids,
        ).aggregate(total=Sum("monto_total"))["total"]
        or ZERO
    )
    return Decimal(str(total))


def _historical_sales_map_by_branch_day(
    *,
    start: date,
    end: date,
    product_ids: set[int],
    branch_ids: set[int],
) -> dict[tuple[int, date], Decimal]:
    if not product_ids or not branch_ids:
        return {}
    authoritative_rows = (
        VentaAutoritativaPoint.objects.filter(
            sale_date__range=(start, end),
            product_id__in=product_ids,
            branch_id__in=branch_ids,
            quantity__gt=0,
            total_amount__gt=0,
        )
        .values("branch_id", "sale_date")
        .annotate(total=Sum("total_amount"))
    )
    fact_rows = (
        PointSalesDailyProductFact.objects.filter(
            sale_date__range=(start, end),
            receta_id__in=product_ids,
            branch__erp_branch_id__in=branch_ids,
            total_cantidad__gt=0,
            total_venta__gt=0,
        )
        .values("branch__erp_branch_id", "sale_date")
        .annotate(total=Sum("total_venta"))
    )
    point_rows = (
        PointDailySale.objects.filter(
            sale_date__range=(start, end),
            receta_id__in=product_ids,
            branch__erp_branch_id__in=branch_ids,
            quantity__gt=0,
            total_amount__gt=0,
        )
        .values("branch__erp_branch_id", "sale_date", "source_endpoint")
        .annotate(total=Sum("total_amount"))
    )
    historical_rows = (
        VentaHistorica.objects.filter(
            fecha__range=(start, end),
            receta_id__in=product_ids,
            sucursal_id__in=branch_ids,
        )
        .values("sucursal_id", "fecha")
        .annotate(total=Sum("monto_total"))
    )
    authoritative_map = {
        (int(row["branch_id"]), row["sale_date"]): Decimal(str(row["total"] or 0))
        for row in authoritative_rows
    }
    fact_map = {
        (int(row["branch__erp_branch_id"]), row["sale_date"]): Decimal(str(row["total"] or 0))
        for row in fact_rows
    }
    official_point_map: dict[tuple[int, date], Decimal] = {}
    legacy_point_map: dict[tuple[int, date], Decimal] = {}
    for row in point_rows:
        key = (int(row["branch__erp_branch_id"]), row["sale_date"])
        total = Decimal(str(row["total"] or 0))
        if row["source_endpoint"] == "/Report/PrintReportes?idreporte=3":
            official_point_map[key] = total
        else:
            legacy_point_map[key] = legacy_point_map.get(key, ZERO) + total
    historical_map = {
        (int(row["sucursal_id"]), row["fecha"]): Decimal(str(row["total"] or 0))
        for row in historical_rows
    }

    selected: dict[tuple[int, date], Decimal] = {}
    current_day = start
    while current_day <= end:
        for branch_id in branch_ids:
            key = (int(branch_id), current_day)
            if authoritative_day_loaded(int(branch_id), current_day):
                selected[key] = authoritative_map.get(key, ZERO)
            elif key in authoritative_map:
                selected[key] = authoritative_map[key]
            elif key in fact_map:
                selected[key] = fact_map[key]
            elif key in official_point_map:
                selected[key] = official_point_map[key]
            elif key in legacy_point_map:
                selected[key] = legacy_point_map[key]
            elif key in historical_map:
                selected[key] = historical_map[key]
        current_day = date.fromordinal(current_day.toordinal() + 1)
    return selected


def _historical_quantity_map_by_branch_day(
    *,
    start: date,
    end: date,
    product_ids: set[int],
    branch_ids: set[int],
) -> dict[tuple[int, date], Decimal]:
    if not product_ids or not branch_ids:
        return {}
    authoritative_rows = (
        VentaAutoritativaPoint.objects.filter(
            sale_date__range=(start, end),
            product_id__in=product_ids,
            branch_id__in=branch_ids,
        )
        .values("branch_id", "sale_date")
        .annotate(total=Sum("quantity"))
    )
    fact_rows = (
        PointSalesDailyProductFact.objects.filter(
            sale_date__range=(start, end),
            receta_id__in=product_ids,
            branch__erp_branch_id__in=branch_ids,
        )
        .values("branch__erp_branch_id", "sale_date")
        .annotate(total=Sum("total_cantidad"))
    )
    point_rows = (
        PointDailySale.objects.filter(
            sale_date__range=(start, end),
            receta_id__in=product_ids,
            branch__erp_branch_id__in=branch_ids,
        )
        .values("branch__erp_branch_id", "sale_date", "source_endpoint")
        .annotate(total=Sum("quantity"))
    )
    historical_rows = (
        VentaHistorica.objects.filter(
            fecha__range=(start, end),
            receta_id__in=product_ids,
            sucursal_id__in=branch_ids,
        )
        .values("sucursal_id", "fecha")
        .annotate(total=Sum("cantidad"))
    )
    authoritative_map = {
        (int(row["branch_id"]), row["sale_date"]): Decimal(str(row["total"] or 0))
        for row in authoritative_rows
    }
    fact_map = {
        (int(row["branch__erp_branch_id"]), row["sale_date"]): Decimal(str(row["total"] or 0))
        for row in fact_rows
    }
    official_point_map: dict[tuple[int, date], Decimal] = {}
    legacy_point_map: dict[tuple[int, date], Decimal] = {}
    for row in point_rows:
        key = (int(row["branch__erp_branch_id"]), row["sale_date"])
        total = Decimal(str(row["total"] or 0))
        if row["source_endpoint"] == "/Report/PrintReportes?idreporte=3":
            official_point_map[key] = total
        else:
            legacy_point_map[key] = legacy_point_map.get(key, ZERO) + total
    historical_map = {
        (int(row["sucursal_id"]), row["fecha"]): Decimal(str(row["total"] or 0))
        for row in historical_rows
    }

    selected: dict[tuple[int, date], Decimal] = {}
    current_day = start
    while current_day <= end:
        for branch_id in branch_ids:
            key = (int(branch_id), current_day)
            if authoritative_day_loaded(int(branch_id), current_day):
                selected[key] = authoritative_map.get(key, ZERO)
            elif key in authoritative_map:
                selected[key] = authoritative_map[key]
            elif key in fact_map:
                selected[key] = fact_map[key]
            elif key in official_point_map:
                selected[key] = official_point_map[key]
            elif key in legacy_point_map:
                selected[key] = legacy_point_map[key]
            elif key in historical_map:
                selected[key] = historical_map[key]
        current_day = date.fromordinal(current_day.toordinal() + 1)
    return selected


def _branch_quantity_map_for_window(
    *,
    start: date,
    end: date,
    product_ids: set[int],
    branch_ids: set[int],
) -> dict[int, Decimal]:
    if not product_ids or not branch_ids:
        return {}
    return {
        int(row["sucursal_id"]): Decimal(str(row["total"] or 0))
        for row in (
            VentaHistorica.objects.filter(
                fecha__range=(start, end),
                receta_id__in=product_ids,
                sucursal_id__in=branch_ids,
            )
            .values("sucursal_id")
            .annotate(total=Sum("cantidad"))
        )
    }


def _branch_sales_map_for_window(
    *,
    start: date,
    end: date,
    product_ids: set[int],
    branch_ids: set[int],
) -> dict[int, Decimal]:
    if not product_ids or not branch_ids:
        return {}
    return {
        int(row["sucursal_id"]): Decimal(str(row["total"] or 0))
        for row in (
            VentaHistorica.objects.filter(
                fecha__range=(start, end),
                receta_id__in=product_ids,
                sucursal_id__in=branch_ids,
            )
            .values("sucursal_id")
            .annotate(total=Sum("monto_total"))
        )
    }


def _branch_quantity_map_for_ytd(
    *,
    anchor: date,
    product_ids: set[int],
    branch_ids: set[int],
    current_year: bool,
) -> dict[int, Decimal]:
    observed_anchor = _observed_anchor(anchor)
    if current_year:
        end_date = observed_anchor
    else:
        end_date = _replace_year_safe(observed_anchor, observed_anchor.year - 1)
    start_date = date(end_date.year, 1, 1)
    return _branch_quantity_map_for_window(
        start=start_date,
        end=end_date,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )


def _branch_indicator_total_window(
    branch: Sucursal,
    *,
    start: date,
    end: date,
) -> Decimal:
    total = (
        PointDailyBranchIndicator.objects.filter(
            branch__erp_branch=branch,
            indicator_date__range=(start, end),
        ).aggregate(total=Sum("total_amount")).get("total")
        or 0
    )
    return Decimal(str(total))


def _branch_indicator_ytd_total(branch: Sucursal, anchor: date, *, current_year: bool) -> Decimal:
    observed_anchor = _observed_anchor(anchor)
    if current_year:
        end_date = observed_anchor
    else:
        end_date = _replace_year_safe(observed_anchor, observed_anchor.year - 1)
    start_date = date(end_date.year, 1, 1)
    return _branch_indicator_total_window(branch, start=start_date, end=end_date)


def _recent_weekday_sample_dates(*, weekday: int, anchor: date, count: int = 8) -> list[date]:
    observed_anchor = _observed_anchor(anchor)
    cursor = observed_anchor - timedelta(days=1)
    samples: list[date] = []
    while len(samples) < count and cursor >= (observed_anchor - timedelta(days=84)):
        if cursor.weekday() == weekday:
            samples.append(cursor)
        cursor -= timedelta(days=1)
    return list(reversed(samples))


def _daily_curve_band_multipliers(event: EventoVenta, day: date) -> tuple[Decimal, Decimal]:
    offset = (day - event.main_date).days
    if offset == 0:
        return Decimal("0.92"), Decimal("1.08")
    if offset == -1:
        return Decimal("0.88"), Decimal("1.10")
    if offset in {-2, -3}:
        return Decimal("0.90"), Decimal("1.12")
    return Decimal("0.85"), Decimal("1.15")


def _event_daily_curve_targets(
    *,
    event: EventoVenta,
    product_ids: set[int],
    branch_ids: set[int],
    branch_week_totals: dict[int, Decimal],
    branch_comparables: dict[int, int] | None = None,
) -> tuple[dict[int, dict[date, Decimal]], dict[int, dict[date, Decimal]], dict[int, str], dict[str, object]]:
    branch_comparables = branch_comparables or {}
    homologue_start, homologue_end, homologue_main_day, homologue_mode = _select_event_homologue_window(
        event,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    current_dates = [
        event.analysis_start_date + timedelta(days=offset)
        for offset in range((event.analysis_end_date - event.analysis_start_date).days + 1)
    ]
    homologue_dates = [
        homologue_start + timedelta(days=offset)
        for offset in range((homologue_end - homologue_start).days + 1)
    ]
    shared_days = min(len(current_dates), len(homologue_dates))
    current_dates = current_dates[:shared_days]
    homologue_dates = homologue_dates[:shared_days]
    if shared_days <= 1:
        return {}, {}, {}, {
            "homologue_start": homologue_start,
            "homologue_end": homologue_end,
            "homologue_main_day": homologue_main_day,
            "homologue_mode": homologue_mode,
        }

    all_curve_branch_ids = set(branch_ids) | set(branch_comparables.values())
    historical_qty_map = _historical_quantity_map_by_branch_day(
        start=homologue_start,
        end=homologue_end,
        product_ids=product_ids,
        branch_ids=all_curve_branch_ids,
    )

    recent_days: set[date] = set()
    for day in current_dates:
        recent_days.update(_recent_weekday_sample_dates(weekday=day.weekday(), anchor=event.main_date))
    recent_qty_map: dict[tuple[int, date], Decimal] = {}
    if recent_days:
        recent_start = min(recent_days)
        recent_end = max(recent_days)
        recent_qty_map = _historical_quantity_map_by_branch_day(
            start=recent_start,
            end=recent_end,
            product_ids=product_ids,
            branch_ids=all_curve_branch_ids,
        )

    event_hist_by_day: dict[date, Decimal] = {}
    event_hist_total = ZERO
    for offset in range(shared_days):
        current_day = current_dates[offset]
        hist_day = homologue_dates[offset]
        value = sum((historical_qty_map.get((branch_id, hist_day), ZERO) for branch_id in all_curve_branch_ids), ZERO)
        event_hist_by_day[current_day] = value
        event_hist_total += value

    target_day_totals_by_branch: dict[int, dict[date, Decimal]] = {}
    floor_day_totals_by_branch: dict[int, dict[date, Decimal]] = {}
    share_sources: dict[int, str] = {}

    for branch_id in branch_ids:
        branch_week_total = branch_week_totals.get(branch_id, ZERO).quantize(Decimal("0.001"))
        if branch_week_total <= ZERO:
            continue

        branch_hist_by_day: dict[date, Decimal] = {}
        branch_hist_total = ZERO
        share_source = "branch_historical_qty_curve"
        for offset in range(shared_days):
            current_day = current_dates[offset]
            hist_day = homologue_dates[offset]
            hist_value = historical_qty_map.get((branch_id, hist_day), ZERO)
            branch_hist_by_day[current_day] = hist_value
            branch_hist_total += hist_value

        if branch_hist_total <= ZERO:
            comparable_branch_id = branch_comparables.get(branch_id)
            if comparable_branch_id:
                for offset in range(shared_days):
                    current_day = current_dates[offset]
                    hist_day = homologue_dates[offset]
                    hist_value = historical_qty_map.get((comparable_branch_id, hist_day), ZERO)
                    branch_hist_by_day[current_day] = hist_value
                branch_hist_total = sum(branch_hist_by_day.values(), ZERO)
                if branch_hist_total > ZERO:
                    share_source = "configured_comparable_qty_curve"

        if branch_hist_total <= ZERO and event_hist_total > ZERO:
            branch_hist_by_day = {day: event_hist_by_day.get(day, ZERO) for day in current_dates}
            branch_hist_total = sum(branch_hist_by_day.values(), ZERO)
            share_source = "event_historical_qty_curve"

        if branch_hist_total <= ZERO:
            continue

        recent_weekday_avgs: dict[date, Decimal] = {}
        for current_day in current_dates:
            samples = _recent_weekday_sample_dates(weekday=current_day.weekday(), anchor=event.main_date)
            sample_values = [recent_qty_map.get((branch_id, sample_day), ZERO) for sample_day in samples]
            recent_weekday_avgs[current_day] = _avg([value for value in sample_values if value > ZERO])
        recent_total = sum(recent_weekday_avgs.values(), ZERO)

        raw_target_shares: dict[date, Decimal] = {}
        raw_floor_shares: dict[date, Decimal] = {}
        for current_day in current_dates:
            hist_share = (branch_hist_by_day.get(current_day, ZERO) / branch_hist_total) if branch_hist_total > ZERO else ZERO
            recent_share = (recent_weekday_avgs.get(current_day, ZERO) / recent_total) if recent_total > ZERO else ZERO
            blended_share = (
                (hist_share * Decimal("0.85")) + (recent_share * Decimal("0.15"))
                if recent_share > ZERO
                else hist_share
            )
            floor_multiplier, ceil_multiplier = _daily_curve_band_multipliers(event, current_day)
            low = hist_share * floor_multiplier
            high = hist_share * ceil_multiplier if hist_share > ZERO else ONE
            raw_target_shares[current_day] = _clamp(blended_share if blended_share > ZERO else hist_share, low=low, high=high)
            raw_floor_shares[current_day] = low

        target_share_total = sum(raw_target_shares.values(), ZERO)
        if target_share_total <= ZERO:
            continue
        target_shares = {
            day: (share / target_share_total).quantize(Decimal("0.000001"))
            for day, share in raw_target_shares.items()
        }
        floor_share_total = sum(raw_floor_shares.values(), ZERO)
        if floor_share_total > ONE:
            scale = (ONE / floor_share_total).quantize(Decimal("0.000001"))
            floor_shares = {
                day: (share * scale).quantize(Decimal("0.000001"))
                for day, share in raw_floor_shares.items()
            }
        else:
            floor_shares = {
                day: share.quantize(Decimal("0.000001"))
                for day, share in raw_floor_shares.items()
            }

        target_day_totals: dict[date, Decimal] = {}
        floor_day_totals: dict[date, Decimal] = {}
        assigned_target = ZERO
        for current_day in current_dates[:-1]:
            target_qty = (branch_week_total * target_shares[current_day]).quantize(Decimal("0.001"))
            floor_qty = (branch_week_total * floor_shares[current_day]).quantize(Decimal("0.001"))
            target_day_totals[current_day] = target_qty
            floor_day_totals[current_day] = floor_qty
            assigned_target += target_qty
        target_day_totals[current_dates[-1]] = max(ZERO, (branch_week_total - assigned_target).quantize(Decimal("0.001")))
        floor_day_totals[current_dates[-1]] = (
            branch_week_total * floor_shares[current_dates[-1]]
        ).quantize(Decimal("0.001"))

        target_day_totals_by_branch[branch_id] = target_day_totals
        floor_day_totals_by_branch[branch_id] = floor_day_totals
        share_sources[branch_id] = share_source

    return target_day_totals_by_branch, floor_day_totals_by_branch, share_sources, {
        "homologue_start": homologue_start,
        "homologue_end": homologue_end,
        "homologue_main_day": homologue_main_day,
        "homologue_mode": homologue_mode,
    }


def _allocate_day_totals_from_targets(
    *,
    target_total: Decimal,
    target_day_totals: dict[date, Decimal],
    protected_minimums: dict[date, Decimal] | None = None,
) -> dict[date, Decimal]:
    protected_minimums = protected_minimums or {}
    if target_total <= ZERO or not target_day_totals:
        return {}

    ordered_days = sorted(target_day_totals.keys())
    protected: dict[date, Decimal] = {
        day: max(ZERO, Decimal(str(protected_minimums.get(day, ZERO)))).quantize(Decimal("0.001"))
        for day in ordered_days
    }
    protected_total = sum(protected.values(), ZERO).quantize(Decimal("0.001"))
    if protected_total > target_total:
        return {}

    remaining = (target_total - protected_total).quantize(Decimal("0.001"))
    flex_weights = {
        day: max(ZERO, (Decimal(str(target_day_totals.get(day, ZERO))) - protected[day])).quantize(Decimal("0.001"))
        for day in ordered_days
    }
    flex_total = sum(flex_weights.values(), ZERO)
    allocated = ZERO
    results: dict[date, Decimal] = {}
    for index, day in enumerate(ordered_days, start=1):
        if index == len(ordered_days):
            results[day] = max(ZERO, (target_total - allocated).quantize(Decimal("0.001")))
            continue
        if remaining <= ZERO:
            day_total = protected[day]
        elif flex_total > ZERO and flex_weights[day] > ZERO:
            day_total = (protected[day] + (remaining * (flex_weights[day] / flex_total))).quantize(Decimal("0.001"))
        else:
            day_total = protected[day]
        results[day] = day_total
        allocated += day_total
    return results


def _compress_protected_minimums_to_target(
    *,
    target_total: Decimal,
    protected_minimums: dict[date, Decimal],
    locked_days: set[date] | None = None,
) -> dict[date, Decimal]:
    if target_total <= ZERO or not protected_minimums:
        return {}

    locked_days = set(locked_days or set())
    ordered_days = sorted(protected_minimums.keys())
    protected = {
        day: max(ZERO, Decimal(str(protected_minimums.get(day, ZERO)))).quantize(Decimal("0.001"))
        for day in ordered_days
    }
    protected_total = sum(protected.values(), ZERO).quantize(Decimal("0.001"))
    if protected_total <= target_total:
        return protected

    locked_total = sum(protected.get(day, ZERO) for day in locked_days).quantize(Decimal("0.001"))
    if locked_total >= target_total:
        results = {day: ZERO.quantize(Decimal("0.001")) for day in ordered_days}
        remaining = target_total.quantize(Decimal("0.001"))
        locked_order = [day for day in ordered_days if day in locked_days] or ordered_days
        for index, day in enumerate(locked_order, start=1):
            if remaining <= ZERO:
                break
            if index == len(locked_order):
                results[day] = remaining
                break
            day_value = min(protected.get(day, ZERO), remaining).quantize(Decimal("0.001"))
            results[day] = day_value
            remaining = (remaining - day_value).quantize(Decimal("0.001"))
        return results

    remaining_capacity = (target_total - locked_total).quantize(Decimal("0.001"))
    flexible_days = [day for day in ordered_days if day not in locked_days]
    flexible_total = sum(protected.get(day, ZERO) for day in flexible_days).quantize(Decimal("0.001"))

    results: dict[date, Decimal] = {}
    allocated = ZERO
    for day in ordered_days:
        if day in locked_days:
            day_value = protected.get(day, ZERO)
        elif flexible_total <= ZERO or remaining_capacity <= ZERO:
            day_value = ZERO
        else:
            day_value = (remaining_capacity * (protected.get(day, ZERO) / flexible_total)).quantize(Decimal("0.001"))
        results[day] = day_value
        allocated += day_value

    delta = (target_total - allocated).quantize(Decimal("0.001"))
    if delta != ZERO:
        adjustment_candidates = [day for day in reversed(ordered_days) if day not in locked_days] or list(reversed(ordered_days))
        for day in adjustment_candidates:
            candidate = (results[day] + delta).quantize(Decimal("0.001"))
            if candidate >= ZERO:
                results[day] = candidate
                break
    return results


def _shrunk_ratio(
    *,
    current_total: Decimal,
    prior_total: Decimal,
    min_weight: Decimal,
    max_weight: Decimal,
    signal_full_at: Decimal,
    low: Decimal,
    high: Decimal,
) -> Decimal:
    if prior_total <= ZERO or current_total <= ZERO:
        return ONE
    raw_ratio = current_total / prior_total
    signal_ratio = prior_total / signal_full_at if signal_full_at > ZERO else ONE
    signal_ratio = _clamp(signal_ratio, low=Decimal("0.20"), high=ONE)
    shrink_weight = min_weight + ((max_weight - min_weight) * signal_ratio)
    return _clamp(ONE + ((raw_ratio - ONE) * shrink_weight), low=low, high=high)


def _resolve_same_store_signal(
    *,
    current_ytd_qty: Decimal,
    prior_ytd_qty: Decimal,
    current_indicator_ytd: Decimal,
    prior_indicator_ytd: Decimal,
) -> tuple[Decimal, str, Decimal, Decimal]:
    if current_indicator_ytd >= Decimal("250000") and prior_indicator_ytd >= Decimal("250000"):
        return (
            _shrunk_ratio(
                current_total=current_indicator_ytd,
                prior_total=prior_indicator_ytd,
                min_weight=Decimal("0.20"),
                max_weight=Decimal("0.56"),
                signal_full_at=Decimal("1800000"),
                low=Decimal("0.82"),
                high=Decimal("1.10"),
            ),
            "branch_indicator_ytd",
            current_indicator_ytd,
            prior_indicator_ytd,
        )

    if current_ytd_qty > ZERO and prior_ytd_qty > ZERO:
        return (
            _shrunk_ratio(
                current_total=current_ytd_qty,
                prior_total=prior_ytd_qty,
                min_weight=Decimal("0.28"),
                max_weight=Decimal("0.68"),
                signal_full_at=Decimal("18000"),
                low=Decimal("0.78"),
                high=Decimal("1.12"),
            ),
            "event_products_ytd",
            current_ytd_qty,
            prior_ytd_qty,
        )

    return ONE, "fallback_no_comparable_signal", current_ytd_qty, prior_ytd_qty


def _resolve_new_branch_increment(
    *,
    branch: Sucursal,
    donor_branch: Sucursal | None,
    donor_scale: Decimal,
    historical_by_branch: dict[int, Decimal],
    current_ytd_by_branch: dict[int, Decimal],
    anchor: date,
) -> tuple[Decimal, str, str, Decimal]:
    branch_current_ytd = current_ytd_by_branch.get(branch.id, ZERO)
    maturity_factor = ZERO
    maturity_source = "fallback_no_signal"
    donor_code = donor_branch.codigo if donor_branch else ""
    donor_hist_event_qty = historical_by_branch.get(donor_branch.id, ZERO) if donor_branch else ZERO

    if donor_branch:
        donor_current_ytd = current_ytd_by_branch.get(donor_branch.id, ZERO)
        if branch_current_ytd > ZERO and donor_current_ytd > ZERO:
            maturity_factor = _shrunk_ratio(
                current_total=branch_current_ytd,
                prior_total=donor_current_ytd,
                min_weight=Decimal("0.45"),
                max_weight=Decimal("0.80"),
                signal_full_at=Decimal("220"),
                low=Decimal("0.18"),
                high=Decimal("1.05"),
            )
            maturity_source = "event_products_ytd_vs_donor"
        else:
            branch_indicator_ytd = _branch_indicator_ytd_total(branch, anchor, current_year=True)
            donor_indicator_ytd = _branch_indicator_ytd_total(donor_branch, anchor, current_year=True)
            if branch_indicator_ytd > ZERO and donor_indicator_ytd > ZERO:
                maturity_factor = _shrunk_ratio(
                    current_total=branch_indicator_ytd,
                    prior_total=donor_indicator_ytd,
                    min_weight=Decimal("0.35"),
                    max_weight=Decimal("0.70"),
                    signal_full_at=Decimal("250000"),
                    low=Decimal("0.18"),
                    high=Decimal("1.00"),
                )
                maturity_source = "branch_indicator_ytd_vs_donor"

    if maturity_factor <= ZERO and donor_scale > ZERO:
        maturity_factor = _clamp(donor_scale, low=Decimal("0.18"), high=Decimal("0.95"))
        maturity_source = "configured_comparable_scale"

    if maturity_factor <= ZERO:
        maturity_factor = Decimal("0.28")

    projected_qty = (donor_hist_event_qty * maturity_factor).quantize(Decimal("0.001")) if donor_hist_event_qty > ZERO else ZERO
    return projected_qty, maturity_source, donor_code, maturity_factor


def build_event_executive_projection_model(
    event: EventoVenta,
    *,
    forecast_rows: list[EventoVentaForecast] | None = None,
) -> dict[str, object]:
    rows = list(forecast_rows) if forecast_rows is not None else list(
        EventoVentaForecast.objects.filter(sales_event=event).select_related("branch", "product")
    )
    if not rows:
        return {}

    product_ids = {int(row.product_id) for row in rows}
    active_branch_ids = {int(row.branch_id) for row in rows}
    branch_map = {int(row.branch_id): row.branch for row in rows}
    current_branch_totals: dict[int, Decimal] = defaultdict(lambda: ZERO)
    current_main_day_totals: dict[int, Decimal] = defaultdict(lambda: ZERO)
    for row in rows:
        qty = Decimal(str(row.final_forecast or 0))
        current_branch_totals[int(row.branch_id)] += qty
        if row.forecast_date == event.main_date:
            current_main_day_totals[int(row.branch_id)] += qty

    branch_links = list(
        EventoVentaSucursal.objects.filter(sales_event=event, is_active=True)
        .select_related("branch", "comparable_branch")
        .order_by("branch__codigo")
    )
    branch_links_all = list(
        EventoVentaSucursal.objects.filter(sales_event=event)
        .select_related("branch", "comparable_branch")
        .order_by("branch__codigo")
    )
    linked_branch_ids = {int(link.branch_id) for link in branch_links_all}
    configured_comparable_ids = {
        int(link.comparable_branch_id)
        for link in branch_links_all
        if link.comparable_branch_id
    }
    all_branch_ids = active_branch_ids | linked_branch_ids | configured_comparable_ids
    if not all_branch_ids:
        return {}

    homologue_start, homologue_end, homologue_main_day, homologue_mode = _select_event_homologue_window(
        event,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
    )
    historical_qty_by_branch = _branch_quantity_map_for_window(
        start=homologue_start,
        end=homologue_end,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
    )
    historical_sales_by_branch = _branch_sales_map_for_window(
        start=homologue_start,
        end=homologue_end,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
    )
    current_ytd_qty_by_branch = _branch_quantity_map_for_ytd(
        anchor=event.main_date,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
        current_year=True,
    )
    prior_ytd_qty_by_branch = _branch_quantity_map_for_ytd(
        anchor=event.main_date,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
        current_year=False,
    )
    benchmark_override_sales = _event_executive_benchmark_sales(event)
    historical_benchmark_sales = _aggregate_historical_sales(
        start=homologue_start,
        end=homologue_end,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
    ).quantize(Decimal("0.01"))
    benchmark_sales = benchmark_override_sales if benchmark_override_sales > ZERO else historical_benchmark_sales
    benchmark_source = "objective_notes" if benchmark_override_sales > ZERO else f"historical_{homologue_mode}"
    benchmark_base_factor = (
        (benchmark_sales / historical_benchmark_sales).quantize(Decimal("0.0001"))
        if benchmark_sales > ZERO and historical_benchmark_sales > ZERO
        else ONE.quantize(Decimal("0.0001"))
    )

    from ventas.services.financials import resolve_unit_price

    current_branch_sales: dict[int, Decimal] = defaultdict(lambda: ZERO)
    price_cache: dict[tuple[int, int], Decimal] = {}
    for row in rows:
        price_key = (int(row.product_id), int(row.branch_id))
        unit_price = price_cache.get(price_key)
        if unit_price is None:
            unit_price = resolve_unit_price(
                int(row.product_id),
                event.analysis_start_date,
                event.analysis_end_date,
                branch_id=int(row.branch_id),
            )
            price_cache[price_key] = unit_price
        current_branch_sales[int(row.branch_id)] += unit_price * Decimal(str(row.final_forecast or 0))
    current_branch_avg_price = {
        branch_id: (current_branch_sales[branch_id] / current_branch_totals[branch_id]).quantize(Decimal("0.0001"))
        for branch_id in current_branch_totals
        if current_branch_totals[branch_id] > ZERO and current_branch_sales[branch_id] > ZERO
    }

    comparable_branches: list[dict[str, object]] = []
    new_branches: list[dict[str, object]] = []
    contracted_branches: list[dict[str, object]] = []
    branch_targets: dict[int, Decimal] = {}
    comparable_target_qty_by_branch: dict[int, Decimal] = {}
    comparable_target_sales_by_branch: dict[int, Decimal] = {}
    comparable_hist_total = ZERO
    comparable_projected_total = ZERO
    comparable_hist_sales_total = ZERO
    comparable_projected_sales_total = ZERO
    expansion_increment_total = ZERO
    expansion_increment_sales_total = ZERO
    contraction_hist_total = ZERO
    contraction_hist_sales_total = ZERO

    candidate_branches = [link.branch for link in branch_links if int(link.branch_id) in active_branch_ids]
    configured_comparables = {
        int(link.branch_id): link.comparable_branch
        for link in branch_links_all
        if link.comparable_branch_id
    }

    for link in branch_links:
        branch = link.branch
        branch_id = int(branch.id)
        historical_qty = (historical_qty_by_branch.get(branch_id, ZERO) * benchmark_base_factor).quantize(Decimal("0.001"))
        historical_sales = (historical_sales_by_branch.get(branch_id, ZERO) * benchmark_base_factor).quantize(Decimal("0.01"))
        current_ytd_qty = current_ytd_qty_by_branch.get(branch_id, ZERO).quantize(Decimal("0.001"))
        prior_ytd_qty = prior_ytd_qty_by_branch.get(branch_id, ZERO).quantize(Decimal("0.001"))
        current_qty = current_branch_totals.get(branch_id, ZERO).quantize(Decimal("0.001"))
        current_main_qty = current_main_day_totals.get(branch_id, ZERO).quantize(Decimal("0.001"))
        current_avg_price = current_branch_avg_price.get(branch_id, ZERO)
        is_current_scope = branch_id in active_branch_ids
        current_indicator_ytd = _branch_indicator_ytd_total(branch, event.main_date, current_year=True)
        prior_indicator_ytd = _branch_indicator_ytd_total(branch, event.main_date, current_year=False)

        if is_current_scope and (prior_indicator_ytd > ZERO or prior_ytd_qty > ZERO):
            same_store_factor, signal_source, comparable_current, comparable_prior = _resolve_same_store_signal(
                current_ytd_qty=current_ytd_qty,
                prior_ytd_qty=prior_ytd_qty,
                current_indicator_ytd=current_indicator_ytd,
                prior_indicator_ytd=prior_indicator_ytd,
            )
            if historical_sales > ZERO and current_avg_price > ZERO:
                target_sales = (historical_sales * same_store_factor).quantize(Decimal("0.01"))
                target_qty = (target_sales / current_avg_price).quantize(Decimal("0.001"))
            elif historical_qty > ZERO:
                target_sales = historical_sales
                target_qty = (historical_qty * same_store_factor).quantize(Decimal("0.001"))
            else:
                target_sales = current_branch_sales.get(branch_id, ZERO).quantize(Decimal("0.01"))
                target_qty = current_qty
            branch_targets[branch_id] = target_qty
            comparable_target_qty_by_branch[branch_id] = target_qty
            comparable_target_sales_by_branch[branch_id] = target_sales
            comparable_hist_total += historical_qty
            comparable_projected_total += target_qty
            comparable_hist_sales_total += historical_sales
            comparable_projected_sales_total += target_sales
            comparable_branches.append(
                {
                    "branch_id": branch_id,
                    "branch_code": branch.codigo,
                    "historical_event_qty": historical_qty,
                    "historical_event_sales": historical_sales,
                    "current_ytd_qty": current_ytd_qty,
                    "prior_ytd_qty": prior_ytd_qty,
                    "current_ytd_indicator": current_indicator_ytd.quantize(Decimal("0.01")),
                    "prior_ytd_indicator": prior_indicator_ytd.quantize(Decimal("0.01")),
                    "same_store_factor": same_store_factor.quantize(Decimal("0.0001")),
                    "target_qty": target_qty,
                    "target_sales": target_sales,
                    "current_avg_price": current_avg_price,
                    "current_qty_before_alignment": current_qty,
                    "current_main_day_qty_before_alignment": current_main_qty,
                    "same_store_signal_source": signal_source,
                    "same_store_signal_current": comparable_current.quantize(Decimal("0.01")),
                    "same_store_signal_prior": comparable_prior.quantize(Decimal("0.01")),
                    "target_source": "same_store_homologue" if historical_qty > ZERO else "same_store_no_homologue",
                }
            )
            continue

    for link in branch_links:
        branch = link.branch
        branch_id = int(branch.id)
        historical_qty = (historical_qty_by_branch.get(branch_id, ZERO) * benchmark_base_factor).quantize(Decimal("0.001"))
        historical_sales = (historical_sales_by_branch.get(branch_id, ZERO) * benchmark_base_factor).quantize(Decimal("0.01"))
        current_ytd_qty = current_ytd_qty_by_branch.get(branch_id, ZERO).quantize(Decimal("0.001"))
        prior_ytd_qty = prior_ytd_qty_by_branch.get(branch_id, ZERO).quantize(Decimal("0.001"))
        current_qty = current_branch_totals.get(branch_id, ZERO).quantize(Decimal("0.001"))
        current_main_qty = current_main_day_totals.get(branch_id, ZERO).quantize(Decimal("0.001"))
        current_avg_price = current_branch_avg_price.get(branch_id, ZERO)
        is_current_scope = branch_id in active_branch_ids
        current_indicator_ytd = _branch_indicator_ytd_total(branch, event.main_date, current_year=True)
        prior_indicator_ytd = _branch_indicator_ytd_total(branch, event.main_date, current_year=False)

        if branch_id in comparable_target_qty_by_branch:
            continue

        if is_current_scope:
            donor_branch, donor_scale = _resolve_comparable_branch(
                branch,
                candidate_branches,
                event.main_date,
                configured_branch=configured_comparables.get(branch_id),
            )
            target_qty, maturity_source, donor_code, maturity_factor = _resolve_new_branch_increment(
                branch=branch,
                donor_branch=donor_branch,
                donor_scale=donor_scale,
                historical_by_branch=historical_qty_by_branch,
                current_ytd_by_branch=current_ytd_qty_by_branch,
                anchor=event.main_date,
            )
            donor_target_sales = comparable_target_sales_by_branch.get(int(donor_branch.id), ZERO) if donor_branch else ZERO
            donor_hist_sales = (
                historical_sales_by_branch.get(donor_branch.id, ZERO) * benchmark_base_factor
            ).quantize(Decimal("0.01")) if donor_branch else ZERO
            donor_sales_base = donor_target_sales if donor_target_sales > ZERO else donor_hist_sales
            if donor_sales_base > ZERO and current_avg_price > ZERO:
                target_sales = (donor_sales_base * maturity_factor).quantize(Decimal("0.01"))
                target_qty = (target_sales / current_avg_price).quantize(Decimal("0.001"))
            else:
                target_sales = ZERO
            if target_qty <= ZERO:
                target_qty = current_qty
                maturity_source = "current_forecast_fallback"
            branch_targets[branch_id] = target_qty
            expansion_increment_total += target_qty
            expansion_increment_sales_total += target_sales
            new_branches.append(
                {
                    "branch_id": branch_id,
                    "branch_code": branch.codigo,
                    "current_ytd_qty": current_ytd_qty,
                    "prior_ytd_qty": prior_ytd_qty,
                    "current_ytd_indicator": current_indicator_ytd.quantize(Decimal("0.01")),
                    "prior_ytd_indicator": prior_indicator_ytd.quantize(Decimal("0.01")),
                    "target_qty": target_qty,
                    "target_sales": target_sales,
                    "current_avg_price": current_avg_price,
                    "current_qty_before_alignment": current_qty,
                    "current_main_day_qty_before_alignment": current_main_qty,
                    "donor_branch_code": donor_code,
                    "donor_sales_base": donor_sales_base,
                    "maturity_source": maturity_source,
                    "maturity_factor": maturity_factor.quantize(Decimal("0.0001")),
                    "configured_comparable": configured_comparables.get(branch_id).codigo if configured_comparables.get(branch_id) else "",
                }
            )
            continue

        if historical_qty > ZERO or prior_ytd_qty > ZERO:
            contraction_hist_total += historical_qty
            contraction_hist_sales_total += historical_sales
            contracted_branches.append(
                {
                    "branch_id": branch_id,
                    "branch_code": branch.codigo,
                    "historical_event_qty": historical_qty,
                    "historical_event_sales": historical_sales,
                    "prior_ytd_qty": prior_ytd_qty,
                    "current_ytd_qty": current_ytd_qty,
                    "current_ytd_indicator": current_indicator_ytd.quantize(Decimal("0.01")),
                    "prior_ytd_indicator": prior_indicator_ytd.quantize(Decimal("0.01")),
                    "target_qty": ZERO,
                    "target_sales": ZERO,
                    "target_source": "branch_out_of_scope_or_inactive",
                }
            )

    current_total_qty = sum(current_branch_totals.values(), ZERO).quantize(Decimal("0.001"))
    same_store_factor_total = (
        (comparable_projected_sales_total / comparable_hist_sales_total).quantize(Decimal("0.0001"))
        if comparable_hist_sales_total > ZERO
        else ONE.quantize(Decimal("0.0001"))
    )
    contraction_factor = (
        (ONE - (contraction_hist_sales_total / (comparable_hist_sales_total + contraction_hist_sales_total))).quantize(Decimal("0.0001"))
        if (comparable_hist_sales_total + contraction_hist_sales_total) > ZERO
        else ONE.quantize(Decimal("0.0001"))
    )
    expansion_factor = (
        (expansion_increment_sales_total / comparable_hist_sales_total).quantize(Decimal("0.0001"))
        if comparable_hist_sales_total > ZERO
        else Decimal("0.0000")
    )

    target_total_qty = sum(branch_targets.values(), ZERO).quantize(Decimal("0.001"))
    growth_anchor_factor = _event_growth_anchor_factor(
        {
            "same_store_factor": same_store_factor_total,
            "expansion_factor": expansion_factor,
            "contraction_factor": contraction_factor,
        }
    )

    return {
        "applied": bool(branch_targets),
        "homologue_start": homologue_start,
        "homologue_end": homologue_end,
        "homologue_main_day": homologue_main_day,
        "homologue_mode": homologue_mode,
        "benchmark_source": benchmark_source,
        "benchmark_sales": benchmark_sales,
        "main_day_benchmark_sales": _event_executive_main_day_benchmark_sales(event),
        "historical_benchmark_sales": historical_benchmark_sales,
        "benchmark_base_factor": benchmark_base_factor,
        "same_store_factor": same_store_factor_total,
        "expansion_factor": expansion_factor,
        "contraction_factor": contraction_factor,
        "growth_anchor_factor": growth_anchor_factor,
        "historical_comparable_qty": comparable_hist_total.quantize(Decimal("0.001")),
        "projected_same_store_qty": comparable_projected_total.quantize(Decimal("0.001")),
        "expansion_increment_qty": expansion_increment_total.quantize(Decimal("0.001")),
        "contraction_qty": contraction_hist_total.quantize(Decimal("0.001")),
        "historical_comparable_sales": comparable_hist_sales_total.quantize(Decimal("0.01")),
        "projected_same_store_sales": comparable_projected_sales_total.quantize(Decimal("0.01")),
        "expansion_increment_sales": expansion_increment_sales_total.quantize(Decimal("0.01")),
        "contraction_sales": contraction_hist_sales_total.quantize(Decimal("0.01")),
        "target_total_qty": target_total_qty,
        "current_total_qty": current_total_qty,
        "final_projection_reasoning": (
            "Forecast ejecutivo construido con benchmark del evento, crecimiento same-store real por sucursal comparable, "
            "expansión incremental explícita para sucursales nuevas, contracción explícita para sucursales fuera de alcance "
            "y conversión final a piezas usando precio real vigente por sucursal/SKU."
        ),
        "mix_adjustment_source": "granular_forecast_scaled_to_branch_targets",
        "branch_targets": {
            int(branch_id): target.quantize(Decimal("0.001"))
            for branch_id, target in branch_targets.items()
        },
        "comparable_branches": comparable_branches,
        "new_branches": new_branches,
        "contracted_branches": contracted_branches,
    }


def _select_event_homologue_window(
    event: EventoVenta,
    *,
    product_ids: set[int],
    branch_ids: set[int],
) -> tuple[date, date, date, str]:
    best: tuple[Decimal, str, date, date, date] | None = None
    for label, start, end, main_day in _event_homologue_window_candidates(event):
        main_total = _aggregate_historical_quantity(
            start=main_day,
            end=main_day,
            product_ids=product_ids,
            branch_ids=branch_ids,
        )
        window_total = _aggregate_historical_quantity(
            start=start,
            end=end,
            product_ids=product_ids,
            branch_ids=branch_ids,
        )
        score = (main_total * Decimal("0.65")) + (window_total * Decimal("0.35"))
        candidate = (score, label, start, end, main_day)
        if best is None or candidate[0] > best[0]:
            best = candidate
    if best is None:
        start, end = _event_window_last_year(event)
        main = _replace_year_safe(event.main_date, event.main_date.year - 1)
        return start, end, main, "calendar"
    return best[2], best[3], best[4], best[1]


def _aggregate_recipe_ytd_factor(
    *,
    product_ids: set[int],
    branch_ids: set[int],
    anchor: date,
) -> Decimal:
    if not product_ids or not branch_ids:
        return ONE
    observed_anchor = _observed_anchor(anchor)
    if observed_anchor.month == 1 and observed_anchor.day < 10:
        return ONE

    current_start = date(observed_anchor.year, 1, 1)
    prior_end = _replace_year_safe(observed_anchor, observed_anchor.year - 1)
    prior_start = date(prior_end.year, 1, 1)

    current_total = _aggregate_historical_quantity(
        start=current_start,
        end=observed_anchor,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    prior_total = _aggregate_historical_quantity(
        start=prior_start,
        end=prior_end,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    if prior_total <= ZERO:
        return ONE
    raw_factor = current_total / prior_total
    return _clamp(ONE + ((raw_factor - ONE) * Decimal("0.70")), low=Decimal("0.86"), high=Decimal("1.03"))


def _apply_scale_to_forecasts(
    forecasts,
    factor: Decimal,
    scope_label: str,
    *,
    modified: dict[int, EventoVentaForecast] | None = None,
    extra_explanation: dict[str, object] | None = None,
) -> None:
    if factor <= 0 or factor == ONE:
        return
    for forecast in forecasts:
        forecast.base_demand = Decimal(str(forecast.base_demand or 0)) * factor
        forecast.event_uplift = Decimal(str(forecast.event_uplift or 0)) * factor
        forecast.trend_adjustment = Decimal(str(forecast.trend_adjustment or 0)) * factor
        forecast.final_forecast = Decimal(str(forecast.final_forecast or 0)) * factor
        forecast.conservative_forecast = Decimal(str(forecast.conservative_forecast or 0)) * factor
        forecast.aggressive_forecast = Decimal(str(forecast.aggressive_forecast or 0)) * factor
        explanation = dict(forecast.explanation_json or {})
        explanation["calibration_applied"] = True
        if scope_label == "event_daily_historical_curve" and explanation.get("calibration_scope"):
            explanation["daily_curve_calibration_applied"] = True
            explanation["daily_curve_calibration_scope"] = scope_label
            explanation["daily_curve_calibration_factor"] = float(factor)
        else:
            explanation["calibration_scope"] = scope_label
            explanation["calibration_factor"] = float(factor)
        if extra_explanation:
            explanation.update(extra_explanation)
        forecast.explanation_json = explanation
        if modified is not None and forecast.pk:
            modified[forecast.pk] = forecast


def _calibrate_forecast_against_event_homologue(event: EventoVenta) -> list[str]:
    warnings: list[str] = []
    current_forecasts = list(EventoVentaForecast.objects.filter(sales_event=event).select_related("product"))
    if not current_forecasts:
        return warnings

    product_ids = {int(forecast.product_id) for forecast in current_forecasts}
    branch_ids = {int(forecast.branch_id) for forecast in current_forecasts}
    last_year_start, last_year_end, last_year_main_day, homologue_mode = _select_event_homologue_window(
        event,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    aggregate_ytd_factor = _aggregate_recipe_ytd_factor(product_ids=product_ids, branch_ids=branch_ids, anchor=event.main_date)
    product_cap_factor = Decimal("1.03")
    family_cap_factor = Decimal("1.02")
    product_floor_guard = Decimal("0.88")
    family_floor_guard = Decimal("0.90")
    main_day_floor_guard = Decimal("0.92")

    # Product-level floor for products with meaningful event history.
    history_by_product = {
        row["receta_id"]: Decimal(str(row["total"] or 0))
        for row in VentaHistorica.objects.filter(
            fecha__range=(last_year_start, last_year_end),
            receta_id__in=product_ids,
            sucursal_id__in=branch_ids,
        )
        .values("receta_id")
        .annotate(total=Sum("cantidad"))
    }
    forecasts_by_product: dict[int, list[EventoVentaForecast]] = defaultdict(list)
    current_by_product: dict[int, Decimal] = defaultdict(lambda: ZERO)
    for forecast in current_forecasts:
        forecasts_by_product[int(forecast.product_id)].append(forecast)
        current_by_product[int(forecast.product_id)] += Decimal(str(forecast.final_forecast or 0))

    product_cap_adjustments = 0
    product_floor_adjustments = 0
    modified: dict[int, EventoVentaForecast] = {}
    for product_id, hist_total in history_by_product.items():
        if hist_total < Decimal("20"):
            continue
        current_total = current_by_product.get(product_id, ZERO)
        product_forecasts = forecasts_by_product.get(product_id, [])
        if not product_forecasts or current_total <= 0:
            continue

        cap_target = hist_total * product_cap_factor
        floor_target = hist_total * aggregate_ytd_factor * product_floor_guard

        if current_total > cap_target:
            factor = cap_target / current_total
            _apply_scale_to_forecasts(
                product_forecasts,
                factor,
                "product_homologue_cap",
                modified=modified,
                extra_explanation={"homologue_mode": homologue_mode, "homologue_ytd_factor": float(aggregate_ytd_factor)},
            )
            product_cap_adjustments += 1
        elif current_total < floor_target:
            factor = floor_target / current_total
            factor = _clamp(factor, low=ONE, high=Decimal("1.85"))
            _apply_scale_to_forecasts(
                product_forecasts,
                factor,
                "product_homologue_floor",
                modified=modified,
                extra_explanation={"homologue_mode": homologue_mode, "homologue_ytd_factor": float(aggregate_ytd_factor)},
            )
            product_floor_adjustments += 1

    # Family/category floor catches category groups that still remain too low after per-product scaling.
    history_by_family_cat: dict[tuple[str, str], Decimal] = defaultdict(lambda: ZERO)
    for row in (
        VentaHistorica.objects.filter(
            fecha__range=(last_year_start, last_year_end),
            receta_id__in=product_ids,
            sucursal_id__in=branch_ids,
        )
        .select_related("receta")
        .only("cantidad", "receta__nombre", "receta__familia", "receta__categoria")
    ):
        family, category = _infer_projection_labels(
            product_name=row.receta.nombre or "",
            family=row.receta.familia or "",
            category=row.receta.categoria or "",
        )
        history_by_family_cat[(family, category)] += Decimal(str(row.cantidad or 0))
    forecasts_by_family_cat: dict[tuple[str, str], list[EventoVentaForecast]] = defaultdict(list)
    current_by_family_cat: dict[tuple[str, str], Decimal] = defaultdict(lambda: ZERO)
    for forecast in current_forecasts:
        key = _infer_projection_labels(
            product_name=forecast.product.nombre or "",
            family=forecast.product.familia or "",
            category=forecast.product.categoria or "",
        )
        forecasts_by_family_cat[key].append(forecast)
        current_by_family_cat[key] += Decimal(str(forecast.final_forecast or 0))

    family_cap_adjustments = 0
    family_floor_adjustments = 0
    for key, hist_total in history_by_family_cat.items():
        if hist_total < Decimal("60"):
            continue
        current_total = current_by_family_cat.get(key, ZERO)
        forecasts = forecasts_by_family_cat.get(key, [])
        if not forecasts or current_total <= 0:
            continue

        cap_target = hist_total * family_cap_factor
        floor_target = hist_total * aggregate_ytd_factor * family_floor_guard

        if current_total > cap_target:
            factor = cap_target / current_total
            factor = _clamp(factor, low=Decimal("0.78"), high=ONE)
            _apply_scale_to_forecasts(
                forecasts,
                factor,
                "family_homologue_cap",
                modified=modified,
                extra_explanation={"homologue_mode": homologue_mode, "homologue_ytd_factor": float(aggregate_ytd_factor)},
            )
            family_cap_adjustments += 1
        elif current_total < floor_target:
            factor = floor_target / current_total
            factor = _clamp(factor, low=ONE, high=Decimal("1.55"))
            _apply_scale_to_forecasts(
                forecasts,
                factor,
                "family_homologue_floor",
                modified=modified,
                extra_explanation={"homologue_mode": homologue_mode, "homologue_ytd_factor": float(aggregate_ytd_factor)},
            )
            family_floor_adjustments += 1

    hist_main_total = _aggregate_historical_quantity(
        start=last_year_main_day,
        end=last_year_main_day,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    main_day_rows = [forecast for forecast in current_forecasts if forecast.forecast_date == event.main_date]
    current_main_total = sum((Decimal(str(forecast.final_forecast or 0)) for forecast in main_day_rows), ZERO)
    main_day_floor_adjusted = 0
    if hist_main_total >= Decimal("120") and current_main_total > ZERO:
        floor_target = hist_main_total * aggregate_ytd_factor * main_day_floor_guard
        if current_main_total < floor_target:
            factor = floor_target / current_main_total
            _apply_scale_to_forecasts(
                main_day_rows,
                factor,
                "event_main_day_homologue_floor",
                modified=modified,
                extra_explanation={
                    "homologue_mode": homologue_mode,
                    "homologue_main_day": last_year_main_day.isoformat(),
                    "homologue_ytd_factor": float(aggregate_ytd_factor),
                },
            )
            main_day_floor_adjusted = 1

    if modified:
        EventoVentaForecast.objects.bulk_update(
            list(modified.values()),
            [
                "base_demand",
                "event_uplift",
                "trend_adjustment",
                "final_forecast",
                "conservative_forecast",
                "aggressive_forecast",
                "explanation_json",
            ],
            batch_size=500,
        )

    if product_cap_adjustments:
        warnings.append(f"Se recalibraron {product_cap_adjustments} productos a la baja contra el homólogo del evento.")
    if product_floor_adjustments:
        warnings.append(f"Se recalibraron {product_floor_adjustments} productos al alza contra el homólogo del evento.")
    if family_cap_adjustments:
        warnings.append(f"Se recalibraron {family_cap_adjustments} familias/categorías a la baja contra el homólogo del evento.")
    if family_floor_adjustments:
        warnings.append(f"Se recalibraron {family_floor_adjustments} familias/categorías al alza contra el homólogo del evento.")
    if main_day_floor_adjusted:
        warnings.append("Se recalibró el día principal al alza contra el homólogo fuerte del evento.")
    return warnings


def _align_forecast_to_executive_branch_model(event: EventoVenta) -> list[str]:
    warnings: list[str] = []
    forecasts = list(EventoVentaForecast.objects.filter(sales_event=event).select_related("branch", "product"))
    if not forecasts:
        return warnings

    model = build_event_executive_projection_model(event, forecast_rows=forecasts)
    branch_targets = {int(branch_id): Decimal(str(target or 0)) for branch_id, target in (model.get("branch_targets") or {}).items()}
    if not branch_targets:
        return warnings

    forecasts_by_branch: dict[int, list[EventoVentaForecast]] = defaultdict(list)
    current_totals: dict[int, Decimal] = defaultdict(lambda: ZERO)
    current_main_day_totals: dict[int, Decimal] = defaultdict(lambda: ZERO)
    for forecast in forecasts:
        branch_id = int(forecast.branch_id)
        forecasts_by_branch[branch_id].append(forecast)
        qty = Decimal(str(forecast.final_forecast or 0))
        current_totals[branch_id] += qty
        if forecast.forecast_date == event.main_date:
            current_main_day_totals[branch_id] += qty

    branch_meta = {
        item["branch_code"]: item
        for item in (
            list(model.get("comparable_branches") or [])
            + list(model.get("new_branches") or [])
            + list(model.get("contracted_branches") or [])
        )
    }
    modified: list[EventoVentaForecast] = []
    aligned_branches = 0
    for branch_id, target_qty in branch_targets.items():
        rows = forecasts_by_branch.get(branch_id, [])
        if not rows:
            continue
        current_total = current_totals.get(branch_id, ZERO)
        if current_total <= ZERO or target_qty <= ZERO:
            continue
        factor = (target_qty / current_total).quantize(Decimal("0.000001"))
        assigned = ZERO
        for index, row in enumerate(rows, start=1):
            original_final = Decimal(str(row.final_forecast or 0))
            original_conservative = Decimal(str(row.conservative_forecast or 0))
            original_aggressive = Decimal(str(row.aggressive_forecast or 0))
            if index == len(rows):
                new_final = max(ZERO, target_qty - assigned)
            else:
                new_final = (original_final * factor).quantize(Decimal("0.001"))
                assigned += new_final
            ratio = (new_final / original_final) if original_final > ZERO else ZERO
            row.final_forecast = new_final
            row.conservative_forecast = (original_conservative * ratio).quantize(Decimal("0.001")) if original_conservative > ZERO else new_final
            row.aggressive_forecast = (original_aggressive * ratio).quantize(Decimal("0.001")) if original_aggressive > ZERO else new_final
            explanation = dict(row.explanation_json or {})
            branch_payload = branch_meta.get(row.branch.codigo, {})
            explanation.update(
                {
                    "executive_model_applied": True,
                    "executive_model_factor": float(factor),
                    "executive_model_branch_target_qty": float(target_qty),
                    "executive_model_branch_current_qty": float(current_total),
                    "benchmark_source": str(model.get("benchmark_source") or ""),
                    "same_store_factor": float(model.get("same_store_factor") or ONE),
                    "expansion_factor": float(model.get("expansion_factor") or ZERO),
                    "contraction_factor": float(model.get("contraction_factor") or ONE),
                    "mix_adjustment_source": str(model.get("mix_adjustment_source") or ""),
                    "final_projection_reasoning": str(model.get("final_projection_reasoning") or ""),
                    "executive_branch_role": (
                        "same_store"
                        if row.branch.codigo in {item["branch_code"] for item in model.get("comparable_branches") or []}
                        else "expansion"
                    ),
                    "branch_target_source": branch_payload.get("target_source", ""),
                    "branch_current_ytd_qty": float(Decimal(str(branch_payload.get("current_ytd_qty") or 0))),
                    "branch_prior_ytd_qty": float(Decimal(str(branch_payload.get("prior_ytd_qty") or 0))),
                    "branch_historical_event_qty": float(Decimal(str(branch_payload.get("historical_event_qty") or 0))),
                    "branch_maturity_source": branch_payload.get("maturity_source", ""),
                    "branch_maturity_factor": float(Decimal(str(branch_payload.get("maturity_factor") or 0))),
                    "branch_donor_code": branch_payload.get("donor_branch_code", ""),
                }
            )
            row.explanation_json = explanation
            modified.append(row)
        aligned_branches += 1

    if modified:
        EventoVentaForecast.objects.bulk_update(
            modified,
            ["final_forecast", "conservative_forecast", "aggressive_forecast", "explanation_json"],
            batch_size=500,
        )
    if aligned_branches:
        warnings.append(
            "Forecast alineado al modelo ejecutivo por cohortes de sucursal "
            f"(same-store {model['same_store_factor']}, expansión {model['expansion_factor']}, contracción {model['contraction_factor']})."
        )
    return warnings


def _apply_family_strategy_adjustments(event: EventoVenta) -> list[str]:
    warnings: list[str] = []
    current_forecasts = list(EventoVentaForecast.objects.filter(sales_event=event).select_related("product"))
    if not current_forecasts:
        return warnings

    last_year_start, last_year_end = _event_window_last_year(event)
    current_year_anchor = _observed_anchor(event.main_date)
    current_ytd_start = date(current_year_anchor.year, 1, 1)
    prior_ytd_end = _replace_year_safe(current_year_anchor, current_year_anchor.year - 1)
    prior_ytd_start = date(prior_ytd_end.year, 1, 1)

    modified: dict[int, EventoVentaForecast] = {}

    family_current: dict[str, Decimal] = defaultdict(lambda: ZERO)
    family_rows: dict[str, list[EventoVentaForecast]] = defaultdict(list)
    product_current: dict[int, Decimal] = defaultdict(lambda: ZERO)
    product_rows: dict[int, list[EventoVentaForecast]] = defaultdict(list)
    for forecast in current_forecasts:
        family, _category = _infer_projection_labels(
            product_name=forecast.product.nombre or "",
            family=forecast.product.familia or "",
            category=forecast.product.categoria or "",
        )
        family = family.strip() or "SIN_FAMILIA"
        qty = Decimal(str(forecast.final_forecast or 0))
        family_current[family] += qty
        family_rows[family].append(forecast)
        product_current[int(forecast.product_id)] += qty
        product_rows[int(forecast.product_id)].append(forecast)

    family_hist: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for row in VentaHistorica.objects.filter(fecha__range=(last_year_start, last_year_end)).select_related("receta").only(
        "cantidad", "receta__nombre", "receta__familia", "receta__categoria"
    ):
        family, _ = _infer_projection_labels(
            product_name=row.receta.nombre or "",
            family=row.receta.familia or "",
            category=row.receta.categoria or "",
        )
        family_hist[family.strip() or "SIN_FAMILIA"] += Decimal(str(row.cantidad or 0))
    family_ytd_current: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for row in VentaHistorica.objects.filter(fecha__range=(current_ytd_start, current_year_anchor)).select_related("receta").only(
        "cantidad", "receta__nombre", "receta__familia", "receta__categoria"
    ):
        family, _ = _infer_projection_labels(
            product_name=row.receta.nombre or "",
            family=row.receta.familia or "",
            category=row.receta.categoria or "",
        )
        family_ytd_current[family.strip() or "SIN_FAMILIA"] += Decimal(str(row.cantidad or 0))
    family_ytd_prior: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for row in VentaHistorica.objects.filter(fecha__range=(prior_ytd_start, prior_ytd_end)).select_related("receta").only(
        "cantidad", "receta__nombre", "receta__familia", "receta__categoria"
    ):
        family, _ = _infer_projection_labels(
            product_name=row.receta.nombre or "",
            family=row.receta.familia or "",
            category=row.receta.categoria or "",
        )
        family_ytd_prior[family.strip() or "SIN_FAMILIA"] += Decimal(str(row.cantidad or 0))
    product_hist = {
        int(row["receta_id"]): Decimal(str(row["total"] or 0))
        for row in VentaHistorica.objects.filter(
            fecha__range=(last_year_start, last_year_end),
            receta_id__in=list(product_current.keys()),
        )
        .values("receta_id")
        .annotate(total=Sum("cantidad"))
    }

    # Family-level soft cap only where business signal says the family is still slightly above history.
    empanadas_current = family_current.get("Empanadas", ZERO)
    empanadas_hist = family_hist.get("Empanadas", ZERO)
    empanadas_ytd_prior = family_ytd_prior.get("Empanadas", ZERO)
    empanadas_ytd_current = family_ytd_current.get("Empanadas", ZERO)
    if (
        empanadas_current > ZERO
        and empanadas_hist > ZERO
        and empanadas_current > empanadas_hist
        and empanadas_ytd_prior > ZERO
        and (empanadas_ytd_current / empanadas_ytd_prior) <= Decimal("0.98")
    ):
        factor = empanadas_hist / empanadas_current
        _apply_scale_to_forecasts(family_rows["Empanadas"], factor, "family_empanadas_soft_cap", modified=modified)
        warnings.append("Se aplicó un ajuste leve a Empanadas para alinearla al homólogo histórico del evento.")

    # Variant-level conservative penalty inside Vasos when family YTD is weak and variants lack history.
    vasos_ytd_prior = family_ytd_prior.get("Vasos Preparados", ZERO)
    vasos_ytd_current = family_ytd_current.get("Vasos Preparados", ZERO)
    vasos_ratio = (vasos_ytd_current / vasos_ytd_prior) if vasos_ytd_prior > ZERO else ZERO
    if vasos_ratio > ZERO and vasos_ratio < Decimal("0.50"):
        sparse_variant_factor = _clamp(vasos_ratio + Decimal("0.30"), low=Decimal("0.65"), high=Decimal("0.80"))
        vasos_adjustments = 0
        for product_id, current_total in product_current.items():
            rows = product_rows.get(product_id, [])
            if not rows:
                continue
            family, _ = _infer_projection_labels(
                product_name=rows[0].product.nombre or "",
                family=rows[0].product.familia or "",
                category=rows[0].product.categoria or "",
            )
            family = family.strip() or "SIN_FAMILIA"
            if family != "Vasos Preparados":
                continue
            hist_total = product_hist.get(product_id, ZERO)
            if hist_total >= Decimal("20"):
                continue
            if current_total <= ZERO:
                continue
            _apply_scale_to_forecasts(rows, sparse_variant_factor, "family_vasos_sparse_variant_cap", modified=modified)
            vasos_adjustments += 1
        if vasos_adjustments:
            warnings.append(
                f"Se ajustaron {vasos_adjustments} variantes escasas de Vasos Preparados por YTD familiar débil y baja historia comparable."
            )

    if modified:
        EventoVentaForecast.objects.bulk_update(
            list(modified.values()),
            [
                "base_demand",
                "event_uplift",
                "trend_adjustment",
                "final_forecast",
                "conservative_forecast",
                "aggressive_forecast",
                "explanation_json",
            ],
            batch_size=500,
        )
    return warnings


def _reshape_forecast_rows_to_total(
    rows: list[EventoVentaForecast],
    *,
    target_total: Decimal,
    modified: dict[int, EventoVentaForecast],
    explanation_updates: dict[str, object],
) -> None:
    if not rows:
        return
    target_total = max(ZERO, target_total).quantize(Decimal("0.001"))
    current_total = sum((Decimal(str(row.final_forecast or 0)) for row in rows), ZERO)
    weight_rows = [row for row in rows if Decimal(str(row.final_forecast or 0)) > ZERO]
    if not weight_rows:
        weight_rows = list(rows)
    weight_total = sum((Decimal(str(row.final_forecast or 0)) for row in weight_rows), ZERO)
    if weight_total <= ZERO:
        weight_total = Decimal(len(weight_rows))
    assigned = ZERO
    for index, row in enumerate(weight_rows, start=1):
        original_final = Decimal(str(row.final_forecast or 0))
        original_conservative = Decimal(str(row.conservative_forecast or 0))
        original_aggressive = Decimal(str(row.aggressive_forecast or 0))
        weight = original_final if original_final > ZERO else ONE
        if index == len(weight_rows):
            new_final = max(ZERO, target_total - assigned)
        else:
            new_final = (target_total * (weight / weight_total)).quantize(Decimal("0.001"))
            assigned += new_final
        ratio = (new_final / original_final) if original_final > ZERO else ZERO
        row.final_forecast = new_final
        row.conservative_forecast = (
            (original_conservative * ratio).quantize(Decimal("0.001"))
            if original_conservative > ZERO and original_final > ZERO
            else new_final
        )
        row.aggressive_forecast = (
            (original_aggressive * ratio).quantize(Decimal("0.001"))
            if original_aggressive > ZERO and original_final > ZERO
            else new_final
        )
        explanation = dict(row.explanation_json or {})
        explanation.update(explanation_updates)
        row.explanation_json = explanation
        modified[row.id] = row
    if current_total <= ZERO:
        return
    untouched = [row for row in rows if row not in weight_rows]
    for row in untouched:
        explanation = dict(row.explanation_json or {})
        explanation.update(explanation_updates)
        row.explanation_json = explanation
        modified[row.id] = row


def _rebalance_main_date_priority(event: EventoVenta) -> list[str]:
    if not (event.analysis_start_date <= event.main_date <= event.analysis_end_date):
        return []
    forecasts = list(
        EventoVentaForecast.objects.filter(
            sales_event=event,
            forecast_date__range=(event.analysis_start_date, event.analysis_end_date),
        ).select_related("branch")
    )
    if not forecasts:
        return []

    product_ids = {int(forecast.product_id) for forecast in forecasts}
    branch_ids = {int(forecast.branch_id) for forecast in forecasts}
    homologue_start, homologue_end, _homologue_main_day, homologue_mode = _select_event_homologue_window(
        event,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    current_dates = [
        event.analysis_start_date + timedelta(days=offset)
        for offset in range((event.analysis_end_date - event.analysis_start_date).days + 1)
    ]
    homologue_dates = [
        homologue_start + timedelta(days=offset)
        for offset in range((homologue_end - homologue_start).days + 1)
    ]
    shared_days = min(len(current_dates), len(homologue_dates))
    if shared_days <= 1:
        return []
    current_dates = current_dates[:shared_days]
    homologue_dates = homologue_dates[:shared_days]

    totals = defaultdict(lambda: ZERO)
    branch_day_totals: dict[tuple[int, date], Decimal] = defaultdict(lambda: ZERO)
    for forecast in forecasts:
        totals[forecast.forecast_date] += Decimal(str(forecast.final_forecast or 0))
        branch_day_totals[(int(forecast.branch_id), forecast.forecast_date)] += Decimal(str(forecast.final_forecast or 0))

    current_week_total = sum((totals[day] for day in current_dates), ZERO)
    if current_week_total <= 0:
        return []

    branch_links = list(
        EventoVentaSucursal.objects.filter(sales_event=event, is_active=True)
        .select_related("branch", "comparable_branch")
    )
    branch_comparables = {
        int(link.branch_id): int(link.comparable_branch_id)
        for link in branch_links
        if link.comparable_branch_id
    }
    branch_week_totals = {
        branch_id: sum((branch_day_totals.get((branch_id, day), ZERO) for day in current_dates), ZERO).quantize(Decimal("0.001"))
        for branch_id in branch_ids
    }
    target_day_totals_by_branch, _floor_day_totals_by_branch, share_sources, curve_context = _event_daily_curve_targets(
        event=event,
        product_ids=product_ids,
        branch_ids=branch_ids,
        branch_week_totals=branch_week_totals,
        branch_comparables=branch_comparables,
    )
    if not target_day_totals_by_branch:
        return []

    modified: dict[int, EventoVentaForecast] = {}
    rebalanced_branches = 0
    for branch_id in branch_ids:
        branch_week_total = branch_week_totals.get(branch_id, ZERO)
        if branch_week_total <= 0:
            continue
        target_day_totals = target_day_totals_by_branch.get(branch_id)
        if not target_day_totals:
            continue

        rebalanced_branches += 1
        for day in current_dates:
            target_day_total = target_day_totals.get(day, ZERO)
            day_rows = [
                forecast
                for forecast in forecasts
                if int(forecast.branch_id) == branch_id and forecast.forecast_date == day
            ]
            if not day_rows:
                continue
            _reshape_forecast_rows_to_total(
                day_rows,
                target_total=target_day_total,
                modified=modified,
                explanation_updates={
                    "daily_curve_calibration_applied": True,
                    "daily_curve_calibration_scope": "event_daily_historical_curve",
                    "daily_curve_source": share_sources.get(branch_id, ""),
                    "homologue_mode": curve_context.get("homologue_mode", homologue_mode),
                    "homologue_start": curve_context["homologue_start"].isoformat(),
                    "homologue_end": curve_context["homologue_end"].isoformat(),
                    "homologue_main_day": curve_context["homologue_main_day"].isoformat(),
                    "target_day_share": float(
                        (target_day_total / branch_week_total).quantize(Decimal("0.0001"))
                    ) if branch_week_total > ZERO else 0.0,
                },
            )

    if modified:
        EventoVentaForecast.objects.bulk_update(
            list(modified.values()),
            ["base_demand", "event_uplift", "trend_adjustment", "final_forecast", "conservative_forecast", "aggressive_forecast", "explanation_json"],
            batch_size=500,
        )
        return [
            "Se redistribuyó la curva diaria usando cantidades históricas por sucursal/comparable y tendencia reciente por día para respetar mejor el pico y los días previos."
        ]
    return []


def _enforce_main_day_peak_floor(event: EventoVenta) -> list[str]:
    warnings: list[str] = []
    forecasts = list(
        EventoVentaForecast.objects.filter(
            sales_event=event,
            forecast_date__range=(event.analysis_start_date, event.analysis_end_date),
        ).select_related("branch")
    )
    if not forecasts:
        return warnings

    product_ids = {int(forecast.product_id) for forecast in forecasts}
    active_branch_ids = {int(forecast.branch_id) for forecast in forecasts}
    if not product_ids or not active_branch_ids:
        return warnings

    model = build_event_executive_projection_model(event, forecast_rows=forecasts)
    growth_anchor_factor = Decimal(str(model.get("growth_anchor_factor") or _event_growth_anchor_factor(model)))
    if growth_anchor_factor <= ZERO:
        return warnings

    branch_links = list(
        EventoVentaSucursal.objects.filter(sales_event=event, is_active=True)
        .select_related("branch", "comparable_branch")
        .order_by("branch__codigo")
    )
    comparable_ids = {
        int(link.comparable_branch_id)
        for link in branch_links
        if link.comparable_branch_id
    }
    all_branch_ids = active_branch_ids | comparable_ids
    homologue_start, homologue_end, homologue_main_day, homologue_mode = _select_event_homologue_window(
        event,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
    )
    hist_qty_by_branch_day = _historical_quantity_map_by_branch_day(
        start=homologue_start,
        end=homologue_end,
        product_ids=product_ids,
        branch_ids=all_branch_ids,
    )
    hist_main_qty_by_branch = {
        int(branch_id): Decimal(str(hist_qty_by_branch_day.get((int(branch_id), homologue_main_day), ZERO)))
        for branch_id in all_branch_ids
    }
    hist_week_total = sum(hist_qty_by_branch_day.values(), ZERO)
    hist_main_total = sum(hist_main_qty_by_branch.values(), ZERO)
    if hist_main_total <= ZERO:
        return warnings

    branch_week_totals: dict[int, Decimal] = defaultdict(lambda: ZERO)
    branch_main_rows: dict[int, list[EventoVentaForecast]] = defaultdict(list)
    branch_non_main_rows: dict[int, list[EventoVentaForecast]] = defaultdict(list)
    current_week_total = ZERO
    current_main_total = ZERO
    for row in forecasts:
        branch_id = int(row.branch_id)
        qty = Decimal(str(row.final_forecast or 0))
        branch_week_totals[branch_id] += qty
        current_week_total += qty
        if row.forecast_date == event.main_date:
            branch_main_rows[branch_id].append(row)
            current_main_total += qty
        else:
            branch_non_main_rows[branch_id].append(row)

    if current_week_total <= ZERO or current_main_total <= ZERO:
        return warnings

    branch_comparables = {
        int(link.branch_id): int(link.comparable_branch_id)
        for link in branch_links
        if link.comparable_branch_id
    }
    target_day_totals_by_branch, floor_day_totals_by_branch, _share_sources, curve_context = _event_daily_curve_targets(
        event=event,
        product_ids=product_ids,
        branch_ids=active_branch_ids,
        branch_week_totals=branch_week_totals,
        branch_comparables=branch_comparables,
    )

    raw_branch_targets: dict[int, Decimal] = {}
    branch_target_sources: dict[int, str] = {}
    event_main_share = (hist_main_total / hist_week_total).quantize(Decimal("0.0001")) if hist_week_total > ZERO else ZERO
    for link in branch_links:
        branch_id = int(link.branch_id)
        historical_main_qty = hist_main_qty_by_branch.get(branch_id, ZERO)
        source = "branch_historical_main_day"
        if historical_main_qty <= ZERO and link.comparable_branch_id:
            historical_main_qty = hist_main_qty_by_branch.get(int(link.comparable_branch_id), ZERO)
            if historical_main_qty > ZERO:
                source = "configured_comparable_main_day"
        if historical_main_qty <= ZERO:
            historical_main_qty = (branch_week_totals.get(branch_id, ZERO) * event_main_share).quantize(Decimal("0.001"))
            source = "event_historical_main_day_share"
        raw_branch_targets[branch_id] = max(
            ZERO,
            (historical_main_qty * growth_anchor_factor).quantize(Decimal("0.001")),
        )
        branch_target_sources[branch_id] = source

    target_main_total = sum(raw_branch_targets.values(), ZERO).quantize(Decimal("0.001"))
    main_day_benchmark_sales = _event_executive_main_day_benchmark_sales(event)
    benchmark_qty_target = ZERO
    benchmark_qty_target_source = ""
    weekly_benchmark_sales = _event_executive_benchmark_sales(event)
    if main_day_benchmark_sales > ZERO:
        from ventas.services.financials import resolve_unit_prices_bulk

        main_day_price_map = resolve_unit_prices_bulk(
            [(int(row.product_id), int(row.branch_id)) for row in forecasts if row.forecast_date == event.main_date],
            event.analysis_start_date,
            event.analysis_end_date,
        )
        implied_main_sales = ZERO
        for row in forecasts:
            if row.forecast_date != event.main_date:
                continue
            qty = Decimal(str(row.final_forecast or 0))
            unit_price = Decimal(str(main_day_price_map.get((int(row.product_id), int(row.branch_id))) or 0))
            implied_main_sales += qty * unit_price
        if implied_main_sales > ZERO and current_main_total > ZERO:
            sales_scale = (main_day_benchmark_sales / implied_main_sales).quantize(Decimal("0.000001"))
            benchmark_qty_target = (current_main_total * sales_scale).quantize(Decimal("0.001"))
            benchmark_qty_target_source = "dg_main_day_sales_scale_anchor"
            target_main_total = benchmark_qty_target
        elif weekly_benchmark_sales > ZERO:
            benchmark_share = (main_day_benchmark_sales / weekly_benchmark_sales).quantize(Decimal("0.0001"))
            benchmark_qty_target = (current_week_total * benchmark_share).quantize(Decimal("0.001"))
            benchmark_qty_target_source = "dg_main_day_share_anchor"
            target_main_total = benchmark_qty_target

    target_main_total = min(current_week_total, target_main_total).quantize(Decimal("0.001"))
    if target_main_total == current_main_total:
        return warnings

    raw_total = sum(raw_branch_targets.values(), ZERO)
    if raw_total <= ZERO:
        return warnings

    scale_factor = (target_main_total / raw_total).quantize(Decimal("0.000001"))
    modified: dict[int, EventoVentaForecast] = {}
    adjusted_branches = 0
    for branch_id, raw_target in raw_branch_targets.items():
        main_rows = branch_main_rows.get(branch_id, [])
        non_main_rows = branch_non_main_rows.get(branch_id, [])
        if not main_rows or not non_main_rows:
            continue

        current_branch_main_total = sum((Decimal(str(row.final_forecast or 0)) for row in main_rows), ZERO)
        if current_branch_main_total <= ZERO:
            continue
        current_branch_non_main_total = sum((Decimal(str(row.final_forecast or 0)) for row in non_main_rows), ZERO)
        target_branch_main_total = (raw_target * scale_factor).quantize(Decimal("0.001"))
        delta_qty = (target_branch_main_total - current_branch_main_total).quantize(Decimal("0.001"))
        if delta_qty == ZERO:
            continue

        current_day_totals = {
            day: sum(
                (Decimal(str(row.final_forecast or 0)) for row in forecasts if int(row.branch_id) == branch_id and row.forecast_date == day),
                ZERO,
            ).quantize(Decimal("0.001"))
            for day in [event.analysis_start_date + timedelta(days=offset) for offset in range((event.analysis_end_date - event.analysis_start_date).days + 1)]
        }
        protected_non_main_mins = {
            day: floor_day_totals_by_branch.get(branch_id, {}).get(day, ZERO).quantize(Decimal("0.001"))
            for day in current_day_totals
            if day != event.main_date
        }
        branch_target_day_totals = target_day_totals_by_branch.get(branch_id, {})

        if delta_qty > ZERO:
            available_by_day = {
                day: max(ZERO, (current_day_totals.get(day, ZERO) - protected_non_main_mins.get(day, ZERO))).quantize(Decimal("0.001"))
                for day in protected_non_main_mins
            }
            available_total = sum(available_by_day.values(), ZERO).quantize(Decimal("0.001"))
            move_qty = min(delta_qty, available_total)
            unresolved_qty = max(ZERO, (delta_qty - move_qty).quantize(Decimal("0.001")))
            reduced_by_day: dict[date, Decimal] = {}
            consumed = ZERO
            available_weight_total = sum(available_by_day.values(), ZERO)
            ordered_days = sorted(available_by_day.keys())
            for index, day in enumerate(ordered_days, start=1):
                available = available_by_day[day]
                if index == len(ordered_days):
                    reduction = max(ZERO, (move_qty - consumed).quantize(Decimal("0.001")))
                elif move_qty <= ZERO or available_weight_total <= ZERO or available <= ZERO:
                    reduction = ZERO
                else:
                    reduction = (move_qty * (available / available_weight_total)).quantize(Decimal("0.001"))
                    reduction = min(reduction, available)
                    consumed += reduction
                reduced_by_day[day] = reduction
            new_day_totals = {
                day: max(ZERO, (current_day_totals.get(day, ZERO) - reduced_by_day.get(day, ZERO)).quantize(Decimal("0.001")))
                for day in reduced_by_day
            }
            new_main_total = (current_branch_main_total + move_qty + unresolved_qty).quantize(Decimal("0.001"))
        else:
            move_qty = min(-delta_qty, current_branch_main_total)
            unresolved_qty = ZERO
            non_main_target_total = (current_branch_non_main_total + move_qty).quantize(Decimal("0.001"))
            non_main_target_days = {
                day: branch_target_day_totals.get(day, ZERO)
                for day in current_day_totals
                if day != event.main_date
            }
            new_day_totals = _allocate_day_totals_from_targets(
                target_total=non_main_target_total,
                target_day_totals=non_main_target_days,
                protected_minimums=protected_non_main_mins,
            )
            if not new_day_totals:
                continue
            new_main_total = (current_branch_main_total - move_qty).quantize(Decimal("0.001"))
        if move_qty <= ZERO and unresolved_qty <= ZERO:
            continue

        explanation_updates = {
            "main_day_peak_floor_applied": True,
            "main_day_peak_source": branch_target_sources.get(branch_id, "historical_main_day"),
            "main_day_peak_target_qty": float(target_branch_main_total),
            "main_day_growth_anchor_factor": float(growth_anchor_factor),
            "main_day_benchmark_sales": float(main_day_benchmark_sales or ZERO),
            "main_day_benchmark_qty_target": float(benchmark_qty_target or ZERO),
            "main_day_benchmark_qty_target_source": benchmark_qty_target_source,
            "main_day_homologue_mode": curve_context.get("homologue_mode", homologue_mode),
            "main_day_homologue_date": curve_context.get("homologue_main_day", homologue_main_day).isoformat(),
            "main_day_curve_protected": True,
            "main_day_unresolved_qty": float(unresolved_qty),
        }
        _reshape_forecast_rows_to_total(
            main_rows,
            target_total=new_main_total,
            modified=modified,
            explanation_updates=explanation_updates,
        )
        for day, day_total in new_day_totals.items():
            day_rows = [
                row
                for row in non_main_rows
                if row.forecast_date == day
            ]
            if not day_rows:
                continue
            _reshape_forecast_rows_to_total(
                day_rows,
                target_total=day_total,
                modified=modified,
                explanation_updates=explanation_updates,
            )
        adjusted_branches += 1

    if modified:
        EventoVentaForecast.objects.bulk_update(
            list(modified.values()),
            ["final_forecast", "conservative_forecast", "aggressive_forecast", "explanation_json"],
            batch_size=500,
        )
    if adjusted_branches:
        benchmark_msg = ""
        if main_day_benchmark_sales > ZERO:
            benchmark_msg = f" usando benchmark DG del día principal ${main_day_benchmark_sales.quantize(Decimal('0.01'))}"
        warnings.append(
            "Se reequilibró el día principal con cantidades por sucursal y se protegieron mínimos diarios históricos "
            f"antes de redistribuir el resto de la semana (factor {growth_anchor_factor}){benchmark_msg}."
        )
    return warnings


def _enforce_weekly_executive_ceiling(event: EventoVenta) -> list[str]:
    warnings: list[str] = []
    forecasts = list(
        EventoVentaForecast.objects.filter(
            sales_event=event,
            forecast_date__range=(event.analysis_start_date, event.analysis_end_date),
        ).select_related("branch")
    )
    if not forecasts:
        return warnings

    model = build_event_executive_projection_model(event, forecast_rows=forecasts)
    branch_targets = {
        int(branch_id): Decimal(str(target or 0)).quantize(Decimal("0.001"))
        for branch_id, target in (model.get("branch_targets") or {}).items()
    }
    target_total_qty = Decimal(str(model.get("target_total_qty") or 0)).quantize(Decimal("0.001"))
    current_total_qty = Decimal(str(model.get("current_total_qty") or 0)).quantize(Decimal("0.001"))
    if not branch_targets or target_total_qty <= ZERO or current_total_qty <= ZERO:
        return warnings
    if current_total_qty <= target_total_qty:
        return warnings

    forecasts_by_branch: dict[int, list[EventoVentaForecast]] = defaultdict(list)
    current_by_branch: dict[int, Decimal] = defaultdict(lambda: ZERO)
    current_day_totals_by_branch: dict[int, dict[date, Decimal]] = defaultdict(lambda: defaultdict(lambda: ZERO))
    branch_roles = {
        item["branch_code"]: "same_store" for item in (model.get("comparable_branches") or [])
    }
    branch_roles.update({item["branch_code"]: "expansion" for item in (model.get("new_branches") or [])})
    branch_roles.update({item["branch_code"]: "contraction" for item in (model.get("contracted_branches") or [])})
    modified: dict[int, EventoVentaForecast] = {}
    adjusted_branches = 0

    for row in forecasts:
        branch_id = int(row.branch_id)
        forecasts_by_branch[branch_id].append(row)
        qty = Decimal(str(row.final_forecast or 0))
        current_by_branch[branch_id] += qty
        current_day_totals_by_branch[branch_id][row.forecast_date] += qty

    branch_comparables = {
        int(link.branch_id): int(link.comparable_branch_id)
        for link in EventoVentaSucursal.objects.filter(sales_event=event, is_active=True).select_related("comparable_branch")
        if link.comparable_branch_id
    }
    target_curve_totals_by_branch, floor_curve_totals_by_branch, _share_sources, _curve_context = _event_daily_curve_targets(
        event=event,
        product_ids={int(row.product_id) for row in forecasts},
        branch_ids=set(branch_targets.keys()),
        branch_week_totals=branch_targets,
        branch_comparables=branch_comparables,
    )

    for branch_id, target_qty in branch_targets.items():
        rows = forecasts_by_branch.get(branch_id, [])
        current_branch_total = current_by_branch.get(branch_id, ZERO).quantize(Decimal("0.001"))
        if not rows or current_branch_total <= ZERO or target_qty <= ZERO:
            continue
        if current_branch_total <= target_qty:
            continue

        rows_by_day: dict[date, list[EventoVentaForecast]] = defaultdict(list)
        for row in rows:
            rows_by_day[row.forecast_date].append(row)
        protected_mins = {
            day: floor_curve_totals_by_branch.get(branch_id, {}).get(day, ZERO).quantize(Decimal("0.001"))
            for day in rows_by_day.keys()
        }
        current_main_day_total = current_day_totals_by_branch.get(branch_id, {}).get(event.main_date, ZERO).quantize(Decimal("0.001"))
        if any(row.explanation_json.get("main_day_peak_floor_applied") for row in rows_by_day.get(event.main_date, [])):
            protected_mins[event.main_date] = max(
                protected_mins.get(event.main_date, ZERO),
                min(current_main_day_total, target_qty).quantize(Decimal("0.001")),
            )
        protected_total = sum(protected_mins.values(), ZERO).quantize(Decimal("0.001"))
        if protected_total > target_qty:
            protected_mins = _compress_protected_minimums_to_target(
                target_total=target_qty,
                protected_minimums=protected_mins,
                locked_days={event.main_date} if protected_mins.get(event.main_date, ZERO) > ZERO else set(),
            )
            protected_total = sum(protected_mins.values(), ZERO).quantize(Decimal("0.001"))
            warnings.append(
                f"Se comprimió la curva diaria protegida de {rows[0].branch.codigo} para respetar el techo ejecutivo semanal sin soltar el día principal."
            )

        target_day_totals = target_curve_totals_by_branch.get(branch_id, {})
        redistributed_day_totals = _allocate_day_totals_from_targets(
            target_total=target_qty,
            target_day_totals=target_day_totals,
            protected_minimums=protected_mins,
        ) if target_day_totals else {}

        explanation_updates = {
            "weekly_executive_ceiling_applied": True,
            "weekly_executive_current_qty": float(current_branch_total),
            "weekly_executive_target_qty": float(target_qty),
            "weekly_executive_target_total_qty": float(target_total_qty),
            "weekly_executive_current_total_qty": float(current_total_qty),
            "weekly_executive_benchmark_source": str(model.get("benchmark_source") or ""),
            "weekly_executive_benchmark_sales": float(Decimal(str(model.get("benchmark_sales") or 0)).quantize(Decimal("0.01"))),
            "weekly_executive_same_store_factor": float(Decimal(str(model.get("same_store_factor") or ONE))),
            "weekly_executive_expansion_factor": float(Decimal(str(model.get("expansion_factor") or ZERO))),
            "weekly_executive_contraction_factor": float(Decimal(str(model.get("contraction_factor") or ONE))),
            "weekly_executive_branch_role": branch_roles.get(rows[0].branch.codigo, ""),
            "weekly_executive_reason": "executive_branch_target_ceiling",
            "weekly_executive_curve_protected": bool(redistributed_day_totals),
        }
        if redistributed_day_totals:
            for day, day_total in redistributed_day_totals.items():
                _reshape_forecast_rows_to_total(
                    rows_by_day.get(day, []),
                    target_total=day_total,
                    modified=modified,
                    explanation_updates=explanation_updates,
                )
        else:
            _reshape_forecast_rows_to_total(
                rows,
                target_total=target_qty,
                modified=modified,
                explanation_updates=explanation_updates,
            )
        adjusted_branches += 1

    if modified:
        EventoVentaForecast.objects.bulk_update(
            list(modified.values()),
            ["final_forecast", "conservative_forecast", "aggressive_forecast", "explanation_json"],
            batch_size=500,
        )
    if adjusted_branches:
        warnings.append(
            "Se aplicó el techo ejecutivo semanal por sucursal preservando una curva diaria protegida para evitar "
            f"semanas infladas sin romper los días clave ({target_total_qty} vs actual {current_total_qty})."
        )
    return warnings


def build_event_inputs(event: EventoVenta) -> ForecastInputs:
    branches: list[Sucursal] = []
    branch_comparables: dict[int, Sucursal] = {}
    branch_links = (
        EventoVentaSucursal.objects.filter(
            sales_event=event,
            is_active=True,
        )
        .exclude(branch__codigo__in=EXCLUDED_BRANCH_CODES)
        .select_related("branch", "comparable_branch")
        .order_by("branch__codigo")
    )
    for link in branch_links:
        branch = link.branch
        is_operational = branch.esta_operativa(event.main_date)
        has_signal = _branch_has_operational_signal(branch, event.main_date)
        is_canonical_point_branch = branch.codigo in POINT_MATURE_BRANCH_CODES
        is_known_point_network_branch = branch.codigo in POINT_NETWORK_BRANCH_CODES
        is_configured_expansion_branch = bool(link.comparable_branch_id)
        if (
            (is_canonical_point_branch and (is_operational or has_signal))
            or (is_configured_expansion_branch and (is_operational or has_signal))
            or has_signal
            or (is_known_point_network_branch and is_operational)
        ):
            branches.append(branch)
            if link.comparable_branch_id:
                branch_comparables[branch.id] = link.comparable_branch
    selected_products = {
        p.product_id: p.product
        for p in EventoVentaProducto.objects.filter(sales_event=event, is_active=True).select_related("product")
    }
    products: list[Receta] = []
    blocked_products: list[Receta] = []
    excluded_products: list[tuple[Receta, str]] = []
    for product in selected_products.values():
        is_executive_eligible, exclusion_reason = executive_event_product_scope(product)
        if not is_executive_eligible:
            excluded_products.append((product, exclusion_reason))
            continue
        interpretation = classify_commercial_recipe(product)
        if interpretation.clasificacion == RULE_BLOQUEADO_POR_AMBIGUEDAD:
            blocked_products.append(product)
            continue
        products.append(product)
    products = sorted(
        products,
        key=lambda product: (
            (product.familia or "").strip().lower(),
            (product.categoria or "").strip().lower(),
            (product.nombre or "").strip().lower(),
        ),
    )
    return ForecastInputs(
        event=event,
        branches=branches,
        products=products,
        branch_comparables=branch_comparables,
        blocked_products=sorted(blocked_products, key=lambda product: (product.nombre or "").strip().lower()),
        excluded_products=sorted(
            excluded_products,
            key=lambda item: ((item[0].nombre or "").strip().lower(), item[1]),
        ),
    )


def _resolve_branch_recipe_inputs(
    *,
    event: EventoVenta,
    receta: Receta,
    sucursal: Sucursal,
    day: date,
    candidate_branches: list[Sucursal],
    branch_comparables: dict[int, Sucursal],
    cache: ForecastRuntimeCache | None = None,
) -> tuple[
    list[Decimal],
    Decimal,
    Decimal,
    Decimal,
    bool,
    str,
    Sucursal | None,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    str,
    str,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    dict,
    Decimal,
    str,
    Decimal,
    Decimal,
    Decimal,
    bool,
]:
    base_values = _weekday_series(receta, sucursal, day, weeks=8, cache=cache)
    comparable_branch: Sucursal | None = None
    comparable_scale = Decimal("0")
    fallback_used = False
    base_method = "weighted_avg_weekday"

    sparse_branch = len(base_values) < 3
    allow_comparable = sucursal.codigo not in POINT_MATURE_BRANCH_CODES or sucursal.id in branch_comparables
    if sparse_branch and allow_comparable:
        comparable_branch, comparable_scale = _resolve_comparable_branch(
            sucursal,
            candidate_branches,
            day,
            configured_branch=branch_comparables.get(sucursal.id),
            cache=cache,
        )
        if comparable_branch and comparable_scale > 0:
            donor_values = _weekday_series(receta, comparable_branch, day, weeks=8, cache=cache)
            if donor_values:
                comparable_scale = min(comparable_scale, Decimal("0.95"))
                comparable_shrink = Decimal("0.72") if sucursal.codigo == "GUAMUCHIL" else Decimal("0.80")
                comparable_scale *= comparable_shrink
                base_values = [value * comparable_scale for value in donor_values]
                fallback_used = True
                base_method = "weekday_comparable_branch"

    trend_branch = comparable_branch if comparable_branch and comparable_scale > 0 else sucursal
    branch_signal_scale = comparable_scale if comparable_branch and comparable_scale > 0 else ONE
    uplift_pct = _uplift_factor(event, receta, trend_branch, cache=cache)
    branch_growth = _branch_growth_signal_cached(sucursal, day, cache=cache)
    product_velocity = _recent_sales_velocity(receta, trend_branch, day, cache=cache)
    share_shift = _recent_product_share_shift(receta, trend_branch, day, cache=cache)
    portfolio_share_shift = _portfolio_share_shift(receta, trend_branch, day, cache=cache)
    category_trend = _category_trend_signal(receta, trend_branch, day, cache=cache)
    temporality_signal = _product_temporality_signal(event, receta)
    event_anchor_qty, event_anchor_method = _event_historical_anchor(event, receta, trend_branch, day, cache=cache)
    event_anchor_qty *= branch_signal_scale
    recent_14 = [value * branch_signal_scale for value in _recent_window_series(receta, trend_branch, anchor=day, days_back=14, cache=cache)]
    recent_28 = [
        value * branch_signal_scale
        for value in _recent_window_series(receta, trend_branch, anchor=day, days_back=28, cache=cache)
    ]
    recent_56 = [
        value * branch_signal_scale
        for value in _recent_window_series(receta, trend_branch, anchor=day, days_back=56, cache=cache)
    ]
    recent_direct_avg = _window_avg(recent_14, 14)
    same_period_baseline = _same_period_baseline(receta, trend_branch, day, cache=cache) * branch_signal_scale
    family_baseline = _fallback_categoria_avg_cached(receta, trend_branch, day, cache=cache) * branch_signal_scale
    ytd_factor = _ytd_recipe_factor(receta, trend_branch, day, cache=cache)
    recent_stddev = _stddev(recent_28 or recent_56)
    (
        group_key,
        group_scope,
        _group_ids,
        share_recent,
        share_prior,
        share_delta,
        ytd_delta,
        preference_score,
        group_recent_total,
        group_prior_total,
    ) = _competitive_group_metrics(
        receta,
        trend_branch,
        day,
        product_velocity=product_velocity,
        ytd_factor=ytd_factor,
        cache=cache,
    )

    intermittent_detected = (
        branch_signal_scale == ONE
        and _nonzero_days(recent_56) > 0
        and (
            _nonzero_days(recent_56) <= 4
            or (_nonzero_days(recent_56) <= 8 and _window_avg(recent_56, 56) <= Decimal("1.20"))
        )
    )
    if intermittent_detected:
        croston_base = _croston_estimate(recent_56)
        base_demand = max(
            croston_base,
            recent_direct_avg * Decimal("0.50"),
            same_period_baseline * Decimal("0.45"),
        )
        base_method = "intermittent_croston_ytd"
    elif base_values:
        weekday_base = _weighted_avg(base_values)
        if event_anchor_qty > 0:
            base_demand = (
                (event_anchor_qty * Decimal("0.30"))
                + (weekday_base * Decimal("0.30"))
                + (recent_direct_avg * Decimal("0.25"))
                + (max(same_period_baseline, family_baseline) * Decimal("0.15"))
            )
            base_method = "event_anchor_ytd_weighted"
        else:
            base_demand = (
                (weekday_base * Decimal("0.40"))
                + (recent_direct_avg * Decimal("0.25"))
                + (same_period_baseline * Decimal("0.20"))
                + (family_baseline * Decimal("0.15"))
            )
            base_method = "weekday_ytd_weighted"
    else:
        if recent_direct_avg > 0:
            base_demand = recent_direct_avg
            base_method = "recent_direct_average"
            fallback_used = False
        else:
            base_demand = family_baseline
            base_method = "fallback_categoria"
            fallback_used = base_demand > 0
        if base_demand <= 0 and comparable_branch and comparable_scale > 0:
            donor_recent_avg = _window_avg(
                _recent_window_series(receta, comparable_branch, anchor=day, days_back=14, cache=cache),
                14,
            )
            if donor_recent_avg > 0:
                base_demand = donor_recent_avg * comparable_scale
                fallback_used = True
                base_method = "recent_comparable_branch"
            else:
                donor_categoria = _fallback_categoria_avg_cached(receta, comparable_branch, day, cache=cache)
                if donor_categoria > 0:
                    base_demand = donor_categoria * comparable_scale
                    fallback_used = True
                    base_method = "fallback_categoria_comparable"

    portfolio_preference_pct = _clamp(
        (portfolio_share_shift * Decimal("0.30")) + (category_trend * Decimal("0.22")),
        low=Decimal("-0.12"),
        high=Decimal("0.12"),
    )
    base_demand = max(ZERO, base_demand * ytd_factor * (ONE + portfolio_preference_pct))
    trend_pct = _clamp(
        (product_velocity * Decimal("0.20"))
        + (share_shift * Decimal("0.10"))
        + (portfolio_share_shift * Decimal("0.15"))
        + (category_trend * Decimal("0.18"))
        + (branch_growth * Decimal("0.20"))
        + (temporality_signal * Decimal("0.05")),
        low=Decimal("-0.10"),
        high=Decimal("0.08"),
    )
    uplift_qty = base_demand * uplift_pct * Decimal("0.10")
    trend_qty = base_demand * trend_pct * Decimal("0.20")
    raw_projection = max(ZERO, base_demand + uplift_qty + trend_qty)
    operational_multiplier, operational_details = _operational_cap_multiplier_cached(
        receta,
        sucursal,
        raw_projection,
        cache=cache,
    )
    return (
        base_values,
        base_demand,
        uplift_pct,
        trend_pct,
        fallback_used,
        base_method,
        comparable_branch,
        comparable_scale,
        branch_growth,
        product_velocity,
        share_shift,
        portfolio_share_shift,
        category_trend,
        portfolio_preference_pct,
        group_key,
        group_scope,
        share_recent,
        share_prior,
        share_delta,
        ytd_delta,
        preference_score,
        group_recent_total * branch_signal_scale,
        group_prior_total * branch_signal_scale,
        operational_multiplier,
        operational_details,
        event_anchor_qty,
        event_anchor_method,
        ytd_factor,
        same_period_baseline,
        recent_stddev,
        intermittent_detected,
    )


def generate_event_forecast(event: EventoVenta, user=None) -> dict:
    inputs = build_event_inputs(event)
    if not inputs.branches or not inputs.products:
        warnings = ["Sin sucursales o productos activos."]
        if inputs.blocked_products:
            warnings.append(
                f"Se bloquearon {len(inputs.blocked_products)} SKU ambiguos antes de generar forecast."
            )
        if inputs.excluded_products:
            warnings.append(
                f"Se excluyeron {len(inputs.excluded_products)} SKU fuera del scope ejecutivo "
                "(accesorios, bebidas o servicios)."
            )
        return {"created": 0, "warnings": warnings}

    dates = _date_range(event.analysis_start_date, event.analysis_end_date)
    created = 0
    warning_counts: dict[str, int] = defaultdict(int)
    warnings: list[str] = []
    runtime_cache = ForecastRuntimeCache()
    forecast_rows: list[EventoVentaForecast] = []
    _prime_product_group_maps(cache=runtime_cache, products=inputs.products)
    _prime_forecast_runtime_cache(event=event, inputs=inputs, dates=dates, cache=runtime_cache)

    with transaction.atomic():
        EventoVenta.objects.select_for_update().filter(pk=event.pk).exists()
        EventoVentaForecast.objects.filter(sales_event=event).delete()
        EventoVentaNotification.objects.filter(
            sales_event=event,
            message__startswith="Sin data base para ",
        ).delete()
        EventoVentaNotification.objects.filter(
            sales_event=event,
            message__icontains="combinaciones sin data directa",
        ).delete()
        EventoVentaNotification.objects.filter(
            sales_event=event,
            message__startswith="SKU bloqueado por ambiguedad:",
        ).delete()

        for sucursal in inputs.branches:
            for receta in inputs.products:
                for day in dates:
                    (
                        base_values,
                        base_demand,
                        uplift_pct,
                        trend_pct,
                        fallback_used,
                        base_method,
                        comparable_branch,
                        comparable_scale,
                        branch_growth,
                        product_velocity,
                        share_shift,
                        portfolio_share_shift,
                        category_trend,
                        portfolio_preference_pct,
                        group_key,
                        group_scope,
                        share_recent,
                        share_prior,
                        share_delta,
                        ytd_delta,
                        preference_score,
                        group_recent_total,
                        group_prior_total,
                        operational_multiplier,
                        operational_details,
                        event_anchor_qty,
                        event_anchor_method,
                        ytd_factor,
                        same_period_baseline,
                        recent_stddev,
                        intermittent_detected,
                    ) = _resolve_branch_recipe_inputs(
                        event=event,
                        receta=receta,
                        sucursal=sucursal,
                        day=day,
                        candidate_branches=inputs.branches,
                        branch_comparables=inputs.branch_comparables,
                        cache=runtime_cache,
                    )

                    uplift_qty = base_demand * uplift_pct * Decimal("0.20")
                    trend_qty = base_demand * trend_pct * Decimal("0.35")
                    unconstrained_final = max(ZERO, base_demand + uplift_qty + trend_qty)
                    final = unconstrained_final * operational_multiplier
                    if operational_multiplier < ONE:
                        trend_qty = final - base_demand - uplift_qty
                    final, starter_floor_details = _starter_branch_floor(
                        sucursal=sucursal,
                        base_method=base_method,
                        fallback_used=fallback_used,
                        comparable_scale=comparable_scale,
                        final_projection=final,
                    )
                    if starter_floor_details["starter_floor_applied"]:
                        trend_qty = final - base_demand - uplift_qty
                        operational_details = {**operational_details, **starter_floor_details}

                    interval_buffer = max(
                        recent_stddev * Decimal("0.55"),
                        final * Decimal("0.05"),
                        abs(event_anchor_qty - same_period_baseline) * Decimal("0.20"),
                    )
                    if intermittent_detected:
                        interval_buffer = max(interval_buffer, final * Decimal("0.18"))
                    if fallback_used:
                        interval_buffer = max(interval_buffer, final * Decimal("0.12"))
                    conservative = max(ZERO, final - interval_buffer)
                    aggressive = final + interval_buffer
                    confidence = _confidence_score(
                        len(base_values),
                        uplift_pct,
                        fallback_used=fallback_used,
                        product_velocity=product_velocity,
                    )
                    if intermittent_detected:
                        confidence = max(Decimal("0.18"), confidence - Decimal("0.12"))
                    if comparable_branch and comparable_scale > 0:
                        confidence = max(Decimal("0.20"), confidence - Decimal("0.15"))

                    explanation = _build_explanation(
                        base_values,
                        uplift_pct,
                        trend_pct,
                        fallback_used,
                        event=event,
                        receta=receta,
                        sucursal=sucursal,
                        base_method=base_method,
                        branch_growth=branch_growth,
                        product_velocity=product_velocity,
                        share_shift=share_shift,
                        portfolio_share_shift=portfolio_share_shift,
                        category_trend_signal=category_trend,
                        portfolio_preference_pct=portfolio_preference_pct,
                        group_key=group_key,
                        group_scope=group_scope,
                        share_recent=share_recent,
                        share_prior=share_prior,
                        share_delta=share_delta,
                        ytd_delta=ytd_delta,
                        preference_score=preference_score,
                        group_recent_total=group_recent_total,
                        group_prior_total=group_prior_total,
                        operational_multiplier=operational_multiplier,
                        operational_details=operational_details,
                        event_anchor_qty=event_anchor_qty,
                        event_anchor_method=event_anchor_method,
                        ytd_factor=ytd_factor,
                        same_period_baseline=same_period_baseline,
                        recent_stddev=recent_stddev,
                        intermittent_detected=intermittent_detected,
                        interval_buffer_qty=interval_buffer,
                        scenario_method="buffered_interval_realista",
                        comparable_branch=comparable_branch,
                        comparable_factor=comparable_scale,
                    )
                    if not base_values and not fallback_used:
                        warning_counts[sucursal.codigo] += 1

                    forecast_rows.append(
                        EventoVentaForecast(
                            sales_event=event,
                            branch=sucursal,
                            product=receta,
                            forecast_date=day,
                            base_demand=base_demand,
                            event_uplift=uplift_qty,
                            trend_adjustment=trend_qty,
                            final_forecast=final,
                            conservative_forecast=conservative,
                            aggressive_forecast=aggressive,
                            confidence_score=confidence,
                            model_version="v8-executive-same-store-expansion",
                            explanation_json=explanation,
                        )
                    )
                    created += 1

        if forecast_rows:
            _apply_group_substitution_adjustments(forecast_rows)
        if forecast_rows:
            EventoVentaForecast.objects.bulk_create(forecast_rows, batch_size=500)
        calibration_warnings = _calibrate_forecast_against_event_homologue(event)
        warnings.extend(calibration_warnings)
        warnings.extend(_apply_family_strategy_adjustments(event))
        warnings.extend(_rebalance_main_date_priority(event))
        warnings.extend(_align_forecast_to_executive_branch_model(event))
        warnings.extend(_enforce_main_day_peak_floor(event))
        for _ in range(3):
            ceiling_warnings = _enforce_weekly_executive_ceiling(event)
            warnings.extend(ceiling_warnings)
            if not ceiling_warnings:
                break

    if warning_counts:
        total_missing = sum(warning_counts.values())
        warnings.append(f"Forecast generado con {total_missing} combinaciones sin data directa; se usó fallback cuando fue posible.")
    for blocked_product in inputs.blocked_products:
        warning = f"SKU bloqueado por ambiguedad: {blocked_product.nombre}."
        warnings.append(warning)
        create_unique_notification(event, warning, severity="WARN")
    for excluded_product, exclusion_reason in inputs.excluded_products:
        warning = (
            f"SKU excluido del forecast ejecutivo: {excluded_product.nombre} "
            f"({exclusion_reason})."
        )
        warnings.append(warning)
        create_unique_notification(event, warning, severity="WARN")
    if created:
        event.status = EventoVenta.STATUS_LISTO_REVISION
        event.version += 1
        event.save(update_fields=["status", "version", "updated_at"])
        create_unique_notification(event, f"Forecast listo para revision. Se generaron {created} filas.")
    if warnings:
        create_unique_notification(
            event,
            warnings[0],
            severity="WARN",
        )

    return {"created": created, "warnings": warnings}
