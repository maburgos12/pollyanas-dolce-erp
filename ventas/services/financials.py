from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from collections import defaultdict
import re

from django.db.models import Count, Max, Sum

from pos_bridge.models import PointDailySale, PointSalesDailyProductFact
from recetas.models import Receta, VentaHistorica
from recetas.utils.commercial_composition import (
    RULE_BLOQUEADO_POR_AMBIGUEDAD,
    CommercialRecipeLookupContext,
    get_commercial_total_cost_map,
    resolve_commercial_sku_interpretation,
)
from ventas.models import EventoVenta, EventoVentaFinancial, EventoVentaForecast, VentaAutoritativaPoint
from ventas.services.forecasting import (
    _event_executive_main_day_benchmark_sales,
    _select_event_homologue_window,
    build_event_executive_projection_model,
)
from ventas.services.notifications import create_unique_notification
from ventas.services.sales_read_service import get_sales_range


def _as_decimal(value) -> Decimal:
    return Decimal(str(value or 0))


PRICE_COVERAGE_MIN = Decimal("0.85")
COST_COVERAGE_MIN = Decimal("0.85")
OFFICIAL_POINT_PRICE_ENDPOINT = "/Report/PrintReportes?idreporte=3"
CURRENT_POINT_PRICE_LOOKBACK_DAYS = 21
CURRENT_POINT_PRICE_OUTLIER_TOLERANCE = Decimal("0.15")
EVENT_REVENUE_PRICE_LIFT_CAP = Decimal("1.12")
EVENT_REVENUE_MIN_HIST_QTY = Decimal("150")
EVENT_REVENUE_MIN_HIST_SALES = Decimal("25000")
EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE = Decimal("0.050")


def _quantize_price(value) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.0001"))


def _event_executive_benchmark_sales(event: EventoVenta) -> Decimal:
    notes = (event.objective_notes or "").strip()
    if not notes:
        return Decimal("0")
    for line in notes.splitlines():
        normalized = re.sub(r"\s+", " ", line.strip().lower())
        if "benchmark" not in normalized or "dg" not in normalized:
            continue
        if "dia principal" in normalized or "día principal" in normalized or "dia fuerte" in normalized or "día fuerte" in normalized or "main day" in normalized:
            continue
        match = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", line)
        if not match:
            continue
        raw = match.group(1).replace(",", "").strip()
        try:
            return Decimal(raw).quantize(Decimal("0.01"))
        except Exception:
            continue
    return Decimal("0")


def _unit_price_from_totals(*, qty, sales) -> Decimal:
    qty_dec = _as_decimal(qty)
    sales_dec = _as_decimal(sales)
    if qty_dec > 0 and sales_dec > 0:
        return _quantize_price(sales_dec / qty_dec)
    return Decimal("0")


def _aggregate_unit_price(*, receta_id: int, start_date, end_date, use_point: bool, branch_id: int | None = None) -> Decimal:
    if use_point:
        aggregate = get_sales_range(
            start_date=start_date,
            end_date=end_date,
            producto=receta_id,
            sucursales=[branch_id] if branch_id else None,
            coverage_policy="prefer_complete",
        )
        qty = _as_decimal(aggregate.get("cantidad"))
        sales = _as_decimal(aggregate.get("monto"))
    else:
        filters = {
            "receta_id": receta_id,
            "fecha__range": (start_date, end_date),
        }
        if branch_id:
            filters["sucursal_id"] = branch_id
        aggregate = VentaHistorica.objects.filter(**filters).aggregate(qty=Sum("cantidad"), sales=Sum("monto_total"))
        qty = _as_decimal(aggregate.get("qty"))
        sales = _as_decimal(aggregate.get("sales"))
    return _unit_price_from_totals(qty=qty, sales=sales)


def _aggregate_fact_unit_price(*, receta_id: int, start_date, end_date, branch_id: int | None = None) -> Decimal:
    filters = {
        "receta_id": receta_id,
        "sale_date__range": (start_date, end_date),
        "total_cantidad__gt": 0,
        "total_venta__gt": 0,
    }
    if branch_id:
        filters["branch__erp_branch_id"] = branch_id
    aggregate = PointSalesDailyProductFact.objects.filter(**filters).aggregate(qty=Sum("total_cantidad"), sales=Sum("total_venta"))
    return _unit_price_from_totals(qty=aggregate.get("qty"), sales=aggregate.get("sales"))


def _aggregate_authoritative_unit_price(*, receta_id: int, start_date, end_date, branch_id: int | None = None) -> Decimal:
    filters = {
        "product_id": receta_id,
        "sale_date__range": (start_date, end_date),
        "quantity__gt": 0,
        "total_amount__gt": 0,
    }
    if branch_id:
        filters["branch_id"] = branch_id
    aggregate = VentaAutoritativaPoint.objects.filter(**filters).aggregate(qty=Sum("quantity"), sales=Sum("total_amount"))
    return _unit_price_from_totals(qty=aggregate.get("qty"), sales=aggregate.get("sales"))


def _aggregate_official_point_unit_price(*, receta_id: int, start_date, end_date, branch_id: int | None = None) -> Decimal:
    filters = {
        "receta_id": receta_id,
        "sale_date__range": (start_date, end_date),
        "quantity__gt": 0,
        "total_amount__gt": 0,
        "source_endpoint": OFFICIAL_POINT_PRICE_ENDPOINT,
    }
    if branch_id:
        filters["branch__erp_branch_id"] = branch_id
    aggregate = PointDailySale.objects.filter(**filters).aggregate(qty=Sum("quantity"), sales=Sum("total_amount"))
    return _unit_price_from_totals(qty=aggregate.get("qty"), sales=aggregate.get("sales"))


def _range_day_count(start_date, end_date) -> int:
    if not start_date or not end_date:
        return 0
    return max((end_date - start_date).days + 1, 0)


def _coverage_ratio(*, covered_days: int, start_date, end_date) -> Decimal:
    requested_days = _range_day_count(start_date, end_date)
    if requested_days <= 0:
        return Decimal("0")
    return (Decimal(covered_days) / Decimal(requested_days)).quantize(Decimal("0.0001"))


def _aggregate_fact_unit_price_stats(*, receta_id: int, start_date, end_date, branch_id: int | None = None) -> tuple[Decimal, Decimal, int]:
    filters = {
        "receta_id": receta_id,
        "sale_date__range": (start_date, end_date),
        "total_cantidad__gt": 0,
        "total_venta__gt": 0,
    }
    if branch_id:
        filters["branch__erp_branch_id"] = branch_id
    aggregate = PointSalesDailyProductFact.objects.filter(**filters).aggregate(
        qty=Sum("total_cantidad"),
        sales=Sum("total_venta"),
        days=Count("sale_date", distinct=True),
    )
    return (
        _unit_price_from_totals(qty=aggregate.get("qty"), sales=aggregate.get("sales")),
        _coverage_ratio(covered_days=int(aggregate.get("days") or 0), start_date=start_date, end_date=end_date),
        int(aggregate.get("days") or 0),
    )


def _aggregate_authoritative_unit_price_stats(
    *, receta_id: int, start_date, end_date, branch_id: int | None = None
) -> tuple[Decimal, Decimal, int]:
    filters = {
        "product_id": receta_id,
        "sale_date__range": (start_date, end_date),
        "quantity__gt": 0,
        "total_amount__gt": 0,
    }
    if branch_id:
        filters["branch_id"] = branch_id
    aggregate = VentaAutoritativaPoint.objects.filter(**filters).aggregate(
        qty=Sum("quantity"),
        sales=Sum("total_amount"),
        days=Count("sale_date", distinct=True),
    )
    return (
        _unit_price_from_totals(qty=aggregate.get("qty"), sales=aggregate.get("sales")),
        _coverage_ratio(covered_days=int(aggregate.get("days") or 0), start_date=start_date, end_date=end_date),
        int(aggregate.get("days") or 0),
    )


def _aggregate_official_point_unit_price_stats(
    *, receta_id: int, start_date, end_date, branch_id: int | None = None
) -> tuple[Decimal, Decimal, int]:
    filters = {
        "receta_id": receta_id,
        "sale_date__range": (start_date, end_date),
        "quantity__gt": 0,
        "total_amount__gt": 0,
        "source_endpoint": OFFICIAL_POINT_PRICE_ENDPOINT,
    }
    if branch_id:
        filters["branch__erp_branch_id"] = branch_id
    aggregate = PointDailySale.objects.filter(**filters).aggregate(
        qty=Sum("quantity"),
        sales=Sum("total_amount"),
        days=Count("sale_date", distinct=True),
    )
    return (
        _unit_price_from_totals(qty=aggregate.get("qty"), sales=aggregate.get("sales")),
        _coverage_ratio(covered_days=int(aggregate.get("days") or 0), start_date=start_date, end_date=end_date),
        int(aggregate.get("days") or 0),
    )


def _last_observed_sale_date(receta_id: int):
    historico_last = VentaHistorica.objects.filter(receta_id=receta_id).aggregate(last=Max("fecha")).get("last")
    authoritative_last = VentaAutoritativaPoint.objects.filter(product_id=receta_id).aggregate(last=Max("sale_date")).get("last")
    rebuilt_last = PointSalesDailyProductFact.objects.filter(receta_id=receta_id).aggregate(last=Max("sale_date")).get("last")
    official_last = (
        PointDailySale.objects.filter(
            receta_id=receta_id,
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )
        .aggregate(last=Max("sale_date"))
        .get("last")
    )
    point_last = PointDailySale.objects.filter(receta_id=receta_id).aggregate(last=Max("sale_date")).get("last")
    candidates = [candidate for candidate in (historico_last, authoritative_last, rebuilt_last, official_last, point_last) if candidate]
    return max(candidates) if candidates else None


def _latest_point_sales_rows(*, receta_id: int, branch_id: int | None = None) -> list[dict]:
    filters = {
        "receta_id": receta_id,
        "quantity__gt": 0,
        "total_amount__gt": 0,
        "source_endpoint": OFFICIAL_POINT_PRICE_ENDPOINT,
    }
    if branch_id:
        filters["branch__erp_branch_id"] = branch_id
    qs = PointDailySale.objects.filter(**filters)
    latest_date = qs.aggregate(last=Max("sale_date")).get("last")
    if not latest_date:
        return []
    start_date = latest_date - timedelta(days=CURRENT_POINT_PRICE_LOOKBACK_DAYS)
    return list(
        qs.filter(sale_date__range=(start_date, latest_date))
        .values("sale_date", "quantity", "total_amount")
        .order_by("-sale_date", "-id")
    )


def _dominant_point_price_from_rows(rows: list[dict]) -> tuple[Decimal, int]:
    price_stats: dict[Decimal, dict[str, object]] = {}
    for row in rows:
        unit_price = _unit_price_from_totals(qty=row.get("quantity"), sales=row.get("total_amount"))
        if unit_price <= 0:
            continue
        entry = price_stats.setdefault(
            unit_price,
            {"occurrences": 0, "qty": Decimal("0"), "latest_date": row.get("sale_date")},
        )
        entry["occurrences"] = int(entry["occurrences"]) + 1
        entry["qty"] = Decimal(entry["qty"]) + _as_decimal(row.get("quantity"))
        if row.get("sale_date") and row.get("sale_date") > entry["latest_date"]:
            entry["latest_date"] = row.get("sale_date")
    if not price_stats:
        return Decimal("0"), 0
    price, stats = max(
        price_stats.items(),
        key=lambda item: (
            int(item[1]["occurrences"]),
            Decimal(item[1]["qty"]),
            item[1]["latest_date"],
            item[0],
        ),
    )
    return price, int(stats["occurrences"])


def _current_point_unit_price(*, receta_id: int, branch_id: int | None = None) -> Decimal:
    global_rows = _latest_point_sales_rows(receta_id=receta_id)
    global_price, _ = _dominant_point_price_from_rows(global_rows)
    if branch_id is None:
        return global_price

    branch_rows = _latest_point_sales_rows(receta_id=receta_id, branch_id=branch_id)
    branch_price, branch_occurrences = _dominant_point_price_from_rows(branch_rows)
    if branch_price <= 0:
        return global_price
    if global_price <= 0 or branch_occurrences >= 2:
        return branch_price
    diff_ratio = abs(branch_price - global_price) / global_price if global_price > 0 else Decimal("0")
    if diff_ratio > CURRENT_POINT_PRICE_OUTLIER_TOLERANCE:
        return global_price
    return branch_price


def _current_point_price_maps(
    *,
    receta_ids: set[int],
    branch_ids: set[int] | None = None,
) -> tuple[dict[int, Decimal], dict[tuple[int, int], tuple[Decimal, int]]]:
    if not receta_ids:
        return {}, {}
    filters = {
        "receta_id__in": receta_ids,
        "quantity__gt": 0,
        "total_amount__gt": 0,
        "source_endpoint": OFFICIAL_POINT_PRICE_ENDPOINT,
    }
    qs = PointDailySale.objects.filter(**filters)
    latest_by_recipe = dict(qs.values_list("receta_id").annotate(last=Max("sale_date")))
    if not latest_by_recipe:
        return {}, {}
    start_date = min(latest - timedelta(days=CURRENT_POINT_PRICE_LOOKBACK_DAYS) for latest in latest_by_recipe.values())
    end_date = max(latest_by_recipe.values())
    requested_branches = set(branch_ids or [])
    global_rows: dict[int, list[dict]] = defaultdict(list)
    branch_rows: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for row in qs.filter(sale_date__range=(start_date, end_date)).values(
        "receta_id",
        "sale_date",
        "quantity",
        "total_amount",
        "branch__erp_branch_id",
    ):
        receta_id = int(row["receta_id"])
        latest = latest_by_recipe.get(receta_id)
        if not latest or row["sale_date"] < latest - timedelta(days=CURRENT_POINT_PRICE_LOOKBACK_DAYS):
            continue
        global_rows[receta_id].append(row)
        branch_id = row.get("branch__erp_branch_id")
        if requested_branches and branch_id in requested_branches:
            branch_rows[(receta_id, int(branch_id))].append(row)
    global_price_map = {
        receta_id: _dominant_point_price_from_rows(rows)[0]
        for receta_id, rows in global_rows.items()
    }
    branch_price_stats = {
        key: _dominant_point_price_from_rows(rows)
        for key, rows in branch_rows.items()
    }
    return global_price_map, branch_price_stats


def resolve_unit_prices_bulk(
    receta_ids: set[int],
    reference_start,
    reference_end,
    *,
    branch_ids: set[int] | None = None,
    commercial_context: CommercialRecipeLookupContext | None = None,
) -> dict[tuple[int, int], Decimal]:
    if not receta_ids or not branch_ids:
        return {}
    recipe_map = {
        recipe.id: recipe
        for recipe in Receta.objects.filter(id__in=receta_ids).only("id", "nombre", "codigo_point")
    }
    pricing_candidates: dict[int, list[int]] = {}
    current_candidate_ids: set[int] = set()
    blocked_products: set[int] = set()
    for receta_id, recipe in recipe_map.items():
        interpretation = resolve_commercial_sku_interpretation(
            recipe,
            context=commercial_context,
        )
        if interpretation.classification == RULE_BLOQUEADO_POR_AMBIGUEDAD:
            blocked_products.add(receta_id)
            pricing_candidates[receta_id] = []
            continue
        candidate_ids = list(interpretation.pricing_receta_ids)
        pricing_candidates[receta_id] = candidate_ids
        current_candidate_ids.update(candidate_ids)

    global_price_map, branch_price_stats = _current_point_price_maps(
        receta_ids=current_candidate_ids,
        branch_ids=branch_ids,
    )
    resolved: dict[tuple[int, int], Decimal] = {}
    unresolved_keys: list[tuple[int, int, tuple[int, ...]]] = []
    for receta_id in receta_ids:
        for branch_id in branch_ids:
            key = (receta_id, branch_id)
            if receta_id in blocked_products:
                resolved[key] = Decimal("0")
                continue
            price = Decimal("0")
            for candidate_id in pricing_candidates.get(receta_id, [receta_id]):
                global_price = global_price_map.get(candidate_id, Decimal("0"))
                branch_price, branch_occurrences = branch_price_stats.get((candidate_id, branch_id), (Decimal("0"), 0))
                if branch_price <= 0:
                    candidate_price = global_price
                elif global_price <= 0 or branch_occurrences >= 2:
                    candidate_price = branch_price
                else:
                    diff_ratio = abs(branch_price - global_price) / global_price if global_price > 0 else Decimal("0")
                    candidate_price = global_price if diff_ratio > CURRENT_POINT_PRICE_OUTLIER_TOLERANCE else branch_price
                if candidate_price > 0:
                    price = candidate_price.quantize(Decimal("0.0001"))
                    break
            if price <= 0:
                unresolved_keys.append(
                    (receta_id, branch_id, tuple(pricing_candidates.get(receta_id, [receta_id])))
                )
                continue
            resolved[key] = price

    if unresolved_keys:
        observed_end_by_recipe = {
            candidate_id: _last_observed_sale_date(candidate_id)
            for candidate_id in {
                candidate_id
                for _receta_id, _branch_id, candidate_ids in unresolved_keys
                for candidate_id in candidate_ids
            }
        }
        historical_price_cache: dict[tuple[tuple[int, ...], int], Decimal] = {}
        for receta_id, branch_id, candidate_ids in unresolved_keys:
            cache_key = (candidate_ids, int(branch_id))
            price = historical_price_cache.get(cache_key)
            if price is None:
                price = resolve_unit_price(
                    receta_id,
                    reference_start,
                    reference_end,
                    branch_id=branch_id,
                    candidate_recipe_ids=candidate_ids,
                    skip_current_lookup=True,
                    observed_end_by_recipe=observed_end_by_recipe,
                )
                historical_price_cache[cache_key] = price
            resolved[(receta_id, branch_id)] = price
    return resolved


def resolve_unit_price(
    receta_id: int,
    reference_start,
    reference_end,
    *,
    branch_id: int | None = None,
    candidate_recipe_ids: tuple[int, ...] | list[int] | None = None,
    skip_current_lookup: bool = False,
    observed_end_by_recipe: dict[int, object] | None = None,
) -> Decimal:
    recipe_ids = [int(recipe_id) for recipe_id in (candidate_recipe_ids or []) if int(recipe_id or 0) > 0]
    if not recipe_ids:
        receta = Receta.objects.filter(id=receta_id).only("id", "nombre", "codigo_point").first()
        recipe_ids = [receta_id]
        if receta is not None:
            interpretation = resolve_commercial_sku_interpretation(receta)
            if interpretation.classification == RULE_BLOQUEADO_POR_AMBIGUEDAD:
                return Decimal("0")
            recipe_ids = list(interpretation.pricing_receta_ids)

    observed_candidates = [
        candidate
        for candidate in (
            (observed_end_by_recipe or {}).get(candidate_id)
            if observed_end_by_recipe is not None
            else _last_observed_sale_date(candidate_id)
            for candidate_id in recipe_ids
        )
        if candidate
    ]
    observed_end = max(observed_candidates) if observed_candidates else None
    current_lookup_allowed = not skip_current_lookup
    if current_lookup_allowed and reference_end and observed_end:
        current_lookup_allowed = reference_end >= (observed_end - timedelta(days=CURRENT_POINT_PRICE_LOOKBACK_DAYS))
    if current_lookup_allowed:
        for candidate_id in recipe_ids:
            current_price = _current_point_unit_price(receta_id=candidate_id, branch_id=branch_id)
            if current_price > 0:
                return current_price.quantize(Decimal("0.0001"))

    candidate_ranges = []
    if observed_end:
        candidate_ranges.extend(
            [
                (observed_end - timedelta(days=90), observed_end),
                (observed_end - timedelta(days=365), observed_end),
            ]
        )
    if reference_start and reference_end:
        candidate_ranges.append((reference_start - timedelta(days=365), reference_end - timedelta(days=365)))

    seen = set()
    for start_date, end_date in candidate_ranges:
        if not start_date or not end_date:
            continue
        key = (start_date, end_date)
        if key in seen:
            continue
        seen.add(key)
        best_partial_price = Decimal("0")
        best_partial_ratio = Decimal("-1")
        best_partial_days = -1
        best_partial_priority = 999
        for candidate_id in recipe_ids:
            for priority, resolver in enumerate(
                (
                    _aggregate_authoritative_unit_price_stats,
                    _aggregate_fact_unit_price_stats,
                    _aggregate_official_point_unit_price_stats,
                )
            ):
                price, coverage_ratio, covered_days = resolver(
                    receta_id=candidate_id,
                    start_date=start_date,
                    end_date=end_date,
                    branch_id=branch_id,
                )
                if price > 0:
                    if coverage_ratio >= PRICE_COVERAGE_MIN:
                        return price.quantize(Decimal("0.0001"))
                    is_better_partial = (
                        coverage_ratio > best_partial_ratio
                        or (
                            coverage_ratio == best_partial_ratio
                            and (
                                covered_days > best_partial_days
                                or (covered_days == best_partial_days and priority < best_partial_priority)
                            )
                        )
                    )
                    if is_better_partial:
                        best_partial_price = price
                        best_partial_ratio = coverage_ratio
                        best_partial_days = covered_days
                        best_partial_priority = priority
            historical_price = _aggregate_unit_price(
                receta_id=candidate_id,
                start_date=start_date,
                end_date=end_date,
                use_point=False,
                branch_id=branch_id,
            )
            if historical_price > 0:
                if best_partial_price > 0:
                    return best_partial_price.quantize(Decimal("0.0001"))
                return historical_price.quantize(Decimal("0.0001"))
        if best_partial_price > 0:
            return best_partial_price.quantize(Decimal("0.0001"))
    return Decimal("0")


def evaluate_event_revenue_plausibility(
    event: EventoVenta,
    *,
    product_ids: set[int],
    branch_ids: set[int],
    projected_qty: Decimal,
    projected_sales: Decimal,
) -> dict[str, object]:
    projected_qty = _as_decimal(projected_qty)
    projected_sales = _as_decimal(projected_sales)
    payload: dict[str, object] = {
        "applied": False,
        "flagged": False,
        "reason": "not_applicable",
        "projected_qty": projected_qty.quantize(Decimal("0.001")) if projected_qty else Decimal("0.000"),
        "projected_sales": projected_sales.quantize(Decimal("0.01")) if projected_sales else Decimal("0.00"),
        "reference_sales_ceiling": projected_sales.quantize(Decimal("0.01")) if projected_sales else Decimal("0.00"),
        "cap_factor": Decimal("1.000000"),
        "historical_qty": Decimal("0.000"),
        "historical_sales": Decimal("0.00"),
        "historical_avg_price": Decimal("0.0000"),
        "current_avg_price": Decimal("0.0000"),
        "avg_price_ratio": Decimal("0.0000"),
        "quantity_ratio": Decimal("0.0000"),
        "allowed_price_lift_cap": EVENT_REVENUE_PRICE_LIFT_CAP,
        "homologue_mode": "",
        "homologue_start": None,
        "homologue_end": None,
        "homologue_main_day": None,
        "benchmark_sales_override": Decimal("0.00"),
        "benchmark_source": "",
    }
    if not product_ids or not branch_ids or projected_qty <= 0 or projected_sales <= 0:
        return payload

    homologue_start, homologue_end, homologue_main_day, homologue_mode = _select_event_homologue_window(
        event,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    aggregate = VentaHistorica.objects.filter(
        fecha__range=(homologue_start, homologue_end),
        receta_id__in=product_ids,
        sucursal_id__in=branch_ids,
    ).aggregate(qty=Sum("cantidad"), sales=Sum("monto_total"))
    historical_qty = _as_decimal(aggregate.get("qty"))
    historical_sales = _as_decimal(aggregate.get("sales"))
    payload.update(
        {
            "historical_qty": historical_qty.quantize(Decimal("0.001")) if historical_qty else Decimal("0.000"),
            "historical_sales": historical_sales.quantize(Decimal("0.01")) if historical_sales else Decimal("0.00"),
            "homologue_mode": homologue_mode,
            "homologue_start": homologue_start,
            "homologue_end": homologue_end,
            "homologue_main_day": homologue_main_day,
        }
    )
    if historical_qty < EVENT_REVENUE_MIN_HIST_QTY or historical_sales < EVENT_REVENUE_MIN_HIST_SALES:
        payload["reason"] = "insufficient_historical_sales"
        return payload

    historical_avg_price = historical_sales / historical_qty
    current_avg_price = projected_sales / projected_qty
    quantity_ratio = projected_qty / historical_qty
    avg_price_ratio = current_avg_price / historical_avg_price if historical_avg_price > 0 else Decimal("0")
    reference_sales_ceiling = (historical_sales * quantity_ratio * EVENT_REVENUE_PRICE_LIFT_CAP).quantize(Decimal("0.01"))
    payload.update(
        {
            "historical_avg_price": historical_avg_price.quantize(Decimal("0.0001")),
            "current_avg_price": current_avg_price.quantize(Decimal("0.0001")),
            "avg_price_ratio": avg_price_ratio.quantize(Decimal("0.0001")),
            "quantity_ratio": quantity_ratio.quantize(Decimal("0.0001")),
            "reference_sales_ceiling": reference_sales_ceiling,
        }
    )
    benchmark_sales_override = _event_executive_benchmark_sales(event)
    if benchmark_sales_override > 0:
        payload.update(
            {
                "benchmark_sales_override": benchmark_sales_override,
                "benchmark_source": "objective_notes",
                "reference_sales_ceiling": benchmark_sales_override,
            }
        )
        if projected_sales <= benchmark_sales_override:
            payload["reason"] = "within_executive_benchmark_override"
            return payload
        payload.update(
            {
                "flagged": True,
                "reason": "above_executive_benchmark_override",
                "cap_factor": (benchmark_sales_override / projected_sales).quantize(Decimal("0.000001")),
            }
        )
        return payload
    if projected_sales <= reference_sales_ceiling or avg_price_ratio <= EVENT_REVENUE_PRICE_LIFT_CAP:
        payload["reason"] = "within_plausible_band"
        return payload

    payload.update(
        {
            "flagged": True,
            "reason": "avg_price_above_homologue_reference",
            "cap_factor": (reference_sales_ceiling / projected_sales).quantize(Decimal("0.000001")),
        }
    )
    return payload


def reconcile_event_revenue_plausibility(
    *,
    event: EventoVenta,
    plausibility: dict[str, object],
    executive_model: dict[str, object] | None = None,
) -> dict[str, object]:
    if not plausibility or not plausibility.get("flagged"):
        return plausibility
    if plausibility.get("benchmark_source") == "objective_notes":
        return plausibility

    model = executive_model or {}
    target_total_qty = _as_decimal(model.get("target_total_qty"))
    current_total_qty = _as_decimal(model.get("current_total_qty"))
    main_day_benchmark_sales = _event_executive_main_day_benchmark_sales(event)
    if (
        main_day_benchmark_sales > 0
        and target_total_qty > 0
        and current_total_qty > 0
        and current_total_qty <= (target_total_qty + EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE)
    ):
        payload = dict(plausibility)
        payload["flagged"] = False
        payload["reason"] = "within_qty_target_with_dg_main_day_benchmark"
        payload["qty_target_total"] = target_total_qty.quantize(Decimal("0.001"))
        payload["qty_current_total"] = current_total_qty.quantize(Decimal("0.001"))
        payload["qty_target_tolerance"] = EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE.quantize(Decimal("0.001"))
        payload["main_day_benchmark_sales"] = main_day_benchmark_sales.quantize(Decimal("0.01"))
        return payload
    return plausibility


def build_financials(event: EventoVenta) -> dict:
    forecasts = EventoVentaForecast.objects.filter(sales_event=event)
    if not forecasts.exists():
        return {"created": 0, "warnings": ["No hay forecast calculado."]}

    valid_scenarios = ("CONSERVADOR", "BASE", "AGRESIVO")
    EventoVentaFinancial.objects.filter(sales_event=event).exclude(scenario__in=valid_scenarios).delete()

    totals = {"CONSERVADOR": Decimal("0"), "BASE": Decimal("0"), "AGRESIVO": Decimal("0")}
    cogs = {"CONSERVADOR": Decimal("0"), "BASE": Decimal("0"), "AGRESIVO": Decimal("0")}
    scenario_qtys = {"CONSERVADOR": Decimal("0"), "BASE": Decimal("0"), "AGRESIVO": Decimal("0")}
    qty_totals = {"all": Decimal("0"), "price": Decimal("0"), "cost": Decimal("0"), "full": Decimal("0")}
    forecast_rows = list(forecasts.select_related("product"))
    product_ids = {row.product_id for row in forecast_rows}
    branch_ids = {row.branch_id for row in forecast_rows}
    price_cache: dict[tuple[int, int], Decimal] = {}
    cost_cache = get_commercial_total_cost_map(product_ids)

    for row in forecast_rows:
        price_key = (row.product_id, row.branch_id)
        avg_price = price_cache.get(price_key)
        if avg_price is None:
            avg_price = resolve_unit_price(
                row.product_id,
                event.analysis_start_date,
                event.analysis_end_date,
                branch_id=row.branch_id,
            )
            price_cache[price_key] = avg_price
        costo_unit = cost_cache.get(row.product_id, Decimal("0"))
        qty = _as_decimal(row.final_forecast)
        qty_totals["all"] += qty
        scenario_qtys["BASE"] += qty
        scenario_qtys["CONSERVADOR"] += _as_decimal(row.conservative_forecast)
        scenario_qtys["AGRESIVO"] += _as_decimal(row.aggressive_forecast)
        if avg_price > 0:
            qty_totals["price"] += qty
        if costo_unit > 0:
            qty_totals["cost"] += qty
        if avg_price > 0 and costo_unit > 0:
            qty_totals["full"] += qty
        totals["BASE"] += avg_price * row.final_forecast
        totals["CONSERVADOR"] += avg_price * row.conservative_forecast
        totals["AGRESIVO"] += avg_price * row.aggressive_forecast
        cogs["BASE"] += costo_unit * row.final_forecast
        cogs["CONSERVADOR"] += costo_unit * row.conservative_forecast
        cogs["AGRESIVO"] += costo_unit * row.aggressive_forecast

    created = 0
    low_margin_scenarios: list[str] = []
    plausibility_warnings: list[str] = []
    executive_model = build_event_executive_projection_model(event, forecast_rows=forecast_rows)
    for scenario in valid_scenarios:
        plausibility = reconcile_event_revenue_plausibility(
            event=event,
            executive_model=executive_model,
            plausibility=evaluate_event_revenue_plausibility(
                event,
                product_ids=product_ids,
                branch_ids=branch_ids,
                projected_qty=scenario_qtys[scenario],
                projected_sales=totals[scenario],
            ),
        )
        sales = totals[scenario]
        cost = cogs[scenario]
        profit = sales - cost
        margin = (profit / sales * Decimal("100")) if sales > 0 else Decimal("0")
        if margin < Decimal("15"):
            low_margin_scenarios.append(scenario)
        if plausibility.get("flagged"):
            plausibility_warnings.append(
                f"{scenario}: ingreso calculado con precio real x piezas = {sales.quantize(Decimal('0.01'))}; "
                f"referencia ejecutiva del homólogo {plausibility['homologue_start']}→{plausibility['homologue_end']} "
                f"({plausibility['homologue_mode']}) = {Decimal(str(plausibility['reference_sales_ceiling'])).quantize(Decimal('0.01'))}."
            )
        EventoVentaFinancial.objects.update_or_create(
            sales_event=event,
            scenario=scenario,
            defaults={
                "estimated_sales": sales,
                "estimated_cogs": cost,
                "estimated_gross_profit": profit,
                "estimated_margin": margin,
                "incremental_investment": cost,
                "break_even_sales": cost,
                "expected_roi": (profit / cost * Decimal("100")) if cost > 0 else Decimal("0"),
            },
        )
        created += 1

    create_unique_notification(event, f"Resumen financiero generado para {created} escenarios.")
    create_unique_notification(
        event,
        "La utilidad y el ROI del evento se muestran en lectura bruta sobre costo directo; no sustituyen una rentabilidad operativa total.",
    )
    if plausibility_warnings:
        create_unique_notification(
            event,
            "Alerta de plausibilidad financiera: el ingreso se conserva como precio real x piezas forecast; "
            "se reporta referencia ejecutiva contra homólogo del evento: "
            + " | ".join(plausibility_warnings),
            severity="WARN",
        )
    if qty_totals["all"] > 0:
        price_coverage = qty_totals["price"] / qty_totals["all"]
        cost_coverage = qty_totals["cost"] / qty_totals["all"]
        if price_coverage < PRICE_COVERAGE_MIN:
            create_unique_notification(
                event,
                f"Valorización comercial con cobertura de precio insuficiente ({(price_coverage * Decimal('100')).quantize(Decimal('0.01'))}%).",
                severity="WARN",
            )
        if cost_coverage < COST_COVERAGE_MIN:
            create_unique_notification(
                event,
                f"Valorización financiera con cobertura de costo insuficiente ({(cost_coverage * Decimal('100')).quantize(Decimal('0.01'))}%).",
                severity="WARN",
            )
    if low_margin_scenarios:
        create_unique_notification(
            event,
            f"Margen esperado bajo en escenarios: {', '.join(low_margin_scenarios)}.",
            severity="WARN",
        )
    return {"created": created, "warnings": plausibility_warnings}
