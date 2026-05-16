from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any

from django.db.models import Count, Q, Sum
from django.db.models.functions import TruncMonth

from core.cache_versions import get_or_set_versioned_cache
from core.models import Sucursal
from pos_bridge.models import PointDailyBranchIndicator, PointDailySale, PointSalesDailyCategoryFact, PointSalesDailyProductFact
from recetas.models import Receta
from reportes.models import AnalyticRefreshWindow, FactVentaDiaria
from ventas.models import VentaAutoritativaPoint
from ventas.services.sales_truth import recipe_point_codes

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
LEGACY_POINT_SOURCE = "/Report/VentasCategorias"

FACT_SOURCE_PRIORITY = [
    FactVentaDiaria.SOURCE_AUTHORITATIVE,
    FactVentaDiaria.SOURCE_V2,
    FactVentaDiaria.SOURCE_LEGACY,
]


def _resolve_sucursal_id(sucursal: Sucursal | int) -> int:
    if isinstance(sucursal, Sucursal):
        return int(sucursal.id)
    return int(sucursal)


def _resolve_producto_id(producto: Receta | int | None) -> int | None:
    if producto is None:
        return None
    if isinstance(producto, Receta):
        return int(producto.id)
    return int(producto)


def _as_decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _empty_response(*, branch_id: int, target_day: date, product_id: int | None) -> dict[str, Any]:
    return {
        "cantidad": ZERO,
        "monto": ZERO,
        "source": "none",
        "source_detail": "",
        "fecha": target_day,
        "sucursal_id": branch_id,
        "producto_id": product_id,
        "rows": 0,
        "fallback_legacy_used": False,
    }


def _build_response(
    *,
    quantity,
    amount,
    source: str,
    source_detail: str,
    branch_id: int,
    target_day: date,
    product_id: int | None,
    rows: int,
) -> dict[str, Any]:
    return {
        "cantidad": _as_decimal(quantity),
        "monto": _as_decimal(amount),
        "source": source,
        "source_detail": source_detail,
        "fecha": target_day,
        "sucursal_id": branch_id,
        "producto_id": product_id,
        "rows": int(rows or 0),
        "fallback_legacy_used": source == "legacy",
    }


def _resolve_sucursal_ids(sucursales) -> list[int] | None:
    if sucursales is None:
        return None
    if isinstance(sucursales, (Sucursal, int)):
        return [_resolve_sucursal_id(sucursales)]
    ids = sorted({_resolve_sucursal_id(sucursal) for sucursal in sucursales})
    return ids


def _normalize_dates(fechas) -> list[date]:
    ordered: list[date] = []
    seen: set[date] = set()
    for value in fechas or []:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _empty_range_response(
    *,
    start_date: date,
    end_date: date,
    product_id: int | None,
    branch_ids: list[int] | None,
) -> dict[str, Any]:
    return {
        "cantidad": ZERO,
        "monto": ZERO,
        "source": "none",
        "source_detail": "",
        "start_date": start_date,
        "end_date": end_date,
        "producto_id": product_id,
        "sucursal_ids": branch_ids,
        "rows": 0,
        "coverage_days": 0,
        "coverage_branches": 0,
        "fallback_legacy_used": False,
        "coverage_accepted": False,
        "coverage_reason": "no_data",
    }


def _build_range_response(
    *,
    quantity,
    amount,
    source: str,
    source_detail: str,
    start_date: date,
    end_date: date,
    product_id: int | None,
    branch_ids: list[int] | None,
    rows: int,
    coverage_days: int,
    coverage_branches: int,
) -> dict[str, Any]:
    return {
        "cantidad": _as_decimal(quantity),
        "monto": _as_decimal(amount),
        "source": source,
        "source_detail": source_detail,
        "start_date": start_date,
        "end_date": end_date,
        "producto_id": product_id,
        "sucursal_ids": branch_ids,
        "rows": int(rows or 0),
        "coverage_days": int(coverage_days or 0),
        "coverage_branches": int(coverage_branches or 0),
        "fallback_legacy_used": source == "legacy",
        "coverage_accepted": False,
        "coverage_reason": "",
    }


def _annotate_range_response(response: dict[str, Any], *, accepted: bool, reason: str) -> dict[str, Any]:
    payload = dict(response)
    payload["coverage_accepted"] = bool(accepted)
    payload["coverage_reason"] = reason
    return payload


def _daily_selection_candidate(
    *,
    quantity,
    amount,
    source: str,
    source_detail: str,
    target_day: date,
    branch_ids: list[int] | None,
    rows: int,
    coverage_branches: int,
) -> dict[str, Any]:
    return _build_range_response(
        quantity=quantity,
        amount=amount,
        source=source,
        source_detail=source_detail,
        start_date=target_day,
        end_date=target_day,
        product_id=None,
        branch_ids=branch_ids,
        rows=rows,
        coverage_days=1 if rows > 0 else 0,
        coverage_branches=coverage_branches,
    )


def _meets_minimum_coverage(
    response: dict[str, Any],
    *,
    min_coverage_days: int | None,
    min_coverage_branches: int | None,
) -> bool:
    if min_coverage_days is not None and int(response.get("coverage_days") or 0) < int(min_coverage_days):
        return False
    if min_coverage_branches is not None and int(response.get("coverage_branches") or 0) < int(min_coverage_branches):
        return False
    return True


def _coverage_materially_better(candidate: dict[str, Any], baseline: dict[str, Any]) -> bool:
    candidate_days = int(candidate.get("coverage_days") or 0)
    candidate_branches = int(candidate.get("coverage_branches") or 0)
    baseline_days = int(baseline.get("coverage_days") or 0)
    baseline_branches = int(baseline.get("coverage_branches") or 0)
    return (
        candidate_days > baseline_days and candidate_branches >= baseline_branches
    ) or (
        candidate_branches > baseline_branches and candidate_days >= baseline_days
    )


def get_point_sales_category_totals(*, start_date: date, end_date: date, sucursal_id: int | None = None) -> list[dict]:
    queryset = PointDailySale.objects.filter(
        sale_date__gte=start_date,
        sale_date__lt=end_date,
        branch__erp_branch__isnull=False,
    )
    if sucursal_id:
        queryset = queryset.filter(branch__erp_branch_id=sucursal_id)
    return list(
        queryset.values("branch__erp_branch_id", "product__category")
        .annotate(total=Sum("quantity"))
        .order_by("branch__erp_branch_id", "product__category")
    )


def _select_range_response(
    candidates: list[dict[str, Any]],
    *,
    coverage_policy: str,
    min_coverage_days: int | None,
    min_coverage_branches: int | None,
    compare_with_lower_priority: bool,
) -> dict[str, Any] | None:
    if not candidates:
        return None
    if coverage_policy not in {"strict_priority", "prefer_complete"}:
        raise ValueError(f"coverage_policy no soportada: {coverage_policy}")

    if coverage_policy == "strict_priority":
        selected = candidates[0]
        accepted = _meets_minimum_coverage(
            selected,
            min_coverage_days=min_coverage_days,
            min_coverage_branches=min_coverage_branches,
        )
        if min_coverage_days is None and min_coverage_branches is None:
            reason = "strict_priority"
            accepted = True
        elif accepted:
            reason = "strict_priority_minimum_coverage_met"
        else:
            reason = "strict_priority_minimum_coverage_not_met"
        return _annotate_range_response(selected, accepted=accepted, reason=reason)

    baseline = candidates[0]
    selected = baseline
    compare_lower = True if coverage_policy == "prefer_complete" else bool(compare_with_lower_priority)
    if compare_lower:
        for candidate in candidates[1:]:
            if _coverage_materially_better(candidate, selected):
                selected = candidate

    accepted = _meets_minimum_coverage(
        selected,
        min_coverage_days=min_coverage_days,
        min_coverage_branches=min_coverage_branches,
    )
    if selected is baseline:
        if min_coverage_days is None and min_coverage_branches is None:
            reason = "prefer_complete_retained_priority_source"
            accepted = True
        elif accepted:
            reason = "prefer_complete_retained_priority_source"
        else:
            reason = "prefer_complete_retained_priority_source_minimum_not_met"
    else:
        if min_coverage_days is None and min_coverage_branches is None:
            reason = "prefer_complete_selected_more_complete_source"
            accepted = True
        elif accepted:
            reason = "prefer_complete_selected_more_complete_source"
        else:
            reason = "prefer_complete_selected_more_complete_source_minimum_not_met"
    return _annotate_range_response(selected, accepted=accepted, reason=reason)


def _prefer_single_legacy_source(queryset):
    official_qs = queryset.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
    if official_qs.exists():
        return official_qs, "point_daily_sale_official"

    legacy_qs = queryset.filter(source_endpoint=LEGACY_POINT_SOURCE)
    if legacy_qs.exists():
        return legacy_qs, "point_daily_sale_legacy"

    return queryset, "point_daily_sale_unknown"


def _legacy_product_filters(*, product_id: int) -> Q:
    filters = Q(receta_id=product_id)
    codes = recipe_point_codes(product_id)
    if codes:
        filters |= Q(product__sku__in=codes)
    return filters


def _fact_source_to_response(source_kind: str) -> tuple[str, str]:
    if source_kind == FactVentaDiaria.SOURCE_AUTHORITATIVE:
        return "authoritative", "fact_venta_diaria:venta_autoritativa_point"
    if source_kind == FactVentaDiaria.SOURCE_V2:
        return "v2_fact", "fact_venta_diaria:point_sales_daily_product_fact"
    if source_kind == FactVentaDiaria.SOURCE_LEGACY:
        return "legacy", "fact_venta_diaria:point_daily_sale_selected"
    return "none", "fact_venta_diaria:unknown"


def _range_day_span(*, start_date: date, end_date: date) -> int:
    return max((end_date - start_date).days + 1, 0)


def _sales_facts_are_clean(*, start_date: date, end_date: date) -> bool:
    return bool(
        get_or_set_versioned_cache(
            key_parts=[
                "erp",
                "analytics-clean",
                "sales",
                start_date.isoformat(),
                end_date.isoformat(),
            ],
            scopes=["ventas"],
            builder=lambda: not AnalyticRefreshWindow.objects.filter(
                dataset=AnalyticRefreshWindow.DATASET_SALES,
                status__in=[AnalyticRefreshWindow.STATUS_PENDING, AnalyticRefreshWindow.STATUS_ERROR],
                date_from__lte=end_date,
                date_to__gte=start_date,
            ).exists(),
            timeout=300,
        )
    )


def _analytic_fact_range_candidates(
    *,
    start_date: date,
    end_date: date,
    product_id: int | None,
    branch_ids: list[int] | None,
) -> list[dict[str, Any]]:
    queryset = FactVentaDiaria.objects.filter(fecha__range=(start_date, end_date))
    if branch_ids is not None:
        queryset = queryset.filter(sucursal_id__in=branch_ids)
    if product_id is not None:
        queryset = queryset.filter(receta_id=product_id)
    rows = list(
        queryset.values("source_kind").annotate(
            qty=Sum("cantidad"),
            amount=Sum("venta_total"),
            row_count=Count("id"),
            coverage_days=Count("fecha", distinct=True),
            coverage_branches=Count("sucursal_id", distinct=True),
        )
    )
    if not rows:
        return []
    grouped = {row["source_kind"]: row for row in rows}
    candidates: list[dict[str, Any]] = []
    for source_kind in FACT_SOURCE_PRIORITY:
        row = grouped.get(source_kind)
        if row is None:
            continue
        source, source_detail = _fact_source_to_response(source_kind)
        candidates.append(
            _build_range_response(
                quantity=row.get("qty"),
                amount=row.get("amount"),
                source=source,
                source_detail=source_detail,
                start_date=start_date,
                end_date=end_date,
                product_id=product_id,
                branch_ids=branch_ids,
                rows=row.get("row_count") or 0,
                coverage_days=row.get("coverage_days") or 0,
                coverage_branches=row.get("coverage_branches") or 0,
            )
        )
    return candidates


def _analytic_fact_read(*, branch_id: int, target_day: date, product_id: int | None) -> dict[str, Any] | None:
    candidates = _analytic_fact_range_candidates(
        start_date=target_day,
        end_date=target_day,
        product_id=product_id,
        branch_ids=[branch_id],
    )
    if not candidates:
        return None
    selected = _select_range_response(
        candidates,
        coverage_policy="strict_priority",
        min_coverage_days=None,
        min_coverage_branches=None,
        compare_with_lower_priority=False,
    )
    if selected is None:
        return None
    return _build_response(
        quantity=selected.get("cantidad"),
        amount=selected.get("monto"),
        source=selected.get("source") or "none",
        source_detail=selected.get("source_detail") or "",
        branch_id=branch_id,
        target_day=target_day,
        product_id=product_id,
        rows=selected.get("rows") or 0,
    )


def _analytic_branch_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = FactVentaDiaria.objects.filter(fecha=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(sucursal_id__in=branch_ids)
    rows = list(
        queryset.values(
            "sucursal_id",
            "sucursal__codigo",
            "sucursal__nombre",
        ).annotate(
            units=Sum("cantidad"),
            amount=Sum("venta_total"),
            tickets=Sum("tickets"),
        )
    )
    payload = []
    for row in rows:
        branch_id = row.get("sucursal_id")
        payload.append(
            {
                "key": branch_id,
                "branch_id": branch_id,
                "branch_code": row.get("sucursal__codigo") or "SIN-COD",
                "branch_name": row.get("sucursal__nombre") or "Sucursal",
                "erp_branch_id": branch_id,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "tickets": int(row.get("tickets") or 0) or None,
            }
        )
    payload.sort(key=lambda item: (str(item.get("branch_code") or ""), str(item.get("branch_name") or "")))
    return payload


def _analytic_product_day_selection(*, target_day: date, branch_ids: list[int] | None) -> dict[str, Any] | None:
    candidates = _analytic_fact_range_candidates(
        start_date=target_day,
        end_date=target_day,
        product_id=None,
        branch_ids=branch_ids,
    )
    if not candidates:
        return None
    return _select_range_response(
        candidates,
        coverage_policy="strict_priority",
        min_coverage_days=None,
        min_coverage_branches=None,
        compare_with_lower_priority=False,
    )


def _analytic_product_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = FactVentaDiaria.objects.filter(fecha=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(sucursal_id__in=branch_ids)
    rows = list(
        queryset.values(
            "point_product_id",
            "receta_id",
            "receta__nombre",
            "producto_nombre",
        ).annotate(
            units=Sum("cantidad"),
            amount=Sum("venta_total"),
            branch_count=Count("sucursal_id", distinct=True),
        )
    )
    payload = []
    for row in rows:
        product_id = row.get("point_product_id")
        recipe_id = row.get("receta_id")
        recipe_name = row.get("receta__nombre") or ""
        product_name = row.get("producto_nombre") or recipe_name or "Producto"
        payload.append(
            {
                "key": _product_row_key(product_id=product_id, recipe_id=recipe_id, product_name=product_name),
                "product_id": product_id,
                "recipe_id": recipe_id,
                "recipe_name": recipe_name or None,
                "product_name": product_name,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "branch_count": int(row.get("branch_count") or 0),
            }
        )
    payload.sort(key=lambda item: (str(item.get("recipe_name") or item.get("product_name") or ""), str(item.get("key") or "")))
    return payload


def _authoritative_read(*, branch_id: int, target_day: date, product_id: int | None) -> dict[str, Any] | None:
    queryset = VentaAutoritativaPoint.objects.filter(branch_id=branch_id, sale_date=target_day)
    if product_id is not None:
        queryset = queryset.filter(product_id=product_id)
    if not queryset.exists():
        return None

    totals = queryset.aggregate(qty=Sum("quantity"), amount=Sum("total_amount"))
    return _build_response(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="authoritative",
        source_detail="venta_autoritativa_point",
        branch_id=branch_id,
        target_day=target_day,
        product_id=product_id,
        rows=queryset.count(),
    )


def _v2_fact_read(*, branch_id: int, target_day: date, product_id: int | None) -> dict[str, Any] | None:
    if product_id is not None:
        queryset = PointSalesDailyProductFact.objects.filter(
            branch__erp_branch_id=branch_id,
            sale_date=target_day,
            receta_id=product_id,
        )
        if not queryset.exists():
            return None
        totals = queryset.aggregate(qty=Sum("total_cantidad"), amount=Sum("total_venta"))
        return _build_response(
            quantity=totals.get("qty"),
            amount=totals.get("amount"),
            source="v2_fact",
            source_detail="point_sales_daily_product_fact",
            branch_id=branch_id,
            target_day=target_day,
            product_id=product_id,
            rows=queryset.count(),
        )

    queryset = PointSalesDailyCategoryFact.objects.filter(
        branch__erp_branch_id=branch_id,
        sale_date=target_day,
    )
    if not queryset.exists():
        return None

    totals = queryset.aggregate(qty=Sum("total_cantidad"), amount=Sum("total_venta"))
    return _build_response(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="v2_fact",
        source_detail="point_sales_daily_category_fact",
        branch_id=branch_id,
        target_day=target_day,
        product_id=product_id,
        rows=queryset.count(),
    )


def _legacy_read(*, branch_id: int, target_day: date, product_id: int | None) -> dict[str, Any] | None:
    queryset = PointDailySale.objects.filter(
        sale_date=target_day,
        branch__erp_branch_id=branch_id,
    )
    if product_id is not None:
        queryset = queryset.filter(_legacy_product_filters(product_id=product_id))
    if not queryset.exists():
        return None

    queryset, source_detail = _prefer_single_legacy_source(queryset)
    totals = queryset.aggregate(qty=Sum("quantity"), amount=Sum("total_amount"))
    result = _build_response(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="legacy",
        source_detail=source_detail,
        branch_id=branch_id,
        target_day=target_day,
        product_id=product_id,
        rows=queryset.count(),
    )
    logger.debug(
        "sales_read_service legacy fallback used",
        extra={
            "sucursal_id": branch_id,
            "fecha": target_day.isoformat(),
            "producto_id": product_id,
            "source_detail": source_detail,
            "rows": result["rows"],
        },
    )
    return result


def _authoritative_range_read(
    *,
    start_date: date,
    end_date: date,
    product_id: int | None,
    branch_ids: list[int] | None,
) -> dict[str, Any] | None:
    queryset = VentaAutoritativaPoint.objects.filter(sale_date__range=(start_date, end_date))
    if branch_ids is not None:
        queryset = queryset.filter(branch_id__in=branch_ids)
    if product_id is not None:
        queryset = queryset.filter(product_id=product_id)
    rows = queryset.count()
    if rows <= 0:
        return None

    totals = queryset.aggregate(
        qty=Sum("quantity"),
        amount=Sum("total_amount"),
        coverage_days=Count("sale_date", distinct=True),
        coverage_branches=Count("branch_id", distinct=True),
    )
    return _build_range_response(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="authoritative",
        source_detail="venta_autoritativa_point",
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
        rows=rows,
        coverage_days=totals.get("coverage_days") or 0,
        coverage_branches=totals.get("coverage_branches") or 0,
    )


def _v2_fact_range_read(
    *,
    start_date: date,
    end_date: date,
    product_id: int | None,
    branch_ids: list[int] | None,
) -> dict[str, Any] | None:
    if product_id is not None:
        queryset = PointSalesDailyProductFact.objects.filter(
            sale_date__range=(start_date, end_date),
            receta_id=product_id,
        )
        source_detail = "point_sales_daily_product_fact"
    else:
        queryset = PointSalesDailyCategoryFact.objects.filter(
            sale_date__range=(start_date, end_date),
        )
        source_detail = "point_sales_daily_category_fact"
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    rows = queryset.count()
    if rows <= 0:
        return None

    totals = queryset.aggregate(
        qty=Sum("total_cantidad"),
        amount=Sum("total_venta"),
        coverage_days=Count("sale_date", distinct=True),
        coverage_branches=Count("branch__erp_branch_id", distinct=True),
    )
    return _build_range_response(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="v2_fact",
        source_detail=source_detail,
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
        rows=rows,
        coverage_days=totals.get("coverage_days") or 0,
        coverage_branches=totals.get("coverage_branches") or 0,
    )


def _legacy_range_read(
    *,
    start_date: date,
    end_date: date,
    product_id: int | None,
    branch_ids: list[int] | None,
) -> dict[str, Any] | None:
    queryset = PointDailySale.objects.filter(sale_date__range=(start_date, end_date))
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    if product_id is not None:
        queryset = queryset.filter(_legacy_product_filters(product_id=product_id))
    rows = queryset.count()
    if rows <= 0:
        return None

    queryset, source_detail = _prefer_single_legacy_source(queryset)
    rows = queryset.count()
    totals = queryset.aggregate(
        qty=Sum("quantity"),
        amount=Sum("total_amount"),
        coverage_days=Count("sale_date", distinct=True),
        coverage_branches=Count("branch__erp_branch_id", distinct=True),
    )
    result = _build_range_response(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="legacy",
        source_detail=source_detail,
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
        rows=rows,
        coverage_days=totals.get("coverage_days") or 0,
        coverage_branches=totals.get("coverage_branches") or 0,
    )
    logger.debug(
        "sales_read_service legacy fallback used",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "sucursal_ids": branch_ids,
            "producto_id": product_id,
            "source_detail": source_detail,
            "rows": result["rows"],
        },
    )
    return result


def _authoritative_branch_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = VentaAutoritativaPoint.objects.filter(sale_date=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(branch_id__in=branch_ids)
    rows = list(
        queryset.values(
            "branch_id",
            "branch__codigo",
            "branch__nombre",
        ).annotate(
            units=Sum("quantity"),
            amount=Sum("total_amount"),
        )
    )
    payload = []
    for row in rows:
        branch_id = row.get("branch_id")
        payload.append(
            {
                "key": branch_id,
                "branch_id": branch_id,
                "branch_code": row.get("branch__codigo") or "SIN-COD",
                "branch_name": row.get("branch__nombre") or "Sucursal",
                "erp_branch_id": branch_id,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "tickets": None,
            }
        )
    payload.sort(key=lambda item: (str(item.get("branch_code") or ""), str(item.get("branch_name") or "")))
    return payload


def _v2_branch_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = PointSalesDailyCategoryFact.objects.filter(
        sale_date=target_day,
        branch__erp_branch_id__isnull=False,
    )
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    rows = list(
        queryset.values(
            "branch__erp_branch_id",
            "branch__erp_branch__codigo",
            "branch__erp_branch__nombre",
        ).annotate(
            units=Sum("total_cantidad"),
            amount=Sum("total_venta"),
        )
    )
    payload = []
    for row in rows:
        branch_id = row.get("branch__erp_branch_id")
        payload.append(
            {
                "key": branch_id,
                "branch_id": branch_id,
                "branch_code": row.get("branch__erp_branch__codigo") or "SIN-COD",
                "branch_name": row.get("branch__erp_branch__nombre") or "Sucursal",
                "erp_branch_id": branch_id,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "tickets": None,
            }
        )
    payload.sort(key=lambda item: (str(item.get("branch_code") or ""), str(item.get("branch_name") or "")))
    return payload


def _legacy_branch_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = PointDailySale.objects.filter(
        sale_date=target_day,
        branch__erp_branch_id__isnull=False,
    )
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    queryset, _ = _prefer_single_legacy_source(queryset)
    rows = list(
        queryset.values(
            "branch__erp_branch_id",
            "branch__erp_branch__codigo",
            "branch__erp_branch__nombre",
        ).annotate(
            units=Sum("quantity"),
            amount=Sum("total_amount"),
            tickets=Sum("tickets"),
        )
    )
    payload = []
    for row in rows:
        branch_id = row.get("branch__erp_branch_id")
        payload.append(
            {
                "key": branch_id,
                "branch_id": branch_id,
                "branch_code": row.get("branch__erp_branch__codigo") or "SIN-COD",
                "branch_name": row.get("branch__erp_branch__nombre") or "Sucursal",
                "erp_branch_id": branch_id,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "tickets": int(row.get("tickets") or 0),
            }
        )
    payload.sort(key=lambda item: (str(item.get("branch_code") or ""), str(item.get("branch_name") or "")))
    return payload


def _branch_indicator_map_for_day(*, target_day: date, branch_ids: list[int] | None) -> dict[int, dict[str, Any]]:
    queryset = PointDailyBranchIndicator.objects.filter(
        indicator_date=target_day,
        branch__erp_branch_id__isnull=False,
    )
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    rows = queryset.values("branch__erp_branch_id").annotate(
        amount=Sum("total_amount"),
        tickets=Sum("total_tickets"),
    )
    return {
        int(row["branch__erp_branch_id"]): {
            "amount": _as_decimal(row.get("amount")),
            "tickets": int(row.get("tickets") or 0),
        }
        for row in rows
        if row.get("branch__erp_branch_id") is not None
    }


def _authoritative_product_day_selection(*, target_day: date, branch_ids: list[int] | None) -> dict[str, Any] | None:
    queryset = VentaAutoritativaPoint.objects.filter(sale_date=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(branch_id__in=branch_ids)
    rows = queryset.count()
    if rows <= 0:
        return None
    totals = queryset.aggregate(
        qty=Sum("quantity"),
        amount=Sum("total_amount"),
        coverage_branches=Count("branch_id", distinct=True),
    )
    return _daily_selection_candidate(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="authoritative",
        source_detail="venta_autoritativa_point",
        target_day=target_day,
        branch_ids=branch_ids,
        rows=rows,
        coverage_branches=totals.get("coverage_branches") or 0,
    )


def _v2_product_day_selection(*, target_day: date, branch_ids: list[int] | None) -> dict[str, Any] | None:
    queryset = PointSalesDailyProductFact.objects.filter(sale_date=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    rows = queryset.count()
    if rows <= 0:
        return None
    totals = queryset.aggregate(
        qty=Sum("total_cantidad"),
        amount=Sum("total_venta"),
        coverage_branches=Count("branch__erp_branch_id", distinct=True),
    )
    return _daily_selection_candidate(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="v2_fact",
        source_detail="point_sales_daily_product_fact",
        target_day=target_day,
        branch_ids=branch_ids,
        rows=rows,
        coverage_branches=totals.get("coverage_branches") or 0,
    )


def _legacy_product_day_selection(*, target_day: date, branch_ids: list[int] | None) -> dict[str, Any] | None:
    queryset = PointDailySale.objects.filter(sale_date=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    rows = queryset.count()
    if rows <= 0:
        return None
    queryset, source_detail = _prefer_single_legacy_source(queryset)
    rows = queryset.count()
    totals = queryset.aggregate(
        qty=Sum("quantity"),
        amount=Sum("total_amount"),
        coverage_branches=Count("branch__erp_branch_id", distinct=True),
    )
    result = _daily_selection_candidate(
        quantity=totals.get("qty"),
        amount=totals.get("amount"),
        source="legacy",
        source_detail=source_detail,
        target_day=target_day,
        branch_ids=branch_ids,
        rows=rows,
        coverage_branches=totals.get("coverage_branches") or 0,
    )
    logger.debug(
        "sales_read_service legacy fallback used",
        extra={
            "start_date": target_day.isoformat(),
            "end_date": target_day.isoformat(),
            "sucursal_ids": branch_ids,
            "producto_id": None,
            "source_detail": source_detail,
            "rows": result["rows"],
        },
    )
    return result


def _select_product_day_source(
    *,
    target_day: date,
    branch_ids: list[int] | None,
    coverage_policy: str,
) -> dict[str, Any]:
    candidates = []
    analytic = _analytic_product_day_selection(target_day=target_day, branch_ids=branch_ids)
    if analytic is not None:
        if _sales_facts_are_clean(start_date=target_day, end_date=target_day):
            return _annotate_range_response(analytic, accepted=True, reason="analytic_fact")
        candidates.append(analytic)
    authoritative = _authoritative_product_day_selection(target_day=target_day, branch_ids=branch_ids)
    if authoritative is not None:
        candidates.append(authoritative)
    rebuilt = _v2_product_day_selection(target_day=target_day, branch_ids=branch_ids)
    if rebuilt is not None:
        candidates.append(rebuilt)
    legacy = _legacy_product_day_selection(target_day=target_day, branch_ids=branch_ids)
    if legacy is not None:
        candidates.append(legacy)

    selected = _select_range_response(
        candidates,
        coverage_policy=coverage_policy,
        min_coverage_days=None,
        min_coverage_branches=None,
        compare_with_lower_priority=False,
    )
    if selected is not None:
        return selected
    return _empty_range_response(
        start_date=target_day,
        end_date=target_day,
        product_id=None,
        branch_ids=branch_ids,
    )


def _product_row_key(*, product_id: int | None, recipe_id: int | None, product_name: str) -> str:
    if product_id is not None:
        return f"point:{product_id}"
    if recipe_id is not None:
        return f"recipe:{recipe_id}"
    return f"name:{product_name}"


def _authoritative_product_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = VentaAutoritativaPoint.objects.filter(sale_date=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(branch_id__in=branch_ids)
    rows = list(
        queryset.values(
            "product_id",
            "product__nombre",
            "product_code",
            "point_name",
        ).annotate(
            units=Sum("quantity"),
            amount=Sum("total_amount"),
            branch_count=Count("branch", distinct=True),
        )
    )
    payload = []
    for row in rows:
        recipe_id = row.get("product_id")
        recipe_name = row.get("product__nombre") or ""
        product_name = row.get("point_name") or recipe_name or row.get("product_code") or "Producto"
        payload.append(
            {
                "key": _product_row_key(product_id=None, recipe_id=recipe_id, product_name=product_name),
                "product_id": None,
                "recipe_id": recipe_id,
                "recipe_name": recipe_name or None,
                "product_name": product_name,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "branch_count": int(row.get("branch_count") or 0),
            }
        )
    payload.sort(key=lambda item: (str(item.get("recipe_name") or item.get("product_name") or ""), str(item.get("key") or "")))
    return payload


def _v2_product_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = PointSalesDailyProductFact.objects.filter(sale_date=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    rows = list(
        queryset.values(
            "point_product_id",
            "receta_id",
            "receta__nombre",
            "producto_nombre_historico",
        ).annotate(
            units=Sum("total_cantidad"),
            amount=Sum("total_venta"),
            branch_count=Count("branch", distinct=True),
        )
    )
    payload = []
    for row in rows:
        product_id = row.get("point_product_id")
        recipe_id = row.get("receta_id")
        recipe_name = row.get("receta__nombre") or ""
        product_name = row.get("producto_nombre_historico") or recipe_name or "Producto"
        payload.append(
            {
                "key": _product_row_key(product_id=product_id, recipe_id=recipe_id, product_name=product_name),
                "product_id": product_id,
                "recipe_id": recipe_id,
                "recipe_name": recipe_name or None,
                "product_name": product_name,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "branch_count": int(row.get("branch_count") or 0),
            }
        )
    payload.sort(key=lambda item: (str(item.get("recipe_name") or item.get("product_name") or ""), str(item.get("key") or "")))
    return payload


def _legacy_product_rows_for_day(*, target_day: date, branch_ids: list[int] | None) -> list[dict[str, Any]]:
    queryset = PointDailySale.objects.filter(sale_date=target_day)
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    queryset, _ = _prefer_single_legacy_source(queryset)
    rows = list(
        queryset.values(
            "product_id",
            "receta_id",
            "receta__nombre",
            "product__name",
        ).annotate(
            units=Sum("quantity"),
            amount=Sum("total_amount"),
            branch_count=Count("branch", distinct=True),
        )
    )
    payload = []
    for row in rows:
        product_id = row.get("product_id")
        recipe_id = row.get("receta_id")
        recipe_name = row.get("receta__nombre") or ""
        product_name = row.get("product__name") or recipe_name or "Producto"
        payload.append(
            {
                "key": _product_row_key(product_id=product_id, recipe_id=recipe_id, product_name=product_name),
                "product_id": product_id,
                "recipe_id": recipe_id,
                "recipe_name": recipe_name or None,
                "product_name": product_name,
                "units": _as_decimal(row.get("units")),
                "amount": _as_decimal(row.get("amount")),
                "branch_count": int(row.get("branch_count") or 0),
            }
        )
    payload.sort(key=lambda item: (str(item.get("recipe_name") or item.get("product_name") or ""), str(item.get("key") or "")))
    return payload


def get_daily_sales(sucursal: Sucursal | int, fecha: date, producto: Receta | int | None = None) -> dict[str, Any]:
    branch_id = _resolve_sucursal_id(sucursal)
    product_id = _resolve_producto_id(producto)

    analytic = _analytic_fact_read(branch_id=branch_id, target_day=fecha, product_id=product_id)
    if analytic is not None and _sales_facts_are_clean(start_date=fecha, end_date=fecha):
        return analytic

    authoritative = _authoritative_read(branch_id=branch_id, target_day=fecha, product_id=product_id)
    if authoritative is not None:
        return authoritative

    rebuilt = _v2_fact_read(branch_id=branch_id, target_day=fecha, product_id=product_id)
    if rebuilt is not None:
        return rebuilt

    legacy = _legacy_read(branch_id=branch_id, target_day=fecha, product_id=product_id)
    if legacy is not None:
        return legacy

    return _empty_response(branch_id=branch_id, target_day=fecha, product_id=product_id)


def get_sales_range(
    *,
    start_date: date,
    end_date: date,
    producto: Receta | int | None = None,
    sucursales=None,
    coverage_policy: str = "strict_priority",
    min_coverage_days: int | None = None,
    min_coverage_branches: int | None = None,
    compare_with_lower_priority: bool = False,
) -> dict[str, Any]:
    product_id = _resolve_producto_id(producto)
    branch_ids = _resolve_sucursal_ids(sucursales)
    candidates = _analytic_fact_range_candidates(
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
    )
    analytic_selected = _select_range_response(
        candidates,
        coverage_policy=coverage_policy,
        min_coverage_days=min_coverage_days,
        min_coverage_branches=min_coverage_branches,
        compare_with_lower_priority=compare_with_lower_priority,
    )
    if analytic_selected is not None and _sales_facts_are_clean(start_date=start_date, end_date=end_date):
        return _annotate_range_response(analytic_selected, accepted=True, reason="analytic_fact")

    authoritative = _authoritative_range_read(
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
    )
    if authoritative is not None:
        candidates.append(authoritative)

    rebuilt = _v2_fact_range_read(
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
    )
    if rebuilt is not None:
        candidates.append(rebuilt)

    legacy = _legacy_range_read(
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
    )
    if legacy is not None:
        candidates.append(legacy)

    selected = _select_range_response(
        candidates,
        coverage_policy=coverage_policy,
        min_coverage_days=min_coverage_days,
        min_coverage_branches=min_coverage_branches,
        compare_with_lower_priority=compare_with_lower_priority,
    )
    if selected is not None:
        return selected

    return _empty_range_response(
        start_date=start_date,
        end_date=end_date,
        product_id=product_id,
        branch_ids=branch_ids,
    )


def get_daily_sales_bulk(
    *,
    fechas,
    dimension: str = "branch",
    sucursales=None,
    include_indicators: bool = False,
    coverage_policy: str = "strict_priority",
) -> dict[str, Any]:
    if dimension not in {"branch", "product"}:
        raise ValueError(f"dimension no soportada: {dimension}")

    branch_ids = _resolve_sucursal_ids(sucursales)
    ordered_dates = _normalize_dates(fechas)
    payload: dict[str, Any] = {
        "dimension": dimension,
        "dates": {},
    }

    for target_day in ordered_dates:
        selected = get_sales_range(
            start_date=target_day,
            end_date=target_day,
            sucursales=branch_ids,
            coverage_policy=coverage_policy,
        )
        if dimension == "branch":
            analytic_rows = _analytic_branch_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            if analytic_rows and _sales_facts_are_clean(start_date=target_day, end_date=target_day):
                rows = analytic_rows
            elif selected["source"] == "authoritative":
                rows = _authoritative_branch_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            elif selected["source"] == "v2_fact":
                rows = _v2_branch_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            elif selected["source"] == "legacy":
                rows = _legacy_branch_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            else:
                rows = []
        else:
            selected = _select_product_day_source(
                target_day=target_day,
                branch_ids=branch_ids,
                coverage_policy=coverage_policy,
            )
            analytic_rows = _analytic_product_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            if analytic_rows and _sales_facts_are_clean(start_date=target_day, end_date=target_day):
                rows = analytic_rows
            elif selected["source"] == "authoritative":
                rows = _authoritative_product_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            elif selected["source"] == "v2_fact":
                rows = _v2_product_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            elif selected["source"] == "legacy":
                rows = _legacy_product_rows_for_day(target_day=target_day, branch_ids=branch_ids)
            else:
                rows = []

        day_payload: dict[str, Any] = {
            "source": selected["source"],
            "source_detail": selected["source_detail"],
            "coverage_accepted": selected["coverage_accepted"],
            "coverage_reason": selected["coverage_reason"],
            "rows": rows,
        }
        if include_indicators and dimension == "branch":
            day_payload["indicator_map"] = _branch_indicator_map_for_day(target_day=target_day, branch_ids=branch_ids)

        payload["dates"][target_day.isoformat()] = day_payload

    return payload


def _fact_source_to_kind(source: str) -> str | None:
    if source == "authoritative":
        return FactVentaDiaria.SOURCE_AUTHORITATIVE
    if source == "v2_fact":
        return FactVentaDiaria.SOURCE_V2
    if source == "legacy":
        return FactVentaDiaria.SOURCE_LEGACY
    return None


def _grouped_rows_from_analytic_facts(
    *,
    start_date: date,
    end_date: date,
    dimension: str,
    branch_ids: list[int] | None,
    source: str,
) -> list[dict[str, Any]]:
    source_kind = _fact_source_to_kind(source)
    if source_kind is None:
        return []
    queryset = FactVentaDiaria.objects.filter(
        fecha__range=(start_date, end_date),
        source_kind=source_kind,
    )
    if branch_ids is not None:
        queryset = queryset.filter(sucursal_id__in=branch_ids)
    if dimension == "branch":
        rows = queryset.values("sucursal_id", "sucursal__codigo", "sucursal__nombre").annotate(
            total_sales=Sum("venta_total"),
            total_quantity=Sum("cantidad"),
            total_tickets=Sum("tickets"),
        )
        return [
            {
                "branch_id": row["sucursal_id"],
                "branch_code": row["sucursal__codigo"] or "",
                "branch_name": row["sucursal__nombre"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": int(row["total_tickets"] or 0),
            }
            for row in rows
        ]
    if dimension == "product":
        rows = queryset.values("receta_id", "receta__nombre", "producto_nombre").annotate(
            total_sales=Sum("venta_total"),
            total_quantity=Sum("cantidad"),
        )
        return [
            {
                "recipe_id": row["receta_id"],
                "recipe_name": row["receta__nombre"] or "",
                "product_name": row["producto_nombre"] or row["receta__nombre"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
            }
            for row in rows
        ]
    if dimension == "month":
        rows = (
            queryset.annotate(month=TruncMonth("fecha"))
            .values("month")
            .annotate(
                total_sales=Sum("venta_total"),
                total_quantity=Sum("cantidad"),
                total_tickets=Sum("tickets"),
            )
            .order_by("month")
        )
        return [
            {
                "period": row["month"].strftime("%Y-%m"),
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": int(row["total_tickets"] or 0),
            }
            for row in rows
        ]
    raise ValueError(f"dimension no soportada: {dimension}")


def _grouped_rows_from_authoritative(
    *,
    start_date: date,
    end_date: date,
    dimension: str,
    branch_ids: list[int] | None,
) -> list[dict[str, Any]]:
    queryset = VentaAutoritativaPoint.objects.filter(sale_date__range=(start_date, end_date))
    if branch_ids is not None:
        queryset = queryset.filter(branch_id__in=branch_ids)
    if dimension == "branch":
        rows = queryset.values("branch_id", "branch__codigo", "branch__nombre").annotate(
            total_sales=Sum("total_amount"),
            total_quantity=Sum("quantity"),
        )
        return [
            {
                "branch_id": row["branch_id"],
                "branch_code": row["branch__codigo"] or "",
                "branch_name": row["branch__nombre"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": 0,
            }
            for row in rows
        ]
    if dimension == "product":
        rows = queryset.values("product_id", "product__nombre", "point_name", "product_code").annotate(
            total_sales=Sum("total_amount"),
            total_quantity=Sum("quantity"),
        )
        return [
            {
                "recipe_id": row["product_id"],
                "recipe_name": row["product__nombre"] or "",
                "product_name": row["point_name"] or row["product__nombre"] or row["product_code"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
            }
            for row in rows
        ]
    if dimension == "month":
        rows = (
            queryset.annotate(month=TruncMonth("sale_date"))
            .values("month")
            .annotate(
                total_sales=Sum("total_amount"),
                total_quantity=Sum("quantity"),
            )
            .order_by("month")
        )
        return [
            {
                "period": row["month"].strftime("%Y-%m"),
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": 0,
            }
            for row in rows
        ]
    raise ValueError(f"dimension no soportada: {dimension}")


def _grouped_rows_from_v2(
    *,
    start_date: date,
    end_date: date,
    dimension: str,
    branch_ids: list[int] | None,
) -> list[dict[str, Any]]:
    if dimension == "branch":
        queryset = PointSalesDailyCategoryFact.objects.filter(sale_date__range=(start_date, end_date))
        if branch_ids is not None:
            queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
        rows = queryset.values("branch__erp_branch_id", "branch__erp_branch__codigo", "branch__erp_branch__nombre").annotate(
            total_sales=Sum("total_venta"),
            total_quantity=Sum("total_cantidad"),
        )
        return [
            {
                "branch_id": row["branch__erp_branch_id"],
                "branch_code": row["branch__erp_branch__codigo"] or "",
                "branch_name": row["branch__erp_branch__nombre"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": 0,
            }
            for row in rows
        ]
    queryset = PointSalesDailyProductFact.objects.filter(sale_date__range=(start_date, end_date))
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    if dimension == "product":
        rows = queryset.values("receta_id", "receta__nombre", "producto_nombre_historico").annotate(
            total_sales=Sum("total_venta"),
            total_quantity=Sum("total_cantidad"),
        )
        return [
            {
                "recipe_id": row["receta_id"],
                "recipe_name": row["receta__nombre"] or "",
                "product_name": row["producto_nombre_historico"] or row["receta__nombre"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
            }
            for row in rows
        ]
    if dimension == "month":
        rows = (
            queryset.annotate(month=TruncMonth("sale_date"))
            .values("month")
            .annotate(
                total_sales=Sum("total_venta"),
                total_quantity=Sum("total_cantidad"),
            )
            .order_by("month")
        )
        return [
            {
                "period": row["month"].strftime("%Y-%m"),
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": 0,
            }
            for row in rows
        ]
    raise ValueError(f"dimension no soportada: {dimension}")


def _grouped_rows_from_legacy(
    *,
    start_date: date,
    end_date: date,
    dimension: str,
    branch_ids: list[int] | None,
) -> list[dict[str, Any]]:
    queryset = PointDailySale.objects.filter(sale_date__range=(start_date, end_date))
    if branch_ids is not None:
        queryset = queryset.filter(branch__erp_branch_id__in=branch_ids)
    queryset, _ = _prefer_single_legacy_source(queryset)
    if dimension == "branch":
        rows = queryset.values("branch__erp_branch_id", "branch__erp_branch__codigo", "branch__erp_branch__nombre").annotate(
            total_sales=Sum("total_amount"),
            total_quantity=Sum("quantity"),
            total_tickets=Sum("tickets"),
        )
        return [
            {
                "branch_id": row["branch__erp_branch_id"],
                "branch_code": row["branch__erp_branch__codigo"] or "",
                "branch_name": row["branch__erp_branch__nombre"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": int(row["total_tickets"] or 0),
            }
            for row in rows
        ]
    if dimension == "product":
        rows = queryset.values("receta_id", "receta__nombre", "product__name").annotate(
            total_sales=Sum("total_amount"),
            total_quantity=Sum("quantity"),
        )
        return [
            {
                "recipe_id": row["receta_id"],
                "recipe_name": row["receta__nombre"] or "",
                "product_name": row["product__name"] or row["receta__nombre"] or "",
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
            }
            for row in rows
        ]
    if dimension == "month":
        rows = (
            queryset.annotate(month=TruncMonth("sale_date"))
            .values("month")
            .annotate(
                total_sales=Sum("total_amount"),
                total_quantity=Sum("quantity"),
                total_tickets=Sum("tickets"),
            )
            .order_by("month")
        )
        return [
            {
                "period": row["month"].strftime("%Y-%m"),
                "total_sales": _as_decimal(row["total_sales"]),
                "total_quantity": _as_decimal(row["total_quantity"]),
                "total_tickets": int(row["total_tickets"] or 0),
            }
            for row in rows
        ]
    raise ValueError(f"dimension no soportada: {dimension}")


def get_sales_range_grouped(
    *,
    start_date: date,
    end_date: date,
    dimension: str,
    sucursales=None,
    limit: int | None = None,
    coverage_policy: str = "prefer_complete",
) -> dict[str, Any]:
    if dimension not in {"branch", "product", "month"}:
        raise ValueError(f"dimension no soportada: {dimension}")
    branch_ids = _resolve_sucursal_ids(sucursales)
    selection = get_sales_range(
        start_date=start_date,
        end_date=end_date,
        sucursales=branch_ids,
        coverage_policy=coverage_policy,
    )
    if selection["coverage_reason"] == "analytic_fact":
        rows = _grouped_rows_from_analytic_facts(
            start_date=start_date,
            end_date=end_date,
            dimension=dimension,
            branch_ids=branch_ids,
            source=selection["source"],
        )
    elif selection["source"] == "authoritative":
        rows = _grouped_rows_from_authoritative(
            start_date=start_date,
            end_date=end_date,
            dimension=dimension,
            branch_ids=branch_ids,
        )
    elif selection["source"] == "v2_fact":
        rows = _grouped_rows_from_v2(
            start_date=start_date,
            end_date=end_date,
            dimension=dimension,
            branch_ids=branch_ids,
        )
    elif selection["source"] == "legacy":
        rows = _grouped_rows_from_legacy(
            start_date=start_date,
            end_date=end_date,
            dimension=dimension,
            branch_ids=branch_ids,
        )
    else:
        rows = []

    if dimension == "branch":
        rows.sort(key=lambda item: (-item["total_sales"], item["branch_name"], item["branch_code"]))
    elif dimension == "product":
        rows.sort(key=lambda item: (-item["total_sales"], item["product_name"], item["recipe_name"]))
    else:
        rows.sort(key=lambda item: item["period"])

    if limit is not None:
        rows = rows[:limit]

    return {
        "source": selection["source"],
        "source_detail": selection["source_detail"],
        "coverage_accepted": selection["coverage_accepted"],
        "coverage_reason": selection["coverage_reason"],
        "rows": rows,
    }
