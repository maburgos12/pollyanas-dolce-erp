from __future__ import annotations

import hashlib
import json
import re
from datetime import date, timedelta
from decimal import Decimal

from django.conf import settings
from django.db.models import Count, F, Max, OuterRef, Q, Subquery, Sum
from django.db.models.functions import Coalesce, TruncMonth
from django.utils import timezone

from core.audit import log_event
from pos_bridge.models import (
    PointBranch,
    PointDailySale,
    PointInventorySnapshot,
    PointMonthlySalesOfficial,
    PointProduct,
    PointRecipeNode,
)
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from pos_bridge.utils.logger import get_pos_bridge_logger
from recetas.models import LineaReceta, Receta, VentaHistorica
from recetas.utils.addon_grouping import (
    approved_addons_for_recipe,
    calculate_grouped_addon_cost,
    resolve_grouped_rule,
    resolve_receta_from_term,
)
from recetas.utils.normalizacion import normalizar_nombre

logger = get_pos_bridge_logger()

ZERO = Decimal("0")

INTENT_KEYWORDS = {
    "sales_summary": ("venta", "ventas", "vendimos", "ingreso", "ingresos", "facturacion", "facturo"),
    "sales_by_branch": ("sucursal", "sucursales", "branch", "tienda", "tiendas", "matriz", "leyva", "colosio", "crucero"),
    "sales_by_product": ("producto", "productos", "top", "mas vendido", "mas vendidos", "ranking"),
    "sales_trend": ("tendencia", "mes a mes", "mensual", "comparar", "vs", "versus"),
    "inventory": ("inventario", "stock", "existencia", "existencias", "disponibilidad"),
    "recipe": ("receta", "bom", "ingrediente", "ingredientes", "formula", "composicion"),
    "low_stock": ("bajo stock", "faltante", "faltantes", "agotado", "agotados", "reabastecer", "resurtir", "minimo"),
}

MONTH_NAMES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

MANUAL_Q1_2026_START = date(2026, 1, 1)
MANUAL_Q1_2026_END = date(2026, 3, 13)
MANUAL_Q1_2026_SOURCE = "POINT_HIST_2026_Q1"
POINT_BRIDGE_SOURCE = "POINT_BRIDGE_SALES"
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"


def _strip_code_fences(raw: str) -> str:
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()
    return cleaned


def classify_intent(query: str) -> str:
    normalized_query = normalizar_nombre(query)
    if any(trigger in normalized_query for trigger in ("receta", "ingrediente", "ingredientes", "formula", "composicion", "costeo")):
        return "recipe"
    scores: dict[str, int] = {}
    for intent, keywords in INTENT_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in normalized_query)
        if score:
            scores[intent] = score
    if not scores:
        return "general"
    return max(scores, key=scores.get)


def _extract_date_range(query: str) -> tuple[date, date]:
    normalized_query = normalizar_nombre(query)
    today = timezone.localdate()

    if "hoy" in normalized_query:
        return today, today
    if "ayer" in normalized_query:
        yesterday = today - timedelta(days=1)
        return yesterday, yesterday
    if "esta semana" in normalized_query:
        start = today - timedelta(days=today.weekday())
        return start, today
    if "este mes" in normalized_query:
        return today.replace(day=1), today
    if "mes pasado" in normalized_query or "mes anterior" in normalized_query:
        first_this_month = today.replace(day=1)
        last_prev_month = first_this_month - timedelta(days=1)
        first_prev_month = last_prev_month.replace(day=1)
        return first_prev_month, last_prev_month
    if "este ano" in normalized_query or "este año" in query.lower():
        return today.replace(month=1, day=1), today

    year_match = re.search(r"\b(20\d{2})\b", query)
    if year_match and not any(month_name in normalized_query for month_name in MONTH_NAMES):
        year = int(year_match.group(1))
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        if year == today.year:
            end = today
        return start, end

    month_tokens = "|".join(sorted(MONTH_NAMES.keys(), key=len, reverse=True))
    month_matches = list(re.finditer(rf"\b({month_tokens})\b", normalized_query))
    if len(month_matches) >= 2 and any(token in normalized_query for token in (" hasta ", " a ", " al ")):
        years = [int(value) for value in re.findall(r"\b(20\d{2})\b", query)]
        start_month_name = month_matches[0].group(1)
        end_month_name = month_matches[-1].group(1)
        start_year = years[0] if years else today.year
        end_year = years[1] if len(years) > 1 else start_year
        start = date(start_year, MONTH_NAMES[start_month_name], 1)
        if MONTH_NAMES[end_month_name] == 12:
            end = date(end_year, 12, 31)
        else:
            end = date(end_year, MONTH_NAMES[end_month_name] + 1, 1) - timedelta(days=1)
        return start, min(end, today)

    for month_name, month_num in MONTH_NAMES.items():
        if month_name in normalized_query:
            year = today.year
            for year_candidate in range(2022, today.year + 2):
                if str(year_candidate) in query:
                    year = year_candidate
                    break
            start = date(year, month_num, 1)
            if month_num == 12:
                end = date(year, 12, 31)
            else:
                end = date(year, month_num + 1, 1) - timedelta(days=1)
            return start, min(end, today)

    return today - timedelta(days=30), today


def _extract_branch_filter(query: str) -> str | None:
    normalized_query = normalizar_nombre(query)
    branches = PointBranch.objects.select_related("erp_branch").filter(status=PointBranch.STATUS_ACTIVE)
    for branch in branches:
        loose_candidates = {
            normalizar_nombre(branch.name),
        }
        exact_candidates = {
            normalizar_nombre(branch.external_id),
        }
        if branch.erp_branch_id:
            loose_candidates.add(normalizar_nombre(branch.erp_branch.nombre))
            loose_candidates.add(normalizar_nombre(branch.erp_branch.codigo))
        loose_candidates = {candidate for candidate in loose_candidates if candidate and len(candidate) >= 3}
        exact_candidates = {candidate for candidate in exact_candidates if candidate}
        if any(candidate in normalized_query for candidate in loose_candidates):
            return branch.name
        if any(re.search(rf"\b{re.escape(candidate)}\b", normalized_query) for candidate in exact_candidates):
            return branch.name
    return None


def _extract_product_filter(query: str) -> str | None:
    normalized_query = normalizar_nombre(query)
    trigger_phrases = (
        "receta de ",
        "de ",
        "del ",
        "del producto ",
        "producto ",
        "inventario de ",
        "stock de ",
    )
    for phrase in trigger_phrases:
        if phrase in normalized_query:
            idx = normalized_query.index(phrase) + len(phrase)
            candidate = normalized_query[idx:].strip()
            candidate = re.split(r"\b(en|por|para|vs|versus)\b", candidate, maxsplit=1)[0].strip()
            if len(candidate) >= 3:
                return candidate
    return None


def _extract_recipe_terms(query: str) -> list[str]:
    normalized_query = normalizar_nombre(query)
    for prefix in (
        "dame la receta de ",
        "receta de ",
        "costeo de ",
        "costo de ",
        "composicion de ",
        "ingredientes de ",
    ):
        if normalized_query.startswith(prefix):
            normalized_query = normalized_query[len(prefix):].strip()
            break
    if " y " not in normalized_query:
        return [normalized_query] if normalized_query else []
    terms = [part.strip() for part in normalized_query.split(" y ") if part.strip()]
    return terms[:2]


def _extract_limit(query: str, default: int = 10) -> int:
    match = re.search(r"(?:top|primeros?|mejores?)\s*(\d+)", normalizar_nombre(query))
    if not match:
        return default
    return min(max(int(match.group(1)), 1), 100)


def _latest_inventory_queryset():
    latest_snapshot = (
        PointInventorySnapshot.objects.filter(branch=OuterRef("branch"), product=OuterRef("product"))
        .order_by("-captured_at")
        .values("captured_at")[:1]
    )
    return (
        PointInventorySnapshot.objects.filter(captured_at=Subquery(latest_snapshot))
        .select_related("branch", "branch__erp_branch", "product")
        .order_by("product__name", "branch__name")
    )


def _serialize_decimal(value):
    if isinstance(value, Decimal):
        return str(value)
    return value


def _resolve_branch_codes(branch: str | None) -> set[str]:
    if not branch:
        return set()
    normalized_branch = normalizar_nombre(branch)
    codes: set[str] = set()
    queryset = PointBranch.objects.select_related("erp_branch").filter(
        Q(name__icontains=branch)
        | Q(external_id__iexact=branch)
        | Q(erp_branch__codigo__iexact=branch)
        | Q(erp_branch__nombre__icontains=branch)
    )
    for point_branch in queryset:
        if point_branch.erp_branch_id and point_branch.erp_branch.codigo:
            codes.add(point_branch.erp_branch.codigo)
        if point_branch.name:
            codes.add(point_branch.name.upper())
    if not codes and normalized_branch:
        codes.add(normalized_branch.upper())
    return codes


def _month_start(day: date) -> date:
    return day.replace(day=1)


def _month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)


def _next_month_start(day: date) -> date:
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def _full_closed_month_starts(*, start: date, end: date, branch: str | None = None) -> list[date] | None:
    if branch:
        return None
    if start != _month_start(start):
        return None
    if end != _month_end(end):
        return None
    current_month_start = timezone.localdate().replace(day=1)
    if end >= current_month_start:
        return None

    months: list[date] = []
    cursor = _month_start(start)
    while cursor <= end:
        months.append(cursor)
        cursor = _next_month_start(cursor)
    return months


def _official_monthly_sales_rows(*, start: date, end: date, branch: str | None = None):
    months = _full_closed_month_starts(start=start, end=end, branch=branch)
    if not months:
        return None
    queryset = PointMonthlySalesOfficial.objects.filter(
        month_start__in=months,
    ).order_by("month_start")
    rows_by_month = {row.month_start: row for row in queryset}
    if len(rows_by_month) != len(months):
        return None
    return [rows_by_month[month_start] for month_start in months]


def _historical_sales_qs(*, start: date, end: date, branch: str | None = None):
    queryset = VentaHistorica.objects.select_related("sucursal", "receta").filter(fecha__gte=start, fecha__lte=end)
    branch_codes = _resolve_branch_codes(branch)
    if branch_codes:
        queryset = queryset.filter(sucursal__codigo__in=branch_codes)
    return queryset


def _point_sales_qs(*, start: date, end: date, branch: str | None = None):
    queryset = PointDailySale.objects.select_related("branch", "product", "receta").filter(
        sale_date__gte=start,
        sale_date__lte=end,
    )
    if branch:
        queryset = queryset.filter(
            Q(branch__name__icontains=branch)
            | Q(branch__external_id__iexact=branch)
            | Q(branch__erp_branch__codigo__iexact=branch)
        )
    return queryset


def _point_sales_are_official(*, start: date, end: date, branch: str | None = None) -> bool:
    queryset = _point_sales_qs(start=start, end=end, branch=branch)
    if not queryset.exists():
        return False
    return not queryset.exclude(source_endpoint=OFFICIAL_POINT_SOURCE).exists()


def _use_historical_sales(*, start: date, end: date, branch: str | None = None) -> bool:
    if _point_sales_are_official(start=start, end=end, branch=branch):
        return False
    return _canonical_historical_sales_qs(start=start, end=end, branch=branch).exists()


def _canonical_historical_sales_qs(*, start: date, end: date, branch: str | None = None):
    queryset = _historical_sales_qs(start=start, end=end, branch=branch)
    manual_window = Q(
        fecha__gte=max(start, MANUAL_Q1_2026_START),
        fecha__lte=min(end, MANUAL_Q1_2026_END),
        fuente=MANUAL_Q1_2026_SOURCE,
    )
    bridge_window_start = max(start, MANUAL_Q1_2026_END + timedelta(days=1))
    bridge_window = Q(fecha__gte=bridge_window_start, fecha__lte=end, fuente=POINT_BRIDGE_SOURCE)

    if end <= MANUAL_Q1_2026_END:
        return queryset.filter(fuente=MANUAL_Q1_2026_SOURCE)
    if start > MANUAL_Q1_2026_END:
        return queryset.filter(fuente=POINT_BRIDGE_SOURCE)
    return queryset.filter(manual_window | bridge_window)


def _historical_reconciliation_issue(*, start: date, end: date, branch: str | None = None) -> dict | None:
    queryset = _historical_sales_qs(start=start, end=end, branch=branch)
    overlap = (
        queryset.values("fecha", "sucursal_id", "receta_id")
        .annotate(row_count=Count("id"), sources=Count("fuente", distinct=True))
        .filter(row_count__gt=1, sources__gt=1)
    )
    overlap_count = overlap.count()
    if not overlap_count:
        return None
    fuentes = sorted(set(queryset.values_list("fuente", flat=True)))
    return {
        "status": "NOT_RECONCILED",
        "overlap_groups": overlap_count,
        "sources": fuentes,
    }


def _reconciliation_warning_payload(*, start: date, end: date, branch: str | None, issue: dict) -> dict:
    branch_label = branch or "todas las sucursales"
    sources = ", ".join(issue.get("sources", [])) or "multiples fuentes"
    return {
        "answer": (
            f"El corte del {start:%d/%m/%Y} al {end:%d/%m/%Y} en {branch_label} no esta reconciliado. "
            f"Detecte traslape entre fuentes ({sources}) en {issue['overlap_groups']} combinaciones fecha-sucursal-producto. "
            "No debo darte un total oficial hasta conciliarlo contra el reporte real de Point."
        ),
        "data": {
            "status": issue["status"],
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "branch_filter": branch or "",
            "overlap_groups": issue["overlap_groups"],
            "sources": issue.get("sources", []),
        },
        "query_type": "reconciliation_required",
    }


def _recipe_linked_only_warning_payload(*, start: date, end: date, branch: str | None) -> dict:
    branch_label = branch or "todas las sucursales"
    return {
        "answer": (
            f"El corte del {start:%d/%m/%Y} al {end:%d/%m/%Y} en {branch_label} solo existe en VentaHistorica, "
            "que es una fuente ligada a recetas y no un corte comercial oficial de Point. "
            "No debo responder un total de venta oficial hasta conciliarlo contra una fuente comercial completa."
        ),
        "data": {
            "status": "RECIPE_LINKED_ONLY",
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "branch_filter": branch or "",
            "source": "VentaHistorica",
        },
        "query_type": "reconciliation_required",
    }


def _execute_sales_summary(query: str) -> dict:
    start, end = _extract_date_range(query)
    branch = _extract_branch_filter(query)
    official_months = _official_monthly_sales_rows(start=start, end=end, branch=branch)
    if official_months is not None:
        total_sales = sum((row.total_amount for row in official_months), ZERO)
        total_quantity = sum((row.total_quantity for row in official_months), ZERO)
        total_net = sum((row.net_amount for row in official_months), ZERO)
        last_sale_date = max(row.month_end for row in official_months)
        return {
            "answer": (
                f"Del {start:%d/%m/%Y} al {end:%d/%m/%Y} en todas las sucursales: "
                f"venta total ${total_sales:,.2f}, "
                f"{total_quantity:,.0f} piezas, "
                "tickets N/D en cache mensual oficial y "
                f"venta neta ${total_net:,.2f}."
            ),
            "data": {
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "branch_filter": "",
                "source": "PointMonthlySalesOfficial",
                "source_status": "OFFICIAL",
                "total_sales": _serialize_decimal(total_sales),
                "total_quantity": _serialize_decimal(total_quantity),
                "total_tickets": None,
                "total_discount": _serialize_decimal(sum((row.discount_amount for row in official_months), ZERO)),
                "total_net": _serialize_decimal(total_net),
                "branches_count": None,
                "products_count": None,
                "days_count": None,
                "last_sale_date": last_sale_date.isoformat(),
            },
            "query_type": "sales_summary",
        }
    using_historical = _use_historical_sales(start=start, end=end, branch=branch)
    if using_historical:
        issue = _historical_reconciliation_issue(start=start, end=end, branch=branch)
        if issue:
            return _reconciliation_warning_payload(start=start, end=end, branch=branch, issue=issue)
        return _recipe_linked_only_warning_payload(start=start, end=end, branch=branch)

    queryset = _point_sales_qs(start=start, end=end, branch=branch)
    source_label = "PointDailySaleOfficial" if _point_sales_are_official(start=start, end=end, branch=branch) else "PointDailySale"
    totals = queryset.aggregate(
        total_sales=Coalesce(Sum("total_amount"), ZERO),
        total_quantity=Coalesce(Sum("quantity"), ZERO),
        total_tickets=Coalesce(Sum("tickets"), 0),
        total_discount=Coalesce(Sum("discount_amount"), ZERO),
        total_net=Coalesce(Sum("net_amount"), ZERO),
        branches_count=Count("branch", distinct=True),
        products_count=Count("product", distinct=True),
        days_count=Count("sale_date", distinct=True),
        last_sale_date=Max("sale_date"),
    )
    branch_label = branch or "todas las sucursales"
    coverage_note = ""
    last_sale_date = totals.get("last_sale_date")
    if last_sale_date and last_sale_date < end:
        coverage_note = f" con datos disponibles hasta el {last_sale_date:%d/%m/%Y}"
    return {
        "answer": (
            f"Del {start:%d/%m/%Y} al {end:%d/%m/%Y} en {branch_label}{coverage_note}: "
            f"venta total ${totals['total_sales']:,.2f}, "
            f"{totals['total_quantity']:,.0f} piezas, "
            f"{totals['total_tickets']:,} tickets y "
            f"venta neta ${totals['total_net']:,.2f}."
        ),
        "data": {
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "branch_filter": branch or "",
            "source": source_label,
            "source_status": "OFFICIAL" if source_label == "PointDailySaleOfficial" else "STAGING",
            **{
                key: (value.isoformat() if isinstance(value, date) else _serialize_decimal(value))
                for key, value in totals.items()
            },
        },
        "query_type": "sales_summary",
    }


def _execute_sales_by_branch(query: str) -> dict:
    start, end = _extract_date_range(query)
    using_historical = _use_historical_sales(start=start, end=end)
    if using_historical:
        issue = _historical_reconciliation_issue(start=start, end=end, branch=None)
        if issue:
            return _reconciliation_warning_payload(start=start, end=end, branch=None, issue=issue)
        return _recipe_linked_only_warning_payload(start=start, end=end, branch=None)
    rows = (
        _point_sales_qs(start=start, end=end)
        .values("branch__name", "branch__external_id")
        .annotate(
            total_sales=Coalesce(Sum("total_amount"), ZERO),
            total_quantity=Coalesce(Sum("quantity"), ZERO),
            total_tickets=Coalesce(Sum("tickets"), 0),
        )
        .order_by("-total_sales", "branch__name")
    )
    lines = [f"Ventas por sucursal del {start:%d/%m/%Y} al {end:%d/%m/%Y}:"]
    payload = []
    for idx, row in enumerate(rows, start=1):
        branch_name = row["sucursal__nombre"] if using_historical else row["branch__name"]
        branch_id = row["sucursal__codigo"] if using_historical else row["branch__external_id"]
        lines.append(
            f"{idx}. {branch_name}: ${row['total_sales']:,.2f} "
            f"({row['total_quantity']:,.0f} pzs, {row['total_tickets']:,} tickets)"
        )
        payload.append(
            {
                "branch": branch_name,
                "branch_id": branch_id,
                "total_sales": str(row["total_sales"]),
                "total_quantity": str(row["total_quantity"]),
                "total_tickets": row["total_tickets"],
            }
        )
    source_label = "PointDailySaleOfficial" if _point_sales_are_official(start=start, end=end, branch=None) else "PointDailySale"
    return {
        "answer": "\n".join(lines),
        "data": {
            "branches": payload,
            "source": "VentaHistorica" if using_historical else source_label,
            "source_status": "OFFICIAL" if source_label == "PointDailySaleOfficial" else "STAGING",
        },
        "query_type": "sales_by_branch",
    }


def _execute_sales_by_product(query: str) -> dict:
    start, end = _extract_date_range(query)
    branch = _extract_branch_filter(query)
    limit = _extract_limit(query)
    using_historical = _use_historical_sales(start=start, end=end, branch=branch)
    if using_historical:
        issue = _historical_reconciliation_issue(start=start, end=end, branch=branch)
        if issue:
            return _reconciliation_warning_payload(start=start, end=end, branch=branch, issue=issue)
        return _recipe_linked_only_warning_payload(start=start, end=end, branch=branch)
    queryset = _point_sales_qs(start=start, end=end, branch=branch)
    rows = (
        queryset.values("product__name", "product__sku")
        .annotate(total_sales=Coalesce(Sum("total_amount"), ZERO), total_quantity=Coalesce(Sum("quantity"), ZERO))
        .order_by("-total_sales", "product__name")[:limit]
    )
    lines = [f"Top {limit} productos:"]
    payload = []
    for idx, row in enumerate(rows, start=1):
        product_name = row["receta__nombre"] if using_historical else row["product__name"]
        sku = row["receta__codigo_point"] if using_historical else row["product__sku"]
        lines.append(
            f"{idx}. {product_name} ({sku}): "
            f"${row['total_sales']:,.2f} - {row['total_quantity']:,.0f} pzs"
        )
        payload.append(
            {
                "product": product_name,
                "sku": sku,
                "total_sales": str(row["total_sales"]),
                "total_quantity": str(row["total_quantity"]),
            }
        )
    source_label = "PointDailySaleOfficial" if _point_sales_are_official(start=start, end=end, branch=branch) else "PointDailySale"
    return {
        "answer": "\n".join(lines),
        "data": {
            "products": payload,
            "source": source_label,
            "source_status": "OFFICIAL" if source_label == "PointDailySaleOfficial" else "STAGING",
        },
        "query_type": "sales_by_product",
    }


def _execute_sales_trend(query: str) -> dict:
    start, end = _extract_date_range(query)
    branch = _extract_branch_filter(query)
    if (end - start).days < 60:
        start = end - timedelta(days=180)
    official_months = _official_monthly_sales_rows(start=start, end=end, branch=branch)
    if official_months is not None:
        payload = [
            {
                "month": row.month_start.strftime("%Y-%m"),
                "total_sales": _serialize_decimal(row.total_amount),
                "total_quantity": _serialize_decimal(row.total_quantity),
                "total_tickets": None,
                "avg_ticket": None,
            }
            for row in official_months
        ]
        lines = [f"Tendencia mensual ({start:%d/%m/%Y} - {end:%d/%m/%Y}):"]
        lines.extend(
            f"{row['month']}: ${Decimal(row['total_sales']):,.2f} ({Decimal(row['total_quantity']):,.0f} pzs)"
            for row in payload
        )
        return {
            "answer": "\n".join(lines),
            "data": {
                "trends": payload,
                "source": "PointMonthlySalesOfficial",
                "source_status": "OFFICIAL",
            },
            "query_type": "sales_trend",
        }
    using_historical = _use_historical_sales(start=start, end=end, branch=branch)
    if using_historical:
        issue = _historical_reconciliation_issue(start=start, end=end, branch=branch)
        if issue:
            return _reconciliation_warning_payload(start=start, end=end, branch=branch, issue=issue)
        return _recipe_linked_only_warning_payload(start=start, end=end, branch=branch)
    queryset = _point_sales_qs(start=start, end=end, branch=branch)
    rows = (
        queryset.annotate(month=TruncMonth("sale_date"))
        .values("month")
        .annotate(
            total_sales=Coalesce(Sum("total_amount"), ZERO),
            total_quantity=Coalesce(Sum("quantity"), ZERO),
            total_tickets=Coalesce(Sum("tickets"), 0),
        )
        .order_by("month")
    )
    payload = []
    for row in rows:
        tickets = row["total_tickets"] or 1
        payload.append(
            {
                "month": row["month"].strftime("%Y-%m"),
                "total_sales": str(row["total_sales"]),
                "total_quantity": str(row["total_quantity"]),
                "total_tickets": row["total_tickets"],
                "avg_ticket": str(round(row["total_sales"] / tickets, 2)),
            }
        )
    lines = [f"Tendencia mensual ({start:%d/%m/%Y} - {end:%d/%m/%Y}):"]
    lines.extend(
        f"{row['month']}: ${Decimal(row['total_sales']):,.2f} ({row['total_quantity']} pzs)"
        for row in payload
    )
    source_label = "PointDailySaleOfficial" if _point_sales_are_official(start=start, end=end, branch=branch) else "PointDailySale"
    return {
        "answer": "\n".join(lines),
        "data": {
            "trends": payload,
            "source": source_label,
            "source_status": "OFFICIAL" if source_label == "PointDailySaleOfficial" else "STAGING",
        },
        "query_type": "sales_trend",
    }


def _execute_inventory(query: str) -> dict:
    branch = _extract_branch_filter(query)
    product = _extract_product_filter(query)
    queryset = _latest_inventory_queryset()
    if branch:
        queryset = queryset.filter(Q(branch__name__icontains=branch) | Q(branch__external_id__iexact=branch))
    if product:
        queryset = queryset.filter(Q(product__name__icontains=product) | Q(product__sku__iexact=product))
    queryset = queryset[:50]
    payload = []
    for snapshot in queryset:
        payload.append(
            {
                "product": snapshot.product.name,
                "sku": snapshot.product.sku,
                "branch": snapshot.branch.name,
                "stock": str(snapshot.stock),
                "min_stock": str(snapshot.min_stock),
                "captured_at": snapshot.captured_at.isoformat(),
            }
        )
    if not payload:
        answer = "No se encontraron existencias con esos filtros."
    else:
        answer = "\n".join(
            f"{row['product']} - {row['branch']}: {row['stock']} (min {row['min_stock']})"
            for row in payload
        )
    return {"answer": answer, "data": {"inventory": payload}, "query_type": "inventory"}


def _execute_recipe(query: str) -> dict:
    recipe_terms = _extract_recipe_terms(query)
    if len(recipe_terms) == 2:
        receta_a = resolve_receta_from_term(recipe_terms[0])
        receta_b = resolve_receta_from_term(recipe_terms[1])
        if receta_a is not None and receta_b is not None:
            grouped_rule = resolve_grouped_rule(receta_a=receta_a, receta_b=receta_b)
            if grouped_rule is not None and grouped_rule.addon_receta_id:
                grouped = calculate_grouped_addon_cost(rule=grouped_rule)
                return {
                    "answer": (
                        f"Costo agrupado {grouped.base_receta.nombre} + {grouped.addon_receta.nombre}: "
                        f"{grouped.grouped_cost}"
                    ),
                    "data": {
                        "base_receta": grouped.base_receta.nombre,
                        "addon_receta": grouped.addon_receta.nombre,
                        "base_cost": _serialize_decimal(grouped.base_cost),
                        "addon_cost": _serialize_decimal(grouped.addon_cost),
                        "grouped_cost": _serialize_decimal(grouped.grouped_cost),
                        "addon_rule": {
                            "addon_codigo_point": grouped.rule.addon_codigo_point,
                            "addon_nombre_point": grouped.rule.addon_nombre_point,
                            "cooccurrence_days": grouped.rule.cooccurrence_days,
                            "cooccurrence_branches": grouped.rule.cooccurrence_branches,
                            "confidence_score": _serialize_decimal(grouped.rule.confidence_score),
                        },
                    },
                    "query_type": "recipe",
                }

    product_filter = _extract_product_filter(query)
    if not product_filter:
        return {
            "answer": "No pude identificar el producto. Intenta con 'Dame la receta de Tres Leches'.",
            "data": {},
            "query_type": "recipe",
        }
    matcher = PointSalesMatchingService()
    receta = matcher.resolve_receta(codigo_point=product_filter, point_name=product_filter)
    if receta is None:
        receta = Receta.objects.filter(nombre_normalizado__icontains=normalizar_nombre(product_filter)).order_by("id").first()
    if receta is None:
        product = PointProduct.objects.filter(
            Q(name__icontains=product_filter) | Q(sku__iexact=product_filter) | Q(external_id__iexact=product_filter)
        ).first()
        if product is not None:
            receta = matcher.resolve_receta(codigo_point=product.sku, point_name=product.name)
    if receta is None:
        return {
            "answer": f"No encontré la receta de '{product_filter}' en el ERP.",
            "data": {},
            "query_type": "recipe",
        }

    lines_queryset = (
        LineaReceta.objects.filter(receta=receta)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .select_related("insumo", "unidad")
        .order_by("posicion", "id")
    )
    bom = []
    for line in lines_queryset:
        bom.append(
            {
                "insumo": line.insumo.nombre if line.insumo_id else line.insumo_texto,
                "cantidad": str(line.cantidad),
                "unidad": line.unidad.codigo if line.unidad_id else line.unidad_texto,
                "costo_unitario": _serialize_decimal(line.costo_unitario_snapshot),
            }
        )
    point_node = (
        PointRecipeNode.objects.filter(erp_recipe=receta)
        .prefetch_related("lines__erp_recipe")
        .order_by("-run_id", "depth", "id")
        .first()
    )
    prepared_inputs = []
    if point_node is not None:
        prepared_inputs = [
            {
                "codigo_point": line.point_code,
                "nombre": line.point_name,
                "child_recipe": line.erp_recipe.nombre if line.erp_recipe_id else "",
            }
            for line in point_node.lines.all()
            if line.classification == "PREPARED_INPUT"
        ]
    yield_label = ""
    if receta.rendimiento_cantidad and receta.rendimiento_unidad_id:
        yield_label = f" Rendimiento base: {receta.rendimiento_cantidad} {receta.rendimiento_unidad.codigo}."
    prepared_label = ""
    if prepared_inputs:
        prepared_label = f" Tiene {len(prepared_inputs)} subreceta(s) preparada(s)."
    addon_payload = []
    for rule in approved_addons_for_recipe(receta):
        if not rule.addon_receta_id:
            continue
        grouped = calculate_grouped_addon_cost(rule=rule)
        addon_payload.append(
            {
                "addon_codigo_point": rule.addon_codigo_point,
                "addon_nombre_point": rule.addon_nombre_point,
                "addon_receta": rule.addon_receta.nombre,
                "addon_cost": _serialize_decimal(grouped.addon_cost),
                "grouped_cost": _serialize_decimal(grouped.grouped_cost),
            }
        )
    detected_payload = []
    from recetas.models import RecetaAgrupacionAddon
    for rule in (
        RecetaAgrupacionAddon.objects.filter(
            base_receta=receta,
            activo=True,
            status=RecetaAgrupacionAddon.STATUS_DETECTED,
        )
        .select_related("addon_receta")
        .order_by("-confidence_score", "addon_nombre_point", "id")[:5]
    ):
        detected_payload.append(
            {
                "addon_codigo_point": rule.addon_codigo_point,
                "addon_nombre_point": rule.addon_nombre_point,
                "confidence_score": _serialize_decimal(rule.confidence_score),
                "cooccurrence_days": rule.cooccurrence_days,
            }
        )
    addon_label = ""
    if addon_payload:
        addon_label = f" Tiene {len(addon_payload)} add-on(s) aprobado(s) para costeo agrupado."
    detected_label = ""
    if detected_payload:
        detected_label = f" Tiene {len(detected_payload)} add-on(s) detectado(s) pendientes de aprobación."
    answer = f"Receta {receta.nombre} con {len(bom)} lineas.{yield_label}{prepared_label}{addon_label}{detected_label}"
    return {
        "answer": answer,
        "data": {
            "receta_id": receta.id,
            "receta_nombre": receta.nombre,
            "receta_tipo": receta.tipo,
            "costo_total": _serialize_decimal(receta.costo_total_estimado_decimal),
            "rendimiento_cantidad": _serialize_decimal(receta.rendimiento_cantidad),
            "rendimiento_unidad": receta.rendimiento_unidad.codigo if receta.rendimiento_unidad_id else "",
            "bom": bom,
            "prepared_inputs": prepared_inputs,
            "approved_addons": addon_payload,
            "detected_addons_pending": detected_payload,
        },
        "query_type": "recipe",
    }


def _execute_low_stock(query: str) -> dict:
    branch = _extract_branch_filter(query)
    queryset = _latest_inventory_queryset().filter(stock__lt=F("min_stock"), min_stock__gt=0)
    if branch:
        queryset = queryset.filter(Q(branch__name__icontains=branch) | Q(branch__external_id__iexact=branch))
    queryset = queryset[:30]
    payload = []
    for snapshot in queryset:
        deficit = snapshot.min_stock - snapshot.stock
        payload.append(
            {
                "product": snapshot.product.name,
                "branch": snapshot.branch.name,
                "stock": str(snapshot.stock),
                "min_stock": str(snapshot.min_stock),
                "deficit": str(deficit),
            }
        )
    answer = (
        "No hay productos con bajo stock en este momento."
        if not payload
        else "\n".join(
            f"{row['product']} en {row['branch']}: {row['stock']} (min {row['min_stock']}, faltan {row['deficit']})"
            for row in payload
        )
    )
    return {"answer": answer, "data": {"low_stock": payload}, "query_type": "low_stock"}


def _execute_with_llm(query: str, api_key: str) -> dict:
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        model = getattr(settings, "POS_BRIDGE_AGENT_MODEL", "gpt-4o-mini")
        branch_names = list(PointBranch.objects.filter(status=PointBranch.STATUS_ACTIVE).values_list("name", flat=True))
        system_prompt = (
            "Eres un clasificador de consultas del ERP Pollyana's Dolce. "
            f"Sucursales activas: {', '.join(branch_names)}. "
            "Responde solo JSON valido con una clave intent y uno de estos valores: "
            "sales_summary, sales_by_branch, sales_by_product, sales_trend, inventory, recipe, low_stock, general."
        )
        response = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query},
            ],
            temperature=0,
            max_output_tokens=120,
        )
        parsed = json.loads(_strip_code_fences(response.output_text))
        intent = parsed.get("intent", "general")
        executor = EXECUTORS.get(intent, _execute_general)
        return executor(query)
    except Exception as exc:
        logger.warning("Fallback LLM de pos_bridge no disponible: %s", exc)
        return {
            "answer": (
                "No logre clasificar la pregunta con certeza. Intenta con una forma mas explicita, por ejemplo: "
                "'Cuanto vendimos en Matriz en febrero', 'Top 10 productos', 'Inventario de 3 leches' o 'Receta de Tres Leches'."
            ),
            "data": {},
            "query_type": "general",
        }


def _execute_general(query: str) -> dict:
    api_key = getattr(settings, "OPENAI_API_KEY", "")
    if api_key:
        return _execute_with_llm(query, api_key)
    return {
        "answer": (
            "No logre clasificar tu pregunta. Prueba con: "
            "'Cuanto vendimos en Matriz en febrero', 'Top 10 productos', 'Inventario de 3 leches', "
            "'Receta de Tres Leches' o 'Productos con bajo stock'."
        ),
        "data": {},
        "query_type": "general",
    }


EXECUTORS = {
    "sales_summary": _execute_sales_summary,
    "sales_by_branch": _execute_sales_by_branch,
    "sales_by_product": _execute_sales_by_product,
    "sales_trend": _execute_sales_trend,
    "inventory": _execute_inventory,
    "recipe": _execute_recipe,
    "low_stock": _execute_low_stock,
    "general": _execute_general,
}


class PosAgentQueryService:
    def process_query(self, *, query: str, user=None, context: dict | None = None) -> dict:
        normalized_query = (query or "").strip()
        if not normalized_query:
            raise ValueError("La consulta no puede ir vacia.")

        intent = classify_intent(normalized_query)
        executor = EXECUTORS.get(intent, _execute_general)

        try:
            result = executor(normalized_query)
        except Exception as exc:
            logger.error("Error ejecutando query del agente: %s", exc, exc_info=True)
            result = {
                "answer": f"Error al procesar la consulta: {exc}",
                "data": {},
                "query_type": intent,
            }

        query_hash = hashlib.sha256(normalized_query.encode("utf-8")).hexdigest()[:12]
        log_event(
            user,
            action="agent_query",
            model="pos_bridge.agent",
            object_id=query_hash,
            payload={
                "query": normalized_query[:500],
                "query_type": result.get("query_type", intent),
                "has_context": bool(context),
            },
        )
        result.setdefault("data", {})
        result.setdefault("query_type", intent)
        return result
