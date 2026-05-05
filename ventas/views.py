from __future__ import annotations

import csv
import math
import shutil
import socket
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse
from zipfile import ZIP_DEFLATED, ZipFile
import unicodedata

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Avg, Case, Count, IntegerField, Max, Min, Q, Sum, When
from django.http import FileResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views import View
from openpyxl.chart import BarChart, LineChart, PieChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from core.access import can_view_rentabilidad, has_any_role, ROLE_DG, ROLE_ADMIN, ROLE_PRODUCCION, ROLE_COMPRAS
from core.branch_catalog import EXCLUDED_BRANCH_CODES, POINT_MATURE_BRANCH_CODES, eligible_sales_event_branch_qs
from core.models import Sucursal
from maestros.models import Insumo
from pos_bridge.models import PointDailySale, PointProduct
from recetas.models import LineaReceta, Receta
from recetas.utils.commercial_composition import (
    build_commercial_recipe_lookup_context,
    classify_commercial_recipe,
    get_commercial_total_cost_map,
)
from ventas.models import (
    EventoVenta,
    EventoVentaApproval,
    EventoVentaAdjustment,
    EventoVentaAdjustmentDraft,
    EventoVentaCapacityRule,
    EventoVentaExecutionMetric,
    EventoVentaForecast,
    EventoVentaFinancial,
    EventoVentaInputRequirement,
    EventoVentaNotification,
    EventoVentaProjectionArtifact,
    EventoVentaProductionLine,
    EventoVentaPurchaseRequirement,
    EventoVentaProducto,
    EventoVentaSucursal,
)
from ventas.services.audit import log_evento_change
from ventas.services.financials import (
    EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE,
    build_financials,
    evaluate_event_revenue_plausibility,
    reconcile_event_revenue_plausibility,
    resolve_unit_price,
    resolve_unit_prices_bulk,
)
from ventas.services.forecasting import (
    ZERO,
    _aggregate_historical_quantity,
    _branch_has_operational_signal,
    _select_event_homologue_window,
    build_event_executive_projection_model,
    executive_event_product_scope,
    generate_event_forecast,
)
from ventas.services.notifications import create_unique_notification
from ventas.services.operational_targets import build_operational_targets
from ventas.services.postmortem import build_postmortem
from ventas.services.production import SAFE_SHELF_LIFE_DAYS, _resolve_production_day, generate_production_plan
from ventas.services.requirements import build_input_requirements, build_purchase_requirements
from ventas.tasks import run_event_projection_pipeline_task


# Factor calibrado post Día del Niño 2026-04-30
EVENTO_PRODUCCION_REVENUE_FACTOR = Decimal("0.98143")
EVENTO_PRODUCCION_MIN_QTY = Decimal("1.0")
EVENTO_PRODUCCION_RECENT_ACTIVITY_DAYS = 30
EVENTO_PRODUCCION_PRICE_LOOKBACK_DAYS = 90


def _can_view_events(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_PRODUCCION, ROLE_COMPRAS, "VENTAS", "LECTURA")


def _can_manage_events(user) -> bool:
    return has_any_role(user, ROLE_ADMIN, "VENTAS")


def _can_approve_events(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN)


def _can_manage_capacity(user) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_PRODUCCION)


def _can_view_event_production_dashboard(user) -> bool:
    return can_view_rentabilidad(user) or has_any_role(user, ROLE_DG, "VENTAS")


def _event_branch_links(event: EventoVenta):
    return (
        EventoVentaSucursal.objects.filter(
            sales_event=event,
            is_active=True,
            branch__activa=True,
        )
        .exclude(branch__codigo__in=EXCLUDED_BRANCH_CODES)
        .select_related("branch")
        .order_by("branch__codigo")
    )


def _event_forecast_qs(event: EventoVenta):
    return EventoVentaForecast.objects.filter(sales_event=event).exclude(branch__codigo__in=EXCLUDED_BRANCH_CODES)


DIRECT_METHODS = (
    "weighted_avg_weekday",
    "recent_direct_average",
    "weekday_ytd_weighted",
    "event_anchor_ytd_weighted",
    "intermittent_croston_ytd",
)
COMPARABLE_METHODS = ("weekday_comparable_branch", "recent_comparable_branch")
FALLBACK_METHODS = ("fallback_categoria", "fallback_categoria_comparable")


def _mature_comparable_options():
    return eligible_sales_event_branch_qs().filter(codigo__in=POINT_MATURE_BRANCH_CODES).order_by("nombre")


def _branch_comparable_options():
    return list(_mature_comparable_options())


def _branch_needs_comparable(branch: Sucursal, anchor: date | None = None) -> bool:
    reference_date = anchor or timezone.localdate()
    if branch.codigo not in POINT_MATURE_BRANCH_CODES:
        return True
    return not _branch_has_operational_signal(branch, reference_date)


def _branch_comparable_controls(*, selected_branch_ids: set[int], event: EventoVenta | None = None, anchor: date | None = None):
    options = _branch_comparable_options()
    selected_branches = {branch.id: branch for branch in eligible_sales_event_branch_qs().filter(id__in=selected_branch_ids)}
    controls = []
    event_links = {}
    if event:
        event_links = {
            link.branch_id: link
            for link in EventoVentaSucursal.objects.filter(sales_event=event).select_related("branch", "comparable_branch")
        }
    for branch_id in sorted(selected_branch_ids):
        branch = selected_branches.get(branch_id)
        if not branch:
            continue
        if not _branch_needs_comparable(branch, anchor or (event.main_date if event else None)):
            continue
        current = None
        if branch_id in event_links and event_links[branch_id].comparable_branch_id:
            current = event_links[branch_id].comparable_branch_id
        controls.append(
            {
                "branch": branch,
                "field_name": f"comparable_branch_{branch.id}",
                "current_id": current,
                "options": options,
                "is_selected": branch.id in selected_branch_ids,
            }
        )
    return controls


def _projection_week_window(main_date: date) -> tuple[date, date]:
    return main_date - timedelta(days=3), main_date + timedelta(days=3)


def _event_projection_window(event: EventoVenta) -> tuple[date, date]:
    start = event.analysis_start_date or None
    end = event.analysis_end_date or None
    if start and end:
        return start, end
    return _projection_week_window(event.main_date)


def _coerce_posted_date(value, fallback: date | None = None) -> date | None:
    if value in (None, ""):
        return fallback
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _week_scope_label(event: EventoVenta) -> str:
    week_start, week_end = _event_projection_window(event)
    return f"{week_start} a {week_end}"


def _week_scope_qs(event: EventoVenta, forecast_qs=None):
    base_qs = forecast_qs if forecast_qs is not None else _event_forecast_qs(event)
    week_start, week_end = _event_projection_window(event)
    return base_qs.filter(forecast_date__range=(week_start, week_end))


def _attach_forecast_source_flags(rows: list[dict]) -> list[dict]:
    enriched_rows: list[dict] = []
    for row in rows:
        direct_count = int(row.get("direct_count") or 0)
        comparable_count = int(row.get("comparable_count") or 0)
        fallback_count = int(row.get("fallback_count") or 0)
        no_data_count = int(row.get("no_data_count") or 0)
        method_counts = [
            ("Directo", direct_count, 3),
            ("Sucursal comparable", comparable_count, 2),
            ("Fallback categoría", fallback_count, 1),
            ("Sin base suficiente", no_data_count, 0),
        ]
        source_label = "Sin base suficiente"
        positive_methods = [item for item in method_counts if item[1] > 0]
        if positive_methods:
            source_label = sorted(positive_methods, key=lambda item: (-item[1], -item[2]))[0][0]

        row["source_label"] = source_label
        row["source_counts"] = {
            "directo": direct_count,
            "comparable": comparable_count,
            "fallback": fallback_count,
            "sin_base": no_data_count,
        }
        enriched_rows.append(row)
    return enriched_rows


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


def _round_projection_qty(value: Decimal | float | int | None) -> int:
    qty = Decimal(str(value or 0))
    return int(qty.quantize(Decimal("1")))


def _decimal_or_zero(value) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _pick_row_value(row: dict, *keys: str):
    normalized = {_ascii_norm(str(key)): value for key, value in row.items()}
    for key in keys:
        value = normalized.get(_ascii_norm(key))
        if value not in (None, ""):
            return value
    return None


def _prepare_projection_rows(rows: list[dict], *, branch_mode: bool = False) -> list[dict]:
    prepared: list[dict] = []
    for row in rows:
        product_name = row.get("product__nombre") or ""
        family, category = _infer_projection_labels(
            product_name=product_name,
            family=row.get("product__familia") or "",
            category=row.get("product__categoria") or "",
        )
        row["product__familia"] = family
        row["product__categoria"] = category
        if branch_mode:
            if "forecast_total" in row:
                row["base_total"] = _round_projection_qty(row.get("base_total"))
                row["uplift_total"] = _round_projection_qty(row.get("uplift_total"))
                row["trend_total"] = _round_projection_qty(row.get("trend_total"))
                row["forecast_total"] = _round_projection_qty(row.get("forecast_total"))
                if row["forecast_total"] < 1:
                    continue
            else:
                total = _round_projection_qty(row.get("total"))
                if total < 1:
                    continue
                row["total"] = total
        else:
            row["base_total"] = _round_projection_qty(row.get("base_total"))
            row["uplift_total"] = _round_projection_qty(row.get("uplift_total"))
            row["trend_total"] = _round_projection_qty(row.get("trend_total"))
            row["forecast_total"] = _round_projection_qty(row.get("forecast_total"))
            if row["forecast_total"] < 1:
                continue
        prepared.append(row)

    sort_keys = ["product__familia", "product__categoria"]
    if branch_mode:
        sort_keys = ["branch__codigo"] + sort_keys
    return sorted(
        prepared,
        key=lambda row: tuple((row.get(key) or "") for key in sort_keys) + (-(row.get("forecast_total") or row.get("total") or 0), row.get("product__nombre") or ""),
    )


def _display_branch_name(branch: Sucursal) -> str:
    name = (branch.nombre or "").strip()
    if name:
        return name.title() if name.upper() == name else name
    code = (branch.codigo or "").strip().replace("_", " ")
    return code.title()


def _mandatory_branch_ids() -> set[int]:
    return set()


def _product_selection_groups():
    grouped: dict[tuple[str, str], list[Receta]] = {}
    products = Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).order_by("nombre")
    for product in products:
        is_eligible, _reason = executive_event_product_scope(product)
        if not is_eligible:
            continue
        family, category = _infer_projection_labels(
            product_name=product.nombre or "",
            family=getattr(product, "familia", "") or "",
            category=getattr(product, "categoria", "") or "",
        )
        grouped.setdefault((family, category), []).append(product)
    ordered_keys = sorted(grouped.keys(), key=lambda item: (item[0], item[1]))
    return [
        {
            "family": family,
            "category": category,
            "products": sorted(grouped[(family, category)], key=lambda product: product.nombre or ""),
        }
        for family, category in ordered_keys
    ]


def _filter_executive_event_products(products):
    included: list[Receta] = []
    excluded: list[tuple[Receta, str]] = []
    for product in products:
        is_eligible, reason = executive_event_product_scope(product)
        if is_eligible:
            included.append(product)
        else:
            excluded.append((product, reason))
    return included, excluded


def _build_projection_explanation(row: dict, *, scope_label: str) -> str:
    source_label = row.get("source_label") or "Sin base suficiente"
    source_counts = row.get("source_counts") or {}
    trend_total = Decimal(str(row.get("trend_total") or 0))
    uplift_total = Decimal(str(row.get("uplift_total") or 0))
    confidence_avg = Decimal(str(row.get("confidence_avg") or 0))

    if trend_total > 0:
        trend_text = "La tendencia reciente empuja la proyección al alza."
    elif trend_total < 0:
        trend_text = "La tendencia reciente está frenando la proyección."
    else:
        trend_text = "La tendencia quedó neutra en este alcance."

    if uplift_total > 0:
        uplift_text = "El evento sí aporta un incremento sobre la base."
    elif uplift_total < 0:
        uplift_text = "El histórico comparable del evento quedó por debajo de la base."
    else:
        uplift_text = "No se detectó uplift adicional del evento."

    return (
        f"{scope_label}: forecast {source_label.lower()}. "
        f"Cobertura usada: directo {int(source_counts.get('directo') or 0)}, "
        f"comparable {int(source_counts.get('comparable') or 0)}, "
        f"fallback {int(source_counts.get('fallback') or 0)}, "
        f"sin base {int(source_counts.get('sin_base') or 0)}. "
        f"{trend_text} {uplift_text} "
        f"Confianza promedio: {confidence_avg:.2f}."
    )


def _event_detail_source_filter(value: str | None) -> tuple[str, str] | None:
    normalized = _ascii_norm(value or "")
    source_map = {
        "directo": ("directo", "Directo"),
        "comparable": ("comparable", "Sucursal comparable"),
        "fallback": ("fallback", "Fallback categoria"),
        "sinbase": ("sin_base", "Sin base suficiente"),
    }
    return source_map.get(normalized)


def _filter_week_product_projection(rows: list[dict], source_key: str) -> list[dict]:
    return [
        row
        for row in rows
        if int((row.get("source_counts") or {}).get(source_key) or 0) > 0
    ]


def _forecast_source_summary(forecast_qs) -> dict[str, int]:
    return {
        "directo": forecast_qs.filter(explanation_json__base_method__in=DIRECT_METHODS).count(),
        "comparable": forecast_qs.filter(explanation_json__base_method__in=COMPARABLE_METHODS).count(),
        "fallback": forecast_qs.filter(explanation_json__base_method__in=FALLBACK_METHODS).count(),
        "sin_base": forecast_qs.filter(explanation_json__base_method="no_data").count(),
    }


def _filter_forecast_qs_by_source(forecast_qs, source_filter: str):
    if source_filter == "directo":
        return forecast_qs.filter(explanation_json__base_method__in=DIRECT_METHODS)
    if source_filter == "comparable":
        return forecast_qs.filter(explanation_json__base_method__in=COMPARABLE_METHODS)
    if source_filter == "fallback":
        return forecast_qs.filter(explanation_json__base_method__in=FALLBACK_METHODS)
    if source_filter == "sin_base":
        return forecast_qs.filter(explanation_json__base_method="no_data")
    return forecast_qs


def _adjustment_scope_mode(value: str | None) -> str:
    normalized = (value or "").strip().upper()
    if normalized == EventoVentaAdjustmentDraft.SCOPE_DAY:
        return EventoVentaAdjustmentDraft.SCOPE_DAY
    return EventoVentaAdjustmentDraft.SCOPE_RANGE


def _adjustment_scope_window(event: EventoVenta, scope_mode: str) -> tuple[date, date, str]:
    if scope_mode == EventoVentaAdjustmentDraft.SCOPE_DAY:
        return event.main_date, event.main_date, str(event.main_date)
    start, end = _projection_week_window(event.main_date)
    forecast_bounds = event.forecasts.aggregate(min_date=Min("forecast_date"), max_date=Max("forecast_date"))
    min_date = forecast_bounds.get("min_date")
    max_date = forecast_bounds.get("max_date")
    if min_date and max_date:
        start = min(start, min_date) if start else min_date
        end = max(end, max_date) if end else max_date
    return start, end, f"{start} a {end}"


def _active_adjustment_draft(event: EventoVenta, scope_mode: str | None = None):
    qs = event.adjustment_drafts.filter(status=EventoVentaAdjustmentDraft.STATUS_DRAFT)
    if scope_mode:
        qs = qs.filter(scope_mode=scope_mode)
    return qs.order_by("-updated_at", "-id").first()


def _draft_entries_map(draft: EventoVentaAdjustmentDraft | None) -> dict[tuple[int, int], Decimal]:
    if not draft:
        return {}
    entries_map: dict[tuple[int, int], Decimal] = {}
    for item in draft.entries_json or []:
        try:
            product_id = int(item.get("product_id"))
            branch_id = int(item.get("branch_id"))
        except (TypeError, ValueError):
            continue
        entries_map[(product_id, branch_id)] = _decimal_or_zero(item.get("target_qty"))
    return entries_map


def _build_adjustment_editor_matrix(
    event: EventoVenta,
    *,
    scope_mode: str,
    draft_entries: dict[tuple[int, int], Decimal] | None = None,
    filters: dict | None = None,
):
    filters = filters or {}
    start_date, end_date, scope_label = _adjustment_scope_window(event, scope_mode)
    branch_filter = (filters.get("branch") or "").strip().upper()
    family_filter = (filters.get("family") or "").strip()
    category_filter = (filters.get("category") or "").strip()
    source_filter = (filters.get("source") or "").strip().lower()
    product_query = (filters.get("q") or "").strip().lower()

    branch_links = list(_event_branch_links(event))
    all_branch_codes = [link.branch.codigo for link in branch_links]
    branch_code_id_map = {link.branch.codigo: link.branch.id for link in branch_links}
    visible_branch_codes = [code for code in all_branch_codes if not branch_filter or code == branch_filter]
    if not visible_branch_codes and branch_filter:
        visible_branch_codes = [branch_filter]

    scoped_qs = _event_forecast_qs(event).filter(forecast_date__range=(start_date, end_date))
    scoped_qs = _filter_forecast_qs_by_source(scoped_qs, source_filter)
    if family_filter:
        scoped_qs = scoped_qs.filter(product__familia=family_filter)
    if category_filter:
        scoped_qs = scoped_qs.filter(product__categoria=category_filter)
    if product_query:
        scoped_qs = scoped_qs.filter(product__nombre__icontains=product_query)

    product_rows = list(
        scoped_qs.values("product_id", "product__familia", "product__categoria", "product__nombre")
        .annotate(
            forecast_total=Sum("final_forecast"),
            direct_count=Count("id", filter=Q(explanation_json__base_method__in=DIRECT_METHODS)),
            comparable_count=Count("id", filter=Q(explanation_json__base_method__in=COMPARABLE_METHODS)),
            fallback_count=Count("id", filter=Q(explanation_json__base_method__in=FALLBACK_METHODS)),
            no_data_count=Count("id", filter=Q(explanation_json__base_method="no_data")),
        )
        .filter(forecast_total__gt=0)
        .order_by("product__familia", "product__categoria", "-forecast_total", "product__nombre")
    )
    product_rows = _attach_forecast_source_flags(product_rows)
    product_rows = _prepare_projection_rows(product_rows)

    branch_rows = list(
        scoped_qs.values("product_id", "branch_id", "branch__codigo")
        .annotate(forecast_total=Sum("final_forecast"))
        .order_by("product_id", "branch__codigo")
    )
    branch_values_map: dict[tuple[int, str], Decimal] = {}
    branch_id_code_map: dict[int, str] = {}
    for row in branch_rows:
        branch_code = str(row.get("branch__codigo") or "")
        branch_id = int(row.get("branch_id"))
        branch_id_code_map[branch_id] = branch_code
        branch_code_id_map[branch_code] = branch_id
        branch_values_map[(int(row.get("product_id")), branch_code)] = Decimal(str(row.get("forecast_total") or 0))

    applied_entries = draft_entries or {}
    rows: list[dict] = []
    current_total = Decimal("0")
    preview_total = Decimal("0")
    changed_cells = 0
    branch_deltas = {code: {"current": ZERO, "preview": ZERO} for code in visible_branch_codes}

    for row in product_rows:
        product_id = int(row["product_id"])
        source_label = row.get("source_label") or ""
        row_family = row.get("product__familia") or "Sin familia"
        row_category = row.get("product__categoria") or "Sin categoria"
        row_name = row.get("product__nombre") or ""
        current_branch_values: dict[str, Decimal] = {}
        preview_branch_values: dict[str, Decimal] = {}
        row_current_total = ZERO
        row_preview_total = ZERO
        row_changed = False
        branch_cells: list[dict] = []

        for branch_code in visible_branch_codes:
            current_value = branch_values_map.get((product_id, branch_code), ZERO)
            branch_id = branch_code_id_map.get(branch_code)
            preview_value = applied_entries.get((product_id, branch_id), current_value) if branch_id else current_value
            current_branch_values[branch_code] = current_value
            preview_branch_values[branch_code] = preview_value
            row_current_total += current_value
            row_preview_total += preview_value
            branch_deltas[branch_code]["current"] += current_value
            branch_deltas[branch_code]["preview"] += preview_value
            if preview_value != current_value:
                changed_cells += 1
                row_changed = True
            branch_cells.append(
                {
                    "branch_code": branch_code,
                    "branch_id": branch_id,
                    "current_value": current_value,
                    "preview_value": preview_value,
                }
            )

        current_total += row_current_total
        preview_total += row_preview_total
        rows.append(
            {
                "product_id": product_id,
                "family": row_family,
                "category": row_category,
                "product_name": row_name,
                "source_label": source_label,
                "current_total": row_current_total,
                "preview_total": row_preview_total,
                "current_branch_values": current_branch_values,
                "preview_branch_values": preview_branch_values,
                "branch_cells": branch_cells,
                "changed": row_changed,
            }
        )

    preview_summary = {
        "changed_cells": changed_cells,
        "current_total": current_total,
        "preview_total": preview_total,
        "delta_total": preview_total - current_total,
        "branch_deltas": [
            {
                "branch_code": code,
                "current_total": payload["current"],
                "preview_total": payload["preview"],
                "delta_total": payload["preview"] - payload["current"],
            }
            for code, payload in branch_deltas.items()
        ],
    }
    distinct_families = sorted({row["family"] for row in rows if row["family"]})
    distinct_categories = sorted({row["category"] for row in rows if row["category"]})
    return {
        "rows": rows,
        "branch_codes": visible_branch_codes,
        "all_branch_codes": all_branch_codes,
        "scope_start": start_date,
        "scope_end": end_date,
        "scope_label": scope_label,
        "preview_summary": preview_summary,
        "family_options": distinct_families,
        "category_options": distinct_categories,
        "branch_code_id_map": branch_code_id_map,
    }


def _merge_adjustment_entries(
    *,
    base_entries: dict[tuple[int, int], Decimal],
    posted_values: dict[tuple[int, int], Decimal],
    baseline_values: dict[tuple[int, int], Decimal],
) -> dict[tuple[int, int], Decimal]:
    merged = dict(base_entries)
    for key, submitted_value in posted_values.items():
        if submitted_value == baseline_values.get(key, ZERO):
            merged.pop(key, None)
        else:
            merged[key] = submitted_value
    return merged


def _draft_entries_payload(entries_map: dict[tuple[int, int], Decimal]) -> list[dict]:
    payload: list[dict] = []
    for (product_id, branch_id), target_qty in sorted(entries_map.items()):
        payload.append(
            {
                "product_id": product_id,
                "branch_id": branch_id,
                "target_qty": str(target_qty),
            }
        )
    return payload


def _event_production_window(event: EventoVenta) -> tuple[date, date]:
    start = getattr(event, "fecha_inicio", None) or (event.main_date - timedelta(days=1))
    end = getattr(event, "fecha_fin", None) or event.main_date
    if start > end:
        start, end = end, start
    return start, end


def _point_price_map_by_recipe_code(product_ids: set[int]) -> dict[int, Decimal]:
    recipes = Receta.objects.filter(id__in=product_ids).only("id", "codigo_point")
    code_by_recipe_id = {recipe.id: (recipe.codigo_point or "").strip() for recipe in recipes}
    codes = {code for code in code_by_recipe_id.values() if code}
    point_prices: dict[str, Decimal] = {}
    products = (
        PointProduct.objects.filter(sku__in=codes, precio__isnull=False)
        .order_by("-precio_activo", "-active", "-precio_actualizado_en", "-updated_at", "id")
        .only("sku", "precio")
    )
    for point_product in products:
        sku = (point_product.sku or "").strip()
        if sku and sku not in point_prices:
            point_prices[sku] = Decimal(str(point_product.precio or 0))
    return {recipe_id: point_prices.get(code, ZERO) for recipe_id, code in code_by_recipe_id.items()}


def _recent_point_sale_product_ids(*, reference_date: date, product_ids: set[int], lookback_days: int) -> set[int]:
    if not product_ids:
        return set()
    start_date = reference_date - timedelta(days=lookback_days)
    return set(
        PointDailySale.objects.filter(
            receta_id__in=product_ids,
            sale_date__gte=start_date,
            sale_date__lt=reference_date,
            quantity__gt=0,
        )
        .values_list("receta_id", flat=True)
        .distinct()
    )


def _historical_point_price_map(*, reference_date: date, product_ids: set[int]) -> dict[int, Decimal]:
    if not product_ids:
        return {}
    start_date = reference_date - timedelta(days=EVENTO_PRODUCCION_PRICE_LOOKBACK_DAYS)
    price_samples: dict[int, list[Decimal]] = {}
    rows = (
        PointDailySale.objects.filter(
            receta_id__in=product_ids,
            sale_date__gte=start_date,
            sale_date__lt=reference_date,
            quantity__gt=0,
            net_amount__gt=0,
        )
        .values_list("receta_id", "quantity", "net_amount")
        .iterator()
    )
    for receta_id, quantity, net_amount in rows:
        qty = Decimal(str(quantity or 0))
        amount = Decimal(str(net_amount or 0))
        if qty <= 0 or amount <= 0:
            continue
        price_samples.setdefault(int(receta_id), []).append(amount / qty)

    prices: dict[int, Decimal] = {}
    for product_id, samples in price_samples.items():
        ordered = sorted(samples)
        midpoint = len(ordered) // 2
        if len(ordered) % 2:
            prices[product_id] = ordered[midpoint]
        else:
            prices[product_id] = (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")
    return prices


def _event_production_price_map(*, reference_date: date, product_ids: set[int]) -> dict[int, Decimal]:
    price_map = _point_price_map_by_recipe_code(product_ids)
    unresolved_ids = {product_id for product_id, price in price_map.items() if Decimal(str(price or 0)) <= 0}
    for product_id, price in _historical_point_price_map(reference_date=reference_date, product_ids=unresolved_ids).items():
        if price > 0:
            price_map[product_id] = price
    return price_map


def _event_production_campaign_noise_product_ids(product_ids: set[int]) -> set[int]:
    excluded_ids: set[int] = set()
    products = Receta.objects.filter(id__in=product_ids).only("id", "nombre", "familia")
    for product in products:
        name_norm = _ascii_norm(product.nombre or "")
        family_norm = _ascii_norm(product.familia or "")
        has_campaign_marker = "arcoiris" in name_norm or "🌈" in (product.nombre or "")
        is_bollo_mini = family_norm == "bollo" and "mini" in name_norm
        if has_campaign_marker or is_bollo_mini:
            excluded_ids.add(product.id)
    return excluded_ids


def _money(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


def _qty(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


def _production_qty(value: Decimal) -> int:
    qty = Decimal(str(value or 0))
    if qty <= 0:
        return 0
    return math.ceil(float(qty))


def _event_production_module_tabs(event: EventoVenta) -> list[dict[str, str | bool]]:
    tabs = [
        ("detalle", reverse("ventas:evento_detail", kwargs={"event_id": event.id}), "Detalle del evento"),
        ("produccion", reverse("ventas:evento_produccion", kwargs={"event_id": event.id}), "Plan de Producción"),
    ]
    return [
        {"key": key, "url": url, "label": label, "active": key == "produccion"}
        for key, url, label in tabs
    ]


def _event_production_families(product_ids: set[int]) -> list[str]:
    return list(
        Receta.objects.filter(id__in=product_ids, tipo=Receta.TIPO_PRODUCTO_FINAL)
        .exclude(familia="")
        .order_by("familia")
        .values_list("familia", flat=True)
        .distinct()
    )


def _event_production_totals(rows: list[dict]) -> dict[str, Decimal | int]:
    return {
        "qty_9": sum((row["qty_9"] for row in rows), 0),
        "qty_10": sum((row["qty_10"] for row in rows), 0),
        "qty_total": sum((row["qty_total"] for row in rows), 0),
        "ingreso_9": _money(sum((row["ingreso_9"] for row in rows), ZERO)),
        "ingreso_10": _money(sum((row["ingreso_10"] for row in rows), ZERO)),
        "ingreso_total": _money(sum((row["ingreso_total"] for row in rows), ZERO)),
    }


def _event_production_group_rows(rows: list[dict]) -> tuple[list[dict], dict[str, Decimal]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["familia"]].append(row)

    groups = []
    grand_rows: list[dict] = []
    for family in sorted(grouped):
        family_rows = sorted(grouped[family], key=lambda item: (-item["qty_total"], item["nombre"]))
        grand_rows.extend(family_rows)
        groups.append({"familia": family, "rows": family_rows, "total": _event_production_totals(family_rows)})
    return groups, _event_production_totals(grand_rows)


def _build_event_production_dashboard(event: EventoVenta, *, familia: str = "") -> dict:
    start_date, end_date = _event_production_window(event)
    forecast_dates = [start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)]
    reference_date = timezone.localdate()
    scoped_qs = _event_forecast_qs(event).filter(forecast_date__in=forecast_dates)
    product_totals = {
        int(row["product_id"]): Decimal(str(row["total_qty"] or 0))
        for row in scoped_qs.values("product_id").annotate(total_qty=Sum("final_forecast"))
    }
    candidate_product_ids = {
        product_id
        for product_id, total_qty in product_totals.items()
        if total_qty >= EVENTO_PRODUCCION_MIN_QTY
    }
    recent_product_ids = _recent_point_sale_product_ids(
        reference_date=reference_date,
        product_ids=candidate_product_ids,
        lookback_days=EVENTO_PRODUCCION_RECENT_ACTIVITY_DAYS,
    )
    price_map = _event_production_price_map(reference_date=reference_date, product_ids=recent_product_ids)
    campaign_noise_ids = _event_production_campaign_noise_product_ids(recent_product_ids)
    eligible_product_ids = {
        product_id
        for product_id in recent_product_ids
        if product_id not in campaign_noise_ids and Decimal(str(price_map.get(product_id) or 0)) > 0
    }
    raw_rows = list(
        scoped_qs.filter(product_id__in=eligible_product_ids)
        .values("product_id", "product__nombre", "forecast_date")
        .annotate(total_qty=Sum("final_forecast"))
        .filter(total_qty__gt=0)
        .order_by("forecast_date", "-total_qty", "product__nombre")
    )
    recipes = {
        recipe.id: recipe
        for recipe in Receta.objects.filter(
            id__in=eligible_product_ids,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        ).only("id", "nombre", "familia")
    }

    by_day: dict[date, dict] = {
        forecast_date: {
            "fecha": forecast_date,
            "label": forecast_date.strftime("%d/%m/%Y"),
            "productos": [],
            "totales": {"qty": ZERO, "ingreso": ZERO},
        }
        for forecast_date in forecast_dates
    }
    consolidated_map: dict[int, dict] = {}

    for row in raw_rows:
        product_id = int(row["product_id"])
        recipe = recipes.get(product_id)
        if not recipe:
            continue
        product_family = (recipe.familia or "").strip() or "Sin familia"
        if familia and product_family != familia:
            continue
        forecast_date = row["forecast_date"]
        product_name = recipe.nombre or row.get("product__nombre") or ""
        qty = Decimal(str(row.get("total_qty") or 0))
        unit_price = price_map.get(product_id, ZERO)
        revenue = qty * unit_price * EVENTO_PRODUCCION_REVENUE_FACTOR
        day_payload = by_day[forecast_date]
        item = {
            "product_id": product_id,
            "nombre": product_name,
            "familia": product_family,
            "qty": _production_qty(qty),
            "precio": _money(unit_price),
            "ingreso": _money(revenue),
            "pct": ZERO,
        }
        day_payload["productos"].append(item)
        day_payload["totales"]["qty"] += qty
        day_payload["totales"]["ingreso"] += revenue

        consolidated = consolidated_map.setdefault(
            product_id,
            {
                "product_id": product_id,
                "nombre": product_name,
                "familia": product_family,
                "qty": ZERO,
                "qty_by_date": {forecast_date: ZERO for forecast_date in forecast_dates},
                "precio": unit_price,
                "ingreso": ZERO,
                "ingreso_by_date": {forecast_date: ZERO for forecast_date in forecast_dates},
                "pct": ZERO,
            },
        )
        consolidated["qty"] += qty
        consolidated["qty_by_date"][forecast_date] += qty
        consolidated["ingreso"] += revenue
        consolidated["ingreso_by_date"][forecast_date] += revenue

    total_qty = sum((payload["totales"]["qty"] for payload in by_day.values()), ZERO)
    total_ingreso = sum((payload["totales"]["ingreso"] for payload in by_day.values()), ZERO)

    for day_payload in by_day.values():
        day_total_ingreso = day_payload["totales"]["ingreso"]
        for item in day_payload["productos"]:
            item["pct"] = (
                ((item["ingreso"] / day_total_ingreso) * Decimal("100")).quantize(Decimal("0.01"))
                if day_total_ingreso > 0
                else ZERO
            )
        day_payload["productos"].sort(key=lambda item: (-item["qty"], item["nombre"]))
        day_payload["totales"]["qty"] = sum((item["qty"] for item in day_payload["productos"]), 0)
        day_payload["totales"]["ingreso"] = _money(day_payload["totales"]["ingreso"])

    consolidated_productos = []
    for item in consolidated_map.values():
        item["qty"] = sum((_production_qty(qty) for qty in item["qty_by_date"].values()), 0)
        item["precio"] = _money(item["precio"])
        item["ingreso"] = _money(item["ingreso"])
        item["pct"] = (
            ((item["ingreso"] / total_ingreso) * Decimal("100")).quantize(Decimal("0.01"))
            if total_ingreso > 0
            else ZERO
        )
        consolidated_productos.append(item)
    consolidated_productos.sort(key=lambda item: (-item["qty"], item["nombre"]))

    dias = sorted(by_day.values(), key=lambda payload: payload["fecha"])
    first_day = dias[0] if dias else None
    last_day = dias[-1] if dias else None
    first_date = first_day["fecha"] if first_day else None
    last_date = last_day["fecha"] if last_day else None
    for day_payload in dias:
        day_rows = []
        for index, item in enumerate(day_payload["productos"], start=1):
            is_first_day = day_payload["fecha"] == first_date
            is_last_day = day_payload["fecha"] == last_date
            day_rows.append(
                {
                    "index": index,
                    "product_id": item["product_id"],
                    "nombre": item["nombre"],
                    "familia": item["familia"],
                    "qty_9": item["qty"] if is_first_day else 0,
                    "qty_10": item["qty"] if is_last_day else 0,
                    "qty_total": item["qty"],
                    "precio": item["precio"],
                    "ingreso_9": item["ingreso"] if is_first_day else ZERO,
                    "ingreso_10": item["ingreso"] if is_last_day else ZERO,
                    "ingreso_total": item["ingreso"],
                    "pct": item["pct"],
                }
            )
        day_payload["groups"], day_payload["grand_total"] = _event_production_group_rows(day_rows)
    table_rows = []
    for index, item in enumerate(consolidated_productos, start=1):
        qty_9 = _production_qty(item["qty_by_date"].get(first_date, ZERO)) if first_date else 0
        qty_10 = _production_qty(item["qty_by_date"].get(last_date, ZERO)) if last_date else 0
        ingreso_9 = _money(item["ingreso_by_date"].get(first_date, ZERO)) if first_date else ZERO
        ingreso_10 = _money(item["ingreso_by_date"].get(last_date, ZERO)) if last_date else ZERO
        table_rows.append(
            {
                "index": index,
                "product_id": item["product_id"],
                "nombre": item["nombre"],
                "familia": item["familia"],
                "qty_9": qty_9,
                "qty_10": qty_10,
                "qty_total": item["qty"],
                "precio": item["precio"],
                "ingreso_9": ingreso_9,
                "ingreso_10": ingreso_10,
                "ingreso_total": item["ingreso"],
                "pct": item["pct"],
            }
        )
    groups, grand_total = _event_production_group_rows(table_rows)
    consolidado = {
        "label": "Consolidado",
        "productos": consolidated_productos,
        "totales": {
            "qty": _qty(total_qty),
            "ingreso": _money(total_ingreso),
        },
        "groups": groups,
        "grand_total": grand_total,
    }
    return {
        "event": event,
        "start_date": start_date,
        "end_date": end_date,
        "factor": EVENTO_PRODUCCION_REVENUE_FACTOR,
        "module_tabs": _event_production_module_tabs(event),
        "familias": _event_production_families(eligible_product_ids),
        "selected_familia": familia,
        "groups": groups,
        "grand_total": grand_total,
        "resumen": {
            "n_productos": len(consolidated_productos),
            "total_piezas": grand_total["qty_total"],
            "ingreso_total": grand_total["ingreso_total"],
            "ingreso_dia_9": grand_total["ingreso_9"],
            "ingreso_dia_10": grand_total["ingreso_10"],
            "first_day_label": first_day["label"] if first_day else "",
            "last_day_label": last_day["label"] if last_day else "",
        },
        "consolidado": consolidado,
        "dias": dias,
    }


def _event_production_csv_response(event: EventoVenta, dashboard: dict) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="evento-{event.id}-produccion.csv"'
    writer = csv.writer(response, delimiter=";")
    writer.writerow(
        [
            "SECCION",
            "DIA",
            "RECETA_ID",
            "RECETA_NOMBRE",
            "FAMILIA",
            "QTY_CONSOLIDADO",
            "QTY_9_MAY",
            "QTY_10_MAY",
            "PRECIO_UNITARIO",
            "INGRESO_PROYECTADO",
            "PORCENTAJE_TOTAL",
        ]
    )
    for group in dashboard["groups"]:
        for row in group["rows"]:
            writer.writerow(
                [
                    "CONSOLIDADO",
                    "",
                    row["product_id"],
                    row["nombre"],
                    row["familia"],
                    row["qty_total"],
                    row["qty_9"],
                    row["qty_10"],
                    row["precio"],
                    row["ingreso_total"],
                    row["pct"],
                ]
            )
    for day in dashboard["dias"]:
        for group in day["groups"]:
            for row in group["rows"]:
                writer.writerow(
                    [
                        "DIA",
                        day["fecha"],
                        row["product_id"],
                        row["nombre"],
                        row["familia"],
                        row["qty_total"],
                        row["qty_9"],
                        row["qty_10"],
                        row["precio"],
                        row["ingreso_total"],
                        row["pct"],
                    ]
                )
    return response

def _extract_posted_adjustment_values(request) -> dict[tuple[int, int], Decimal]:
    values: dict[tuple[int, int], Decimal] = {}
    for key, raw_value in request.POST.items():
        if not key.startswith("value__"):
            continue
        try:
            _, product_id, branch_id = key.split("__", 2)
            values[(int(product_id), int(branch_id))] = _decimal_or_zero(raw_value)
        except (ValueError, TypeError, InvalidOperation):
            continue
    return values


def _save_adjustment_draft(
    event: EventoVenta,
    *,
    scope_mode: str,
    draft: EventoVentaAdjustmentDraft | None,
    entries_map: dict[tuple[int, int], Decimal],
    preview_summary: dict,
    notes: str,
    user,
) -> EventoVentaAdjustmentDraft:
    if draft is None:
        draft = EventoVentaAdjustmentDraft(
            sales_event=event,
            scope_mode=scope_mode,
            created_by=user,
        )
    draft.status = EventoVentaAdjustmentDraft.STATUS_DRAFT
    draft.forecast_version = event.version
    draft.notes = notes
    draft.entries_json = _draft_entries_payload(entries_map)
    draft.preview_json = {
        "changed_cells": preview_summary.get("changed_cells", 0),
        "current_total": str(preview_summary.get("current_total", ZERO)),
        "preview_total": str(preview_summary.get("preview_total", ZERO)),
        "delta_total": str(preview_summary.get("delta_total", ZERO)),
    }
    draft.save()
    return draft


def _finalize_adjustment_draft(
    event: EventoVenta,
    *,
    scope_mode: str,
    entries_map: dict[tuple[int, int], Decimal],
    notes: str,
    user,
) -> int:
    scope_start, scope_end, _scope_label = _adjustment_scope_window(event, scope_mode)
    changed = 0
    for (product_id, branch_id), target_qty in entries_map.items():
        product = Receta.objects.filter(pk=product_id).first()
        branch = Sucursal.objects.filter(pk=branch_id).first()
        if not product or not branch:
            continue
        qs = _event_forecast_qs(event).filter(
            product=product,
            branch=branch,
            forecast_date__range=(scope_start, scope_end),
        )
        if _apply_adjustment_to_queryset(
            event=event,
            rows_qs=qs,
            target_qty=target_qty,
            actor=user,
            reason=notes or "Ajuste interactivo ventas",
            branch=branch,
            product=product,
        ):
            changed += 1
    return changed


def _product_projection_rows(forecast_qs, *, forecast_date: date | None = None):
    scoped_qs = forecast_qs.filter(forecast_date=forecast_date) if forecast_date else forecast_qs
    scope_label = str(forecast_date) if forecast_date else "Semana del evento"
    rows = list(
        scoped_qs.values("product__familia", "product__categoria", "product__nombre")
        .annotate(
            base_total=Sum("base_demand"),
            uplift_total=Sum("event_uplift"),
            trend_total=Sum("trend_adjustment"),
            forecast_total=Sum("final_forecast"),
            confidence_avg=Avg("confidence_score"),
            direct_count=Count("id", filter=Q(explanation_json__base_method__in=DIRECT_METHODS)),
            comparable_count=Count("id", filter=Q(explanation_json__base_method__in=COMPARABLE_METHODS)),
            fallback_count=Count("id", filter=Q(explanation_json__base_method__in=FALLBACK_METHODS)),
            no_data_count=Count("id", filter=Q(explanation_json__base_method="no_data")),
        )
        .filter(forecast_total__gt=0)
        .order_by("product__familia", "product__categoria", "-forecast_total", "product__nombre")
    )
    rows = _attach_forecast_source_flags(rows)
    rows = _prepare_projection_rows(rows)
    for row in rows:
        row["explanation_text"] = _build_projection_explanation(row, scope_label=scope_label)
    return rows


def _branch_projection_rows(forecast_qs, *, forecast_date: date | None = None):
    scoped_qs = forecast_qs.filter(forecast_date=forecast_date) if forecast_date else forecast_qs
    rows = list(
        scoped_qs.values("branch__codigo")
        .annotate(
            total=Sum("final_forecast"),
            direct_count=Count("id", filter=Q(explanation_json__base_method__in=DIRECT_METHODS)),
            comparable_count=Count("id", filter=Q(explanation_json__base_method__in=COMPARABLE_METHODS)),
            fallback_count=Count("id", filter=Q(explanation_json__base_method__in=FALLBACK_METHODS)),
            no_data_count=Count("id", filter=Q(explanation_json__base_method="no_data")),
        )
        .filter(total__gt=0)
        .order_by("-total", "branch__codigo")
    )
    rows = _attach_forecast_source_flags(rows)
    for row in rows:
        row["total"] = _round_projection_qty(row.get("total"))
    return [row for row in rows if row["total"] >= 1]


def _branch_product_projection_rows(forecast_qs, *, forecast_date: date | None = None):
    scoped_qs = forecast_qs.filter(forecast_date=forecast_date) if forecast_date else forecast_qs
    rows = list(
        scoped_qs.values("branch__codigo", "product__familia", "product__categoria", "product__nombre")
        .annotate(
            base_total=Sum("base_demand"),
            uplift_total=Sum("event_uplift"),
            trend_total=Sum("trend_adjustment"),
            forecast_total=Sum("final_forecast"),
            confidence_avg=Avg("confidence_score"),
            direct_count=Count("id", filter=Q(explanation_json__base_method__in=DIRECT_METHODS)),
            comparable_count=Count("id", filter=Q(explanation_json__base_method__in=COMPARABLE_METHODS)),
            fallback_count=Count("id", filter=Q(explanation_json__base_method__in=FALLBACK_METHODS)),
            no_data_count=Count("id", filter=Q(explanation_json__base_method="no_data")),
        )
        .filter(forecast_total__gt=0)
        .order_by("branch__codigo", "product__familia", "product__categoria", "-forecast_total", "product__nombre")
    )
    rows = _attach_forecast_source_flags(rows)
    return _prepare_projection_rows(rows, branch_mode=True)


def _style_projection_sheet(ws, *, report_title: str, subtitle: str, max_column: int, data_start_row: int, money_columns: tuple[int, ...] = ()) -> None:
    brand_fill = PatternFill("solid", fgColor="7A163F")
    soft_fill = PatternFill("solid", fgColor="F8E6EE")
    zebra_fill = PatternFill("solid", fgColor="FCF6F8")
    section_fill = PatternFill("solid", fgColor="EBC8D7")
    white_font = Font(color="FFFFFF", bold=True, size=14)
    title_font = Font(color="FFFFFF", bold=True, size=16)
    header_font = Font(color="7A163F", bold=True)
    normal_font = Font(color="3E2A32", size=11)
    thin_border = Border(bottom=Side(style="thin", color="E8C8D5"))

    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max_column)
    ws["A1"] = report_title
    ws["A1"].fill = brand_fill
    ws["A1"].font = title_font
    ws["A1"].alignment = Alignment(horizontal="left", vertical="center")

    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=max_column)
    ws["A2"] = subtitle
    ws["A2"].fill = soft_fill
    ws["A2"].font = Font(color="7A163F", italic=True, size=11)
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center")

    for cell in ws[data_start_row]:
        cell.fill = section_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = thin_border

    for row_idx in range(data_start_row + 1, ws.max_row + 1):
        fill = zebra_fill if (row_idx - data_start_row) % 2 == 0 else None
        for col_idx in range(1, max_column + 1):
            cell = ws.cell(row_idx, col_idx)
            cell.font = normal_font
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.border = thin_border
            if fill:
                cell.fill = fill
        for col_idx in money_columns:
            ws.cell(row_idx, col_idx).number_format = '#,##0.00'
        for col_idx in range(4, max_column + 1):
            ws.cell(row_idx, col_idx).number_format = '#,##0.00'

    ws.freeze_panes = f"A{data_start_row + 1}"
    ws.auto_filter.ref = f"A{data_start_row}:{get_column_letter(max_column)}{ws.max_row}"


def _apply_wrapped_row_heights(
    ws,
    *,
    start_row: int,
    end_row: int,
    text_columns: tuple[int, ...],
    default_height: float = 20,
    tall_height: float = 36,
    extra_tall_height: float = 54,
) -> None:
    for row_idx in range(start_row, end_row + 1):
        longest = 0
        for col_idx in text_columns:
            value = ws.cell(row_idx, col_idx).value
            if value is None:
                continue
            longest = max(longest, len(str(value)))
        if longest >= 180:
            ws.row_dimensions[row_idx].height = extra_tall_height
        elif longest >= 90:
            ws.row_dimensions[row_idx].height = tall_height
        elif longest > 0:
            ws.row_dimensions[row_idx].height = default_height


def _style_dashboard_table_sheet(
    ws,
    *,
    report_title: str,
    subtitle: str,
    max_column: int,
    money_columns: tuple[int, ...] = (),
    qty_columns: tuple[int, ...] = (),
    percent_columns: tuple[int, ...] = (),
    integer_columns: tuple[int, ...] = (),
    extra_header_rows: tuple[int, ...] = (),
    text_heavy_columns: tuple[int, ...] = (),
    enable_filter: bool = True,
) -> None:
    ws.insert_rows(1, amount=2)
    brand_fill = PatternFill("solid", fgColor="7A163F")
    soft_fill = PatternFill("solid", fgColor="F8E6EE")
    zebra_fill = PatternFill("solid", fgColor="FCF6F8")
    section_fill = PatternFill("solid", fgColor="EBC8D7")
    title_font = Font(color="FFFFFF", bold=True, size=16)
    header_font = Font(color="7A163F", bold=True)
    normal_font = Font(color="3E2A32", size=11)
    thin_border = Border(bottom=Side(style="thin", color="E8C8D5"))

    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max_column)
    ws["A1"] = report_title
    ws["A1"].fill = brand_fill
    ws["A1"].font = title_font
    ws["A1"].alignment = Alignment(horizontal="left", vertical="center")

    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=max_column)
    ws["A2"] = subtitle
    ws["A2"].fill = soft_fill
    ws["A2"].font = Font(color="7A163F", italic=True, size=11)
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center")

    header_rows = {3, *(row + 2 for row in extra_header_rows)}
    for header_idx in sorted(header_rows):
        for cell in ws[header_idx]:
            cell.fill = section_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = thin_border

    for row_idx in range(4, ws.max_row + 1):
        if row_idx in header_rows:
            continue
        fill = zebra_fill if (row_idx - 3) % 2 == 0 else None
        for col_idx in range(1, max_column + 1):
            cell = ws.cell(row_idx, col_idx)
            cell.font = normal_font
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.border = thin_border
            if fill:
                cell.fill = fill
        for col_idx in money_columns:
            ws.cell(row_idx, col_idx).number_format = '$#,##0.00'
        for col_idx in qty_columns:
            ws.cell(row_idx, col_idx).number_format = '#,##0.000'
        for col_idx in percent_columns:
            ws.cell(row_idx, col_idx).number_format = '0.00"%"'
        for col_idx in integer_columns:
            ws.cell(row_idx, col_idx).number_format = '#,##0'

    if text_heavy_columns:
        _apply_wrapped_row_heights(ws, start_row=4, end_row=ws.max_row, text_columns=text_heavy_columns)

    ws.freeze_panes = "A4"
    if enable_filter:
        ws.auto_filter.ref = f"A3:{get_column_letter(max_column)}{ws.max_row}"


def _style_dashboard_summary_sheet(
    ws,
    *,
    report_title: str,
    subtitle: str,
    branch_start_row: int,
    family_start_row: int,
    financial_trusted: bool,
) -> None:
    brand_fill = PatternFill("solid", fgColor="7A163F")
    soft_fill = PatternFill("solid", fgColor="F8E6EE")
    section_fill = PatternFill("solid", fgColor="EBC8D7")
    zebra_fill = PatternFill("solid", fgColor="FCF6F8")
    title_font = Font(color="FFFFFF", bold=True, size=16)
    header_font = Font(color="7A163F", bold=True)
    normal_font = Font(color="3E2A32", size=11)
    thin_border = Border(bottom=Side(style="thin", color="E8C8D5"))

    ws.insert_rows(1, amount=2)
    ws.sheet_view.showGridLines = False
    branch_header_row = branch_start_row + 1
    family_header_row = family_start_row + 1

    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=24)
    ws["A1"] = report_title
    ws["A1"].fill = brand_fill
    ws["A1"].font = title_font
    ws["A1"].alignment = Alignment(horizontal="left", vertical="center")

    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=24)
    ws["A2"] = subtitle
    ws["A2"].fill = soft_fill
    ws["A2"].font = Font(color="7A163F", italic=True, size=11)
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center")

    for row_idx in range(3, ws.max_row + 1):
        for col_idx in range(1, 3):
            cell = ws.cell(row_idx, col_idx)
            cell.font = normal_font
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.border = thin_border
            if row_idx >= 11 and row_idx % 2 == 1:
                cell.fill = zebra_fill

    for row_idx in (10, branch_header_row, family_header_row):
        for col_idx in range(1, 3):
            cell = ws.cell(row_idx, col_idx)
            cell.fill = section_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = thin_border

    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 24
    for column in ("C", "D", "E", "F", "G", "H", "I"):
        ws.column_dimensions[column].width = 3
    for column in ("J", "K", "L", "M", "N", "O", "P", "Q"):
        ws.column_dimensions[column].width = 11
    ws.column_dimensions["R"].width = 3
    for column in ("S", "T", "U", "V", "W", "X"):
        ws.column_dimensions[column].width = 11
    _apply_wrapped_row_heights(ws, start_row=3, end_row=ws.max_row, text_columns=(1, 2))
    ws.row_dimensions[1].height = 24
    ws.row_dimensions[2].height = 20
    ws.row_dimensions[8].height = 56
    ws.row_dimensions[9].height = 40
    ws.row_dimensions[11].height = 22
    ws.row_dimensions[branch_header_row].height = 22
    ws.row_dimensions[family_header_row].height = 22

    qty_labels = {"Volumen proyectado"}
    money_labels = {
        "Venta proyectada",
        "Costo estimado",
        "Utilidad bruta estimada",
        "Ingresos escenario foco",
        "Utilidad bruta escenario foco",
        "Valorizacion parcial con costo resuelto",
        "Costo parcial con costo resuelto",
        "Utilidad bruta parcial con costo resuelto",
    }
    percent_labels = {
        "Cobertura precio %",
        "Cobertura costo %",
        "Margen bruto estimado %",
        "ROI bruto esperado %",
    }
    integer_labels = {"Productos con precio", "Productos con costo"}

    for row_idx in range(11, branch_header_row - 1):
        label = str(ws.cell(row_idx, 1).value or "").strip()
        value_cell = ws.cell(row_idx, 2)
        if label in qty_labels:
            value_cell.number_format = '#,##0.000'
        elif label in money_labels:
            value_cell.number_format = '$#,##0.00'
        elif label in percent_labels:
            value_cell.number_format = '0.00"%"'
        elif label in integer_labels:
            value_cell.number_format = '#,##0'

    branch_data_start = branch_header_row + 1
    branch_data_end = family_header_row - 1
    family_data_start = family_header_row + 1
    family_data_end = ws.max_row
    branch_value_format = '$#,##0.00' if financial_trusted else '#,##0.000'
    family_value_format = '$#,##0.00' if financial_trusted else '#,##0.000'

    for row_idx in range(branch_data_start, branch_data_end + 1):
        ws.cell(row_idx, 2).number_format = branch_value_format
    for row_idx in range(family_data_start, family_data_end + 1):
        ws.cell(row_idx, 2).number_format = family_value_format


def _branch_day_projection_rows(forecast_qs):
    rows = list(
        forecast_qs.values("forecast_date", "branch__codigo")
        .annotate(
            total=Sum("final_forecast"),
            direct_count=Count("id", filter=Q(explanation_json__base_method__in=DIRECT_METHODS)),
            comparable_count=Count("id", filter=Q(explanation_json__base_method__in=COMPARABLE_METHODS)),
            fallback_count=Count("id", filter=Q(explanation_json__base_method__in=FALLBACK_METHODS)),
            no_data_count=Count("id", filter=Q(explanation_json__base_method="no_data")),
        )
        .filter(total__gt=0)
        .order_by("forecast_date", "branch__codigo")
    )
    return _attach_forecast_source_flags(rows)


def _branch_day_projection_matrix_rows(event: EventoVenta, forecast_qs, *, start_date: date, end_date: date):
    branch_codes = [link.branch.codigo for link in _event_branch_links(event)]
    date_columns: list[date] = []
    current_day = start_date
    while current_day <= end_date:
        date_columns.append(current_day)
        current_day += timedelta(days=1)
    totals: dict[tuple[str, date], int] = {
        (row["branch__codigo"], row["forecast_date"]): _round_projection_qty(row.get("total"))
        for row in _branch_day_projection_rows(forecast_qs)
    }
    rows: list[dict] = []
    for branch_code in branch_codes:
        day_values = {day: totals.get((branch_code, day), 0) for day in date_columns}
        rows.append(
            {
                "branch__codigo": branch_code,
                "day_values": day_values,
                "total": sum(day_values.values()),
            }
        )
    return rows, date_columns


def _event_ready_for_operations(event: EventoVenta) -> bool:
    return event.status in {
        EventoVenta.STATUS_APROBADO,
        EventoVenta.STATUS_APROBADO_AJUSTES,
        EventoVenta.STATUS_ENVIADO_PROD,
        EventoVenta.STATUS_VALIDADO_PROD,
        EventoVenta.STATUS_ENVIADO_COMPRAS,
        EventoVenta.STATUS_EN_EJECUCION,
        EventoVenta.STATUS_CERRADO,
        EventoVenta.STATUS_EVALUADO,
    }


def _event_forecast_version_label(event: EventoVenta, *, active_adjustment_draft, latest_finalized_draft) -> str:
    if active_adjustment_draft:
        return "Forecast ajustado en borrador"
    if _event_ready_for_operations(event):
        return "Forecast aprobado para operación"
    if latest_finalized_draft:
        return "Forecast ajustado final listo para Dirección"
    if event.adjustments.exists():
        return "Forecast ajustado final"
    return "Forecast calculado"


def _guard_event_operations(request, event: EventoVenta):
    if _active_adjustment_draft(event):
        messages.error(request, "Existe un borrador de ajustes pendiente. Finalízalo o cancélalo antes de usar salida operativa.")
        return redirect("ventas:evento_adjustments_editor", event_id=event.id)
    if not _event_ready_for_operations(event):
        messages.error(request, "La salida operativa solo se habilita cuando la versión comercial ya fue aprobada por Dirección.")
        return redirect("ventas:evento_detail", event_id=event.id)
    return None


def _homologue_sanity_snapshot(event: EventoVenta) -> dict[str, object]:
    forecasts = list(_event_forecast_qs(event).only("product_id", "branch_id", "forecast_date", "final_forecast"))
    if not forecasts:
        return {}

    product_ids = {int(forecast.product_id) for forecast in forecasts}
    branch_ids = {int(forecast.branch_id) for forecast in forecasts}
    if not product_ids or not branch_ids:
        return {}

    _start, _end, homologue_main_day, homologue_mode = _select_event_homologue_window(
        event,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    hist_main_total = _aggregate_historical_quantity(
        start=homologue_main_day,
        end=homologue_main_day,
        product_ids=product_ids,
        branch_ids=branch_ids,
    )
    current_main_total = sum(
        (Decimal(str(forecast.final_forecast or 0)) for forecast in forecasts if forecast.forecast_date == event.main_date),
        ZERO,
    )
    executive_model = build_event_executive_projection_model(event, forecast_rows=forecasts)
    growth_anchor_factor = Decimal(str(executive_model.get("growth_anchor_factor") or executive_model.get("same_store_factor") or 1))
    floor_target = (hist_main_total * growth_anchor_factor * Decimal("0.98")) if hist_main_total > ZERO else ZERO
    below_floor = hist_main_total >= Decimal("120") and current_main_total > ZERO and current_main_total < floor_target
    return {
        "homologue_mode": homologue_mode,
        "homologue_main_day": homologue_main_day,
        "hist_main_total": hist_main_total,
        "current_main_total": current_main_total,
        "homologue_ytd_factor": growth_anchor_factor,
        "floor_target": floor_target,
        "below_floor": below_floor,
        "main_day_benchmark_sales": executive_model.get("main_day_benchmark_sales"),
        }


def _approval_blocking_findings(event: EventoVenta) -> list[str]:
    findings: list[str] = []
    forecast_qs = _event_forecast_qs(event)
    if not forecast_qs.exists():
        findings.append("El evento no tiene forecast persistido para aprobación.")
        return findings
    if not event.financials.filter(scenario="BASE").exists():
        findings.append("El evento no tiene resumen financiero BASE persistido.")

    week_start, week_end = _event_projection_window(event)
    dataset = _event_financial_dataset(event, forecast_qs, start_date=week_start, end_date=week_end)
    plausibility = dataset.get("plausibility", {})
    projection_model = dataset.get("projection_model", {}) or {}
    if plausibility.get("flagged"):
        findings.append(
            "El ingreso semanal proyectado quedó por encima de la referencia ejecutiva defendible del homólogo. "
            f"Actual=${Decimal(str(dataset['summary']['sales'])).quantize(Decimal('0.01'))} "
            f"vs referencia=${Decimal(str(plausibility['reference_sales_ceiling'])).quantize(Decimal('0.01'))} "
            f"usando homólogo {plausibility['homologue_start']}→{plausibility['homologue_end']} "
            f"({plausibility['homologue_mode']})."
        )
    target_total_qty = Decimal(str(projection_model.get("target_total_qty") or 0))
    current_total_qty = Decimal(str(projection_model.get("current_total_qty") or 0))
    target_total_with_tolerance = target_total_qty + EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE
    if target_total_qty > ZERO and current_total_qty > target_total_with_tolerance:
        findings.append(
            "La semana total del evento sigue por encima del target ejecutivo defendible. "
            f"Actual={current_total_qty.quantize(Decimal('0.001'))} "
            f"vs target={target_total_qty.quantize(Decimal('0.001'))} "
            f"(tol={EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE.quantize(Decimal('0.001'))}) "
            f"con benchmark {projection_model.get('benchmark_source') or 'sin benchmark'}."
        )

    sanity = _homologue_sanity_snapshot(event)
    if sanity.get("below_floor"):
        findings.append(
            "El día principal del evento quedó por debajo del homólogo fuerte ajustado. "
            f"Actual={Decimal(str(sanity['current_main_total'])).quantize(Decimal('0.001'))} "
            f"vs piso={Decimal(str(sanity['floor_target'])).quantize(Decimal('0.001'))} "
            f"usando homólogo {sanity['homologue_main_day']} ({sanity['homologue_mode']})."
        )
    return findings


def _enforce_approval_guard(request, event: EventoVenta, *, action_label: str):
    findings = _approval_blocking_findings(event)
    if not findings:
        return None

    detail = " | ".join(findings)
    create_unique_notification(
        event,
        f"Guard de aprobación bloqueó {action_label.lower()}: {detail[:180]}",
        severity=EventoVentaNotification.SEVERITY_CRIT,
    )
    log_evento_change(
        event,
        "EventoVenta",
        str(event.id),
        "approval_guard_blocked",
        old_data={"status": event.status},
        new_data={"action": action_label, "findings": findings},
        actor=request.user,
    )
    for finding in findings:
        messages.error(request, finding)
    return redirect("ventas:evento_detail", event_id=event.id)


def _sync_event_review_status_with_guardrails(event: EventoVenta, *, actor=None) -> bool:
    if event.status not in {EventoVenta.STATUS_LISTO_REVISION, EventoVenta.STATUS_PENDIENTE_DG}:
        return False

    findings = _approval_blocking_findings(event)
    if not findings:
        return False

    previous_status = event.status
    event.status = EventoVenta.STATUS_MODELADO
    event.save(update_fields=["status", "updated_at"])
    detail = " | ".join(findings)
    create_unique_notification(
        event,
        f"Guard de forecast regresó el evento a modelado: {detail[:180]}",
        severity=EventoVentaNotification.SEVERITY_CRIT,
    )
    log_evento_change(
        event,
        "EventoVenta",
        str(event.id),
        "approval_guard_status_reset",
        old_data={"status": previous_status},
        new_data={"status": event.status, "findings": findings},
        actor=actor,
    )
    return True


def _round_money(value) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


COMMERCIAL_CLASSIFICATION_LABELS = {
    "HISTORICO_LEGADO": "Histórico legado",
    "COMPLEMENTO_OBLIGATORIO": "Base + addon",
    "PRODUCTO_BASE_DIRECTO": "Receta directa",
    "SIN_RELACION": "Sin regla curada",
    "BLOQUEADO_POR_AMBIGUEDAD": "Bloqueado por ambigüedad",
}


def _product_interpretation_status(
    product: Receta,
    *,
    interpretation=None,
    active_lines: int | None = None,
    total_lines: int | None = None,
    has_price: bool,
    has_cost: bool,
) -> dict[str, object]:
    interpretation = interpretation or classify_commercial_recipe(product)
    if active_lines is None or total_lines is None:
        line_counts = (
            LineaReceta.objects.filter(receta=product)
            .aggregate(
                total=Count("id"),
                active=Count("id", filter=~Q(match_status=LineaReceta.STATUS_REJECTED)),
            )
        )
        total_lines = int(line_counts.get("total") or 0)
        active_lines = int(line_counts.get("active") or 0)
    component_summary = ", ".join(
        part
        for part in [
            interpretation.producto_base or "",
            interpretation.complemento or "",
            interpretation.producto_historico or "",
        ]
        if part
    )
    if has_cost:
        cost_state = "Costo confiable"
        cost_reason = "Costo comercial resuelto con la clasificación central del SKU."
    elif total_lines == 0:
        cost_state = "Bloqueado"
        cost_reason = "SKU sin BOM operativa; no existe receta utilizable para costeo."
    elif active_lines == 0:
        cost_state = "Bloqueado"
        cost_reason = "Todas las líneas están rechazadas o fuera de maestro; requiere homologación."
    elif interpretation.clasificacion == "COMPLEMENTO_OBLIGATORIO":
        cost_state = "Bloqueado"
        cost_reason = "La composición base + addon no quedó resuelta con costo confiable."
    elif interpretation.clasificacion == "HISTORICO_LEGADO":
        cost_state = "Bloqueado"
        cost_reason = "SKU legado útil para forecast, pero sin costeo comercial vigente."
    else:
        cost_state = "Bloqueado"
        cost_reason = "Costo comercial no resuelto para la interpretación central del SKU."
    if not has_price and has_cost:
        price_reason = "Precio de venta no resuelto en la ventana de referencia."
    elif has_price:
        price_reason = "Precio confiable"
    else:
        price_reason = "Precio y costo no resueltos."
    return {
        "classification_code": interpretation.clasificacion,
        "classification_label": COMMERCIAL_CLASSIFICATION_LABELS.get(
            interpretation.clasificacion,
            interpretation.clasificacion.replace("_", " ").title(),
        ),
        "classification_note": interpretation.nota_negocio or interpretation.regla_costeo,
        "component_summary": component_summary,
        "cost_state": cost_state,
        "cost_reason": cost_reason,
        "price_reason": price_reason,
    }


def _with_bar_pct(rows: list[dict], key: str) -> list[dict]:
    if not rows:
        return rows
    max_value = max(Decimal(str(row.get(key) or 0)) for row in rows) or Decimal("0")
    for row in rows:
        value = Decimal(str(row.get(key) or 0))
        row["bar_pct"] = float((value / max_value * Decimal("100")) if max_value > 0 else Decimal("0"))
    return rows


def _event_financial_dataset(event: EventoVenta, forecast_qs, *, start_date: date, end_date: date) -> dict:
    scoped_rows = list(
        forecast_qs.filter(forecast_date__range=(start_date, end_date)).select_related("product", "branch")
    )
    product_ids = {row.product_id for row in scoped_rows}
    branch_ids = {row.branch_id for row in scoped_rows}
    commercial_context = build_commercial_recipe_lookup_context(product_ids)
    recipe_map = {recipe.id: recipe for recipe in Receta.objects.filter(id__in=product_ids)}
    interpretation_map = {
        recipe_id: classify_commercial_recipe(recipe, context=commercial_context)
        for recipe_id, recipe in recipe_map.items()
    }
    line_count_map = {
        row["receta_id"]: {
            "total": int(row["total"] or 0),
            "active": int(row["active"] or 0),
        }
        for row in (
            LineaReceta.objects.filter(receta_id__in=product_ids)
            .values("receta_id")
            .annotate(
                total=Count("id"),
                active=Count("id", filter=~Q(match_status=LineaReceta.STATUS_REJECTED)),
            )
        )
    }
    price_cache = resolve_unit_prices_bulk(
        product_ids,
        start_date,
        end_date,
        branch_ids=branch_ids,
        commercial_context=commercial_context,
    )
    cost_cache = get_commercial_total_cost_map(product_ids, context=commercial_context)
    summary = {
        "qty": ZERO,
        "sales": Decimal("0"),
        "cogs": Decimal("0"),
        "profit": Decimal("0"),
        "margin": Decimal("0"),
    }
    validated_summary = {
        "sales": Decimal("0"),
        "cogs": Decimal("0"),
        "profit": Decimal("0"),
    }
    branch_data: dict[str, dict] = {}
    family_data: dict[str, dict] = {}
    product_data: dict[int, dict] = {}
    daily_data: dict[date, dict] = {}
    qty_with_price = ZERO
    qty_with_cost = ZERO
    qty_with_full_financials = ZERO
    products_with_price: set[int] = set()
    products_with_cost: set[int] = set()
    missing_price_products: set[str] = set()
    missing_cost_products: set[str] = set()
    validation_rows: dict[int, dict[str, object]] = {}
    product_branch_counts: dict[int, set[int]] = {}
    product_priced_branches: dict[int, set[int]] = {}

    for row in scoped_rows:
        qty = Decimal(str(row.final_forecast or 0))
        if qty <= 0:
            continue
        price_key = (row.product_id, row.branch_id)
        price = price_cache.get(price_key, Decimal("0"))
        cost = cost_cache.get(row.product_id, Decimal("0"))
        has_price = price > 0
        has_cost = cost > 0
        sales = price * qty
        cogs = cost * qty
        profit = sales - cogs
        family = row.product.familia or "Sin familia"
        category = row.product.categoria or "Sin categoria"
        branch_code = row.branch.codigo

        summary["qty"] += qty
        summary["sales"] += sales
        summary["cogs"] += cogs
        summary["profit"] += profit
        if has_price:
            qty_with_price += qty
            products_with_price.add(row.product_id)
        else:
            missing_price_products.add(row.product.nombre)
        if has_cost:
            qty_with_cost += qty
            products_with_cost.add(row.product_id)
        else:
            missing_cost_products.add(row.product.nombre)
        if has_price and has_cost:
            qty_with_full_financials += qty
            validated_summary["sales"] += sales
            validated_summary["cogs"] += cogs
            validated_summary["profit"] += profit
        product_branch_counts.setdefault(row.product_id, set()).add(row.branch_id)
        if has_price:
            product_priced_branches.setdefault(row.product_id, set()).add(row.branch_id)
        interpretation_meta = validation_rows.get(row.product_id)
        if interpretation_meta is None:
            interpretation = interpretation_map[row.product_id]
            line_counts = line_count_map.get(row.product_id, {"total": 0, "active": 0})
            interpretation_meta = _product_interpretation_status(
                recipe_map[row.product_id],
                interpretation=interpretation,
                total_lines=line_counts["total"],
                active_lines=line_counts["active"],
                has_price=has_price,
                has_cost=has_cost,
            )
            validation_rows[row.product_id] = {
                "product_id": row.product_id,
                "product_name": row.product.nombre,
                **interpretation_meta,
            }

        branch_entry = branch_data.setdefault(
            branch_code,
            {
                "branch_code": branch_code,
                "qty": ZERO,
                "sales": Decimal("0"),
                "cogs": Decimal("0"),
                "profit": Decimal("0"),
                "validated_sales": Decimal("0"),
                "validated_cogs": Decimal("0"),
                "validated_profit": Decimal("0"),
            },
        )
        branch_entry["qty"] += qty
        branch_entry["sales"] += sales
        branch_entry["cogs"] += cogs
        branch_entry["profit"] += profit
        if has_price and has_cost:
            branch_entry["validated_sales"] += sales
            branch_entry["validated_cogs"] += cogs
            branch_entry["validated_profit"] += profit

        family_entry = family_data.setdefault(
            family,
            {
                "family": family,
                "qty": ZERO,
                "sales": Decimal("0"),
                "cogs": Decimal("0"),
                "profit": Decimal("0"),
                "validated_sales": Decimal("0"),
                "validated_cogs": Decimal("0"),
                "validated_profit": Decimal("0"),
            },
        )
        family_entry["qty"] += qty
        family_entry["sales"] += sales
        family_entry["cogs"] += cogs
        family_entry["profit"] += profit
        if has_price and has_cost:
            family_entry["validated_sales"] += sales
            family_entry["validated_cogs"] += cogs
            family_entry["validated_profit"] += profit

        product_entry = product_data.setdefault(
            row.product_id,
            {
                "product_id": row.product_id,
                "product_name": row.product.nombre,
                "family": family,
                "category": category,
                "qty": ZERO,
                "sales": Decimal("0"),
                "cogs": Decimal("0"),
                "profit": Decimal("0"),
                "unit_price": price,
                "unit_cost": cost,
                "has_price": has_price,
                "has_cost": has_cost,
                "classification_label": interpretation_meta["classification_label"],
                "classification_note": interpretation_meta["classification_note"],
                "component_summary": interpretation_meta["component_summary"],
                "cost_state": interpretation_meta["cost_state"],
                "cost_reason": interpretation_meta["cost_reason"],
                "price_reason": interpretation_meta["price_reason"],
            },
        )
        product_entry["qty"] += qty
        product_entry["sales"] += sales
        product_entry["cogs"] += cogs
        product_entry["profit"] += profit

        daily_entry = daily_data.setdefault(
            row.forecast_date,
            {
                "date": row.forecast_date,
                "qty": ZERO,
                "sales": Decimal("0"),
                "cogs": Decimal("0"),
                "profit": Decimal("0"),
                "validated_sales": Decimal("0"),
                "validated_cogs": Decimal("0"),
                "validated_profit": Decimal("0"),
            },
        )
        daily_entry["qty"] += qty
        daily_entry["sales"] += sales
        daily_entry["cogs"] += cogs
        daily_entry["profit"] += profit
        if has_price and has_cost:
            daily_entry["validated_sales"] += sales
            daily_entry["validated_cogs"] += cogs
            daily_entry["validated_profit"] += profit

    projection_model = build_event_executive_projection_model(event, forecast_rows=scoped_rows)

    def _finalize(rows: list[dict]) -> list[dict]:
        finalized: list[dict] = []
        for row in rows:
            sales = _round_money(row.get("sales"))
            cogs = _round_money(row.get("cogs"))
            profit = _round_money(row.get("profit"))
            validated_sales = _round_money(row.get("validated_sales"))
            validated_cogs = _round_money(row.get("validated_cogs"))
            validated_profit = _round_money(row.get("validated_profit"))
            qty = Decimal(str(row.get("qty") or 0))
            row["qty"] = qty.quantize(Decimal("0.001"))
            row["sales"] = sales
            row["cogs"] = cogs
            row["profit"] = profit
            row["validated_sales"] = validated_sales
            row["validated_cogs"] = validated_cogs
            row["validated_profit"] = validated_profit
            row["margin"] = (profit / sales * Decimal("100")).quantize(Decimal("0.01")) if sales > 0 else Decimal("0.00")
            row["validated_margin"] = (
                (validated_profit / validated_sales * Decimal("100")).quantize(Decimal("0.01"))
                if validated_sales > 0
                else Decimal("0.00")
            )
            finalized.append(row)
        return finalized

    total_qty = summary["qty"].quantize(Decimal("0.001"))
    total_products = len(product_data)
    for product_id, row in validation_rows.items():
        total_branch_count = len(product_branch_counts.get(product_id, set()))
        priced_branch_count = len(product_priced_branches.get(product_id, set()))
        has_any_price = priced_branch_count > 0
        has_all_branch_prices = total_branch_count > 0 and priced_branch_count == total_branch_count
        if has_all_branch_prices:
            row["price_reason"] = "Precio confiable"
        elif has_any_price:
            row["price_reason"] = f"Precio resuelto en {priced_branch_count}/{total_branch_count} sucursales del mix."
        else:
            row["price_reason"] = "Precio de venta no resuelto en la ventana de referencia."
        if product_id in product_data:
            product_data[product_id]["price_reason"] = row["price_reason"]
    plausibility = reconcile_event_revenue_plausibility(
        event=event,
        executive_model=projection_model,
        plausibility=evaluate_event_revenue_plausibility(
            event,
            product_ids=product_ids,
            branch_ids=branch_ids,
            projected_qty=total_qty,
            projected_sales=summary["sales"],
        ),
    )
    price_qty_coverage = (
        (qty_with_price / total_qty * Decimal("100")).quantize(Decimal("0.01"))
        if total_qty > 0
        else Decimal("0.00")
    )
    cost_qty_coverage = (
        (qty_with_cost / total_qty * Decimal("100")).quantize(Decimal("0.01"))
        if total_qty > 0
        else Decimal("0.00")
    )
    full_qty_coverage = (
        (qty_with_full_financials / total_qty * Decimal("100")).quantize(Decimal("0.01"))
        if total_qty > 0
        else Decimal("0.00")
    )
    price_product_coverage = (
        (Decimal(len(products_with_price)) / Decimal(total_products) * Decimal("100")).quantize(Decimal("0.01"))
        if total_products
        else Decimal("0.00")
    )
    cost_product_coverage = (
        (Decimal(len(products_with_cost)) / Decimal(total_products) * Decimal("100")).quantize(Decimal("0.01"))
        if total_products
        else Decimal("0.00")
    )
    branch_rows = _with_bar_pct(
        sorted(_finalize(list(branch_data.values())), key=lambda item: (-item["sales"], item["branch_code"])),
        "sales",
    )
    family_rows = _with_bar_pct(
        sorted(_finalize(list(family_data.values())), key=lambda item: (-item["sales"], item["family"])),
        "sales",
    )
    product_rows = _with_bar_pct(
        sorted(_finalize(list(product_data.values())), key=lambda item: (-item["sales"], item["product_name"])),
        "sales",
    )
    daily_rows = sorted(_finalize(list(daily_data.values())), key=lambda item: item["date"])
    summary["sales"] = _round_money(summary["sales"])
    summary["cogs"] = _round_money(summary["cogs"])
    summary["profit"] = _round_money(summary["profit"])
    summary["qty"] = total_qty
    summary["margin"] = (
        (summary["profit"] / summary["sales"] * Decimal("100")).quantize(Decimal("0.01"))
        if summary["sales"] > 0
        else Decimal("0.00")
    )
    validated_summary["sales"] = _round_money(validated_summary["sales"])
    validated_summary["cogs"] = _round_money(validated_summary["cogs"])
    validated_summary["profit"] = _round_money(validated_summary["profit"])
    validated_summary["margin"] = (
        (validated_summary["profit"] / validated_summary["sales"] * Decimal("100")).quantize(Decimal("0.01"))
        if validated_summary["sales"] > 0
        else Decimal("0.00")
    )
    financial_trusted = price_qty_coverage >= Decimal("95") and cost_qty_coverage >= Decimal("95")
    validation_message = ""
    if not financial_trusted:
        validation_message = (
            "Valorización financiera bloqueada: "
            f"{len(missing_cost_products)} de {total_products or 0} productos sin costo resuelto "
            f"(cobertura de costo {cost_qty_coverage}%)."
        )
    elif plausibility.get("flagged"):
        validation_message = (
            "Ingreso calculado con precio real x piezas forecast; alerta ejecutiva contra homólogo: "
            f"referencia ${Decimal(str(plausibility['reference_sales_ceiling'])).quantize(Decimal('0.01'))} "
            f"contra homólogo {plausibility['homologue_start']}→{plausibility['homologue_end']} "
            f"({plausibility['homologue_mode']})."
        )
    elif projection_model:
        validation_message = (
            "Forecast ejecutivo trazado con cohortes de sucursal: "
            f"same-store {Decimal(str(projection_model.get('same_store_factor') or 1)).quantize(Decimal('0.0001'))}, "
            f"expansión {Decimal(str(projection_model.get('expansion_factor') or 0)).quantize(Decimal('0.0001'))}, "
            f"contracción {Decimal(str(projection_model.get('contraction_factor') or 1)).quantize(Decimal('0.0001'))}. "
            f"Benchmark {projection_model.get('benchmark_source') or 'sin benchmark'}=${Decimal(str(projection_model.get('benchmark_sales') or 0)).quantize(Decimal('0.01'))}."
        )
    return {
        "summary": summary,
        "validated_summary": validated_summary,
        "branch_rows": branch_rows,
        "family_rows": family_rows,
        "product_rows": product_rows,
        "daily_rows": daily_rows,
        "financial_trusted": financial_trusted,
        "validation_message": validation_message,
        "plausibility": plausibility,
        "projection_model": projection_model,
        "coverage": {
            "price_qty_pct": price_qty_coverage,
            "cost_qty_pct": cost_qty_coverage,
            "full_qty_pct": full_qty_coverage,
            "price_product_pct": price_product_coverage,
            "cost_product_pct": cost_product_coverage,
            "products_total": total_products,
            "products_with_price": len(products_with_price),
            "products_with_cost": len(products_with_cost),
            "missing_price_products": sorted(missing_price_products),
            "missing_cost_products": sorted(missing_cost_products),
        },
        "validation_rows": sorted(validation_rows.values(), key=lambda item: (item["cost_state"] != "Bloqueado", item["product_name"])),
    }


def _semaphore_from_pct(value: Decimal, *, green: Decimal, yellow: Decimal) -> str:
    if value >= green:
        return "VERDE"
    if value >= yellow:
        return "AMARILLO"
    return "ROJO"


def _blocked_event_products(event: EventoVenta, *, validation_rows: list[dict] | None = None) -> list[dict]:
    if validation_rows is not None:
        return [
            {
                "sku": row.get("classification_code", ""),
                "product_name": row.get("product_name", ""),
                "reason": row.get("classification_note") or row.get("cost_reason") or "",
            }
            for row in validation_rows
            if row.get("classification_code") == "BLOQUEADO_POR_AMBIGUEDAD"
        ]
    rows: list[dict] = []
    for link in (
        EventoVentaProducto.objects.filter(sales_event=event, is_active=True)
        .select_related("product")
        .order_by("product__nombre", "product__codigo_point")
    ):
        interpretation = classify_commercial_recipe(link.product)
        if interpretation.clasificacion == "BLOQUEADO_POR_AMBIGUEDAD":
            rows.append(
                {
                    "sku": interpretation.sku_actual,
                    "product_name": interpretation.producto_actual,
                    "reason": interpretation.nota_negocio or interpretation.regla_costeo,
                }
            )
    return rows


def _event_module_audit_rows(
    event: EventoVenta,
    *,
    forecast_qs=None,
    dataset: dict | None = None,
    dashboard_sheetnames: list[str] | None = None,
) -> list[dict]:
    forecast_qs = forecast_qs if forecast_qs is not None else _event_forecast_qs(event)
    week_start, week_end = _event_projection_window(event)
    dataset = dataset or _event_financial_dataset(event, forecast_qs, start_date=week_start, end_date=week_end)
    blocked_rows = _blocked_event_products(event, validation_rows=dataset.get("validation_rows"))
    daily_totals = {row["date"]: Decimal(str(row["qty"] or 0)) for row in dataset["daily_rows"]}
    previous_day_qty = daily_totals.get(event.main_date - timedelta(days=1), ZERO)
    main_day_qty = daily_totals.get(event.main_date, ZERO)
    forecast_summary = forecast_qs.aggregate(total_rows=Count("id"), avg=Avg("confidence_score"))
    forecast_count = int(forecast_summary.get("total_rows") or 0)
    avg_confidence = forecast_summary.get("avg") or Decimal("0")
    avg_confidence = Decimal(str(avg_confidence or 0)).quantize(Decimal("0.01"))
    production_summary = EventoVentaProductionLine.objects.filter(production_plan__sales_event=event).aggregate(
        total=Count("id"),
        blocked=Count("id", filter=Q(constraint_reason__icontains="ambigu")),
        constrained=Count("id", filter=Q(capacity_gap_qty__gt=0)),
    )
    input_summary = event.input_requirements.aggregate(
        total=Count("id"),
        absurd=Count("id", filter=Q(required_qty__gt=Decimal("100000"))),
        shortages=Count("id", filter=Q(net_shortage_qty__gt=0)),
        purchase_sensitive_shortages=Count(
            "id",
            filter=Q(
                net_shortage_qty__gt=0,
                input_item__tipo_item__in=[Insumo.TIPO_MATERIA_PRIMA, Insumo.TIPO_EMPAQUE],
            ),
        ),
    )
    purchase_summary = event.purchase_requirements.aggregate(
        total=Count("id"),
        pending=Count("id", filter=Q(status=EventoVentaPurchaseRequirement.STATUS_PENDIENTE)),
    )
    production_total = int(production_summary.get("total") or 0)
    production_blocked = int(production_summary.get("blocked") or 0)
    production_constrained = int(production_summary.get("constrained") or 0)
    input_total = int(input_summary.get("total") or 0)
    input_absurd = int(input_summary.get("absurd") or 0)
    input_shortages = int(input_summary.get("shortages") or 0)
    purchase_sensitive_shortages = int(input_summary.get("purchase_sensitive_shortages") or 0)
    purchase_total = int(purchase_summary.get("total") or 0)
    purchase_pending = int(purchase_summary.get("pending") or 0)
    artifact_map = {artifact.export_type: artifact for artifact in _latest_projection_artifacts(event)}
    resolved_sheetnames = list(dashboard_sheetnames or [])
    if not resolved_sheetnames:
        dashboard_artifact = artifact_map.get(EventoVentaProjectionArtifact.TYPE_DASHBOARD)
        if dashboard_artifact and Path(dashboard_artifact.file_path).exists():
            try:
                wb = load_workbook(dashboard_artifact.file_path, read_only=True)
                resolved_sheetnames = list(wb.sheetnames)
            except Exception:
                resolved_sheetnames = []

    rows = [
        {
            "module": "Catálogo / Gobernanza SKU",
            "status": "ROJO" if blocked_rows else "VERDE",
            "detail": (
                f"{len(blocked_rows)} SKU bloqueados por ambigüedad en el evento."
                if blocked_rows
                else "Todos los SKU del evento tienen clasificación comercial defendible."
            ),
        },
        {
            "module": "Forecast comercial",
            "status": (
                "ROJO"
                if not forecast_count or blocked_rows
                else "AMARILLO"
                if avg_confidence < Decimal("0.55") or (previous_day_qty > main_day_qty and main_day_qty > 0)
                else "VERDE"
            ),
            "detail": (
                f"{forecast_count} filas, confianza promedio {avg_confidence}, 9-mayo={previous_day_qty}, 10-mayo={main_day_qty}."
            ),
        },
        {
            "module": "Pricing / ingresos",
            "status": (
                "AMARILLO"
                if dataset.get("plausibility", {}).get("flagged")
                else _semaphore_from_pct(dataset["coverage"]["price_qty_pct"], green=Decimal("95"), yellow=Decimal("85"))
            ),
            "detail": (
                f"Cobertura de precio por volumen {dataset['coverage']['price_qty_pct']}%."
                if not dataset.get("plausibility", {}).get("flagged")
                else (
                    f"Cobertura de precio {dataset['coverage']['price_qty_pct']}%; ingreso calculado con precio real x piezas "
                    f"y referencia ejecutiva ${Decimal(str(dataset['plausibility']['reference_sales_ceiling'])).quantize(Decimal('0.01'))} "
                    f"contra homólogo {dataset['plausibility']['homologue_start']}→{dataset['plausibility']['homologue_end']}."
                )
            ),
        },
        {
            "module": "Costeo / utilidad / ROI",
            "status": (
                "VERDE"
                if dataset["financial_trusted"]
                else _semaphore_from_pct(dataset["coverage"]["cost_qty_pct"], green=Decimal("95"), yellow=Decimal("85"))
            ),
            "detail": (
                f"Cobertura de costo {dataset['coverage']['cost_qty_pct']}%. "
                + (
                    "Valorización confiable. El ROI visible es bruto sobre costo directo."
                    if dataset["financial_trusted"]
                    else "Valorización bloqueada o parcial."
                )
            ),
        },
        {
            "module": "Producción",
            "status": (
                "ROJO"
                if not production_total or production_blocked
                else "AMARILLO"
                if production_constrained
                else "VERDE"
            ),
            "detail": (
                f"{production_total} líneas; bloqueadas={production_blocked}, limitadas por capacidad={production_constrained}."
            ),
        },
        {
            "module": "Insumos",
            "status": (
                "ROJO"
                if not input_total or input_absurd
                else "AMARILLO"
                if input_shortages
                else "VERDE"
            ),
            "detail": (
                f"{input_total} requerimientos; absurdos={input_absurd}, faltantes={input_shortages}."
            ),
        },
        {
            "module": "Compras",
            "status": (
                "ROJO"
                if purchase_sensitive_shortages and not purchase_total
                else "AMARILLO"
                if purchase_pending
                else "VERDE"
            ),
            "detail": (
                f"{purchase_total} requerimientos de compra; pendientes={purchase_pending}."
            ),
        },
        {
            "module": "Dashboard UI",
            "status": (
                "VERDE"
                if {"Gobernanza SKU", "Auditoria"}.issubset(set(resolved_sheetnames))
                else "AMARILLO"
                if resolved_sheetnames
                else "ROJO"
            ),
            "detail": (
                f"Hojas dashboard: {', '.join(resolved_sheetnames)}."
                if resolved_sheetnames
                else "No existe dashboard ejecutivo vigente."
            ),
        },
        {
            "module": "Excel / exports",
            "status": (
                "VERDE"
                if {
                    EventoVentaProjectionArtifact.TYPE_WEEK,
                    EventoVentaProjectionArtifact.TYPE_DAY,
                    EventoVentaProjectionArtifact.TYPE_DAILY,
                    EventoVentaProjectionArtifact.TYPE_DASHBOARD,
                    EventoVentaProjectionArtifact.TYPE_PACKAGE,
                }.issubset(set(artifact_map.keys()))
                else "ROJO"
            ),
            "detail": f"Artifacts vigentes: {', '.join(sorted(artifact_map.keys())) or 'ninguno'}.",
        },
        {
            "module": "Trazabilidad / auditabilidad",
            "status": (
                "VERDE"
                if artifact_map and event.notifications.exists()
                else "AMARILLO"
                if artifact_map or event.notifications.exists()
                else "ROJO"
            ),
            "detail": f"Notificaciones={event.notifications.count()}, artifacts={len(artifact_map)}, versión actual={event.version}.",
        },
    ]
    return rows


def _production_operational_dataset(event: EventoVenta, forecast_qs, *, start_date: date, end_date: date) -> dict:
    scoped_forecasts = list(
        forecast_qs.filter(forecast_date__range=(start_date, end_date)).only(
            "id",
            "forecast_date",
            "branch_id",
            "product_id",
            "final_forecast",
            "conservative_forecast",
            "aggressive_forecast",
            "confidence_score",
        )
    )
    demand_rows_raw = list(
        forecast_qs.filter(forecast_date__range=(start_date, end_date))
        .values("forecast_date", "branch_id", "branch__codigo", "product_id", "product__nombre")
        .annotate(total=Sum("final_forecast"))
        .order_by("forecast_date", "branch__codigo", "product__nombre")
    )
    commercial_context = build_commercial_recipe_lookup_context({row.product_id for row in scoped_forecasts})
    target_map = build_operational_targets(event, commercial_context=commercial_context)
    forecast_rows = {
        (row.forecast_date, row.branch_id, row.product_id): row
        for row in scoped_forecasts
    }
    demand_rows: list[dict] = []
    supply_rows: list[dict] = []
    sunday_qty = ZERO
    for row in demand_rows_raw:
        demand_qty = Decimal(str(row.get("total") or 0)).quantize(Decimal("0.001"))
        demand_rows.append(
            {
                "date": row["forecast_date"],
                "branch_code": row["branch__codigo"],
                "product_name": row["product__nombre"],
                "qty": demand_qty,
            }
        )
        if row["forecast_date"].weekday() == 6:
            sunday_qty += demand_qty
        forecast_row = forecast_rows.get((row["forecast_date"], row["branch_id"], row["product_id"]))
        target = target_map.get(int(forecast_row.id)) if forecast_row else None
        suggested_supply = (
            Decimal(str(target.target_qty)).quantize(Decimal("0.001"))
            if target
            else demand_qty
        )
        supply_rows.append(
            {
                "sale_date": row["forecast_date"],
                "branch_code": row["branch__codigo"],
                "product_name": row["product__nombre"],
                "approved_demand": demand_qty,
                "visible_inventory": "N/D",
                "policy_target": suggested_supply,
                "suggested_supply": suggested_supply,
                "coverage_days": SAFE_SHELF_LIFE_DAYS,
                "warning": target.reason if target else "Sin objetivo operativo calculado; sugerencia igual a la demanda esperada.",
            }
        )

    demand_destination_map: dict[tuple[date, int], dict[str, Decimal]] = {}
    for row in demand_rows_raw:
        plan_day, _warning = _resolve_production_day(row["forecast_date"])
        key = (plan_day, row["product_id"])
        demand_destination_map.setdefault(key, {})
        demand_destination_map[key][row["branch__codigo"]] = demand_destination_map[key].get(row["branch__codigo"], ZERO) + Decimal(
            str(row.get("total") or 0)
        )

    production_plans = list(
        event.production_plans.prefetch_related("lines__product").order_by("plan_date", "id")
    )
    production_rows: list[dict] = []
    capacity_gap_total = ZERO
    for plan in production_plans:
        for line in plan.lines.all():
            destinations = demand_destination_map.get((plan.plan_date, line.product_id), {})
            destination_label = ", ".join(
                f"{branch_code}: {Decimal(str(qty or 0)).quantize(Decimal('0.001')).normalize()}"
                for branch_code, qty in sorted(destinations.items(), key=lambda item: (-item[1], item[0]))[:4]
            ) or "Sin desglose por sucursal"
            capacity_gap_total += Decimal(str(line.capacity_gap_qty or 0))
            production_rows.append(
                {
                    "plan_date": plan.plan_date,
                    "product_name": line.product.nombre,
                    "required_qty": Decimal(str(line.required_qty or 0)).quantize(Decimal("0.001")),
                    "planned_qty": Decimal(str(line.planned_qty or 0)).quantize(Decimal("0.001")),
                    "net_qty_to_produce": Decimal(str(line.net_qty_to_produce or 0)).quantize(Decimal("0.001")),
                    "existing_stock": Decimal(str(line.existing_finished_stock or 0)).quantize(Decimal("0.001")),
                    "capacity_gap_qty": Decimal(str(line.capacity_gap_qty or 0)).quantize(Decimal("0.001")),
                    "priority": line.priority or "MEDIA",
                    "destinations": destination_label,
                    "constraint_reason": line.constraint_reason or "",
                }
            )

    high_risk_inputs = list(
        event.input_requirements.filter(risk_level=EventoVentaInputRequirement.RISK_HIGH)
        .select_related("input_item")
        .order_by("-net_shortage_qty", "input_item__nombre")[:10]
    )
    purchase_rows = list(
        event.purchase_requirements.select_related("input_requirement__input_item", "supplier")
        .order_by("purchase_deadline", "-estimated_cost")[:12]
    )
    alerts: list[dict] = []
    if sunday_qty > 0:
        alerts.append(
            {
                "level": "WARN",
                "title": "Demanda dominical reprogramada",
                "detail": f"Hay {sunday_qty.quantize(Decimal('0.001'))} unidades de venta dominical que deben cubrirse con producción anticipada por regla operativa.",
            }
        )
    if capacity_gap_total > 0:
        alerts.append(
            {
                "level": "CRIT",
                "title": "Capacidad insuficiente",
                "detail": f"El plan actual deja una brecha de {capacity_gap_total.quantize(Decimal('0.001'))} unidades por restricciones de capacidad.",
            }
        )
    if high_risk_inputs:
        alerts.append(
            {
                "level": "WARN",
                "title": "Insumos críticos",
                "detail": f"Hay {len(high_risk_inputs)} insumos en riesgo alto que pueden comprometer la ejecución del evento.",
            }
        )
    if not _event_ready_for_operations(event):
        alerts.append(
            {
                "level": "INFO",
                "title": "Versión preliminar",
                "detail": "Producción e insumos se muestran como proyección operativa preliminar. La versión oficial se libera al aprobar el evento.",
            }
        )

    return {
        "demand_rows": demand_rows[:120],
        "supply_rows": supply_rows[:120],
        "production_rows": production_rows[:120],
        "alerts": alerts,
        "summary": {
            "approved_basis": _event_ready_for_operations(event),
            "shelf_life_days": SAFE_SHELF_LIFE_DAYS,
            "demand_lines": len(demand_rows_raw),
            "production_lines": len(production_rows),
            "high_risk_inputs": len(high_risk_inputs),
            "pending_purchases": event.purchase_requirements.filter(status=EventoVentaPurchaseRequirement.STATUS_PENDIENTE).count(),
            "capacity_gap_total": capacity_gap_total.quantize(Decimal("0.001")),
        },
        "high_risk_inputs": high_risk_inputs,
        "purchase_rows": purchase_rows,
    }


def _approval_traceability_dataset(event: EventoVenta) -> dict:
    approvals = list(event.approvals.select_related("requested_to_user", "responded_by_user").order_by("-created_at")[:10])
    audits = list(event.audit_logs.select_related("actor_user").order_by("-created_at")[:12])
    finalized_drafts = list(
        event.adjustment_drafts.select_related("created_by", "finalized_by")
        .filter(status=EventoVentaAdjustmentDraft.STATUS_FINALIZED)
        .order_by("-finalized_at", "-updated_at")[:6]
    )
    return {
        "approvals": approvals,
        "audits": audits,
        "finalized_drafts": finalized_drafts,
    }


def _branch_product_day_projection_sheets(event: EventoVenta, forecast_qs, *, start_date: date, end_date: date):
    branch_codes = [link.branch.codigo for link in _event_branch_links(event)]
    date_columns: list[date] = []
    current_day = start_date
    while current_day <= end_date:
        date_columns.append(current_day)
        current_day += timedelta(days=1)

    aggregated_rows = list(
        forecast_qs.values(
            "branch__codigo",
            "product__familia",
            "product__categoria",
            "product__nombre",
            "forecast_date",
        )
        .annotate(total=Sum("final_forecast"))
        .order_by("branch__codigo", "product__familia", "product__categoria", "product__nombre", "forecast_date")
    )
    day_map: dict[tuple[str, tuple[str, str, str], date], int] = {}
    branch_products: dict[str, set[tuple[str, str, str]]] = {branch_code: set() for branch_code in branch_codes}
    for row in aggregated_rows:
        branch_code = row.get("branch__codigo") or ""
        product_key = (
            row.get("product__familia") or "Sin familia",
            row.get("product__categoria") or "Sin categoria",
            row.get("product__nombre") or "",
        )
        branch_products.setdefault(branch_code, set()).add(product_key)
        day_map[(branch_code, product_key, row["forecast_date"])] = _round_projection_qty(row.get("total"))

    sheet_payloads: list[dict] = []
    for branch_code in branch_codes:
        product_rows: list[dict] = []
        for product_key in sorted(branch_products.get(branch_code, set()), key=lambda item: (item[0], item[1], item[2])):
            day_values = {day: day_map.get((branch_code, product_key, day), 0) for day in date_columns}
            product_rows.append(
                {
                    "family": product_key[0],
                    "category": product_key[1],
                    "product_name": product_key[2],
                    "day_values": day_values,
                    "total": sum(day_values.values()),
                }
            )
        sheet_payloads.append(
            {
                "branch_code": branch_code,
                "rows": product_rows,
            }
        )
    return sheet_payloads, date_columns


def _projection_trend_note(rows: list[dict], *, label: str) -> str:
    if not rows:
        return f"Sin datos para calcular ajuste por tendencia en {label}."
    zero_rows = sum(1 for row in rows if (row.get("trend_total") or 0) == 0)
    if zero_rows == len(rows):
        return (
            f"En {label}, si el ajuste por tendencia = 0 es porque no se detectó aceleración o desaceleración reciente "
            "o la serie comparable fue insuficiente."
        )
    return (
        f"En {label}, si el ajuste por tendencia = 0 significa que para ese producto no se detectó un cambio reciente suficiente "
        "para subir o bajar la base."
    )


def _safe_sheet_title(raw_title: str) -> str:
    invalid_chars = set('[]:*?/\\')
    clean = "".join(ch for ch in raw_title if ch not in invalid_chars).strip() or "Hoja"
    return clean[:31]


def _append_projection_sheet(ws, rows: list[dict], *, scope_label: str) -> None:
    ws.append(
        [
            "Familia",
            "Categoria",
            "Producto",
            "Base",
            "Incremento evento",
            "Ajuste tendencia",
            "Proyeccion",
            "Confianza",
            "Origen forecast",
            "Directo",
            "Comparable",
            "Fallback",
            "Sin base",
            "Explicacion",
        ]
    )
    for row in rows:
        ws.append(
            [
                row.get("product__familia") or "Sin familia",
                row.get("product__categoria") or "Sin categoria",
                row.get("product__nombre") or "",
                float(row.get("base_total") or 0),
                float(row.get("uplift_total") or 0),
                float(row.get("trend_total") or 0),
                float(row.get("forecast_total") or 0),
                float(row.get("confidence_avg") or 0),
                row.get("source_label") or "",
                int((row.get("source_counts") or {}).get("directo") or 0),
                int((row.get("source_counts") or {}).get("comparable") or 0),
                int((row.get("source_counts") or {}).get("fallback") or 0),
                int((row.get("source_counts") or {}).get("sin_base") or 0),
                row.get("explanation_text") or "",
            ]
        )
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 40
    ws.column_dimensions["D"].width = 14
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 18
    ws.column_dimensions["G"].width = 14
    ws.column_dimensions["H"].width = 12
    ws.column_dimensions["I"].width = 20
    ws.column_dimensions["J"].width = 10
    ws.column_dimensions["K"].width = 12
    ws.column_dimensions["L"].width = 10
    ws.column_dimensions["M"].width = 10
    ws.column_dimensions["N"].width = 88


def _append_projection_summary_sheet(ws, general_rows: list[dict], branch_rows: list[dict], *, branch_codes: list[str], scope_label: str) -> None:
    headers = ["Familia", "Categoria", "Producto", "Proyeccion general", *branch_codes]
    ws.append(headers)

    branch_totals: dict[tuple[str, str, str], dict[str, int]] = {}
    for row in branch_rows:
        key = (
            row.get("product__familia") or "Sin familia",
            row.get("product__categoria") or "Sin categoria",
            row.get("product__nombre") or "",
        )
        branch_totals.setdefault(key, {})[row.get("branch__codigo") or ""] = int(
            row.get("forecast_total") or row.get("total") or 0
        )

    for row in general_rows:
        key = (
            row.get("product__familia") or "Sin familia",
            row.get("product__categoria") or "Sin categoria",
            row.get("product__nombre") or "",
        )
        values = [
            key[0],
            key[1],
            key[2],
            int(row.get("forecast_total") or 0),
            *[int(branch_totals.get(key, {}).get(branch_code, 0) or 0) for branch_code in branch_codes],
        ]
        ws.append(values)

    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 40
    ws.column_dimensions["D"].width = 18
    for idx in range(5, 5 + len(branch_codes)):
        ws.column_dimensions[get_column_letter(idx)].width = 14


def _append_branch_day_projection_sheet(ws, event: EventoVenta, forecast_qs, *, start_date: date, end_date: date, scope_label: str) -> None:
    matrix_rows, date_columns = _branch_day_projection_matrix_rows(event, forecast_qs, start_date=start_date, end_date=end_date)
    headers = ["Sucursal", "Total", *[day.strftime("%Y-%m-%d") for day in date_columns]]
    ws.append(headers)
    for row in matrix_rows:
        ws.append(
            [
                row["branch__codigo"],
                row["total"],
                *[row["day_values"][day] for day in date_columns],
            ]
        )
    ws.column_dimensions["A"].width = 16
    ws.column_dimensions["B"].width = 14
    for idx in range(3, 3 + len(date_columns)):
        ws.column_dimensions[get_column_letter(idx)].width = 12


def _append_branch_product_day_sheet(ws, branch_code: str, rows: list[dict], *, date_columns: list[date]) -> None:
    ws.append(["Sucursal", branch_code])
    ws.append([])
    ws.append(["Familia", "Categoria", "Producto", "Total", *[day.strftime("%Y-%m-%d") for day in date_columns]])
    for row in rows:
        ws.append(
            [
                row["family"],
                row["category"],
                row["product_name"],
                row["total"],
                *[row["day_values"][day] for day in date_columns],
            ]
        )
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 42
    ws.column_dimensions["D"].width = 14
    for idx in range(5, 5 + len(date_columns)):
        ws.column_dimensions[get_column_letter(idx)].width = 12


def _build_branch_day_workbook_file(event: EventoVenta) -> tuple[str, bytes]:
    forecast_qs = _week_scope_qs(event, _event_forecast_qs(event))
    scope_start, scope_end = _event_projection_window(event)
    scope_label = _week_scope_label(event)
    sheet_payloads, date_columns = _branch_product_day_projection_sheets(
        event,
        forecast_qs,
        start_date=scope_start,
        end_date=scope_end,
    )

    workbook = Workbook()
    if sheet_payloads:
        first_payload = sheet_payloads[0]
        ws = workbook.active
        ws.title = _safe_sheet_title(first_payload["branch_code"])
        ws.append(["Evento", event.name])
        ws.append(["Codigo", event.code])
        ws.append(["Fecha principal", str(event.main_date)])
        ws.append(["Alcance", scope_label])
        ws.append([])
        _append_branch_product_day_sheet(
            ws,
            first_payload["branch_code"],
            first_payload["rows"],
            date_columns=date_columns,
        )
        _style_projection_sheet(
            ws,
            report_title="Pollyana's Dolce · Proyeccion por dia",
            subtitle=f"{event.name} · {first_payload['branch_code']} · {scope_label}",
            max_column=4 + len(date_columns),
            data_start_row=8,
        )
        for payload in sheet_payloads[1:]:
            branch_ws = workbook.create_sheet(_safe_sheet_title(payload["branch_code"]))
            branch_ws.append(["Evento", event.name])
            branch_ws.append(["Codigo", event.code])
            branch_ws.append(["Fecha principal", str(event.main_date)])
            branch_ws.append(["Alcance", scope_label])
            branch_ws.append([])
            _append_branch_product_day_sheet(
                branch_ws,
                payload["branch_code"],
                payload["rows"],
                date_columns=date_columns,
            )
            _style_projection_sheet(
                branch_ws,
                report_title="Pollyana's Dolce · Proyeccion por dia",
                subtitle=f"{event.name} · {payload['branch_code']} · {scope_label}",
                max_column=4 + len(date_columns),
                data_start_row=8,
            )
    else:
        ws = workbook.active
        ws.title = "Sin datos"
        ws.append(["Evento", event.name])
        ws.append(["Codigo", event.code])
        ws.append(["Fecha principal", str(event.main_date)])
        ws.append(["Alcance", scope_label])
        ws.append([])
        ws.append(["Familia", "Categoria", "Producto", "Total", *[day.strftime("%Y-%m-%d") for day in date_columns]])
        _style_projection_sheet(
            ws,
            report_title="Pollyana's Dolce · Proyeccion por dia",
            subtitle=f"{event.name} · Sin datos · {scope_label}",
            max_column=4 + len(date_columns),
            data_start_row=6,
        )

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"ventas_evento_{event.code.lower()}_por_dia_{scope_start}_{scope_end}.xlsx"
    return filename, output.getvalue()


def _build_executive_dashboard_workbook_file(event: EventoVenta) -> tuple[str, bytes]:
    forecast_qs = _event_forecast_qs(event)
    week_start, week_end = _event_projection_window(event)
    dataset = _event_financial_dataset(event, forecast_qs, start_date=week_start, end_date=week_end)
    focused_financial = _focused_financial_row(event) if dataset["financial_trusted"] else None
    workbook = Workbook()
    ws_summary = workbook.active
    ws_summary.title = "Dashboard"
    ws_daily = workbook.create_sheet("Demanda diaria")
    ws_branches = workbook.create_sheet("Sucursales")
    ws_products = workbook.create_sheet("Productos")
    ws_families = workbook.create_sheet("Familias")
    ws_validation = workbook.create_sheet("Validacion")
    ws_audit = workbook.create_sheet("Auditoria")
    ws_governance = workbook.create_sheet("Gobernanza SKU")

    summary = dataset["summary"]
    validated_summary = dataset["validated_summary"]
    coverage = dataset["coverage"]
    scope_label = _week_scope_label(event)
    ws_summary.append(["Evento", event.name])
    ws_summary.append(["Codigo", event.code])
    ws_summary.append(["Alcance", scope_label])
    ws_summary.append(["Escenario foco", event.get_scenario_focus_display()])
    ws_summary.append(["Estado valorizacion", "Confiable" if dataset["financial_trusted"] else "Bloqueada por cobertura"])
    if dataset["validation_message"]:
        ws_summary.append(["Observacion", dataset["validation_message"]])
    ws_summary.append(["Nota financiera", "ROI y margen visibles corresponden a lectura bruta sobre costo directo; no sustituyen un P&L operativo total."])
    ws_summary.append([])
    ws_summary.append(["Indicador", "Valor"])
    ws_summary.append(["Volumen proyectado", float(summary["qty"])])
    ws_summary.append(["Cobertura precio %", float(coverage["price_qty_pct"])])
    ws_summary.append(["Cobertura costo %", float(coverage["cost_qty_pct"])])
    ws_summary.append(["Productos con precio", int(coverage["products_with_price"])])
    ws_summary.append(["Productos con costo", int(coverage["products_with_cost"])])
    ws_summary.append(["Interpretación SKU", "Clasificación comercial central unificada"])
    if dataset["financial_trusted"]:
        ws_summary.append(["Venta proyectada", float(summary["sales"])])
        ws_summary.append(["Costo estimado", float(summary["cogs"])])
        ws_summary.append(["Utilidad bruta estimada", float(summary["profit"])])
        ws_summary.append(["Margen bruto estimado %", float(summary["margin"])])
        ws_summary.append(["Ingresos escenario foco", float(focused_financial.estimated_sales if focused_financial else summary["sales"])])
        ws_summary.append(["Utilidad bruta escenario foco", float(focused_financial.estimated_gross_profit if focused_financial else summary["profit"])])
        ws_summary.append(["ROI bruto esperado %", float(focused_financial.expected_roi if focused_financial else 0)])
    else:
        ws_summary.append(["Venta proyectada", "Pendiente validar"])
        ws_summary.append(["Costo estimado", "Pendiente validar"])
        ws_summary.append(["Utilidad bruta estimada", "Pendiente validar"])
        ws_summary.append(["Margen bruto estimado %", "Pendiente validar"])
        ws_summary.append(["Valorizacion parcial con costo resuelto", float(validated_summary["sales"])])
        ws_summary.append(["Costo parcial con costo resuelto", float(validated_summary["cogs"])])
        ws_summary.append(["Utilidad bruta parcial con costo resuelto", float(validated_summary["profit"])])

    ws_summary.append([])
    ws_summary.append(["Top sucursales", "Ingresos" if dataset["financial_trusted"] else "Volumen"])
    branch_start_row = ws_summary.max_row + 1
    for row in dataset["branch_rows"][:6]:
        ws_summary.append([row["branch_code"], float(row["sales"] if dataset["financial_trusted"] else row["qty"])])

    ws_summary.append([])
    ws_summary.append(["Mix por familia", "Ingresos" if dataset["financial_trusted"] else "Volumen"])
    family_start_row = ws_summary.max_row + 1
    for row in dataset["family_rows"][:6]:
        ws_summary.append([row["family"], float(row["sales"] if dataset["financial_trusted"] else row["qty"])])

    ws_daily.append(
        ["Fecha", "Volumen", "Ingresos", "Utilidad"]
        if dataset["financial_trusted"]
        else ["Fecha", "Volumen", "Valorizacion parcial", "Estado"]
    )
    for row in dataset["daily_rows"]:
        if dataset["financial_trusted"]:
            ws_daily.append([str(row["date"]), float(row["qty"]), float(row["sales"]), float(row["profit"])])
        else:
            ws_daily.append([str(row["date"]), float(row["qty"]), float(row["validated_sales"]), "Pendiente validar"])

    ws_branches.append(
        ["Sucursal", "Volumen", "Ingresos", "Costo", "Utilidad", "Margen %"]
        if dataset["financial_trusted"]
        else ["Sucursal", "Volumen", "Valorizacion parcial", "Cobertura costo", "Observacion"]
    )
    for row in dataset["branch_rows"]:
        if dataset["financial_trusted"]:
            ws_branches.append(
                [row["branch_code"], float(row["qty"]), float(row["sales"]), float(row["cogs"]), float(row["profit"]), float(row["margin"])]
            )
        else:
            ws_branches.append(
                [
                    row["branch_code"],
                    float(row["qty"]),
                    float(row["validated_sales"]),
                    f"{coverage['cost_qty_pct']}%",
                    "Valorizacion pendiente de costeo completo",
                ]
            )

    ws_products.append(
        ["Producto", "Familia", "Categoria", "Volumen", "Precio promedio", "Costo unitario", "Ingresos", "Utilidad", "Margen %"]
        if dataset["financial_trusted"]
        else [
            "Producto",
            "Familia",
            "Categoria",
            "Volumen",
            "Interpretacion SKU",
            "Estado costo",
            "Motivo bloqueo",
            "Precio resuelto",
            "Costo resuelto",
        ]
    )
    for row in dataset["product_rows"][:25]:
        if dataset["financial_trusted"]:
            ws_products.append(
                [
                    row["product_name"],
                    row["family"],
                    row["category"],
                    float(row["qty"]),
                    float(row["unit_price"]),
                    float(row["unit_cost"]),
                    float(row["sales"]),
                    float(row["profit"]),
                    float(row["margin"]),
                ]
            )
        else:
            ws_products.append(
                [
                    row["product_name"],
                    row["family"],
                    row["category"],
                    float(row["qty"]),
                    row["classification_label"],
                    row["cost_state"],
                    row["cost_reason"],
                    "SI" if row.get("has_price") else "NO",
                    "SI" if row.get("has_cost") else "NO",
                ]
            )

    ws_families.append(
        ["Familia", "Volumen", "Ingresos", "Costo", "Utilidad", "Margen %"]
        if dataset["financial_trusted"]
        else ["Familia", "Volumen", "Valorizacion parcial"]
    )
    for row in dataset["family_rows"]:
        if dataset["financial_trusted"]:
            ws_families.append(
                [row["family"], float(row["qty"]), float(row["sales"]), float(row["cogs"]), float(row["profit"]), float(row["margin"])]
            )
        else:
            ws_families.append([row["family"], float(row["qty"]), float(row["validated_sales"])])

    ws_validation.append(["Indicador", "Valor"])
    ws_validation.append(["Estado valorizacion", "Confiable" if dataset["financial_trusted"] else "Bloqueada"])
    ws_validation.append(["Cobertura precio volumen %", float(coverage["price_qty_pct"])])
    ws_validation.append(["Cobertura costo volumen %", float(coverage["cost_qty_pct"])])
    ws_validation.append(["Productos totales", int(coverage["products_total"])])
    ws_validation.append(["Productos sin precio", len(coverage["missing_price_products"])])
    ws_validation.append(["Productos sin costo", len(coverage["missing_cost_products"])])
    ws_validation.append([])
    ws_validation.append(["Producto", "Interpretacion SKU", "Estado costo", "Motivo", "Componentes/relacion"])
    for row in dataset["validation_rows"][:100]:
        ws_validation.append(
            [
                row["product_name"],
                row["classification_label"],
                row["cost_state"],
                row["cost_reason"],
                row["component_summary"] or row["classification_note"],
            ]
        )

    governance_counts: dict[str, int] = {}
    event_products = (
        EventoVentaProducto.objects.filter(sales_event=event)
        .select_related("product")
        .order_by("product__nombre", "product__codigo_point")
    )
    ws_governance.append(
        [
            "SKU actual",
            "Producto actual",
            "Clasificacion",
            "SKU base",
            "Producto base",
            "SKU historico",
            "Producto historico",
            "Nota negocio",
        ]
    )
    for link in event_products:
        interpretation = classify_commercial_recipe(link.product)
        governance_counts[interpretation.clasificacion] = governance_counts.get(interpretation.clasificacion, 0) + 1
        ws_governance.append(
            [
                interpretation.sku_actual,
                interpretation.producto_actual,
                interpretation.clasificacion,
                interpretation.sku_base,
                interpretation.producto_base,
                interpretation.sku_historico,
                interpretation.producto_historico,
                interpretation.nota_negocio,
            ]
        )
    ws_validation.append([])
    ws_validation.append(["Clasificacion SKU evento", "Cantidad"])
    for classification, qty in sorted(governance_counts.items()):
        ws_validation.append([classification, qty])

    ws_audit.append(["Modulo", "Semaforo", "Detalle"])
    dashboard_tabs = [
        "Dashboard",
        "Demanda diaria",
        "Sucursales",
        "Productos",
        "Familias",
        "Validacion",
        "Auditoria",
        "Gobernanza SKU",
    ]
    for row in _event_module_audit_rows(
        event,
        forecast_qs=forecast_qs,
        dataset=dataset,
        dashboard_sheetnames=dashboard_tabs,
    ):
        ws_audit.append([row["module"], row["status"], row["detail"]])

    _style_dashboard_summary_sheet(
        ws_summary,
        report_title="Pollyana's Dolce · Dashboard ejecutivo",
        subtitle=f"{event.name} · KPIs, ingresos y utilidad · {scope_label}",
        branch_start_row=branch_start_row,
        family_start_row=family_start_row,
        financial_trusted=dataset["financial_trusted"],
    )
    _style_dashboard_table_sheet(
        ws_daily,
        report_title="Pollyana's Dolce · Ventas diarias" if dataset["financial_trusted"] else "Pollyana's Dolce · Demanda diaria",
        subtitle=(
            f"{event.name} · Serie diaria · {scope_label}"
            if dataset["financial_trusted"]
            else f"{event.name} · Volumen proyectado · {scope_label}"
        ),
        max_column=4,
        money_columns=(3, 4) if dataset["financial_trusted"] else (),
        qty_columns=(2,),
        text_heavy_columns=(1,),
    )
    ws_daily.column_dimensions["A"].width = 16
    ws_daily.column_dimensions["B"].width = 14
    ws_daily.column_dimensions["C"].width = 16
    ws_daily.column_dimensions["D"].width = 16

    _style_dashboard_table_sheet(
        ws_branches,
        report_title="Pollyana's Dolce · Rentabilidad por sucursal" if dataset["financial_trusted"] else "Pollyana's Dolce · Sucursales",
        subtitle=(
            f"{event.name} · Sucursales · {scope_label}"
            if dataset["financial_trusted"]
            else f"{event.name} · Cobertura comercial por sucursal · {scope_label}"
        ),
        max_column=6 if dataset["financial_trusted"] else 5,
        money_columns=(3, 4, 5) if dataset["financial_trusted"] else (3,),
        qty_columns=(2,),
        percent_columns=(6,) if dataset["financial_trusted"] else (),
        text_heavy_columns=(1, 5),
    )
    ws_branches.column_dimensions["A"].width = 18
    for col in ("B", "C", "D", "E", "F"):
        ws_branches.column_dimensions[col].width = 16

    _style_dashboard_table_sheet(
        ws_products,
        report_title="Pollyana's Dolce · Rentabilidad por producto" if dataset["financial_trusted"] else "Pollyana's Dolce · Validacion por producto",
        subtitle=(
            f"{event.name} · Productos top · {scope_label}"
            if dataset["financial_trusted"]
            else f"{event.name} · Interpretacion comercial SKU y cobertura de costo · {scope_label}"
        ),
        max_column=9 if dataset["financial_trusted"] else 9,
        money_columns=(5, 6, 7, 8) if dataset["financial_trusted"] else (),
        qty_columns=(4,),
        percent_columns=(9,) if dataset["financial_trusted"] else (),
        text_heavy_columns=(1, 2, 3, 7),
    )
    ws_products.column_dimensions["A"].width = 34
    ws_products.column_dimensions["B"].width = 18
    ws_products.column_dimensions["C"].width = 22
    ws_products.column_dimensions["D"].width = 14
    ws_products.column_dimensions["E"].width = 18
    ws_products.column_dimensions["F"].width = 18
    ws_products.column_dimensions["G"].width = 34
    ws_products.column_dimensions["H"].width = 16
    ws_products.column_dimensions["I"].width = 16

    _style_dashboard_table_sheet(
        ws_families,
        report_title="Pollyana's Dolce · Mix por familia",
        subtitle=(
            f"{event.name} · Familias · {scope_label}"
            if dataset["financial_trusted"]
            else f"{event.name} · Volumen por familia · {scope_label}"
        ),
        max_column=6 if dataset["financial_trusted"] else 3,
        money_columns=(3, 4, 5) if dataset["financial_trusted"] else (3,),
        qty_columns=(2,),
        percent_columns=(6,) if dataset["financial_trusted"] else (),
        text_heavy_columns=(1,),
    )
    ws_families.column_dimensions["A"].width = 24
    for col in ("B", "C", "D", "E", "F"):
        ws_families.column_dimensions[col].width = 16

    validation_details_header_row = 9
    validation_classification_header_row = ws_validation.max_row - len(governance_counts)
    _style_dashboard_table_sheet(
        ws_validation,
        report_title="Pollyana's Dolce · Validacion financiera",
        subtitle=f"{event.name} · Cobertura precio/costo e interpretacion comercial SKU · {scope_label}",
        max_column=5,
        money_columns=(),
        extra_header_rows=(validation_details_header_row, validation_classification_header_row),
        text_heavy_columns=(1, 4, 5),
        enable_filter=False,
    )
    ws_validation.column_dimensions["A"].width = 28
    ws_validation.column_dimensions["B"].width = 24
    ws_validation.column_dimensions["C"].width = 18
    ws_validation.column_dimensions["D"].width = 42
    ws_validation.column_dimensions["E"].width = 54
    for row_idx in range(4, 10):
        label = str(ws_validation.cell(row_idx, 1).value or "").strip()
        value_cell = ws_validation.cell(row_idx, 2)
        if label in {"Cobertura precio volumen %", "Cobertura costo volumen %"}:
            value_cell.number_format = '0.00"%"'
        elif label in {"Productos totales", "Productos sin precio", "Productos sin costo"}:
            value_cell.number_format = '#,##0'

    _style_dashboard_table_sheet(
        ws_audit,
        report_title="Pollyana's Dolce · Auditoria operativa",
        subtitle=f"{event.name} · Semáforo rojo/amarillo/verde por módulo · {scope_label}",
        max_column=3,
        money_columns=(),
        text_heavy_columns=(1, 3),
        enable_filter=False,
    )
    ws_audit.column_dimensions["A"].width = 24
    ws_audit.column_dimensions["B"].width = 16
    ws_audit.column_dimensions["C"].width = 78

    _style_dashboard_table_sheet(
        ws_governance,
        report_title="Pollyana's Dolce · Gobernanza SKU",
        subtitle=f"{event.name} · Clasificacion comercial central · {scope_label}",
        max_column=8,
        money_columns=(),
        text_heavy_columns=(2, 5, 7, 8),
    )
    ws_governance.column_dimensions["A"].width = 16
    ws_governance.column_dimensions["B"].width = 30
    ws_governance.column_dimensions["C"].width = 22
    ws_governance.column_dimensions["D"].width = 16
    ws_governance.column_dimensions["E"].width = 30
    ws_governance.column_dimensions["F"].width = 16
    ws_governance.column_dimensions["G"].width = 30
    ws_governance.column_dimensions["H"].width = 48

    if dataset["daily_rows"]:
        revenue_chart = LineChart()
        revenue_chart.title = "Ingresos esperados por día" if dataset["financial_trusted"] else "Volumen proyectado por día"
        revenue_chart.style = 2
        revenue_chart.y_axis.title = "Ingresos" if dataset["financial_trusted"] else "Volumen"
        revenue_chart.x_axis.title = "Fecha"
        revenue_chart.legend = None
        revenue_data = Reference(
            ws_daily,
            min_col=3 if dataset["financial_trusted"] else 2,
            min_row=3,
            max_row=ws_daily.max_row,
        )
        revenue_categories = Reference(ws_daily, min_col=1, min_row=4, max_row=ws_daily.max_row)
        revenue_chart.add_data(revenue_data, titles_from_data=True)
        revenue_chart.set_categories(revenue_categories)
        revenue_chart.height = 8
        revenue_chart.width = 14.5
        ws_summary.add_chart(revenue_chart, "J4")

    branch_data_start_row = branch_start_row + 2
    branch_data_end_row = branch_data_start_row + min(6, len(dataset["branch_rows"])) - 1 if dataset["branch_rows"] else branch_data_start_row
    if dataset["branch_rows"]:
        branch_chart = BarChart()
        branch_chart.type = "bar"
        branch_chart.style = 10
        branch_chart.title = "Top sucursales por ingresos" if dataset["financial_trusted"] else "Top sucursales por volumen"
        branch_chart.y_axis.title = "Sucursal"
        branch_chart.x_axis.title = "Ingresos" if dataset["financial_trusted"] else "Volumen"
        branch_chart.legend = None
        branch_data = Reference(
            ws_summary,
            min_col=2,
            min_row=branch_data_start_row,
            max_row=branch_data_end_row,
        )
        branch_categories = Reference(
            ws_summary,
            min_col=1,
            min_row=branch_data_start_row,
            max_row=branch_data_end_row,
        )
        branch_chart.add_data(branch_data, titles_from_data=False)
        branch_chart.set_categories(branch_categories)
        branch_chart.height = 7
        branch_chart.width = 12.5
        ws_summary.add_chart(branch_chart, "J24")

    family_data_start_row = family_start_row + 2
    family_data_end_row = family_data_start_row + min(6, len(dataset["family_rows"])) - 1 if dataset["family_rows"] else family_data_start_row
    if dataset["family_rows"]:
        family_chart = PieChart()
        family_chart.title = "Mix comercial por familia" if dataset["financial_trusted"] else "Mix de volumen por familia"
        family_data = Reference(
            ws_summary,
            min_col=2,
            min_row=family_data_start_row,
            max_row=family_data_end_row,
        )
        family_labels = Reference(
            ws_summary,
            min_col=1,
            min_row=family_data_start_row,
            max_row=family_data_end_row,
        )
        family_chart.add_data(family_data, titles_from_data=False)
        family_chart.set_categories(family_labels)
        family_chart.height = 8
        family_chart.width = 10.5
        family_chart.dataLabels = DataLabelList()
        family_chart.dataLabels.showPercent = True
        family_chart.legend.position = "r"
        ws_summary.add_chart(family_chart, "S4")

    workbook.active = 0

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"ventas_evento_{event.code.lower()}_dashboard_{week_start}_{week_end}.xlsx"
    return filename, output.getvalue()


def _build_projection_workbook_response(event: EventoVenta, *, exact_day: bool) -> HttpResponse:
    filename, content = _build_projection_workbook_file(event, exact_day=exact_day)
    response = HttpResponse(
        content,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def _build_projection_workbook_file(event: EventoVenta, *, exact_day: bool) -> tuple[str, bytes]:
    forecast_qs = _event_forecast_qs(event)
    scope_start, scope_end = _event_projection_window(event) if not exact_day else (event.main_date, event.main_date)
    scope_days: list[date] = []
    current_day = scope_start
    while current_day <= scope_end:
        scope_days.append(current_day)
        current_day += timedelta(days=1)
    scoped_date = event.main_date if exact_day else None
    if exact_day:
        scoped_qs = forecast_qs.filter(forecast_date=event.main_date)
        scope_label = str(event.main_date)
    else:
        scoped_qs = _week_scope_qs(event, forecast_qs)
        scope_label = _week_scope_label(event)
    general_rows = _product_projection_rows(forecast_qs, forecast_date=scoped_date)
    if exact_day:
        branch_rows = _branch_product_projection_rows(forecast_qs, forecast_date=scoped_date)
    else:
        branch_rows = _branch_product_projection_rows(scoped_qs)
        general_rows = _product_projection_rows(scoped_qs)

    workbook = Workbook()
    ws_summary = workbook.active
    ws_summary.title = "Resumen"
    ws_general = workbook.create_sheet("General", 1)
    ws_general.append(["Evento", event.name])
    ws_general.append(["Codigo", event.code])
    ws_general.append(["Fecha principal", str(event.main_date)])
    ws_general.append(["Alcance", scope_label])
    ws_general.append([])
    _append_projection_sheet(ws_general, general_rows, scope_label=scope_label)
    branch_map: dict[str, list[dict]] = {}
    for row in branch_rows:
        branch_map.setdefault(row["branch__codigo"], []).append(row)

    branch_codes = [link.branch.codigo for link in _event_branch_links(event)]
    ws_summary.append(["Evento", event.name])
    ws_summary.append(["Codigo", event.code])
    ws_summary.append(["Fecha principal", str(event.main_date)])
    ws_summary.append(["Alcance", scope_label])
    ws_summary.append([])
    _append_projection_summary_sheet(
        ws_summary,
        general_rows,
        branch_rows,
        branch_codes=sorted(set(branch_codes) | set(branch_map.keys())),
        scope_label=scope_label,
    )
    _style_projection_sheet(
        ws_summary,
        report_title="Pollyana's Dolce · Resumen ejecutivo",
        subtitle=f"{event.name} · Resumen por sucursal · {scope_label}",
        max_column=4 + len(sorted(set(branch_codes) | set(branch_map.keys()))),
        data_start_row=6,
    )
    _style_projection_sheet(
        ws_general,
        report_title="Pollyana's Dolce · Proyeccion comercial",
        subtitle=f"{event.name} · General · {scope_label}",
        max_column=14,
        data_start_row=6,
    )
    workbook.active = 0

    for branch_code in sorted(set(branch_codes) | set(branch_map.keys())):
        ws = workbook.create_sheet(_safe_sheet_title(branch_code))
        ws.append(["Evento", event.name])
        ws.append(["Sucursal", branch_code])
        ws.append(["Alcance", scope_label])
        ws.append([])
        _append_projection_sheet(ws, branch_map.get(branch_code, []), scope_label=scope_label)
        _style_projection_sheet(
            ws,
            report_title="Pollyana's Dolce · Proyeccion comercial",
            subtitle=f"{event.name} · {branch_code} · {scope_label}",
            max_column=14,
            data_start_row=5,
        )

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    timestamp = timezone.localtime().strftime("%Y%m%d_%H%M")
    scope_token = "dia" if exact_day else "semana"
    filename = f"ventas_evento_{event.code.lower()}_{scope_token}_{timestamp}.xlsx"
    return filename, output.getvalue()


def _projection_artifact_dir(event: EventoVenta) -> Path:
    safe_code = _ascii_norm(event.code or "") or (event.code or "").strip().lower()
    return Path(settings.BASE_DIR) / "output" / "spreadsheet" / "ventas_eventos" / safe_code / "actual"


def _latest_projection_artifacts(event: EventoVenta):
    export_order = (
        EventoVentaProjectionArtifact.TYPE_WEEK,
        EventoVentaProjectionArtifact.TYPE_DAY,
        EventoVentaProjectionArtifact.TYPE_DAILY,
        EventoVentaProjectionArtifact.TYPE_DASHBOARD,
        EventoVentaProjectionArtifact.TYPE_PACKAGE,
    )
    export_position = {export_type: index for index, export_type in enumerate(export_order)}
    latest_by_type: dict[str, EventoVentaProjectionArtifact] = {}
    for artifact in event.projection_artifacts.all().order_by("export_type", "-forecast_version", "-created_at", "-id"):
        if artifact.export_type not in latest_by_type:
            latest_by_type[artifact.export_type] = artifact
    return sorted(
        latest_by_type.values(),
        key=lambda artifact: export_position.get(artifact.export_type, 99),
    )


def _projection_artifact_variants(event: EventoVenta) -> set[str]:
    variants = {
        _ascii_norm(event.code or "") or "",
        (event.code or "").strip().lower(),
    }
    for file_path in event.projection_artifacts.exclude(file_path="").values_list("file_path", flat=True):
        candidate = Path(str(file_path))
        if "ventas_eventos" not in candidate.parts:
            continue
        anchor = candidate.parts.index("ventas_eventos")
        if len(candidate.parts) > anchor + 1:
            variants.add(candidate.parts[anchor + 1])
    return {variant for variant in variants if variant}


def _archive_projection_dir(directory: Path) -> Path:
    archive_root = (
        Path(settings.BASE_DIR)
        / "output"
        / "spreadsheet"
        / "ventas_eventos"
        / "_historico_no_vigente"
        / timezone.localdate().isoformat()
    )
    archive_root.mkdir(parents=True, exist_ok=True)
    archive_target = archive_root / directory.name
    if archive_target.exists():
        shutil.rmtree(archive_target)
    shutil.move(str(directory), str(archive_target))
    return archive_target


def _cleanup_projection_artifact_history(event: EventoVenta) -> dict[str, object]:
    keep_version = event.version
    obsolete_rows = list(
        event.projection_artifacts.exclude(forecast_version=keep_version).values_list("id", flat=True)
    )
    if obsolete_rows:
        event.projection_artifacts.filter(id__in=obsolete_rows).delete()

    base_dir = Path(settings.BASE_DIR) / "output" / "spreadsheet" / "ventas_eventos"
    keep_parent = _projection_artifact_dir(event).parent
    archived_dirs: list[str] = []
    for variant in sorted(_projection_artifact_variants(event)):
        candidate = base_dir / variant
        if not candidate.exists() or candidate == keep_parent:
            continue
        archived_dirs.append(str(_archive_projection_dir(candidate)))
    return {
        "deleted_rows": len(obsolete_rows),
        "archived_dirs": archived_dirs,
    }


def _avg_price_for_recipe(receta_id: int, start_date: date, end_date: date) -> Decimal:
    return resolve_unit_price(receta_id, start_date, end_date)


def _projected_sales_amount(forecast_qs, start_date: date, end_date: date) -> Decimal:
    totals = Decimal("0")
    scoped_rows = list(
        forecast_qs.filter(forecast_date__range=(start_date, end_date)).only(
            "product_id",
            "branch_id",
            "final_forecast",
        )
    )
    if not scoped_rows:
        return totals.quantize(Decimal("0.01"))
    product_ids = {row.product_id for row in scoped_rows}
    branch_ids = {row.branch_id for row in scoped_rows}
    commercial_context = build_commercial_recipe_lookup_context(product_ids)
    price_cache = resolve_unit_prices_bulk(
        product_ids,
        start_date,
        end_date,
        branch_ids=branch_ids,
        commercial_context=commercial_context,
    )
    for row in scoped_rows:
        avg_price = price_cache.get((row.product_id, row.branch_id), Decimal("0"))
        totals += avg_price * Decimal(str(row.final_forecast or 0))
    return totals.quantize(Decimal("0.01"))


def _focused_financial_row(event: EventoVenta):
    focused = event.financials.filter(scenario=event.scenario_focus).first()
    if focused:
        return focused
    build_financials(event)
    return event.financials.filter(scenario=event.scenario_focus).first()


def _input_investment_amount(event: EventoVenta) -> Decimal:
    if not event.input_requirements.exists():
        build_input_requirements(event)
    total_cost = Decimal("0")
    for row in event.input_requirements.all().values("net_shortage_qty", "unit_cost_estimate"):
        total_cost += Decimal(str(row.get("net_shortage_qty") or 0)) * Decimal(str(row.get("unit_cost_estimate") or 0))
    focused = _focused_financial_row(event)
    scenario_cost = Decimal(str(focused.incremental_investment or 0)).quantize(Decimal("0.01")) if focused else Decimal("0.00")
    if total_cost <= 0:
        return scenario_cost
    if scenario_cost > 0:
        # Guard against unit-conversion or master-data contamination in input requirements.
        if total_cost > (scenario_cost * Decimal("10")) or total_cost < (scenario_cost * Decimal("0.10")):
            return scenario_cost
    return total_cost.quantize(Decimal("0.01"))


def _refresh_event_support_outputs(event: EventoVenta) -> None:
    generate_production_plan(event, promote_status=False)
    build_input_requirements(event)
    build_financials(event)
    _refresh_event_detail_snapshot(event)
    _sync_event_review_status_with_guardrails(event)


def _refresh_event_detail_snapshot(event: EventoVenta, generated_by=None) -> None:
    from ventas.services.event_detail_snapshot import refresh_event_detail_snapshot

    refresh_event_detail_snapshot(event, generated_by=generated_by)


def _reprocess_event_for_audit(
    event: EventoVenta,
    generated_by=None,
    *,
    skip_purchases: bool = False,
    skip_postmortem: bool = False,
) -> dict:
    original_status = event.status
    forecast_result = {}
    production_result = {}
    input_result = {}
    purchase_result = {"created": 0, "warnings": ["Reproceso de compras omitido."]} if skip_purchases else {}
    financial_result = {}
    postmortem_result = {"created": 0, "warnings": ["Postmortem omitido en reproceso."]} if skip_postmortem else {}
    artifacts = []
    try:
        forecast_result = generate_event_forecast(event, generated_by)
        event.refresh_from_db()
        production_result = generate_production_plan(event, promote_status=False)
        input_result = build_input_requirements(event)
        if not skip_purchases:
            purchase_result = build_purchase_requirements(event)
        financial_result = build_financials(event)
        if not skip_postmortem:
            postmortem_result = build_postmortem(event)
        event.refresh_from_db()
        _refresh_event_detail_snapshot(event, generated_by=generated_by)
        event.refresh_from_db()
        artifacts = _persist_projection_artifacts(event, generated_by, force=True)
        return {
            "forecast": forecast_result,
            "production": production_result,
            "inputs": input_result,
            "purchases": purchase_result,
            "financials": financial_result,
            "postmortem": postmortem_result,
            "artifacts": artifacts,
        }
    finally:
        event.refresh_from_db()
        if event.status != original_status:
            event.status = original_status
            event.save(update_fields=["status", "updated_at"])


def _decode_uploaded_rows(uploaded_file) -> list[dict]:
    file_name = (uploaded_file.name or "").lower()
    raw = uploaded_file.read()
    if file_name.endswith(".csv"):
        text = raw.decode("utf-8-sig")
        return list(csv.DictReader(text.splitlines()))
    if file_name.endswith(".xlsx"):
        wb = load_workbook(filename=BytesIO(raw), data_only=True)
        ws = wb.active
        header_row_idx = 1
        headers: list[str] = []
        for row_idx in range(1, min(ws.max_row, 15) + 1):
            candidate = [str(cell.value or "").strip() for cell in ws[row_idx]]
            normalized = {_ascii_norm(value) for value in candidate if value}
            if "producto" in normalized and (
                "proyeccion general" in normalized or "proyeccion" in normalized or "nueva proyeccion" in normalized
            ):
                header_row_idx = row_idx
                headers = candidate
                break
        if not headers:
            headers = [str(cell.value or "").strip() for cell in ws[1]]
        rows: list[dict] = []
        for values in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
            if not any(value not in (None, "") for value in values):
                continue
            rows.append({headers[idx]: values[idx] for idx in range(len(headers))})
        return rows
    raise ValueError("Formato no soportado. Sube un archivo CSV o XLSX.")


def _resolve_product_for_adjustment(product_label: str):
    normalized = _ascii_norm(product_label)
    if not normalized:
        return None
    for recipe in Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL):
        if _ascii_norm(recipe.nombre or "") == normalized:
            return recipe
    for recipe in Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL, nombre__icontains=product_label.strip()[:30]):
        if normalized in _ascii_norm(recipe.nombre or ""):
            return recipe
    return None


def _resolve_branch_for_adjustment(branch_label: str):
    normalized = _ascii_norm(branch_label)
    if not normalized:
        return None
    for branch in eligible_sales_event_branch_qs():
        if normalized in {_ascii_norm(branch.codigo or ""), _ascii_norm(branch.nombre or "")}:
            return branch
    return None


def _apply_adjustment_to_queryset(*, event: EventoVenta, rows_qs, target_qty: Decimal, actor, reason: str, branch=None, product=None):
    rows = list(rows_qs.order_by("forecast_date", "branch__codigo", "product__nombre"))
    if not rows:
        return False
    current_total = sum(Decimal(str(row.final_forecast or 0)) for row in rows)
    if target_qty < 0:
        target_qty = Decimal("0")

    if current_total > 0:
        scale = target_qty / current_total if current_total else Decimal("0")
        assigned = Decimal("0")
        for index, row in enumerate(rows, start=1):
            original_final = Decimal(str(row.final_forecast or 0))
            original_conservative = Decimal(str(row.conservative_forecast or 0))
            original_aggressive = Decimal(str(row.aggressive_forecast or 0))
            if index == len(rows):
                new_final = max(Decimal("0"), target_qty - assigned)
            else:
                new_final = (original_final * scale).quantize(Decimal("0.001"))
                assigned += new_final
            ratio = (new_final / original_final) if original_final > 0 else Decimal("0")
            row.final_forecast = new_final
            row.conservative_forecast = (original_conservative * ratio).quantize(Decimal("0.001")) if original_conservative > 0 else new_final
            row.aggressive_forecast = (original_aggressive * ratio).quantize(Decimal("0.001")) if original_aggressive > 0 else new_final
            row.explanation_json = {
                **(row.explanation_json or {}),
                "manual_adjustment_applied": True,
                "manual_adjustment_reason": reason,
            }
            row.save(update_fields=["final_forecast", "conservative_forecast", "aggressive_forecast", "explanation_json"])
    else:
        split_value = (target_qty / Decimal(len(rows))).quantize(Decimal("0.001")) if rows else Decimal("0")
        assigned = Decimal("0")
        for index, row in enumerate(rows, start=1):
            if index == len(rows):
                new_final = max(Decimal("0"), target_qty - assigned)
            else:
                new_final = split_value
                assigned += split_value
            row.final_forecast = new_final
            row.conservative_forecast = new_final
            row.aggressive_forecast = new_final
            row.explanation_json = {
                **(row.explanation_json or {}),
                "manual_adjustment_applied": True,
                "manual_adjustment_reason": reason,
            }
            row.save(update_fields=["final_forecast", "conservative_forecast", "aggressive_forecast", "explanation_json"])

    EventoVentaAdjustment.objects.create(
        sales_event=event,
        branch=branch,
        product=product,
        field_name="final_forecast",
        old_value=str(current_total),
        new_value=str(target_qty),
        adjustment_reason=reason or "Carga masiva de ventas",
        adjusted_by=actor,
    )
    return True


def _apply_adjustment_rows(event: EventoVenta, rows: list[dict], actor, *, scope_mode: str) -> tuple[int, list[str]]:
    warnings: list[str] = []
    applied = 0
    week_start, week_end, _scope_label = _adjustment_scope_window(event, scope_mode)
    branch_lookup = {
        _ascii_norm(branch.codigo or ""): branch
        for branch in eligible_sales_event_branch_qs()
    }
    branch_lookup.update({
        _ascii_norm(branch.nombre or ""): branch
        for branch in eligible_sales_event_branch_qs()
    })
    base_qs = _event_forecast_qs(event)

    for row in rows:
        product_label = _pick_row_value(row, "producto", "product", "sku", "nombre")
        if not product_label:
            continue
        product = _resolve_product_for_adjustment(str(product_label))
        if not product:
            warnings.append(f"No se encontro producto para '{product_label}'.")
            continue

        explicit_date = _pick_row_value(row, "fecha", "forecast_date")
        scope_value = _ascii_norm(str(_pick_row_value(row, "alcance", "scope") or scope_mode))
        if explicit_date:
            scope_qs = base_qs.filter(product=product, forecast_date=date.fromisoformat(str(explicit_date)))
        elif scope_value in {"dia", "diaexacto", "diaexacto30", "especial"}:
            scope_qs = base_qs.filter(product=product, forecast_date=event.main_date)
        else:
            scope_qs = base_qs.filter(product=product, forecast_date__range=(week_start, week_end))

        branch_value_columns = []
        for key, value in row.items():
            normalized_key = _ascii_norm(str(key))
            branch = branch_lookup.get(normalized_key)
            if branch and value not in (None, ""):
                branch_value_columns.append((branch, _decimal_or_zero(value)))

        reason = str(_pick_row_value(row, "motivo", "razon", "reason") or "Carga masiva ventas").strip()
        if branch_value_columns:
            for branch, target in branch_value_columns:
                if _apply_adjustment_to_queryset(
                    event=event,
                    rows_qs=scope_qs.filter(branch=branch),
                    target_qty=target,
                    actor=actor,
                    reason=reason,
                    branch=branch,
                    product=product,
                ):
                    applied += 1
            continue

        branch_label = _pick_row_value(row, "sucursal", "branch")
        target_qty = _pick_row_value(row, "nueva_proyeccion", "proyeccion", "proyeccion general", "forecast", "cantidad")
        if target_qty in (None, ""):
            warnings.append(f"No se encontro proyeccion para '{product.nombre}'.")
            continue
        if branch_label not in (None, ""):
            branch = _resolve_branch_for_adjustment(str(branch_label))
            if not branch:
                warnings.append(f"No se encontro sucursal para '{branch_label}'.")
                continue
            qs = scope_qs.filter(branch=branch)
        else:
            branch = None
            qs = scope_qs

        if _apply_adjustment_to_queryset(
            event=event,
            rows_qs=qs,
            target_qty=_decimal_or_zero(target_qty),
            actor=actor,
            reason=reason,
            branch=branch,
            product=product,
        ):
            applied += 1

    return applied, warnings


def _upsert_projection_artifact(
    *,
    event: EventoVenta,
    export_type: str,
    generated_by,
    file_name: str,
    file_path: Path,
    scope_start: date | None,
    scope_end: date | None,
) -> EventoVentaProjectionArtifact:
    return EventoVentaProjectionArtifact.objects.update_or_create(
        sales_event=event,
        export_type=export_type,
        forecast_version=event.version,
        defaults={
            "generated_by": generated_by,
            "file_name": file_name,
            "file_path": str(file_path),
            "size_bytes": file_path.stat().st_size if file_path.exists() else 0,
            "scope_start": scope_start,
            "scope_end": scope_end,
        },
    )[0]


def _persist_projection_artifacts(event: EventoVenta, generated_by, *, force: bool = False) -> list[EventoVentaProjectionArtifact]:
    artifact_dir = _projection_artifact_dir(event)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    week_start, week_end = _event_projection_window(event)
    safe_code = _ascii_norm(event.code or "") or (event.code or "").strip().lower()
    file_prefix = f"{safe_code}_"
    current_artifacts = list(
        event.projection_artifacts.filter(forecast_version=event.version).order_by("export_type", "-id")
    )
    if not force and len(current_artifacts) == 5 and all(Path(artifact.file_path).exists() for artifact in current_artifacts):
        _cleanup_projection_artifact_history(event)
        return current_artifacts

    for stale_file in artifact_dir.glob(f"{file_prefix}*"):
        if stale_file.is_file():
            stale_file.unlink()

    week_name, week_bytes = _build_projection_workbook_file(event, exact_day=False)
    day_name, day_bytes = _build_projection_workbook_file(event, exact_day=True)
    daily_name, daily_bytes = _build_branch_day_workbook_file(event)
    dashboard_name, dashboard_bytes = _build_executive_dashboard_workbook_file(event)

    safe_week_name = f"{safe_code}_proyeccion_semana_{week_start}_{week_end}.xlsx"
    safe_day_name = f"{safe_code}_proyeccion_dia_{event.main_date}.xlsx"
    safe_daily_name = f"{safe_code}_proyeccion_por_dia_{week_start}_{week_end}.xlsx"
    safe_dashboard_name = f"{safe_code}_dashboard_ejecutivo_{week_start}_{week_end}.xlsx"
    week_path = artifact_dir / safe_week_name
    day_path = artifact_dir / safe_day_name
    daily_path = artifact_dir / safe_daily_name
    dashboard_path = artifact_dir / safe_dashboard_name
    week_path.write_bytes(week_bytes)
    day_path.write_bytes(day_bytes)
    daily_path.write_bytes(daily_bytes)
    dashboard_path.write_bytes(dashboard_bytes)

    package_name = f"{safe_code}_paquete_proyeccion.zip"
    package_path = artifact_dir / package_name
    with ZipFile(package_path, "w", compression=ZIP_DEFLATED) as zf:
        zf.write(week_path, arcname=safe_week_name)
        zf.write(day_path, arcname=safe_day_name)
        zf.write(daily_path, arcname=safe_daily_name)
        zf.write(dashboard_path, arcname=safe_dashboard_name)

    artifacts = [
        _upsert_projection_artifact(
            event=event,
            export_type=EventoVentaProjectionArtifact.TYPE_WEEK,
            generated_by=generated_by,
            file_name=safe_week_name,
            file_path=week_path,
            scope_start=week_start,
            scope_end=week_end,
        ),
        _upsert_projection_artifact(
            event=event,
            export_type=EventoVentaProjectionArtifact.TYPE_DAY,
            generated_by=generated_by,
            file_name=safe_day_name,
            file_path=day_path,
            scope_start=event.main_date,
            scope_end=event.main_date,
        ),
        _upsert_projection_artifact(
            event=event,
            export_type=EventoVentaProjectionArtifact.TYPE_DAILY,
            generated_by=generated_by,
            file_name=safe_daily_name,
            file_path=daily_path,
            scope_start=week_start,
            scope_end=week_end,
        ),
        _upsert_projection_artifact(
            event=event,
            export_type=EventoVentaProjectionArtifact.TYPE_DASHBOARD,
            generated_by=generated_by,
            file_name=safe_dashboard_name,
            file_path=dashboard_path,
            scope_start=week_start,
            scope_end=week_end,
        ),
        _upsert_projection_artifact(
            event=event,
            export_type=EventoVentaProjectionArtifact.TYPE_PACKAGE,
            generated_by=generated_by,
            file_name=package_name,
            file_path=package_path,
            scope_start=week_start,
            scope_end=week_end,
        ),
    ]
    _cleanup_projection_artifact_history(event)
    return artifacts


def _ensure_projection_artifact_file(event: EventoVenta, artifact: EventoVentaProjectionArtifact, generated_by):
    file_path = Path(artifact.file_path)
    if artifact.forecast_version == event.version and file_path.exists():
        return artifact, file_path

    artifacts = _persist_projection_artifacts(event, generated_by, force=True)
    for candidate in artifacts:
        if candidate.export_type == artifact.export_type:
            refreshed_path = Path(candidate.file_path)
            if refreshed_path.exists():
                return candidate, refreshed_path
    return artifact, file_path


def _run_event_projection_pipeline(event: EventoVenta, generated_by) -> tuple[dict, list[EventoVentaProjectionArtifact]]:
    result = generate_event_forecast(event, generated_by)
    artifacts: list[EventoVentaProjectionArtifact] = []
    if result.get("created"):
        event.refresh_from_db()
        artifacts = _persist_projection_artifacts(event, generated_by, force=True)
        _refresh_event_support_outputs(event)
        _refresh_event_detail_snapshot(event, generated_by=generated_by)
    return result, artifacts


def _celery_broker_ready(timeout: float = 0.25) -> bool:
    broker_url = getattr(settings, "CELERY_BROKER_URL", "") or ""
    if not broker_url:
        return False
    parsed = urlparse(broker_url)
    host = parsed.hostname
    if not host:
        return False
    port = parsed.port
    if port is None:
        if parsed.scheme.startswith("redis"):
            port = 6379
        elif parsed.scheme.startswith("amqp"):
            port = 5672
        else:
            return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _dispatch_event_projection_pipeline(event: EventoVenta) -> bool:
    if not getattr(settings, "EVENT_PIPELINE_ASYNC_ENABLED", False):
        return False
    if not _celery_broker_ready():
        return False
    event.status = EventoVenta.STATUS_MODELADO
    event.save(update_fields=["status", "updated_at"])
    try:
        run_event_projection_pipeline_task.delay(event.id)
        return True
    except Exception:
        return False


@login_required
def evento_list(request):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para ver eventos comerciales.")

    status = (request.GET.get("status") or "").strip().upper()
    event_type = (request.GET.get("event_type") or "").strip()
    q = (request.GET.get("q") or "").strip()

    qs = EventoVenta.objects.all()
    if status:
        qs = qs.filter(status=status)
    if event_type:
        qs = qs.filter(event_type__icontains=event_type)
    if q:
        qs = qs.filter(name__icontains=q)

    context = {
        "events": qs[:200],
        "status_choices": EventoVenta.STATUS_CHOICES,
        "status_filter": status,
        "event_type_filter": event_type,
        "query": q,
        "can_manage": _can_manage_events(request.user),
    }
    return render(request, "ventas/eventos_list.html", context)


@login_required
def evento_create(request):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para crear eventos comerciales.")

    if request.method == "POST":
        name = (request.POST.get("name") or "").strip()
        if not name:
            messages.error(request, "El nombre del evento es requerido.")
            return redirect("ventas:evento_create")

        main_date = request.POST.get("main_date") or timezone.localdate().isoformat()
        parsed_main_date = date.fromisoformat(main_date)
        default_start, default_end = _projection_week_window(parsed_main_date)
        analysis_start_date = request.POST.get("analysis_start_date") or default_start.isoformat()
        analysis_end_date = request.POST.get("analysis_end_date") or default_end.isoformat()

        with transaction.atomic():
            event = EventoVenta.objects.create(
                name=name,
                event_type=(request.POST.get("event_type") or "").strip(),
                main_date=parsed_main_date,
                analysis_start_date=date.fromisoformat(str(analysis_start_date)),
                analysis_end_date=date.fromisoformat(str(analysis_end_date)),
                objective_type=(request.POST.get("objective_type") or "").strip(),
                objective_notes=(request.POST.get("objective_notes") or "").strip(),
                approval_deadline=request.POST.get("approval_deadline") or None,
                priority=(request.POST.get("priority") or EventoVenta.PRIORIDAD_MEDIA),
                scenario_focus=(request.POST.get("scenario_focus") or EventoVenta.SCENARIO_BASE),
                conservative_pct=Decimal(request.POST.get("conservative_pct") or "0.90"),
                aggressive_pct=Decimal(request.POST.get("aggressive_pct") or "1.10"),
                created_by=request.user,
            )

            branch_ids = set(request.POST.getlist("branches"))
            product_ids = request.POST.getlist("products")
            branches = eligible_sales_event_branch_qs().filter(id__in=branch_ids)
            selected_products = list(Receta.objects.filter(id__in=product_ids))
            products, excluded_products = _filter_executive_event_products(selected_products)

            for branch in branches:
                link = EventoVentaSucursal.objects.create(sales_event=event, branch=branch)
                comparable_branch_id = request.POST.get(f"comparable_branch_{branch.id}") or None
                if comparable_branch_id:
                    link.comparable_branch_id = comparable_branch_id
                    link.save(update_fields=["comparable_branch"])
            for product in products:
                EventoVentaProducto.objects.create(sales_event=event, product=product)

            log_evento_change(event, "EventoVenta", event.id, "CREATE", new_data={"name": event.name}, actor=request.user)

        if _dispatch_event_projection_pipeline(event):
            messages.success(
                request,
                f"Evento {event.code} creado. Recalculo de forecast y archivos ERP enviado a segundo plano.",
            )
        else:
            result, artifacts = _run_event_projection_pipeline(event, request.user)
            if result.get("created"):
                messages.success(
                    request,
                    f"Evento {event.code} creado. Forecast y archivos ERP generados ({len(artifacts)} archivos).",
                )
            else:
                messages.success(request, f"Evento {event.code} creado.")
                if result.get("warnings"):
                    messages.warning(request, result["warnings"][0])
        if excluded_products:
            excluded_names = ", ".join(product.nombre for product, _reason in excluded_products[:6])
            if len(excluded_products) > 6:
                excluded_names += ", ..."
            messages.warning(
                request,
                "Se omitieron productos fuera del scope ejecutivo "
                f"(accesorios, bebidas o servicios): {excluded_names}.",
            )
        return redirect("ventas:evento_detail", event_id=event.id)

    context = {
        "event": None,
        "branches": eligible_sales_event_branch_qs(),
        "product_groups": _product_selection_groups(),
        "status_choices": EventoVenta.STATUS_CHOICES,
        "priority_choices": EventoVenta.PRIORIDAD_CHOICES,
        "selected_branch_ids": _mandatory_branch_ids(),
        "mandatory_branch_ids": _mandatory_branch_ids(),
        "branch_comparable_controls": _branch_comparable_controls(
            selected_branch_ids=_mandatory_branch_ids(),
            anchor=timezone.localdate(),
        ),
        "selected_product_ids": set(),
    }
    return render(request, "ventas/eventos_form.html", context)


@login_required
def evento_detail(request, event_id: int):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para ver eventos comerciales.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    from ventas.services.event_detail_snapshot import get_event_detail_snapshot_payload

    snapshot_payload = get_event_detail_snapshot_payload(event, generated_by=request.user)
    branches = _event_branch_links(event)
    active_adjustment_draft = _active_adjustment_draft(event)
    latest_finalized_draft = (
        event.adjustment_drafts.filter(status=EventoVentaAdjustmentDraft.STATUS_FINALIZED)
        .order_by("-finalized_at", "-updated_at")
        .first()
    )
    traceability_dataset = _approval_traceability_dataset(event)
    workflow_state = _event_forecast_version_label(
        event,
        active_adjustment_draft=active_adjustment_draft,
        latest_finalized_draft=latest_finalized_draft,
    )
    detail_source = _event_detail_source_filter(request.GET.get("detail_source"))
    week_product_projection = list(snapshot_payload.get("week_product_projection", []))
    if detail_source:
        week_product_projection = _filter_week_product_projection(week_product_projection, detail_source[0])
    context = {
        "event": event,
        "branches": branches,
        "considered_product_count": snapshot_payload.get("considered_product_count", 0),
        "week_total_qty": snapshot_payload.get("week_total_qty", 0),
        "main_day_total_qty": snapshot_payload.get("main_day_total_qty", 0),
        "week_projected_revenue": snapshot_payload.get("week_projected_revenue"),
        "main_day_projected_revenue": snapshot_payload.get("main_day_projected_revenue"),
        "input_investment_required": snapshot_payload.get("input_investment_required", 0),
        "week_branch_breakdown": snapshot_payload.get("week_branch_breakdown", []),
        "week_product_projection": week_product_projection,
        "week_scope_label": snapshot_payload.get("week_scope_label") or _week_scope_label(event),
        "week_trend_note": snapshot_payload.get("week_trend_note", ""),
        "projection_artifacts": _latest_projection_artifacts(event),
        "adjustments": event.adjustments.select_related("product", "branch", "adjusted_by")[:20],
        "active_adjustment_draft": active_adjustment_draft,
        "latest_finalized_draft": latest_finalized_draft,
        "workflow_state": workflow_state,
        "executive_dataset": snapshot_payload.get("executive_dataset", {}),
        "focused_financial": snapshot_payload.get("focused_financial"),
        "production_dataset": snapshot_payload.get("production_dataset", {}),
        "purchase_summary": snapshot_payload.get("purchase_summary", {}),
        "traceability_dataset": traceability_dataset,
        "event_ready_for_operations": _event_ready_for_operations(event),
        "can_manage": _can_manage_events(request.user),
        "can_approve": _can_approve_events(request.user),
        "can_manage_capacity": _can_manage_capacity(request.user),
        "can_view_production_dashboard": _can_view_event_production_dashboard(request.user),
        "detail_source_filter": detail_source[0] if detail_source else "",
        "detail_source_filter_label": detail_source[1] if detail_source else "",
    }
    return render(request, "ventas/eventos_detail.html", context)


class EventoProduccionView(LoginRequiredMixin, View):
    template_name = "ventas/evento_produccion.html"

    def get(self, request, event_id: int):
        if not _can_view_event_production_dashboard(request.user):
            raise PermissionDenied("No tienes permisos para ver el dashboard de produccion del evento.")
        event = get_object_or_404(EventoVenta, pk=event_id)
        dashboard = _build_event_production_dashboard(event, familia=(request.GET.get("familia") or "").strip())
        if request.GET.get("formato") == "csv":
            return _event_production_csv_response(event, dashboard)
        return render(request, self.template_name, dashboard)


@login_required
def evento_export_week_projection(request, event_id: int):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para exportar la proyeccion comercial.")
    event = get_object_or_404(EventoVenta, pk=event_id)
    return _build_projection_workbook_response(event, exact_day=False)


@login_required
def evento_export_main_day_projection(request, event_id: int):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para exportar la proyeccion comercial.")
    event = get_object_or_404(EventoVenta, pk=event_id)
    return _build_projection_workbook_response(event, exact_day=True)


@login_required
def evento_export_branch_day_projection(request, event_id: int):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para exportar la proyeccion comercial.")
    event = get_object_or_404(EventoVenta, pk=event_id)
    filename, content = _build_branch_day_workbook_file(event)
    response = HttpResponse(
        content,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@login_required
def evento_export_executive_dashboard(request, event_id: int):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para exportar el dashboard ejecutivo.")
    event = get_object_or_404(EventoVenta, pk=event_id)
    filename, content = _build_executive_dashboard_workbook_file(event)
    response = HttpResponse(
        content,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@login_required
def evento_generate_projection_files(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para generar archivos de proyeccion.")
    event = get_object_or_404(EventoVenta, pk=event_id)
    artifacts = _persist_projection_artifacts(event, request.user, force=True)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(
        request,
        f"Se generaron {len(artifacts)} archivos de proyeccion para el evento {event.code}.",
    )
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_download_projection_artifact(request, event_id: int, artifact_id: int):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para descargar archivos de proyeccion.")
    event = get_object_or_404(EventoVenta, pk=event_id)
    artifact = get_object_or_404(EventoVentaProjectionArtifact, pk=artifact_id, sales_event=event)
    artifact, file_path = _ensure_projection_artifact_file(event, artifact, request.user)
    if not file_path.exists():
        raise Http404("El archivo de proyeccion ya no existe en disco.")
    response = FileResponse(file_path.open("rb"), as_attachment=True, filename=artifact.file_name)
    return response


@login_required
def evento_adjustments_editor(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para ajustar la proyección comercial.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    if event.status in {
        EventoVenta.STATUS_APROBADO,
        EventoVenta.STATUS_APROBADO_AJUSTES,
        EventoVenta.STATUS_ENVIADO_PROD,
        EventoVenta.STATUS_VALIDADO_PROD,
        EventoVenta.STATUS_ENVIADO_COMPRAS,
        EventoVenta.STATUS_CERRADO,
    }:
        messages.error(request, "El evento ya no admite ajustes interactivos.")
        return redirect("ventas:evento_detail", event_id=event.id)

    default_scope = request.POST.get("scope_mode") or request.GET.get("scope")
    if not default_scope:
        any_active_draft = _active_adjustment_draft(event)
        default_scope = any_active_draft.scope_mode if any_active_draft else EventoVentaAdjustmentDraft.SCOPE_RANGE
    selected_scope = _adjustment_scope_mode(default_scope)
    draft = _active_adjustment_draft(event, scope_mode=selected_scope)
    filters = {
        "branch": request.POST.get("branch") or request.GET.get("branch") or "",
        "family": request.POST.get("family") or request.GET.get("family") or "",
        "category": request.POST.get("category") or request.GET.get("category") or "",
        "source": request.POST.get("source") or request.GET.get("source") or "",
        "q": request.POST.get("q") or request.GET.get("q") or "",
    }
    notes = (request.POST.get("notes") if request.method == "POST" else (draft.notes if draft else "")) or ""

    entries_map = _draft_entries_map(draft)
    state = _build_adjustment_editor_matrix(
        event,
        scope_mode=selected_scope,
        draft_entries=entries_map,
        filters=filters,
    )
    baseline_values = {
        (int(row["product_id"]), int(state["branch_code_id_map"][branch_code])): row["current_branch_values"][branch_code]
        for row in state["rows"]
        for branch_code in state["branch_codes"]
        if branch_code in state["branch_code_id_map"]
    }
    preview_summary = state["preview_summary"]

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip().lower()
        posted_values = _extract_posted_adjustment_values(request)
        merged_entries = _merge_adjustment_entries(
            base_entries=entries_map,
            posted_values=posted_values,
            baseline_values=baseline_values,
        )
        state = _build_adjustment_editor_matrix(
            event,
            scope_mode=selected_scope,
            draft_entries=merged_entries,
            filters=filters,
        )
        preview_summary = state["preview_summary"]

        if action == "cancel_draft" and draft:
            draft.status = EventoVentaAdjustmentDraft.STATUS_CANCELED
            draft.finalized_by = request.user
            draft.finalized_at = timezone.now()
            draft.save(update_fields=["status", "finalized_by", "finalized_at", "updated_at"])
            messages.success(request, "Borrador de ajustes cancelado.")
            return redirect("ventas:evento_adjustments_editor", event_id=event.id)

        if action == "save_draft":
            draft = _save_adjustment_draft(
                event,
                scope_mode=selected_scope,
                draft=draft,
                entries_map=merged_entries,
                preview_summary=preview_summary,
                notes=notes,
                user=request.user,
            )
            messages.success(request, "Borrador de ajustes guardado.")
        elif action == "finalize":
            if not merged_entries:
                messages.warning(request, "No hay cambios para finalizar.")
            else:
                with transaction.atomic():
                    applied = _finalize_adjustment_draft(
                        event,
                        scope_mode=selected_scope,
                        entries_map=merged_entries,
                        notes=notes,
                        user=request.user,
                    )
                    draft = _save_adjustment_draft(
                        event,
                        scope_mode=selected_scope,
                        draft=draft,
                        entries_map=merged_entries,
                        preview_summary=preview_summary,
                        notes=notes,
                        user=request.user,
                    )
                    draft.status = EventoVentaAdjustmentDraft.STATUS_FINALIZED
                    draft.finalized_by = request.user
                    draft.finalized_at = timezone.now()
                    draft.save(update_fields=["status", "finalized_by", "finalized_at", "updated_at"])
                    _persist_projection_artifacts(event, request.user, force=True)
                    _refresh_event_support_outputs(event)
                    log_evento_change(
                        event,
                        "EventoVentaAdjustmentDraft",
                        str(draft.id),
                        "FINALIZE",
                        new_data={
                            "scope_mode": selected_scope,
                            "changed_cells": preview_summary.get("changed_cells", 0),
                            "applied": applied,
                        },
                        actor=request.user,
                    )
                messages.success(
                    request,
                    f"Ajuste finalizado. Se aplicaron {applied} combinaciones sucursal-producto y se regeneraron archivos.",
                )
                return redirect("ventas:evento_detail", event_id=event.id)
        elif action == "preview":
            messages.success(request, "Preview actualizado. Revisa el impacto antes de guardar o finalizar.")

        if action in {"preview", "save_draft"}:
            entries_map = merged_entries

    context = {
        "event": event,
        "scope_mode": selected_scope,
        "draft": draft,
        "notes": notes,
        "filters": filters,
        "branch_codes": state["branch_codes"],
        "all_branch_codes": state["all_branch_codes"],
        "rows": state["rows"],
        "family_options": state["family_options"],
        "category_options": state["category_options"],
        "preview_summary": preview_summary,
        "can_manage": _can_manage_events(request.user),
    }
    return render(request, "ventas/eventos_adjustments_editor.html", context)


@login_required
def evento_update(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para editar eventos comerciales.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    if request.method == "POST":
        event.name = (request.POST.get("name") or event.name).strip()
        event.event_type = (request.POST.get("event_type") or event.event_type).strip()
        parsed_main_date = _coerce_posted_date(request.POST.get("main_date"), fallback=event.main_date)
        if parsed_main_date:
            event.main_date = parsed_main_date
        default_start, default_end = _projection_week_window(parsed_main_date)
        event.analysis_start_date = _coerce_posted_date(request.POST.get("analysis_start_date"), fallback=default_start)
        event.analysis_end_date = _coerce_posted_date(request.POST.get("analysis_end_date"), fallback=default_end)
        event.objective_type = (request.POST.get("objective_type") or event.objective_type).strip()
        event.objective_notes = (request.POST.get("objective_notes") or event.objective_notes).strip()
        event.approval_deadline = _coerce_posted_date(
            request.POST.get("approval_deadline"),
            fallback=event.approval_deadline,
        )
        event.priority = (request.POST.get("priority") or event.priority)
        event.scenario_focus = (request.POST.get("scenario_focus") or event.scenario_focus)
        event.guamuchil_comparable_branch_id = None
        event.conservative_pct = Decimal(request.POST.get("conservative_pct") or event.conservative_pct)
        event.aggressive_pct = Decimal(request.POST.get("aggressive_pct") or event.aggressive_pct)
        event.save()

        branch_ids = set(request.POST.getlist("branches"))
        product_ids = request.POST.getlist("products")
        selected_products = list(Receta.objects.filter(id__in=product_ids))
        products, excluded_products = _filter_executive_event_products(selected_products)
        EventoVentaSucursal.objects.filter(sales_event=event).delete()
        EventoVentaProducto.objects.filter(sales_event=event).delete()
        for branch in eligible_sales_event_branch_qs().filter(id__in=branch_ids):
            link = EventoVentaSucursal.objects.create(sales_event=event, branch=branch)
            comparable_branch_id = request.POST.get(f"comparable_branch_{branch.id}") or None
            if comparable_branch_id:
                link.comparable_branch_id = comparable_branch_id
                link.save(update_fields=["comparable_branch"])
        for product in products:
            EventoVentaProducto.objects.create(sales_event=event, product=product)

        log_evento_change(event, "EventoVenta", event.id, "UPDATE", new_data={"name": event.name}, actor=request.user)
        if _dispatch_event_projection_pipeline(event):
            messages.success(
                request,
                "Evento actualizado. Recalculo de forecast y archivos ERP enviado a segundo plano.",
            )
        else:
            result, artifacts = _run_event_projection_pipeline(event, request.user)
            if result.get("created"):
                messages.success(request, f"Evento actualizado. Forecast y archivos ERP regenerados ({len(artifacts)} archivos).")
            else:
                messages.success(request, "Evento actualizado.")
                if result.get("warnings"):
                    messages.warning(request, result["warnings"][0])
        if excluded_products:
            excluded_names = ", ".join(product.nombre for product, _reason in excluded_products[:6])
            if len(excluded_products) > 6:
                excluded_names += ", ..."
            messages.warning(
                request,
                "Se omitieron productos fuera del scope ejecutivo "
                f"(accesorios, bebidas o servicios): {excluded_names}.",
            )
        return redirect("ventas:evento_detail", event_id=event.id)

    context = {
        "event": event,
        "branches": eligible_sales_event_branch_qs(),
        "product_groups": _product_selection_groups(),
        "priority_choices": EventoVenta.PRIORIDAD_CHOICES,
        "selected_branch_ids": set(_event_branch_links(event).values_list("branch_id", flat=True)) | _mandatory_branch_ids(),
        "mandatory_branch_ids": _mandatory_branch_ids(),
        "branch_comparable_controls": _branch_comparable_controls(
            selected_branch_ids=set(_event_branch_links(event).values_list("branch_id", flat=True)) | _mandatory_branch_ids(),
            event=event,
            anchor=event.main_date,
        ),
        "selected_product_ids": set(EventoVentaProducto.objects.filter(sales_event=event).values_list("product_id", flat=True)),
    }
    return render(request, "ventas/eventos_form.html", context)


@login_required
def evento_delete(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para eliminar eventos comerciales.")
    if request.method != "POST":
        return redirect("ventas:eventos")

    event = get_object_or_404(EventoVenta, pk=event_id)
    if event.status not in {EventoVenta.STATUS_BORRADOR, EventoVenta.STATUS_RECHAZADO}:
        messages.error(request, "Solo se pueden eliminar eventos en borrador o rechazados.")
        return redirect("ventas:eventos")

    event_label = f"{event.code} · {event.name}"
    event.delete()
    messages.success(request, f"Evento eliminado: {event_label}.")
    return redirect("ventas:eventos")


@login_required
def evento_upload_adjustments(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para cargar ajustes de ventas.")
    if request.method != "POST":
        return redirect("ventas:evento_detail", event_id=event_id)

    event = get_object_or_404(EventoVenta, pk=event_id)
    if event.status in {EventoVenta.STATUS_APROBADO, EventoVenta.STATUS_APROBADO_AJUSTES, EventoVenta.STATUS_CERRADO}:
        messages.error(request, "El evento ya no admite ajustes de ventas.")
        return redirect("ventas:evento_detail", event_id=event.id)

    uploaded = request.FILES.get("adjustments_file")
    if not uploaded:
        messages.error(request, "Sube un archivo CSV o XLSX con los ajustes.")
        return redirect("ventas:evento_detail", event_id=event.id)

    scope_mode = (request.POST.get("adjust_scope") or "SEMANA").strip().upper()
    try:
        rows = _decode_uploaded_rows(uploaded)
        with transaction.atomic():
            applied, warnings = _apply_adjustment_rows(event, rows, request.user, scope_mode=scope_mode)
            if applied:
                _persist_projection_artifacts(event, request.user, force=True)
                _refresh_event_support_outputs(event)
                log_evento_change(
                    event,
                    "EventoVentaAdjustment",
                    str(event.id),
                    "BULK_UPLOAD",
                    new_data={"rows": applied, "scope_mode": scope_mode, "file_name": uploaded.name},
                    actor=request.user,
                )
    except ValueError as exc:
        messages.error(request, str(exc))
        return redirect("ventas:evento_detail", event_id=event.id)

    if applied:
        messages.success(request, f"Ajustes de ventas aplicados: {applied}. Se actualizaron proyección, ingresos e inversión.")
    else:
        messages.warning(request, "No se aplicaron ajustes.")
    for warning in warnings[:8]:
        messages.warning(request, warning)
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_generate_forecast(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para generar forecast.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    result = generate_event_forecast(event, request.user)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    _sync_event_review_status_with_guardrails(event, actor=request.user)
    if result.get("created"):
        messages.success(request, f"Forecast generado. Filas: {result['created']}.")
    if result.get("warnings"):
        messages.warning(request, result["warnings"][0])
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_submit_approval(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para enviar a aprobación.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    if _active_adjustment_draft(event):
        messages.error(request, "Existe un borrador de ajustes pendiente. Finalízalo o cancélalo antes de enviar a aprobación.")
        return redirect("ventas:evento_adjustments_editor", event_id=event.id)
    guard_redirect = _enforce_approval_guard(request, event, action_label="envío a aprobación")
    if guard_redirect:
        return guard_redirect
    EventoVentaApproval.objects.create(
        sales_event=event,
        approval_stage=EventoVentaApproval.STAGE_DIRECCION,
        role_required="DG",
    )
    event.status = EventoVenta.STATUS_PENDIENTE_DG
    event.save(update_fields=["status", "updated_at"])
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, "Evento enviado a Dirección para aprobación.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_approve(request, event_id: int):
    if not _can_approve_events(request.user):
        raise PermissionDenied("No tienes permisos para aprobar.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    if _active_adjustment_draft(event):
        messages.error(request, "Existe un borrador de ajustes pendiente. Finalízalo o cancélalo antes de aprobar.")
        return redirect("ventas:evento_adjustments_editor", event_id=event.id)
    guard_redirect = _enforce_approval_guard(request, event, action_label="aprobación")
    if guard_redirect:
        return guard_redirect
    event.status = EventoVenta.STATUS_APROBADO
    event.approved_by = request.user
    event.approved_at = timezone.now()
    event.save(update_fields=["status", "approved_by", "approved_at", "updated_at"])
    _persist_projection_artifacts(event, request.user, force=True)
    _refresh_event_support_outputs(event)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, "Evento aprobado.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_reject(request, event_id: int):
    if not _can_approve_events(request.user):
        raise PermissionDenied("No tienes permisos para rechazar.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    event.status = EventoVenta.STATUS_RECHAZADO
    event.rejected_by = request.user
    event.rejected_at = timezone.now()
    event.save(update_fields=["status", "rejected_by", "rejected_at", "updated_at"])
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.error(request, "Evento rechazado.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_generate_production(request, event_id: int):
    if not _can_manage_capacity(request.user):
        raise PermissionDenied("No tienes permisos para generar plan de producción.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    guard = _guard_event_operations(request, event)
    if guard:
        return guard
    result = generate_production_plan(event)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, f"Plan de producción generado: {result.get('created', 0)} líneas.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_confirm_production(request, event_id: int):
    if not _can_manage_capacity(request.user):
        raise PermissionDenied("No tienes permisos para confirmar producción.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    guard = _guard_event_operations(request, event)
    if guard:
        return guard
    updated = event.production_plans.update(
        status="CONFIRMADO",
        approved_by_production=request.user,
        approved_at=timezone.now(),
    )
    event.status = EventoVenta.STATUS_VALIDADO_PROD
    event.save(update_fields=["status", "updated_at"])
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, f"Producción confirmada en {updated} plan(es).")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_generate_inputs(request, event_id: int):
    if not has_any_role(request.user, ROLE_ADMIN, ROLE_PRODUCCION, ROLE_COMPRAS):
        raise PermissionDenied("No tienes permisos para generar requerimientos.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    guard = _guard_event_operations(request, event)
    if guard:
        return guard
    result = build_input_requirements(event)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, f"Requerimientos de insumo generados: {result.get('created', 0)}.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_generate_purchases(request, event_id: int):
    if not has_any_role(request.user, ROLE_ADMIN, ROLE_COMPRAS):
        raise PermissionDenied("No tienes permisos para generar requerimientos de compra.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    guard = _guard_event_operations(request, event)
    if guard:
        return guard
    result = build_purchase_requirements(event)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, f"Requerimientos de compra generados: {result.get('created', 0)}.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_generate_financials(request, event_id: int):
    if not has_any_role(request.user, ROLE_ADMIN, ROLE_DG):
        raise PermissionDenied("No tienes permisos para calcular finanzas.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    guard = _guard_event_operations(request, event)
    if guard:
        return guard
    result = build_financials(event)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, f"Resumen financiero generado: {result.get('created', 0)} escenarios.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_close(request, event_id: int):
    if not _can_manage_events(request.user):
        raise PermissionDenied("No tienes permisos para cerrar eventos.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    event.status = EventoVenta.STATUS_CERRADO
    event.save(update_fields=["status", "updated_at"])
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, "Evento cerrado.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_postmortem(request, event_id: int):
    if not _can_view_events(request.user):
        raise PermissionDenied("No tienes permisos para ver el postmortem.")

    event = get_object_or_404(EventoVenta, pk=event_id)
    result = build_postmortem(event)
    if result.get("created"):
        messages.success(request, f"Postmortem actualizado con {result['created']} métricas.")
    if result.get("warnings"):
        messages.warning(request, result["warnings"][0])
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_capacity_rule_create(request, event_id: int):
    if not _can_manage_capacity(request.user):
        raise PermissionDenied("No tienes permisos para gestionar capacidad.")
    if request.method != "POST":
        return redirect("ventas:evento_detail", event_id=event_id)

    event = get_object_or_404(EventoVenta, pk=event_id)
    raw_limit = str(request.POST.get("max_production_qty") or "").strip()
    if not raw_limit:
        messages.error(request, "La capacidad máxima es requerida.")
        return redirect("ventas:evento_detail", event_id=event.id)

    capacity_rule = EventoVentaCapacityRule.objects.create(
        sales_event=event,
        capacity_date=request.POST.get("capacity_date") or None,
        product_id=request.POST.get("product_id") or None,
        max_production_qty=Decimal(raw_limit),
        notes=(request.POST.get("notes") or "").strip(),
    )
    log_evento_change(
        event,
        "EventoVentaCapacityRule",
        capacity_rule.id,
        "CREATE",
        new_data={
            "capacity_date": capacity_rule.capacity_date.isoformat() if capacity_rule.capacity_date else "",
            "product_id": capacity_rule.product_id,
            "max_production_qty": str(capacity_rule.max_production_qty),
            "notes": capacity_rule.notes,
        },
        actor=request.user,
    )
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, "Regla de capacidad registrada.")
    return redirect("ventas:evento_detail", event_id=event.id)


@login_required
def evento_capacity_rule_delete(request, event_id: int, rule_id: int):
    if not _can_manage_capacity(request.user):
        raise PermissionDenied("No tienes permisos para gestionar capacidad.")
    if request.method != "POST":
        return redirect("ventas:evento_detail", event_id=event_id)

    event = get_object_or_404(EventoVenta, pk=event_id)
    rule = get_object_or_404(EventoVentaCapacityRule, pk=rule_id, sales_event=event)
    old_data = {
        "capacity_date": rule.capacity_date.isoformat() if rule.capacity_date else "",
        "product_id": rule.product_id,
        "max_production_qty": str(rule.max_production_qty),
        "notes": rule.notes,
    }
    rule.delete()
    log_evento_change(event, "EventoVentaCapacityRule", rule_id, "DELETE", old_data=old_data, actor=request.user)
    _refresh_event_detail_snapshot(event, generated_by=request.user)
    messages.success(request, "Regla de capacidad eliminada.")
    return redirect("ventas:evento_detail", event_id=event.id)
