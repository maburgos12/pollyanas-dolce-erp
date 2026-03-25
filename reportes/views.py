import csv
from io import BytesIO
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from urllib.parse import urlencode
from calendar import monthrange

from openpyxl import Workbook
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Sum, Max, Count, Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone

from core.access import can_view_reportes
from inventario.models import ExistenciaInsumo, MovimientoInventario
from control.models import MermaPOS, VentaPOS
from maestros.models import CostoInsumo, Insumo
from maestros.utils.canonical_catalog import canonicalized_active_insumos, enterprise_readiness_profile, latest_costo_canonico
from recetas.models import Receta, LineaReceta, VentaHistorica, PlanProduccion, PlanProduccionItem, PronosticoVenta, SolicitudVenta
from compras.models import SolicitudCompra, OrdenCompra, RecepcionCompra
from pos_bridge.models import PointDailySale, PointDailyBranchIndicator

from .bi_utils import compute_bi_snapshot
from .executive_panels import build_executive_bi_panels

POINT_BRIDGE_SALES_SOURCE = "POINT_BRIDGE_SALES"
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
RECENT_POINT_SOURCE = "/Report/VentasCategorias"


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _canonical_catalog_maps(limit: int = 2000) -> tuple[dict[int, dict], dict[int, dict]]:
    canonical_rows = canonicalized_active_insumos(limit=limit)
    member_to_row = {}
    canonical_by_id = {}
    for row in canonical_rows:
        canonical = row["canonical"]
        canonical_by_id[canonical.id] = row
        for member_id in row["member_ids"]:
            member_to_row[member_id] = row
    return member_to_row, canonical_by_id


def _official_sales_stage_max_date():
    return (
        PointDailySale.objects.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
        .order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )


def _recent_sales_stage_max_date():
    return (
        PointDailySale.objects.filter(source_endpoint=RECENT_POINT_SOURCE)
        .order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )


def _operational_sales_filters(*, start_date, end_date) -> Q:
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


def _operational_sales_rows_for_date(target_date):
    if PointDailySale.objects.filter(sale_date=target_date, source_endpoint=OFFICIAL_POINT_SOURCE).exists():
        return PointDailySale.objects.filter(sale_date=target_date, source_endpoint=OFFICIAL_POINT_SOURCE)
    return PointDailySale.objects.filter(sale_date=target_date, source_endpoint=RECENT_POINT_SOURCE)


def _sales_source_context() -> dict[str, object]:
    latest_point_date = max(
        [value for value in [_official_sales_stage_max_date(), _recent_sales_stage_max_date()] if value],
        default=None,
    )
    latest_bridge_date = (
        VentaHistorica.objects.filter(fuente=POINT_BRIDGE_SALES_SOURCE)
        .order_by("-fecha")
        .values_list("fecha", flat=True)
        .first()
    )
    latest_hist_date = VentaHistorica.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    if latest_point_date:
        return {"mode": "point_stage", "latest_date": latest_point_date, "label": "Point directo", "detail": "Fuente canónica Point bridge.", "canonical": True}
    if latest_bridge_date:
        return {"mode": "point_history", "latest_date": latest_bridge_date, "label": "Point conciliado", "detail": "Histórico Point materializado a ERP.", "canonical": True}
    if latest_hist_date:
        return {"mode": "historical_fallback", "latest_date": latest_hist_date, "label": "Histórico importado no canónico", "detail": "Fuente referencial; no representa Point directo.", "canonical": False}
    return {"mode": "none", "latest_date": None, "label": "Sin fuente", "detail": "No hay ventas cargadas.", "canonical": False}


def _sales_rows_for_date(source: dict[str, object], target_date):
    if source["mode"] == "point_stage":
        return _operational_sales_rows_for_date(target_date)
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fecha=target_date, fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.filter(fecha=target_date)
    return VentaHistorica.objects.none()


def _sales_rows_for_month(source: dict[str, object], year: int, month: int):
    if source["mode"] == "point_stage":
        start_date = date(year, month, 1)
        end_date = date(year, month, monthrange(year, month)[1])
        return PointDailySale.objects.filter(
            sale_date__year=year,
            sale_date__month=month,
        ).filter(
            _operational_sales_filters(start_date=start_date, end_date=end_date)
        )
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fecha__year=year, fecha__month=month, fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.filter(fecha__year=year, fecha__month=month)
    return VentaHistorica.objects.none()


def _point_sales_month_total(year: int, month: int) -> dict[str, object]:
    direct_qs = PointDailySale.objects.filter(
        sale_date__year=year,
        sale_date__month=month,
        source_endpoint=OFFICIAL_POINT_SOURCE,
    )
    direct_total = direct_qs.aggregate(total=Sum("total_amount")).get("total") or Decimal("0")
    if direct_total > 0:
        return {"value": Decimal(str(direct_total)), "source_label": "Point directo"}

    bridge_qs = VentaHistorica.objects.filter(
        fecha__year=year,
        fecha__month=month,
        fuente=POINT_BRIDGE_SALES_SOURCE,
    )
    bridge_total = bridge_qs.aggregate(total=Sum("monto_total")).get("total") or Decimal("0")
    if bridge_total > 0:
        return {"value": Decimal(str(bridge_total)), "source_label": "Point conciliado"}

    return {"value": Decimal("0"), "source_label": "Sin dato oficial"}


def _sales_previous_dates(source: dict[str, object], target_date) -> list:
    if source["mode"] == "point_stage":
        return list(
            PointDailySale.objects.filter(
                sale_date__lt=target_date,
                source_endpoint__in=[OFFICIAL_POINT_SOURCE, RECENT_POINT_SOURCE],
            )
            .order_by("-sale_date")
            .values_list("sale_date", flat=True)
            .distinct()
        )
    if source["mode"] == "point_history":
        return list(
            VentaHistorica.objects.filter(fecha__lt=target_date, fuente=POINT_BRIDGE_SALES_SOURCE)
            .order_by("-fecha")
            .values_list("fecha", flat=True)
            .distinct()
        )
    if source["mode"] == "historical_fallback":
        return list(VentaHistorica.objects.filter(fecha__lt=target_date).order_by("-fecha").values_list("fecha", flat=True).distinct())
    return []


def _sales_history_queryset(source: dict[str, object]):
    if source["mode"] == "point_stage":
        official_max = _official_sales_stage_max_date()
        q = Q(source_endpoint=OFFICIAL_POINT_SOURCE)
        if official_max:
            q |= Q(source_endpoint=RECENT_POINT_SOURCE, sale_date__gt=official_max)
        else:
            q = Q(source_endpoint=RECENT_POINT_SOURCE)
        return PointDailySale.objects.filter(q)
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.all()
    return VentaHistorica.objects.none()


def _ventas_historicas_bi_summary() -> dict[str, object]:
    source = _sales_source_context()
    rows_qs = _sales_history_queryset(source)
    total_rows = rows_qs.count()
    if total_rows == 0:
        return {
            "available": False,
            "status": "Sin histórico",
            "tone": "warning",
            "detail": "No hay ventas históricas cargadas para análisis ejecutivo.",
            "source_label": source["label"],
            "date_label": "Sin cobertura",
            "active_days": 0,
            "expected_days": 0,
            "missing_days": 0,
            "branch_count": 0,
            "recipe_count": 0,
            "total_rows": 0,
            "total_units": Decimal("0"),
            "total_amount": Decimal("0"),
            "top_branches": [],
            "top_recipes": [],
        }
    if source["mode"] == "point_stage":
        total_units = rows_qs.aggregate(total=Sum("quantity")).get("total") or Decimal("0")
        total_amount = rows_qs.aggregate(total=Sum("total_amount")).get("total") or Decimal("0")
        first_date = rows_qs.order_by("sale_date").values_list("sale_date", flat=True).first()
        last_date = rows_qs.order_by("-sale_date").values_list("sale_date", flat=True).first()
        active_days = rows_qs.values_list("sale_date", flat=True).distinct().count()
        branch_count = rows_qs.values_list("branch_id", flat=True).distinct().count()
        recipe_count = rows_qs.values_list("product_id", flat=True).distinct().count()
        top_branches = list(rows_qs.values("branch__external_id", "branch__name").annotate(total=Sum("quantity")).order_by("-total", "branch__external_id")[:4])
        top_recipes = list(rows_qs.values("product__name").annotate(total=Sum("quantity")).order_by("-total", "product__name")[:5])
    else:
        total_units = rows_qs.aggregate(total=Sum("cantidad")).get("total") or Decimal("0")
        total_amount = rows_qs.aggregate(total=Sum("monto_total")).get("total") or Decimal("0")
        first_date = rows_qs.order_by("fecha").values_list("fecha", flat=True).first()
        last_date = rows_qs.order_by("-fecha").values_list("fecha", flat=True).first()
        active_days = rows_qs.values_list("fecha", flat=True).distinct().count()
        branch_count = rows_qs.exclude(sucursal_id__isnull=True).values_list("sucursal_id", flat=True).distinct().count()
        recipe_count = rows_qs.values_list("receta_id", flat=True).distinct().count()
        top_branches = list(rows_qs.exclude(sucursal_id__isnull=True).values("sucursal__codigo", "sucursal__nombre").annotate(total=Sum("cantidad")).order_by("-total", "sucursal__codigo")[:4])
        top_recipes = list(rows_qs.values("receta__nombre").annotate(total=Sum("cantidad")).order_by("-total", "receta__nombre")[:5])
    expected_days = ((last_date - first_date).days + 1) if first_date and last_date else 0
    missing_days = max(expected_days - active_days, 0)
    return {
        "available": True,
        "status": "Cobertura cerrada" if missing_days == 0 else "Cobertura parcial",
        "tone": "success" if missing_days == 0 and source["canonical"] else "warning",
        "official_ready": bool(source["canonical"] and missing_days == 0),
        "detail": (
            f"{source['detail']} La serie diaria está lista para lectura ejecutiva y planeación."
            if missing_days == 0
            else f"{source['detail']} Hay {missing_days} día(s) faltantes dentro del rango histórico cargado."
        ),
        "source_label": source["label"],
        "date_label": f"{first_date.strftime('%d/%m/%Y')} → {last_date.strftime('%d/%m/%Y')}" if first_date and last_date else "Sin cobertura",
        "active_days": active_days,
        "expected_days": expected_days,
        "missing_days": missing_days,
        "branch_count": branch_count,
        "recipe_count": recipe_count,
        "total_rows": total_rows,
        "total_units": total_units,
        "total_amount": total_amount,
        "top_branches": top_branches,
        "top_recipes": top_recipes,
    }


def _bi_daily_sales_snapshot() -> dict[str, object]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return {
            "status": "Sin cortes",
            "tone": "warning",
            "detail": "Todavía no hay ventas recientes para lectura ejecutiva diaria.",
            "date_label": "Sin fecha",
            "source_label": "Sin fuente",
            "total_units": Decimal("0"),
            "total_amount": Decimal("0"),
            "total_tickets": 0,
            "branch_count": 0,
            "recipe_count": 0,
            "comparison_label": "Sin comparativo",
            "comparison_tone": "warning",
            "comparison_detail": "Carga cortes de venta para habilitar lectura diaria.",
            "comparison_basis": "Sin referencia disponible",
            "top_branches": [],
            "top_products": [],
        }
    rows = _sales_rows_for_date(source, latest_date)
    if source["mode"] == "point_stage":
        indicator_rows = PointDailyBranchIndicator.objects.filter(indicator_date=latest_date)
        totals = rows.aggregate(total_units=Sum("quantity"), total_amount=Sum("total_amount"), branch_count=Count("branch", distinct=True), recipe_count=Count("product", distinct=True))
        indicator_totals = indicator_rows.aggregate(total_tickets=Sum("total_tickets"))
        mapped_totals = rows.filter(receta_id__isnull=False, branch__erp_branch_id__isnull=False).aggregate(mapped_units=Sum("quantity"), mapped_amount=Sum("total_amount"))
    else:
        totals = rows.aggregate(total_units=Sum("cantidad"), total_amount=Sum("monto_total"), total_tickets=Sum("tickets"), branch_count=Count("sucursal", distinct=True), recipe_count=Count("receta", distinct=True))
        indicator_totals = {}
        mapped_totals = {"mapped_units": totals.get("total_units"), "mapped_amount": totals.get("total_amount")}
    total_units = Decimal(str(totals.get("total_units") or 0))
    total_amount = Decimal(str(totals.get("total_amount") or 0))
    prev_date = next(iter(_sales_previous_dates(source, latest_date)), None)
    comparison_label = "Base inicial"
    comparison_tone = "warning"
    comparison_detail = "Aún no hay un corte previo comparable."
    comparison_basis = "Contra el corte inmediato anterior"
    if prev_date:
        prev_rows = _sales_rows_for_date(source, prev_date)
        if source["mode"] == "point_stage":
            prev_totals = prev_rows.aggregate(total_units=Sum("quantity"), total_amount=Sum("total_amount"))
        else:
            prev_totals = prev_rows.aggregate(total_units=Sum("cantidad"), total_amount=Sum("monto_total"))
        prev_amount = Decimal(str(prev_totals.get("total_amount") or 0))
        prev_units = Decimal(str(prev_totals.get("total_units") or 0))
        if prev_amount > 0:
            delta_pct = ((total_amount - prev_amount) / prev_amount) * Decimal("100")
            comparison_label = "Arriba" if delta_pct >= 0 else "Abajo"
            comparison_tone = "success" if delta_pct >= 0 else "warning"
            comparison_detail = f"{abs(delta_pct):.1f}% vs corte previo ({prev_date.isoformat()})"
        elif prev_units > 0:
            delta_pct = ((total_units - prev_units) / prev_units) * Decimal("100")
            comparison_label = "Arriba" if delta_pct >= 0 else "Abajo"
            comparison_tone = "success" if delta_pct >= 0 else "warning"
            comparison_detail = f"{abs(delta_pct):.1f}% en unidades vs corte previo ({prev_date.isoformat()})"
    month_rows = _sales_rows_for_month(source, latest_date.year, latest_date.month)
    if source["mode"] == "point_stage":
        month_indicator_rows = PointDailyBranchIndicator.objects.filter(indicator_date__year=latest_date.year, indicator_date__month=latest_date.month)
        month_totals = month_rows.aggregate(total_units=Sum("quantity"), total_amount=Sum("total_amount"))
        month_indicator_totals = month_indicator_rows.aggregate(total_tickets=Sum("total_tickets"))
        top_branches = list(rows.values("branch__external_id", "branch__name").annotate(total=Sum("quantity"), amount=Sum("total_amount")).order_by("-amount", "-total", "branch__name")[:5])
        top_products = list(rows.values("product__name").annotate(total=Sum("quantity"), amount=Sum("total_amount")).order_by("-amount", "-total", "product__name")[:5])
    else:
        month_totals = month_rows.aggregate(total_units=Sum("cantidad"), total_amount=Sum("monto_total"), total_tickets=Sum("tickets"))
        month_indicator_totals = {}
        top_branches = list(rows.exclude(sucursal_id__isnull=True).values("sucursal__codigo", "sucursal__nombre").annotate(total=Sum("cantidad"), amount=Sum("monto_total")).order_by("-amount", "-total", "sucursal__nombre")[:5])
        top_products = list(rows.values("receta__nombre").annotate(total=Sum("cantidad"), amount=Sum("monto_total")).order_by("-amount", "-total", "receta__nombre")[:5])
    total_tickets = int((indicator_totals.get("total_tickets") if source["mode"] == "point_stage" else totals.get("total_tickets")) or 0)
    branch_count = int(totals.get("branch_count") or 0)
    mapped_amount = Decimal(str(mapped_totals.get("mapped_amount") or 0))
    mapped_units = Decimal(str(mapped_totals.get("mapped_units") or 0))
    unmapped_amount = total_amount - mapped_amount
    unmapped_units = total_units - mapped_units
    mapping_coverage_pct = ((mapped_amount / total_amount) * Decimal("100")) if total_amount > 0 else None
    tickets_available = total_tickets > 0
    avg_ticket = (total_amount / Decimal(total_tickets)) if tickets_available else None
    avg_branch_amount = (total_amount / Decimal(branch_count)) if branch_count else Decimal("0")
    return {
        "status": "Corte cargado" if source["canonical"] else "Corte referencial",
        "tone": "success" if source["canonical"] else "warning",
        "detail": f"Resumen del último corte disponible. {source['detail']}",
        "date_label": latest_date.isoformat(),
        "month_label": f"{latest_date.year}-{latest_date.month:02d}",
        "source_label": source["label"],
        "total_units": total_units,
        "total_amount": total_amount,
        "total_tickets": total_tickets,
        "tickets_available": tickets_available,
        "branch_count": branch_count,
        "recipe_count": int(totals.get("recipe_count") or 0),
        "avg_ticket": avg_ticket,
        "avg_branch_amount": avg_branch_amount,
        "mapped_amount": mapped_amount,
        "mapped_units": mapped_units,
        "unmapped_amount": unmapped_amount,
        "unmapped_units": unmapped_units,
        "mapping_coverage_pct": mapping_coverage_pct,
        "month_amount": Decimal(str(month_totals.get("total_amount") or 0)),
        "month_units": Decimal(str(month_totals.get("total_units") or 0)),
        "month_tickets": int((month_indicator_totals.get("total_tickets") if source["mode"] == "point_stage" else month_totals.get("total_tickets")) or 0),
        "comparison_label": comparison_label,
        "comparison_tone": comparison_tone,
        "comparison_detail": comparison_detail,
        "comparison_basis": comparison_basis,
        "top_branches": [{"label": row.get("branch__external_id") or row.get("sucursal__codigo") or "Sucursal", "secondary": row.get("branch__name") or row.get("sucursal__nombre") or "", "amount": row.get("amount") or Decimal("0"), "total": row.get("total") or Decimal("0")} for row in top_branches],
        "top_products": [{"label": row.get("product__name") or row.get("receta__nombre") or "Producto", "secondary": "", "amount": row.get("amount") or Decimal("0"), "total": row.get("total") or Decimal("0")} for row in top_products],
    }


def _bi_branch_weekday_comparisons(limit: int = 5) -> list[dict[str, object]]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return []
    comparable_date = next((date_value for date_value in _sales_previous_dates(source, latest_date) if date_value.weekday() == latest_date.weekday()), None)
    if not comparable_date:
        return []
    if source["mode"] == "point_stage":
        current_rows = list(
            _operational_sales_rows_for_date(latest_date)
            .values("branch_id", "branch__external_id", "branch__name")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"))
        )
        current_indicator_map = {
            row["branch_id"]: row
            for row in PointDailyBranchIndicator.objects.filter(indicator_date=latest_date)
            .values("branch_id")
            .annotate(amount=Sum("total_amount"), tickets=Sum("total_tickets"))
        }
        comparable_map = {
            row["branch_id"]: row
            for row in PointDailySale.objects.filter(
                sale_date=comparable_date,
                source_endpoint__in=[OFFICIAL_POINT_SOURCE, RECENT_POINT_SOURCE],
            )
            .filter(_operational_sales_filters(start_date=comparable_date, end_date=comparable_date))
            .values("branch_id")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"))
        }
        comparable_indicator_map = {
            row["branch_id"]: row
            for row in PointDailyBranchIndicator.objects.filter(indicator_date=comparable_date)
            .values("branch_id")
            .annotate(amount=Sum("total_amount"), tickets=Sum("total_tickets"))
        }
    else:
        current_rows = list(_sales_rows_for_date(source, latest_date).exclude(sucursal_id__isnull=True).values("sucursal_id", "sucursal__codigo", "sucursal__nombre").annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets")))
        comparable_map = {row["sucursal_id"]: row for row in _sales_rows_for_date(source, comparable_date).exclude(sucursal_id__isnull=True).values("sucursal_id").annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"))}
    rows: list[dict[str, object]] = []
    for row in current_rows:
        branch_id = row["branch_id"] if source["mode"] == "point_stage" else row["sucursal_id"]
        comparable = comparable_map.get(branch_id)
        if not comparable:
            continue
        if source["mode"] == "point_stage":
            current_indicator = current_indicator_map.get(branch_id) or {}
            comparable_indicator = comparable_indicator_map.get(branch_id) or {}
            current_amount = Decimal(str(current_indicator.get("amount") or row.get("amount") or 0))
            comparable_amount = Decimal(str(comparable_indicator.get("amount") or comparable.get("amount") or 0))
            current_tickets = int(current_indicator.get("tickets") or 0)
        else:
            current_amount = Decimal(str(row.get("amount") or 0))
            comparable_amount = Decimal(str(comparable.get("amount") or 0))
            current_tickets = int(row.get("tickets") or 0)
        current_units = Decimal(str(row.get("units") or 0))
        comparable_units = Decimal(str(comparable.get("units") or 0))
        delta_pct = ((current_amount - comparable_amount) / comparable_amount) * Decimal("100") if comparable_amount > 0 else (((current_units - comparable_units) / comparable_units) * Decimal("100") if comparable_units > 0 else None)
        if delta_pct is None:
            continue
        if delta_pct <= Decimal("-12"):
            status, tone, detail, rank_score = "Abajo del comparable", "danger", f"-{abs(delta_pct):.1f}% vs {comparable_date.isoformat()}", abs(delta_pct) + Decimal("100")
        elif delta_pct >= Decimal("12"):
            status, tone, detail, rank_score = "Arriba del comparable", "success", f"+{delta_pct:.1f}% vs {comparable_date.isoformat()}", delta_pct
        else:
            status, tone, detail, rank_score = "Dentro de rango", "warning", f"{delta_pct:.1f}% vs {comparable_date.isoformat()}", abs(delta_pct)
        rows.append({"branch_code": row.get("branch__external_id") or row.get("sucursal__codigo") or "SIN-COD", "branch_name": row.get("branch__name") or row.get("sucursal__nombre") or "Sucursal", "units": current_units, "amount": current_amount, "tickets": current_tickets, "status": status, "tone": tone, "detail": detail, "rank_score": rank_score})
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0)))
    return rows[:limit]


def _bi_product_weekday_comparisons(limit: int = 5) -> list[dict[str, object]]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return []
    comparable_date = next((date_value for date_value in _sales_previous_dates(source, latest_date) if date_value.weekday() == latest_date.weekday()), None)
    if not comparable_date:
        return []
    if source["mode"] == "point_stage":
        current_rows = list(
            _operational_sales_rows_for_date(latest_date)
            .filter(total_amount__gt=0)
            .values("product_id", "product__name")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), branch_count=Count("branch", distinct=True))
        )
        comparable_map = {
            row["product_id"]: row
            for row in PointDailySale.objects.filter(
                sale_date=comparable_date,
                source_endpoint__in=[OFFICIAL_POINT_SOURCE, RECENT_POINT_SOURCE],
            )
            .filter(_operational_sales_filters(start_date=comparable_date, end_date=comparable_date))
            .filter(total_amount__gt=0)
            .values("product_id")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), branch_count=Count("branch", distinct=True))
        }
    else:
        current_rows = list(_sales_rows_for_date(source, latest_date).exclude(receta_id__isnull=True).filter(monto_total__gt=0).values("receta_id", "receta__nombre").annotate(units=Sum("cantidad"), amount=Sum("monto_total"), branch_count=Count("sucursal", distinct=True)))
        comparable_map = {row["receta_id"]: row for row in _sales_rows_for_date(source, comparable_date).exclude(receta_id__isnull=True).filter(monto_total__gt=0).values("receta_id").annotate(units=Sum("cantidad"), amount=Sum("monto_total"), branch_count=Count("sucursal", distinct=True))}
    rows: list[dict[str, object]] = []
    for row in current_rows:
        product_id = row["product_id"] if source["mode"] == "point_stage" else row["receta_id"]
        comparable = comparable_map.get(product_id)
        if not comparable:
            continue
        current_amount = Decimal(str(row.get("amount") or 0))
        current_units = Decimal(str(row.get("units") or 0))
        comparable_amount = Decimal(str(comparable.get("amount") or 0))
        comparable_units = Decimal(str(comparable.get("units") or 0))
        delta_pct = ((current_amount - comparable_amount) / comparable_amount) * Decimal("100") if comparable_amount > 0 else (((current_units - comparable_units) / comparable_units) * Decimal("100") if comparable_units > 0 else None)
        if delta_pct is None:
            continue
        if delta_pct <= Decimal("-15"):
            status, tone, detail, rank_score = "Abajo del comparable", "danger", f"-{abs(delta_pct):.1f}% vs {comparable_date.isoformat()}", abs(delta_pct) + Decimal("100")
        elif delta_pct >= Decimal("15"):
            status, tone, detail, rank_score = "Arriba del comparable", "success", f"+{delta_pct:.1f}% vs {comparable_date.isoformat()}", delta_pct
        else:
            status, tone, detail, rank_score = "Dentro de rango", "warning", f"{delta_pct:.1f}% vs {comparable_date.isoformat()}", abs(delta_pct)
        rows.append({"recipe_name": row.get("product__name") or row.get("receta__nombre") or "Producto", "units": current_units, "amount": current_amount, "branch_count": int(row.get("branch_count") or 0), "status": status, "tone": tone, "detail": detail, "rank_score": rank_score})
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0)))
    return rows[:limit]


def _bi_bar_rows(
    raw_rows: list[dict[str, object]],
    label_key: str,
    value_key: str,
    secondary_key: str | None = None,
    limit: int = 6,
) -> list[dict[str, object]]:
    rows = list(raw_rows or [])[:limit]
    max_value = max((Decimal(str(item.get(value_key) or 0)) for item in rows), default=Decimal("0"))
    output: list[dict[str, object]] = []
    for item in rows:
        value = Decimal(str(item.get(value_key) or 0))
        pct = float((value / max_value) * Decimal("100")) if max_value > 0 else 0.0
        output.append(
            {
                "label": str(item.get(label_key) or "Sin dato"),
                "secondary": str(item.get(secondary_key) or "") if secondary_key else "",
                "value": value,
                "pct": max(8.0, pct) if value > 0 else 0.0,
            }
        )
    return output


def _bi_monthly_sales_rows(snapshot: dict[str, object]) -> list[dict[str, object]]:
    month_names = {
        "01": "Ene",
        "02": "Feb",
        "03": "Mar",
        "04": "Abr",
        "05": "May",
        "06": "Jun",
        "07": "Jul",
        "08": "Ago",
        "09": "Sep",
        "10": "Oct",
        "11": "Nov",
        "12": "Dic",
    }
    today = timezone.localdate()
    year, month = today.year, today.month
    rows: list[dict[str, object]] = []
    for _ in range(6):
        resolved = _point_sales_month_total(year, month)
        rows.append(
            {
                "periodo": f"{year:04d}-{month:02d}",
                "ventas": resolved["value"],
                "source_label": resolved["source_label"],
            }
        )
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    rows.reverse()
    max_value = max((Decimal(str(item.get("ventas") or 0)) for item in rows), default=Decimal("0"))
    output: list[dict[str, object]] = []
    for item in rows:
        periodo = str(item.get("periodo") or "")
        label = periodo
        if len(periodo) == 7 and "-" in periodo:
            year, month = periodo.split("-", 1)
            label = f"{month_names.get(month, month)} {year[-2:]}"
        value = Decimal(str(item.get("ventas") or 0))
        pct = float((value / max_value) * Decimal("100")) if max_value > 0 else 0.0
        output.append(
            {
                "label": label,
                "value": value,
                "pct": max(8.0, pct) if value > 0 else 0.0,
                "source_label": str(item.get("source_label") or ""),
            }
        )
    return output


def _bi_monthly_margin_rows(snapshot: dict[str, object]) -> list[dict[str, object]]:
    if not snapshot.get("kpis", {}).get("margin_ready"):
        return []
    month_names = {
        "01": "Ene",
        "02": "Feb",
        "03": "Mar",
        "04": "Abr",
        "05": "May",
        "06": "Jun",
        "07": "Jul",
        "08": "Ago",
        "09": "Sep",
        "10": "Oct",
        "11": "Nov",
        "12": "Dic",
    }
    rows = list(snapshot.get("series_mensual") or [])
    max_value = max((abs(Decimal(str(item.get("margen") or 0))) for item in rows), default=Decimal("0"))
    output: list[dict[str, object]] = []
    for item in rows:
        periodo = str(item.get("periodo") or "")
        label = periodo
        if len(periodo) == 7 and "-" in periodo:
            year, month = periodo.split("-", 1)
            label = f"{month_names.get(month, month)} {year[-2:]}"
        margin_value = item.get("margen")
        if margin_value is None:
            continue
        value = Decimal(str(margin_value or 0))
        pct = float((abs(value) / max_value) * Decimal("100")) if max_value > 0 else 0.0
        output.append(
            {
                "label": label,
                "value": value,
                "pct": max(8.0, pct) if value != 0 else 0.0,
                "tone": "success" if value >= 0 else "danger",
            }
        )
    return output


def _bi_comparison_bar_rows(
    raw_rows: list[dict[str, object]],
    label_key: str,
    amount_key: str = "amount",
    secondary_key: str | None = None,
    limit: int = 6,
) -> list[dict[str, object]]:
    rows = list(raw_rows or [])[:limit]
    max_delta = max((abs(Decimal(str(item.get("delta_pct") or 0))) for item in rows), default=Decimal("0"))
    output: list[dict[str, object]] = []
    for item in rows:
        delta = Decimal(str(item.get("delta_pct") or 0))
        tone = str(item.get("tone") or "warning")
        pct = float((abs(delta) / max_delta) * Decimal("100")) if max_delta > 0 else 0.0
        output.append(
            {
                "label": str(item.get(label_key) or "Sin dato"),
                "secondary": str(item.get(secondary_key) or "") if secondary_key else "",
                "detail": str(item.get("detail") or ""),
                "status": str(item.get("status") or ""),
                "tone": tone,
                "delta_label": f"{delta:.1f}%",
                "value": Decimal(str(item.get(amount_key) or 0)),
                "pct": max(8.0, pct) if delta != 0 else 0.0,
            }
        )
    return output


def _bi_purchase_snapshot() -> dict[str, object]:
    today = timezone.localdate()
    solicitudes_abiertas = SolicitudCompra.objects.exclude(estatus=SolicitudCompra.STATUS_RECHAZADA)
    ordenes_abiertas = OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_CERRADA)
    recepciones_abiertas = RecepcionCompra.objects.exclude(estatus=RecepcionCompra.STATUS_CERRADA)
    solicitudes_vencidas = solicitudes_abiertas.filter(fecha_requerida__lt=today).count()
    recepciones_abiertas_count = recepciones_abiertas.count()
    return {
        "solicitudes_abiertas": solicitudes_abiertas.count(),
        "solicitudes_aprobadas": solicitudes_abiertas.filter(estatus=SolicitudCompra.STATUS_APROBADA).count(),
        "solicitudes_vencidas": solicitudes_vencidas,
        "ordenes_abiertas": ordenes_abiertas.count(),
        "ordenes_por_recibir": ordenes_abiertas.filter(
            fecha_entrega_estimada__isnull=False,
            fecha_entrega_estimada__lte=today + timedelta(days=3),
        ).count(),
        "recepciones_abiertas": recepciones_abiertas_count,
        "status": "En seguimiento" if (solicitudes_vencidas or recepciones_abiertas_count) else "Controlado",
        "tone": "warning" if (solicitudes_vencidas or recepciones_abiertas_count) else "success",
        "detail": (
            "Hay documentos de compra que aún requieren cierre."
            if (solicitudes_vencidas or recepciones_abiertas_count)
            else "El flujo documental de compra está controlado."
        ),
    }


def _bi_inventory_snapshot() -> dict[str, object]:
    rows = list(ExistenciaInsumo.objects.select_related("insumo")[:2000])
    total = len(rows)
    criticos = 0
    bajo_reorden = 0
    for row in rows:
        stock = _to_decimal(getattr(row, "stock_actual", 0))
        reorden = _to_decimal(getattr(row, "punto_reorden", 0))
        minimo = _to_decimal(getattr(row, "stock_minimo", 0))
        if stock <= 0:
            criticos += 1
        elif reorden > 0 and stock < reorden:
            bajo_reorden += 1
        elif minimo > 0 and stock < minimo:
            bajo_reorden += 1
    movimientos_hoy = MovimientoInventario.objects.filter(fecha__date=timezone.now().date()).count()
    return {
        "total": total,
        "criticos": criticos,
        "bajo_reorden": bajo_reorden,
        "movimientos_hoy": movimientos_hoy,
        "status": "Con riesgo" if criticos else ("En revisión" if bajo_reorden else "Controlado"),
        "tone": "danger" if criticos else ("warning" if bajo_reorden else "success"),
        "detail": (
            "Hay artículos críticos que ya comprometen surtido."
            if criticos
            else "Hay artículos que piden reabasto."
            if bajo_reorden
            else "El inventario relevante del tablero está bajo control."
        ),
    }


def _bi_production_snapshot() -> dict[str, object]:
    today = timezone.localdate()
    plan_hoy = PlanProduccion.objects.filter(fecha_produccion=today).order_by("-creado_en").first()
    solicitudes_activas = SolicitudVenta.objects.filter(fecha_inicio__lte=today, fecha_fin__gte=today).count()
    planes_abiertos = PlanProduccion.objects.filter(fecha_produccion__gte=today - timedelta(days=7)).exclude(
        estado=PlanProduccion.ESTADO_CERRADO
    ).count()
    return {
        "plan_hoy": plan_hoy.nombre if plan_hoy else "Sin plan para hoy",
        "plan_hoy_estado": plan_hoy.get_estado_display() if plan_hoy else "Sin plan",
        "planes_abiertos": planes_abiertos,
        "solicitudes_activas": solicitudes_activas,
        "status": "En curso" if plan_hoy else "Sin plan",
        "tone": "warning" if not plan_hoy else "success",
        "detail": (
            "No hay plan cargado para hoy."
            if not plan_hoy
            else "Producción ya tiene plan y puede compararse contra demanda activa."
        ),
    }


def _bi_production_summary(date_from, date_to) -> dict[str, object]:
    plans = list(
        PlanProduccion.objects.filter(fecha_produccion__gte=date_from, fecha_produccion__lte=date_to).order_by("fecha_produccion", "id")
    )
    plan_ids = [int(plan.id) for plan in plans]
    items = list(
        PlanProduccionItem.objects.filter(plan_id__in=plan_ids).select_related("receta", "plan").order_by("plan__fecha_produccion")
    )
    total_units = Decimal("0")
    total_cost = Decimal("0")
    final_units = Decimal("0")
    final_recipe_ids: set[int] = set()
    produced_by_recipe: dict[int, dict[str, object]] = {}

    for item in items:
        qty = _to_decimal(item.cantidad)
        if qty <= 0:
            continue
        total_units += qty
        total_cost += _to_decimal(item.costo_total_estimado)
        bucket = produced_by_recipe.setdefault(
            int(item.receta_id),
            {
                "label": item.receta.nombre,
                "value": Decimal("0"),
                "cost": Decimal("0"),
            },
        )
        bucket["value"] = _to_decimal(bucket["value"]) + qty
        bucket["cost"] = _to_decimal(bucket["cost"]) + _to_decimal(item.costo_total_estimado)
        if item.receta.tipo == Receta.TIPO_PRODUCTO_FINAL:
            final_units += qty
            final_recipe_ids.add(int(item.receta_id))

    sold_units = Decimal("0")
    if final_recipe_ids:
        sold_units = _to_decimal(
            VentaHistorica.objects.filter(
                receta_id__in=final_recipe_ids,
                fecha__gte=date_from,
                fecha__lte=date_to,
            ).aggregate(total=Sum("cantidad")).get("total")
        )

    coverage_pct = None
    if sold_units > 0:
        coverage_pct = (final_units * Decimal("100")) / sold_units

    status = "Sin producción"
    tone = "warning"
    detail = "No hay renglones de producción capturados en la ventana BI."
    if total_units > 0 and coverage_pct is not None:
        if coverage_pct >= Decimal("90"):
            status = "Cubre venta"
            tone = "success"
        elif coverage_pct >= Decimal("70"):
            status = "Cobertura ajustada"
            tone = "warning"
        else:
            status = "Producción corta"
            tone = "danger"
        detail = f"Producción final {final_units:.1f} u contra {sold_units:.1f} u vendidas en la ventana BI."
    elif total_units > 0:
        status = "Producción sin comparable"
        tone = "warning"
        detail = "Hay producción en la ventana BI, pero no existe venta final comparable suficiente."

    top_products = sorted(
        produced_by_recipe.values(),
        key=lambda row: (_to_decimal(row.get("value")), str(row.get("label") or "")),
        reverse=True,
    )[:6]
    return {
        "period_label": f"{date_from.isoformat()} a {date_to.isoformat()}",
        "total_units": total_units,
        "total_cost": total_cost,
        "plan_count": len(plans),
        "open_plan_count": sum(1 for plan in plans if plan.estado != PlanProduccion.ESTADO_CERRADO),
        "final_units": final_units,
        "sales_units": sold_units,
        "coverage_pct": coverage_pct,
        "status": status,
        "tone": tone,
        "detail": detail,
        "top_products": top_products,
        "conversion_note": "Conversión a enteros equivalentes pendiente de catálogo específico por presentación.",
    }


def _bi_waste_summary(date_from, date_to) -> dict[str, object]:
    days_window = max((date_to - date_from).days + 1, 1)
    prev_end = date_from - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days_window - 1)

    branch_rows_qs = MermaPOS.objects.filter(fecha__gte=date_from, fecha__lte=date_to)
    branch_rows = list(
        branch_rows_qs
        .select_related("receta", "sucursal")
        .order_by("-fecha", "-id")
    )
    prev_branch_units = _to_decimal(
        MermaPOS.objects.filter(fecha__gte=prev_start, fecha__lte=prev_end).aggregate(total=Sum("cantidad")).get("total")
    )
    branch_units = Decimal("0")
    branch_cost_est = Decimal("0")
    branch_cost_covered = 0
    branch_by_sucursal: dict[str, dict[str, object]] = {}
    for row in branch_rows:
        qty = _to_decimal(row.cantidad)
        branch_units += qty
        code = row.sucursal.codigo if row.sucursal_id else "SIN SUCURSAL"
        bucket = branch_by_sucursal.setdefault(
            code,
            {"label": code, "secondary": row.sucursal.nombre if row.sucursal_id else "Sin sucursal", "value": Decimal("0")},
        )
        bucket["value"] = _to_decimal(bucket["value"]) + qty
        if row.receta_id:
            branch_cost_est += qty * _to_decimal(getattr(row.receta, "costo_total_estimado_decimal", 0))
            branch_cost_covered += 1

    cedis_rows = list(
        MovimientoInventario.objects.filter(
            fecha__date__gte=date_from,
            fecha__date__lte=date_to,
            tipo=MovimientoInventario.TIPO_CONSUMO,
            referencia__startswith="MERMA|",
        ).select_related("insumo")
    )
    prev_cedis_units = _to_decimal(
        MovimientoInventario.objects.filter(
            fecha__date__gte=prev_start,
            fecha__date__lte=prev_end,
            tipo=MovimientoInventario.TIPO_CONSUMO,
            referencia__startswith="MERMA|",
        ).aggregate(total=Sum("cantidad")).get("total")
    )
    cedis_units = Decimal("0")
    cedis_cost_est = Decimal("0")
    cedis_cost_covered = 0
    cedis_by_insumo: dict[str, dict[str, object]] = {}
    cost_cache: dict[int, Decimal | None] = {}
    for row in cedis_rows:
        qty = _to_decimal(row.cantidad)
        cedis_units += qty
        label = row.insumo.nombre if row.insumo_id else "Sin insumo"
        bucket = cedis_by_insumo.setdefault(label, {"label": label, "secondary": "Merma CEDIS", "value": Decimal("0")})
        bucket["value"] = _to_decimal(bucket["value"]) + qty
        if row.insumo_id:
            if int(row.insumo_id) not in cost_cache:
                cost_cache[int(row.insumo_id)] = latest_costo_canonico(insumo_id=int(row.insumo_id))
            unit_cost = cost_cache[int(row.insumo_id)]
            if unit_cost is not None:
                cedis_cost_est += qty * _to_decimal(unit_cost)
                cedis_cost_covered += 1

    total_units = branch_units + cedis_units
    prev_total_units = prev_branch_units + prev_cedis_units
    comparison_label = "Base inicial"
    comparison_tone = "warning"
    comparison_detail = "Aún no hay una ventana previa equivalente para merma."
    if prev_total_units > 0:
        delta_pct = ((total_units - prev_total_units) / prev_total_units) * Decimal("100")
        comparison_label = "Sube" if delta_pct >= 0 else "Baja"
        comparison_tone = "warning" if delta_pct >= 0 else "success"
        comparison_detail = f"{abs(delta_pct):.1f}% vs la ventana previa ({prev_start.isoformat()} a {prev_end.isoformat()})"

    return {
        "period_label": f"{date_from.isoformat()} a {date_to.isoformat()}",
        "branch_available": branch_rows_qs.exists(),
        "branch_units": branch_units,
        "branch_cost_est": branch_cost_est,
        "branch_branch_count": len(branch_by_sucursal),
        "branch_cost_note": (
            f"Costo estimado en sucursal sobre {branch_cost_covered} capturas con receta mapeada."
            if branch_cost_covered
            else "Merma sucursal sin costo estimable: faltan recetas mapeadas."
        ),
        "cedis_units": cedis_units,
        "cedis_cost_est": cedis_cost_est,
        "cedis_row_count": len(cedis_rows),
        "cedis_cost_note": (
            f"Costo estimado en CEDIS sobre {cedis_cost_covered} movimientos con costo canónico."
            if cedis_cost_covered
            else "Merma CEDIS sin costo estimable: faltan costos canónicos para esos insumos."
        ),
        "comparison_label": comparison_label,
        "comparison_tone": comparison_tone,
        "comparison_detail": comparison_detail,
        "branch_rows": sorted(branch_by_sucursal.values(), key=lambda row: _to_decimal(row.get("value")), reverse=True)[:6],
        "cedis_rows": sorted(cedis_by_insumo.values(), key=lambda row: _to_decimal(row.get("value")), reverse=True)[:6],
    }


def _bi_forecast_summary(periodo_mes: str) -> dict[str, object]:
    try:
        year, month = periodo_mes.split("-")
        y = int(year)
        m = int(month)
    except Exception:
        today = timezone.localdate()
        y = today.year
        m = today.month
        periodo_mes = f"{y:04d}-{m:02d}"

    pron_rows = list(
        PronosticoVenta.objects.filter(periodo=periodo_mes)
        .values("receta_id", "receta__nombre")
        .annotate(total=Sum("cantidad"))
    )
    plan_rows = list(
        PlanProduccionItem.objects.filter(plan__fecha_produccion__year=y, plan__fecha_produccion__month=m)
        .values("receta_id", "receta__nombre")
        .annotate(total=Sum("cantidad"))
    )

    merged: dict[int, dict[str, object]] = {}
    for row in pron_rows:
        merged[int(row["receta_id"])] = {
            "label": row["receta__nombre"],
            "pronostico": _to_decimal(row["total"]),
            "plan": Decimal("0"),
        }
    for row in plan_rows:
        payload = merged.setdefault(
            int(row["receta_id"]),
            {"label": row["receta__nombre"], "pronostico": Decimal("0"), "plan": Decimal("0")},
        )
        payload["plan"] = _to_decimal(row["total"])

    top_rows: list[dict[str, object]] = []
    recipes_with_gap = 0
    total_forecast = Decimal("0")
    total_plan = Decimal("0")
    for payload in merged.values():
        pron = _to_decimal(payload["pronostico"])
        plan = _to_decimal(payload["plan"])
        delta = plan - pron
        total_forecast += pron
        total_plan += plan
        if delta != 0:
            recipes_with_gap += 1
            top_rows.append(
                {
                    "label": str(payload["label"]),
                    "secondary": f"Plan {plan:.1f} · Forecast {pron:.1f}",
                    "value": abs(delta),
                    "tone": "danger" if delta > 0 else "warning",
                }
            )
    top_rows.sort(key=lambda row: _to_decimal(row["value"]), reverse=True)
    delta_units = total_plan - total_forecast
    deviation_pct = None
    if total_forecast > 0:
        deviation_pct = (abs(delta_units) * Decimal("100")) / total_forecast

    if total_forecast <= 0 and total_plan <= 0:
        status = "Sin datos"
        tone = "warning"
        detail = "No hay forecast ni producción planificada para el periodo."
    elif total_forecast <= 0 and total_plan > 0:
        status = "Rojo"
        tone = "danger"
        detail = "Hay producción planificada sin forecast cargado en el periodo."
    elif deviation_pct is not None and deviation_pct <= Decimal("10"):
        status = "Verde"
        tone = "success"
        detail = "El plan del mes está alineado con el forecast cargado."
    elif deviation_pct is not None and deviation_pct <= Decimal("25"):
        status = "Amarillo"
        tone = "warning"
        detail = "Hay desviación relevante entre plan y forecast."
    else:
        status = "Rojo"
        tone = "danger"
        detail = "La desviación plan vs forecast ya exige ajuste ejecutivo."

    return {
        "period_label": periodo_mes,
        "forecast_units": total_forecast,
        "plan_units": total_plan,
        "delta_units": delta_units,
        "deviation_pct": deviation_pct,
        "status": status,
        "tone": tone,
        "detail": detail,
        "recipes_total": len(merged),
        "recipes_with_gap": recipes_with_gap,
        "top_rows": top_rows[:6],
        "basis_note": "Forecast mensual cargado en ERP. La exclusión automática de semanas atípicas aún no está parametrizada.",
    }


def _bi_operational_plan() -> PlanProduccion | None:
    today = timezone.localdate()
    plan_hoy = PlanProduccion.objects.filter(fecha_produccion=today).order_by("-creado_en").first()
    if plan_hoy:
        return plan_hoy
    return (
        PlanProduccion.objects.exclude(estado=PlanProduccion.ESTADO_CERRADO)
        .filter(fecha_produccion__gte=today)
        .order_by("fecha_produccion", "-creado_en")
        .first()
    )


def _bi_supply_watchlist(limit: int = 6) -> dict[str, object] | None:
    plan = _bi_operational_plan()
    if not plan:
        return None

    items = list(plan.items.select_related("receta")[:250])
    if not items:
        return None

    recipe_ids = [int(item.receta_id) for item in items if getattr(item, "receta_id", None)]
    lineas = list(
        LineaReceta.objects.filter(receta_id__in=recipe_ids, insumo_id__isnull=False)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .select_related("insumo", "receta", "insumo__unidad_base")
    )
    if not lineas:
        return None

    lineas_by_recipe: dict[int, list[LineaReceta]] = {}
    canonical_map: dict[int, Insumo] = {}
    canonical_ids: set[int] = set()
    for linea in lineas:
        if not linea.insumo:
            continue
        canonical_map[linea.id] = linea.insumo
        canonical_ids.add(linea.insumo.id)
        lineas_by_recipe.setdefault(int(linea.receta_id), []).append(linea)

    historico_map = {
        int(row["receta_id"]): Decimal(str(row["total"] or 0))
        for row in (
            VentaHistorica.objects.filter(
                receta_id__in=recipe_ids,
                fecha__gte=timezone.localdate() - timedelta(days=45),
            )
            .values("receta_id")
            .annotate(total=Sum("cantidad"))
        )
    }
    existencia_map = {
        int(existencia.insumo_id): existencia
        for existencia in ExistenciaInsumo.objects.filter(insumo_id__in=canonical_ids).select_related("insumo")
    }

    aggregated: dict[int, dict[str, object]] = {}
    for item in items:
        item_qty = Decimal(str(item.cantidad or 0))
        if item_qty <= 0:
            continue
        historico_units = historico_map.get(int(item.receta_id), Decimal("0"))
        for linea in lineas_by_recipe.get(int(item.receta_id), []):
            insumo = canonical_map.get(linea.id)
            if insumo is None:
                continue
            required_qty = Decimal(str(linea.cantidad or 0)) * item_qty
            if required_qty <= 0:
                continue
            bucket = aggregated.setdefault(
                insumo.id,
                {
                    "insumo": insumo,
                    "required_qty": Decimal("0"),
                    "historico_units": Decimal("0"),
                    "recipe_names": [],
                },
            )
            bucket["required_qty"] = Decimal(str(bucket["required_qty"])) + required_qty
            bucket["historico_units"] = Decimal(str(bucket["historico_units"])) + historico_units
            recipe_names = list(bucket["recipe_names"])
            if item.receta.nombre not in recipe_names:
                recipe_names.append(item.receta.nombre)
            bucket["recipe_names"] = recipe_names[:3]

    rows: list[dict[str, object]] = []
    for payload in aggregated.values():
        insumo = payload["insumo"]
        required_qty = Decimal(str(payload["required_qty"] or 0))
        historico_units = Decimal(str(payload["historico_units"] or 0))
        existencia = existencia_map.get(int(insumo.id))
        stock_actual = Decimal(str(getattr(existencia, "stock_actual", 0) or 0))
        shortage = max(required_qty - stock_actual, Decimal("0"))
        readiness = enterprise_readiness_profile(insumo)
        missing = list(readiness.get("missing") or [])
        missing_cost = latest_costo_canonico(insumo_id=insumo.id) is None
        if shortage <= 0 and not missing and not missing_cost:
            continue
        priority_score = (shortage * Decimal("100")) + (Decimal(str(len(missing))) * Decimal("50")) + historico_units
        if missing_cost:
            priority_score += Decimal("25")
        rows.append(
            {
                "insumo_nombre": insumo.nombre,
                "required_qty": required_qty,
                "stock_actual": stock_actual,
                "shortage": shortage,
                "historico_units": historico_units,
                "master_missing": missing,
                "missing_cost": missing_cost,
                "recipe_names": list(payload["recipe_names"] or []),
                "action_url": reverse("maestros:insumo_update", args=[insumo.id]),
                "action_label": "Asegurar artículo",
                "priority_score": priority_score,
            }
        )

    rows.sort(
        key=lambda item: (
            Decimal(str(item.get("priority_score") or 0)),
            Decimal(str(item.get("shortage") or 0)),
            Decimal(str(item.get("historico_units") or 0)),
        ),
        reverse=True,
    )
    if not rows:
        return None
    return {
        "plan_id": plan.id,
        "plan_nombre": plan.nombre,
        "plan_fecha": plan.fecha_produccion,
        "rows": rows[:limit],
        "url": f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}",
        "cta": "Abrir plan",
    }


def _bi_daily_decisions(
    *,
    daily_sales_snapshot: dict[str, object],
    branch_weekday_rows: list[dict[str, object]],
    product_weekday_rows: list[dict[str, object]],
    purchase_snapshot: dict[str, object],
    inventory_snapshot: dict[str, object],
    production_snapshot: dict[str, object],
    waste_summary: dict[str, object],
    forecast_summary: dict[str, object],
    supply_watchlist: dict[str, object] | None,
) -> list[dict[str, object]]:
    decisions: list[dict[str, object]] = []

    def push(priority: int, tone: str, title: str, detail: str, url: str, cta: str) -> None:
        decisions.append(
            {
                "priority": priority,
                "tone": tone,
                "title": title,
                "detail": detail,
                "url": url,
                "cta": cta,
            }
        )

    if int(inventory_snapshot.get("criticos") or 0) > 0:
        push(
            100,
            "danger",
            "Cerrar stock crítico",
            f"Hay {inventory_snapshot.get('criticos', 0)} insumos en crítico y {inventory_snapshot.get('bajo_reorden', 0)} bajo reorden con riesgo directo de surtido.",
            reverse("inventario:alertas"),
            "Abrir alertas",
        )

    if supply_watchlist and list(supply_watchlist.get("rows") or []):
        top_supply = list(supply_watchlist.get("rows") or [])[0]
        missing = list(top_supply.get("master_missing") or [])
        if Decimal(str(top_supply.get("shortage") or 0)) > 0 or missing or bool(top_supply.get("missing_cost")):
            faltante = ", ".join(missing) if missing else ("costo pendiente" if top_supply.get("missing_cost") else "stock corto")
            push(
                98,
                "danger" if Decimal(str(top_supply.get("shortage") or 0)) > 0 else "warning",
                "Asegurar insumo del plan",
                (
                    f"{top_supply.get('insumo_nombre', 'Artículo')} está frenando "
                    f"{supply_watchlist.get('plan_nombre', 'el plan operativo')}: brecha {Decimal(str(top_supply.get('shortage') or 0)):.2f} "
                    f"y faltante {faltante}."
                ),
                str(top_supply.get("action_url") or supply_watchlist.get("url") or reverse("inventario:alertas")),
                str(top_supply.get("action_label") or "Asegurar artículo"),
            )

    if int(purchase_snapshot.get("solicitudes_vencidas") or 0) > 0:
        push(
            95,
            "danger",
            "Liberar solicitudes vencidas",
            f"Hay {purchase_snapshot.get('solicitudes_vencidas', 0)} solicitudes vencidas que ya deberían estar resueltas con compras.",
            reverse("compras:solicitudes"),
            "Abrir compras",
        )

    if str(production_snapshot.get("plan_hoy_estado") or "") == "Sin plan":
        push(
            92,
            "danger",
            "Confirmar producción del día",
            "No hay plan operativo cargado para hoy. No conviene empujar compras o surtido sin ese documento.",
            reverse("recetas:plan_produccion"),
            "Abrir plan",
        )

    if str(forecast_summary.get("status") or "") in {"Rojo", "Amarillo"}:
        deviation_pct = forecast_summary.get("deviation_pct")
        deviation_label = f"{_to_decimal(deviation_pct):.1f}%" if deviation_pct is not None else "sin %"
        push(
            90,
            "danger" if str(forecast_summary.get("status")) == "Rojo" else "warning",
            "Ajustar forecast del periodo",
            f"Forecast {forecast_summary.get('period_label')} en {forecast_summary.get('status')} con desviación {deviation_label}.",
            reverse("recetas:plan_produccion"),
            "Abrir forecast",
        )

    if _to_decimal(waste_summary.get("branch_units")) + _to_decimal(waste_summary.get("cedis_units")) > 0:
        push(
            88,
            "warning" if str(waste_summary.get("comparison_label") or "") == "Sube" else "success",
            "Atender merma operativa",
            (
                f"Merma sucursal {_to_decimal(waste_summary.get('branch_units')):.1f} u "
                f"y CEDIS {_to_decimal(waste_summary.get('cedis_units')):.1f} u. "
                f"{waste_summary.get('comparison_detail')}"
            ),
            reverse("control:discrepancias"),
            "Abrir merma",
        )

    if str(daily_sales_snapshot.get("comparison_label") or "") == "Abajo":
        push(
            80,
            str(daily_sales_snapshot.get("comparison_tone") or "warning"),
            "Revisar caída del corte reciente",
            str(daily_sales_snapshot.get("comparison_detail") or "La venta del corte reciente viene abajo contra la referencia inmediata."),
            reverse("reportes:bi"),
            "Abrir BI",
        )

    top_branch = branch_weekday_rows[0] if branch_weekday_rows else None
    if top_branch:
        push(
            60,
            str(top_branch.get("tone") or "warning"),
            f"Revisar sucursal {top_branch.get('branch_name', 'Sucursal')}",
            str(top_branch.get("detail") or ""),
            reverse("reportes:bi"),
            "Ver BI",
        )

    top_product = product_weekday_rows[0] if product_weekday_rows else None
    if top_product:
        push(
            58,
            str(top_product.get("tone") or "warning"),
            f"Revisar producto {top_product.get('recipe_name', 'Producto')}",
            str(top_product.get("detail") or ""),
            reverse("reportes:bi"),
            "Ver BI",
        )

    if not decisions:
        push(
            10,
            "success",
            "Operación sin alertas dominantes",
            "El corte reciente no muestra excepciones críticas en ventas, stock, compras o producción.",
            reverse("reportes:bi"),
            "Actualizar BI",
        )

    decisions.sort(key=lambda item: int(item.get("priority") or 0), reverse=True)
    return decisions[:5]


def _reportes_enterprise_chain(
    *,
    focus: str,
    open_count: int,
    blocked_count: int,
    date_from: str | None = None,
    date_to: str | None = None,
    nivel: str | None = None,
) -> list[dict[str, object]]:
    def _enrich(items: list[dict[str, object]], owner: str) -> list[dict[str, object]]:
        total = len(items)
        enriched: list[dict[str, object]] = []
        for index, item in enumerate(items):
            dependency = items[index - 1] if index > 0 else None
            completion = int(round(((index + 1) / total) * 100)) if total else 0
            enriched.append(
                {
                    **item,
                    "owner": owner,
                    "next_step": item.get("cta", "Abrir"),
                    "completion": completion,
                    "depends_on": dependency.get("title") if dependency else "Inicio del flujo",
                    "dependency_status": dependency.get("status") if dependency else "Listo",
                }
            )
        return enriched

    if focus == "costeo":
        return _enrich([
            {
                "step": "01",
                "title": "Maestro ERP",
                "count": blocked_count,
                "status": "Bloqueos" if blocked_count else "Listo",
                "tone": "warning" if blocked_count else "success",
                "detail": "Artículos con faltantes maestros o costo incompleto dentro del costeo.",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
                "cta": "Abrir maestro",
            },
            {
                "step": "02",
                "title": "BOM costeado",
                "count": open_count,
                "status": "Con costo" if open_count else "Sin cobertura",
                "tone": "success" if open_count else "warning",
                "detail": "Recetas con cobertura suficiente para costeo consolidado.",
                "url": reverse("reportes:costo_receta"),
                "cta": "Ver costeo",
            },
            {
                "step": "03",
                "title": "Precio sugerido",
                "count": open_count,
                "status": "Analítico",
                "tone": "primary",
                "detail": "Precio objetivo derivado de costo y margen para lectura ejecutiva.",
                "url": reverse("reportes:costo_receta"),
                "cta": "Revisar precios",
            },
        ], "Costeo")
    if focus == "consumo":
        return _enrich([
            {
                "step": "01",
                "title": "Maestro ERP",
                "count": blocked_count,
                "status": "Bloqueos" if blocked_count else "Listo",
                "tone": "warning" if blocked_count else "success",
                "detail": "Artículos con uso operativo pendiente dentro del periodo consultado.",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
                "cta": "Abrir maestro",
            },
            {
                "step": "02",
                "title": "Consumo consolidado",
                "count": open_count,
                "status": "Periodo activo",
                "tone": "primary",
                "detail": f"Movimientos entre {date_from} y {date_to}.",
                "url": reverse("reportes:consumo") + f"?date_from={date_from}&date_to={date_to}&tipo=ALL",
                "cta": "Ver periodo",
            },
            {
                "step": "03",
                "title": "Costo y reposición",
                "count": blocked_count,
                "status": "Por revisar" if blocked_count else "Alineado",
                "tone": "warning" if blocked_count else "success",
                "detail": "Consumibles con impacto en compras o reabasto por maestro incompleto.",
                "url": reverse("reportes:faltantes"),
                "cta": "Ver reabasto",
            },
        ], "Inventario y Compras")
    if focus == "faltantes":
        return _enrich([
            {
                "step": "01",
                "title": "Maestro ERP",
                "count": blocked_count,
                "status": "Bloqueos" if blocked_count else "Listo",
                "tone": "warning" if blocked_count else "success",
                "detail": "Artículos debajo de mínimo con ficha maestra aún incompleta.",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
                "cta": "Abrir maestro",
            },
            {
                "step": "02",
                "title": "Stock crítico",
                "count": open_count,
                "status": "Nivel " + (nivel or "alerta"),
                "tone": "danger" if (nivel or "alerta") in {"critico", "alerta"} else "warning",
                "detail": "Existencias analizadas contra punto de reorden y sugerencia de compra.",
                "url": reverse("reportes:faltantes") + f"?nivel={nivel or 'alerta'}",
                "cta": "Ver alertas",
            },
            {
                "step": "03",
                "title": "Abastecimiento",
                "count": open_count,
                "status": "Accionable",
                "tone": "primary",
                "detail": "Usa este tablero para mover compra, reorden o conciliación de almacén.",
                "url": reverse("compras:solicitudes"),
                "cta": "Ir a compras",
            },
        ], "Abastecimiento")
    return _enrich([
        {
            "step": "01",
            "title": "Fuente ejecutiva",
            "count": open_count,
            "status": "Mensual",
            "tone": "primary",
            "detail": "Serie consolidada de ventas, compras, nómina y logística.",
            "url": reverse("reportes:bi"),
            "cta": "Abrir BI",
        },
        {
            "step": "02",
            "title": "Disciplina maestra",
            "count": blocked_count,
            "status": "Bloqueos" if blocked_count else "Listo",
            "tone": "warning" if blocked_count else "success",
            "detail": "El maestro incompleto afecta consistencia de indicadores y costos.",
            "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
            "cta": "Corregir maestro",
        },
        {
            "step": "03",
            "title": "Acción directiva",
            "count": open_count,
            "status": "Cockpit activo",
            "tone": "primary",
            "detail": "Usa el tablero para seguimiento financiero, comercial y operativo.",
            "url": reverse("dashboard"),
            "cta": "Ir al dashboard",
        },
    ], "Dirección General")


def _reportes_document_stage_rows(
    *,
    focus: str,
    open_count: int,
    blocked_count: int,
    total_count: int,
    date_from: str | None = None,
    date_to: str | None = None,
    nivel: str | None = None,
) -> list[dict[str, object]]:
    if focus == "costeo":
        rows = [
            {
                "label": "Maestro ERP",
                "open": blocked_count,
                "closed": max(total_count - blocked_count, 0),
                "detail": "Recetas soportadas por artículos listos y costos consistentes.",
                "url": reverse("maestros:insumo_list"),
                "owner": "Maestros / Costeo",
                "next_step": "Cerrar faltantes maestros y costo antes del análisis final.",
            },
            {
                "label": "Cobertura de costo",
                "open": open_count,
                "closed": max(total_count - open_count, 0),
                "detail": "Recetas con costo consolidado frente a recetas sin cobertura completa.",
                "url": reverse("reportes:costo_receta"),
                "owner": "Costeo / Finanzas",
                "next_step": "Completar cobertura de costo y validar consolidado por receta.",
            },
            {
                "label": "Lectura ejecutiva",
                "open": open_count,
                "closed": 0,
                "detail": "Precio sugerido y margen objetivo listos para decisión gerencial.",
                "url": reverse("dashboard"),
                "owner": "Dirección General",
                "next_step": "Revisar margen objetivo y tomar decisión comercial.",
            },
        ]
        for row in rows:
            total = int(row["open"]) + int(row["closed"])
            row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
        return rows
    if focus == "consumo":
        rows = [
            {
                "label": "Maestro ERP",
                "open": blocked_count,
                "closed": max(total_count - blocked_count, 0),
                "detail": "Artículos con ficha lista para análisis.",
                "url": reverse("maestros:insumo_list"),
                "owner": "Maestros / Inventario",
                "next_step": "Completar fichas maestras antes de consolidar consumo.",
            },
            {
                "label": "Movimiento consolidado",
                "open": open_count,
                "closed": 0,
                "detail": f"Consumo entre {date_from} y {date_to}.",
                "url": reverse("reportes:consumo") + f"?date_from={date_from}&date_to={date_to}&tipo=ALL",
                "owner": "Inventario / BI",
                "next_step": "Validar consolidado del periodo y revisar desvíos relevantes.",
            },
            {
                "label": "Acción de reabasto",
                "open": blocked_count,
                "closed": max(total_count - blocked_count, 0),
                "detail": "Cruce de consumo contra gobierno maestro.",
                "url": reverse("reportes:faltantes"),
                "owner": "Compras / Abastecimiento",
                "next_step": "Convertir hallazgos en acción de reabasto y seguimiento.",
            },
        ]
        for row in rows:
            total = int(row["open"]) + int(row["closed"])
            row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
        return rows
    if focus == "faltantes":
        rows = [
            {
                "label": "Maestro ERP",
                "open": blocked_count,
                "closed": max(total_count - blocked_count, 0),
                "detail": "Artículos listos vs bloqueados para compra.",
                "url": reverse("maestros:insumo_list"),
                "owner": "Maestros / Compras",
                "next_step": "Cerrar faltantes maestros que bloquean abastecimiento.",
            },
            {
                "label": "Existencia crítica",
                "open": open_count,
                "closed": max(total_count - open_count, 0),
                "detail": f"Filtro activo: {nivel or 'alerta'}.",
                "url": reverse("reportes:faltantes") + f"?nivel={nivel or 'alerta'}",
                "owner": "Inventario / Abastecimiento",
                "next_step": "Confirmar criticidad y priorizar reabasto según nivel operativo.",
            },
            {
                "label": "Solicitud de compra",
                "open": open_count,
                "closed": 0,
                "detail": "Sugerencias de compra y reorden accionables.",
                "url": reverse("compras:solicitudes"),
                "owner": "Compras",
                "next_step": "Emitir solicitud u orden de compra según urgencia.",
            },
        ]
        for row in rows:
            total = int(row["open"]) + int(row["closed"])
            row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
        return rows
    rows = [
        {
            "label": "Serie ejecutiva",
            "open": open_count,
            "closed": 0,
            "detail": "Meses consolidados dentro del tablero BI.",
            "url": reverse("reportes:bi"),
            "owner": "BI / Dirección",
            "next_step": "Mantener serie ejecutiva lista para lectura gerencial.",
        },
        {
            "label": "Maestro ERP",
            "open": blocked_count,
            "closed": max(total_count - blocked_count, 0),
            "detail": "Artículos listos que ya soportan análisis consistente.",
            "url": reverse("maestros:insumo_list"),
            "owner": "Maestros",
            "next_step": "Cerrar brechas de maestro para sostener indicadores limpios.",
        },
        {
            "label": "Seguimiento directivo",
            "open": open_count,
            "closed": 0,
            "detail": "Lectura mensual para decisión operativa.",
            "url": reverse("dashboard"),
            "owner": "Dirección General",
            "next_step": "Usar el cockpit para seguimiento y decisiones ejecutivas.",
        },
    ]
    for row in rows:
        total = int(row["open"]) + int(row["closed"])
        row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
    return rows


def _reportes_governance_rows(
    rows: list[dict[str, object]],
    owner_default: str = "Reportes / Operación",
) -> list[dict[str, object]]:
    governance_rows: list[dict[str, object]] = []
    for row in rows:
        governance_rows.append(
            {
                "front": row.get("label", "Reportes"),
                "owner": row.get("owner") or owner_default,
                "blockers": int(row.get("open") or 0),
                "completion": int(row.get("completion") or 0),
                "detail": row.get("detail", ""),
                "next_step": row.get("next_step") or "Revisar frente operativo",
                "url": row.get("url") or reverse("reportes:bi"),
                "cta": row.get("cta") or "Abrir",
            }
        )
    return governance_rows


def _reportes_operational_health_cards(
    *,
    focus: str,
    open_count: int,
    blocked_count: int,
    total_count: int,
) -> list[dict[str, object]]:
    ready_count = max(total_count - blocked_count, 0)
    if focus == "costeo":
        return [
            {
                "label": "Recetas con costo",
                "value": open_count,
                "tone": "success" if open_count else "warning",
                "detail": "Recetas con costo consolidado y lectura válida para el periodo.",
            },
            {
                "label": "Bloqueos maestros",
                "value": blocked_count,
                "tone": "warning" if blocked_count else "success",
                "detail": "Faltantes de maestro o costo que degradan el cálculo.",
            },
            {
                "label": "Recetas listas ERP",
                "value": ready_count,
                "tone": "success",
                "detail": "Recetas con estructura y artículos listos para costeo consistente.",
            },
        ]
    if focus == "consumo":
        return [
            {
                "label": "Movimientos consolidados",
                "value": open_count,
                "tone": "primary",
                "detail": "Movimientos dentro del rango activo del reporte.",
            },
            {
                "label": "Maestro bloqueado",
                "value": blocked_count,
                "tone": "warning" if blocked_count else "success",
                "detail": "Artículos con faltantes maestros que afectan costeo o análisis.",
            },
            {
                "label": "Artículos listos ERP",
                "value": ready_count,
                "tone": "success",
                "detail": "Artículos listos para consumo, costo y reposición.",
            },
        ]
    if focus == "faltantes":
        return [
            {
                "label": "Alertas activas",
                "value": open_count,
                "tone": "danger" if open_count else "success",
                "detail": "Artículos por debajo del nivel esperado.",
            },
            {
                "label": "Bloqueos maestros",
                "value": blocked_count,
                "tone": "warning" if blocked_count else "success",
                "detail": "Faltantes de datos maestros que frenan compra o reorden.",
            },
            {
                "label": "Referencias listas",
                "value": ready_count,
                "tone": "success",
                "detail": "Artículos listos para solicitud o reabasto.",
            },
        ]
    return [
        {
            "label": "Meses consolidados",
            "value": open_count,
            "tone": "primary",
            "detail": "Horizonte ejecutivo activo del tablero BI.",
        },
        {
            "label": "Brechas maestras",
            "value": blocked_count,
            "tone": "warning" if blocked_count else "success",
            "detail": "Faltantes del maestro que degradan consistencia de KPIs.",
        },
        {
            "label": "Cobertura ejecutiva",
            "value": total_count,
            "tone": "success",
            "detail": "Indicadores consolidados listos para seguimiento directivo.",
        },
    ]


def _reportes_maturity_summary(*, chain: list[dict], default_url: str) -> dict:
    total_steps = len(chain)
    completed_steps = sum(1 for item in chain if item.get("tone") == "success")
    attention_steps = max(total_steps - completed_steps, 0)
    coverage_pct = int(round((completed_steps / total_steps) * 100)) if total_steps else 0
    next_priority = next((item for item in chain if item.get("tone") != "success"), None)
    return {
        "completed_steps": completed_steps,
        "attention_steps": attention_steps,
        "coverage_pct": coverage_pct,
        "next_priority_title": next_priority.get("title", "Cadena documental estabilizada") if next_priority else "Cadena documental estabilizada",
        "next_priority_detail": next_priority.get("detail", "El tablero ya está alineado con maestro, consumo y acción operativa.") if next_priority else "El tablero ya está alineado con maestro, consumo y acción operativa.",
        "next_priority_url": next_priority.get("url", default_url) if next_priority else default_url,
        "next_priority_cta": next_priority.get("cta", "Abrir tablero") if next_priority else "Abrir tablero",
    }


def _reportes_critical_path_rows(chain: list[dict[str, object]]) -> list[dict[str, object]]:
    severity_order = {"danger": 0, "warning": 1, "success": 2, "primary": 3}
    ranked = sorted(
        chain,
        key=lambda item: (
            severity_order.get(str(item.get("tone") or "warning"), 9),
            -int(item.get("count") or 0),
            int(item.get("completion") or 0),
        ),
    )
    rows: list[dict[str, object]] = []
    for index, item in enumerate(ranked[:4], start=1):
        rows.append(
            {
                "rank": f"R{index}",
                "title": item.get("title", "Tramo de reportes"),
                "owner": item.get("owner", "Reportes / Operación"),
                "status": item.get("status", "Sin estado"),
                "tone": item.get("tone", "warning"),
                "count": int(item.get("count") or 0),
                "completion": int(item.get("completion") or 0),
                "depends_on": item.get("depends_on", "Inicio del flujo"),
                "dependency_status": item.get("dependency_status", "Sin dependencia registrada"),
                "detail": item.get("detail", ""),
                "next_step": item.get("next_step", "Continuar lectura ejecutiva"),
                "url": item.get("url", reverse("reportes:bi")),
                "cta": item.get("cta", "Abrir"),
            }
        )
    return rows


def _reportes_executive_radar_rows(
    governance_rows: list[dict[str, object]],
    *,
    default_owner: str = "Reportes / Operación",
    fallback_url: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in governance_rows[:4]:
        completion = int(row.get("completion") or 0)
        blockers = int(row.get("blockers") or 0)
        if blockers <= 0 and completion >= 90:
            tone = "success"
            status = "Controlado"
            dominant_blocker = "Sin bloqueo activo"
        elif completion >= 50:
            tone = "warning"
            status = "En seguimiento"
            dominant_blocker = row.get("detail", "") or "Brecha operativa en seguimiento"
        else:
            tone = "danger"
            status = "Con bloqueo"
            dominant_blocker = row.get("detail", "") or "Bloqueo operativo abierto"
        rows.append(
            {
                "phase": row.get("front", "Frente de reportes"),
                "owner": row.get("owner") or default_owner,
                "status": status,
                "tone": tone,
                "blockers": blockers,
                "progress_pct": completion,
                "dominant_blocker": dominant_blocker,
                "depends_on": row.get("front", "Origen del módulo"),
                "dependency_status": row.get("next_step", "Sin dependencia registrada"),
                "next_step": row.get("next_step", "Abrir frente"),
                "url": row.get("url", fallback_url),
                "cta": row.get("cta", "Abrir"),
            }
        )
    return rows


def _reportes_command_center(*, governance_rows: list[dict[str, object]], maturity_summary: dict[str, object], default_owner: str) -> dict[str, object]:
    blockers = sum(int(row.get("blockers") or 0) for row in governance_rows)
    primary_row = max(governance_rows, key=lambda row: int(row.get("blockers") or 0), default={}) if governance_rows else {}
    tone = "success" if blockers == 0 else ("warning" if blockers <= 3 else "danger")
    status = "Listo para operar" if blockers == 0 else ("En atención" if blockers <= 3 else "Crítico")
    return {
        "owner": primary_row.get("owner") or default_owner,
        "status": status,
        "tone": tone,
        "blockers": blockers,
        "next_step": maturity_summary.get("next_priority_detail") or "Continuar cierre documental del módulo.",
        "cta": maturity_summary.get("next_priority_cta") or primary_row.get("cta") or "Abrir",
        "url": maturity_summary.get("next_priority_url") or primary_row.get("url") or reverse("reportes:bi"),
    }


def _reportes_handoff_map(
    *,
    focus: str,
    blocked_count: int,
    open_count: int,
    total_count: int,
    default_url: str,
) -> list[dict]:
    if focus == "costeo":
        return [
            {
                "label": "Maestro -> BOM",
                "detail": "El artículo debe quedar completo y con costo vigente antes de entrar al costeo.",
                "count": blocked_count,
                "tone": "success" if blocked_count == 0 else "warning",
                "status": "Controlado" if blocked_count == 0 else "Bloqueos maestros",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
                "cta": "Abrir maestro",
                "owner": "Maestros / Costeo",
                "depends_on": "Artículo completo + costo vigente",
                "exit_criteria": "El maestro ya sostiene BOM y costo sin brechas activas.",
                "next_step": "Cerrar brechas maestras antes del costeo.",
                "completion": 100 if blocked_count == 0 else 55,
            },
            {
                "label": "BOM -> Costeo",
                "detail": "La estructura de la receta debe cerrar rendimiento y cobertura de componentes.",
                "count": open_count,
                "tone": "success" if open_count else "warning",
                "status": "Cobertura activa" if open_count else "Sin recetas listas",
                "url": default_url,
                "cta": "Ver costeo",
                "owner": "Recetas / Costeo",
                "depends_on": "BOM trazable + recetas listas",
                "exit_criteria": "La cobertura de componentes ya permite costeo consolidado por receta.",
                "next_step": "Completar cobertura de recetas listas.",
                "completion": 100 if open_count else 35,
            },
            {
                "label": "Costeo -> Precio",
                "detail": "El costo consolidado se transforma en precio sugerido para lectura directiva.",
                "count": total_count,
                "tone": "primary",
                "status": "Analítico" if total_count else "Sin base",
                "url": default_url,
                "cta": "Revisar precios",
                "owner": "Costeo / Dirección",
                "depends_on": "Costo consolidado",
                "exit_criteria": "El precio sugerido ya queda disponible para lectura ejecutiva y decisión comercial.",
                "next_step": "Revisar margen y precio sugerido.",
                "completion": 100 if total_count else 20,
            },
        ]
    if focus == "consumo":
        return [
            {
                "label": "Maestro -> Consumo",
                "detail": "El maestro ERP debe estar completo antes de usar el análisis de consumo para costeo y compra.",
                "count": blocked_count,
                "tone": "success" if blocked_count == 0 else "warning",
                "status": "Controlado" if blocked_count == 0 else "Bloqueos maestros",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
                "cta": "Abrir maestro",
                "owner": "Maestros / Inventario",
                "depends_on": "Artículo completo",
                "exit_criteria": "El consumo ya puede leerse sin distorsiones del maestro.",
                "next_step": "Cerrar faltantes de maestro antes del análisis.",
                "completion": 100 if blocked_count == 0 else 55,
            },
            {
                "label": "Consumo -> Reabasto",
                "detail": "Los movimientos consolidados deben alimentar faltantes, compras y reorden.",
                "count": open_count,
                "tone": "primary",
                "status": "Periodo activo" if open_count else "Sin movimientos",
                "url": default_url,
                "cta": "Ver consumo",
                "owner": "Inventario / Planeación",
                "depends_on": "Movimientos consolidados",
                "exit_criteria": "El periodo ya alimenta reposición y lectura de desviaciones.",
                "next_step": "Validar cobertura del periodo.",
                "completion": 100 if open_count else 35,
            },
            {
                "label": "Reabasto -> Compras",
                "detail": "Los artículos listos deben escalar a compra sin fricción documental.",
                "count": max(total_count - blocked_count, 0),
                "tone": "success" if total_count else "warning",
                "status": "Artículos listos" if total_count else "Sin base operativa",
                "url": reverse("compras:solicitudes"),
                "cta": "Ir a compras",
                "owner": "Compras",
                "depends_on": "Consumo validado + maestro estable",
                "exit_criteria": "El reabasto ya puede escalar a documentos de compra sin bloqueo.",
                "next_step": "Documentar abastecimiento.",
                "completion": 100 if total_count else 30,
            },
        ]
    if focus == "faltantes":
        return [
            {
                "label": "Maestro -> Stock",
                "detail": "Sin ficha maestra completa, el reorden queda bloqueado aunque exista alerta de stock.",
                "count": blocked_count,
                "tone": "success" if blocked_count == 0 else "warning",
                "status": "Controlado" if blocked_count == 0 else "Bloqueos maestros",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
                "cta": "Abrir maestro",
                "owner": "Maestros / Inventario",
                "depends_on": "Ficha maestra lista",
                "exit_criteria": "Las alertas ya se apoyan en artículos listos y trazables.",
                "next_step": "Regularizar artículos bajo alerta.",
                "completion": 100 if blocked_count == 0 else 55,
            },
            {
                "label": "Stock -> Reorden",
                "detail": "Las alertas deben convertirse en sugerencia y solicitud accionable.",
                "count": open_count,
                "tone": "danger" if open_count else "success",
                "status": "Alertas activas" if open_count else "Sin alertas",
                "url": default_url,
                "cta": "Ver faltantes",
                "owner": "Inventario / Compras",
                "depends_on": "Existencias + parámetros de reorden",
                "exit_criteria": "Los faltantes ya quedan listos para acción documental.",
                "next_step": "Disparar reorden o compra.",
                "completion": 100 if open_count == 0 else 35,
            },
            {
                "label": "Reorden -> Compras",
                "detail": "El abastecimiento debe pasar a compras con datos consistentes del artículo.",
                "count": max(total_count - blocked_count, 0),
                "tone": "success" if total_count else "warning",
                "status": "Listo para compra" if total_count else "Sin artículos críticos",
                "url": reverse("compras:solicitudes"),
                "cta": "Ir a compras",
                "owner": "Compras",
                "depends_on": "Alerta consolidada",
                "exit_criteria": "Las alertas ya se convierten en abastecimiento o recepción.",
                "next_step": "Emitir solicitud u orden.",
                "completion": 100 if total_count else 25,
            },
        ]
    return [
        {
            "label": "Fuente -> KPIs",
            "detail": "Las series ejecutivas deben conservar consistencia antes de llegar al dashboard directivo.",
            "count": open_count,
            "tone": "primary",
            "status": "Serie activa" if open_count else "Sin serie",
            "url": default_url,
            "cta": "Abrir BI",
            "owner": "BI / Dirección",
            "depends_on": "Fuentes consolidadas",
            "exit_criteria": "La serie ejecutiva ya queda consolidada para lectura directiva.",
            "next_step": "Validar tablero BI mensual.",
            "completion": 100 if open_count else 25,
        },
        {
            "label": "Maestro -> BI",
            "detail": "Las brechas del maestro degradan consistencia de indicadores y lectura ejecutiva.",
            "count": blocked_count,
            "tone": "success" if blocked_count == 0 else "warning",
            "status": "Controlado" if blocked_count == 0 else "Bloqueos maestros",
            "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
            "cta": "Corregir maestro",
            "owner": "Maestros / Dirección",
            "depends_on": "Catálogo ERP estable",
            "exit_criteria": "Los indicadores ya corren sobre datos maestros consistentes.",
            "next_step": "Cerrar brechas del maestro.",
            "completion": 100 if blocked_count == 0 else 55,
        },
        {
            "label": "BI -> Dirección",
            "detail": "La lectura consolidada debe cerrar el ciclo de decisión directiva.",
            "count": total_count,
            "tone": "success" if total_count else "warning",
            "status": "Cockpit activo" if total_count else "Sin indicadores",
            "url": reverse("dashboard"),
            "cta": "Ir al dashboard",
            "owner": "Dirección General",
            "depends_on": "BI + disciplina maestra",
            "exit_criteria": "El tablero ya soporta decisiones ejecutivas con datos confiables.",
            "next_step": "Tomar decisión ejecutiva sobre el periodo.",
            "completion": 100 if total_count else 25,
        },
    ]


def _reportes_release_gate_rows(
    *,
    focus: str,
    open_count: int,
    blocked_count: int,
    total_count: int,
    date_from: str | None = None,
    date_to: str | None = None,
    nivel: str | None = None,
) -> list[dict[str, object]]:
    ready_count = max(total_count - blocked_count, 0)
    if focus == "costeo":
        return [
            {
                "label": "Maestro ERP completo",
                "open": blocked_count,
                "closed": ready_count,
                "detail": "Artículos con costo y ficha maestra suficientes para liberar costeo.",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
            },
            {
                "label": "Cobertura de receta",
                "open": max(total_count - open_count, 0),
                "closed": open_count,
                "detail": "Recetas con costo consolidado y cobertura suficiente de componentes.",
                "url": reverse("reportes:costo_receta"),
            },
            {
                "label": "Lectura ejecutiva",
                "open": 0,
                "closed": total_count,
                "detail": "Precio sugerido y margen disponibles para lectura directiva.",
                "url": reverse("dashboard"),
            },
        ]
    if focus == "consumo":
        return [
            {
                "label": "Maestro ERP completo",
                "open": blocked_count,
                "closed": ready_count,
                "detail": "Artículos listos para análisis de consumo y costo.",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
            },
            {
                "label": "Periodo consolidado",
                "open": 0 if open_count else 1,
                "closed": open_count,
                "detail": f"Consumo consolidado entre {date_from} y {date_to}.",
                "url": reverse("reportes:consumo") + f"?date_from={date_from}&date_to={date_to}&tipo=ALL",
            },
            {
                "label": "Acción de reabasto",
                "open": blocked_count,
                "closed": ready_count,
                "detail": "Consumo ya listo para escalar a faltantes, reorden y compras.",
                "url": reverse("reportes:faltantes"),
            },
        ]
    if focus == "faltantes":
        return [
            {
                "label": "Maestro ERP completo",
                "open": blocked_count,
                "closed": ready_count,
                "detail": "Artículos con ficha suficiente para compra o reorden.",
                "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
            },
            {
                "label": "Alertas accionables",
                "open": 0 if open_count else 1,
                "closed": open_count,
                "detail": f"Nivel activo: {nivel or 'alerta'}.",
                "url": reverse("reportes:faltantes") + f"?nivel={nivel or 'alerta'}",
            },
            {
                "label": "Escalamiento a compras",
                "open": blocked_count,
                "closed": ready_count,
                "detail": "Referencias listas para entrar al flujo documental de compras.",
                "url": reverse("compras:solicitudes"),
            },
        ]
    return [
        {
            "label": "Serie ejecutiva consolidada",
            "open": 0 if open_count else 1,
            "closed": open_count,
            "detail": "Horizonte mensual listo para lectura directiva.",
            "url": reverse("reportes:bi"),
        },
        {
            "label": "Disciplina maestra",
            "open": blocked_count,
            "closed": ready_count,
            "detail": "El maestro no debe degradar consistencia del cockpit ejecutivo.",
            "url": reverse("maestros:insumo_list") + "?erp_status=incompleto",
        },
        {
            "label": "Cockpit directivo",
            "open": 0,
            "closed": total_count,
            "detail": "Indicadores disponibles para seguimiento DG.",
            "url": reverse("dashboard"),
        },
    ]


def _reportes_release_gate_completion(rows: list[dict[str, object]]) -> dict[str, int]:
    total = sum(int(row.get("open", 0)) + int(row.get("closed", 0)) for row in rows)
    closed = sum(int(row.get("closed", 0)) for row in rows)
    pct = int(round((closed / total) * 100)) if total else 0
    return {"closed": closed, "total": total, "pct": pct}


def _enterprise_usage_label(insumo: Insumo) -> str:
    if insumo.tipo_item == Insumo.TIPO_INTERNO:
        return "Producción interna"
    if insumo.tipo_item == Insumo.TIPO_EMPAQUE:
        return "Empaque final"
    return "Compra directa"


def _enterprise_missing_field(missing: list[str]) -> str | None:
    primary_missing = missing[0] if missing else ""
    return (
        "unidad"
        if primary_missing == "unidad base"
        else "proveedor"
        if primary_missing == "proveedor principal"
        else "categoria"
        if primary_missing == "categoría"
        else "codigo_point"
        if primary_missing == "código Point"
        else None
    )


def _build_report_enterprise_meta(insumo: Insumo) -> dict[str, object]:
    profile = enterprise_readiness_profile(insumo)
    missing_field = _enterprise_missing_field(profile["missing"])
    list_query = {"insumo_id": insumo.id, "usage_scope": "reports"}
    if missing_field:
        list_query["missing_field"] = missing_field
    return {
        "enterprise_status": profile["readiness_label"],
        "enterprise_missing": profile["missing"],
        "enterprise_usage_label": _enterprise_usage_label(insumo),
        "enterprise_edit_url": reverse("maestros:insumo_update", args=[insumo.id]),
        "enterprise_list_url": reverse("maestros:insumo_list") + f"?{urlencode(list_query)}",
    }


def _consumo_rows(date_from: str, date_to: str, tipo: str):
    movimientos = MovimientoInventario.objects.select_related("insumo").filter(
        fecha__date__gte=date_from,
        fecha__date__lte=date_to,
    )
    if tipo != "ALL":
        movimientos = movimientos.filter(tipo=tipo)
    member_to_row, _ = _canonical_catalog_maps()
    grouped = {}
    for movimiento in movimientos:
        row = member_to_row.get(movimiento.insumo_id)
        if not row:
            continue
        canonical = row["canonical"]
        bucket = grouped.setdefault(
            canonical.id,
            {
                "insumo_id": canonical.id,
                "insumo__nombre": canonical.nombre,
                "insumo": canonical,
                "cantidad_total": Decimal("0"),
                "ultima_fecha": None,
                "canonical_variant_count": row["variant_count"],
            },
        )
        bucket["cantidad_total"] += _to_decimal(movimiento.cantidad, "0")
        if bucket["ultima_fecha"] is None or (movimiento.fecha and movimiento.fecha > bucket["ultima_fecha"]):
            bucket["ultima_fecha"] = movimiento.fecha

    resumen = sorted(
        grouped.values(),
        key=lambda item: (-_to_decimal(item["cantidad_total"], "0"), item["insumo__nombre"].lower()),
    )
    for item in resumen:
        item.update(_build_report_enterprise_meta(item["insumo"]))
    return movimientos, resumen


def _export_consumo_csv(rows, date_from: str, date_to: str, tipo: str) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="reporte_consumo_{date_from}_{date_to}.csv"'
    writer = csv.writer(response)
    writer.writerow(["Insumo", "Cantidad total", "Ultimo movimiento", "Filtro tipo", "Desde", "Hasta"])
    for row in rows:
        writer.writerow(
            [
                row["insumo__nombre"],
                row["cantidad_total"],
                row["ultima_fecha"].strftime("%Y-%m-%d %H:%M") if row["ultima_fecha"] else "",
                tipo,
                date_from,
                date_to,
            ]
        )
    return response


def _export_consumo_xlsx(rows, date_from: str, date_to: str, tipo: str) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Consumo"
    ws.append(["Insumo", "Cantidad total", "Ultimo movimiento", "Filtro tipo", "Desde", "Hasta"])
    for row in rows:
        ws.append(
            [
                row["insumo__nombre"],
                float(row["cantidad_total"] or 0),
                row["ultima_fecha"].strftime("%Y-%m-%d %H:%M") if row["ultima_fecha"] else "",
                tipo,
                date_from,
                date_to,
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="reporte_consumo_{date_from}_{date_to}.xlsx"'
    return response


def _faltantes_rows(nivel: str):
    member_to_row, canonical_by_id = _canonical_catalog_maps()
    raw_existencias = list(ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base").order_by("insumo__nombre")[:1000])
    grouped = {}
    for existencia in raw_existencias:
        row = member_to_row.get(existencia.insumo_id)
        if not row:
            continue
        canonical = row["canonical"]
        bucket = grouped.get(canonical.id)
        if bucket is None:
            bucket = SimpleNamespace(
                insumo=canonical,
                stock_actual=Decimal("0"),
                stock_minimo=_to_decimal(existencia.stock_minimo, "0"),
                stock_maximo=_to_decimal(existencia.stock_maximo, "0"),
                punto_reorden=_to_decimal(existencia.punto_reorden, "0"),
                inventario_promedio=_to_decimal(existencia.inventario_promedio, "0"),
                dias_llegada_pedido=int(existencia.dias_llegada_pedido or 0),
                consumo_diario_promedio=_to_decimal(existencia.consumo_diario_promedio, "0"),
                canonical_variant_count=canonical_by_id[canonical.id]["variant_count"],
            )
            grouped[canonical.id] = bucket
        bucket.stock_actual += _to_decimal(existencia.stock_actual, "0")

    existencias = list(grouped.values())

    criticos_count = 0
    bajo_count = 0
    rows = []
    for e in existencias:
        stock = e.stock_actual
        reorden = e.punto_reorden
        if stock <= 0:
            e.criticidad = "Alta"
            e.criticidad_badge = "bg-danger"
            e.nivel = "critico"
            criticos_count += 1
        elif stock < reorden:
            e.criticidad = "Media"
            e.criticidad_badge = "bg-warning"
            e.nivel = "bajo"
            bajo_count += 1
        else:
            e.criticidad = "Sin riesgo"
            e.criticidad_badge = "bg-success"
            e.nivel = "ok"

        e.sugerencia_compra = max(reorden - stock, 0)

        include = False
        if nivel == "all":
            include = True
        elif nivel == "alerta":
            include = e.nivel in {"critico", "bajo"}
        else:
            include = e.nivel == nivel

        if include:
            meta = _build_report_enterprise_meta(e.insumo)
            e.enterprise_status = meta["enterprise_status"]
            e.enterprise_missing = meta["enterprise_missing"]
            e.enterprise_usage_label = meta["enterprise_usage_label"]
            e.enterprise_edit_url = meta["enterprise_edit_url"]
            e.enterprise_list_url = meta["enterprise_list_url"]
            rows.append(e)

    return rows, criticos_count, bajo_count


def _export_faltantes_csv(rows, nivel: str) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="reporte_faltantes.csv"'
    writer = csv.writer(response)
    writer.writerow(["Insumo", "Unidad", "Stock actual", "Punto reorden", "Sugerencia compra", "Criticidad", "Nivel filtro"])
    for row in rows:
        writer.writerow(
            [
                row.insumo.nombre,
                row.insumo.unidad_base.codigo if row.insumo.unidad_base else "-",
                row.stock_actual,
                row.punto_reorden,
                row.sugerencia_compra,
                row.criticidad,
                nivel,
            ]
        )
    return response


def _export_faltantes_xlsx(rows, nivel: str) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Faltantes"
    ws.append(["Insumo", "Unidad", "Stock actual", "Punto reorden", "Sugerencia compra", "Criticidad", "Nivel filtro"])
    for row in rows:
        ws.append(
            [
                row.insumo.nombre,
                row.insumo.unidad_base.codigo if row.insumo.unidad_base else "-",
                float(row.stock_actual or 0),
                float(row.punto_reorden or 0),
                float(row.sugerencia_compra or 0),
                row.criticidad,
                nivel,
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="reporte_faltantes.xlsx"'
    return response


@login_required
def costo_receta(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")

    margen_pct = _to_decimal(request.GET.get("margen"), "35")
    if margen_pct < 0:
        margen_pct = Decimal("0")
    if margen_pct > 500:
        margen_pct = Decimal("500")
    factor_margen = Decimal("1") + (margen_pct / Decimal("100"))

    recetas = list(Receta.objects.prefetch_related("lineas", "lineas__insumo").order_by("nombre")[:500])
    member_to_row, canonical_by_id = _canonical_catalog_maps()
    total_qty_by_canonical = {}
    for receta in recetas:
        for linea in receta.lineas.all():
            if linea.insumo_id:
                row = member_to_row.get(linea.insumo_id)
                if not row:
                    continue
                canonical = row["canonical"]
                total_qty_by_canonical[canonical.id] = total_qty_by_canonical.get(canonical.id, Decimal("0")) + (
                    _to_decimal(linea.cantidad, "0")
                )

    latest_cost_by_insumo = {}
    for canonical_id in total_qty_by_canonical.keys():
        latest = latest_costo_canonico(insumo_id=canonical_id)
        if latest is not None:
            latest_cost_by_insumo[canonical_id] = latest

    rows = []
    total_costo = Decimal("0")
    with_costo = 0
    blocked_recipes = 0
    for receta in recetas:
        costo_total = Decimal("0")
        lineas_total = 0
        lineas_costeadas = 0
        recipe_blocked = False
        for linea in receta.lineas.all():
            if linea.match_status == LineaReceta.STATUS_REJECTED:
                continue
            lineas_total += 1

            costo_linea = Decimal("0")
            if linea.costo_linea_excel is not None:
                costo_linea = _to_decimal(linea.costo_linea_excel, "0")
            elif linea.cantidad is not None and linea.costo_unitario_snapshot is not None:
                costo_linea = _to_decimal(linea.cantidad, "0") * _to_decimal(linea.costo_unitario_snapshot, "0")
            elif linea.cantidad is not None and linea.insumo_id:
                row = member_to_row.get(linea.insumo_id)
                canonical = row["canonical"] if row else None
                if canonical and canonical.id in latest_cost_by_insumo:
                    costo_linea = _to_decimal(linea.cantidad, "0") * _to_decimal(
                        latest_cost_by_insumo[canonical.id], "0"
                    )
                if canonical:
                    profile = enterprise_readiness_profile(canonical)
                    if profile["readiness_label"] != "Listo ERP":
                        recipe_blocked = True
                else:
                    recipe_blocked = True
            elif linea.insumo_texto:
                recipe_blocked = True

            if costo_linea > 0:
                lineas_costeadas += 1
                costo_total += costo_linea

        cobertura_pct = (Decimal("100") * Decimal(lineas_costeadas) / Decimal(lineas_total)) if lineas_total else Decimal("0")
        precio_sugerido = costo_total * factor_margen
        if recipe_blocked:
            blocked_recipes += 1
        row = {
            "receta": receta,
            "costo_total": costo_total,
            "costo_por_kg": receta.costo_por_kg_estimado,
            "rendimiento_cantidad": receta.rendimiento_cantidad,
            "rendimiento_unidad": receta.rendimiento_unidad.codigo if receta.rendimiento_unidad else "",
            "margen_pct": margen_pct,
            "precio_sugerido": precio_sugerido,
            "lineas_total": lineas_total,
            "lineas_costeadas": lineas_costeadas,
            "cobertura_pct": cobertura_pct,
        }
        rows.append(row)
        total_costo += costo_total
        if costo_total > 0:
            with_costo += 1

    rows.sort(key=lambda r: (r["costo_total"], r["receta"].nombre), reverse=True)

    context = {
        "rows": rows,
        "margen_pct": margen_pct,
        "total_recetas": len(rows),
        "recetas_con_costo": with_costo,
        "total_costo": total_costo,
        "enterprise_chain": _reportes_enterprise_chain(
            focus="costeo",
            open_count=with_costo,
            blocked_count=blocked_recipes,
        ),
        "document_stage_rows": _reportes_document_stage_rows(
            focus="costeo",
            open_count=with_costo,
            blocked_count=blocked_recipes,
            total_count=len(rows),
        ),
        "erp_governance_rows": _reportes_governance_rows(
            _reportes_document_stage_rows(
                focus="costeo",
                open_count=with_costo,
                blocked_count=blocked_recipes,
                total_count=len(rows),
            )
        ),
        "operational_health_cards": _reportes_operational_health_cards(
            focus="costeo",
            open_count=with_costo,
            blocked_count=blocked_recipes,
            total_count=len(rows),
        ),
        "maturity_summary": _reportes_maturity_summary(
            chain=_reportes_enterprise_chain(
                focus="costeo",
                open_count=with_costo,
                blocked_count=blocked_recipes,
            ),
            default_url=reverse("reportes:costo_receta"),
        ),
        "handoff_map": _reportes_handoff_map(
            focus="costeo",
            blocked_count=blocked_recipes,
            open_count=with_costo,
            total_count=len(rows),
            default_url=reverse("reportes:costo_receta"),
        ),
        "release_gate_rows": _reportes_release_gate_rows(
            focus="costeo",
            open_count=with_costo,
            blocked_count=blocked_recipes,
            total_count=len(rows),
        ),
    }
    context["release_gate_completion"] = _reportes_release_gate_completion(context["release_gate_rows"])
    context["critical_path_rows"] = _reportes_critical_path_rows(context["enterprise_chain"])
    context["executive_radar_rows"] = _reportes_executive_radar_rows(
        context["erp_governance_rows"],
        default_owner="Costeo / Finanzas",
        fallback_url=reverse("reportes:costo_receta"),
    )
    context["erp_command_center"] = _reportes_command_center(
        governance_rows=context["erp_governance_rows"],
        maturity_summary=context["maturity_summary"],
        default_owner="Costeo / Finanzas",
    )
    return render(request, "reportes/costo_receta.html", context)


@login_required
def consumo(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    today = timezone.localdate()
    default_from = today - timedelta(days=30)

    date_from = request.GET.get("date_from") or default_from.isoformat()
    date_to = request.GET.get("date_to") or today.isoformat()
    tipo = (request.GET.get("tipo") or "all").upper()
    valid_tipos = {"ALL", "CONSUMO", "SALIDA", "ENTRADA"}
    if tipo not in valid_tipos:
        tipo = "ALL"

    movimientos, resumen = _consumo_rows(date_from, date_to, tipo)

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_consumo_csv(resumen, date_from, date_to, tipo)
    if export_format == "xlsx":
        return _export_consumo_xlsx(resumen, date_from, date_to, tipo)

    context = {
        "rows": resumen,
        "total_movimientos": movimientos.count(),
        "total_insumos": len(resumen),
        "total_cantidad": sum((row["cantidad_total"] or 0) for row in resumen),
        "governance_ready": sum(1 for row in resumen if row["enterprise_status"] == "Listo ERP"),
        "governance_blocked": sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
        "enterprise_chain": _reportes_enterprise_chain(
            focus="consumo",
            open_count=movimientos.count(),
            blocked_count=sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
            date_from=date_from,
            date_to=date_to,
        ),
        "document_stage_rows": _reportes_document_stage_rows(
            focus="consumo",
            open_count=movimientos.count(),
            blocked_count=sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
            total_count=len(resumen),
            date_from=date_from,
            date_to=date_to,
        ),
        "erp_governance_rows": _reportes_governance_rows(
            _reportes_document_stage_rows(
                focus="consumo",
                open_count=movimientos.count(),
                blocked_count=sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
                total_count=len(resumen),
                date_from=date_from,
                date_to=date_to,
            )
        ),
        "operational_health_cards": _reportes_operational_health_cards(
            focus="consumo",
            open_count=movimientos.count(),
            blocked_count=sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
            total_count=len(resumen),
        ),
        "maturity_summary": _reportes_maturity_summary(
            chain=_reportes_enterprise_chain(
                focus="consumo",
                open_count=movimientos.count(),
                blocked_count=sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
                date_from=date_from,
                date_to=date_to,
            ),
            default_url=reverse("reportes:consumo"),
        ),
        "handoff_map": _reportes_handoff_map(
            focus="consumo",
            blocked_count=sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
            open_count=movimientos.count(),
            total_count=len(resumen),
            default_url=reverse("reportes:consumo") + f"?date_from={date_from}&date_to={date_to}&tipo={tipo}",
        ),
        "release_gate_rows": _reportes_release_gate_rows(
            focus="consumo",
            open_count=movimientos.count(),
            blocked_count=sum(1 for row in resumen if row["enterprise_status"] != "Listo ERP"),
            total_count=len(resumen),
            date_from=date_from,
            date_to=date_to,
        ),
        "filters": {
            "date_from": date_from,
            "date_to": date_to,
            "tipo": tipo,
        },
    }
    context["release_gate_completion"] = _reportes_release_gate_completion(context["release_gate_rows"])
    context["critical_path_rows"] = _reportes_critical_path_rows(context["enterprise_chain"])
    context["executive_radar_rows"] = _reportes_executive_radar_rows(
        context["erp_governance_rows"],
        default_owner="Inventario y Compras",
        fallback_url=reverse("reportes:consumo"),
    )
    context["erp_command_center"] = _reportes_command_center(
        governance_rows=context["erp_governance_rows"],
        maturity_summary=context["maturity_summary"],
        default_owner="Inventario y Compras",
    )
    return render(request, "reportes/consumo.html", context)


@login_required
def faltantes(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    nivel = (request.GET.get("nivel") or "alerta").lower()
    valid_levels = {"alerta", "critico", "bajo", "all"}
    if nivel not in valid_levels:
        nivel = "alerta"

    rows, criticos_count, bajo_count = _faltantes_rows(nivel)

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_faltantes_csv(rows, nivel)
    if export_format == "xlsx":
        return _export_faltantes_xlsx(rows, nivel)

    context = {
        "rows": rows,
        "nivel": nivel,
        "criticos_count": criticos_count,
        "bajo_count": bajo_count,
        "alertas_count": criticos_count + bajo_count,
        "governance_ready": sum(1 for row in rows if row.enterprise_status == "Listo ERP"),
        "governance_blocked": sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
        "enterprise_chain": _reportes_enterprise_chain(
            focus="faltantes",
            open_count=criticos_count + bajo_count,
            blocked_count=sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
            nivel=nivel,
        ),
        "document_stage_rows": _reportes_document_stage_rows(
            focus="faltantes",
            open_count=criticos_count + bajo_count,
            blocked_count=sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
            total_count=len(rows),
            nivel=nivel,
        ),
        "erp_governance_rows": _reportes_governance_rows(
            _reportes_document_stage_rows(
                focus="faltantes",
                open_count=criticos_count + bajo_count,
                blocked_count=sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
                total_count=len(rows),
                nivel=nivel,
            )
        ),
        "operational_health_cards": _reportes_operational_health_cards(
            focus="faltantes",
            open_count=criticos_count + bajo_count,
            blocked_count=sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
            total_count=len(rows),
        ),
        "maturity_summary": _reportes_maturity_summary(
            chain=_reportes_enterprise_chain(
                focus="faltantes",
                open_count=criticos_count + bajo_count,
                blocked_count=sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
                nivel=nivel,
            ),
            default_url=reverse("reportes:faltantes"),
        ),
        "handoff_map": _reportes_handoff_map(
            focus="faltantes",
            blocked_count=sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
            open_count=criticos_count + bajo_count,
            total_count=len(rows),
            default_url=reverse("reportes:faltantes") + f"?nivel={nivel}",
        ),
        "release_gate_rows": _reportes_release_gate_rows(
            focus="faltantes",
            open_count=criticos_count + bajo_count,
            blocked_count=sum(1 for row in rows if row.enterprise_status != "Listo ERP"),
            total_count=len(rows),
            nivel=nivel,
        ),
    }
    context["release_gate_completion"] = _reportes_release_gate_completion(context["release_gate_rows"])
    context["critical_path_rows"] = _reportes_critical_path_rows(context["enterprise_chain"])
    context["executive_radar_rows"] = _reportes_executive_radar_rows(
        context["erp_governance_rows"],
        default_owner="Abastecimiento",
        fallback_url=reverse("reportes:faltantes"),
    )
    context["erp_command_center"] = _reportes_command_center(
        governance_rows=context["erp_governance_rows"],
        maturity_summary=context["maturity_summary"],
        default_owner="Abastecimiento",
    )
    return render(request, "reportes/faltantes.html", context)


def _export_bi_csv(snapshot: dict) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="reporte_bi_mensual.csv"'
    writer = csv.writer(response)
    writer.writerow(["Periodo", "Compras", "Ventas", "Nomina", "Margen", "Entregas"])
    for row in snapshot["series_mensual"]:
        writer.writerow(
            [
                row["periodo"],
                row["compras"],
                row["ventas"],
                row["nomina"],
                row["margen"],
                row["entregas"],
            ]
        )
    return response


def _export_bi_xlsx(snapshot: dict) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "BI"
    ws.append(["Periodo", "Compras", "Ventas", "Nomina", "Margen", "Entregas"])
    for row in snapshot["series_mensual"]:
        ws.append(
            [
                row["periodo"],
                float(row["compras"] or 0),
                float(row["ventas"] or 0),
                float(row["nomina"] or 0),
                float(row["margen"] or 0),
                int(row["entregas"] or 0),
            ]
        )
    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="reporte_bi_mensual.xlsx"'
    return response


@login_required
def bi(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")

    try:
        period_days = int(request.GET.get("period_days") or "90")
    except (TypeError, ValueError):
        period_days = 90
    try:
        months_window = int(request.GET.get("months") or "6")
    except (TypeError, ValueError):
        months_window = 6
    snapshot = compute_bi_snapshot(period_days=period_days, months_window=months_window)
    executive_panels = build_executive_bi_panels()

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_bi_csv(snapshot)
    if export_format == "xlsx":
        return _export_bi_xlsx(snapshot)

    context = {
        "snapshot": snapshot,
        "executive_panels": executive_panels,
        "forecast_panel": executive_panels["forecast_panel"],
        "yoy_panel": executive_panels["yoy_panel"],
        "profitability_panel": executive_panels["profitability_panel"],
        "production_sales_panel": executive_panels["production_sales_panel"],
        "central_flow_panel": executive_panels["central_flow_panel"],
        "inventory_ledger_panel": executive_panels["inventory_ledger_panel"],
        "daily_sales_snapshot": _bi_daily_sales_snapshot(),
        "branch_weekday_rows": _bi_branch_weekday_comparisons(),
        "product_weekday_rows": _bi_product_weekday_comparisons(),
        "purchase_snapshot": _bi_purchase_snapshot(),
        "inventory_snapshot": _bi_inventory_snapshot(),
        "production_snapshot": _bi_production_snapshot(),
        "production_summary": _bi_production_summary(snapshot["range"]["from"], snapshot["range"]["to"]),
        "waste_summary": _bi_waste_summary(snapshot["range"]["from"], snapshot["range"]["to"]),
        "forecast_summary": _bi_forecast_summary(snapshot["range"]["to"].strftime("%Y-%m")),
        "supply_watchlist": _bi_supply_watchlist(),
        "ventas_historicas_summary": _ventas_historicas_bi_summary(),
        "period_days": snapshot["range"]["days"],
        "months_window": snapshot["range"]["months_window"],
        "enterprise_chain": _reportes_enterprise_chain(
            focus="bi",
            open_count=len(snapshot.get("series_mensual", [])),
            blocked_count=0,
        ),
        "document_stage_rows": _reportes_document_stage_rows(
            focus="bi",
            open_count=len(snapshot.get("series_mensual", [])),
            blocked_count=0,
            total_count=len(snapshot.get("series_mensual", [])),
        ),
        "erp_governance_rows": _reportes_governance_rows(
            _reportes_document_stage_rows(
                focus="bi",
                open_count=len(snapshot.get("series_mensual", [])),
                blocked_count=0,
                total_count=len(snapshot.get("series_mensual", [])),
            )
        ),
        "operational_health_cards": _reportes_operational_health_cards(
            focus="bi",
            open_count=len(snapshot.get("series_mensual", [])),
            blocked_count=0,
            total_count=len(snapshot.get("series_mensual", [])),
        ),
        "maturity_summary": _reportes_maturity_summary(
            chain=_reportes_enterprise_chain(
                focus="bi",
                open_count=len(snapshot.get("series_mensual", [])),
                blocked_count=0,
            ),
            default_url=reverse("reportes:bi"),
        ),
        "handoff_map": _reportes_handoff_map(
            focus="bi",
            blocked_count=0,
            open_count=len(snapshot.get("series_mensual", [])),
            total_count=len(snapshot.get("series_mensual", [])),
            default_url=reverse("reportes:bi"),
        ),
        "release_gate_rows": _reportes_release_gate_rows(
            focus="bi",
            open_count=len(snapshot.get("series_mensual", [])),
            blocked_count=0,
            total_count=len(snapshot.get("series_mensual", [])),
        ),
    }
    context["sales_branch_bar_rows"] = _bi_bar_rows(
        list(context["daily_sales_snapshot"].get("top_branches") or []),
        label_key="label",
        secondary_key="secondary",
        value_key="amount",
    )
    context["sales_product_bar_rows"] = _bi_bar_rows(
        list(context["daily_sales_snapshot"].get("top_products") or []),
        label_key="label",
        value_key="amount",
    )
    context["monthly_sales_rows"] = _bi_monthly_sales_rows(snapshot)
    context["monthly_margin_rows"] = _bi_monthly_margin_rows(snapshot)
    context["supplier_bar_rows"] = _bi_bar_rows(
        list(snapshot.get("top_proveedores") or []),
        label_key="proveedor__nombre",
        value_key="total",
    )
    context["consumption_bar_rows"] = _bi_bar_rows(
        list(snapshot.get("top_insumos_consumo") or []),
        label_key="insumo__nombre",
        value_key="total",
    )
    context["production_product_bar_rows"] = _bi_bar_rows(
        list(context["production_summary"].get("top_products") or []),
        label_key="label",
        value_key="value",
    )
    context["waste_branch_bar_rows"] = _bi_bar_rows(
        list(context["waste_summary"].get("branch_rows") or []),
        label_key="label",
        secondary_key="secondary",
        value_key="value",
    )
    context["waste_cedis_bar_rows"] = _bi_bar_rows(
        list(context["waste_summary"].get("cedis_rows") or []),
        label_key="label",
        secondary_key="secondary",
        value_key="value",
    )
    context["forecast_gap_bar_rows"] = _bi_bar_rows(
        list(context["forecast_summary"].get("top_rows") or []),
        label_key="label",
        secondary_key="secondary",
        value_key="value",
    )
    context["branch_weekday_bar_rows"] = _bi_comparison_bar_rows(
        list(context["branch_weekday_rows"] or []),
        label_key="branch_code",
        secondary_key="branch_name",
    )
    context["product_weekday_bar_rows"] = _bi_comparison_bar_rows(
        list(context["product_weekday_rows"] or []),
        label_key="recipe_name",
    )
    context["daily_decision_rows"] = _bi_daily_decisions(
        daily_sales_snapshot=context["daily_sales_snapshot"],
        branch_weekday_rows=context["branch_weekday_rows"],
        product_weekday_rows=context["product_weekday_rows"],
        purchase_snapshot=context["purchase_snapshot"],
        inventory_snapshot=context["inventory_snapshot"],
        production_snapshot=context["production_snapshot"],
        waste_summary=context["waste_summary"],
        forecast_summary=context["forecast_summary"],
        supply_watchlist=context["supply_watchlist"],
    )
    context["release_gate_completion"] = _reportes_release_gate_completion(context["release_gate_rows"])
    context["critical_path_rows"] = _reportes_critical_path_rows(context["enterprise_chain"])
    context["executive_radar_rows"] = _reportes_executive_radar_rows(
        context["erp_governance_rows"],
        default_owner="Dirección General",
        fallback_url=reverse("reportes:bi"),
    )
    context["erp_command_center"] = _reportes_command_center(
        governance_rows=context["erp_governance_rows"],
        maturity_summary=context["maturity_summary"],
        default_owner="Dirección General",
    )
    return render(request, "reportes/bi.html", context)
