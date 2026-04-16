from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from calendar import monthrange

from django.conf import settings
from django.db.models import Q, Sum

from core.cache_versions import get_or_set_versioned_cache
from pos_bridge.models import PointDailySale, PointSalesDailyCategoryFact, PointSalesDailyProductFact
from recetas.models import VentaHistorica
from ventas.models import VentaAutoritativaPoint
from ventas.services.sales_read_service import get_sales_range

POINT_BRIDGE_SALES_SOURCE = "POINT_BRIDGE_SALES"
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
RECENT_POINT_SOURCE = "/Report/VentasCategorias"


def official_sales_stage_max_date():
    return (
        PointDailySale.objects.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
        .order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )


def recent_sales_stage_max_date():
    return (
        PointDailySale.objects.filter(source_endpoint=RECENT_POINT_SOURCE)
        .order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )


def authoritative_sales_max_date():
    return (
        VentaAutoritativaPoint.objects.order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )


def v2_category_sales_max_date():
    return (
        PointSalesDailyCategoryFact.objects.order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )


def v2_product_sales_max_date():
    return (
        PointSalesDailyProductFact.objects.order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )


def canonical_point_max_date():
    return max(
        [
            value
            for value in [
                official_sales_stage_max_date(),
                recent_sales_stage_max_date(),
                authoritative_sales_max_date(),
                v2_category_sales_max_date(),
                v2_product_sales_max_date(),
            ]
            if value
        ],
        default=None,
    )


def canonical_point_previous_dates(target_date) -> list:
    if not target_date:
        return []
    values = set()
    values.update(
        VentaAutoritativaPoint.objects.filter(sale_date__lt=target_date).values_list("sale_date", flat=True).distinct()
    )
    values.update(
        PointSalesDailyCategoryFact.objects.filter(sale_date__lt=target_date).values_list("sale_date", flat=True).distinct()
    )
    values.update(
        PointSalesDailyProductFact.objects.filter(sale_date__lt=target_date).values_list("sale_date", flat=True).distinct()
    )
    values.update(
        PointDailySale.objects.filter(
            sale_date__lt=target_date,
            source_endpoint__in=[OFFICIAL_POINT_SOURCE, RECENT_POINT_SOURCE],
        ).values_list("sale_date", flat=True).distinct()
    )
    return sorted(values, reverse=True)


def build_sales_source_context() -> dict[str, object]:
    stage_latest_date = max(
        [value for value in [official_sales_stage_max_date(), recent_sales_stage_max_date()] if value],
        default=None,
    )
    latest_point_date = canonical_point_max_date()
    latest_bridge_date = (
        VentaHistorica.objects.filter(fuente=POINT_BRIDGE_SALES_SOURCE)
        .order_by("-fecha")
        .values_list("fecha", flat=True)
        .first()
    )
    latest_hist_date = VentaHistorica.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    if latest_point_date:
        return {
            "mode": "point_stage",
            "latest_date": stage_latest_date or latest_point_date,
            "canonical_latest_date": latest_point_date,
            "stage_latest_date": stage_latest_date,
            "label": "Point directo",
            "detail": "Fuente canónica Point bridge.",
            "canonical": True,
        }
    if latest_bridge_date:
        return {
            "mode": "point_history",
            "latest_date": latest_bridge_date,
            "label": "Point conciliado",
            "detail": "Histórico Point materializado a ERP.",
            "canonical": True,
        }
    if latest_hist_date:
        return {
            "mode": "historical_fallback",
            "latest_date": latest_hist_date,
            "label": "Histórico importado no canónico",
            "detail": "Fuente referencial; no representa Point directo.",
            "canonical": False,
        }
    return {
        "mode": "none",
        "latest_date": None,
        "label": "Sin fuente",
        "detail": "No hay ventas cargadas.",
        "canonical": False,
    }


def get_sales_source_context(*, cache_key_parts: tuple[object, ...]) -> dict[str, object]:
    if bool(getattr(settings, "RUNNING_TESTS", False)):
        return build_sales_source_context()
    return get_or_set_versioned_cache(
        key_parts=cache_key_parts,
        scopes=("ventas", "dashboard"),
        builder=build_sales_source_context,
    )


def operational_sales_filters(*, start_date: date, end_date: date) -> Q:
    official_max = official_sales_stage_max_date()
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


def operational_sales_rows_for_date(target_date: date):
    if PointDailySale.objects.filter(sale_date=target_date, source_endpoint=OFFICIAL_POINT_SOURCE).exists():
        return PointDailySale.objects.filter(sale_date=target_date, source_endpoint=OFFICIAL_POINT_SOURCE)
    return PointDailySale.objects.filter(sale_date=target_date, source_endpoint=RECENT_POINT_SOURCE)


def sales_rows_for_date(source: dict[str, object], target_date):
    if source["mode"] == "point_stage":
        return operational_sales_rows_for_date(target_date)
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fecha=target_date, fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.filter(fecha=target_date)
    return VentaHistorica.objects.none()


def sales_rows_for_month(source: dict[str, object], year: int, month: int):
    if source["mode"] == "point_stage":
        start_date = date(year, month, 1)
        end_date = date(year, month, monthrange(year, month)[1])
        return PointDailySale.objects.filter(
            sale_date__year=year,
            sale_date__month=month,
        ).filter(
            operational_sales_filters(start_date=start_date, end_date=end_date)
        )
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fecha__year=year, fecha__month=month, fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.filter(fecha__year=year, fecha__month=month)
    return VentaHistorica.objects.none()


def point_sales_month_total(year: int, month: int) -> dict[str, object]:
    start_date = date(year, month, 1)
    end_date = date(year, month, monthrange(year, month)[1])
    canonical = get_sales_range(
        start_date=start_date,
        end_date=end_date,
        coverage_policy="prefer_complete",
    )
    canonical_total = canonical.get("monto") or Decimal("0")
    if canonical_total > 0:
        return {"value": Decimal(str(canonical_total)), "source_label": "Point directo"}

    bridge_qs = VentaHistorica.objects.filter(
        fecha__year=year,
        fecha__month=month,
        fuente=POINT_BRIDGE_SALES_SOURCE,
    )
    bridge_total = bridge_qs.aggregate(total=Sum("monto_total")).get("total") or Decimal("0")
    if bridge_total > 0:
        return {"value": Decimal(str(bridge_total)), "source_label": "Point conciliado"}

    return {"value": Decimal("0"), "source_label": "Sin dato oficial"}


def sales_previous_dates(source: dict[str, object], target_date) -> list:
    if source["mode"] == "point_stage":
        if not source.get("stage_latest_date"):
            return canonical_point_previous_dates(target_date)
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
        return list(
            VentaHistorica.objects.filter(fecha__lt=target_date)
            .order_by("-fecha")
            .values_list("fecha", flat=True)
            .distinct()
        )
    return []


def sales_history_queryset(source: dict[str, object]):
    if source["mode"] == "point_stage":
        official_max = official_sales_stage_max_date()
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
