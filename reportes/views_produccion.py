from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.db.models import Min, Sum
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.utils import timezone
from django.views.generic import TemplateView

from control.models import MermaMensualSucursal
from core.access import can_view_reportes
from core.models import Sucursal
from pos_bridge.models import PointDailySale, PointProductionLine, PointSalesDailyProductFact
from recetas.models import ProductoMonthClosure, ProductoMonthClosureLine, Receta
from recetas.utils.derived_product_presentations import get_total_cost_map
from reportes.models import FactProduccionDiaria


ZERO = Decimal("0")


@dataclass(frozen=True)
class PeriodSelection:
    month_start: date
    month_end: date

    @property
    def value(self) -> str:
        return self.month_start.strftime("%Y-%m")

    @property
    def label(self) -> str:
        return self.month_start.strftime("%B %Y").title()


def _parse_period(raw_value: str | None) -> PeriodSelection:
    today = timezone.localdate()
    fallback = date(today.year, today.month, 1)
    if raw_value:
        try:
            year_raw, month_raw = raw_value.split("-", 1)
            selected = date(int(year_raw), int(month_raw), 1)
        except (TypeError, ValueError):
            selected = fallback
    else:
        selected = fallback
    last_day = monthrange(selected.year, selected.month)[1]
    return PeriodSelection(month_start=selected, month_end=date(selected.year, selected.month, last_day))


def _parse_int(raw_value: str | None) -> int | None:
    try:
        value = int(raw_value or 0)
    except (TypeError, ValueError):
        return None
    return value or None


def _decimal(value: Any) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value or 0))


def _aggregate_by_recipe(queryset, field_name: str, recipe_field: str = "receta_id") -> dict[int, Decimal]:
    rows = queryset.values(recipe_field).annotate(total=Sum(field_name))
    return {
        int(row[recipe_field]): _decimal(row["total"])
        for row in rows
        if row.get(recipe_field)
    }


def _first_available_date() -> dict[str, date | None]:
    return {
        "ventas": PointSalesDailyProductFact.objects.aggregate(min_date=Min("sale_date"))["min_date"],
        "produccion": FactProduccionDiaria.objects.aggregate(min_date=Min("fecha"))["min_date"]
        or PointProductionLine.objects.aggregate(min_date=Min("production_date"))["min_date"],
        "merma": MermaMensualSucursal.objects.aggregate(min_date=Min("periodo"))["min_date"],
        "cierre": ProductoMonthClosure.objects.aggregate(min_date=Min("month_start"))["min_date"],
    }


class ProducidoVsVendidoMermaView(LoginRequiredMixin, TemplateView):
    template_name = "reportes/producido_vs_vendido.html"

    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        if not can_view_reportes(request.user):
            raise PermissionDenied("No tienes permisos para ver Reportes.")
        return super().dispatch(request, *args, **kwargs)

    def get(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        context = self._build_context(request)
        if request.resolver_match and request.resolver_match.url_name == "producido_vs_vendido_data":
            return JsonResponse(
                {
                    "periodo": context["selected_period"],
                    "fuentes": context["fuentes"],
                    "rows": context["json_rows"],
                    "totals": context["grand_total"],
                }
            )
        return self.render_to_response(context)

    def _build_context(self, request: HttpRequest) -> dict[str, Any]:
        period = _parse_period(request.GET.get("periodo") or request.GET.get("period"))
        sucursal_id = _parse_int(request.GET.get("sucursal"))
        familia = (request.GET.get("familia") or "").strip()

        sales_map, sales_source = self._sales_map(period, sucursal_id)
        recipe_ids = set(sales_map)
        production_map, production_source = self._production_map(period, sucursal_id)
        merma_map, merma_cost_map, merma_source = self._merma_maps(period, sucursal_id)
        closure_map = self._closure_map(period)

        if not sucursal_id and sales_source == "sin_datos" and closure_map["vendido"]:
            sales_map = closure_map["vendido"]
            sales_source = "ProductoMonthClosureLine"
            recipe_ids.update(recipe_id for recipe_id, value in sales_map.items() if value)
        if not sucursal_id and not recipe_ids:
            recipe_ids.update(recipe_id for recipe_id, value in closure_map["vendido"].items() if value)
        recipe_ids.update(recipe_id for recipe_id, value in merma_map.items() if value)

        recipes_qs = Receta.objects.filter(
            id__in=recipe_ids,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        ).order_by("familia", "nombre")
        if familia:
            recipes_qs = recipes_qs.filter(familia=familia)
        recipes = list(recipes_qs)

        if not sucursal_id and production_source == "sin_datos" and closure_map["producido"]:
            production_map = closure_map["producido"]
            production_source = "ProductoMonthClosureLine"

        cost_map = get_total_cost_map([recipe.id for recipe in recipes])
        rows = [self._build_row(recipe, sales_map, production_map, merma_map, merma_cost_map, cost_map) for recipe in recipes]
        groups, grand_total = self._group_rows(rows)

        source_dates = _first_available_date()
        banners = self._banners(
            sales_source=sales_source,
            production_source=production_source,
            merma_source=merma_source,
            source_dates=source_dates,
        )

        return {
            "module_tabs": self._module_tabs("producido_vs_vendido"),
            "selected_period": period.value,
            "selected_period_label": period.label,
            "selected_sucursal": sucursal_id or "",
            "selected_familia": familia,
            "sucursales": Sucursal.objects.filter(activa=True).order_by("codigo", "nombre"),
            "familias": self._families(),
            "groups": groups,
            "grand_total": grand_total,
            "json_rows": [row["json"] for row in rows],
            "fuentes": {
                "ventas": sales_source,
                "produccion": production_source,
                "merma": merma_source,
            },
            "banners": banners,
            "source_dates": source_dates,
        }

    def _sales_map(self, period: PeriodSelection, sucursal_id: int | None) -> tuple[dict[int, Decimal], str]:
        facts = PointSalesDailyProductFact.objects.filter(
            sale_date__gte=period.month_start,
            sale_date__lte=period.month_end,
            receta_id__isnull=False,
        )
        if sucursal_id:
            facts = facts.filter(branch__erp_branch_id=sucursal_id)
        if facts.exists():
            return _aggregate_by_recipe(facts, "total_cantidad"), "PointSalesDailyProductFact"

        sales = PointDailySale.objects.filter(
            sale_date__gte=period.month_start,
            sale_date__lte=period.month_end,
            receta_id__isnull=False,
        )
        if sucursal_id:
            sales = sales.filter(branch__erp_branch_id=sucursal_id)
        if sales.exists():
            return _aggregate_by_recipe(sales, "quantity"), "PointDailySale"
        return {}, "sin_datos"

    def _production_map(self, period: PeriodSelection, sucursal_id: int | None) -> tuple[dict[int, Decimal], str]:
        facts = FactProduccionDiaria.objects.filter(
            fecha__gte=period.month_start,
            fecha__lte=period.month_end,
            receta_id__isnull=False,
        )
        if sucursal_id:
            facts = facts.filter(sucursal_id=sucursal_id)
        if facts.exists():
            return _aggregate_by_recipe(facts, "producido"), "FactProduccionDiaria"

        point_rows = PointProductionLine.objects.filter(
            production_date__gte=period.month_start,
            production_date__lte=period.month_end,
            receta_id__isnull=False,
            is_insumo=False,
        )
        if sucursal_id:
            point_rows = point_rows.filter(erp_branch_id=sucursal_id)
        if point_rows.exists():
            return _aggregate_by_recipe(point_rows, "produced_quantity"), "PointProductionLine"
        return {}, "sin_datos"

    def _merma_maps(
        self,
        period: PeriodSelection,
        sucursal_id: int | None,
    ) -> tuple[dict[int, Decimal], dict[int, Decimal], str]:
        rows = MermaMensualSucursal.objects.filter(periodo=period.month_start, receta_id__isnull=False)
        if sucursal_id:
            rows = rows.filter(sucursal_id=sucursal_id)
        if not rows.exists():
            return {}, {}, "sin_datos"
        return (
            _aggregate_by_recipe(rows, "unidades_merma"),
            _aggregate_by_recipe(rows, "costo_merma"),
            "MermaMensualSucursal",
        )

    def _closure_map(self, period: PeriodSelection) -> dict[str, dict[int, Decimal]]:
        closure = ProductoMonthClosure.objects.filter(month_start=period.month_start).first()
        if closure is None:
            return {"producido": {}, "vendido": {}, "merma": {}}
        lines = ProductoMonthClosureLine.objects.filter(closure=closure)
        return {
            "producido": _aggregate_by_recipe(lines, "produccion_mes", recipe_field="receta_padre_id"),
            "vendido": _aggregate_by_recipe(lines, "venta_total_equivalente", recipe_field="receta_padre_id"),
            "merma": _aggregate_by_recipe(lines, "merma_total_equivalente", recipe_field="receta_padre_id"),
        }

    def _build_row(
        self,
        recipe: Receta,
        sales_map: dict[int, Decimal],
        production_map: dict[int, Decimal],
        merma_map: dict[int, Decimal],
        merma_cost_map: dict[int, Decimal],
        cost_map: dict[int, Decimal],
    ) -> dict[str, Any]:
        vendido = sales_map.get(recipe.id)
        producido = production_map.get(recipe.id)
        merma_reportada = merma_map.get(recipe.id)
        merma_calculada = None
        if producido is not None and vendido is not None:
            merma_calculada = producido - vendido
        costo_unitario = cost_map.get(recipe.id, ZERO)
        costo_merma = merma_cost_map.get(recipe.id)
        if costo_merma is None and merma_reportada is not None and costo_unitario:
            costo_merma = merma_reportada * costo_unitario
        merma_for_pct = merma_reportada if merma_reportada is not None else merma_calculada
        pct_merma = None
        if merma_for_pct is not None and vendido and vendido > ZERO:
            pct_merma = (merma_for_pct / vendido) * Decimal("100")
        familia = (recipe.familia or "").strip() or "Sin familia"
        row = {
            "receta_id": recipe.id,
            "receta": recipe.nombre,
            "familia": familia,
            "vendido": vendido,
            "producido": producido,
            "dif": merma_calculada,
            "merma_reportada": merma_reportada,
            "merma_calculada": merma_calculada,
            "costo_merma": costo_merma,
            "pct_merma": pct_merma,
        }
        row["json"] = {
            key: (str(value) if isinstance(value, Decimal) else value)
            for key, value in row.items()
            if key != "json"
        }
        return row

    def _group_rows(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[row["familia"]].append(row)

        groups = []
        grand_rows: list[dict[str, Any]] = []
        for family in sorted(grouped):
            family_rows = grouped[family]
            grand_rows.extend(family_rows)
            groups.append({"familia": family, "rows": family_rows, "total": self._totals(family_rows)})
        return groups, self._totals(grand_rows)

    def _totals(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        totals = {
            "vendido": sum((row["vendido"] for row in rows if row["vendido"] is not None), ZERO),
            "producido": sum((row["producido"] for row in rows if row["producido"] is not None), ZERO),
            "dif": sum((row["dif"] for row in rows if row["dif"] is not None), ZERO),
            "merma_reportada": sum((row["merma_reportada"] for row in rows if row["merma_reportada"] is not None), ZERO),
            "merma_calculada": sum((row["merma_calculada"] for row in rows if row["merma_calculada"] is not None), ZERO),
            "costo_merma": sum((row["costo_merma"] for row in rows if row["costo_merma"] is not None), ZERO),
        }
        totals["pct_merma"] = (
            (totals["merma_reportada"] / totals["vendido"]) * Decimal("100")
            if totals["vendido"]
            else None
        )
        return totals

    def _banners(
        self,
        *,
        sales_source: str,
        production_source: str,
        merma_source: str,
        source_dates: dict[str, date | None],
    ) -> list[str]:
        banners = []
        if source_dates.get("produccion"):
            banners.append(f"Módulo de producción: datos disponibles desde {source_dates['produccion']:%Y-%m-%d}.")
        if sales_source == "ProductoMonthClosureLine":
            banners.append("Ventas: sin registros diarios para este periodo; se usó cierre mensual consolidado.")
        elif sales_source == "sin_datos":
            banners.append("Ventas: sin registros para este periodo.")
        if production_source == "ProductoMonthClosureLine":
            banners.append("Producción: sin registros diarios para este periodo; se usó cierre mensual consolidado.")
        elif production_source == "sin_datos":
            banners.append("Producción: sin registros para este periodo.")
        if merma_source == "sin_datos":
            banners.append("Merma: sin registros consolidados para este periodo.")
        return banners

    def _families(self) -> list[str]:
        return list(
            Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL)
            .exclude(familia="")
            .order_by("familia")
            .values_list("familia", flat=True)
            .distinct()
        )

    def _module_tabs(self, active: str) -> list[dict[str, str | bool]]:
        tabs = [
            ("ventas", "/reportes/ventas/", "Ventas"),
            ("cierre_operativo", "/reportes/cierre-operativo/", "Cierre diario"),
            ("cierre_producto", "/reportes/cierre-producto/", "Cierre producto"),
            ("producido_vs_vendido", "/reportes/produccion/", "Producido vs Vendido"),
            ("financiero", "/reportes/financiero/", "Financiero"),
            ("mermas_devoluciones", "/reportes/mermas-devoluciones/", "Mermas y Devoluciones"),
            ("auditoria_insumos", "/reportes/auditoria-insumos/", "Auditoría Insumos"),
            ("proyeccion_produccion", "/reportes/proyeccion-produccion/", "Proyección Producción"),
            ("bi", "/reportes/bi/", "BI"),
        ]
        return [
            {"key": key, "url": url, "label": label, "active": key == active}
            for key, url, label in tabs
        ]
