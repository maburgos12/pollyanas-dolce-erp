from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Avg, Count, Max, Q, Sum
from django.shortcuts import render

from core.access import has_any_role, ROLE_ADMIN, ROLE_COMPRAS, ROLE_DG, ROLE_PRODUCCION
from core.models import Sucursal
from pos_bridge.models import PointSalesDailyProductFact
from recetas.models import Receta


def _can_view_pronostico(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_PRODUCCION, ROLE_COMPRAS, "VENTAS", "LECTURA")


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _ceil_decimal(value: Decimal) -> int:
    return int(math.ceil(float(value))) if value > 0 else 0


def _trend_label(factor: Decimal) -> str:
    if factor >= Decimal("1.08"):
        return "sube"
    if factor <= Decimal("0.92"):
        return "baja"
    return "estable"


def _build_pronostico_ventas(*, start_date: date, end_date: date, branch_ids: set[int]) -> dict:
    selected_days = list(_date_range(start_date, end_date))
    if not selected_days or not branch_ids:
        return {}

    base_qs = PointSalesDailyProductFact.objects.filter(
        receta_id__isnull=False,
        point_product_id__isnull=False,
        point_product__precio_temporada=False,
        branch__erp_branch_id__in=branch_ids,
        branch__erp_branch__activa=True,
    ).exclude(
        Q(point_product__name__icontains="sobre pedido") | Q(receta__nombre__icontains="sobre pedido")
    ).exclude(
        Q(point_product__name__icontains="sp ") | Q(point_product__name__icontains=" sp") |
        Q(receta__nombre__icontains="sp ") | Q(receta__nombre__icontains=" sp")
    ).exclude(
        Q(receta__familia__icontains="pay") &
        (Q(point_product__name__icontains="rebanada") | Q(receta__nombre__icontains="rebanada"))
    )
    latest_sale_date = base_qs.aggregate(max_date=Max("sale_date")).get("max_date")
    if not latest_sale_date:
        return {}

    recent_end = min(latest_sale_date, start_date - timedelta(days=1))
    recent_start = recent_end - timedelta(days=89)
    recent_rotation_start = recent_end - timedelta(days=29)
    comparable_start = recent_start - timedelta(days=364)
    comparable_end = recent_end - timedelta(days=364)

    rotating_recipe_ids = set(
        base_qs.filter(sale_date__range=(recent_rotation_start, recent_end))
        .values("receta_id")
        .annotate(qty=Sum("total_cantidad"))
        .filter(qty__gt=0)
        .values_list("receta_id", flat=True)
    )
    if not rotating_recipe_ids:
        return {}
    base_qs = base_qs.filter(receta_id__in=rotating_recipe_ids)

    recent_rows = list(
        base_qs.filter(sale_date__range=(recent_start, recent_end))
        .values("receta_id", "branch__erp_branch_id")
        .annotate(
            qty=Sum("total_cantidad"),
            revenue=Sum("total_venta_neta"),
            sale_days=Count("sale_date", distinct=True),
        )
    )
    eligible_pairs = {
        (int(row["receta_id"]), int(row["branch__erp_branch_id"]))
        for row in recent_rows
        if _decimal(row.get("qty")) > 0
    }
    if not eligible_pairs:
        return {}

    recipe_ids = {recipe_id for recipe_id, _branch_id in eligible_pairs}
    recent_qty = {
        (int(row["receta_id"]), int(row["branch__erp_branch_id"])): _decimal(row.get("qty"))
        for row in recent_rows
    }
    recent_revenue = {
        (int(row["receta_id"]), int(row["branch__erp_branch_id"])): _decimal(row.get("revenue"))
        for row in recent_rows
    }

    comparable_qty = {
        (int(row["receta_id"]), int(row["branch__erp_branch_id"])): _decimal(row.get("qty"))
        for row in base_qs.filter(
            sale_date__range=(comparable_start, comparable_end),
            receta_id__in=recipe_ids,
        )
        .values("receta_id", "branch__erp_branch_id")
        .annotate(qty=Sum("total_cantidad"))
    }

    factors: dict[tuple[int, int], Decimal] = {}
    alpha = Decimal("1.0")
    recent_days = Decimal("90")
    for pair in eligible_pairs:
        recent_avg = recent_qty.get(pair, Decimal("0")) / recent_days
        comparable_avg = comparable_qty.get(pair, Decimal("0")) / recent_days
        factor = (recent_avg + alpha) / (comparable_avg + alpha)
        factors[pair] = min(Decimal("1.80"), max(Decimal("0.60"), factor))

    homologue_dates = [day - timedelta(days=364) for day in selected_days]
    history_start = min(homologue_dates) - timedelta(days=3)
    history_end = max(homologue_dates) + timedelta(days=3)
    history_qty: dict[tuple[int, int, date], Decimal] = {}
    for row in (
        base_qs.filter(
            sale_date__range=(history_start, history_end),
            receta_id__in=recipe_ids,
        )
        .values("receta_id", "branch__erp_branch_id", "sale_date")
        .annotate(qty=Sum("total_cantidad"))
    ):
        key = (int(row["receta_id"]), int(row["branch__erp_branch_id"]), row["sale_date"])
        history_qty[key] = _decimal(row.get("qty"))

    recipe_map = {
        receta.id: receta
        for receta in Receta.objects.filter(id__in=recipe_ids).only("id", "nombre", "familia", "categoria")
    }
    branch_map = {
        branch.id: branch
        for branch in Sucursal.objects.filter(id__in=branch_ids).only("id", "codigo", "nombre")
    }

    price_by_recipe: dict[int, Decimal] = {}
    for recipe_id in recipe_ids:
        qty = sum(qty for (rid, _bid), qty in recent_qty.items() if rid == recipe_id)
        revenue = sum(amount for (rid, _bid), amount in recent_revenue.items() if rid == recipe_id)
        if qty > 0 and revenue > 0:
            price_by_recipe[recipe_id] = (revenue / qty).quantize(Decimal("0.01"))

    for row in (
        base_qs.filter(receta_id__in=recipe_ids, point_product__precio__isnull=False, point_product__precio_activo=True)
        .values("receta_id")
        .annotate(avg_price=Avg("point_product__precio"))
    ):
        recipe_id = int(row["receta_id"])
        if recipe_id not in price_by_recipe and row.get("avg_price") is not None:
            price_by_recipe[recipe_id] = _decimal(row.get("avg_price")).quantize(Decimal("0.01"))

    product_totals: dict[int, dict] = {}
    branch_totals: dict[int, dict] = {}
    day_totals: dict[date, dict] = {}
    day_product_totals: dict[date, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    product_factor_values: dict[int, list[Decimal]] = defaultdict(list)
    product_branch_details: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    for day in selected_days:
        day_totals[day] = {"fecha": day, "total_piezas": 0, "total_ingreso": Decimal("0"), "top_5_productos": []}

    for recipe_id, branch_id in sorted(eligible_pairs):
        receta = recipe_map.get(recipe_id)
        branch = branch_map.get(branch_id)
        if not receta or not branch:
            continue
        price = price_by_recipe.get(recipe_id, Decimal("0"))
        factor = factors.get((recipe_id, branch_id), Decimal("1"))
        product_factor_values[recipe_id].append(factor)
        recent_avg = recent_qty.get((recipe_id, branch_id), Decimal("0")) / recent_days

        product_total = product_totals.setdefault(
            recipe_id,
            {
                "receta_id": recipe_id,
                "nombre": receta.nombre,
                "familia": receta.familia or "Sin familia",
                "total_piezas": 0,
                "precio": price,
                "total_ingreso": Decimal("0"),
                "factor_tendencia": Decimal("1"),
                "tendencia": "estable",
            },
        )
        branch_total = branch_totals.setdefault(
            branch_id,
            {
                "sucursal": branch.nombre,
                "codigo": branch.codigo,
                "total_piezas": 0,
                "total_ingreso": Decimal("0"),
                "productos": defaultdict(lambda: {"nombre": "", "total_piezas": 0, "total_ingreso": Decimal("0")}),
            },
        )

        for day in selected_days:
            homologue = day - timedelta(days=364)
            exact_base = history_qty.get((recipe_id, branch_id, homologue))
            if exact_base is None:
                window_values = [
                    history_qty.get((recipe_id, branch_id, homologue + timedelta(days=offset)), Decimal("0"))
                    for offset in range(-3, 4)
                ]
                positive_values = [value for value in window_values if value > 0]
                base_qty = sum(positive_values, Decimal("0")) / Decimal(len(positive_values)) if positive_values else Decimal("0")
            else:
                base_qty = exact_base

            forecast_qty = _ceil_decimal(base_qty * factor) if base_qty > 0 else _ceil_decimal(recent_avg)
            if forecast_qty <= 0:
                continue
            revenue = Decimal(forecast_qty) * price

            product_total["total_piezas"] += forecast_qty
            product_total["total_ingreso"] += revenue
            branch_total["total_piezas"] += forecast_qty
            branch_total["total_ingreso"] += revenue
            branch_product = branch_total["productos"][recipe_id]
            branch_product["nombre"] = receta.nombre
            branch_product["total_piezas"] += forecast_qty
            branch_product["total_ingreso"] += revenue
            day_totals[day]["total_piezas"] += forecast_qty
            day_totals[day]["total_ingreso"] += revenue
            day_product_totals[day][recipe_id] += forecast_qty
            product_branch_details[recipe_id][branch_id] += forecast_qty

    total_pieces = sum(row["total_piezas"] for row in product_totals.values())
    total_income = sum((row["total_ingreso"] for row in product_totals.values()), Decimal("0"))

    por_producto = []
    for recipe_id, row in product_totals.items():
        factor_values = product_factor_values.get(recipe_id) or [Decimal("1")]
        avg_factor = sum(factor_values, Decimal("0")) / Decimal(len(factor_values))
        row["factor_tendencia"] = avg_factor.quantize(Decimal("0.01"))
        row["tendencia"] = _trend_label(avg_factor)
        row["pct_del_total"] = (Decimal(row["total_piezas"]) / Decimal(total_pieces) * Decimal("100")).quantize(Decimal("0.01")) if total_pieces else Decimal("0")
        row["total_ingreso"] = row["total_ingreso"].quantize(Decimal("0.01"))
        por_producto.append(row)

    por_producto.sort(key=lambda item: (item["familia"], -item["total_piezas"], item["nombre"]))
    producto_nombre = {row["receta_id"]: row["nombre"] for row in por_producto}

    family_groups = []
    for familia in sorted({row["familia"] for row in por_producto}):
        rows = [row for row in por_producto if row["familia"] == familia]
        rows.sort(key=lambda item: (-item["total_piezas"], item["nombre"]))
        subtotal_pieces = sum(row["total_piezas"] for row in rows)
        subtotal_income = sum((row["total_ingreso"] for row in rows), Decimal("0"))
        family_groups.append(
            {
                "familia": familia,
                "total_piezas": subtotal_pieces,
                "total_ingreso": subtotal_income.quantize(Decimal("0.01")),
                "rows": rows,
            }
        )

    por_dia = []
    for day in selected_days:
        top_products = sorted(day_product_totals[day].items(), key=lambda item: (-item[1], producto_nombre.get(item[0], "")))[:5]
        row = day_totals[day]
        row["total_ingreso"] = row["total_ingreso"].quantize(Decimal("0.01"))
        row["top_5_productos"] = [{"nombre": producto_nombre.get(recipe_id, "Producto"), "total_piezas": qty} for recipe_id, qty in top_products]
        row["top_producto"] = row["top_5_productos"][0] if row["top_5_productos"] else None
        por_dia.append(row)

    por_sucursal = []
    for row in branch_totals.values():
        products = list(row["productos"].values())
        products.sort(key=lambda item: (-item["total_piezas"], item["nombre"]))
        row["productos"] = products[:5]
        row["total_ingreso"] = row["total_ingreso"].quantize(Decimal("0.01"))
        por_sucursal.append(row)
    por_sucursal.sort(key=lambda item: (-item["total_piezas"], item["sucursal"]))

    return {
        "resumen": {
            "total_piezas": total_pieces,
            "total_ingreso": total_income.quantize(Decimal("0.01")),
            "dias": len(selected_days),
            "productos": len(por_producto),
            "sucursales": len(por_sucursal),
            "recent_start": recent_start,
            "recent_end": recent_end,
            "recent_rotation_start": recent_rotation_start,
            "comparable_start": comparable_start,
            "comparable_end": comparable_end,
        },
        "por_dia": por_dia,
        "por_producto": por_producto,
        "por_producto_familias": family_groups,
        "por_sucursal": por_sucursal,
    }




@login_required
def PronosticoVentasView(request):
    if not _can_view_pronostico(request.user):
        raise PermissionDenied("No tienes permisos para ver pronosticos de ventas.")

    active_branches = Sucursal.objects.filter(activa=True).order_by("nombre")
    default_branch_ids = set(active_branches.values_list("id", flat=True))
    selected_branch_ids = {int(value) for value in request.GET.getlist("sucursales") if str(value).isdigit()}
    if not selected_branch_ids:
        selected_branch_ids = set(default_branch_ids)
    else:
        selected_branch_ids &= default_branch_ids

    fecha_inicio_raw = (request.GET.get("fecha_inicio") or "").strip()
    fecha_fin_raw = (request.GET.get("fecha_fin") or "").strip()
    resultados = {}
    form_errors = []
    fecha_inicio = None
    fecha_fin = None

    if fecha_inicio_raw or fecha_fin_raw:
        try:
            fecha_inicio = date.fromisoformat(fecha_inicio_raw)
            fecha_fin = date.fromisoformat(fecha_fin_raw)
        except ValueError:
            form_errors.append("Selecciona fechas validas para calcular el pronostico.")
        if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
            form_errors.append("La fecha inicio no puede ser posterior a la fecha fin.")
        if fecha_inicio and fecha_fin and (fecha_fin - fecha_inicio).days > 45:
            form_errors.append("El rango maximo permitido es de 46 dias para mantener el calculo operativo.")
        if not selected_branch_ids:
            form_errors.append("Selecciona al menos una sucursal activa.")

        if not form_errors and fecha_inicio and fecha_fin:
            resultados = _build_pronostico_ventas(
                start_date=fecha_inicio,
                end_date=fecha_fin,
                branch_ids=selected_branch_ids,
            )
            if not resultados:
                messages.warning(request, "No se encontro base suficiente para calcular el pronostico con esos filtros.")

    context = {
        "branches": active_branches,
        "selected_branch_ids": selected_branch_ids,
        "fecha_inicio": fecha_inicio_raw,
        "fecha_fin": fecha_fin_raw,
        "form_errors": form_errors,
        "resultados": resultados,
        "resumen": resultados.get("resumen") if resultados else None,
        "por_dia": resultados.get("por_dia", []) if resultados else [],
        "por_producto_familias": resultados.get("por_producto_familias", []) if resultados else [],
        "por_sucursal": resultados.get("por_sucursal", []) if resultados else [],
    }
    return render(request, "ventas/pronostico.html", context)
