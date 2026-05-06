from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
from django.db.models import Avg, Max, Q, Sum

from core.models import Sucursal
from pos_bridge.models import PointProduct, PointSalesDailyProductFact
from recetas.models import Receta

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
except ImportError:  # pragma: no cover - production dependency is installed in the container.
    ExponentialSmoothing = None


HISTORY_START = date(2022, 1, 1)
TRAINING_DAYS = 730
FECHAS_ESPECIALES = {
    (1, 6): "Reyes",
    (2, 14): "San Valentín",
    (4, 30): "Día del Niño",
    (5, 10): "Día de las Madres",
    (6, 21): "Día del Padre",
    (10, 31): "Halloween",
    (11, 2): "Día de Muertos",
    (12, 24): "Nochebuena",
    (12, 25): "Navidad",
    (12, 31): "Año Nuevo",
}
ORDEN_CATEGORIAS = [
    "Bollo",
    "Empanadas",
    "Galletas",
    "Cheesecake",
    "Individual",
    "Pastel Grande",
    "Pastel Mediano",
    "Pastel Chico",
    "Pastel Mini",
    "Rebanada",
    "Pay Grande",
    "Pay Mediano",
    "Vasos Preparados Grande",
    "Otros postres",
    "Accesorios de repostería",
    "Alegría",
    "Café",
    "Chico",
    "Clarita",
    "Coca-cola",
    "D-rigaldi",
    "Glow",
    "Grande",
    "Granmark",
    "Hielo y agua mar de cortéz",
    "Industrias lec",
    "Letrero B",
    "Letreros",
    "Media Plancha",
    "Mediano",
    "Pillines",
    "Plásticos",
    "REGALOS",
    "Rosca",
    "San Valentín",
    "TE",
    "Vaso Preparado Mini",
    "Vasos Grande",
    "Vasos Mini",
    "Vela Sparklers",
    "Velas Sparklers",
    "Viva party",
    "Xtudio",
]
CATEGORY_INDEX = {category: index for index, category in enumerate(ORDEN_CATEGORIAS)}
WEEKDAYS_ES = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
MONTHS_ES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

FORECASTABLE_TERMS = (
    "bollo",
    "empanada",
    "galleta",
    "cheesecake",
    "pastel",
    "pay",
    "vaso",
    "fresas con crema",
)
EXCLUDED_TERMS = (
    "sobre pedido",
    "sp ",
    " sp",
    "topping",
    "vela",
    "velas",
    "caja ",
    "servicio",
    "domicilio",
    "extra ",
    "letrero",
    "empaque",
    "coca-cola",
    "cafe",
    "starbucks",
    "encendedor",
    "tarjeta",
)


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


def _ceil(value: Decimal | float) -> int:
    numeric = float(value)
    return int(math.ceil(numeric)) if numeric > 0 else 0


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _money(value: Decimal) -> Decimal:
    return _decimal(value).quantize(Decimal("0.01"))


def _date_label(value: date) -> str:
    return f"{WEEKDAYS_ES[value.weekday()]} {value.day} {MONTHS_ES[value.month - 1]}"


def _period_label(start: date, end: date) -> str:
    return f"{start.day} {MONTHS_ES[start.month - 1].lower()} {start.year} - {end.day} {MONTHS_ES[end.month - 1].lower()} {end.year}"


def _trend_label(factor: float) -> str:
    if factor > 1.10:
        return "sube"
    if factor < 0.90:
        return "baja"
    return "estable"


def _clean_label(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _category_sort_key(category: str | None) -> tuple[int, int, str]:
    label = _clean_label(category)
    if label in CATEGORY_INDEX:
        return (0, CATEGORY_INDEX[label], label.casefold())
    return (1, len(CATEGORY_INDEX), label.casefold())


def _product_sort_key(product: dict) -> tuple[tuple[int, int, str], int, str]:
    return (_category_sort_key(product.get("categoria")), -product["total_piezas"], product["nombre"])


def categoria_producto(
    *,
    point_category: str | None,
    familia: str | None,
    receta_codigo_point: str | None = None,
    category_by_sku: dict[str, str] | None = None,
) -> str:
    codigo_point = _clean_label(receta_codigo_point)
    if codigo_point and category_by_sku:
        category = _clean_label(category_by_sku.get(codigo_point))
        if category:
            return category

    category = _clean_label(point_category)
    if category:
        return category

    fallback = _clean_label(familia)
    return fallback or "Sin categoría"


def _is_forecastable_product(*, nombre: str, familia: str, receta: Receta | None) -> bool:
    haystack = f"{nombre} {familia} {(receta.categoria if receta else '')}".casefold()
    if any(term in haystack for term in EXCLUDED_TERMS):
        return False
    if receta:
        return receta.tipo == Receta.TIPO_PRODUCTO_FINAL
    return any(term in haystack for term in FORECASTABLE_TERMS)


def _empty_result(start_date: date, end_date: date, branch_count: int = 0) -> dict:
    selected_days = list(_date_range(start_date, end_date))
    return {
        "fechas": [day.isoformat() for day in selected_days],
        "fechas_tabla": [{"iso": day.isoformat(), "label": _date_label(day)} for day in selected_days],
        "resumen": {
            "total_piezas": 0,
            "total_ingreso": Decimal("0.00"),
            "dias": len(selected_days),
            "n_productos": 0,
            "productos": 0,
            "n_sucursales": branch_count,
            "sucursales": branch_count,
            "fechas_especiales": _special_days(selected_days),
            "metodo": "sin-datos",
            "confianza_promedio": 0.0,
            "tendencia_reciente": "",
            "comparable": "",
        },
        "por_categoria": [],
        "por_dia": [],
        "por_sucursal": [],
        "por_producto": [],
        "por_producto_familias": [],
    }


def _special_days(days: list[date]) -> list[dict]:
    return [
        {"fecha": day, "fecha_iso": day.isoformat(), "nombre": FECHAS_ESPECIALES[(day.month, day.day)]}
        for day in days
        if (day.month, day.day) in FECHAS_ESPECIALES
    ]


def _fit_holt_winters(series: pd.Series, horizon: int) -> tuple[pd.Series, float, str]:
    if ExponentialSmoothing is None or horizon <= 0 or series.sum() <= 0:
        return pd.Series(dtype=float), 0.0, "promedio-simple"

    clean_series = series.astype(float).clip(lower=0).tail(730)
    attempts = []
    if len(clean_series) >= 21:
        attempts.append(7)

    for seasonal_periods in attempts:
        try:
            model = ExponentialSmoothing(
                clean_series,
                trend="add",
                seasonal="add",
                seasonal_periods=seasonal_periods,
                initialization_method="estimated",
            )
            fit = model.fit(
                optimized=False,
                smoothing_level=0.25,
                smoothing_trend=0.05,
                smoothing_seasonal=0.20,
                remove_bias=False,
            )
            forecast = fit.forecast(horizon).clip(lower=0)
            fitted = fit.fittedvalues.reindex(clean_series.index).fillna(0)
            residual = clean_series - fitted
            sse = float(np.square(residual).sum())
            centered = clean_series - float(clean_series.mean())
            sst = float(np.square(centered).sum())
            confidence = _clamp(1 - (sse / sst), 0.0, 1.0) if sst > 0 else 0.0
            return forecast, confidence, "holt-winters"
        except Exception:
            continue

    return pd.Series(dtype=float), 0.0, "promedio-simple"


def _simple_average_forecast(series: pd.Series, horizon: int) -> pd.Series:
    if horizon <= 0:
        return pd.Series(dtype=float)
    if series.empty:
        avg = 0.0
    else:
        window = series.tail(min(30, len(series)))
        avg = float(window.mean()) if len(window) else 0.0
    return pd.Series([max(avg, 0.0)] * horizon)


def _series_from_history(history: dict[date, Decimal], *, end_date: date) -> pd.Series:
    positive_dates = [day for day, qty in history.items() if qty > 0]
    start_date = min(positive_dates) if positive_dates else end_date
    index = pd.date_range(start=start_date, end=end_date, freq="D")
    values = [float(history.get(day.date(), Decimal("0"))) for day in index]
    return pd.Series(values, index=index)


def _forecastable_queryset(branch_ids: set[int]):
    forecastable_filter = Q(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
    for term in FORECASTABLE_TERMS:
        forecastable_filter |= Q(point_product__name__icontains=term) | Q(point_product__category__icontains=term)

    return (
        PointSalesDailyProductFact.objects.filter(
            point_product_id__isnull=False,
            point_product__precio_temporada=False,
            point_product__active=True,
            branch__erp_branch_id__in=branch_ids,
            branch__erp_branch__activa=True,
        )
        .filter(forecastable_filter)
        .exclude(
            Q(point_product__name__icontains="sobre pedido")
            | Q(receta__nombre__icontains="sobre pedido")
            | Q(point_product__name__icontains="sp ")
            | Q(point_product__name__icontains=" sp")
            | Q(receta__nombre__icontains="sp ")
            | Q(receta__nombre__icontains=" sp")
        )
    )


def calcular_pronostico(fecha_inicio: date, fecha_fin: date, sucursal_ids: set[int] | list[int] | None = None) -> dict:
    selected_days = list(_date_range(fecha_inicio, fecha_fin))
    if not selected_days:
        return _empty_result(fecha_inicio, fecha_fin)

    active_branches = Sucursal.objects.filter(activa=True).order_by("nombre")
    active_branch_ids = set(active_branches.values_list("id", flat=True))
    if sucursal_ids:
        branch_ids = {int(value) for value in sucursal_ids} & active_branch_ids
    else:
        branch_ids = set(active_branch_ids)
    if not branch_ids:
        return _empty_result(fecha_inicio, fecha_fin)

    branch_map = {
        branch.id: branch
        for branch in Sucursal.objects.filter(id__in=branch_ids).only("id", "codigo", "nombre").order_by("nombre")
    }
    base_qs = _forecastable_queryset(branch_ids)
    latest_sale_date = base_qs.filter(sale_date__lt=fecha_inicio).aggregate(max_date=Max("sale_date")).get("max_date")
    if not latest_sale_date:
        return _empty_result(fecha_inicio, fecha_fin, len(branch_ids))

    history_end = min(latest_sale_date, fecha_inicio - timedelta(days=1))
    trend_start = history_end - timedelta(days=29)
    trend_comparable_start = trend_start - timedelta(days=364)
    trend_comparable_end = history_end - timedelta(days=364)
    context_start = history_end - timedelta(days=89)
    context_comparable_start = context_start - timedelta(days=364)
    context_comparable_end = history_end - timedelta(days=364)
    recent_pair_rows = list(
        base_qs.filter(sale_date__range=(trend_start, history_end))
        .values("point_product_id", "branch__erp_branch_id")
        .annotate(qty=Sum("total_cantidad"))
        .filter(qty__gt=0)
    )
    recent_pairs = {
        (int(row["point_product_id"]), int(row["branch__erp_branch_id"]))
        for row in recent_pair_rows
        if row.get("point_product_id") and row.get("branch__erp_branch_id")
    }
    if not recent_pairs:
        return _empty_result(fecha_inicio, fecha_fin, len(branch_ids))
    recent_product_ids = {product_id for product_id, _branch_id in recent_pairs}

    history_start = max(HISTORY_START, history_end - timedelta(days=TRAINING_DAYS))
    rows = list(
        base_qs.filter(sale_date__range=(history_start, history_end), point_product_id__in=recent_product_ids)
        .values("point_product_id", "branch__erp_branch_id", "sale_date", "receta_id")
        .annotate(qty=Sum("total_cantidad"), revenue=Sum("total_venta_neta"))
        .order_by("point_product_id", "branch__erp_branch_id", "sale_date")
    )
    if not rows:
        return _empty_result(fecha_inicio, fecha_fin, len(branch_ids))

    product_ids = {int(row["point_product_id"]) for row in rows if row.get("point_product_id")}
    product_map = {
        product.id: product
        for product in PointProduct.objects.filter(id__in=product_ids).only("id", "sku", "name", "category", "precio")
    }
    recipe_by_product: dict[int, int] = {}
    recipe_ids = set()
    for row in rows:
        if row.get("receta_id"):
            product_id = int(row["point_product_id"])
            recipe_id = int(row["receta_id"])
            recipe_by_product.setdefault(product_id, recipe_id)
            recipe_ids.add(recipe_id)
    recipe_map = {
        receta.id: receta
        for receta in Receta.objects.filter(id__in=recipe_ids).only("id", "codigo_point", "nombre", "familia", "categoria", "tipo")
    }
    recipe_codes = {_clean_label(receta.codigo_point) for receta in recipe_map.values() if _clean_label(receta.codigo_point)}
    category_by_sku = {
        _clean_label(row["sku"]): _clean_label(row["category"])
        for row in PointProduct.objects.filter(sku__in=recipe_codes)
        .exclude(category="")
        .values("sku", "category")
    }

    histories: dict[tuple[int, int], dict[date, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    recent_pair_qty: dict[tuple[int, int], Decimal] = defaultdict(Decimal)
    revenue_by_product_recent: dict[int, dict[str, Decimal]] = defaultdict(lambda: {"qty": Decimal("0"), "revenue": Decimal("0")})
    revenue_by_product_all: dict[int, dict[str, Decimal]] = defaultdict(lambda: {"qty": Decimal("0"), "revenue": Decimal("0")})
    for row in rows:
        product_id = int(row["point_product_id"])
        branch_id = int(row["branch__erp_branch_id"])
        if (product_id, branch_id) not in recent_pairs:
            continue
        sale_date = row["sale_date"]
        qty = _decimal(row.get("qty"))
        revenue = _decimal(row.get("revenue"))
        histories[(product_id, branch_id)][sale_date] += qty
        revenue_by_product_all[product_id]["qty"] += qty
        revenue_by_product_all[product_id]["revenue"] += revenue
        if trend_start <= sale_date <= history_end:
            recent_pair_qty[(product_id, branch_id)] += qty
            revenue_by_product_recent[product_id]["qty"] += qty
            revenue_by_product_recent[product_id]["revenue"] += revenue

    price_by_product: dict[int, Decimal] = {}
    for product_id in product_ids:
        stats = revenue_by_product_recent[product_id]
        if stats["qty"] <= 0:
            stats = revenue_by_product_all[product_id]
        if stats["qty"] > 0 and stats["revenue"] > 0:
            price_by_product[product_id] = _money(stats["revenue"] / stats["qty"])

    for row in (
        base_qs.filter(point_product_id__in=product_ids, point_product__precio__isnull=False, point_product__precio_activo=True)
        .values("point_product_id")
        .annotate(avg_price=Avg("point_product__precio"))
    ):
        product_id = int(row["point_product_id"])
        if product_id not in price_by_product and row.get("avg_price") is not None:
            price_by_product[product_id] = _money(_decimal(row["avg_price"]))

    forecast_horizon = max(1, (fecha_fin - history_end).days)
    product_totals: dict[int, dict] = {}
    branch_products: dict[int, dict[int, dict]] = defaultdict(dict)
    day_totals: dict[date, dict] = {
        day: {
            "fecha": day,
            "fecha_iso": day.isoformat(),
            "fecha_label": _date_label(day),
            "es_fecha_especial": (day.month, day.day) in FECHAS_ESPECIALES,
            "nombre_especial": FECHAS_ESPECIALES.get((day.month, day.day)),
            "total_piezas": 0,
            "total_ingreso": Decimal("0.00"),
            "top_productos": [],
        }
        for day in selected_days
    }
    product_day_rank: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    method_counts = defaultdict(int)
    confidence_values: list[float] = []

    for (product_id, branch_id), history in histories.items():
        if recent_pair_qty.get((product_id, branch_id), Decimal("0")) <= 0:
            continue
        product = product_map.get(product_id)
        if not product or branch_id not in branch_map:
            continue
        receta = recipe_map.get(recipe_by_product.get(product_id))
        name = product.name or (receta.nombre if receta else "Producto")
        family = (receta.familia if receta else "") or product.category or "Sin familia"
        if not _is_forecastable_product(nombre=name, familia=family, receta=receta):
            continue

        sale_days = sum(1 for qty in history.values() if qty > 0)
        series = _series_from_history(history, end_date=history_end)
        pair_recent_qty = recent_pair_qty.get((product_id, branch_id), Decimal("0"))
        if sale_days >= 30 and pair_recent_qty >= Decimal("50"):
            hw_forecast, confidence, method = _fit_holt_winters(series, forecast_horizon)
        else:
            hw_forecast, confidence, method = pd.Series(dtype=float), 0.0, "promedio-simple"
        if hw_forecast.empty:
            hw_forecast = _simple_average_forecast(series, forecast_horizon)
            confidence = min(0.50, sale_days / 60) if sale_days else 0.0
            method = "promedio-simple"

        recent_avg = sum(
            (qty for day, qty in history.items() if trend_start <= day <= history_end),
            Decimal("0"),
        ) / Decimal("30")
        comparable_avg = sum(
            (qty for day, qty in history.items() if trend_comparable_start <= day <= trend_comparable_end),
            Decimal("0"),
        ) / Decimal("30")
        factor = _clamp(float((recent_avg + Decimal("1.0")) / (comparable_avg + Decimal("1.0"))), 0.70, 1.50)
        category = categoria_producto(
            point_category=product.category,
            familia=receta.familia if receta else family,
            receta_codigo_point=receta.codigo_point if receta else "",
            category_by_sku=category_by_sku,
        )
        price = price_by_product.get(product_id, Decimal("0.00"))

        day_values = []
        total_recommended = 0
        total_conservative = 0
        total_aggressive = 0
        total_income = Decimal("0.00")
        used_method = method

        for day in selected_days:
            forecast_index = max(0, (day - history_end).days - 1)
            if forecast_index < len(hw_forecast):
                hw_value = Decimal(str(float(hw_forecast.iloc[forecast_index])))
            else:
                hw_value = Decimal(str(float(hw_forecast.iloc[-1]))) if not hw_forecast.empty else Decimal("0")
            model_value = max(Decimal("0"), hw_value * Decimal(str(factor)))
            special_name = FECHAS_ESPECIALES.get((day.month, day.day))
            anchor_value = history.get(date(day.year - 1, day.month, day.day), Decimal("0")) if special_name else Decimal("0")
            if special_name and anchor_value > 0:
                final_value = (anchor_value * Decimal("0.70")) + (model_value * Decimal("0.30"))
                used_method = f"{method}+fecha-especial"
            else:
                final_value = model_value

            conservador = _ceil(final_value * Decimal("0.90"))
            recomendado = _ceil(final_value)
            agresivo = _ceil(final_value * Decimal("1.12"))
            if recomendado <= 0 and conservador <= 0 and agresivo <= 0:
                continue

            income = _money(Decimal(recomendado) * price)
            total_recommended += recomendado
            total_conservative += conservador
            total_aggressive += agresivo
            total_income += income
            day_values.append(
                {
                    "fecha": day,
                    "fecha_iso": day.isoformat(),
                    "fecha_label": _date_label(day),
                    "conservador": conservador,
                    "recomendado": recomendado,
                    "agresivo": agresivo,
                }
            )
            day_totals[day]["total_piezas"] += recomendado
            day_totals[day]["total_ingreso"] += income
            product_day_rank[day][name] += recomendado

        if total_recommended <= 0:
            continue

        product_payload = {
            "point_product_id": product_id,
            "receta_id": receta.id if receta else None,
            "nombre": name,
            "familia": family,
            "categoria": category,
            "categoria_pronostico": category,
            "precio": price,
            "dias": {
                item["fecha_iso"]: {
                    "conservador": item["conservador"],
                    "recomendado": item["recomendado"],
                    "agresivo": item["agresivo"],
                }
                for item in day_values
            },
            "dias_lista": day_values,
            "por_dia": {item["fecha_iso"]: item["recomendado"] for item in day_values},
            "escenarios": {
                "conservador": total_conservative,
                "recomendado": total_recommended,
                "agresivo": total_aggressive,
            },
            "total_piezas": total_recommended,
            "total_ingreso": _money(total_income),
            "pct_total": 0.0,
            "pct_del_total": Decimal("0.00"),
            "factor_tendencia": round(factor, 2),
            "tendencia": _trend_label(factor),
            "metodo_usado": used_method,
            "confianza": round(confidence, 3),
        }
        branch_products[branch_id][product_id] = dict(product_payload)

        aggregate = product_totals.get(product_id)
        if aggregate is None:
            aggregate = dict(product_payload)
            aggregate["_factor_weight"] = factor * total_recommended
            aggregate["_confidence_sum"] = confidence
            aggregate["_pairs"] = 1
            aggregate["_method_counts"] = {used_method: 1}
            product_totals[product_id] = aggregate
        else:
            aggregate["total_piezas"] += total_recommended
            aggregate["total_ingreso"] = _money(aggregate["total_ingreso"] + total_income)
            aggregate["escenarios"]["conservador"] += total_conservative
            aggregate["escenarios"]["recomendado"] += total_recommended
            aggregate["escenarios"]["agresivo"] += total_aggressive
            for item in day_values:
                day_key = item["fecha_iso"]
                day_bucket = aggregate["dias"].setdefault(
                    day_key,
                    {"conservador": 0, "recomendado": 0, "agresivo": 0},
                )
                day_bucket["conservador"] += item["conservador"]
                day_bucket["recomendado"] += item["recomendado"]
                day_bucket["agresivo"] += item["agresivo"]
            aggregate["_factor_weight"] += factor * total_recommended
            aggregate["_confidence_sum"] += confidence
            aggregate["_pairs"] += 1
            aggregate["_method_counts"][used_method] = aggregate["_method_counts"].get(used_method, 0) + 1
        method_counts[used_method] += 1
        confidence_values.append(confidence)

    total_pieces = sum(product["total_piezas"] for product in product_totals.values())
    total_income = sum((product["total_ingreso"] for product in product_totals.values()), Decimal("0.00"))
    for product in product_totals.values():
        factor_weight = product.pop("_factor_weight", product.get("factor_tendencia", 1.0))
        confidence_sum = product.pop("_confidence_sum", product.get("confianza", 0.0))
        pairs = product.pop("_pairs", 1) or 1
        method_counts_for_product = product.pop("_method_counts", {})
        weighted_factor = factor_weight / product["total_piezas"] if product["total_piezas"] else 1.0
        product["factor_tendencia"] = round(weighted_factor, 2)
        product["tendencia"] = _trend_label(weighted_factor)
        product["confianza"] = round(confidence_sum / pairs, 3)
        if method_counts_for_product:
            product["metodo_usado"] = max(method_counts_for_product.items(), key=lambda item: item[1])[0]
        product["dias_lista"] = []
        product["por_dia"] = {}
        for day in selected_days:
            day_key = day.isoformat()
            day_data = product["dias"].get(day_key, {"conservador": 0, "recomendado": 0, "agresivo": 0})
            product["dias_lista"].append(
                {
                    "fecha": day,
                    "fecha_iso": day_key,
                    "fecha_label": _date_label(day),
                    "conservador": day_data.get("conservador", 0),
                    "recomendado": day_data.get("recomendado", 0),
                    "agresivo": day_data.get("agresivo", 0),
                }
            )
            product["por_dia"][day_key] = day_data.get("recomendado", 0)
        pct = (Decimal(product["total_piezas"]) / Decimal(total_pieces) * Decimal("100")) if total_pieces else Decimal("0")
        product["pct_del_total"] = pct.quantize(Decimal("0.01"))
        product["pct_total"] = float(product["pct_del_total"])

    categories = _build_categories(list(product_totals.values()))
    por_dia = []
    for day in selected_days:
        row = day_totals[day]
        row["total_ingreso"] = _money(row["total_ingreso"])
        row["top_productos"] = [
            {"nombre": name, "piezas": qty, "total_piezas": qty}
            for name, qty in sorted(product_day_rank[day].items(), key=lambda item: (-item[1], item[0]))[:5]
        ]
        row["top_producto"] = row["top_productos"][0] if row["top_productos"] else None
        por_dia.append(row)

    por_sucursal = []
    for branch_id, products_by_id in branch_products.items():
        products = list(products_by_id.values())
        branch_total_pieces = sum(product["total_piezas"] for product in products)
        branch_total_income = sum((product["total_ingreso"] for product in products), Decimal("0.00"))
        for product in products:
            pct = (Decimal(product["total_piezas"]) / Decimal(branch_total_pieces) * Decimal("100")) if branch_total_pieces else Decimal("0")
            product["pct_del_total"] = pct.quantize(Decimal("0.01"))
            product["pct_total"] = float(product["pct_del_total"])
        por_sucursal.append(
            {
                "sucursal_id": branch_id,
                "sucursal_nombre": branch_map[branch_id].nombre,
                "sucursal": branch_map[branch_id].nombre,
                "codigo": branch_map[branch_id].codigo,
                "total_piezas": branch_total_pieces,
                "total_ingreso": _money(branch_total_income),
                "categorias": _build_categories(products),
                "productos": sorted(
                    products,
                    key=_product_sort_key,
                ),
            }
        )
    por_sucursal.sort(key=lambda item: item["sucursal_nombre"])

    main_method = "holt-winters" if any("holt-winters" in method for method in method_counts) else "promedio-simple"
    if _special_days(selected_days):
        main_method = f"{main_method}+fecha-especial"
    confidence_avg = round(sum(confidence_values) / len(confidence_values), 3) if confidence_values else 0.0
    fechas_especiales = _special_days(selected_days)

    return {
        "fechas": [day.isoformat() for day in selected_days],
        "fechas_tabla": [{"iso": day.isoformat(), "label": _date_label(day)} for day in selected_days],
        "resumen": {
            "total_piezas": total_pieces,
            "total_ingreso": _money(total_income),
            "dias": len(selected_days),
            "n_productos": len(product_totals),
            "productos": len(product_totals),
            "n_sucursales": len(por_sucursal),
            "sucursales": len(por_sucursal),
            "fechas_especiales": fechas_especiales,
            "metodo": main_method,
            "confianza_promedio": confidence_avg,
            "tendencia_reciente": _period_label(context_start, history_end),
            "comparable": _period_label(context_comparable_start, context_comparable_end),
            "trend_30": _period_label(trend_start, history_end),
            "trend_30_comparable": _period_label(trend_comparable_start, trend_comparable_end),
        },
        "por_categoria": categories,
        "por_dia": por_dia,
        "por_sucursal": por_sucursal,
        "por_producto": sorted(
            product_totals.values(),
            key=_product_sort_key,
        ),
        "por_producto_familias": [
            {
                "familia": category["categoria"],
                "total_piezas": category["subtotal_piezas"],
                "total_ingreso": category["subtotal_ingreso"],
                "rows": category["productos"],
            }
            for category in categories
        ],
    }


def _build_categories(products: list[dict]) -> list[dict]:
    categories = []
    category_names = sorted({product.get("categoria") or "Sin categoría" for product in products}, key=_category_sort_key)
    for category in category_names:
        category_products = [product for product in products if product.get("categoria") == category]
        if not category_products:
            continue
        category_products.sort(key=lambda item: (-item["total_piezas"], item["nombre"]))
        subtotal_pieces = sum(product["total_piezas"] for product in category_products)
        subtotal_income = sum((product["total_ingreso"] for product in category_products), Decimal("0.00"))
        categories.append(
            {
                "categoria": category,
                "productos": category_products,
                "subtotal_piezas": subtotal_pieces,
                "subtotal_ingreso": _money(subtotal_income),
            }
        )
    return categories
