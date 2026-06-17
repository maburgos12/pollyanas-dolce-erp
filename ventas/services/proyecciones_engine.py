from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_CEILING

from core.models import Sucursal
from pos_bridge.models import PointSalesDailyProductFact
from recetas.models import Receta
from reportes.forecast_service import build_daily_forecast_context
from ventas.services.pronostico_engine import MONTHS_ES, ORDEN_CATEGORIAS, WEEKDAYS_ES


ZERO = Decimal("0")
THREE_WEEK_LOOKBACK = 3


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _decimal(value) -> Decimal:
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _money(value) -> Decimal:
    return _decimal(value).quantize(Decimal("0.01"))


def _units(value) -> int:
    numeric = _decimal(value)
    if numeric <= ZERO:
        return 0
    return int(numeric.to_integral_value(rounding=ROUND_CEILING))


def _date_label(value: date) -> str:
    return f"{WEEKDAYS_ES[value.weekday()]} {value.day} {MONTHS_ES[value.month - 1]}"


def _category_sort_key(category: str | None) -> tuple[int, int, str]:
    label = (category or "Sin categoria").strip()
    try:
        return (0, ORDEN_CATEGORIAS.index(label), label.casefold())
    except ValueError:
        return (1, len(ORDEN_CATEGORIAS), label.casefold())


def _product_sort_key(product: dict) -> tuple[tuple[int, int, str], int, str]:
    return (_category_sort_key(product.get("categoria")), -int(product.get("total_piezas") or 0), product.get("nombre") or "")


def _trend_label(factor: Decimal) -> str:
    if factor > Decimal("1.10"):
        return "sube"
    if factor < Decimal("0.90"):
        return "baja"
    return "estable"


def _empty_result(fecha_inicio: date, fecha_fin: date, sucursales: int = 0) -> dict:
    selected_days = list(_date_range(fecha_inicio, fecha_fin))
    return {
        "fechas": [day.isoformat() for day in selected_days],
        "fechas_tabla": [{"iso": day.isoformat(), "label": _date_label(day)} for day in selected_days],
        "resumen": {
            "total_piezas": 0,
            "total_ingreso": ZERO,
            "dias": len(selected_days),
            "n_productos": 0,
            "productos": 0,
            "n_sucursales": sucursales,
            "sucursales": sucursales,
            "metodo": "forecast-operativo-3-semanas",
            "confianza_promedio": 0,
            "tendencia_reciente": "ultimas 3 semanas por patron diario",
            "comparable": "mismo dia de la semana",
            "explicacion_modelo": "Sin ventas suficientes en el forecast operativo para el rango seleccionado.",
        },
        "por_categoria": [],
        "por_dia": [],
        "por_sucursal": [],
        "por_producto": [],
        "por_producto_familias": [],
    }


def _selected_recipe_ids(skus_incluidos: set[str] | list[str] | None) -> set[int] | None:
    if skus_incluidos is None:
        return None
    skus = {str(value).strip() for value in skus_incluidos if str(value).strip()}
    if not skus:
        return set()

    recipe_ids = set(
        PointSalesDailyProductFact.objects.filter(
            point_product__sku__in=skus,
            receta_id__isnull=False,
        ).values_list("receta_id", flat=True).distinct()
    )
    recipe_ids.update(Receta.objects.filter(codigo_point__in=skus).values_list("id", flat=True))
    return {int(value) for value in recipe_ids if value}


def _build_categories(products: list[dict]) -> list[dict]:
    categories = []
    names = sorted({product.get("categoria") or "Sin categoria" for product in products}, key=_category_sort_key)
    for category in names:
        category_products = [product for product in products if (product.get("categoria") or "Sin categoria") == category]
        category_products.sort(key=_product_sort_key)
        subtotal_pieces = sum(int(product.get("total_piezas") or 0) for product in category_products)
        subtotal_income = sum((_decimal(product.get("total_ingreso")) for product in category_products), ZERO)
        categories.append(
            {
                "categoria": category,
                "productos": category_products,
                "subtotal_piezas": subtotal_pieces,
                "subtotal_ingreso": _money(subtotal_income),
            }
        )
    return categories


def calcular_proyeccion_operativa(
    fecha_inicio: date,
    fecha_fin: date,
    sucursal_ids: set[int] | list[int] | None = None,
    skus_incluidos: set[str] | list[str] | None = None,
) -> dict:
    selected_days = list(_date_range(fecha_inicio, fecha_fin))
    if not selected_days:
        return _empty_result(fecha_inicio, fecha_fin)

    active_branch_ids = set(Sucursal.objects.filter(activa=True).values_list("id", flat=True))
    branch_ids = ({int(value) for value in sucursal_ids} & active_branch_ids) if sucursal_ids else active_branch_ids
    if not branch_ids:
        return _empty_result(fecha_inicio, fecha_fin)

    branch_map = {
        branch.id: branch
        for branch in Sucursal.objects.filter(id__in=branch_ids).only("id", "codigo", "nombre").order_by("nombre")
    }
    selected_recipes = _selected_recipe_ids(skus_incluidos)
    if selected_recipes == set():
        return _empty_result(fecha_inicio, fecha_fin, len(branch_ids))

    products_by_recipe: dict[int, dict] = {}
    branch_products: dict[int, dict[int, dict]] = defaultdict(dict)
    day_totals: dict[date, dict] = {
        day: {
            "fecha": day,
            "fecha_iso": day.isoformat(),
            "fecha_label": _date_label(day),
            "es_fecha_especial": False,
            "nombre_especial": "",
            "contexto_evento": None,
            "total_piezas": 0,
            "total_ingreso": ZERO,
            "top_productos": [],
        }
        for day in selected_days
    }
    product_day_rank: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    factors_by_recipe: dict[int, list[Decimal]] = defaultdict(list)
    validations = []

    for target_day in selected_days:
        forecast = build_daily_forecast_context(
            target_date=target_day,
            lookback_weeks=THREE_WEEK_LOOKBACK,
            top_n=None,
        )
        validations.append(forecast.get("validation") or {})
        for row in forecast.get("rows") or []:
            branch_id = int(row.get("branch_id") or 0)
            recipe_id = int(row.get("recipe_id") or 0)
            if branch_id not in branch_ids or not recipe_id:
                continue
            if selected_recipes is not None and recipe_id not in selected_recipes:
                continue

            qty = _units(row.get("forecast_qty"))
            if qty <= 0:
                continue
            amount = _money(row.get("forecast_amount"))
            price = _money(amount / Decimal(qty)) if qty else ZERO
            conservative = _units(row.get("forecast_min_qty"))
            aggressive = _units(row.get("forecast_max_qty"))
            day_key = target_day.isoformat()
            name = row.get("recipe_name") or "Producto"
            category = row.get("category") or row.get("family") or "Sin categoria"
            family = row.get("family") or category
            trend_factor = _decimal(row.get("trend_factor") or 1)
            factors_by_recipe[recipe_id].append(trend_factor)

            product = products_by_recipe.setdefault(
                recipe_id,
                {
                    "point_product_id": None,
                    "receta_id": recipe_id,
                    "nombre": name,
                    "familia": family,
                    "categoria": category,
                    "categoria_pronostico": category,
                    "precio": price,
                    "dias": {},
                    "total_piezas": 0,
                    "total_ingreso": ZERO,
                    "escenarios": {"conservador": 0, "recomendado": 0, "agresivo": 0},
                    "pct_total": 0.0,
                    "pct_del_total": ZERO,
                    "factor_tendencia": 1.0,
                    "tendencia": "estable",
                    "metodo_usado": "forecast-operativo-3-semanas",
                    "confianza": 0.0,
                },
            )
            product["precio"] = price or product["precio"]
            product["dias"][day_key] = {
                "conservador": product["dias"].get(day_key, {}).get("conservador", 0) + conservative,
                "recomendado": product["dias"].get(day_key, {}).get("recomendado", 0) + qty,
                "agresivo": product["dias"].get(day_key, {}).get("agresivo", 0) + aggressive,
            }
            product["total_piezas"] += qty
            product["total_ingreso"] = _money(product["total_ingreso"] + amount)
            product["escenarios"]["conservador"] += conservative
            product["escenarios"]["recomendado"] += qty
            product["escenarios"]["agresivo"] += aggressive

            branch_product = branch_products[branch_id].setdefault(
                recipe_id,
                {
                    **product,
                    "dias": {},
                    "total_piezas": 0,
                    "total_ingreso": ZERO,
                    "escenarios": {"conservador": 0, "recomendado": 0, "agresivo": 0},
                },
            )
            branch_product["dias"][day_key] = {
                "conservador": conservative,
                "recomendado": qty,
                "agresivo": aggressive,
            }
            branch_product["total_piezas"] += qty
            branch_product["total_ingreso"] = _money(branch_product["total_ingreso"] + amount)
            branch_product["escenarios"]["conservador"] += conservative
            branch_product["escenarios"]["recomendado"] += qty
            branch_product["escenarios"]["agresivo"] += aggressive

            day_totals[target_day]["total_piezas"] += qty
            day_totals[target_day]["total_ingreso"] = _money(day_totals[target_day]["total_ingreso"] + amount)
            product_day_rank[target_day][name] += qty

    total_pieces = sum(product["total_piezas"] for product in products_by_recipe.values())
    total_income = sum((_decimal(product["total_ingreso"]) for product in products_by_recipe.values()), ZERO)

    def finalize_product(product: dict) -> dict:
        recipe_id = int(product.get("receta_id") or 0)
        product["dias_lista"] = []
        product["por_dia"] = {}
        for day in selected_days:
            day_key = day.isoformat()
            values = product["dias"].get(day_key, {"conservador": 0, "recomendado": 0, "agresivo": 0})
            product["dias_lista"].append(
                {
                    "fecha": day,
                    "fecha_iso": day_key,
                    "fecha_label": _date_label(day),
                    "conservador": values.get("conservador", 0),
                    "recomendado": values.get("recomendado", 0),
                    "agresivo": values.get("agresivo", 0),
                }
            )
            product["por_dia"][day_key] = values.get("recomendado", 0)
        trend_factor = sum(factors_by_recipe.get(recipe_id, [Decimal("1")]), ZERO) / Decimal(
            len(factors_by_recipe.get(recipe_id, [Decimal("1")]))
        )
        product["factor_tendencia"] = float(trend_factor.quantize(Decimal("0.01")))
        product["tendencia"] = _trend_label(trend_factor)
        product["pct_del_total"] = (
            (Decimal(product["total_piezas"]) / Decimal(total_pieces) * Decimal("100")).quantize(Decimal("0.01"))
            if total_pieces
            else ZERO
        )
        product["pct_total"] = float(product["pct_del_total"])
        product["total_ingreso"] = _money(product["total_ingreso"])
        return product

    products = [finalize_product(product) for product in products_by_recipe.values()]
    products.sort(key=_product_sort_key)

    por_dia = []
    for day in selected_days:
        row = day_totals[day]
        row["top_productos"] = [
            {"nombre": name, "piezas": qty, "total_piezas": qty}
            for name, qty in sorted(product_day_rank[day].items(), key=lambda item: (-item[1], item[0]))[:5]
        ]
        row["top_producto"] = row["top_productos"][0] if row["top_productos"] else None
        por_dia.append(row)

    por_sucursal = []
    for branch_id, products_by_id in branch_products.items():
        branch_products_list = [finalize_product(product) for product in products_by_id.values()]
        branch_total_pieces = sum(product["total_piezas"] for product in branch_products_list)
        branch_total_income = sum((_decimal(product["total_ingreso"]) for product in branch_products_list), ZERO)
        branch = branch_map[branch_id]
        por_sucursal.append(
            {
                "sucursal_id": branch_id,
                "sucursal_nombre": branch.nombre,
                "sucursal": branch.nombre,
                "codigo": branch.codigo,
                "total_piezas": branch_total_pieces,
                "total_ingreso": _money(branch_total_income),
                "categorias": _build_categories(branch_products_list),
                "productos": sorted(branch_products_list, key=_product_sort_key),
            }
        )
    por_sucursal.sort(key=lambda item: item["sucursal_nombre"])

    validation_wapes = [_decimal(item.get("wape_pct")) for item in validations if item.get("wape_pct") is not None]
    confidence = 0.0
    if validation_wapes:
        avg_wape = sum(validation_wapes, ZERO) / Decimal(len(validation_wapes))
        confidence = float(max(ZERO, Decimal("1") - (avg_wape / Decimal("100"))).quantize(Decimal("0.001")))

    return {
        "fechas": [day.isoformat() for day in selected_days],
        "fechas_tabla": [{"iso": day.isoformat(), "label": _date_label(day)} for day in selected_days],
        "resumen": {
            "total_piezas": total_pieces,
            "total_ingreso": _money(total_income),
            "dias": len(selected_days),
            "n_productos": len(products),
            "productos": len(products),
            "n_sucursales": len(por_sucursal),
            "sucursales": len(por_sucursal),
            "fechas_especiales": [],
            "metodo": "forecast-operativo-3-semanas",
            "confianza_promedio": confidence,
            "tendencia_reciente": "ultimas 3 semanas por patron diario",
            "comparable": "mismo dia de la semana + calibracion operativa",
            "explicacion_modelo": (
                "Proyeccion basada en el forecast operativo diario: promedio ponderado, factor de dia de semana, "
                "tendencia reciente, volatilidad y calibracion por sucursal/familia/patron semanal."
            ),
        },
        "por_categoria": _build_categories(products),
        "por_dia": por_dia,
        "por_sucursal": por_sucursal,
        "por_producto": products,
        "por_producto_familias": [
            {
                "familia": category["categoria"],
                "total_piezas": category["subtotal_piezas"],
                "total_ingreso": category["subtotal_ingreso"],
                "rows": category["productos"],
            }
            for category in _build_categories(products)
        ],
    }
