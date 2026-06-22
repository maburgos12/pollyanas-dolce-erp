from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_CEILING

from django.db.models import Sum

from core.models import Sucursal
from pos_bridge.models import PointSalesDailyProductFact
from recetas.models import Receta
from reportes.models import FactVentaDiaria
from reportes.forecast_service import build_daily_forecast_context
from ventas.services.pronostico_engine import (
    MONTHS_ES,
    ORDEN_CATEGORIAS,
    SPECIAL_CONTEXT_YEAR_WEIGHTS,
    WEEKDAYS_ES,
    _special_context_comparable_days,
    _special_context_explanations,
    _special_day_name,
    _special_days,
)


ZERO = Decimal("0")
ONE = Decimal("1")
THREE_WEEK_LOOKBACK = 3
EVENT_UPLIFT_MIN = Decimal("0.60")
EVENT_UPLIFT_MAX = Decimal("3.50")
NORMAL_CONTEXT_WEEKS = (1, 2, 3, 4)
TEMPORADAS_ALTAS = (
    ("Temporada Reyes", (1, 4), (1, 6)),
    ("Temporada San Valentín", (2, 13), (2, 14)),
    ("Temporada Día del Niño", (4, 28), (4, 30)),
    ("Temporada Día de las Madres", (5, 8), (5, 10)),
    ("Temporada Halloween / Día de Muertos", (10, 31), (11, 2)),
    ("Temporada Navidad", (12, 20), (12, 25)),
    ("Temporada Año Nuevo", (12, 26), (12, 31)),
)


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


def _month_day_between(value: date, start: tuple[int, int], end: tuple[int, int]) -> bool:
    marker = (value.month, value.day)
    return start <= marker <= end if start <= end else marker >= start or marker <= end


def _season_name(value: date) -> str:
    for name, start, end in TEMPORADAS_ALTAS:
        if _month_day_between(value, start, end):
            return name
    return ""


def _same_month_day(value: date, year: int) -> date | None:
    try:
        return date(year, value.month, value.day)
    except ValueError:
        return None


def _context_comparable_days(target_day: date, days: list[date], years_back: int = 3) -> list[date]:
    event_days = _special_context_comparable_days(target_day, days, years_back=years_back)
    if event_days:
        return event_days
    if not _season_name(target_day):
        return []
    return [
        comparable
        for previous_year in range(target_day.year - 1, target_day.year - years_back - 1, -1)
        if (comparable := _same_month_day(target_day, previous_year))
    ]


def _normal_context_days(comparable_day: date, blocked_days: set[date]) -> list[date]:
    candidates = []
    for weeks in NORMAL_CONTEXT_WEEKS:
        candidates.extend([comparable_day - timedelta(days=7 * weeks), comparable_day + timedelta(days=7 * weeks)])
    return [day for day in candidates if day not in blocked_days and not _special_day_name(day) and not _season_name(day)]


def _context_explanations(days: list[date]) -> list[dict]:
    explanations_by_date = {item["fecha_iso"]: item for item in _special_context_explanations(days)}
    for target_day in days:
        if target_day.isoformat() in explanations_by_date or not _season_name(target_day):
            continue
        comparables = _context_comparable_days(target_day, days)
        explanations_by_date[target_day.isoformat()] = {
            "fecha": target_day,
            "fecha_iso": target_day.isoformat(),
            "fecha_label": _date_label(target_day),
            "evento": _season_name(target_day),
            "fecha_evento": target_day.isoformat(),
            "fecha_evento_label": _date_label(target_day),
            "relacion_evento": _season_name(target_day),
            "comparables": [
                {
                    "fecha": comparable_day,
                    "fecha_iso": comparable_day.isoformat(),
                    "fecha_label": _date_label(comparable_day),
                    "anio": comparable_day.year,
                }
                for comparable_day in comparables
            ],
        }
    return [explanations_by_date[key] for key in sorted(explanations_by_date)]


def _temporadas_en_rango(days: list[date]) -> list[dict]:
    seen = set()
    rows = []
    for day in days:
        name = _season_name(day)
        if not name or name in seen:
            continue
        seen.add(name)
        rows.append({"nombre": name, "fecha_iso": day.isoformat(), "fecha": day})
    return rows


def _context_uplift_lookup(days: list[date], branch_ids: set[int]) -> dict[tuple[date, int, int], Decimal]:
    comparable_by_day = {target_day: _context_comparable_days(target_day, days) for target_day in days}
    blocked_days = {day for comparables in comparable_by_day.values() for day in comparables}
    normal_by_comparable = {
        comparable_day: _normal_context_days(comparable_day, blocked_days)
        for comparable_day in blocked_days
    }
    lookup_days = blocked_days | {day for normal_days in normal_by_comparable.values() for day in normal_days}
    if not lookup_days:
        return {}
    rows = (
        FactVentaDiaria.objects.filter(
            fecha__in=lookup_days,
            sucursal_id__in=branch_ids,
            receta_id__isnull=False,
        )
        .values("fecha", "sucursal_id", "receta_id")
        .annotate(qty=Sum("cantidad"))
    )
    qty_by_key = {
        (row["fecha"], int(row["sucursal_id"]), int(row["receta_id"])): _decimal(row.get("qty"))
        for row in rows
        if row.get("sucursal_id") and row.get("receta_id")
    }
    recipe_branch_keys = {(branch_id, recipe_id) for _day, branch_id, recipe_id in qty_by_key}
    uplifts = {}
    for target_day, comparable_days in comparable_by_day.items():
        if not comparable_days:
            continue
        for branch_id, recipe_id in recipe_branch_keys:
            weighted_uplift = ZERO
            used_weight = ZERO
            for index, comparable_day in enumerate(comparable_days):
                event_qty = qty_by_key.get((comparable_day, branch_id, recipe_id), ZERO)
                normal_values = [
                    qty_by_key.get((normal_day, branch_id, recipe_id), ZERO)
                    for normal_day in normal_by_comparable.get(comparable_day, [])
                ]
                normal_values = [value for value in normal_values if value > ZERO]
                if event_qty <= ZERO or not normal_values:
                    continue
                normal_avg = sum(normal_values, ZERO) / Decimal(len(normal_values))
                if normal_avg <= ZERO:
                    continue
                weight = SPECIAL_CONTEXT_YEAR_WEIGHTS[min(index, len(SPECIAL_CONTEXT_YEAR_WEIGHTS) - 1)]
                uplift = max(EVENT_UPLIFT_MIN, min(event_qty / normal_avg, EVENT_UPLIFT_MAX))
                weighted_uplift += uplift * weight
                used_weight += weight
            if used_weight > ZERO:
                uplifts[(target_day, branch_id, recipe_id)] = weighted_uplift / used_weight
    return uplifts


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

    event_explanations = _context_explanations(selected_days)
    event_context_by_day = {item["fecha_iso"]: item for item in event_explanations}
    event_uplifts = _context_uplift_lookup(selected_days, branch_ids) if event_explanations else {}
    has_event_context = bool(event_uplifts)
    products_by_recipe: dict[int, dict] = {}
    branch_products: dict[int, dict[int, dict]] = defaultdict(dict)
    day_totals: dict[date, dict] = {
        day: {
            "fecha": day,
            "fecha_iso": day.isoformat(),
            "fecha_label": _date_label(day),
            "es_fecha_especial": bool(_special_day_name(day)),
            "nombre_especial": _special_day_name(day),
            "contexto_evento": event_context_by_day.get(day.isoformat()),
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

            raw_qty = _decimal(row.get("forecast_qty"))
            uplift = event_uplifts.get((target_day, branch_id, recipe_id), ONE)
            qty_source = "uplift-evento" if uplift != ONE else ""
            forecast_qty = raw_qty * uplift
            qty = _units(forecast_qty)
            if qty <= 0:
                continue
            avg_price = _money(row.get("avg_price"))
            if avg_price <= ZERO:
                amount_source = _money(row.get("forecast_amount"))
                avg_price = _money(amount_source / raw_qty) if raw_qty > ZERO else ZERO
            price = avg_price
            amount = _money(forecast_qty * avg_price)
            ratio = (forecast_qty / raw_qty) if raw_qty > ZERO else Decimal("1")
            conservative = _units(_decimal(row.get("forecast_min_qty")) * ratio)
            aggressive = _units(_decimal(row.get("forecast_max_qty")) * ratio)
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
                    "metodo_usado": "forecast-operativo-3-semanas+uplift-evento" if qty_source else "forecast-operativo-3-semanas",
                    "confianza": 0.0,
                },
            )
            if qty_source:
                product["metodo_usado"] = "forecast-operativo-3-semanas+uplift-evento"
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
            if qty_source:
                branch_product["metodo_usado"] = "forecast-operativo-3-semanas+uplift-evento"
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
            "fechas_especiales": _special_days(selected_days),
            "metodo": "forecast-operativo-3-semanas+uplift-evento" if has_event_context else "forecast-operativo-3-semanas",
            "confianza_promedio": confidence,
            "tendencia_reciente": "ultimas 3 semanas por patron diario",
            "comparable": "uplift historico de evento/temporada sobre baseline reciente" if has_event_context else "mismo dia de la semana + calibracion operativa",
            "comparables_evento": event_explanations,
            "temporadas_altas": _temporadas_en_rango(selected_days),
            "explicacion_modelo": (
                "Proyeccion basada en forecast operativo diario; si el rango cruza evento o temporada alta, multiplica el "
                "baseline reciente por el uplift historico del mismo contexto contra dias normales comparables."
                if has_event_context
                else "Proyeccion basada en el forecast operativo diario: promedio ponderado, factor de dia de semana, "
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
