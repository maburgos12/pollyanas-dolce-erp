from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation

from django.utils import timezone

from maestros.models import CostoInsumo, Insumo
from recetas.models import LineaReceta
from reportes.forecast_service import build_daily_forecast_context
from ventas.models import PronosticoGuardado


ZERO = Decimal("0")
SCENARIO_LABELS = {
    "conservador": "Conservador",
    "recomendado": "Recomendado",
    "agresivo": "Agresivo",
}


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _insumo_class(insumo: Insumo) -> dict[str, str]:
    tipo = (getattr(insumo, "tipo_item", "") or "").strip()
    if tipo == Insumo.TIPO_INTERNO:
        return {"key": "interno", "label": "CEDIS produce"}
    if tipo == Insumo.TIPO_EMPAQUE:
        return {"key": "empaque", "label": "Empaque"}
    return {"key": "materia_prima", "label": "Materia prima"}


def _latest_costs_by_insumo(insumo_ids: list[int]) -> dict[int, Decimal]:
    latest: dict[int, Decimal] = {}
    if not insumo_ids:
        return latest
    for cost in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
        latest.setdefault(int(cost.insumo_id), _to_decimal(cost.costo_unitario))
    return latest


def _build_projection_supply_context_from_rows(
    *,
    forecast_rows: list[dict[str, object]],
    target_date: date | None,
    target_label: str,
    forecast_units: Decimal,
    source_type: str,
    source_id: int | None = None,
    source_name: str = "",
    scenario: str = "recomendado",
    date_range_label: str = "",
    exclude_internal: bool = False,
    blockers: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    forecast_rows = [row for row in forecast_rows if _to_decimal(row.get("forecast_qty")) > ZERO]
    recipe_ids = sorted({int(row["recipe_id"]) for row in forecast_rows if row.get("recipe_id")})

    bom_query = (
        LineaReceta.objects.filter(receta_id__in=recipe_ids, insumo_id__isnull=False)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .select_related("insumo", "insumo__unidad_base", "receta")
        .order_by("receta_id", "posicion", "id")
    )
    if exclude_internal:
        bom_query = bom_query.exclude(insumo__tipo_item=Insumo.TIPO_INTERNO)
    bom_lines = list(bom_query)
    bom_by_recipe: dict[int, list[LineaReceta]] = defaultdict(list)
    for line in bom_lines:
        if line.insumo_id and _to_decimal(line.cantidad) > ZERO:
            bom_by_recipe[int(line.receta_id)].append(line)

    unit_costs = _latest_costs_by_insumo(sorted({int(line.insumo_id) for line in bom_lines if line.insumo_id}))
    blockers = list(blockers or [])
    product_rows: list[dict[str, object]] = []
    missing_bom_recipes: set[int] = set()
    insumo_rows: dict[int, dict[str, object]] = {}
    for row in forecast_rows:
        recipe_id = int(row["recipe_id"])
        projection_units = _quantize_units(_to_decimal(row.get("forecast_qty")))
        buffer_units = _quantize_units(_to_decimal(row.get("buffer_units")))
        product_items: list[dict[str, object]] = []
        recipe_bom = bom_by_recipe.get(recipe_id, [])
        if not recipe_bom:
            missing_bom_recipes.add(recipe_id)
            blockers.append(
                {
                    "tipo": "BOM pendiente",
                    "nombre": row.get("recipe_name") or f"Receta {recipe_id}",
                    "detalle": "La receta no tiene líneas de insumo operables para explotar el BOM.",
                }
            )
        for bom_line in recipe_bom:
            gross_required = _quantize_units(projection_units * _to_decimal(bom_line.cantidad))
            if gross_required <= ZERO:
                continue
            unidad_codigo = getattr(getattr(bom_line.insumo, "unidad_base", None), "codigo", "") or ""
            unit_cost = unit_costs.get(int(bom_line.insumo_id), ZERO)
            cost_estimate = (gross_required * unit_cost).quantize(Decimal("0.01")) if unit_cost else ZERO
            article_class = _insumo_class(bom_line.insumo)
            product_items.append(
                {
                    "insumo_id": int(bom_line.insumo_id),
                    "insumo_nombre": bom_line.insumo.nombre,
                    "unidad_codigo": unidad_codigo,
                    "required_gross_qty": gross_required,
                    "formula_qty_per_unit": _quantize_units(_to_decimal(bom_line.cantidad)),
                    "article_class": article_class["key"],
                    "article_class_label": article_class["label"],
                    "cost_estimate": cost_estimate,
                }
            )
            bucket = insumo_rows.setdefault(
                int(bom_line.insumo_id),
                {
                    "insumo_id": int(bom_line.insumo_id),
                    "insumo_nombre": bom_line.insumo.nombre,
                    "unidad_codigo": unidad_codigo,
                    "article_class": article_class["key"],
                    "article_class_label": article_class["label"],
                    "required_gross_qty": ZERO,
                    "cost_estimate": ZERO,
                    "recipes": set(),
                    "branches": set(),
                },
            )
            bucket["required_gross_qty"] += gross_required
            bucket["cost_estimate"] += cost_estimate
            bucket["recipes"].add(row.get("recipe_name") or "")
            bucket["branches"].add(row.get("branch_code") or row.get("branch_name") or "")

        product_rows.append(
            {
                "branch_id": int(row.get("branch_id") or 0),
                "branch_code": row.get("branch_code") or "",
                "branch_name": row.get("branch_name") or "",
                "recipe_id": recipe_id,
                "recipe_name": row.get("recipe_name") or "",
                "forecast_qty": projection_units,
                "buffer_units": buffer_units,
                "gross_basis_qty": projection_units,
                "notes": "Requerimiento bruto por proyección. No descuenta stock actual.",
                "items": product_items,
            }
        )

    product_rows.sort(key=lambda item: (-item["forecast_qty"], item["branch_code"], item["recipe_name"]))
    aggregate_rows = []
    total_cost_estimate = ZERO
    for insumo_id, row in insumo_rows.items():
        cost_estimate = _to_decimal(row["cost_estimate"]).quantize(Decimal("0.01"))
        total_cost_estimate += cost_estimate
        aggregate_rows.append(
            {
                "insumo_id": insumo_id,
                "insumo_nombre": row["insumo_nombre"],
                "unidad_codigo": row["unidad_codigo"],
                "article_class": row["article_class"],
                "article_class_label": row["article_class_label"],
                "required_gross_qty": _quantize_units(_to_decimal(row["required_gross_qty"])),
                "cost_estimate": cost_estimate,
                "recipes_text": ", ".join(sorted(filter(None, row["recipes"]))),
                "branches_text": ", ".join(sorted(filter(None, row["branches"]))),
            }
        )
    aggregate_rows.sort(key=lambda item: (-item["required_gross_qty"], item["insumo_nombre"]))

    unlinked_products = len([row for row in blockers if row.get("tipo") == "Producto sin receta"])
    return {
        "target_date": target_date,
        "target_label": target_label,
        "source_type": source_type,
        "source_id": source_id,
        "source_name": source_name,
        "scenario": scenario,
        "scenario_label": SCENARIO_LABELS.get(scenario, scenario.title()),
        "date_range_label": date_range_label,
        "mode": "PROJECTION_EVENT",
        "uses_stock": False,
        "formula_note": "Simulación BOM: usa piezas proyectadas como base y no crea plan, compra ni autorización.",
        "summary": {
            "projected_products": len(product_rows),
            "projected_insumos": len(aggregate_rows),
            "forecast_units": _quantize_units(forecast_units),
            "unlinked_products": unlinked_products,
            "bom_missing_products": len(missing_bom_recipes),
            "blockers": len(blockers),
            "total_cost_estimate": total_cost_estimate.quantize(Decimal("0.01")),
        },
        "products": product_rows,
        "insumos": aggregate_rows,
        "blockers": blockers,
        "generated_at": timezone.now(),
    }


def build_projection_supply_context(
    *,
    target_date: date,
    top_n: int | None = 24,
    forecast_context: dict[str, object] | None = None,
) -> dict[str, object]:
    forecast_context = forecast_context or build_daily_forecast_context(target_date=target_date, top_n=top_n)
    return _build_projection_supply_context_from_rows(
        forecast_rows=list(forecast_context.get("rows") or []),
        target_date=target_date,
        target_label=forecast_context.get("target_label") or target_date.isoformat(),
        forecast_units=_to_decimal(forecast_context.get("summary", {}).get("forecast_units")),
        source_type="daily_forecast",
        exclude_internal=True,
    )


def _forecast_qty_for_scenario(product: dict[str, object], scenario: str) -> Decimal:
    scenarios = product.get("escenarios") if isinstance(product.get("escenarios"), dict) else {}
    if scenario in scenarios:
        return _to_decimal(scenarios.get(scenario))
    if scenario == "recomendado":
        return _to_decimal(product.get("total_piezas"))
    return ZERO


def _saved_forecast_rows(pronostico: PronosticoGuardado, scenario: str) -> tuple[list[dict[str, object]], list[dict[str, object]], Decimal]:
    payload = pronostico.resultado_json or {}
    rows: list[dict[str, object]] = []
    blockers: list[dict[str, object]] = []
    total_units = ZERO
    branches = payload.get("por_sucursal") if isinstance(payload.get("por_sucursal"), list) else []

    if branches:
        for branch in branches:
            products = branch.get("productos") if isinstance(branch.get("productos"), list) else []
            for product in products:
                qty = _forecast_qty_for_scenario(product, scenario)
                if qty <= ZERO:
                    continue
                recipe_id = product.get("receta_id")
                if not recipe_id:
                    blockers.append(
                        {
                            "tipo": "Producto sin receta",
                            "nombre": product.get("nombre") or "Producto sin nombre",
                            "detalle": "El pronóstico existe, pero el producto no está ligado a una receta ERP.",
                        }
                    )
                    continue
                total_units += qty
                rows.append(
                    {
                        "branch_id": int(branch.get("sucursal_id") or 0),
                        "branch_code": branch.get("codigo") or "",
                        "branch_name": branch.get("sucursal_nombre") or branch.get("sucursal") or "",
                        "recipe_id": int(recipe_id),
                        "recipe_name": product.get("nombre") or "",
                        "forecast_qty": qty,
                        "buffer_units": ZERO,
                    }
                )
        return rows, blockers, total_units

    categories = payload.get("por_categoria") if isinstance(payload.get("por_categoria"), list) else []
    for category in categories:
        products = category.get("productos") if isinstance(category.get("productos"), list) else []
        for product in products:
            qty = _forecast_qty_for_scenario(product, scenario)
            if qty <= ZERO:
                continue
            recipe_id = product.get("receta_id")
            if not recipe_id:
                blockers.append(
                    {
                        "tipo": "Producto sin receta",
                        "nombre": product.get("nombre") or "Producto sin nombre",
                        "detalle": "El pronóstico existe, pero el producto no está ligado a una receta ERP.",
                    }
                )
                continue
            total_units += qty
            rows.append(
                {
                    "branch_id": 0,
                    "branch_code": "TOTAL",
                    "branch_name": "Todas las sucursales",
                    "recipe_id": int(recipe_id),
                    "recipe_name": product.get("nombre") or "",
                    "forecast_qty": qty,
                    "buffer_units": ZERO,
                }
            )
    return rows, blockers, total_units


def build_projection_supply_context_from_saved_forecast(
    pronostico: PronosticoGuardado,
    *,
    scenario: str = "recomendado",
) -> dict[str, object]:
    scenario = (scenario or "recomendado").strip().lower()
    if scenario not in SCENARIO_LABELS:
        scenario = "recomendado"
    rows, blockers, forecast_units = _saved_forecast_rows(pronostico, scenario)
    date_range_label = f"{pronostico.fecha_inicio:%d/%m/%Y} al {pronostico.fecha_fin:%d/%m/%Y}"
    return _build_projection_supply_context_from_rows(
        forecast_rows=rows,
        target_date=pronostico.fecha_fin,
        target_label=date_range_label,
        forecast_units=forecast_units,
        source_type="pronostico_guardado",
        source_id=pronostico.id,
        source_name=pronostico.nombre,
        scenario=scenario,
        date_range_label=date_range_label,
        exclude_internal=False,
        blockers=blockers,
    )
