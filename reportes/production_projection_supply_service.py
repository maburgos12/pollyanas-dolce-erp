from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation

from django.utils import timezone

from recetas.models import LineaReceta
from reportes.forecast_service import build_daily_forecast_context


ZERO = Decimal("0")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def build_projection_supply_context(
    *,
    target_date: date,
    top_n: int | None = 24,
    forecast_context: dict[str, object] | None = None,
) -> dict[str, object]:
    forecast_context = forecast_context or build_daily_forecast_context(target_date=target_date, top_n=top_n)
    forecast_rows = [row for row in (forecast_context.get("rows") or []) if _to_decimal(row.get("forecast_qty")) > ZERO]
    recipe_ids = sorted({int(row["recipe_id"]) for row in forecast_rows if row.get("recipe_id")})

    bom_lines = list(
        LineaReceta.objects.filter(receta_id__in=recipe_ids, insumo_id__isnull=False)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .exclude(insumo__tipo_item="INSUMO_INTERNO")
        .select_related("insumo", "insumo__unidad_base", "receta")
        .order_by("receta_id", "posicion", "id")
    )
    bom_by_recipe: dict[int, list[LineaReceta]] = defaultdict(list)
    for line in bom_lines:
        if line.insumo_id and _to_decimal(line.cantidad) > ZERO:
            bom_by_recipe[int(line.receta_id)].append(line)

    product_rows: list[dict[str, object]] = []
    insumo_rows: dict[int, dict[str, object]] = {}
    for row in forecast_rows:
        recipe_id = int(row["recipe_id"])
        projection_units = _quantize_units(_to_decimal(row.get("forecast_qty")))
        buffer_units = _quantize_units(_to_decimal(row.get("buffer_units")))
        product_items: list[dict[str, object]] = []
        for bom_line in bom_by_recipe.get(recipe_id, []):
            gross_required = _quantize_units(projection_units * _to_decimal(bom_line.cantidad))
            if gross_required <= ZERO:
                continue
            unidad_codigo = getattr(getattr(bom_line.insumo, "unidad_base", None), "codigo", "") or ""
            product_items.append(
                {
                    "insumo_id": int(bom_line.insumo_id),
                    "insumo_nombre": bom_line.insumo.nombre,
                    "unidad_codigo": unidad_codigo,
                    "required_gross_qty": gross_required,
                    "formula_qty_per_unit": _quantize_units(_to_decimal(bom_line.cantidad)),
                }
            )
            bucket = insumo_rows.setdefault(
                int(bom_line.insumo_id),
                {
                    "insumo_id": int(bom_line.insumo_id),
                    "insumo_nombre": bom_line.insumo.nombre,
                    "unidad_codigo": unidad_codigo,
                    "required_gross_qty": ZERO,
                    "recipes": set(),
                    "branches": set(),
                },
            )
            bucket["required_gross_qty"] += gross_required
            bucket["recipes"].add(row.get("recipe_name") or "")
            bucket["branches"].add(row.get("branch_code") or "")

        product_rows.append(
            {
                "branch_id": int(row["branch_id"]),
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
    for insumo_id, row in insumo_rows.items():
        aggregate_rows.append(
            {
                "insumo_id": insumo_id,
                "insumo_nombre": row["insumo_nombre"],
                "unidad_codigo": row["unidad_codigo"],
                "required_gross_qty": _quantize_units(_to_decimal(row["required_gross_qty"])),
                "recipes_text": ", ".join(sorted(filter(None, row["recipes"]))),
                "branches_text": ", ".join(sorted(filter(None, row["branches"]))),
            }
        )
    aggregate_rows.sort(key=lambda item: (-item["required_gross_qty"], item["insumo_nombre"]))

    return {
        "target_date": target_date,
        "target_label": forecast_context.get("target_label") or target_date.isoformat(),
        "mode": "PROJECTION_EVENT",
        "uses_stock": False,
        "formula_note": "Se usa forecast_qty como base de producción proyectada y se explota receta a insumos sin descontar stock actual.",
        "summary": {
            "projected_products": len(product_rows),
            "projected_insumos": len(aggregate_rows),
            "forecast_units": forecast_context.get("summary", {}).get("forecast_units", ZERO),
        },
        "products": product_rows,
        "insumos": aggregate_rows,
        "generated_at": timezone.now(),
    }
