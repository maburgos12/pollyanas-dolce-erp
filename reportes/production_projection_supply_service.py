from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from django.utils import timezone

from maestros.models import Insumo, UnidadMedida
from recetas.models import LineaReceta, Receta, RecetaAgrupacionAddon, RecetaPresentacionDerivada
from recetas.utils.costeo_snapshot import (
    resolve_line_snapshot_cost,
    resolve_preparation_recipe_for_insumo,
)
from reportes.forecast_service import build_daily_forecast_context


ZERO = Decimal("0")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _article_class(insumo: Insumo) -> dict[str, str]:
    if insumo.tipo_item == Insumo.TIPO_EMPAQUE:
        return {"key": Insumo.TIPO_EMPAQUE, "label": "Empaque"}
    if insumo.tipo_item == Insumo.TIPO_INTERNO:
        return {"key": Insumo.TIPO_INTERNO, "label": "Insumo interno"}
    return {"key": Insumo.TIPO_MATERIA_PRIMA, "label": "Materia prima"}


def _unit_code(unit: UnidadMedida | None) -> str:
    return getattr(unit, "codigo", "") or ""


def _line_unit(line: LineaReceta) -> UnidadMedida | None:
    return line.unidad or getattr(line.insumo, "unidad_base", None)


def _compatible_units(source_unit: UnidadMedida | None, target_unit: UnidadMedida | None) -> bool:
    if source_unit is None or target_unit is None:
        return False
    return (source_unit.tipo or "").strip().upper() == (target_unit.tipo or "").strip().upper()


def _convert_quantity(
    quantity: Decimal,
    *,
    source_unit: UnidadMedida | None,
    target_unit: UnidadMedida | None,
) -> Decimal | None:
    if quantity <= ZERO:
        return ZERO
    if source_unit is None or target_unit is None:
        return None
    if source_unit.id == target_unit.id:
        return quantity
    if not _compatible_units(source_unit, target_unit):
        return None
    source_factor = _to_decimal(getattr(source_unit, "factor_to_base", None))
    target_factor = _to_decimal(getattr(target_unit, "factor_to_base", None))
    if source_factor <= ZERO or target_factor <= ZERO:
        return None
    return quantity * source_factor / target_factor


def _insumo_family(insumo: Insumo, article_class: dict[str, str]) -> str:
    category = (insumo.categoria or "").strip()
    if article_class["key"] == Insumo.TIPO_EMPAQUE:
        return "Empaques"
    if article_class["key"] == Insumo.TIPO_INTERNO:
        return "Preparados sin explosion"
    return category or "Materia prima"


def _insumo_category(insumo: Insumo, article_class: dict[str, str]) -> str:
    category = (insumo.categoria or "").strip()
    if article_class["key"] == Insumo.TIPO_EMPAQUE:
        return category or "Empaque"
    if article_class["key"] == Insumo.TIPO_INTERNO:
        return category or "Preparacion pendiente"
    return category or "Sin categoria"


def _prepared_family(insumo: Insumo, prep_recipe: Receta | None) -> str:
    return (
        (getattr(prep_recipe, "familia", "") or "").strip()
        or (insumo.categoria or "").strip()
        or "Preparados"
    )


def _prepared_category(insumo: Insumo, prep_recipe: Receta | None) -> str:
    return (
        (getattr(prep_recipe, "categoria", "") or "").strip()
        or (insumo.categoria or "").strip()
        or "Sin categoria"
    )


def _forecast_row_recipe_id(row: dict[str, object]) -> int | None:
    raw = row.get("recipe_id") or row.get("receta_id")
    try:
        return int(raw) if raw else None
    except (TypeError, ValueError):
        return None


def _forecast_row_recipe_name(row: dict[str, object]) -> str:
    return str(row.get("recipe_name") or row.get("receta") or "")


def _forecast_row_branch_id(row: dict[str, object]) -> int:
    raw = row.get("branch_id") or row.get("sucursal_id") or 0
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def _forecast_row_branch_code(row: dict[str, object]) -> str:
    return str(row.get("branch_code") or row.get("sucursal_codigo") or "")


def _forecast_row_branch_name(row: dict[str, object]) -> str:
    return str(row.get("branch_name") or row.get("sucursal_nombre") or "")


def _scenario_qty_key(escenario: str) -> str:
    scenario = (escenario or "base").strip().lower()
    if scenario == "bajo":
        return "forecast_low"
    if scenario == "alto":
        return "forecast_high"
    return "forecast_qty"


def build_projection_supply_context_from_forecast_preview(
    forecast_preview: dict[str, object] | None,
    *,
    escenario: str = "base",
) -> dict[str, object] | None:
    if not forecast_preview:
        return None
    qty_key = _scenario_qty_key(escenario)
    rows: list[dict[str, object]] = []
    for row in forecast_preview.get("rows") or []:
        if not isinstance(row, dict):
            continue
        recipe_id = _forecast_row_recipe_id(row)
        if not recipe_id:
            continue
        forecast_qty = _to_decimal(row.get(qty_key) or row.get("forecast_qty"))
        if forecast_qty <= ZERO:
            continue
        rows.append(
            {
                "branch_id": _forecast_row_branch_id(row),
                "branch_code": _forecast_row_branch_code(row),
                "branch_name": _forecast_row_branch_name(row),
                "recipe_id": recipe_id,
                "recipe_name": _forecast_row_recipe_name(row),
                "forecast_qty": forecast_qty,
                "buffer_units": ZERO,
            }
        )

    target_raw = str(forecast_preview.get("target_start") or forecast_preview.get("periodo") or "")
    target_date = timezone.localdate()
    if target_raw:
        try:
            target_date = date.fromisoformat(target_raw[:10])
        except ValueError:
            try:
                target_date = date.fromisoformat(f"{target_raw[:7]}-01")
            except ValueError:
                target_date = timezone.localdate()

    forecast_context = {
        "target_label": forecast_preview.get("target_label")
        or f"{forecast_preview.get('target_start', '')} a {forecast_preview.get('target_end', '')}".strip(),
        "summary": {"forecast_units": sum((_to_decimal(row.get("forecast_qty")) for row in rows), ZERO)},
        "rows": rows,
    }
    context = build_projection_supply_context(
        target_date=target_date,
        top_n=None,
        forecast_context=forecast_context,
    )
    context["mode"] = "FORECAST_PREVIEW"
    context["scenario"] = (escenario or "base").strip().lower() or "base"
    return context


def build_projection_supply_context(
    *,
    target_date: date,
    top_n: int | None = 24,
    forecast_context: dict[str, object] | None = None,
) -> dict[str, object]:
    forecast_context = forecast_context or build_daily_forecast_context(target_date=target_date, top_n=top_n)
    forecast_rows = [row for row in (forecast_context.get("rows") or []) if _to_decimal(row.get("forecast_qty")) > ZERO]
    recipe_ids = sorted({recipe_id for row in forecast_rows if (recipe_id := _forecast_row_recipe_id(row))})

    line_cache: dict[int, list[LineaReceta]] = {}

    def _load_recipe_lines(recipe_id: int) -> list[LineaReceta]:
        if recipe_id not in line_cache:
            line_cache[recipe_id] = list(
                LineaReceta.objects.filter(receta_id=recipe_id, insumo_id__isnull=False)
                .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
                .select_related("insumo", "insumo__unidad_base", "unidad", "receta")
                .order_by("receta_id", "posicion", "id")
            )
        return [line for line in line_cache[recipe_id] if line.insumo_id and _to_decimal(line.cantidad) > ZERO]

    for recipe_id in recipe_ids:
        _load_recipe_lines(recipe_id)

    # Precargar reglas de presentaciones derivadas (rebanadas) para los productos del forecast
    derivada_map: dict[int, tuple[int, Decimal]] = {}
    for row in RecetaPresentacionDerivada.objects.filter(
        receta_derivada_id__in=recipe_ids, activo=True
    ).values("receta_derivada_id", "receta_padre_id", "unidades_por_padre"):
        child_id = int(row["receta_derivada_id"])
        parent_id = int(row["receta_padre_id"])
        units = _to_decimal(row["unidades_por_padre"])
        if units > ZERO:
            derivada_map[child_id] = (parent_id, units)
    for parent_id in {pid for pid, _ in derivada_map.values()}:
        _load_recipe_lines(parent_id)

    # Precargar reglas de agrupación addon (Sabor Fresa → Pay de Queso base) para productos del forecast
    addon_base_map: dict[int, int] = {}
    for row in RecetaAgrupacionAddon.objects.filter(
        addon_receta_id__in=recipe_ids,
        activo=True,
        status=RecetaAgrupacionAddon.STATUS_APPROVED,
        addon_receta__isnull=False,
    ).values("addon_receta_id", "base_receta_id"):
        addon_id = int(row["addon_receta_id"])
        base_id = int(row["base_receta_id"])
        if addon_id not in addon_base_map:
            addon_base_map[addon_id] = base_id
    for base_id in set(addon_base_map.values()):
        _load_recipe_lines(base_id)

    product_rows: list[dict[str, object]] = []
    purchase_rows: dict[tuple[int, str], dict[str, object]] = {}
    prepared_rows: dict[tuple[int, str], dict[str, object]] = {}
    explosion_rows: list[dict[str, object]] = []
    issue_rows: list[dict[str, object]] = []

    def add_issue(
        *,
        code: str,
        severity: str,
        recipe_name: str,
        insumo: Insumo | None,
        detail: str,
        product_name: str,
    ) -> None:
        issue_rows.append(
            {
                "code": code,
                "severity": severity,
                "recipe_name": recipe_name,
                "insumo_id": int(insumo.id) if insumo and insumo.id else None,
                "insumo_nombre": insumo.nombre if insumo else "",
                "detail": detail,
                "product_name": product_name,
            }
        )

    def add_purchase_row(
        *,
        line: LineaReceta,
        quantity: Decimal,
        product_row: dict[str, object],
        path_text: str,
        level: int,
        forced_source: str | None = None,
    ) -> dict[str, object]:
        insumo = line.insumo
        unit = _line_unit(line)
        unidad_codigo = _unit_code(unit)
        article_class = _article_class(insumo)
        resolved_cost, cost_source = resolve_line_snapshot_cost(line)
        unit_cost = _to_decimal(resolved_cost)
        required_qty = _quantize_units(quantity)
        estimated_spend = _quantize_money(required_qty * unit_cost)
        family = _insumo_family(insumo, article_class)
        category = _insumo_category(insumo, article_class)
        if unit_cost <= ZERO:
            add_issue(
                code="SIN_COSTO",
                severity="warning",
                recipe_name=str(product_row["recipe_name"]),
                insumo=insumo,
                detail=f"El insumo requerido no tiene costo resoluble para {unidad_codigo or 'su unidad'}.",
                product_name=str(product_row["recipe_name"]),
            )
        key = (int(insumo.id), unidad_codigo)
        bucket = purchase_rows.setdefault(
            key,
            {
                "insumo_id": int(insumo.id),
                "insumo_nombre": insumo.nombre,
                "unidad_codigo": unidad_codigo,
                "article_class_key": article_class["key"],
                "article_class_label": article_class["label"],
                "family": family,
                "category": category,
                "required_gross_qty": ZERO,
                "unit_cost": unit_cost,
                "cost_sources": set(),
                "estimated_spend": ZERO,
                "recipes": set(),
                "branches": set(),
            },
        )
        bucket["required_gross_qty"] += required_qty
        bucket["estimated_spend"] += estimated_spend
        bucket["cost_sources"].add(cost_source)
        bucket["recipes"].add(str(product_row["recipe_name"]))
        bucket["branches"].add(str(product_row["branch_code"]))
        explosion_rows.append(
            {
                "level": level,
                "path": path_text,
                "product_name": product_row["recipe_name"],
                "recipe_name": getattr(line.receta, "nombre", ""),
                "insumo_id": int(insumo.id),
                "insumo_nombre": insumo.nombre,
                "article_class_label": article_class["label"],
                "required_gross_qty": required_qty,
                "unidad_codigo": unidad_codigo,
                "unit_cost": unit_cost,
                "estimated_spend": estimated_spend,
                "cost_source": cost_source,
                "fallback_reason": forced_source or "",
                "rollup_kind": "COMPRA",
            }
        )
        return {
            "insumo_id": int(insumo.id),
            "insumo_nombre": insumo.nombre,
            "unidad_codigo": unidad_codigo,
            "article_class_key": article_class["key"],
            "article_class_label": article_class["label"],
            "family": family,
            "category": category,
            "required_gross_qty": required_qty,
            "formula_qty_per_unit": _quantize_units(_to_decimal(line.cantidad)),
            "unit_cost": unit_cost,
            "cost_source": cost_source,
            "estimated_spend": estimated_spend,
            "rollup_kind": "COMPRA",
        }

    def add_prepared_row(
        *,
        line: LineaReceta,
        prep_recipe: Receta | None,
        quantity: Decimal,
        product_row: dict[str, object],
        path_text: str,
        level: int,
    ) -> dict[str, object]:
        insumo = line.insumo
        unit = _line_unit(line)
        unidad_codigo = _unit_code(unit)
        required_qty = _quantize_units(quantity)
        family = _prepared_family(insumo, prep_recipe)
        category = _prepared_category(insumo, prep_recipe)
        key = (int(insumo.id), unidad_codigo)
        bucket = prepared_rows.setdefault(
            key,
            {
                "insumo_id": int(insumo.id),
                "insumo_nombre": insumo.nombre,
                "prep_recipe_id": int(prep_recipe.id) if prep_recipe else None,
                "prep_recipe_name": prep_recipe.nombre if prep_recipe else "",
                "unidad_codigo": unidad_codigo,
                "family": family,
                "category": category,
                "required_gross_qty": ZERO,
                "recipes": set(),
                "branches": set(),
            },
        )
        bucket["required_gross_qty"] += required_qty
        bucket["recipes"].add(str(product_row["recipe_name"]))
        bucket["branches"].add(str(product_row["branch_code"]))
        explosion_rows.append(
            {
                "level": level,
                "path": path_text,
                "product_name": product_row["recipe_name"],
                "recipe_name": prep_recipe.nombre if prep_recipe else getattr(line.receta, "nombre", ""),
                "insumo_id": int(insumo.id),
                "insumo_nombre": insumo.nombre,
                "article_class_label": "Insumo interno",
                "required_gross_qty": required_qty,
                "unidad_codigo": unidad_codigo,
                "unit_cost": ZERO,
                "estimated_spend": ZERO,
                "cost_source": "PRODUCCION_INTERNA_EXPLOTADA" if prep_recipe else "INSUMO_INTERNO_SIN_PREPARACION",
                "rollup_kind": "PREPARADO",
            }
        )
        return {
            "insumo_id": int(insumo.id),
            "insumo_nombre": insumo.nombre,
            "unidad_codigo": unidad_codigo,
            "article_class_key": Insumo.TIPO_INTERNO,
            "article_class_label": "Insumo interno",
            "family": family,
            "category": category,
            "required_gross_qty": required_qty,
            "formula_qty_per_unit": _quantize_units(_to_decimal(line.cantidad)),
            "unit_cost": ZERO,
            "cost_source": "PRODUCCION_INTERNA_EXPLOTADA",
            "estimated_spend": ZERO,
            "rollup_kind": "PREPARADO",
        }

    def explode_line(
        *,
        line: LineaReceta,
        quantity: Decimal,
        product_row: dict[str, object],
        path: list[str],
        active_recipe_ids: tuple[int, ...],
        level: int,
    ) -> dict[str, object] | None:
        if quantity <= ZERO or line.insumo is None:
            return None
        insumo = line.insumo
        line_path = [*path, insumo.nombre]
        path_text = " > ".join(filter(None, line_path))
        if insumo.tipo_item != Insumo.TIPO_INTERNO:
            return add_purchase_row(
                line=line,
                quantity=quantity,
                product_row=product_row,
                path_text=path_text,
                level=level,
            )

        prep_recipe = resolve_preparation_recipe_for_insumo(insumo)
        if prep_recipe is None:
            add_issue(
                code="PREPARACION_NO_ENCONTRADA",
                severity="warning",
                recipe_name=getattr(line.receta, "nombre", ""),
                insumo=insumo,
                detail="El insumo interno no está ligado a una receta de preparación; se costea como fallback para no perder gasto.",
                product_name=str(product_row["recipe_name"]),
            )
            return add_purchase_row(
                line=line,
                quantity=quantity,
                product_row=product_row,
                path_text=path_text,
                level=level,
                forced_source="INSUMO_INTERNO_NO_EXPLOTADO",
            )

        prepared_item = add_prepared_row(
            line=line,
            prep_recipe=prep_recipe,
            quantity=quantity,
            product_row=product_row,
            path_text=path_text,
            level=level,
        )
        prep_id = int(prep_recipe.id)
        if prep_id in active_recipe_ids:
            add_issue(
                code="CICLO_PREPARACION",
                severity="error",
                recipe_name=prep_recipe.nombre,
                insumo=insumo,
                detail="Se detectó ciclo en receta de preparación; se detuvo la explosión para evitar inflar cantidades.",
                product_name=str(product_row["recipe_name"]),
            )
            return prepared_item

        child_lines = _load_recipe_lines(prep_id)
        if not child_lines:
            add_issue(
                code="PREPARACION_SIN_BOM",
                severity="warning",
                recipe_name=prep_recipe.nombre,
                insumo=insumo,
                detail="La preparación existe, pero no tiene líneas de receta; se costea como fallback cerrado.",
                product_name=str(product_row["recipe_name"]),
            )
            return add_purchase_row(
                line=line,
                quantity=quantity,
                product_row=product_row,
                path_text=path_text,
                level=level,
                forced_source="PREPARADO_SIN_BOM",
            )

        line_unit = _line_unit(line)
        yield_unit = prep_recipe.rendimiento_unidad or line_unit
        required_in_yield_unit = _convert_quantity(quantity, source_unit=line_unit, target_unit=yield_unit)
        yield_qty = _to_decimal(prep_recipe.rendimiento_cantidad)
        if required_in_yield_unit is None or yield_qty <= ZERO:
            add_issue(
                code="PREPARACION_UNIDAD_INCOMPATIBLE",
                severity="warning",
                recipe_name=prep_recipe.nombre,
                insumo=insumo,
                detail="La unidad requerida no se puede convertir contra el rendimiento de la preparación; se costea como fallback.",
                product_name=str(product_row["recipe_name"]),
            )
            return add_purchase_row(
                line=line,
                quantity=quantity,
                product_row=product_row,
                path_text=path_text,
                level=level,
                forced_source="PREPARADO_UNIDAD_INCOMPATIBLE",
            )

        multiplier = required_in_yield_unit / yield_qty
        for child_line in child_lines:
            child_qty = _to_decimal(child_line.cantidad) * multiplier
            explode_line(
                line=child_line,
                quantity=child_qty,
                product_row=product_row,
                path=line_path,
                active_recipe_ids=(*active_recipe_ids, prep_id),
                level=level + 1,
            )
        return prepared_item

    for row in forecast_rows:
        recipe_id = _forecast_row_recipe_id(row)
        if not recipe_id:
            continue
        projection_units = _quantize_units(_to_decimal(row.get("forecast_qty")))
        buffer_units = _quantize_units(_to_decimal(row.get("buffer_units")))
        product_row = {
            "branch_id": _forecast_row_branch_id(row),
            "branch_code": _forecast_row_branch_code(row),
            "branch_name": _forecast_row_branch_name(row),
            "recipe_id": recipe_id,
            "recipe_name": _forecast_row_recipe_name(row),
            "forecast_qty": projection_units,
            "buffer_units": buffer_units,
            "gross_basis_qty": projection_units,
            "notes": "Requerimiento bruto por proyección. No descuenta stock actual.",
            "items": [],
        }
        bom_lines_to_explode: list[tuple[LineaReceta, Decimal]] = []

        # Líneas directas del producto (empaque, componentes propios)
        for bom_line in _load_recipe_lines(recipe_id):
            gross_required = projection_units * _to_decimal(bom_line.cantidad)
            if gross_required > ZERO:
                bom_lines_to_explode.append((bom_line, gross_required))

        # Si es presentación derivada (rebanada), agregar líneas del padre escaladas
        if recipe_id in derivada_map:
            parent_id, units_per_parent = derivada_map[recipe_id]
            for parent_line in _load_recipe_lines(parent_id):
                gross_required = projection_units * _to_decimal(parent_line.cantidad) / units_per_parent
                if gross_required > ZERO:
                    bom_lines_to_explode.append((parent_line, gross_required))

        # Si es un addon (Sabor Fresa, Guayaba, etc.), agregar líneas del pay base (1:1)
        if recipe_id in addon_base_map:
            base_id = addon_base_map[recipe_id]
            for base_line in _load_recipe_lines(base_id):
                gross_required = projection_units * _to_decimal(base_line.cantidad)
                if gross_required > ZERO:
                    bom_lines_to_explode.append((base_line, gross_required))

        for bom_line, gross_required in bom_lines_to_explode:
            item = explode_line(
                line=bom_line,
                quantity=gross_required,
                product_row=product_row,
                path=[str(product_row["recipe_name"])],
                active_recipe_ids=(recipe_id,),
                level=1,
            )
            if item:
                product_row["items"].append(item)
        product_rows.append(product_row)

    product_rows.sort(key=lambda item: (-item["forecast_qty"], item["branch_code"], item["recipe_name"]))

    aggregate_rows = []
    for (insumo_id, _unidad_codigo), row in purchase_rows.items():
        aggregate_rows.append(
            {
                "insumo_id": insumo_id,
                "insumo_nombre": row["insumo_nombre"],
                "unidad_codigo": row["unidad_codigo"],
                "article_class_key": row["article_class_key"],
                "article_class_label": row["article_class_label"],
                "family": row["family"],
                "category": row["category"],
                "required_gross_qty": _quantize_units(_to_decimal(row["required_gross_qty"])),
                "unit_cost": _to_decimal(row["unit_cost"]),
                "estimated_spend": _quantize_money(_to_decimal(row["estimated_spend"])),
                "missing_cost": _to_decimal(row["unit_cost"]) <= ZERO,
                "cost_sources_text": ", ".join(sorted(filter(None, row["cost_sources"]))),
                "recipes_text": ", ".join(sorted(filter(None, row["recipes"]))),
                "branches_text": ", ".join(sorted(filter(None, row["branches"]))),
            }
        )
    aggregate_rows.sort(
        key=lambda item: (
            str(item["family"]).lower(),
            str(item["category"]).lower(),
            str(item["article_class_label"]).lower(),
            str(item["insumo_nombre"]).lower(),
        )
    )

    prepared_aggregate_rows = []
    for (insumo_id, _unidad_codigo), row in prepared_rows.items():
        prepared_aggregate_rows.append(
            {
                "insumo_id": insumo_id,
                "insumo_nombre": row["insumo_nombre"],
                "prep_recipe_id": row["prep_recipe_id"],
                "prep_recipe_name": row["prep_recipe_name"],
                "unidad_codigo": row["unidad_codigo"],
                "family": row["family"],
                "category": row["category"],
                "required_gross_qty": _quantize_units(_to_decimal(row["required_gross_qty"])),
                "recipes_text": ", ".join(sorted(filter(None, row["recipes"]))),
                "branches_text": ", ".join(sorted(filter(None, row["branches"]))),
            }
        )
    prepared_aggregate_rows.sort(
        key=lambda item: (
            str(item["family"]).lower(),
            str(item["category"]).lower(),
            str(item["insumo_nombre"]).lower(),
        )
    )

    estimated_spend_total = _quantize_money(
        sum((_to_decimal(row["estimated_spend"]) for row in aggregate_rows), ZERO)
    )
    missing_cost_insumos = sum(1 for row in aggregate_rows if row["missing_cost"])

    return {
        "target_date": target_date,
        "target_label": forecast_context.get("target_label") or target_date.isoformat(),
        "mode": "PROJECTION_EVENT",
        "uses_stock": False,
        "formula_note": (
            "Se usa forecast_qty como base de producción proyectada; las preparaciones internas se listan como producción "
            "requerida y solo se suma gasto en materias primas/empaques hoja para evitar doble costeo."
        ),
        "summary": {
            "projected_products": len(product_rows),
            "projected_insumos": len(aggregate_rows),
            "prepared_insumos": len(prepared_aggregate_rows),
            "forecast_units": forecast_context.get("summary", {}).get("forecast_units", ZERO),
            "estimated_spend": estimated_spend_total,
            "missing_cost_insumos": missing_cost_insumos,
            "issues": len(issue_rows),
        },
        "products": product_rows,
        "insumos": aggregate_rows,
        "purchase_insumos": aggregate_rows,
        "prepared_insumos": prepared_aggregate_rows,
        "explosion_rows": explosion_rows,
        "issues": issue_rows,
        "generated_at": timezone.now(),
    }
