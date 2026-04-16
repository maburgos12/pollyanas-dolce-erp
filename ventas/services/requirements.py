from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Sum

from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from recetas.models import LineaReceta
from recetas.utils.commercial_composition import ensure_curated_commercial_mappings, resolve_commercial_sku_interpretation
from recetas.utils.costeo_snapshot import resolve_preparation_recipe_for_insumo
from ventas.models import (
    EventoVenta,
    EventoVentaInputRequirement,
    EventoVentaProductionLine,
    EventoVentaPurchaseRequirement,
    EventoVentaNotification,
)
from ventas.services.notifications import create_unique_notification


def _latest_cost(insumo_id: int) -> Decimal:
    latest = CostoInsumo.objects.filter(insumo_id=insumo_id).order_by("-fecha", "-id").first()
    if not latest:
        return Decimal("0")
    return Decimal(str(latest.costo_unitario or 0))


def _to_decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def _convert_quantity(
    qty: Decimal,
    *,
    source_unit: UnidadMedida | None,
    target_unit: UnidadMedida | None,
) -> Decimal | None:
    if qty <= 0:
        return Decimal("0")
    if source_unit is None or target_unit is None:
        return None
    if source_unit.id == target_unit.id:
        return qty
    if (source_unit.tipo or "").strip().upper() != (target_unit.tipo or "").strip().upper():
        return None
    source_factor = _to_decimal(source_unit.factor_to_base)
    target_factor = _to_decimal(target_unit.factor_to_base)
    if source_factor <= 0 or target_factor <= 0:
        return None
    return qty * (source_factor / target_factor)


def _line_quantity_in_input_base(linea: LineaReceta) -> Decimal | None:
    if not linea.insumo_id or linea.cantidad is None:
        return None
    qty = _to_decimal(linea.cantidad)
    if qty <= 0:
        return Decimal("0")
    source_unit = linea.unidad or linea.insumo.unidad_base
    target_unit = linea.insumo.unidad_base or source_unit
    return _convert_quantity(qty, source_unit=source_unit, target_unit=target_unit)


def _preparation_yield_in_input_base(prep_recipe, insumo: Insumo) -> Decimal | None:
    yield_qty = _to_decimal(prep_recipe.rendimiento_cantidad)
    if yield_qty <= 0:
        return None
    source_unit = prep_recipe.rendimiento_unidad or insumo.unidad_base
    target_unit = insumo.unidad_base or source_unit
    return _convert_quantity(yield_qty, source_unit=source_unit, target_unit=target_unit)


def _explode_recipe_requirements(
    *,
    recipe,
    multiplier: Decimal,
    production_day: date | None,
    required_map: dict[int, Decimal],
    required_by_date: dict[int, date],
    warnings: list[str],
    stack: tuple[int, ...] = (),
) -> None:
    if multiplier <= 0:
        return
    if recipe.id in stack:
        warnings.append(f"Se detectó un ciclo de receta en {recipe.nombre}; la explosión se truncó para evitar duplicados.")
        return

    for receta_linea in (
        LineaReceta.objects.filter(receta=recipe)
        .exclude(match_status=LineaReceta.STATUS_REJECTED)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .select_related("insumo", "unidad", "insumo__unidad_base")
    ):
        insumo = receta_linea.insumo
        if not insumo:
            continue
        line_qty = _line_quantity_in_input_base(receta_linea)
        if line_qty is None:
            warnings.append(
                f"No se pudo convertir la línea '{receta_linea.insumo_texto}' de {recipe.nombre}; se omitió del cálculo de insumos."
            )
            continue
        required_qty = line_qty * multiplier
        if required_qty <= 0:
            continue

        if insumo.tipo_item == Insumo.TIPO_INTERNO:
            prep_recipe = resolve_preparation_recipe_for_insumo(insumo)
            prep_yield = _preparation_yield_in_input_base(prep_recipe, insumo) if prep_recipe else None
            if prep_recipe and prep_yield and prep_yield > 0:
                prep_batches = required_qty / prep_yield
                _explode_recipe_requirements(
                    recipe=prep_recipe,
                    multiplier=prep_batches,
                    production_day=production_day,
                    required_map=required_map,
                    required_by_date=required_by_date,
                    warnings=warnings,
                    stack=(*stack, recipe.id),
                )
                continue
            warnings.append(
                f"Insumo interno '{insumo.nombre}' sin receta/rendimiento confiable; se conservará como requerimiento interno."
            )

        required_map[insumo.id] += required_qty
        if production_day:
            current = required_by_date.get(insumo.id)
            if not current or production_day < current:
                required_by_date[insumo.id] = production_day


def build_input_requirements(event: EventoVenta) -> dict:
    lines = EventoVentaProductionLine.objects.filter(production_plan__sales_event=event).select_related("product")
    if not lines.exists():
        return {"created": 0, "warnings": ["No hay plan de producción."]}

    ensure_curated_commercial_mappings()
    EventoVentaInputRequirement.objects.filter(sales_event=event).delete()

    required_map: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
    required_by_date: dict[int, date] = {}
    warnings: list[str] = []

    for line in lines:
        qty = _to_decimal(line.net_qty_to_produce)
        interpretation = resolve_commercial_sku_interpretation(line.product)
        if interpretation.is_blocked:
            warnings.append(
                interpretation.blocked_reason
                or f"{line.product.nombre} quedó bloqueado por ambigüedad y no se explotó a insumos."
            )
            continue
        for note in interpretation.notes:
            warnings.append(note)
        for recipe_component in interpretation.component_recetas:
            _explode_recipe_requirements(
                recipe=recipe_component,
                multiplier=qty,
                production_day=line.production_day,
                required_map=required_map,
                required_by_date=required_by_date,
                warnings=warnings,
            )

    created = 0
    shortage_count = 0
    for insumo_id, required_qty in required_map.items():
        existencia = ExistenciaInsumo.objects.filter(insumo_id=insumo_id).first()
        on_hand = _to_decimal(existencia.stock_actual) if existencia else Decimal("0")
        net_shortage = max(Decimal("0"), required_qty - on_hand)
        unit_cost = _latest_cost(insumo_id)
        if net_shortage > 0:
            shortage_count += 1
        coverage_ratio = (on_hand / required_qty) if required_qty > 0 else Decimal("1")
        if net_shortage <= 0:
            risk = "BAJO"
        elif coverage_ratio >= Decimal("0.7"):
            risk = "MEDIO"
        else:
            risk = "ALTO"
        req_date = required_by_date.get(insumo_id)

        EventoVentaInputRequirement.objects.create(
            sales_event=event,
            input_item_id=insumo_id,
            required_qty=required_qty,
            on_hand_qty=on_hand,
            net_shortage_qty=net_shortage,
            unit_cost_estimate=unit_cost,
            risk_level=risk,
            required_by_date=req_date,
        )
        created += 1

    if shortage_count:
        create_unique_notification(
            event,
            f"Se detectaron {shortage_count} insumos con faltante para el evento.",
            EventoVentaNotification.SEVERITY_WARN,
        )
    else:
        create_unique_notification(event, "Insumos calculados sin faltantes netos.")

    for warning in warnings[:3]:
        create_unique_notification(event, warning, EventoVentaNotification.SEVERITY_WARN)

    return {"created": created, "shortages": shortage_count, "warnings": warnings[:20]}


def build_purchase_requirements(event: EventoVenta) -> dict:
    reqs = EventoVentaInputRequirement.objects.filter(
        sales_event=event,
        net_shortage_qty__gt=0,
        input_item__tipo_item__in=[Insumo.TIPO_MATERIA_PRIMA, Insumo.TIPO_EMPAQUE],
    ).select_related("input_item")
    if not reqs.exists():
        return {"created": 0, "warnings": ["No hay faltantes comprables de insumos."]}

    EventoVentaPurchaseRequirement.objects.filter(sales_event=event).delete()
    internal_shortages = EventoVentaInputRequirement.objects.filter(
        sales_event=event,
        net_shortage_qty__gt=0,
        input_item__tipo_item=Insumo.TIPO_INTERNO,
    ).count()

    created = 0
    for req in reqs:
        lead_days = 0
        if req.input_item_id:
            existencia = ExistenciaInsumo.objects.filter(insumo_id=req.input_item_id).first()
            lead_days = int(existencia.dias_llegada_pedido or 0) if existencia else 0
        deadline = req.required_by_date - timedelta(days=lead_days) if req.required_by_date else None

        EventoVentaPurchaseRequirement.objects.create(
            sales_event=event,
            input_requirement=req,
            suggested_purchase_qty=req.net_shortage_qty,
            purchase_deadline=deadline,
            estimated_cost=req.net_shortage_qty * req.unit_cost_estimate,
        )
        created += 1

    create_unique_notification(
        event,
        f"Se prepararon {created} requerimientos de compra para revisión de producción/compras.",
        EventoVentaNotification.SEVERITY_WARN if created else EventoVentaNotification.SEVERITY_INFO,
    )
    if internal_shortages:
        create_unique_notification(
            event,
            f"Se bloquearon {internal_shortages} faltantes de insumo interno para compras; requieren preparación interna.",
            EventoVentaNotification.SEVERITY_WARN,
        )

    return {"created": created, "blocked_internal": internal_shortages}
