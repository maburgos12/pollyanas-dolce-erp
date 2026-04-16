from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from django.utils import timezone

from inventario.models import AlmacenSyncRun, ExistenciaInsumo
from inventario.stock_trace import TRACE_LABELS, TRACE_UNTRACED
from recetas.models import LineaReceta
from reportes.models import AutoPurchaseRequestSnapshot, ProductionOrder


ZERO = Decimal("0")
ACTIVE_ORDER_STATUSES = (
    ProductionOrder.STATUS_PROPOSED,
    ProductionOrder.STATUS_APPROVED,
    ProductionOrder.STATUS_RELEASED,
)
ORDER_PRIORITY = {
    ProductionOrder.STATUS_RELEASED: 0,
    ProductionOrder.STATUS_APPROVED: 1,
    ProductionOrder.STATUS_PROPOSED: 2,
}


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _selected_units(line) -> Decimal:
    approved = _to_decimal(getattr(line, "cantidad_aprobada", ZERO))
    if approved > ZERO:
        return approved
    return max(_to_decimal(getattr(line, "cantidad_recomendada", ZERO)), ZERO)


def _pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= ZERO:
        return ZERO
    return (numerator / denominator) * Decimal("100")


def _stock_status(*, available_before: Decimal, shortage_qty: Decimal) -> tuple[str, str]:
    if available_before <= ZERO:
        return "SIN_STOCK", "Sin stock"
    if shortage_qty > ZERO:
        return "STOCK_PARCIAL", "Stock parcial"
    return "DISPONIBLE", "Disponible"


def _serialize_sync_run(run: AlmacenSyncRun | None) -> dict[str, object] | None:
    if run is None:
        return None
    return {
        "id": run.id,
        "source": run.source,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "folder_name": run.folder_name,
        "target_month": run.target_month,
        "message": run.message,
    }


@dataclass
class _OrderRequirement:
    insumo_id: int
    insumo_nombre: str
    unidad_codigo: str
    recipes: set[str]
    required_qty: Decimal = ZERO


def build_production_supply_context(
    *,
    target_date: date,
    orders: list[ProductionOrder] | None = None,
    sucursal_id: int | None = None,
) -> dict[str, object]:
    if orders is None:
        orders = list(
            ProductionOrder.objects.filter(fecha=target_date, status__in=ACTIVE_ORDER_STATUSES)
            .select_related("sucursal")
            .prefetch_related("lines__receta")
            .order_by("sucursal__codigo", "id")
        )

    selected_orders = [
        order
        for order in orders
        if order.status in ACTIVE_ORDER_STATUSES and (sucursal_id is None or order.sucursal_id == sucursal_id)
    ]
    selected_orders.sort(key=lambda order: (ORDER_PRIORITY.get(order.status, 99), order.sucursal.codigo, order.id))
    if not selected_orders:
        return {
            "target_date": target_date,
            "source_of_truth": "ExistenciaInsumo + MovimientoInventario + importaciones/ajustes del ERP",
            "source_is_point": False,
            "inventory_scope": "GLOBAL_INSUMO",
            "summary": {
                "active_orders": 0,
                "unique_insumos": 0,
                "shortage_insumos": 0,
                "covered_insumos": 0,
                "coverage_rows_pct": ZERO,
                "purchase_generated_rows": 0,
                "traceable_rows": 0,
            },
            "rows": [],
            "notes": [
                "No hay órdenes activas del día para explotar insumos.",
                "La fuente de verdad del inventario de insumos es el ledger del ERP, no Point.",
            ],
            "latest_visible_update": None,
            "latest_sync_ok": _serialize_sync_run(
                AlmacenSyncRun.objects.filter(status=AlmacenSyncRun.STATUS_OK).order_by("-started_at", "-id").first()
            ),
            "latest_sync_any": _serialize_sync_run(AlmacenSyncRun.objects.order_by("-started_at", "-id").first()),
            "orders": {},
        }

    recipe_ids = sorted(
        {
            int(line.receta_id)
            for order in selected_orders
            for line in order.lines.all()
            if line.receta_id and _selected_units(line) > ZERO
        }
    )
    bom_lines = list(
        LineaReceta.objects.filter(receta_id__in=recipe_ids, insumo_id__isnull=False)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .exclude(insumo__tipo_item="INSUMO_INTERNO")
        .select_related("insumo", "insumo__unidad_base", "receta")
        .order_by("receta_id", "posicion", "id")
    )
    bom_by_recipe: dict[int, list[LineaReceta]] = defaultdict(list)
    insumo_ids: set[int] = set()
    for bom_line in bom_lines:
        if bom_line.insumo_id and _to_decimal(bom_line.cantidad) > ZERO:
            bom_by_recipe[int(bom_line.receta_id)].append(bom_line)
            insumo_ids.add(int(bom_line.insumo_id))

    existing_purchase_snapshots = list(
        AutoPurchaseRequestSnapshot.objects.select_related("solicitud", "proveedor", "insumo", "sucursal")
        .filter(fecha=target_date, insumo_id__in=sorted(insumo_ids))
        .order_by("sucursal__codigo", "insumo__nombre", "id")
    )
    purchase_by_branch_insumo: dict[tuple[int, int], list[AutoPurchaseRequestSnapshot]] = defaultdict(list)
    purchase_by_insumo: dict[int, list[AutoPurchaseRequestSnapshot]] = defaultdict(list)
    for snapshot in existing_purchase_snapshots:
        if snapshot.sucursal_id:
            purchase_by_branch_insumo[(int(snapshot.sucursal_id), int(snapshot.insumo_id))].append(snapshot)
        purchase_by_insumo[int(snapshot.insumo_id)].append(snapshot)

    existencias = list(
        ExistenciaInsumo.objects.filter(insumo_id__in=sorted(insumo_ids))
        .select_related("insumo", "insumo__unidad_base")
        .order_by("insumo__nombre")
    )
    inventory_by_insumo = {int(existencia.insumo_id): existencia for existencia in existencias}
    inventory_pool = {insumo_id: _to_decimal(existencia.stock_actual) for insumo_id, existencia in inventory_by_insumo.items()}
    initial_inventory = dict(inventory_pool)

    aggregate_rows: dict[int, dict[str, object]] = {}
    order_context: dict[int, dict[str, object]] = {}

    for order in selected_orders:
        order_requirements: dict[int, _OrderRequirement] = {}
        order_lines = list(order.lines.all())
        recipe_inventory_pool = dict(inventory_pool)
        order_recipe_rows: list[dict[str, object]] = []
        for line in order_lines:
            units = _selected_units(line)
            if units <= ZERO:
                continue
            recipe_items: list[dict[str, object]] = []
            for bom_line in bom_by_recipe.get(int(line.receta_id), []):
                required = _quantize_units(units * _to_decimal(bom_line.cantidad))
                if required <= ZERO:
                    continue
                unidad_codigo = getattr(getattr(bom_line.insumo, "unidad_base", None), "codigo", "") or ""
                bucket = order_requirements.setdefault(
                    int(bom_line.insumo_id),
                    _OrderRequirement(
                        insumo_id=int(bom_line.insumo_id),
                        insumo_nombre=bom_line.insumo.nombre,
                        unidad_codigo=unidad_codigo,
                        recipes=set(),
                    ),
                )
                bucket.required_qty += required
                bucket.recipes.add(line.receta.nombre)
                available_before = recipe_inventory_pool.get(int(bom_line.insumo_id), ZERO)
                committed_qty = _quantize_units(min(required, available_before))
                shortage_qty = _quantize_units(max(required - available_before, ZERO))
                recipe_inventory_pool[int(bom_line.insumo_id)] = _quantize_units(max(available_before - committed_qty, ZERO))
                status_code, status_label = _stock_status(
                    available_before=_quantize_units(available_before),
                    shortage_qty=shortage_qty,
                )
                recipe_items.append(
                    {
                        "insumo_id": int(bom_line.insumo_id),
                        "insumo_nombre": bom_line.insumo.nombre,
                        "unidad_codigo": unidad_codigo,
                        "required_qty": required,
                        "available_before_qty": _quantize_units(available_before),
                        "shortage_qty": shortage_qty,
                        "status_code": status_code,
                        "status_label": status_label,
                    }
                )
            shortage_items = sum(1 for item in recipe_items if item["shortage_qty"] > ZERO)
            if recipe_items:
                if shortage_items == 0:
                    recipe_status_code, recipe_status_label = "DISPONIBLE", "Stock disponible"
                elif shortage_items == len(recipe_items):
                    recipe_status_code, recipe_status_label = "SIN_STOCK", "Sin stock suficiente"
                else:
                    recipe_status_code, recipe_status_label = "STOCK_PARCIAL", "Stock parcial"
                order_recipe_rows.append(
                    {
                        "recipe_id": int(line.receta_id),
                        "recipe_name": line.receta.nombre,
                        "units": _quantize_units(units),
                        "status_code": recipe_status_code,
                        "status_label": recipe_status_label,
                        "item_count": len(recipe_items),
                        "shortage_items": shortage_items,
                        "items": sorted(recipe_items, key=lambda item: (-item["shortage_qty"], item["insumo_nombre"])),
                    }
                )

        order_items: list[dict[str, object]] = []
        order_shortage_items = 0
        order_generated_rows = 0
        for insumo_id, requirement in sorted(order_requirements.items(), key=lambda item: item[1].insumo_nombre):
            available_before = inventory_pool.get(insumo_id, ZERO)
            required_qty = _quantize_units(requirement.required_qty)
            committed_qty = _quantize_units(min(required_qty, available_before))
            shortage_qty = _quantize_units(max(required_qty - available_before, ZERO))
            available_after = _quantize_units(max(available_before - committed_qty, ZERO))
            inventory_pool[insumo_id] = available_after

            purchase_snapshots = purchase_by_branch_insumo.get((int(order.sucursal_id), int(insumo_id)), [])
            purchase_generated_qty = _quantize_units(
                sum((_to_decimal(snapshot.cantidad_sugerida) for snapshot in purchase_snapshots), ZERO)
            )
            purchase_gap_qty = _quantize_units(max(shortage_qty - purchase_generated_qty, ZERO))

            existencia = inventory_by_insumo.get(insumo_id)
            trace = dict(getattr(existencia, "trazabilidad_stock", {}) or {})
            trace_source = trace.get("source") or TRACE_UNTRACED
            trace_label = trace.get("label") or TRACE_LABELS.get(trace_source, trace_source)
            trace_effective_at = trace.get("effective_at") or ""
            trace_quality = trace.get("quality") or ""
            status_code, status_label = _stock_status(
                available_before=_quantize_units(available_before),
                shortage_qty=shortage_qty,
            )

            if shortage_qty > ZERO:
                order_shortage_items += 1
            if purchase_generated_qty > ZERO:
                order_generated_rows += 1

            order_items.append(
                {
                    "insumo_id": insumo_id,
                    "insumo_nombre": requirement.insumo_nombre,
                    "unidad_codigo": requirement.unidad_codigo,
                    "required_qty": required_qty,
                    "available_before_qty": _quantize_units(available_before),
                    "committed_qty": committed_qty,
                    "shortage_qty": shortage_qty,
                    "available_after_qty": available_after,
                    "coverage_pct": _pct(committed_qty, required_qty).quantize(Decimal("0.01")) if required_qty > ZERO else ZERO,
                    "recipes_text": ", ".join(sorted(requirement.recipes)),
                    "trace_label": trace_label,
                    "trace_effective_at": trace_effective_at,
                    "trace_quality": trace_quality,
                    "visible_updated_at": getattr(existencia, "actualizado_en", None),
                    "purchase_generated_qty": purchase_generated_qty,
                    "purchase_gap_qty": purchase_gap_qty,
                    "purchase_folios_text": ", ".join(snapshot.solicitud.folio for snapshot in purchase_snapshots),
                    "purchase_status_text": ", ".join(snapshot.solicitud.get_estatus_display() for snapshot in purchase_snapshots),
                    "status_code": status_code,
                    "status_label": status_label,
                }
            )

            aggregate = aggregate_rows.setdefault(
                insumo_id,
                {
                    "insumo_id": insumo_id,
                    "insumo_nombre": requirement.insumo_nombre,
                    "unidad_codigo": requirement.unidad_codigo,
                    "available_qty": _quantize_units(initial_inventory.get(insumo_id, ZERO)),
                    "committed_qty": ZERO,
                    "required_qty": ZERO,
                    "shortage_qty": ZERO,
                    "recipes": set(),
                    "branches": set(),
                    "trace_label": trace_label,
                    "trace_effective_at": trace_effective_at,
                    "trace_quality": trace_quality,
                    "visible_updated_at": getattr(existencia, "actualizado_en", None),
                    "purchase_generated_qty": ZERO,
                    "purchase_folios": set(),
                    "purchase_statuses": set(),
                },
            )
            aggregate["committed_qty"] += committed_qty
            aggregate["required_qty"] += required_qty
            aggregate["shortage_qty"] += shortage_qty
            aggregate["recipes"].update(requirement.recipes)
            aggregate["branches"].add(order.sucursal.codigo)
            aggregate["purchase_generated_qty"] += purchase_generated_qty
            aggregate["purchase_folios"].update(snapshot.solicitud.folio for snapshot in purchase_snapshots)
            aggregate["purchase_statuses"].update(snapshot.solicitud.get_estatus_display() for snapshot in purchase_snapshots)

        order_context[int(order.id)] = {
            "item_count": len(order_items),
            "shortage_items": order_shortage_items,
            "covered_items": max(len(order_items) - order_shortage_items, 0),
            "generated_purchase_rows": order_generated_rows,
            "coverage_rows_pct": _pct(
                Decimal(max(len(order_items) - order_shortage_items, 0)),
                Decimal(len(order_items)),
            ).quantize(Decimal("0.01"))
            if order_items
            else ZERO,
            "items": sorted(order_items, key=lambda item: (-item["shortage_qty"], item["insumo_nombre"])),
            "recipe_rows": order_recipe_rows,
            "has_shortage": order_shortage_items > 0,
            "inventory_scope": "GLOBAL_INSUMO",
        }

    rows: list[dict[str, object]] = []
    latest_visible_update = None
    for insumo_id, row in aggregate_rows.items():
        purchase_snapshots = purchase_by_insumo.get(insumo_id, [])
        purchase_generated_qty = _quantize_units(_to_decimal(row["purchase_generated_qty"]))
        shortage_qty = _quantize_units(_to_decimal(row["shortage_qty"]))
        purchase_gap_qty = _quantize_units(max(shortage_qty - purchase_generated_qty, ZERO))
        remaining_after_commit = _quantize_units(max(_to_decimal(row["available_qty"]) - _to_decimal(row["committed_qty"]), ZERO))
        visible_updated_at = row.get("visible_updated_at")
        if visible_updated_at and (latest_visible_update is None or visible_updated_at > latest_visible_update):
            latest_visible_update = visible_updated_at
        rows.append(
            {
                "insumo_id": insumo_id,
                "insumo_nombre": row["insumo_nombre"],
                "unidad_codigo": row["unidad_codigo"],
                "available_qty": _quantize_units(_to_decimal(row["available_qty"])),
                "committed_qty": _quantize_units(_to_decimal(row["committed_qty"])),
                "required_qty": _quantize_units(_to_decimal(row["required_qty"])),
                "shortage_qty": shortage_qty,
                "coverage_pct": _pct(_to_decimal(row["committed_qty"]), _to_decimal(row["required_qty"])).quantize(Decimal("0.01"))
                if _to_decimal(row["required_qty"]) > ZERO
                else ZERO,
                "remaining_after_commit": remaining_after_commit,
                "branches_text": ", ".join(sorted(row["branches"])),
                "recipes_text": ", ".join(sorted(row["recipes"])),
                "trace_label": row["trace_label"],
                "trace_effective_at": row["trace_effective_at"],
                "trace_quality": row["trace_quality"],
                "visible_updated_at": visible_updated_at,
                "purchase_generated_qty": purchase_generated_qty,
                "purchase_gap_qty": purchase_gap_qty,
                "purchase_folios_text": ", ".join(sorted(row["purchase_folios"])),
                "purchase_status_text": ", ".join(sorted(row["purchase_statuses"])),
                "is_blocker": shortage_qty > ZERO,
                "status_code": _stock_status(
                    available_before=_quantize_units(_to_decimal(row["available_qty"])),
                    shortage_qty=shortage_qty,
                )[0],
                "status_label": _stock_status(
                    available_before=_quantize_units(_to_decimal(row["available_qty"])),
                    shortage_qty=shortage_qty,
                )[1],
            }
        )

    rows.sort(key=lambda item: (-item["shortage_qty"], -item["required_qty"], item["insumo_nombre"]))
    latest_sync_ok = AlmacenSyncRun.objects.filter(status=AlmacenSyncRun.STATUS_OK).order_by("-started_at", "-id").first()
    latest_sync_any = AlmacenSyncRun.objects.order_by("-started_at", "-id").first()
    shortage_insumos = sum(1 for row in rows if row["shortage_qty"] > ZERO)
    purchase_generated_rows = sum(1 for row in rows if row["purchase_generated_qty"] > ZERO)
    unique_insumos = len(rows)
    traceable_rows = sum(1 for row in rows if row["trace_label"] != TRACE_LABELS.get(TRACE_UNTRACED))

    notes = [
        "Fuente de verdad para Producción: ledger ERP de insumos (ExistenciaInsumo + MovimientoInventario + importaciones/ajustes). Point no es la fuente física del inventario.",
        "Comprometido del día se calcula contra órdenes activas de la fecha; hoy es una reserva operativa calculada, no una reserva física persistida.",
        "El inventario de insumos hoy es global en ERP; no existe separación física por sucursal en esta conciliación.",
    ]
    if latest_sync_ok is None:
        notes.append("No existe un sync formal OK de almacén registrado; la confiabilidad depende de movimientos/importaciones/ajustes capturados en ERP.")
    elif latest_sync_ok.started_at.date() < target_date:
        notes.append(
            f"El último sync formal OK de almacén es {latest_sync_ok.started_at.date().isoformat()}; el stock visible puede estar más reciente por movimientos o ajustes."
        )

    return {
        "target_date": target_date,
        "source_of_truth": "ExistenciaInsumo + MovimientoInventario + importaciones/ajustes del ERP",
        "source_is_point": False,
        "inventory_scope": "GLOBAL_INSUMO",
        "summary": {
            "active_orders": len(selected_orders),
            "unique_insumos": unique_insumos,
            "shortage_insumos": shortage_insumos,
            "covered_insumos": max(unique_insumos - shortage_insumos, 0),
            "coverage_rows_pct": _pct(Decimal(max(unique_insumos - shortage_insumos, 0)), Decimal(unique_insumos)).quantize(Decimal("0.01"))
            if unique_insumos
            else ZERO,
            "purchase_generated_rows": purchase_generated_rows,
            "traceable_rows": traceable_rows,
        },
        "rows": rows,
        "orders": order_context,
        "notes": notes,
        "latest_visible_update": latest_visible_update,
        "latest_sync_ok": _serialize_sync_run(latest_sync_ok),
        "latest_sync_any": _serialize_sync_run(latest_sync_any),
        "generated_at": timezone.now(),
    }
