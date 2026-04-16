from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Q, Sum

from recetas.models import InventarioCedisProducto
from recetas.utils.commercial_composition import resolve_commercial_sku_interpretation
from ventas.models import (
    EventoVenta,
    EventoVentaCapacityRule,
    EventoVentaForecast,
    EventoVentaProductionLine,
    EventoVentaProductionPlan,
)
from ventas.services.notifications import create_unique_notification
from ventas.services.operational_targets import build_operational_targets

SAFE_SHELF_LIFE_DAYS = 2


def _resolve_production_day(demand_day: date) -> tuple[date, str | None]:
    if demand_day.weekday() == 6:
        shifted = demand_day - timedelta(days=1)
        return shifted, "Demanda dominical reprogramada a sabado por regla operativa sin produccion el domingo."
    return demand_day, None


def _resolve_capacity_limit(
    event: EventoVenta,
    plan_date: date,
    *,
    sold_product_id: int,
    planning_product_id: int,
) -> tuple[Decimal | None, str]:
    rules = EventoVentaCapacityRule.objects.filter(sales_event=event, is_active=True).filter(
        Q(product_id=sold_product_id) | Q(product_id=planning_product_id) | Q(product__isnull=True),
        Q(branch__isnull=True),
    )
    exact = rules.filter(capacity_date=plan_date, product_id=sold_product_id).order_by("-id").first()
    if exact:
        return Decimal(str(exact.max_production_qty or 0)), exact.notes or "Capacidad exacta por producto y fecha."
    if planning_product_id != sold_product_id:
        planning_exact = rules.filter(capacity_date=plan_date, product_id=planning_product_id).order_by("-id").first()
        if planning_exact:
            return (
                Decimal(str(planning_exact.max_production_qty or 0)),
                planning_exact.notes or "Capacidad exacta por SKU comercial efectivo y fecha.",
            )
    by_day = rules.filter(capacity_date=plan_date, product__isnull=True).order_by("-id").first()
    if by_day:
        return Decimal(str(by_day.max_production_qty or 0)), by_day.notes or "Capacidad general por fecha."
    by_product = rules.filter(capacity_date__isnull=True, product_id=sold_product_id).order_by("-id").first()
    if by_product:
        return Decimal(str(by_product.max_production_qty or 0)), by_product.notes or "Capacidad general por producto."
    if planning_product_id != sold_product_id:
        planning_product = rules.filter(capacity_date__isnull=True, product_id=planning_product_id).order_by("-id").first()
        if planning_product:
            return (
                Decimal(str(planning_product.max_production_qty or 0)),
                planning_product.notes or "Capacidad general por SKU comercial efectivo.",
            )
    global_rule = rules.filter(capacity_date__isnull=True, product__isnull=True).order_by("-id").first()
    if global_rule:
        return Decimal(str(global_rule.max_production_qty or 0)), global_rule.notes or "Capacidad general del evento."
    return None, ""


def _resolve_capacity_limit_cached(
    rules: list[EventoVentaCapacityRule],
    plan_date: date,
    *,
    sold_product_id: int,
    planning_product_id: int,
) -> tuple[Decimal | None, str]:
    exact = next(
        (
            rule
            for rule in rules
            if rule.capacity_date == plan_date and rule.product_id == sold_product_id
        ),
        None,
    )
    if exact:
        return Decimal(str(exact.max_production_qty or 0)), exact.notes or "Capacidad exacta por producto y fecha."
    if planning_product_id != sold_product_id:
        planning_exact = next(
            (
                rule
                for rule in rules
                if rule.capacity_date == plan_date and rule.product_id == planning_product_id
            ),
            None,
        )
        if planning_exact:
            return (
                Decimal(str(planning_exact.max_production_qty or 0)),
                planning_exact.notes or "Capacidad exacta por SKU comercial efectivo y fecha.",
            )
    by_day = next(
        (
            rule
            for rule in rules
            if rule.capacity_date == plan_date and rule.product_id is None
        ),
        None,
    )
    if by_day:
        return Decimal(str(by_day.max_production_qty or 0)), by_day.notes or "Capacidad general por fecha."
    by_product = next(
        (
            rule
            for rule in rules
            if rule.capacity_date is None and rule.product_id == sold_product_id
        ),
        None,
    )
    if by_product:
        return Decimal(str(by_product.max_production_qty or 0)), by_product.notes or "Capacidad general por producto."
    if planning_product_id != sold_product_id:
        planning_product = next(
            (
                rule
                for rule in rules
                if rule.capacity_date is None and rule.product_id == planning_product_id
            ),
            None,
        )
        if planning_product:
            return (
                Decimal(str(planning_product.max_production_qty or 0)),
                planning_product.notes or "Capacidad general por SKU comercial efectivo.",
            )
    global_rule = next(
        (
            rule
            for rule in rules
            if rule.capacity_date is None and rule.product_id is None
        ),
        None,
    )
    if global_rule:
        return Decimal(str(global_rule.max_production_qty or 0)), global_rule.notes or "Capacidad general del evento."
    return None, ""


def generate_production_plan(event: EventoVenta, *, promote_status: bool = True) -> dict:
    forecasts = EventoVentaForecast.objects.filter(sales_event=event)
    if not forecasts.exists():
        return {"created": 0, "warnings": ["No hay forecast calculado."]}

    EventoVentaProductionLine.objects.filter(production_plan__sales_event=event).delete()
    EventoVentaProductionPlan.objects.filter(sales_event=event).delete()

    warnings: list[str] = []
    constrained_lines = 0
    operational_targets = build_operational_targets(event)
    capacity_rules = list(
        EventoVentaCapacityRule.objects.filter(sales_event=event, is_active=True, branch__isnull=True).order_by("-id")
    )
    forecast_rows_map: dict[tuple[date, int], list[EventoVentaForecast]] = defaultdict(list)
    for forecast in forecasts.only(
        "id",
        "forecast_date",
        "product_id",
        "branch_id",
        "final_forecast",
        "conservative_forecast",
        "aggressive_forecast",
        "confidence_score",
    ):
        forecast_rows_map[(forecast.forecast_date, forecast.product_id)].append(forecast)
    grouped = list(
        forecasts.values("forecast_date", "product_id").annotate(
            total=Sum("final_forecast"),
            target_total=Sum("aggressive_forecast"),
        )
    )
    product_ids = {row["product_id"] for row in grouped}
    recipe_map = {
        recipe.id: recipe
        for recipe in forecasts.model.product.field.related_model.objects.filter(id__in=product_ids).only("id", "nombre", "codigo_point")
    }
    normalized_grouped: list[dict] = []
    for row in grouped:
        product = recipe_map.get(row["product_id"])
        if product is None:
            continue
        interpretation = resolve_commercial_sku_interpretation(product)
        planning_blocked = interpretation.is_blocked and interpretation.resolution_kind not in {
            "DIRECT_RECIPE",
            "ALIASED_RECIPE",
        }
        normalized_grouped.append(
            {
                **row,
                "sold_recipe": product,
                "planning_product_id": interpretation.planning_receta.id,
                "planning_notes": interpretation.notes,
                "resolution_kind": interpretation.resolution_kind,
                "is_blocked": planning_blocked,
                "blocked_reason": interpretation.blocked_reason,
            }
        )
    normalized_grouped.sort(key=lambda row: (row["planning_product_id"], row["forecast_date"], row["product_id"]))
    plan_map: dict[date, EventoVentaProductionPlan] = {}
    created_lines = 0
    planning_product_ids = {row["planning_product_id"] for row in normalized_grouped if not row.get("is_blocked")}
    inventory_map = {
        inventory.receta_id: Decimal(str(inventory.disponible or 0))
        for inventory in InventarioCedisProducto.objects.filter(receta_id__in=planning_product_ids)
    }
    remaining_cedis_stock: dict[int, Decimal] = {product_id: inventory_map.get(product_id, Decimal("0")) for product_id in planning_product_ids}

    for row in normalized_grouped:
        demand_day = row["forecast_date"]
        plan_date, warning = _resolve_production_day(demand_day)
        product_id = row["product_id"]
        planning_product_id = row["planning_product_id"]
        required_qty = Decimal(str(row["total"] or 0))
        forecast_rows = forecast_rows_map.get((demand_day, product_id), [])
        operational_target_qty = sum(
            (
                operational_targets.get(int(forecast.id)).target_qty
                if operational_targets.get(int(forecast.id))
                else Decimal(str(forecast.final_forecast or 0))
            )
            for forecast in forecast_rows
        )
        operational_target_qty = operational_target_qty.quantize(Decimal("0.001")) if operational_target_qty > 0 else Decimal("0")
        is_blocked = bool(row.get("is_blocked"))
        blocked_reason = str(row.get("blocked_reason") or "")

        if plan_date not in plan_map:
            plan_map[plan_date], _ = EventoVentaProductionPlan.objects.get_or_create(
                sales_event=event,
                plan_date=plan_date,
            )
        if warning:
            warnings.append(warning)
        for note in row["planning_notes"]:
            warnings.append(note)
        if is_blocked:
            EventoVentaProductionLine.objects.create(
                production_plan=plan_map[plan_date],
                product_id=product_id,
                required_qty=required_qty,
                planned_qty=Decimal("0"),
                existing_finished_stock=Decimal("0"),
                net_qty_to_produce=Decimal("0"),
                capacity_limit_qty=Decimal("0"),
                capacity_gap_qty=required_qty,
                constraint_reason=blocked_reason or "SKU bloqueado por ambigüedad; no se planifica producción.",
                production_day=plan_date,
                priority="CRITICA",
            )
            created_lines += 1
            constrained_lines += 1
            continue

        visible_stock = remaining_cedis_stock[planning_product_id]
        target_before_stock = max(required_qty, operational_target_qty)
        stock_applied = min(visible_stock, target_before_stock)
        remaining_cedis_stock[planning_product_id] = max(Decimal("0"), visible_stock - stock_applied)
        net_qty = max(Decimal("0"), target_before_stock - stock_applied)
        capacity_limit, constraint_reason = _resolve_capacity_limit_cached(
            capacity_rules,
            plan_date,
            sold_product_id=product_id,
            planning_product_id=planning_product_id,
        )
        capacity_gap = Decimal("0")
        planned_qty = net_qty
        if capacity_limit is not None and net_qty > capacity_limit:
            capacity_gap = net_qty - capacity_limit
            planned_qty = capacity_limit
            constrained_lines += 1
            warnings.append(
                f"Capacidad limitada para producto {product_id} el {plan_date}: objetivo operativo {net_qty} y permitido {capacity_limit}."
            )

        target_reason = ""
        if operational_target_qty > required_qty:
            sample_target = next(
                (
                    operational_targets.get(int(forecast.id))
                    for forecast in forecast_rows
                    if operational_targets.get(int(forecast.id))
                ),
                None,
            )
            target_reason = sample_target.reason if sample_target else "Objetivo operativo con service level superior al forecast."

        EventoVentaProductionLine.objects.create(
            production_plan=plan_map[plan_date],
            product_id=product_id,
            required_qty=required_qty,
            planned_qty=planned_qty,
            existing_finished_stock=stock_applied,
            net_qty_to_produce=planned_qty,
            capacity_limit_qty=capacity_limit or Decimal("0"),
            capacity_gap_qty=capacity_gap,
            constraint_reason=" · ".join(part for part in (constraint_reason, target_reason) if part),
            production_day=plan_date,
            priority="CRITICA" if capacity_gap > 0 else ("ALTA" if planned_qty > 0 else "BAJA"),
        )
        created_lines += 1

    if promote_status:
        event.status = EventoVenta.STATUS_ENVIADO_PROD
        event.save(update_fields=["status", "updated_at"])
    create_unique_notification(event, f"Plan de produccion generado con {created_lines} lineas.")
    if warnings:
        create_unique_notification(event, warnings[0], severity="WARN")
    if constrained_lines:
        create_unique_notification(
            event,
            f"Plan de produccion con {constrained_lines} lineas limitadas por capacidad.",
            severity="WARN",
        )

    return {
        "created": created_lines,
        "plans": len(plan_map),
        "warnings": warnings[:20],
        "shelf_life_days": SAFE_SHELF_LIFE_DAYS,
        "constrained_lines": constrained_lines,
    }
