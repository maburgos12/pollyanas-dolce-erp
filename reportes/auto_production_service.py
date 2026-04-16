from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation

from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from reportes.decision_score_service import build_decision_score_context
from reportes.forecast_calibration_service import summarize_latest_forecast_calibration
from reportes.forecast_service import build_daily_forecast_context
from reportes.models import FactProduccionDiaria, FactVentaDiaria, ProductionExecutionLog, ProductionOrder, ProductionOrderLine
from reportes.production_recommendation_service import build_production_recommendation_context


ZERO = Decimal("0")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _recommendation_version(target_date: date) -> str:
    calibration = summarize_latest_forecast_calibration(reference_date=target_date)
    calibration_ref = calibration.get("reference_date") if calibration else None
    return f"fcstcal:{calibration_ref or target_date.isoformat()}"


def _is_mutable_order(order: ProductionOrder) -> bool:
    return order.status in {
        ProductionOrder.STATUS_DRAFT,
        ProductionOrder.STATUS_PROPOSED,
    }


def generate_daily_production_orders(
    target_date: date,
    *,
    sucursal_id: int | None = None,
    created_by=None,
) -> dict[str, object]:
    if target_date.weekday() == 6:
        return {
            "target_date": target_date.isoformat(),
            "generated_orders": 0,
            "updated_orders": 0,
            "skipped_locked_orders": 0,
            "lines": 0,
            "note": "Domingo queda sin producción programada; no se generan órdenes automáticas.",
        }

    forecast_context = build_daily_forecast_context(target_date=target_date, top_n=None)
    production_context = build_production_recommendation_context(
        target_date=target_date,
        forecast_context=forecast_context,
        top_n=None,
    )
    score_context = build_decision_score_context(
        target_date=target_date,
        forecast_context=forecast_context,
        production_context=production_context,
        top_n=None,
    )

    score_map = {
        (int(row["branch_id"]), int(row["recipe_id"])): row
        for row in (score_context.get("rows") or [])
    }
    grouped_rows: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in production_context.get("rows") or []:
        branch_id = int(row["branch_id"])
        if sucursal_id and branch_id != sucursal_id:
            continue
        suggested_units = _to_decimal(row.get("suggested_units"))
        if suggested_units <= ZERO:
            continue
        grouped_rows[branch_id].append(row)

    generated_orders = 0
    updated_orders = 0
    skipped_locked_orders = 0
    total_lines = 0
    version = _recommendation_version(target_date)
    orders_payload: list[dict[str, object]] = []

    for branch_id, rows in grouped_rows.items():
        sample = rows[0]
        defaults = {
            "status": ProductionOrder.STATUS_PROPOSED,
            "source": ProductionOrder.SOURCE_AUTO,
            "recommendation_version": version,
            "created_by": created_by,
            "metadata": {
                "target_label": production_context.get("target_label"),
                "target_date": production_context.get("target_date"),
                "rows_considered": len(rows),
            },
        }
        with transaction.atomic():
            order, created = ProductionOrder.objects.select_for_update().get_or_create(
                fecha=target_date,
                sucursal_id=branch_id,
                defaults=defaults,
            )
            if not created and not _is_mutable_order(order):
                skipped_locked_orders += 1
                continue
            if not created:
                order.status = ProductionOrder.STATUS_PROPOSED
                order.source = ProductionOrder.SOURCE_AUTO
                order.recommendation_version = version
                order.metadata = defaults["metadata"]
                if created_by and not order.created_by_id:
                    order.created_by = created_by
                order.save(
                    update_fields=[
                        "status",
                        "source",
                        "recommendation_version",
                        "metadata",
                        "created_by",
                        "updated_at",
                    ]
                )
                updated_orders += 1
            else:
                generated_orders += 1

            existing_lines = {line.receta_id: line for line in order.lines.all()}
            keep_recipe_ids: set[int] = set()
            line_payload: list[dict[str, object]] = []
            for row in rows:
                recipe_id = int(row["recipe_id"])
                keep_recipe_ids.add(recipe_id)
                score_row = score_map.get((branch_id, recipe_id), {})
                motivo_parts = [str(row.get("why") or "").strip(), str(score_row.get("why") or "").strip()]
                motivo = " ".join(part for part in motivo_parts if part)
                defaults_line = {
                    "cantidad_recomendada": _quantize_units(_to_decimal(row.get("suggested_units"))),
                    "cantidad_aprobada": ZERO,
                    "cantidad_ejecutada": ZERO,
                    "decision_score": _to_decimal(score_row.get("score")),
                    "riesgo_merma": row.get("risk_level") or ProductionOrderLine.RISK_LOW,
                    "motivo": motivo,
                    "metadata": {
                        "branch_code": row.get("branch_code"),
                        "branch_name": row.get("branch_name"),
                        "forecast_qty": str(row.get("forecast_qty") or "0"),
                        "forecast_min_qty": str(row.get("forecast_min_qty") or "0"),
                        "stock_units": str(row.get("stock_units") or "0"),
                        "stock_cover_days": str(row.get("stock_cover_days") or "0"),
                        "waste_rate_pct": str(row.get("waste_rate_pct") or "0"),
                        "decision_priority": score_row.get("priority"),
                        "recommended_action": score_row.get("recommended_action"),
                    },
                }
                line = existing_lines.get(recipe_id)
                if line is None:
                    line = ProductionOrderLine.objects.create(order=order, receta_id=recipe_id, **defaults_line)
                else:
                    line.cantidad_recomendada = defaults_line["cantidad_recomendada"]
                    if line.cantidad_aprobada <= ZERO:
                        line.cantidad_aprobada = ZERO
                    line.decision_score = defaults_line["decision_score"]
                    line.riesgo_merma = defaults_line["riesgo_merma"]
                    line.motivo = defaults_line["motivo"]
                    line.metadata = defaults_line["metadata"]
                    line.save(
                        update_fields=[
                            "cantidad_recomendada",
                            "decision_score",
                            "riesgo_merma",
                            "motivo",
                            "metadata",
                            "updated_at",
                        ]
                    )
                total_lines += 1
                line_payload.append(
                    {
                        "recipe_id": recipe_id,
                        "recipe_name": row.get("recipe_name"),
                        "recommended_units": str(line.cantidad_recomendada),
                        "score": str(line.decision_score),
                        "risk_level": line.riesgo_merma,
                    }
                )
            order.lines.exclude(receta_id__in=keep_recipe_ids).delete()
            orders_payload.append(
                {
                    "order_id": order.id,
                    "branch_id": branch_id,
                    "branch_code": sample.get("branch_code"),
                    "branch_name": sample.get("branch_name"),
                    "status": order.status,
                    "created": created,
                    "line_count": len(line_payload),
                    "lines": line_payload,
                }
            )

    return {
        "target_date": target_date.isoformat(),
        "recommendation_version": version,
        "generated_orders": generated_orders,
        "updated_orders": updated_orders,
        "skipped_locked_orders": skipped_locked_orders,
        "lines": total_lines,
        "orders": orders_payload,
    }


def approve_production_order(
    order: ProductionOrder,
    *,
    approved_by,
    approved_quantities: dict[int, Decimal] | None = None,
) -> ProductionOrder:
    approved_quantities = approved_quantities or {}
    if order.status not in {ProductionOrder.STATUS_DRAFT, ProductionOrder.STATUS_PROPOSED}:
        return order
    with transaction.atomic():
        order = ProductionOrder.objects.select_for_update().prefetch_related("lines").get(pk=order.pk)
        for line in order.lines.all():
            override_qty = approved_quantities.get(line.receta_id)
            line.cantidad_aprobada = _quantize_units(
                _to_decimal(override_qty) if override_qty is not None else max(line.cantidad_recomendada, ZERO)
            )
            line.save(update_fields=["cantidad_aprobada", "updated_at"])
        order.status = ProductionOrder.STATUS_APPROVED
        order.approved_by = approved_by
        order.approved_at = timezone.now()
        order.save(update_fields=["status", "approved_by", "approved_at", "updated_at"])
    return order


def release_production_order(order: ProductionOrder) -> ProductionOrder:
    if order.status != ProductionOrder.STATUS_APPROVED:
        return order
    with transaction.atomic():
        order = ProductionOrder.objects.select_for_update().get(pk=order.pk)
        order.status = ProductionOrder.STATUS_RELEASED
        order.released_at = timezone.now()
        order.save(update_fields=["status", "released_at", "updated_at"])
    return order


def execute_production_order(
    order: ProductionOrder,
    *,
    executed_quantities: dict[int, Decimal] | None = None,
) -> ProductionOrder:
    executed_quantities = executed_quantities or {}
    if order.status not in {ProductionOrder.STATUS_APPROVED, ProductionOrder.STATUS_RELEASED}:
        return order
    with transaction.atomic():
        order = ProductionOrder.objects.select_for_update().prefetch_related("lines").get(pk=order.pk)
        for line in order.lines.all():
            override_qty = executed_quantities.get(line.receta_id)
            baseline_qty = line.cantidad_aprobada if _to_decimal(line.cantidad_aprobada) > ZERO else line.cantidad_recomendada
            line.cantidad_ejecutada = _quantize_units(
                _to_decimal(override_qty) if override_qty is not None else max(_to_decimal(baseline_qty), ZERO)
            )
            line.save(update_fields=["cantidad_ejecutada", "updated_at"])
        order.status = ProductionOrder.STATUS_EXECUTED
        order.executed_at = timezone.now()
        order.save(update_fields=["status", "executed_at", "updated_at"])
    return order


def sync_production_execution_logs(
    *,
    target_date: date,
    sucursal_id: int | None = None,
    actor=None,
) -> dict[str, object]:
    orders = list(
        ProductionOrder.objects.filter(
            fecha=target_date,
            status__in=[ProductionOrder.STATUS_RELEASED, ProductionOrder.STATUS_EXECUTED],
        )
        .select_related("sucursal")
        .prefetch_related("lines__receta")
        .order_by("sucursal__codigo", "id")
    )
    if sucursal_id:
        orders = [order for order in orders if order.sucursal_id == sucursal_id]
    if not orders:
        return {
            "target_date": target_date.isoformat(),
            "orders": 0,
            "logs": 0,
        }

    branch_ids = sorted({order.sucursal_id for order in orders})
    sales_map = {
        (int(row["sucursal_id"]), int(row["receta_id"])): {
            "qty": _to_decimal(row.get("total")),
            "cost_total": _to_decimal(row.get("cost_total")),
        }
        for row in FactVentaDiaria.objects.filter(
            fecha=target_date,
            sucursal_id__in=branch_ids,
            receta_id__isnull=False,
        )
        .values("sucursal_id", "receta_id")
        .annotate(total=Sum("cantidad"), cost_total=Sum("costo_estimado"))
    }
    production_fact_map = {
        (int(row["sucursal_id"]), int(row["receta_id"])): {
            "vendido": _to_decimal(row.get("vendido")),
            "merma": _to_decimal(row.get("merma")),
        }
        for row in FactProduccionDiaria.objects.filter(
            fecha=target_date,
            sucursal_id__in=branch_ids,
            receta_id__isnull=False,
        )
        .values("sucursal_id", "receta_id")
        .annotate(vendido=Sum("vendido"), merma=Sum("merma"))
    }

    total_logs = 0
    with transaction.atomic():
        for order in orders:
            if order.status != ProductionOrder.STATUS_EXECUTED:
                order.status = ProductionOrder.STATUS_EXECUTED
                order.executed_at = order.executed_at or timezone.now()
                order.save(update_fields=["status", "executed_at", "updated_at"])
            for line in order.lines.all():
                key = (order.sucursal_id, line.receta_id)
                fact_row = production_fact_map.get(key, {})
                sales_row = sales_map.get(key, {})
                vendido_real = _to_decimal(sales_row.get("qty"), str(_to_decimal(fact_row.get("vendido"))))
                merma_real = _to_decimal(fact_row.get("merma"))
                stock_visible = _to_decimal((line.metadata or {}).get("stock_units"))
                estimated_unit_cost = ZERO
                if vendido_real > ZERO:
                    estimated_unit_cost = _to_decimal(sales_row.get("cost_total")) / vendido_real
                ProductionExecutionLog.objects.update_or_create(
                    fecha=target_date,
                    sucursal_id=order.sucursal_id,
                    receta_id=line.receta_id,
                    defaults={
                        "recomendado": line.cantidad_recomendada,
                        "aprobado": line.cantidad_aprobada,
                        "producido_real": line.cantidad_ejecutada,
                        "vendido_real": vendido_real,
                        "merma": merma_real,
                        "desviacion": _quantize_units(line.cantidad_ejecutada - line.cantidad_recomendada),
                        "stock_visible": stock_visible,
                        "decision_score": line.decision_score,
                        "recommendation_version": order.recommendation_version,
                        "usuario": actor or order.approved_by or order.created_by,
                        "comentario": line.motivo,
                        "metadata": {
                            "order_id": order.id,
                            "order_line_id": line.id,
                            "order_status": order.status,
                            "branch_code": order.sucursal.codigo,
                            "estimated_unit_cost": str(_quantize_units(estimated_unit_cost)),
                        },
                    },
                )
                total_logs += 1

    return {
        "target_date": target_date.isoformat(),
        "orders": len(orders),
        "logs": total_logs,
    }
