from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.db.models import Sum
from django.utils import timezone

from reportes.models import FactVentaDiaria, OperationsMetricSnapshot, ProductionExecutionLog, ProductionOrder


ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
EPSILON = Decimal("0.001")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= ZERO:
        return ZERO
    return ((numerator / denominator) * HUNDRED).quantize(Decimal("0.01"))


def _avg(total: Decimal, count: int) -> Decimal:
    if count <= 0:
        return ZERO
    return (total / Decimal(count)).quantize(Decimal("0.001"))


def _band(adoption_pct: Decimal) -> str:
    if adoption_pct >= Decimal("80"):
        return "ALTA"
    if adoption_pct >= Decimal("50"):
        return "MEDIA"
    return "BAJA"


def _load_sales_maps(
    *,
    target_date: date,
    keys: set[tuple[int, int]],
) -> tuple[dict[tuple[int, int], Decimal], dict[tuple[int, int], Decimal]]:
    if not keys:
        return {}, {}
    branch_ids = sorted({branch_id for branch_id, _ in keys})
    recipe_ids = sorted({recipe_id for _, recipe_id in keys})
    current_rows = (
        FactVentaDiaria.objects.filter(
            fecha=target_date,
            sucursal_id__in=branch_ids,
            receta_id__in=recipe_ids,
        )
        .values("sucursal_id", "receta_id")
        .annotate(total=Sum("venta_neta"))
    )
    actual_sales_map = {
        (int(row["sucursal_id"]), int(row["receta_id"])): _to_decimal(row.get("total"))
        for row in current_rows
    }
    baseline_rows = (
        FactVentaDiaria.objects.filter(
            fecha__gte=target_date - timedelta(days=56),
            fecha__lt=target_date,
            sucursal_id__in=branch_ids,
            receta_id__in=recipe_ids,
        )
        .values("fecha", "sucursal_id", "receta_id")
        .annotate(total=Sum("venta_neta"))
    )
    grouped_baseline: dict[tuple[int, int], list[Decimal]] = defaultdict(list)
    target_weekday = target_date.weekday()
    for row in baseline_rows:
        if row["fecha"].weekday() != target_weekday:
            continue
        grouped_baseline[(int(row["sucursal_id"]), int(row["receta_id"]))].append(_to_decimal(row.get("total")))
    baseline_sales_map = {
        key: (sum(values, ZERO) / Decimal(len(values))).quantize(Decimal("0.01"))
        for key, values in grouped_baseline.items()
        if values
    }
    return actual_sales_map, baseline_sales_map


def rebuild_operations_metrics(*, target_date: date | None = None) -> dict[str, object]:
    target_date = target_date or timezone.localdate()
    orders = list(
        ProductionOrder.objects.filter(fecha=target_date)
        .select_related("sucursal")
        .prefetch_related("lines__receta")
        .order_by("sucursal__codigo", "id")
    )
    execution_logs = list(
        ProductionExecutionLog.objects.filter(fecha=target_date)
        .select_related("sucursal", "receta")
        .order_by("sucursal__codigo", "receta__nombre")
    )

    order_line_map: dict[tuple[int, int], dict[str, Decimal]] = {}
    lines_total = 0
    lines_without_change = 0
    approval_deviation_total = ZERO
    approval_deviation_count = 0
    by_branch: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "orders": 0,
            "lines": 0,
            "adopted_lines": 0,
            "unchanged_executed_lines": 0,
            "modified_lines": 0,
            "ignored_lines": 0,
            "recommended_units": ZERO,
            "approved_units": ZERO,
            "executed_units": ZERO,
            "sold_units": ZERO,
            "merma_units": ZERO,
            "avoidable_overproduction_units": ZERO,
            "impacto_estimado": ZERO,
            "impacto_real": ZERO,
        }
    )
    for order in orders:
        branch_bucket = by_branch[order.sucursal.codigo]
        branch_bucket["orders"] += 1
        for line in order.lines.all():
            lines_total += 1
            branch_bucket["lines"] += 1
            recommended = _to_decimal(line.cantidad_recomendada)
            approved = _to_decimal(line.cantidad_aprobada)
            if approved == ZERO:
                approved = recommended
            deviation = abs(approved - recommended)
            approval_deviation_total += deviation
            approval_deviation_count += 1
            branch_bucket["recommended_units"] += recommended
            branch_bucket["approved_units"] += approved
            if deviation <= EPSILON:
                lines_without_change += 1
                branch_bucket["adopted_lines"] += 1
            order_line_map[(int(order.sucursal_id), int(line.receta_id))] = {
                "recommended": recommended,
                "approved": approved,
            }

    actual_sales_map, baseline_sales_map = _load_sales_maps(target_date=target_date, keys=set(order_line_map.keys()))
    execution_deviation_total = ZERO
    execution_deviation_count = 0
    merma_total = ZERO
    sold_total = ZERO
    approved_total = ZERO
    avoidable_cost_total = ZERO
    real_impact_total = ZERO
    by_recipe_merma: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for log in execution_logs:
        branch_bucket = by_branch[log.sucursal.codigo]
        recommended = _to_decimal(log.recomendado)
        approved = _to_decimal(log.aprobado) if _to_decimal(log.aprobado) > ZERO else recommended
        executed = _to_decimal(log.producido_real)
        sold = _to_decimal(log.vendido_real)
        merma = _to_decimal(log.merma)
        approved_total += approved
        execution_deviation_total += abs(executed - approved)
        execution_deviation_count += 1
        merma_total += merma
        sold_total += sold
        branch_bucket["executed_units"] += executed
        branch_bucket["sold_units"] += sold
        branch_bucket["merma_units"] += merma
        branch_bucket["avoidable_overproduction_units"] += max(executed - sold, ZERO)
        unit_cost = ZERO
        metadata = log.metadata or {}
        if metadata.get("estimated_unit_cost"):
            unit_cost = _to_decimal(metadata.get("estimated_unit_cost"))
        avoidable_cost = max(executed - sold, ZERO) * unit_cost
        avoidable_cost_total += avoidable_cost
        branch_bucket["impacto_estimado"] += avoidable_cost
        key = (int(log.sucursal_id), int(log.receta_id))
        actual_sales = actual_sales_map.get(key, ZERO)
        baseline_sales = baseline_sales_map.get(key, ZERO)
        merma_cost = merma * unit_cost
        impact_real = actual_sales - baseline_sales - merma_cost
        real_impact_total += impact_real
        branch_bucket["impacto_real"] += impact_real
        if abs(approved - recommended) <= EPSILON and abs(executed - approved) <= EPSILON:
            branch_bucket["unchanged_executed_lines"] += 1
        elif executed <= ZERO:
            branch_bucket["ignored_lines"] += 1
        else:
            branch_bucket["modified_lines"] += 1
        by_recipe_merma[log.receta.nombre] += merma

    missing_execution_keys = set(order_line_map.keys()) - {
        (int(log.sucursal_id), int(log.receta_id)) for log in execution_logs
    }
    for branch_id, recipe_id in missing_execution_keys:
        branch_code = next((order.sucursal.codigo for order in orders if int(order.sucursal_id) == branch_id), "")
        if branch_code:
            by_branch[branch_code]["ignored_lines"] += 1

    adoption_pct = _pct(Decimal(lines_without_change), Decimal(lines_total))
    approval_deviation_avg = _avg(approval_deviation_total, approval_deviation_count)
    execution_deviation_avg = _avg(execution_deviation_total, execution_deviation_count)
    impacto_economico_estimado = avoidable_cost_total.quantize(Decimal("0.01"))
    impacto_real = real_impact_total.quantize(Decimal("0.01"))
    desviacion_impacto = (impacto_real - impacto_economico_estimado).quantize(Decimal("0.01"))
    adopcion_real = _pct(
        sum(Decimal(int(values["unchanged_executed_lines"])) for values in by_branch.values()),
        Decimal(lines_total),
    )
    efectividad_recomendaciones = min(
        _pct(sold_total, max(approved_total, ONE)),
        Decimal("100.00"),
    )

    payload = {
        "orders": len(orders),
        "lines": lines_total,
        "logs": len(execution_logs),
        "adopted_lines": lines_without_change,
        "sold_units": str(sold_total.quantize(Decimal("0.001"))),
        "impact": {
            "baseline_type": "historical_same_weekday_8w_avg",
            "impacto_estimado": str(impacto_economico_estimado),
            "impacto_real": str(impacto_real),
            "desviacion_impacto": str(desviacion_impacto),
            "missing_data": [
                "baseline pre-automatización por sucursal/producto cerrado",
            ],
        },
        "by_branch": {
            branch_code: {
                **values,
                "adoption_pct": str(_pct(Decimal(int(values["adopted_lines"])), Decimal(int(values["lines"] or 0)))),
                "adopcion_real_pct": str(_pct(Decimal(int(values["unchanged_executed_lines"])), Decimal(int(values["lines"] or 0)))),
                "modified_pct": str(_pct(Decimal(int(values["modified_lines"])), Decimal(int(values["lines"] or 0)))),
                "ignored_pct": str(_pct(Decimal(int(values["ignored_lines"])), Decimal(int(values["lines"] or 0)))),
                "adoption_band": _band(_pct(Decimal(int(values["unchanged_executed_lines"])), Decimal(int(values["lines"] or 0)))),
                "recommended_units": str(values["recommended_units"].quantize(Decimal("0.001"))),
                "approved_units": str(values["approved_units"].quantize(Decimal("0.001"))),
                "executed_units": str(values["executed_units"].quantize(Decimal("0.001"))),
                "sold_units": str(values["sold_units"].quantize(Decimal("0.001"))),
                "merma_units": str(values["merma_units"].quantize(Decimal("0.001"))),
                "avoidable_overproduction_units": str(values["avoidable_overproduction_units"].quantize(Decimal("0.001"))),
                "impacto_estimado": str(values["impacto_estimado"].quantize(Decimal("0.01"))),
                "impacto_real": str(values["impacto_real"].quantize(Decimal("0.01"))),
            }
            for branch_code, values in by_branch.items()
        },
        "top_merma_products": [
            {"recipe_name": recipe_name, "merma_units": str(merma.quantize(Decimal("0.001")))}
            for recipe_name, merma in sorted(by_recipe_merma.items(), key=lambda item: item[1], reverse=True)[:10]
        ],
    }
    snapshot, _ = OperationsMetricSnapshot.objects.update_or_create(
        fecha=target_date,
        defaults={
            "adoption_pct": adoption_pct,
            "approval_deviation_avg": approval_deviation_avg,
            "execution_deviation_avg": execution_deviation_avg,
            "merma_total": merma_total.quantize(Decimal("0.001")),
            "impacto_economico_estimado": impacto_economico_estimado,
            "impacto_real": impacto_real,
            "desviacion_impacto": desviacion_impacto,
            "adopcion_real": adopcion_real,
            "efectividad_recomendaciones": efectividad_recomendaciones,
            "payload": payload,
            "generated_at": timezone.now(),
        },
    )
    return {
        "target_date": target_date.isoformat(),
        "snapshot_id": snapshot.id,
        "adoption_pct": str(snapshot.adoption_pct),
        "approval_deviation_avg": str(snapshot.approval_deviation_avg),
        "execution_deviation_avg": str(snapshot.execution_deviation_avg),
        "merma_total": str(snapshot.merma_total),
        "impacto_economico_estimado": str(snapshot.impacto_economico_estimado),
        "impacto_real": str(snapshot.impacto_real),
        "desviacion_impacto": str(snapshot.desviacion_impacto),
        "adopcion_real": str(snapshot.adopcion_real),
        "efectividad_recomendaciones": str(snapshot.efectividad_recomendaciones),
        "orders": len(orders),
        "logs": len(execution_logs),
    }
