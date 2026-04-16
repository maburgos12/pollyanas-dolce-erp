from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import csv

from django.conf import settings
from django.db.models import Q, Sum

from core.models import Sucursal
from pos_bridge.models import PointDailySale
from pos_bridge.services.sales_materialization_repair_service import BridgeSalesMaterializationRepairService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import VentaHistorica
from ventas.models import EventoVenta, VentaAutoritativaPoint


@dataclass
class PointReconciliationSummary:
    start_date: date
    end_date: date
    scanned_rows: int
    authoritative_rows: int
    unresolved_rows: int
    non_recipe_rows: int
    bridge_rows: int
    mismatch_rows: int
    qty_diff_total: Decimal
    sales_diff_total: Decimal
    report_path: str


def _as_decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def _reconciliation_dir(event: EventoVenta) -> Path:
    root = Path(settings.BASE_DIR) / "output" / "spreadsheet" / "ventas_eventos" / event.code.lower() / "actual"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _historical_window_for_event(event: EventoVenta) -> tuple[date, date]:
    return event.analysis_start_date.replace(year=event.analysis_start_date.year - 1), event.analysis_end_date.replace(
        year=event.analysis_end_date.year - 1
    )


def reconcile_event_point_sales(event: EventoVenta) -> PointReconciliationSummary:
    start_date, end_date = _historical_window_for_event(event)
    repair = BridgeSalesMaterializationRepairService().repair(start_date=start_date, end_date=end_date)

    matcher = PointSalesMatchingService()
    branch_ids = list(event.branches.filter(is_active=True).values_list("branch_id", flat=True))
    point_qs = (
        PointDailySale.objects.select_related("branch__erp_branch", "product")
        .filter(sale_date__range=(start_date, end_date), branch__erp_branch_id__in=branch_ids)
        .order_by("sale_date", "branch__erp_branch_id", "product__sku", "id")
    )

    truth: dict[tuple[date, int, int], dict[str, object]] = {}
    unresolved_rows = 0
    non_recipe_rows = 0

    for sale in point_qs:
        payload = {
            "sku": sale.product.sku,
            "name": sale.product.name,
            "category": sale.product.category,
            "family": (sale.product.metadata or {}).get("family", ""),
        }
        if matcher.is_non_recipe_sale_row(payload):
            non_recipe_rows += 1
            continue
        receta = matcher.resolve_receta(codigo_point=sale.product.sku, point_name=sale.product.name)
        if receta is None:
            unresolved_rows += 1
            continue
        branch_id = sale.branch.erp_branch_id
        if not branch_id:
            continue
        key = (sale.sale_date, branch_id, receta.id)
        bucket = truth.setdefault(
            key,
            {
                "recipe_id": receta.id,
                "recipe_name": receta.nombre,
                "branch_id": branch_id,
                "branch_code": sale.branch.erp_branch.codigo,
                "branch_name": sale.branch.erp_branch.nombre,
                "sale_date": sale.sale_date,
                "point_qty": Decimal("0"),
                "point_sales": Decimal("0"),
                "source": "POINT_RAW",
            },
        )
        bucket["point_qty"] = _as_decimal(bucket["point_qty"]) + _as_decimal(sale.quantity)
        bucket["point_sales"] = _as_decimal(bucket["point_sales"]) + _as_decimal(sale.total_amount)

    authoritative_qs = VentaAutoritativaPoint.objects.filter(
        sale_date__range=(start_date, end_date),
        branch_id__in=branch_ids,
        product_id__isnull=False,
    ).select_related("branch", "product")
    authoritative_rows = 0
    for row in authoritative_qs:
        key = (row.sale_date, row.branch_id, row.product_id)
        truth[key] = {
            "recipe_id": row.product_id,
            "recipe_name": row.product.nombre if row.product_id else row.point_name,
            "branch_id": row.branch_id,
            "branch_code": row.branch.codigo,
            "branch_name": row.branch.nombre,
            "sale_date": row.sale_date,
            "point_qty": _as_decimal(row.quantity),
            "point_sales": _as_decimal(row.total_amount),
            "source": "POINT_AUTORITATIVO",
        }
        authoritative_rows += 1

    bridge_rows = {
        (row["fecha"], row["sucursal_id"], row["receta_id"]): {
            "bridge_qty": _as_decimal(row["cantidad"]),
            "bridge_sales": _as_decimal(row["monto_total"]),
        }
        for row in VentaHistorica.objects.filter(
            fuente=BridgeSalesMaterializationRepairService.SALES_HISTORY_SOURCE,
            fecha__range=(start_date, end_date),
            sucursal_id__in=branch_ids,
        )
        .values("fecha", "sucursal_id", "receta_id", "cantidad", "monto_total")
    }

    report_rows: list[dict[str, object]] = []
    qty_diff_total = Decimal("0")
    sales_diff_total = Decimal("0")
    mismatch_rows = 0

    all_keys = sorted(set(truth.keys()) | set(bridge_rows.keys()))
    for sale_date, branch_id, recipe_id in all_keys:
        point = truth.get((sale_date, branch_id, recipe_id), {})
        bridge = bridge_rows.get((sale_date, branch_id, recipe_id), {})
        point_qty = _as_decimal(point.get("point_qty"))
        point_sales = _as_decimal(point.get("point_sales"))
        bridge_qty = _as_decimal(bridge.get("bridge_qty"))
        bridge_sales = _as_decimal(bridge.get("bridge_sales"))
        qty_diff = point_qty - bridge_qty
        sales_diff = point_sales - bridge_sales
        if qty_diff == 0 and sales_diff == 0:
            continue
        mismatch_rows += 1
        qty_diff_total += qty_diff
        sales_diff_total += sales_diff
        report_rows.append(
            {
                "sale_date": sale_date.isoformat(),
                "branch_code": point.get("branch_code") or Sucursal.objects.filter(id=branch_id).values_list("codigo", flat=True).first() or "",
                "branch_name": point.get("branch_name") or Sucursal.objects.filter(id=branch_id).values_list("nombre", flat=True).first() or "",
                "recipe_id": recipe_id,
                "recipe_name": point.get("recipe_name") or "",
                "point_source": point.get("source") or "POINT_RAW",
                "point_qty": f"{point_qty:.3f}",
                "bridge_qty": f"{bridge_qty:.3f}",
                "qty_diff": f"{qty_diff:.3f}",
                "point_sales": f"{point_sales:.2f}",
                "bridge_sales": f"{bridge_sales:.2f}",
                "sales_diff": f"{sales_diff:.2f}",
            }
        )

    report_dir = _reconciliation_dir(event)
    report_path = report_dir / f"{event.code.lower()}_point_reconciliation_{start_date.isoformat()}_{end_date.isoformat()}.csv"
    with report_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "sale_date",
                "branch_code",
                "branch_name",
                "recipe_id",
                "recipe_name",
                "point_source",
                "point_qty",
                "bridge_qty",
                "qty_diff",
                "point_sales",
                "bridge_sales",
                "sales_diff",
            ],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    return PointReconciliationSummary(
        start_date=start_date,
        end_date=end_date,
        scanned_rows=repair.scanned_rows,
        authoritative_rows=authoritative_rows,
        unresolved_rows=unresolved_rows + repair.unresolved_rows,
        non_recipe_rows=non_recipe_rows + repair.non_recipe_rows,
        bridge_rows=len(bridge_rows),
        mismatch_rows=mismatch_rows,
        qty_diff_total=qty_diff_total,
        sales_diff_total=sales_diff_total,
        report_path=str(report_path),
    )
