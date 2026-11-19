"""Observed operational stock, separate from monthly audit frontiers.

The user established 03 October as Leyva's operational opening. This archived
capture is later that morning, not proof of a midnight or 30 September balance.
"""
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from pos_bridge.models import PointExtractionLog, PointInventorySnapshot, PointSyncJob
from reportes.models import ProductInventoryAuditCase

_CAPTURED_AT = datetime(2026, 10, 3, 17, 48, 28, 494613, tzinfo=timezone.utc)
_HEADERS = ["Código", "Producto", "Cantidad", "Unidad", "Costo Unitario", "Costo Total", "Último Movimiento", "Opciones"]


def add_operational_opening_evidence(cutoff, groups):
    """Enrich only already-visible HTML cases; never recalculate or persist."""
    if cutoff != date(2026, 10, 31):
        return
    cases = {case["id"]: case for group in groups for row in group["rows"]
             for case in row["cases"] if case["opening"] is None}
    if not cases:
        return
    pairs = dict(ProductInventoryAuditCase.objects.filter(
        pk__in=cases, month=date(2026, 10, 1), branch_id=28,
        branch__external_id="14", branch__erp_branch_id=18,
    ).values_list("product_id", "pk"))
    if not pairs or not PointExtractionLog.objects.filter(
        pk=302317, sync_job_id=81086, level=PointExtractionLog.LEVEL_INFO,
        message="Sucursal procesada 14.",
        context__branch_id=28, context__branch_external_id="14",
    ).exists():
        return
    snapshots = list(PointInventorySnapshot.objects.filter(
        sync_job_id=81086, branch_id=28, product_id__in=pairs,
        captured_at=_CAPTURED_AT, sync_job__status=PointSyncJob.STATUS_SUCCESS,
        sync_job__job_type=PointSyncJob.JOB_TYPE_INVENTORY,
    ).select_related("product"))
    counts = Counter(item.product_id for item in snapshots)
    for snapshot in snapshots:
        raw = snapshot.raw_payload
        row = raw.get("row") if isinstance(raw, dict) else None
        if (counts[snapshot.product_id] != 1 or not isinstance(row, list) or len(row) != 10
                or raw.get("headers") != _HEADERS
                or row[0] != snapshot.product.external_id
                or not (row[9] is False or isinstance(row[9], str) and row[9].casefold() == "false")
                or not isinstance(row[5], str) or not row[5].strip()
                or type(row[4]) is bool):
            continue
        try:
            quantity = Decimal(str(row[4]))
            if not quantity.is_finite() or quantity < 0 or quantity != snapshot.stock:
                continue
        except (InvalidOperation, TypeError, ValueError):
            continue
        cases[pairs[snapshot.product_id]]["operational_opening_evidence"] = {
            "quantity": quantity, "unit": row[5], "snapshot_id": snapshot.pk,
            "job_id": snapshot.sync_job_id, "log_id": 302317,
            "captured_at": snapshot.captured_at.astimezone(ZoneInfo("America/Mazatlan")),
        }
