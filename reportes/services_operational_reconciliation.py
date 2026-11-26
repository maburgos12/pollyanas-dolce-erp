"""Read an observed operational interval without changing monthly projections."""
from collections import defaultdict
from datetime import date
from decimal import Decimal

from django.db import connection, transaction
from django.db.models import Q

from pos_bridge.models import (
    PointConversionLine, PointHistoricalInventoryClosingLine, PointInventorySnapshot,
    PointProduct, PointProductHistoryImport, PointProductHistoryRow,
    PointProductionLine, PointTransferLine, PointWasteLine, PointOpenTransferSnapshotMember,
)
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from pos_bridge.services.branch_inventory_traceability_service import BranchInventoryTraceabilityService
from pos_bridge.services.monthly_product_balance_service import documentary_historical_boundary
from pos_bridge.services.historical_inventory_capture import point_stock_history_instant, HistoricalInventoryCaptureError
from pos_bridge.services.product_month_source_mutex import PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE
from ventas.services.sales_canonical_source import official_point_sales_rows_for_range

_MONTH = date(2026, 10, 1)
_END = date(2026, 10, 31)
_FAMILIES = ("sales", "production", "waste", "transfer_in", "transfer_out",
             "open_transfer_snapshot_in", "open_transfer_snapshot_out", "conversion_in", "conversion_out")
_FLOWS = ("production", "sales", "waste", "transfer_in", "transfer_out", "conversion_in", "conversion_out")
ZERO = Decimal("0")


def verify_operational_balance(snapshot, flows, closing, history, *, blocked=False):
    """The same Stock equation and coverage used by the audit, for this interval."""
    if (blocked or closing is None or history is None or history.coverage_status != "COMPLETE"
            or history.unknown_movement_ids):
        return None
    # A complete Stock response cannot replace a contradictory commercial sale.
    if any(Decimal(flows[field]) != getattr(history, field) for field in _FLOWS):
        return None
    calculated = history.expected_closing(Decimal(snapshot.stock))
    difference = Decimal(closing) - calculated
    if difference:
        return None
    return {
        "verified": True, "opening": snapshot.stock, "closing": closing,
        "calculated": calculated, "difference": difference,
        "flows": {**flows, "identified_adjustment": history.identified_adjustment},
        "no_activity": (not snapshot.stock and not closing and not history.movement_ids
                        and not history.identified_adjustment and not any(flows.values())),
        "through": _END,
    }


def transfer_temporal_blockers(rows, members, *, service, indexes, locations, product_ids, lower, capture):
    """Mutable lines and immutable members keep distinct primary-key domains."""
    frozen_references = {(row.transfer_external_id, row.detail_external_id) for row in members}
    records = [row for row in rows
               if (row.transfer_external_id, row.detail_external_id) not in frozen_references]
    blocked = set()
    for row in [*records, *members]:
        if row.is_insumo or row.is_cancelled:
            continue
        product, issue = service._resolve_transfer_product(row, indexes)
        if product is None:
            blocked.update(product_ids)
            continue
        if product not in product_ids:
            continue
        if issue:
            blocked.add(product)
        for branch, quantity, instant in (
            (row.origin_branch_id, row.sent_quantity, row.sent_at),
            (row.destination_branch_id, row.received_quantity, row.received_at),
        ):
            if branch in locations and quantity and (instant is None or lower <= instant < capture):
                blocked.add(product)
    return blocked


@transaction.atomic
def read_operational_reconciliation(cases, snapshots):
    """One fresh batch for the visible pairs; no cache or materialization."""
    if not cases:
        return {}
    with connection.cursor() as cursor:
        # Same namespace and YYYYMM key as the existing monthly mutex, but a
        # reader must not queue behind a long-running writer.
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s, %s)",
            [PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE, _MONTH.year * 100 + _MONTH.month])
        if not cursor.fetchone()[0]:
            return None
    # The display capture was read before this transaction. Recheck its exact
    # content under the existing monthly mutex before certifying an interval.
    fresh = list(PointInventorySnapshot.objects.filter(sync_job_id=81086, branch_id=28,
        product_id__in=snapshots, captured_at=min(s.captured_at for s in snapshots.values()),
        sync_job__status="SUCCESS", sync_job__job_type="inventory")
        .select_related("shared_payload")
        .only("product_id", "captured_at", "stock", "raw_payload", "shared_payload__payload"))
    current = defaultdict(list)
    for snapshot in fresh:
        current[snapshot.product_id].append(snapshot)
    valid = {product for product, original in snapshots.items()
             if len(current[product]) == 1 and current[product][0].pk == original.pk
             and current[product][0].stock == original.stock
             and current[product][0].effective_raw_payload == original.effective_raw_payload}
    cases = [case for case in cases if case.product_id in valid]
    if not cases:
        return {}
    snapshots = {product: snapshot for product, snapshot in snapshots.items() if product in valid}
    svc = BranchInventoryTraceabilityService()
    aliases, branches = svc.canonical_branch_identity()
    locations = {pk for pk, canonical in aliases.items() if canonical == 28}
    products = list(PointProduct.objects.only("id", "external_id", "sku", "name", "normalized_name"))
    indexes = svc._build_product_indexes(products)
    source = svc._load_direct_movements(month_start=_MONTH, closing_date=_END,
        product_indexes=indexes, branch_ids=locations)
    if source[-1]:
        # Global source authority is unavailable, not an individual product
        # discrepancy. Retain observed captures through the existing None path.
        return None
    maps = {name: svc._canonicalize_balance_map(source[index], aliases)
            for index, name in enumerate(_FAMILIES)}
    issues = source[-2]
    histories = AuditStockHistoryService().reconcile_many(cases, _MONTH, include_zero_difference=True)
    closing = svc._select_closing(_END)
    if closing is None:
        return {}
    lines = list(PointHistoricalInventoryClosingLine.objects.filter(closing=closing,
        branch_id__in=locations, product_id__in=[case.product_id for case in cases])
        .select_related("branch", "product"))
    cache = {"source_transaction_scope": connection.run_on_commit, _MONTH: histories}
    quantities, _evidence, _unproven = documentary_historical_boundary(
        closing, lines, month=_MONTH, boundary="closing", cache=cache)
    closings = defaultdict(lambda: ZERO)
    for line in lines:
        if line.pk in quantities:
            closings[aliases[line.branch_id], line.product_id] += quantities[line.pk]
    blocked = set()
    product_ids = {case.product_id for case in cases}
    # Daily commercial sales and production cannot be placed before/after a
    # morning capture. Resolve identity with the existing ledger's exact rules.
    for row in official_point_sales_rows_for_range(
        start_date=_MONTH, end_date=date(2026, 10, 3),
    ).filter(branch_id__in=locations).exclude(quantity=0):
        product, _issue = svc._resolve_sales_product(row, indexes)
        if product in product_ids:
            blocked.add(product)
    for row in PointProductionLine.objects.filter(branch_id__in=locations,
        production_date__gte=_MONTH, production_date__lte=date(2026, 10, 3),
        is_insumo=False).exclude(produced_quantity=0):
        product, _issue = svc._resolve_production_product(row, indexes)
        if product in product_ids:
            blocked.add(product)
    records = dict(PointProductHistoryImport.objects.filter(point_branch_id=28,
        point_product_id__in=product_ids, raw_metadata__source="POINT_STOCK_HISTORY_API")
        .values_list("pk", "point_product_id"))
    # Use exact raw Stock instants and the reconciler's membership. The stored
    # movement_at can be a legacy wall clock and must not define this frontier.
    movement_ids = {pk for history in histories.values() for pk in history.movement_ids}
    capture = min(item.captured_at for item in snapshots.values())
    for row in PointProductHistoryRow.objects.filter(import_record_id__in=records,
        row_number__in=movement_ids).only("import_record_id", "row_number", "raw_payload", "quantity", "cancelled"):
        product = records[row.import_record_id]
        history = histories.get((28, product))
        if (history is None or history.coverage_status != "COMPLETE"
                or row.row_number not in history.movement_ids or row.cancelled or not row.quantity):
            continue
        try:
            instant = point_stock_history_instant(row.raw_payload)
            if svc._month_datetime_bounds(_MONTH)[0] <= instant < capture:
                blocked.add(product)
        except HistoricalInventoryCaptureError:
            blocked.add(product)
    # Inspect current family dates in batch too: ERP and Stock timestamps can
    # disagree even when a monthly sum is unchanged.
    ids = {name: {pk for case in cases for pk in maps[name].get((28, case.product_id), (ZERO, ()))[1]}
           for name in ("waste", "conversion_in", "conversion_out")}
    before_ids = {}
    for name, model, field, quantity_field in (
        ("waste", PointWasteLine, "movement_at", "quantity"),
        ("conversion_in", PointConversionLine, "movement_at", "quantity"),
        ("conversion_out", PointConversionLine, "movement_at", "quantity"),
    ):
        before_ids[name] = {row[0] for row in model.objects.filter(pk__in=ids[name])
            .values_list("pk", field, quantity_field)
            if row[2] and (row[1] is None or row[1] < capture)}
    # The balance maps merge IDs from two models. Never look those IDs up in
    # PointTransferLine. Read live rows by location/date, and frozen evidence
    # explicitly in its own model; override live rows by stable business refs.
    lower, upper = svc._month_datetime_bounds(_MONTH)
    real_rows = list(PointTransferLine.objects.filter(
        Q(origin_branch_id__in=locations) | Q(destination_branch_id__in=locations),
        Q(sent_at__gte=lower, sent_at__lt=upper)
        | Q(received_at__gte=lower, received_at__lt=upper)
        | Q(registered_at__gte=lower, registered_at__lt=upper),
        is_current_snapshot=True, is_insumo=False, is_cancelled=False))
    member_ids = {pk for name in ("open_transfer_snapshot_in", "open_transfer_snapshot_out")
                  for _quantity, source_ids in maps[name].values() for pk in source_ids}
    members = list(PointOpenTransferSnapshotMember.objects.filter(pk__in=member_ids))
    if len(members) != len(member_ids):
        blocked.update(product_ids)
    blocked.update(transfer_temporal_blockers(real_rows, members, service=svc, indexes=indexes,
        locations=locations, product_ids=product_ids, lower=lower, capture=capture))
    results = {}
    for case in cases:
        key = (28, case.product_id)
        snapshot = snapshots[case.product_id]
        candidate_issues = [issue for issue in issues if issue.product_id in (None, case.product_id)
                            and (issue.branch_id in locations or issue.branch_id is None)]
        temporal = any(set(maps[name].get(key, (ZERO, ()))[1]) & bad for name, bad in before_ids.items())
        flows = {name: maps[name].get(key, (ZERO, ()))[0] for name in _FLOWS}
        result = verify_operational_balance(snapshot, flows, closings.get(key), histories.get(key),
            blocked=case.product_id in blocked or bool(candidate_issues) or temporal)
        if result is not None:
            results[case.product_id] = result
    return results
