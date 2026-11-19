"""Read-only Point references at the time of an immutable count submission."""
from decimal import Decimal, InvalidOperation
from datetime import timedelta

from django.utils import timezone
from django.utils.dateparse import parse_datetime

from inventario.conteos_conciliacion import _intervalo, cierre_dia
from inventario.conteos_point import misma_unidad
from pos_bridge.models import PointProductHistoryImport
from pos_bridge.services.audit_stock_history_service import (
    AuditStockHistoryService, AuditStockHistoryError, SOURCE_NAME, _month_bounds,
)
from pos_bridge.services.historical_inventory_capture import (
    HistoricalInventoryCaptureError, point_stock_history_instant,
)
from pos_bridge.services.monthly_product_balance_service import has_documentary_boundary
from reportes.models import ProductInventoryAuditCase


def compare_recorded_time(case, reading, comparison, record, reconciliation, end):
    if case.month != (end - timedelta(microseconds=1)).date().replace(day=1):
        return {"reason": "El historial y el conteo corresponden a meses distintos."}
    stamp = reading.get("counted_at")
    month_start, _ = _month_bounds(case.month)
    if not stamp or timezone.is_naive(stamp) or not month_start <= stamp < end <= timezone.now():
        return {"reason": "La hora registrada no permite comparar este cierre."}
    if (record is None or record.point_branch_id != case.branch_id
            or record.point_product_id != case.product_id):
        return {"reason": "Falta historial original de este producto y sucursal."}
    if comparison.get("closing") is None or not has_documentary_boundary(case.source_trace, "closing"):
        return {"reason": "Falta acreditar el saldo final Point."}
    if not reconciliation or reconciliation.coverage_status != "COMPLETE":
        return {"reason": "El historial no acredita todos los movimientos del mes."}
    if reconciliation.unknown_movement_ids:
        return {"reason": "Hay tipos de movimiento por comprobar en el historial."}
    try:
        batch = AuditStockHistoryService._original_batch(record)
        if not batch:
            return {"reason": "Falta el historial original íntegro."}
        proof = batch["evidence"]
        previous_proof = reconciliation.original_batch_evidence or {}
        if (not proof.get("response_fingerprint")
                or proof["response_fingerprint"] != previous_proof.get("response_fingerprint")):
            return {"reason": "La fuente cambió durante la comprobación; vuelve a consultar."}
        receipt = parse_datetime(proof["retrieved_at"])
        if receipt is None or timezone.is_naive(receipt) or not end <= receipt <= timezone.now():
            return {"reason": "El historial se consultó antes de completar el cierre."}
        rows = batch["unique_rows"]
        instants = [point_stock_history_instant(raw) for raw in rows]
        unit_rows = [raw for raw, instant in zip(rows, instants) if month_start <= instant < end]
        before_count = [(instant, raw) for raw, instant in zip(rows, instants) if instant < stamp]
        if before_count:
            unit_rows.append(max(before_count, key=lambda item: (item[0], item[1]["FK_Movimiento"]))[1])
        if not unit_rows or any(not isinstance(raw.get("Unidad"), str)
                or not misma_unidad(raw["Unidad"], reading.get("unit")) for raw in unit_rows):
            return {"reason": "La unidad del historial no acredita la unidad del conteo."}
        if proof["original_count"] >= proof["history_limit"] and (not instants or min(instants) >= stamp):
            return {"reason": "El historial disponible no cubre la hora del conteo."}
        gaps = [parse_datetime(gap["movement_at"]) for gap in batch["stock_chain_gaps"]]
        if any(gap is None or timezone.is_naive(gap) for gap in gaps):
            return {"reason": "La cadena del historial tiene fechas por comprobar."}
        if any(stamp <= gap <= end for gap in gaps):
            return {"reason": "Hay un salto de saldos entre el conteo y el cierre."}
        if any(abs((instant - stamp).total_seconds()) < 1 for instant in instants):
            return {"reason": "Conteo y movimiento coinciden en el mismo segundo; falta confirmar su orden."}
        if reconciliation.documentary_closing is None:
            reason = ("El historial tiene un salto previo al conteo; su cierre sigue por acreditar."
                      if any(gap < stamp for gap in gaps) else
                      "El historial no acredita su saldo al cierre.")
            return {"reason": reason}
        if reconciliation.documentary_closing != comparison["closing"]:
            return {"reason": "El saldo del historial contradice el cierre mostrado; revisar la fuente."}
        delta, movement_ids = _intervalo([raw for raw, instant in zip(rows, instants)
            if stamp <= instant < end], stamp, end, fisico=False)
        point = comparison["closing"] - delta
        quantity = Decimal(str(reading["quantity"]))
        if not quantity.is_finite() or quantity < 0:
            return {"reason": "La cantidad enviada requiere revisión."}
        return {"point": point, "difference": quantity - point, "stamp": stamp,
                "movement_ids": movement_ids, "variation": delta, "source_sha": proof["raw_sha256"]}
    except (AuditStockHistoryError, HistoricalInventoryCaptureError, InvalidOperation, KeyError, TypeError, ValueError):
        return {"reason": "No se pudo validar el historial original de esta comparación."}


def add_recorded_time_comparisons(cases, cutoff):
    selected = {}
    for case in cases:
        comparison = case.get("physical_comparison", {})
        if "delta" not in comparison:
            continue
        comparison["basis"] = f"Referencia del cierre declarado: {cutoff:%d/%m/%Y} (Mazatlán)"
        comparison["summary"] += " como referencia"
        comparison["label"] = "Diferencia con cierre declarado" if comparison["delta"] else "Coincide con cierre declarado"
        readings = [item for item in case.get("physical_evidence", []) if item["quantity"] is not None]
        reading = max(readings, key=lambda item: item["round"] or 0)
        if reading.get("counted_at"):
            comparison["provisional"] = "Referencia del cierre; no se verificó Point a la hora registrada."
            if comparison["delta"]:
                selected[case["id"]] = (case, reading)
    if not selected:
        return
    objects = ProductInventoryAuditCase.objects.filter(pk__in=selected).select_related("branch", "product")
    objects = list(objects)
    service = AuditStockHistoryService()
    histories = service.reconcile_many(objects, cutoff.replace(day=1), include_zero_difference=True)
    hashes = {service._file_hash(obj.branch, obj.product) for obj in objects}
    records = {record.file_hash: record for record in PointProductHistoryImport.objects.filter(
        file_hash__in=hashes, raw_metadata__source=SOURCE_NAME,
    ).select_related("point_branch", "point_product")}
    end = cierre_dia(cutoff)
    for obj in objects:
        case, reading = selected[obj.pk]
        comparison = case["physical_comparison"]
        result = compare_recorded_time(obj, reading, comparison,
            records.get(service._file_hash(obj.branch, obj.product)),
            histories.get((obj.branch_id, obj.product_id)), end)
        if "reason" in result:
            comparison["provisional"] = result["reason"]
            continue
        comparison["at_recorded_time"] = result
        comparison["label"] = "Diferencia al momento registrado" if result["difference"] else "Coincide al momento registrado"
        comparison["summary"] = "con diferencia al momento registrado" if result["difference"] else "coinciden al momento registrado"
        comparison["provisional"] = ""
