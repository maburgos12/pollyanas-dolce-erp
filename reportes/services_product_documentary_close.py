"""Cierre documental por producto/sucursal; no bloquea el cierre mensual ni el conteo físico."""

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import F

from pos_bridge.models import PointTransferLine, PointWasteLine
from pos_bridge.services.audit_stock_history_service import (
    AuditStockHistoryService, AuditStockHistoryError, HistoricalInventoryCaptureError,
)
from pos_bridge.services.branch_inventory_traceability_service import BranchInventoryTraceabilityService
from pos_bridge.services.product_month_source_mutex import lock_product_month_sources
from pos_bridge.services.monthly_product_balance_service import has_documentary_boundary
from reportes.models import ProductInventoryDocumentaryEvent
from reportes.services_inventory_traceability import InventoryAuditMaterializer, _sha256


_MOVEMENT_FIELDS = (
    "production", "sales", "waste", "transfer_in", "transfer_out",
    "conversion_in", "conversion_out", "identified_adjustment",
)
_INDEPENDENT_CONVERSION_ISSUES = {"MISSING_CONVERSION_ORIGIN", "NON_DERIVED_CONVERSION"}


class ProductDocumentaryCloseService:
    def evaluate(self, month: date) -> dict[tuple[int, int], dict]:
        month = month.replace(day=1)
        trace = BranchInventoryTraceabilityService().build(month, allow_partial=True)
        blocked_branches = set()
        aliases = None
        for issue in trace.global_issues:
            if issue.code == "SOURCE_INCOMPLETE" and issue.message.startswith(
                "El cierre Point verificado tiene cobertura incompleta para "
            ):
                continue
            local_production = issue.code in {"AMBIGUOUS_PRODUCT", "UNRESOLVED_PRODUCT"} and issue.message.endswith(
                "de production a un único producto Point."
            )
            local_conversion = issue.code == "MISSING_CONVERSION_DESTINATION"
            if issue.branch_id is None or not (local_production or local_conversion):
                return {}
            if aliases is None:
                aliases, _ = BranchInventoryTraceabilityService.canonical_branch_identity()
            blocked_branches.add(aliases.get(issue.branch_id, issue.branch_id))
        safe_lines = tuple(line for line in trace.lines if line.branch.id not in blocked_branches)
        histories = AuditStockHistoryService().reconcile_many(
            safe_lines, month, include_zero_difference=True
        )
        returns = self._documentary_returns(safe_lines, month)
        waste_proofs = self._documentary_waste(safe_lines, histories, month)
        prepare = InventoryAuditMaterializer()._prepare_line
        decisions = {}
        for line in trace.lines:
            key = (line.branch.id, line.product.id)
            if key[0] in blocked_branches:
                decisions[key] = {
                    "eligible": False,
                    "reason": "Hay movimientos de esta sucursal sin producto identificado.",
                }
                continue
            history = histories.get(key)
            reason = self._pending_reason(line, history, returns, waste_proofs)
            if reason:
                decisions[key] = {"eligible": False, "reason": reason}
                continue
            prepared = prepare(line, point_history=history)
            evidence = {
                "month": month.isoformat(),
                "branch_id": key[0],
                "product_id": key[1],
                "quantities": prepared["quantities"],
                "calculation_fingerprint": prepared["fingerprint"],
                "source_trace": prepared["source_trace"],
                "point_history": history.as_dict(
                    opening=Decimal(line.opening), point_closing=Decimal(line.point_closing)
                ),
            }
            return_evidence = {
                str(source_id): returns[source_id]
                for issue in line.issues if issue.code == "TRANSFER_QUANTITY_MISMATCH"
                for source_id in issue.source_ids
            }
            if return_evidence:
                evidence["administrative_returns"] = return_evidence
            waste_evidence = {str(source_id): waste_proofs[key + (source_id,)]
                              for source_id in getattr(line, "source_trace", {}).get("waste", ())
                              if key + (source_id,) in waste_proofs}
            if waste_evidence:
                evidence["corroborated_waste"] = waste_evidence
            decisions[key] = {
                "eligible": True,
                "reason": "",
                "evidence": evidence,
                "fingerprint": _sha256(evidence),
            }
        return decisions

    @staticmethod
    def _pending_reason(line, history, returns=None, waste_proofs=None) -> str:
        batch = getattr(history, "original_batch_evidence", None) or {}
        month_ids = set(getattr(history, "movement_ids", ()))
        if any(gap["movement_id"] in month_ids for gap in batch.get("stock_chain_gaps", ())):
            return "Point presenta un salto de existencias sin movimiento intermedio. Pendiente de aclaración con Point."
        boundary = line.source_trace.get("historical_boundary_evidence", {})
        if not all(has_documentary_boundary(line.source_trace, role) for role in ("opening", "closing")):
            return "Falta comprobar el saldo inicial o el saldo final de Point."
        if Decimal(line.difference) != 0:
            return "El saldo final no coincide con las entradas y salidas comprobadas."
        if history is None or history.coverage_status != "COMPLETE":
            return "Falta comprobar la secuencia completa de movimientos del mes."
        if history.unknown_movement_ids:
            return "Hay movimientos cuyo efecto todavía no está identificado."
        if history.unexplained_remainder(Decimal(line.opening), Decimal(line.point_closing)) != 0:
            return "La secuencia de Point no llega al saldo final comprobado."
        for field in _MOVEMENT_FIELDS:
            if Decimal(getattr(line, field)) != Decimal(getattr(history, field)):
                return "Un movimiento de Point no coincide con el resumen del mes."
        if history.documentary_opening is None or history.documentary_closing is None:
            empty_original = (
                not history.movement_ids
                and history.original_batch_evidence
                and Decimal(line.opening) == Decimal(line.point_closing)
                and boundary.get("opening") and boundary.get("closing")
            )
            if not empty_original:
                return "Falta comprobar la continuidad de las existencias de Point."
        else:
            if Decimal(history.documentary_opening) != Decimal(line.opening):
                return "El saldo inicial del historial no coincide con el documento de apertura."
            if Decimal(history.documentary_closing) != Decimal(line.point_closing):
                return "El saldo final del historial no coincide con el documento de cierre."
        for issue in line.issues:
            if (issue.code == "PRODUCT_RESOLVED_BY_NAME" and " de waste se asignó " in getattr(issue, "message", "")
                    and issue.source_ids and all(
                        (line.branch.id, line.product.id, source_id) in (waste_proofs or {})
                        for source_id in issue.source_ids)):
                continue
            if issue.code == "TRANSFER_QUANTITY_MISMATCH":
                proofs = [(returns or {}).get(source_id) for source_id in issue.source_ids]
                if proofs and all(
                    proof
                    and proof["product_external_id"] == str(line.product.external_id)
                    and line.branch.id in (proof["origin_branch_id"], proof["destination_branch_id"])
                    and source_id in line.source_trace.get("transfers", ())
                    and (line.branch.id != proof["origin_branch_id"]
                         or source_id in line.source_trace.get("transfer_in", ()))
                    for source_id, proof in zip(issue.source_ids, proofs)
                ):
                    continue
            if issue.code not in _INDEPENDENT_CONVERSION_ISSUES:
                return "Hay una fuente de este producto que necesita aclaración."
            if not line.source_trace.get("conversion_in"):
                return "Falta comprobar la entrada por conversión de este producto."
        return ""

    @staticmethod
    def _documentary_waste(lines, histories, month):
        candidates = {(line.branch.id, line.product.id): [source_id
            for issue in line.issues if issue.code == "PRODUCT_RESOLVED_BY_NAME"
            and " de waste se asignó " in getattr(issue, "message", "") for source_id in issue.source_ids
            if source_id in line.source_trace.get("waste", ())] for line in lines}
        ids = {source_id for sources in candidates.values() for source_id in sources}
        if not ids:
            return {}
        lower, upper = BranchInventoryTraceabilityService._month_datetime_bounds(month)
        aliases, _ = BranchInventoryTraceabilityService.canonical_branch_identity()
        waste_rows = PointWasteLine.objects.filter(pk__in=ids, insumo__isnull=True,
            movement_at__gte=lower, movement_at__lt=upper).in_bulk()
        stock = AuditStockHistoryService()
        proofs = {}
        for line in lines:
            key = (line.branch.id, line.product.id)
            history = histories.get(key)
            if not candidates[key] or not history or history.coverage_status != "COMPLETE":
                continue
            record = stock._existing_import(line.branch, line.product)
            if record is None:
                continue
            try:
                batch = stock._original_batch(record)
            except (AuditStockHistoryError, HistoricalInventoryCaptureError, InvalidOperation, TypeError, ValueError):
                continue
            if batch is None or batch["evidence"] != history.original_batch_evidence:
                continue
            originals = {raw["FK_Movimiento"]: raw for raw in batch["unique_rows"]}
            for source_id in candidates[key]:
                row = waste_rows.get(source_id)
                if (row is None or aliases.get(row.branch_id, row.branch_id) != line.branch.id
                        or row.item_name != line.product.name or not row.source_hash):
                    continue
                payload = row.raw_payload if isinstance(row.raw_payload, dict) else {}
                header, details = payload.get("movement"), payload.get("details")
                if not isinstance(header, dict) or not isinstance(details, list):
                    continue
                movement_id = header.get("PK_Movimiento")
                if (type(movement_id) is not int or movement_id <= 0
                        or str(movement_id) != row.movement_external_id
                        or movement_id not in history.movement_ids_by_category.get("waste", ())):
                    continue
                original = originals.get(movement_id)
                matching = [detail for detail in details if isinstance(detail, dict)
                            and detail.get("Articulo") == row.item_name]
                if original is None or len(matching) != 1:
                    continue
                detail = matching[0]
                try:
                    if (type(detail.get("Cantidad")) is bool
                            or Decimal(str(detail.get("Cantidad"))) != row.quantity
                            or Decimal(str(original["Cantidad"])) != row.quantity
                            or str(detail.get("Unidad", "")).strip() != row.unit):
                        continue
                except (InvalidOperation, TypeError, ValueError):
                    continue
                proofs[key + (source_id,)] = {"movement_id": movement_id,
                    "branch_id": line.branch.id, "product_id": line.product.id,
                    "quantity": str(row.quantity), "waste_source_hash": row.source_hash,
                    "waste_raw_sha256": _sha256(payload), "stock_original": batch["evidence"]}
        return proofs

    @staticmethod
    def _documentary_returns(lines, month):
        ids = {source_id for line in lines for issue in line.issues
               if issue.code == "TRANSFER_QUANTITY_MISMATCH" for source_id in issue.source_ids}
        if not ids:
            return {}
        lower, upper = BranchInventoryTraceabilityService._month_datetime_bounds(month)
        aliases, _ = BranchInventoryTraceabilityService.canonical_branch_identity()
        proofs = {}
        for row in PointTransferLine.objects.filter(
            pk__in=ids, is_received=True, is_cancelled=False, is_current_snapshot=True,
            is_insumo=False, received_at__gte=lower, received_at__lt=upper,
            received_quantity__gte=0, sent_quantity__gt=F("received_quantity"),
        ):
            detail = row.raw_payload.get("detail", {}) if isinstance(row.raw_payload, dict) else {}
            fk = detail.get("FK_articulo") if isinstance(detail, dict) else None
            if isinstance(fk, bool) or not str(fk).isdecimal() or int(fk) <= 0:
                continue
            if detail.get("isInsumo") is not False:
                continue
            proofs[row.id] = {
                "product_external_id": str(int(fk)),
                "origin_branch_id": aliases.get(row.origin_branch_id, row.origin_branch_id),
                "destination_branch_id": aliases.get(row.destination_branch_id, row.destination_branch_id),
                "sent": str(row.sent_quantity), "received": str(row.received_quantity),
                "returned": str(row.sent_quantity - row.received_quantity),
                "received_at": row.received_at.isoformat(), "source_hash": row.source_hash,
                "physical_custody_verified": False,
            }
        return proofs

    def close_eligible(self, month: date, *, actor) -> dict[str, int]:
        if not actor or not actor.is_active or not actor.has_perm("reportes.approve_product_inventory_audit"):
            raise PermissionDenied("Se requiere autorización de auditoría de inventario.")
        month = month.replace(day=1)
        counts = {"closed": 0, "reopened": 0, "unchanged": 0, "pending": 0}
        with transaction.atomic():
            lock_product_month_sources([month, month - timedelta(days=1)])
            decisions = self.evaluate(month)
            latest = {}
            for event in ProductInventoryDocumentaryEvent.objects.filter(month=month).order_by("-id"):
                latest.setdefault((event.branch_id, event.product_id), event)
            for key in sorted(decisions.keys() | latest.keys()):
                decision = decisions.get(key, {"eligible": False, "reason": "Falta evidencia vigente del producto y sucursal."})
                prior = latest.get(key)
                if decision["eligible"]:
                    if prior and prior.action == ProductInventoryDocumentaryEvent.Action.CLOSE and prior.source_fingerprint == decision["fingerprint"]:
                        counts["unchanged"] += 1
                        continue
                    ProductInventoryDocumentaryEvent.objects.create(
                        month=month, branch_id=key[0], product_id=key[1],
                        action=ProductInventoryDocumentaryEvent.Action.CLOSE,
                        source_fingerprint=decision["fingerprint"],
                        evidence=decision["evidence"], actor=actor,
                    )
                    counts["closed"] += 1
                elif prior and prior.action == ProductInventoryDocumentaryEvent.Action.CLOSE:
                    ProductInventoryDocumentaryEvent.objects.create(
                        month=month, branch_id=key[0], product_id=key[1],
                        action=ProductInventoryDocumentaryEvent.Action.REOPEN,
                        source_fingerprint=prior.source_fingerprint,
                        evidence=prior.evidence, reason=decision["reason"], actor=actor,
                    )
                    counts["reopened"] += 1
                else:
                    if not prior or prior.reason != decision["reason"]:
                        ProductInventoryDocumentaryEvent.objects.create(
                            month=month, branch_id=key[0], product_id=key[1],
                            action=ProductInventoryDocumentaryEvent.Action.PENDING,
                            source_fingerprint="",
                            reason=decision["reason"], actor=actor,
                        )
                    counts["pending"] += 1
        return counts
