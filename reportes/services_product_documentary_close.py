"""Cierre documental por producto/sucursal; no bloquea el cierre mensual ni el conteo físico."""

from datetime import date, timedelta
from decimal import Decimal

from django.core.exceptions import PermissionDenied
from django.db import transaction

from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from pos_bridge.services.branch_inventory_traceability_service import BranchInventoryTraceabilityService
from pos_bridge.services.product_month_source_mutex import lock_product_month_sources
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
            reason = self._pending_reason(line, history)
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
            decisions[key] = {
                "eligible": True,
                "reason": "",
                "evidence": evidence,
                "fingerprint": _sha256(evidence),
            }
        return decisions

    @staticmethod
    def _pending_reason(line, history) -> str:
        boundary = line.source_trace.get("historical_boundary_evidence", {})
        if not (line.source_trace.get("opening") and line.source_trace.get("closing")):
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
            if issue.code not in _INDEPENDENT_CONVERSION_ISSUES:
                return "Hay una fuente de este producto que necesita aclaración."
            if not line.source_trace.get("conversion_in"):
                return "Falta comprobar la entrada por conversión de este producto."
        return ""

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
