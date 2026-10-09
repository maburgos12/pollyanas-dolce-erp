from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase

from pos_bridge.models import PointBranch, PointProduct
from pos_bridge.services.audit_stock_history_service import PointHistoryReconciliation
from reportes.models import ProductInventoryDocumentaryEvent
from reportes.services_product_documentary_close import ProductDocumentaryCloseService


class ProductDocumentaryCloseTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_superuser(
            username="dg-documental", email="dg@example.test", password="local-test-only"
        )
        self.branch = PointBranch.objects.create(external_id="TEST", name="Prueba")
        self.product = PointProduct.objects.create(external_id="P1", sku="P1", name="Producto")
        self.key = (self.branch.id, self.product.id)
        self.service = ProductDocumentaryCloseService()

    def test_close_is_idempotent_and_reopens_when_proof_is_lost(self):
        proof = {"eligible": True, "reason": "", "fingerprint": "a" * 64, "evidence": {"opening": "2"}}
        with patch.object(self.service, "evaluate", return_value={self.key: proof}):
            first = self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
            second = self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
        self.assertEqual(first["closed"], 1)
        self.assertEqual(second["unchanged"], 1)
        self.assertEqual(ProductInventoryDocumentaryEvent.objects.count(), 1)

        pending = {"eligible": False, "reason": "Falta comprobar el saldo final de Point."}
        with patch.object(self.service, "evaluate", return_value={self.key: pending}):
            third = self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
            fourth = self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
        self.assertEqual(third["reopened"], 1)
        self.assertEqual(fourth["reopened"], 0)
        self.assertEqual(ProductInventoryDocumentaryEvent.objects.count(), 2)
        self.assertEqual(ProductInventoryDocumentaryEvent.objects.first().action, "REOPEN")
        with self.assertRaises(ValidationError):
            ProductInventoryDocumentaryEvent.objects.update(reason="alterado")

    def test_pending_reason_is_recorded_once_and_updated_only_when_it_changes(self):
        pending = {"eligible": False, "reason": "Falta comprobar el saldo final de Point."}
        with patch.object(self.service, "evaluate", return_value={self.key: pending}):
            self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
            self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
        self.assertEqual(ProductInventoryDocumentaryEvent.objects.count(), 1)
        self.assertEqual(ProductInventoryDocumentaryEvent.objects.first().action, "PENDING")
        pending["reason"] = "Falta comprobar la secuencia completa de movimientos del mes."
        with patch.object(self.service, "evaluate", return_value={self.key: pending}):
            self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
        self.assertEqual(ProductInventoryDocumentaryEvent.objects.count(), 2)
        self.assertEqual(ProductInventoryDocumentaryEvent.objects.first().reason, pending["reason"])

    def test_original_month_gap_has_plain_reason_but_prior_month_gap_does_not(self):
        line = SimpleNamespace(source_trace={})
        history = PointHistoryReconciliation(
            coverage_status="COMPLETE",
            movement_ids=(1668705,),
            original_batch_evidence={"stock_chain_gaps": [{"movement_id": 1668705}]},
        )
        self.assertEqual(
            self.service._pending_reason(line, history),
            "Point presenta un salto de existencias sin movimiento intermedio. Pendiente de aclaración con Point.",
        )
        history.original_batch_evidence["stock_chain_gaps"][0]["movement_id"] = 1650279
        self.assertEqual(
            self.service._pending_reason(line, history),
            "Falta comprobar el saldo inicial o el saldo final de Point.",
        )

    def test_conversion_without_common_folio_can_be_proven_independently(self):
        zero = Decimal("0")
        values = {field: zero for field in (
            "production", "sales", "waste", "transfer_in", "transfer_out",
            "conversion_in", "conversion_out", "identified_adjustment",
        )}
        values["conversion_in"] = Decimal("1")
        line = SimpleNamespace(
            opening=zero, point_closing=Decimal("1"), difference=zero,
            source_trace={
                "opening": (1,), "closing": (2,), "conversion_in": (3,),
                "historical_boundary_evidence": {"opening": {"id": 1}, "closing": {"id": 2}},
            },
            issues=(SimpleNamespace(code="MISSING_CONVERSION_ORIGIN"),),
            **values,
        )
        history = SimpleNamespace(
            coverage_status="COMPLETE", unknown_movement_ids=(),
            documentary_opening=zero, documentary_closing=Decimal("1"),
            unexplained_remainder=lambda opening, closing: zero,
            **values,
        )
        self.assertEqual(self.service._pending_reason(line, history), "")
        line.source_trace.pop("historical_boundary_evidence")
        self.assertEqual(self.service._pending_reason(line, history), "")
        history.conversion_in = zero
        self.assertIn("no coincide", self.service._pending_reason(line, history))

    def test_evaluate_uses_independent_point_movement_without_linked_folio(self):
        zero = Decimal("0")
        values = {field: zero for field in (
            "production", "sales", "waste", "transfer_in", "transfer_out",
            "conversion_in", "conversion_out", "identified_adjustment",
        )}
        values["conversion_in"] = Decimal("1")
        line = SimpleNamespace(
            month=date(2026, 9, 1), branch=self.branch, product=self.product,
            opening=zero, point_closing=Decimal("1"), expected_closing=Decimal("1"), difference=zero,
            source_trace={
                "opening": (1,), "closing": (2,), "conversion_in": (3,),
                "historical_boundary_evidence": {"opening": {"id": 1}, "closing": {"id": 2}},
            },
            issues=(SimpleNamespace(code="MISSING_CONVERSION_ORIGIN", message="Sin origen", branch_id=self.branch.id, product_id=self.product.id, source_ids=(3,)),),
            **values,
        )
        history = PointHistoryReconciliation(
            coverage_status="COMPLETE", conversion_in=Decimal("1"),
            documentary_opening=zero, documentary_closing=Decimal("1"),
        )
        trace = SimpleNamespace(lines=(line,), global_issues=())
        with patch("reportes.services_product_documentary_close.BranchInventoryTraceabilityService.build", return_value=trace), patch(
            "reportes.services_product_documentary_close.AuditStockHistoryService.reconcile_many",
            return_value={self.key: history},
        ):
            decision = self.service.evaluate(date(2026, 9, 1))[self.key]
        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["evidence"]["point_history"]["conversion_in"], "1")

    def test_global_unproven_source_prevents_any_close(self):
        trace = SimpleNamespace(
            lines=(),
            global_issues=(SimpleNamespace(code="SOURCE_INCOMPLETE", message="Falta fuente de merma.", branch_id=None),),
        )
        with patch("reportes.services_product_documentary_close.BranchInventoryTraceabilityService.build", return_value=trace):
            self.assertEqual(self.service.evaluate(date(2026, 9, 1)), {})

    def test_unidentified_product_blocks_only_its_branch(self):
        other = PointBranch.objects.create(external_id="OTHER", name="Otra sucursal")
        lines = tuple(SimpleNamespace(branch=branch, product=self.product, opening=0, point_closing=0) for branch in (self.branch, other))
        trace = SimpleNamespace(
            lines=lines,
            global_issues=(SimpleNamespace(
                code="AMBIGUOUS_PRODUCT", message="No fue posible asignar la fila 11715 de production a un único producto Point.",
                branch_id=self.branch.id, product_id=None,
            ),),
        )
        history = SimpleNamespace(as_dict=lambda **kwargs: {})
        with patch("reportes.services_product_documentary_close.BranchInventoryTraceabilityService.build", return_value=trace), patch(
            "reportes.services_product_documentary_close.BranchInventoryTraceabilityService.canonical_branch_identity",
            return_value=({self.branch.id: self.branch.id, other.id: other.id}, {}),
        ), patch(
            "reportes.services_product_documentary_close.AuditStockHistoryService.reconcile_many",
            return_value={(other.id, self.product.id): history},
        ), patch.object(self.service, "_pending_reason", return_value=""), patch(
            "reportes.services_product_documentary_close.InventoryAuditMaterializer._prepare_line",
            return_value={"quantities": {}, "fingerprint": "f", "source_trace": {}},
        ):
            decisions = self.service.evaluate(date(2026, 9, 1))
        self.assertFalse(decisions[self.key]["eligible"])
        self.assertTrue(decisions[(other.id, self.product.id)]["eligible"])
