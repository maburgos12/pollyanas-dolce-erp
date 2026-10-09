import hashlib
import json
from datetime import date, datetime
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

from pos_bridge.models import PointBranch, PointProduct, PointTransferLine, PointWasteLine
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService, PointHistoryReconciliation
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

    def test_original_waste_movement_proves_identity_without_master_alias(self):
        month = date(2026, 9, 1)
        raw = {'FK_Movimiento': 101, 'Movimiento': 'MERMA', 'FK_Tipo_Movimiento': 5,
               'Fecha': '2026-09-15T18:00:00', 'Cantidad': 2, 'Existencia_anterior': 2,
               'Existencia_nueva': 0, 'Cancelado': False, 'isCargo': True}
        rows = [raw]
        code = '# bounded original acquisition\n'
        evidence = {'source': 'POINT_STOCK_HISTORY_API', 'domain': 'PRODUCT', 'response_complete': True,
            'branch_id': self.branch.pk, 'product_id': self.product.pk,
            'request': {'path': '/Stock/GetHistorial', 'params': {'tipo': 'false',
                'almacen': self.branch.external_id, 'pkproducto': self.product.external_id,
                'movimientos': '5', 'tipoMovimiento': ''}},
            'retrieved_at': '2026-10-04T18:00:00+00:00', 'history_limit': 5, 'fetched_rows': 1,
            'raw_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest(),
            'original_locator': {'source_file': '/evidence/waste.jsonl', 'source_line': 1},
            'request_provenance': {'kind': 'DERIVED_FROM_ACQUISITION_SCRIPT',
                'source_file': '/evidence/reader.py', 'source_code': code,
                'source_sha256': hashlib.sha256(code.encode()).hexdigest(),
                'client_contract': 'PointHttpSessionClient.get_stock_history'}}
        stock = AuditStockHistoryService()
        history = stock.ingest_original_response(self.branch, self.product, month, rows, evidence=evidence)
        waste = PointWasteLine.objects.create(branch=self.branch, movement_external_id='101',
            source_hash='d' * 64, movement_at=timezone.make_aware(datetime(2026, 9, 15, 11)),
            item_name=self.product.name, quantity=2, unit='PZA', raw_payload={
                'movement': {'PK_Movimiento': 101},
                'details': [{'Articulo': self.product.name, 'Cantidad': 2, 'Unidad': 'PZA'}]})
        zero = Decimal('0')
        values = {field: zero for field in ('production', 'sales', 'waste', 'transfer_in',
            'transfer_out', 'conversion_in', 'conversion_out', 'identified_adjustment')}
        values['waste'] = Decimal('2')
        issue = SimpleNamespace(code='PRODUCT_RESOLVED_BY_NAME',
            message=f'La fila {waste.pk} de waste se asignó por una coincidencia secundaria que requiere auditoría.',
            branch_id=self.branch.id, product_id=self.product.id, source_ids=(waste.id,))
        line = SimpleNamespace(month=month, branch=self.branch, product=self.product,
            opening=Decimal('2'), point_closing=zero, expected_closing=zero, difference=zero,
            source_trace={'opening': (1,), 'closing': (2,), 'waste': (waste.id,)}, issues=(issue,), **values)
        trace = SimpleNamespace(lines=(line,), global_issues=())
        with patch('reportes.services_product_documentary_close.BranchInventoryTraceabilityService.build', return_value=trace), patch(
            'reportes.services_product_documentary_close.AuditStockHistoryService.reconcile_many', return_value={self.key: history}):
            decision = self.service.evaluate(month)[self.key]
            self.assertTrue(decision['eligible'])
            proof = decision['evidence']['corroborated_waste'][str(waste.id)]
            self.assertEqual(proof['movement_id'], 101)
            self.assertEqual(proof['product_id'], self.product.id)
            self.assertEqual(line.issues, (issue,))
            self.assertEqual(self.service.close_eligible(month, actor=self.actor)['closed'], 1)
            self.assertEqual(self.service.close_eligible(month, actor=self.actor)['unchanged'], 1)
            self.assertEqual(ProductInventoryDocumentaryEvent.objects.count(), 1)
            original_payload = waste.raw_payload
            for change in (
                {'quantity': 1}, {'item_name': 'Otro producto'}, {'unit': 'KG'},
                {'source_hash': ''}, {'insumo_id': None, 'movement_external_id': '102'},
                {'movement_at': waste.movement_at.replace(month=10)},
                {'raw_payload': {'movement': {'PK_Movimiento': True}, 'details': original_payload['details']}},
                {'raw_payload': {'movement': {'PK_Movimiento': 102}, 'details': original_payload['details']}},
                {'raw_payload': {'movement': original_payload['movement'], 'details': original_payload['details'] * 2}},
                {'raw_payload': {'movement': original_payload['movement'], 'details': [
                    {'Articulo': self.product.name, 'Cantidad': True, 'Unidad': 'PZA'}]}},
                {'raw_payload': []},
            ):
                with self.subTest(change=change):
                    PointWasteLine.objects.filter(pk=waste.pk).update(**change)
                    self.assertFalse(self.service.evaluate(month)[self.key]['eligible'])
                    PointWasteLine.objects.filter(pk=waste.pk).update(
                        quantity=2, item_name=self.product.name, unit='PZA', source_hash='d' * 64,
                        movement_external_id='101', movement_at=waste.movement_at,
                        insumo_id=None, raw_payload=original_payload)
            for altered in (replace(history, coverage_status='INCOMPLETE'),
                            replace(history, original_batch_evidence={}),
                            replace(history, waste=Decimal('1'))):
                with patch('reportes.services_product_documentary_close.AuditStockHistoryService.reconcile_many',
                           return_value={self.key: altered}):
                    self.assertFalse(self.service.evaluate(month)[self.key]['eligible'])
            with patch.object(AuditStockHistoryService, '_original_batch', return_value=None):
                self.assertFalse(self.service.evaluate(month)[self.key]['eligible'])
            issue.message = issue.message.replace('waste', 'production')
            self.assertFalse(self.service.evaluate(month)[self.key]['eligible'])

    def test_received_return_is_documentary_but_preserves_custody_warning(self):
        destination = PointBranch.objects.create(external_id="DEST", name="Destino")
        received = timezone.make_aware(datetime(2026, 9, 15, 12))
        row = PointTransferLine.objects.create(
            origin_branch=self.branch, destination_branch=destination,
            transfer_external_id="T1", detail_external_id="D1", source_hash="a" * 64,
            registered_at=received, sent_at=received, received_at=received,
            item_code=self.product.external_id, item_name=self.product.name,
            sent_quantity=2, received_quantity=0, is_received=True, is_finalized=False,
            raw_payload={"detail": {"FK_articulo": 101, "isInsumo": False}},
        )
        self.product.external_id = "101"
        self.product.save(update_fields=["external_id"])
        zero = Decimal("0")
        values = {field: zero for field in (
            "production", "sales", "waste", "transfer_in", "transfer_out",
            "conversion_in", "conversion_out", "identified_adjustment",
        )}
        values.update(transfer_in=Decimal("2"), transfer_out=Decimal("2"))
        issue = SimpleNamespace(
            code="TRANSFER_QUANTITY_MISMATCH", message="Retorno Point; custodia física pendiente.",
            branch_id=self.branch.id, product_id=self.product.id, source_ids=(row.id,),
        )
        line = SimpleNamespace(
            month=date(2026, 9, 1), branch=self.branch, product=self.product,
            opening=zero, point_closing=zero, expected_closing=zero, difference=zero,
            source_trace={"opening": (1,), "closing": (2,), "transfers": (row.id,),
                          "transfer_in": (row.id,), "transfer_out": (row.id,)},
            issues=(issue,), **values,
        )
        history = PointHistoryReconciliation(
            coverage_status="COMPLETE", documentary_opening=zero, documentary_closing=zero,
            **values,
        )
        trace = SimpleNamespace(lines=(line,), global_issues=())
        with patch("reportes.services_product_documentary_close.BranchInventoryTraceabilityService.build", return_value=trace), patch(
            "reportes.services_product_documentary_close.AuditStockHistoryService.reconcile_many", return_value={self.key: history},
        ):
            decision = self.service.evaluate(date(2026, 9, 1))[self.key]
            self.assertTrue(decision["eligible"])
            self.assertEqual(line.issues, (issue,))
            self.assertFalse(decision["evidence"]["administrative_returns"][str(row.id)]["physical_custody_verified"])
            first = self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
            second = self.service.close_eligible(date(2026, 9, 1), actor=self.actor)
            self.assertEqual(first["closed"], 1)
            self.assertEqual(second["unchanged"], 1)
            self.assertEqual(ProductInventoryDocumentaryEvent.objects.count(), 1)
            for trace_key in ("transfers", "transfer_in"):
                with self.subTest(trace_key=trace_key):
                    saved = line.source_trace.pop(trace_key)
                    self.assertFalse(self.service.evaluate(date(2026, 9, 1))[self.key]["eligible"])
                    line.source_trace[trace_key] = saved
            for change in (
                {"is_received": False}, {"is_cancelled": True}, {"is_current_snapshot": False},
                {"received_at": None}, {"received_at": received.replace(month=10)},
                {"received_quantity": 3}, {"received_quantity": -1}, {"is_insumo": True},
                {"raw_payload": {"detail": {"FK_articulo": 102, "isInsumo": False}}},
                {"raw_payload": {"detail": {"FK_articulo": True, "isInsumo": False}}},
                {"raw_payload": {"detail": {"FK_articulo": 101, "isInsumo": True}}},
                {"raw_payload": []},
            ):
                with self.subTest(change=change):
                    PointTransferLine.objects.filter(pk=row.pk).update(**change)
                    self.assertFalse(self.service.evaluate(date(2026, 9, 1))[self.key]["eligible"])
                    PointTransferLine.objects.filter(pk=row.pk).update(
                        is_received=True, is_cancelled=False, is_current_snapshot=True,
                        received_at=received, received_quantity=0, is_insumo=False,
                        raw_payload={"detail": {"FK_articulo": 101, "isInsumo": False}},
                    )
            self.assertIn("no coincide", self.service._pending_reason(
                line, replace(history, transfer_in=Decimal("1"), transfer_out=Decimal("1"))
            ))

    def test_name_warning_is_not_a_documentary_identity(self):
        zero = Decimal("0")
        values = {field: zero for field in (
            "production", "sales", "waste", "transfer_in", "transfer_out",
            "conversion_in", "conversion_out", "identified_adjustment",
        )}
        line = SimpleNamespace(
            opening=zero, point_closing=zero, difference=zero,
            source_trace={"opening": (1,), "closing": (2,)},
            issues=(SimpleNamespace(code="PRODUCT_RESOLVED_BY_NAME"),), **values,
        )
        history = PointHistoryReconciliation(
            coverage_status="COMPLETE", documentary_opening=zero, documentary_closing=zero, **values,
        )
        self.assertIn("necesita aclaración", self.service._pending_reason(line, history))

    def test_unidentified_product_blocks_only_its_branch(self):
        other = PointBranch.objects.create(external_id="OTHER", name="Otra sucursal")
        lines = tuple(SimpleNamespace(branch=branch, product=self.product, opening=0, point_closing=0, issues=()) for branch in (self.branch, other))
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
