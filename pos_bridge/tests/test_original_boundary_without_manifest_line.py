import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase

from pos_bridge.models import (PointBranch, PointProduct, PointProductHistoryImport,
    PointHistoricalInventoryClosing, PointHistoricalInventoryClosingLine)
from recetas.models import Receta
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from pos_bridge.services.monthly_product_balance_service import (
    documentary_original_boundaries, has_documentary_boundary,
    MonthlyPointProductBalanceService,
)
from pos_bridge.services.branch_inventory_traceability_service import BranchInventoryTraceabilityService
from reportes.services_inventory_traceability import InventoryAuditMaterializer
from reportes.services_product_documentary_close import ProductDocumentaryCloseService


class OriginalBoundaryWithoutManifestLineTests(TestCase):
    def setUp(self):
        self.month = date(2026, 9, 1)
        self.branch = PointBranch.objects.create(external_id='24', name='CEDIS')
        self.product = PointProduct.objects.create(external_id='1048', sku='4358', name='Dot Cake')
        self.line = type('Candidate', (), {'branch': self.branch, 'product': self.product})()
        rows = [
            {'FK_Movimiento': 101, 'Movimiento': 'ENTRADA POR PRODUCCIÓN',
             'Fecha': '2026-09-04T18:00:00', 'Cantidad': 3,
             'Existencia_anterior': 0, 'Existencia_nueva': 3, 'Cancelado': False},
            {'FK_Movimiento': 102, 'Movimiento': 'VENTA', 'Fecha': '2026-09-20T18:00:00',
             'Cantidad': 1, 'Existencia_anterior': 3, 'Existencia_nueva': 2, 'Cancelado': False},
            {'FK_Movimiento': 103, 'Movimiento': 'VENTA', 'Fecha': '2026-10-02T18:00:00',
             'Cantidad': 1, 'Existencia_anterior': 2, 'Existencia_nueva': 1, 'Cancelado': False},
        ]
        code = '# original acquisition reader\n'
        proof = {'source': 'POINT_STOCK_HISTORY_API', 'domain': 'PRODUCT', 'response_complete': True,
            'branch_id': self.branch.pk, 'product_id': self.product.pk,
            'request': {'path': '/Stock/GetHistorial', 'params': {'tipo': 'false',
                'almacen': '24', 'pkproducto': '1048', 'movimientos': '5', 'tipoMovimiento': ''}},
            'retrieved_at': '2026-10-04T18:26:15.406476+00:00', 'history_limit': 5,
            'fetched_rows': len(rows),
            'raw_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest(),
            'original_locator': {'source_file': '/evidence/dot.jsonl', 'source_line': 1},
            'request_provenance': {'kind': 'DERIVED_FROM_ACQUISITION_SCRIPT',
                'source_file': '/evidence/read.py', 'source_code': code,
                'source_sha256': hashlib.sha256(code.encode()).hexdigest(),
                'client_contract': 'PointHttpSessionClient.get_stock_history'}}
        AuditStockHistoryService().ingest_original_response(
            self.branch, self.product, self.month, rows, evidence=proof)

    def read(self, *, excluded=(), boundary='opening'):
        return documentary_original_boundaries(
            [self.line], month=self.month, boundary=boundary, excluded_keys=set(excluded), cache={})

    def test_original_proves_new_product_zero_without_inventing_manifest_reference(self):
        key = (self.branch.pk, self.product.pk)
        quantity, proof = self.read()[key]
        self.assertEqual(quantity, Decimal('0'))
        self.assertEqual(proof['contract'], 'POINT_ORIGINAL_HISTORY_BOUNDARY_V1')
        self.assertEqual(proof['movement_ids'], (101, 102))
        self.assertIsNone(proof['line_id'])
        self.assertFalse(proof['physical_count_verified'])
        self.assertTrue(has_documentary_boundary({'historical_boundary_evidence': {'opening': proof}}, 'opening'))
        self.assertEqual(self.read(boundary='closing')[key][0], Decimal('2'))
        self.assertEqual(PointProductHistoryImport.objects.count(), 1)

    def test_existing_unproven_manifest_line_cannot_be_overridden(self):
        self.assertEqual(self.read(excluded=[(self.branch.pk, self.product.pk)]), {})

    def test_corrupt_original_membership_fails_closed(self):
        record = PointProductHistoryImport.objects.get()
        metadata = record.raw_metadata
        metadata['fetched_movement_ids'] = [101, 102]
        record.raw_metadata = metadata
        record.save(update_fields=['raw_metadata'])
        self.assertEqual(self.read(), {})

    def test_case_evidence_displays_independent_import_without_closing_line_lookup(self):
        from types import SimpleNamespace
        from reportes.views_inventory_traceability import _source_evidence_by_step

        proof = self.read()[(self.branch.pk, self.product.pk)][1]
        case = SimpleNamespace(branch=self.branch, source_trace={
            'historical_boundary_evidence': {'opening': proof}})
        evidence, _ = _source_evidence_by_step(case)
        row, = evidence['opening_point']
        self.assertEqual(row['quantity'], Decimal('0'))
        self.assertIn('Historial original de Point', row['reference'])
        self.assertEqual(PointHistoricalInventoryClosingLine.objects.count(), 0)

    def test_placeholder_or_unverified_independent_proof_is_not_a_boundary(self):
        self.assertFalse(has_documentary_boundary({'opening': []}, 'opening'))
        self.assertFalse(has_documentary_boundary({'historical_boundary_evidence': {
            'opening': {'contract': 'POINT_ORIGINAL_HISTORY_BOUNDARY_V1', 'effective_stock': '0'}}}, 'opening'))

    def manifests(self):
        old = PointProduct.objects.create(external_id='old', sku='OLD', name='Anterior')
        Receta.objects.create(nombre='Dot Cake', codigo_point='4358', tipo=Receta.TIPO_PRODUCTO_FINAL,
                              hash_contenido='dot-test')
        opening = PointHistoricalInventoryClosing.objects.create(
            operational_date=date(2026, 8, 31), status='VERIFIED', source='POINT_STOCK_HISTORY',
            source_fingerprint='opening-preserved', expected_branch_ids=[self.branch.pk],
            expected_product_ids=[old.pk], metadata={'method': 'point_stock_history_boundary'},
            retrieved_at=datetime(2026, 10, 4, tzinfo=timezone.utc))
        PointHistoricalInventoryClosingLine.objects.create(
            closing=opening, branch=self.branch, product=old, stock=0)
        closing = PointHistoricalInventoryClosing.objects.create(
            operational_date=date(2026, 9, 30), status='VERIFIED', source='POINT_STOCK_HISTORY',
            source_fingerprint='closing-preserved', expected_branch_ids=[self.branch.pk],
            expected_product_ids=[self.product.pk], metadata={'method': 'point_stock_history_boundary'},
            retrieved_at=datetime(2026, 10, 4, tzinfo=timezone.utc))
        PointHistoricalInventoryClosingLine.objects.create(
            closing=closing, branch=self.branch, product=self.product, stock=2)
        return opening, closing

    def test_monthly_reader_supplements_without_claiming_original_manifest_membership(self):
        opening, closing = self.manifests()
        values, meta, unresolved = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=opening.operational_date, source='opening_snapshot')
        recipe = Receta.objects.get(codigo_point='4358')
        self.assertEqual(values[recipe.pk], (Decimal('0'), 1))
        self.assertEqual(meta['selected_coverage_key_count'], 1)
        self.assertEqual(meta['independent_original_boundary_keys'], ((self.branch.pk, self.product.pk),))
        self.assertNotIn((self.branch.pk, self.product.pk), meta['selected_coverage_keys'])
        self.assertEqual(opening.lines.count(), 1)
        self.assertEqual(opening.source_fingerprint, 'opening-preserved')
        self.assertTrue(unresolved)  # The unrelated old product remains unproven.

    def test_trace_materializer_and_documentary_close_share_independent_proof(self):
        self.manifests()
        service = BranchInventoryTraceabilityService()
        movements = tuple({} for _ in range(11)) + ([], [])
        movements[0][(self.branch.pk, self.product.pk)] = (Decimal('1'), [102])
        with patch.object(service, '_load_direct_movements', return_value=movements):
            trace = service.build(self.month, allow_partial=True)
        line = next(line for line in trace.lines if line.product.pk == self.product.pk)
        self.assertEqual(line.opening, Decimal('0'))
        self.assertEqual(line.point_closing, Decimal('2'))
        self.assertEqual(line.source_trace['opening'], ())
        self.assertTrue(has_documentary_boundary(line.source_trace, 'opening'))
        self.assertFalse(any(issue.code == 'SOURCE_INCOMPLETE' for issue in line.issues))
        history = AuditStockHistoryService().reconcile(self.branch, self.product, self.month)
        prepared = InventoryAuditMaterializer()._prepare_line(line, point_history=history)
        self.assertEqual(Decimal(prepared['quantities']['difference']), Decimal('0'))
        from dataclasses import replace
        reconciled = replace(line, production=history.production, sales=history.sales,
                             expected_closing=Decimal('2'), difference=Decimal('0'))
        self.assertEqual(ProductDocumentaryCloseService._pending_reason(reconciled, history), '')
