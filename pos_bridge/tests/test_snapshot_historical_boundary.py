from datetime import date, datetime, timedelta, timezone as dt_timezone
from decimal import Decimal
from unittest.mock import patch

from django.db import connection
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext

from pos_bridge.models import (
    PointBranch, PointProduct, PointSyncJob, PointExtractionLog,
    PointInventorySnapshot, PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine, PointProductHistoryImport, PointProductHistoryRow,
)
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService, PointHistoryReconciliation
from pos_bridge.services.monthly_product_balance_service import (
    MonthlyPointProductBalanceService, documentary_historical_boundary,
)
from pos_bridge.services.branch_inventory_traceability_service import BranchInventoryTraceabilityService
from recetas.models import Receta


class SnapshotHistoricalBoundaryTests(TestCase):
    def setUp(self):
        self.month = date(2026, 9, 1)
        self.cutoff = datetime(2026, 10, 1, 7, tzinfo=dt_timezone.utc)
        self.branch = PointBranch.objects.create(external_id="branch-exact", name="Sucursal")
        self.product = PointProduct.objects.create(external_id="product-exact", sku="sku", name="Producto")
        self.closing = PointHistoricalInventoryClosing.objects.create(
            operational_date=date(2026, 9, 30), status="VERIFIED", source="POINT_STOCK_HISTORY",
            source_fingerprint="boundary-snapshot", expected_branch_ids=[self.branch.pk],
            expected_product_ids=[self.product.pk], metadata={"method": "point_stock_history_boundary"},
            retrieved_at=self.cutoff + timedelta(hours=3),
        )
        self.line = PointHistoricalInventoryClosingLine.objects.create(
            closing=self.closing, branch=self.branch, product=self.product, stock=Decimal("3"),
        )
        self.job = PointSyncJob.objects.create(job_type="inventory", status="SUCCESS")
        self.log = PointExtractionLog.objects.create(
            sync_job=self.job, level="INFO", message="Sucursal procesada branch-exact.",
            context={"branch_id": self.branch.pk, "branch_external_id": self.branch.external_id,
                     "products_seen": 1, "snapshots_created": 1},
        )

    def _snapshot(self, *, stock="1", raw=None, **overrides):
        values = dict(branch=self.branch, product=self.product, sync_job=self.job,
                      stock=Decimal(stock), captured_at=self.cutoff + timedelta(hours=2),
                      raw_payload=raw if raw is not None else {
                          "headers": ["Código", "Producto", "Cantidad", "Unidad", "Costo unitario",
                                      "Costo total", "Último Movimiento"],
                          "row": [self.product.external_id, "sku", "Producto", "Pasteles", stock,
                                  "Pza", "10", "10", "2026-10-01T02:18:44.487", "False"],
                      })
        values.update(overrides)
        return PointInventorySnapshot.objects.create(**values)

    def _read(self, *, cache=None, lines=None, closing=None, boundary="closing"):
        return documentary_historical_boundary(
            closing or self.closing, lines or [self.line], month=self.month,
            boundary=boundary, cache={} if cache is None else cache,
        )

    def _incomplete_history(self, *, stamp="2026-10-01T02:18:44.487", previous=2, new=1, product=None):
        service = AuditStockHistoryService()
        record = service._canonical_import(self.branch, product or self.product)
        raw = {"FK_Movimiento": 910, "Movimiento": "VENTA", "Fecha": stamp,
               "Cantidad": 1, "Existencia_anterior": previous, "Existencia_nueva": new,
               "Costo_Total": 0, "Costo_Unitario": 0, "Cancelado": False}
        movement_id, defaults = service._parse_row(raw)
        PointProductHistoryRow.objects.create(import_record=record, row_number=movement_id, **defaults)
        record.row_count = 1
        record.raw_metadata = {"source": "POINT_STOCK_HISTORY_API", "fetched_rows": 1,
                               "history_limit": 500, "fetched_movement_ids": [910],
                               "fetched_at": "2026-10-01T03:00:00+00:00"}
        record.save()
        return record

    def test_independent_snapshot_with_incomplete_history_preserves_coverage_and_originals(self):
        self._snapshot()
        record = self._incomplete_history()
        before = list(record.rows.values())
        metadata = dict(record.raw_metadata)
        values, proofs, unproven = self._read()
        self.assertEqual(values, {self.line.pk: Decimal("1")})
        self.assertEqual(unproven, ())
        proof = proofs[self.line.pk]
        self.assertTrue(proof["snapshot_boundary_verified"])
        self.assertFalse(proof["canonical_history_verified"])
        self.assertFalse(proof["physical_count_verified"])
        self.assertEqual(proof["coverage_status"], "INCOMPLETE")
        self.assertEqual(proof["snapshot_boundary_evidence"]["canonical_consistency"]["import_id"], record.pk)
        record.refresh_from_db()
        self.assertEqual(record.raw_metadata, metadata)
        self.assertEqual(list(record.rows.values()), before)

    def test_saturated_partial_membership_does_not_block_independent_frontier(self):
        self._snapshot()
        record = self._incomplete_history()
        record.raw_metadata.update(fetched_rows=500)
        record.raw_metadata.pop("fetched_movement_ids")
        record.save()
        values, proofs, _ = self._read()
        self.assertEqual(values, {self.line.pk: Decimal("1")})
        proof = proofs[self.line.pk]["snapshot_boundary_evidence"]["canonical_consistency"]
        self.assertFalse(proof["canonical_membership_verified"])
        self.assertEqual(proofs[self.line.pk]["coverage_status"], "INCOMPLETE")

    def test_retained_movement_outside_latest_batch_can_veto_snapshot(self):
        self._snapshot()
        record = self._incomplete_history()
        raw = {"FK_Movimiento": 911, "Movimiento": "VENTA", "Fecha": "2026-10-01T03:00:00",
               "Cantidad": 1, "Existencia_anterior": 1, "Existencia_nueva": 0,
               "Costo_Total": 0, "Costo_Unitario": 0, "Cancelado": False}
        _, defaults = AuditStockHistoryService._parse_row(raw)
        PointProductHistoryRow.objects.create(import_record=record, row_number=911, **defaults)
        record.row_count = 2
        record.save()
        self.assertEqual(self._read()[0], {})

    def test_known_unknown_raw_or_store_contradictions_reject_snapshot(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        original = dict(row.raw_payload)
        for field, value in [("FK_Movimiento", True), ("Fecha", "bad"),
                             ("Cancelado", "garbage"), ("Cantidad", 2)]:
            with self.subTest(field=field):
                row.raw_payload = {**original, field: value}
                row.save(update_fields=["raw_payload"])
                self.assertEqual(self._read()[0], {})

    def test_missing_original_fields_or_wrong_explicit_domain_reject(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        original = dict(row.raw_payload)
        for field in ("Cantidad", "Existencia_anterior", "Existencia_nueva"):
            with self.subTest(field=field):
                row.raw_payload = dict(original)
                row.raw_payload.pop(field)
                row.save(update_fields=["raw_payload"])
                self.assertEqual(self._read()[0], {})
        row.raw_payload = {**original, "isInsumo": True}
        row.save(update_fields=["raw_payload"])
        self.assertEqual(self._read()[0], {})

    def test_stored_cost_mutation_is_not_original_raw_consistency(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        row.raw_payload["Costo_Total"] = 4
        row.save(update_fields=["raw_payload"])
        self.assertEqual(self._read()[0], {})

    def test_original_cost_precision_follows_postgresql_storage_not_a_false_veto(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        raw = {**row.raw_payload, "Costo_Total": "1.1234567", "Costo_Unitario": "0.9876545"}
        _, defaults = AuditStockHistoryService._parse_row(raw)
        for field, value in defaults.items():
            setattr(row, field, value)
        row.save()
        row.refresh_from_db()
        self.assertEqual(row.total_cost, Decimal("1.123457"))
        self.assertEqual(self._read()[0], {self.line.pk: Decimal("1")})

    def test_original_stock_precision_uses_raw_delta_and_only_normalizes_db_comparison(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        for previous, quantity, new in [("2", "0.0045", "1.9955"),
                                         ("-2", "0.0005", "-2.0005")]:
            with self.subTest(new=new):
                raw = {**row.raw_payload, "Fecha": "2026-10-01T01:00:00",
                       "Existencia_anterior": previous, "Cantidad": quantity, "Existencia_nueva": new}
                _, defaults = AuditStockHistoryService._parse_row(raw)
                for field, value in defaults.items():
                    setattr(row, field, value)
                row.save()
                self.assertEqual(self._read()[0], {self.line.pk: Decimal("1")})

    def test_original_string_movement_identity_matches_exact_integer_row(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        row.raw_payload["FK_Movimiento"] = "910"
        row.save(update_fields=["raw_payload"])
        self.assertEqual(self._read()[0], {self.line.pk: Decimal("1")})
        for value in (True, 910.0, "+910", "x", "٩١٠"):
            with self.subTest(value=value):
                row.raw_payload["FK_Movimiento"] = value
                row.save(update_fields=["raw_payload"])
                self.assertEqual(self._read()[0], {})

    def test_raw_last_movement_stock_cannot_match_only_after_db_rounding(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        raw = {**row.raw_payload, "Existencia_anterior": "2.0004", "Existencia_nueva": "1.0004"}
        _, defaults = AuditStockHistoryService._parse_row(raw)
        for field, value in defaults.items():
            setattr(row, field, value)
        row.save()
        self.assertEqual(self._read()[0], {})

    def test_incomplete_membership_metadata_invalid_present_ids_reject(self):
        self._snapshot()
        record = self._incomplete_history()
        for ids in [[True], [910, 910], [999], "910", None]:
            with self.subTest(ids=ids):
                record.raw_metadata["fetched_movement_ids"] = ids
                record.save()
                self.assertEqual(self._read()[0], {})

    def test_source_mutation_changes_independent_fingerprint_without_quantity_change(self):
        self._snapshot()
        record = self._incomplete_history()
        before = self._read()[1][self.line.pk]["snapshot_boundary_evidence"]["canonical_consistency"]["source_signature"]
        record.raw_metadata["evidence_note"] = "original addition"
        record.save()
        after = self._read()[1][self.line.pk]["snapshot_boundary_evidence"]["canonical_consistency"]["source_signature"]
        self.assertNotEqual(before, after)

    def test_partial_retained_gap_does_not_claim_continuity(self):
        self._snapshot()
        record = self._incomplete_history(stamp="2026-10-01T01:00:00", previous=9, new=8)
        record.raw_metadata.pop("fetched_movement_ids")
        record.save()
        values, proofs, _ = self._read()
        self.assertEqual(values, {self.line.pk: Decimal("1")})
        self.assertFalse(proofs[self.line.pk]["canonical_history_verified"])

    def test_cancelled_original_is_neutral_but_effective_reversal_can_veto(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        raw = {**row.raw_payload, "Fecha": "2026-10-01T03:00:00", "Cancelado": True}
        _, defaults = AuditStockHistoryService._parse_row(raw)
        for field, value in defaults.items():
            setattr(row, field, value)
        row.save()
        self.assertEqual(self._read()[0], {self.line.pk: Decimal("1")})
        raw.update(Cancelado=False, Movimiento="CANCELACION VENTA", Existencia_anterior=1, Existencia_nueva=2)
        _, defaults = AuditStockHistoryService._parse_row(raw)
        for field, value in defaults.items():
            setattr(row, field, value)
        row.save()
        self.assertEqual(self._read()[0], {})

    def test_matching_stock_does_not_override_conflicting_snapshot_last_movement(self):
        self._snapshot()
        other = self._snapshot(captured_at=self.cutoff + timedelta(hours=4))
        other.raw_payload["row"][8] = "2026-10-01T02:18:45.487"
        other.save(update_fields=["raw_payload"])
        self.assertEqual(self._read()[0], {})

    def test_same_timestamp_invalid_adjustment_is_not_known_consistency(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        raw = {**row.raw_payload, "Movimiento": "AJUSTE SALIDA", "Cantidad": 2}
        _, defaults = AuditStockHistoryService._parse_row(raw)
        for field, value in defaults.items():
            setattr(row, field, value)
        row.save()
        self.assertEqual(self._read()[0], {})

    def test_post_cut_snapshot_proves_documentary_quantity_without_canonical_or_physical_claim(self):
        snapshot = self._snapshot()
        values, proofs, unproven = self._read()
        self.assertEqual(values, {self.line.pk: Decimal("1")})
        self.assertEqual(unproven, ())
        proof = proofs[self.line.pk]
        self.assertFalse(proof["canonical_history_verified"])
        self.assertFalse(proof["physical_count_verified"])
        self.assertTrue(proof["snapshot_boundary_verified"])
        self.assertEqual(proof["coverage_status"], "MISSING")
        self.assertEqual(proof["movement_ids"], ())
        source = proof["snapshot_boundary_evidence"]
        self.assertEqual(source["snapshot_id"], snapshot.pk)
        self.assertEqual(source["sync_job_id"], self.job.pk)
        self.assertEqual(source["branch_id"], self.branch.pk)
        self.assertEqual(source["product_id"], self.product.pk)
        self.assertEqual(source["product_external_id"], self.product.external_id)
        self.assertEqual(source["last_movement_at"], "2026-10-01T02:18:44.487000+00:00")
        self.assertEqual(source["effective_at"], self.cutoff.isoformat())
        self.assertEqual(len(source["raw_signature"]), 64)
        self.line.refresh_from_db()
        self.assertEqual(self.line.stock, Decimal("3"))

    def test_opening_uses_its_own_cutoff(self):
        cutoff = datetime(2026, 9, 1, 7, tzinfo=dt_timezone.utc)
        self.closing.operational_date = date(2026, 8, 31)
        self.closing.retrieved_at = cutoff + timedelta(hours=3)
        snapshot = self._snapshot(captured_at=cutoff + timedelta(hours=2))
        snapshot.raw_payload["row"][8] = "2026-09-01T01:10:17.903"
        snapshot.save(update_fields=["raw_payload"])
        values, proofs, _ = self._read(boundary="opening")
        self.assertEqual(values, {self.line.pk: Decimal("1")})
        self.assertEqual(proofs[self.line.pk]["snapshot_boundary_evidence"]["effective_at"], cutoff.isoformat())

    def test_snapshot_at_cut_and_movement_at_cut_do_not_prove_previous_day(self):
        snapshot = self._snapshot(captured_at=self.cutoff)
        self.assertEqual(self._read()[0], {})
        snapshot.captured_at = self.cutoff + timedelta(hours=2)
        snapshot.raw_payload["row"][8] = "2026-10-01T07:00:00"
        snapshot.save(update_fields=["captured_at", "raw_payload"])
        self.assertEqual(self._read()[0], {})

    def test_consolidated_opening_uses_independent_snapshot_not_original_quantity_or_zero_proof(self):
        cutoff = datetime(2026, 9, 1, 7, tzinfo=dt_timezone.utc)
        self.closing.operational_date = date(2026, 8, 31)
        self.closing.retrieved_at = cutoff + timedelta(hours=3)
        self.closing.metadata = {"method": "consolidated_point_stock_history_attempts", "source_closing_ids": [4, 5]}
        snapshot = self._snapshot(captured_at=cutoff + timedelta(hours=2))
        snapshot.raw_payload["row"][8] = "2026-08-31T23:10:17.903"
        snapshot.save(update_fields=["raw_payload"])
        values, proofs, unproven = self._read(boundary="opening")
        self.assertEqual(values, {self.line.pk: Decimal("1")})
        self.assertEqual(unproven, ())
        self.assertTrue(proofs[self.line.pk]["snapshot_boundary_verified"])
        self.assertFalse(proofs[self.line.pk]["original_boundary_verified"])
        self.assertFalse(proofs[self.line.pk]["canonical_history_verified"])
        snapshot.delete()
        self.line.stock = Decimal("0")
        self.line.evidence = {"method": "no_history_current_zero", "history_rows": 0, "history_limit": 500}
        self.assertEqual(self._read(boundary="opening")[0], {})

    def test_missing_coverage_metadata_does_not_certify_or_block_independent_snapshot(self):
        self._snapshot()
        PointProductHistoryImport.objects.create(
            file_hash="incomplete", source_filename="history", report_path="/Stock/GetHistorial",
            product_name=self.product.name, point_branch=self.branch, point_product=self.product,
            raw_metadata={"source": "POINT_STOCK_HISTORY_API"},
        )
        values, proofs, unproven = self._read()
        self.assertEqual(values, {self.line.pk: Decimal("1")})
        self.assertEqual(unproven, ())
        self.assertEqual(proofs[self.line.pk]["coverage_status"], "INCOMPLETE")
        self.assertFalse(proofs[self.line.pk]["canonical_history_verified"])

    def test_multiple_exact_pair_imports_cannot_overwrite_a_known_veto(self):
        self._snapshot()
        record = self._incomplete_history()
        other = PointProductHistoryImport.objects.create(
            file_hash="retained-other-source", source_filename="history", report_path="/Stock/GetHistorial",
            product_name=self.product.name, point_branch=self.branch, point_product=self.product,
            raw_metadata=dict(record.raw_metadata), row_count=1)
        row = record.rows.get()
        raw = {**row.raw_payload, "Movimiento": "UNKNOWN MOVEMENT"}
        _, defaults = AuditStockHistoryService._parse_row(raw)
        PointProductHistoryRow.objects.create(import_record=other, row_number=910, **defaults)
        self.assertEqual(self._read()[0], {})
        # Invert the auto ordering: ambiguity cannot disappear with row order.
        record.created_at, other.created_at = other.created_at, record.created_at
        record.save(update_fields=["created_at"])
        other.save(update_fields=["created_at"])
        self.assertEqual(self._read()[0], {})

    def test_impossible_known_incoming_or_outgoing_delta_is_not_snapshot_consistency(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        original = dict(row.raw_payload)
        for movement in ["PRODUCCION", "TRANSFERENCIA ENTRADA", "CONVERSION ENTRADA"]:
            with self.subTest(movement=movement):
                raw = {**original, "Movimiento": movement}
                _, defaults = AuditStockHistoryService._parse_row(raw)
                for field, value in defaults.items():
                    setattr(row, field, value)
                row.save()
                self.assertEqual(self._read()[0], {})

    def test_cancelled_raw_missing_movement_is_still_malformed(self):
        self._snapshot()
        record = self._incomplete_history()
        row = record.rows.get()
        raw = {**row.raw_payload, "Cancelado": True}
        raw.pop("Movimiento")
        _, defaults = AuditStockHistoryService._parse_row(raw)
        for field, value in defaults.items():
            setattr(row, field, value)
        row.save()
        self.assertEqual(self._read()[0], {})

    def test_complete_canonical_takes_precedence(self):
        self._snapshot()
        result = PointHistoryReconciliation(coverage_status="COMPLETE", documentary_closing=Decimal("5"))
        with patch("pos_bridge.services.monthly_product_balance_service.AuditStockHistoryService.reconcile_many",
                   return_value={(self.branch.pk, self.product.pk): result}):
            values, proofs, _ = self._read()
        self.assertEqual(values, {self.line.pk: Decimal("5")})
        self.assertTrue(proofs[self.line.pk]["canonical_history_verified"])

    def test_rejects_malformed_domain_identity_stock_or_time(self):
        mutations = [
            ("bad-length", lambda raw: raw["row"].pop()),
            ("domain", lambda raw: raw["row"].__setitem__(9, "True")),
            ("empty-domain", lambda raw: raw["row"].__setitem__(9, "")),
            ("identity", lambda raw: raw["row"].__setitem__(0, "other-product")),
            ("stock", lambda raw: raw["row"].__setitem__(4, "3")),
            ("invalid-stock", lambda raw: raw["row"].__setitem__(4, "NaN")),
            ("future", lambda raw: raw["row"].__setitem__(8, "2026-10-01T07:00:00.001")),
            ("invalid-time", lambda raw: raw["row"].__setitem__(8, "garbage")),
            ("date-without-time", lambda raw: raw["row"].__setitem__(8, "2026-09-30")),
            ("aware-time", lambda raw: raw["row"].__setitem__(8, "2026-10-01T02:18:44Z")),
            ("header", lambda raw: raw.__setitem__("headers", ["Unrelated"])),
        ]
        for name, mutate in mutations:
            with self.subTest(name=name):
                snapshot = self._snapshot()
                mutate(snapshot.raw_payload)
                snapshot.save(update_fields=["raw_payload"])
                try:
                    values, _, unproven = self._read()
                    self.assertEqual(values, {})
                    self.assertEqual(unproven, (self.line,))
                finally:
                    snapshot.delete()

    def test_failed_job_wrong_inventory_job_or_outside_window_are_unproven(self):
        snapshot = self._snapshot()
        for when, status, job_type in [
            (self.cutoff + timedelta(hours=2), "FAILED", "inventory"),
            (self.cutoff - timedelta(microseconds=1), "SUCCESS", "inventory"),
            (self.cutoff + timedelta(days=3, microseconds=1), "SUCCESS", "inventory"),
            (self.cutoff + timedelta(hours=2), "SUCCESS", "sales"),
        ]:
            with self.subTest(when=when, status=status, job_type=job_type):
                snapshot.captured_at = when
                snapshot.save(update_fields=["captured_at"])
                self.job.status = status
                self.job.job_type = job_type
                self.job.save(update_fields=["status", "job_type"])
                self.assertEqual(self._read()[0], {})

    def test_absent_or_contradictory_original_branch_provenance_is_unproven(self):
        self._snapshot()
        for context in [{}, {**self.log.context, "branch_external_id": "other"}]:
            with self.subTest(context=context):
                self.log.context = context
                self.log.save(update_fields=["context"])
                self.assertEqual(self._read()[0], {})

    def test_snapshot_of_other_exact_branch_or_product_is_not_reused(self):
        other_branch = PointBranch.objects.create(external_id="other-branch", name="Otra")
        other_product = PointProduct.objects.create(external_id="other-product", name="Otro")
        self._snapshot(branch=other_branch)
        self._snapshot(product=other_product)
        self.assertEqual(self._read()[0], {})

    def test_contradictory_qualified_snapshots_do_not_select_convenient_stock(self):
        self._snapshot(stock="1")
        self._snapshot(stock="3", captured_at=self.cutoff + timedelta(hours=4))
        values, _, unproven = self._read()
        self.assertEqual(values, {})
        self.assertEqual(unproven, (self.line,))

    def test_invalid_original_manifest_does_not_enable_snapshot_fallback(self):
        self._snapshot()
        for field, invalid in [("status", "DRAFT"), ("operational_date", date(2026, 9, 29)),
                               ("metadata", {}), ("metadata", {"method": []}),
                               ("metadata", {"method": {}}), ("expected_product_ids", []),
                               ("retrieved_at", self.cutoff - timedelta(seconds=1))]:
            with self.subTest(field=field):
                original = getattr(self.closing, field)
                setattr(self.closing, field, invalid)
                self.assertEqual(self._read()[0], {})
                setattr(self.closing, field, original)

    def test_raw_change_changes_proof_signature_on_fresh_read(self):
        snapshot = self._snapshot()
        proof = self._read()[1][self.line.pk]
        self.assertIn("snapshot_boundary_evidence", proof)
        before = proof["snapshot_boundary_evidence"]["raw_signature"]
        snapshot.raw_payload["row"][8] = "2026-10-01T02:18:45.487"
        snapshot.save(update_fields=["raw_payload"])
        after = self._read()[1][self.line.pk]["snapshot_boundary_evidence"]["raw_signature"]
        self.assertNotEqual(before, after)

    def test_bulk_query_count_and_boundary_cache_do_not_grow_per_pair(self):
        self._snapshot()
        lines = [self.line]
        for index in range(12):
            product = PointProduct.objects.create(external_id=f"product-{index}", name=f"Producto {index}")
            lines.append(PointHistoricalInventoryClosingLine.objects.create(
                closing=self.closing, branch=self.branch, product=product, stock=3,
            ))
            self._snapshot(product=product, raw={
                "headers": ["Código", "Producto", "Cantidad", "Último Movimiento"],
                "row": [product.external_id, "sku", product.name, "Pasteles", "1", "Pza",
                        "10", "10", "2026-10-01T02:18:44.487", "False"],
            })
        self.closing.expected_product_ids = [line.product_id for line in lines]
        cache = {}
        with CaptureQueriesContext(connection) as queries:
            values, _, _ = self._read(lines=lines, cache=cache)
        self.assertEqual(len(values), 13)
        # Only the source mutex is added to the original five-query budget.
        self.assertLessEqual(len(queries), 6)
        with CaptureQueriesContext(connection) as cached:
            self._read(lines=lines, cache=cache)
        self.assertFalse([query for query in cached if ' FROM ' in query["sql"]])
        self.assertEqual(len(cached), 1)

    def test_incomplete_consistency_bulk_queries_remain_constant_for_thirteen_pairs(self):
        self._snapshot()
        self._incomplete_history()
        lines = [self.line]
        for index in range(12):
            product = PointProduct.objects.create(external_id=f"partial-{index}", name=f"Partial {index}")
            lines.append(PointHistoricalInventoryClosingLine.objects.create(
                closing=self.closing, branch=self.branch, product=product, stock=3))
            self._snapshot(product=product, raw={
                "headers": ["Código", "Producto", "Cantidad", "Último Movimiento"],
                "row": [product.external_id, "sku", product.name, "Pasteles", "1", "Pza",
                        "10", "10", "2026-10-01T02:18:44.487", "False"]})
            self._incomplete_history(product=product)
        self.closing.expected_product_ids = [line.product_id for line in lines]
        with CaptureQueriesContext(connection) as queries:
            values, _, _ = self._read(lines=lines)
        self.assertEqual(len(values), 13)
        self.assertLessEqual(len(queries), 8)

    def _consumer_fixture(self):
        recipe = Receta.objects.create(
            nombre=self.product.name, codigo_point=self.product.sku,
            tipo=Receta.TIPO_PRODUCTO_FINAL, hash_contenido="snapshot-consumer-primary",
        )
        missing_product = PointProduct.objects.create(
            external_id="missing-snapshot", sku="missing-sku", name="Producto sin frontera",
        )
        missing_recipe = Receta.objects.create(
            nombre=missing_product.name, codigo_point=missing_product.sku,
            tipo=Receta.TIPO_PRODUCTO_FINAL, hash_contenido="snapshot-consumer-missing",
        )
        missing_line = PointHistoricalInventoryClosingLine.objects.create(
            closing=self.closing, branch=self.branch, product=missing_product, stock=Decimal("7"),
        )
        self.closing.expected_product_ids = [self.product.pk, missing_product.pk]
        self.closing.save(update_fields=["expected_product_ids"])
        return recipe, missing_recipe, missing_line, self._snapshot()

    def _assert_consumer_snapshot_proof(self, proof, snapshot, coverage="MISSING"):
        self.assertEqual(Decimal(proof["effective_stock"]), snapshot.stock)
        self.assertEqual(proof["coverage_status"], coverage)
        self.assertFalse(proof["canonical_history_verified"])
        self.assertFalse(proof["physical_count_verified"])
        self.assertTrue(proof["snapshot_boundary_verified"])
        source = proof["snapshot_boundary_evidence"]
        self.assertEqual(source["snapshot_id"], snapshot.pk)
        self.assertEqual(source["sync_job_id"], self.job.pk)
        self.assertEqual(source["branch_id"], self.branch.pk)
        self.assertEqual(source["product_id"], self.product.pk)
        self.assertEqual(source["effective_at"], self.cutoff.isoformat())

    def test_monthly_consumer_uses_snapshot_proof_and_filters_unproven_original_stock(self):
        recipe, missing_recipe, missing_line, snapshot = self._consumer_fixture()
        values, metadata, unresolved = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=self.closing.operational_date, source="closing_snapshot",
        )
        self.assertEqual(values, {recipe.pk: (Decimal("1"), 1)})
        self.assertNotIn(missing_recipe.pk, values)
        self.assertFalse(metadata["authoritative"])
        self.assertTrue(metadata["source_present"])
        self.assertEqual(metadata["applied_rows"], 1)
        self.assertEqual(metadata["unresolved_rows"], 1)
        self.assertEqual([(item.movement_id, item.issue) for item in unresolved],
                         [(str(missing_line.pk), "SOURCE_INCOMPLETE")])
        proofs = {item["line_id"]: item for item in metadata["historical_boundary_evidence"]}
        self._assert_consumer_snapshot_proof(proofs[self.line.pk], snapshot)
        self.assertIsNone(proofs[missing_line.pk]["effective_stock"])
        self.assertFalse(proofs[missing_line.pk]["snapshot_boundary_verified"])

    def test_branch_consumer_uses_snapshot_proof_and_keeps_unproven_pair_issue(self):
        _, _, missing_line, snapshot = self._consumer_fixture()
        service = BranchInventoryTraceabilityService()
        values = service._load_closing(self.closing, month=self.month, boundary="closing")
        key = (self.branch.pk, self.product.pk)
        missing_key = (missing_line.branch_id, missing_line.product_id)
        self.assertEqual(values, {key: (Decimal("1"), [self.line.pk])})
        self.assertNotIn(missing_key, values)
        self._assert_consumer_snapshot_proof(service._historical_boundary_evidence[key]["closing"], snapshot)
        missing_proof = service._historical_boundary_evidence[missing_key]["closing"]
        self.assertIsNone(missing_proof["effective_stock"])
        self.assertEqual(missing_proof["coverage_status"], "MISSING")
        self.assertFalse(missing_proof["snapshot_boundary_verified"])
        self.assertEqual(
            [(item.code, item.branch_id, item.product_id, item.source_ids)
             for item in service._historical_boundary_issues],
            [("SOURCE_INCOMPLETE", missing_line.branch_id, missing_line.product_id, (missing_line.pk,))],
        )

    def test_monthly_and_branch_consumers_preserve_incomplete_snapshot_authority(self):
        recipe, _, _, snapshot = self._consumer_fixture()
        self._incomplete_history()
        values, metadata, _ = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=self.closing.operational_date, source="closing_snapshot")
        self.assertEqual(values[recipe.pk], (Decimal("1"), 1))
        proof = next(item for item in metadata["historical_boundary_evidence"] if item["line_id"] == self.line.pk)
        self._assert_consumer_snapshot_proof(proof, snapshot, coverage="INCOMPLETE")
        branch = BranchInventoryTraceabilityService()
        self.assertEqual(branch._load_closing(self.closing, month=self.month)[
            (self.branch.pk, self.product.pk)], (Decimal("1"), [self.line.pk]))
        self._assert_consumer_snapshot_proof(branch._historical_boundary_evidence[
            (self.branch.pk, self.product.pk)]["closing"], snapshot, coverage="INCOMPLETE")

    def test_rejected_partial_proof_contains_exact_source_signature_and_reason(self):
        self._snapshot()
        record = self._incomplete_history()
        record.raw_metadata["fetched_movement_ids"] = [999]
        record.save()
        values, proof, _ = self._read()
        self.assertEqual(values, {})
        rejected = proof[self.line.pk]["canonical_consistency_evidence"]
        self.assertFalse(rejected["valid"])
        self.assertEqual(rejected["reason"], "INVALID_BATCH_METADATA")
        self.assertEqual(len(rejected["source_signature"]), 64)

    def test_provenance_mutation_changes_qualified_snapshot_signature(self):
        self._snapshot()
        before = self._read()[1][self.line.pk]["snapshot_boundary_evidence"]["qualified_snapshot_signature"]
        self.log.context["original_extra"] = "captured source detail"
        self.log.save(update_fields=["context"])
        after = self._read()[1][self.line.pk]["snapshot_boundary_evidence"]["qualified_snapshot_signature"]
        self.assertNotEqual(before, after)

    def test_empty_partial_capture_does_not_override_exact_snapshot(self):
        self._snapshot()
        record = AuditStockHistoryService()._canonical_import(self.branch, self.product)
        record.raw_metadata.update(fetched_rows=0, history_limit=500, fetched_movement_ids=[],
                                   fetched_at="2026-10-01T03:00:00+00:00")
        record.save()
        self.assertEqual(self._read()[0], {self.line.pk: Decimal("1")})


class SnapshotBoundaryTransactionTests(TransactionTestCase):
    def test_external_cache_cannot_reuse_source_proof_across_transactions(self):
        fixture = SnapshotHistoricalBoundaryTests(methodName="test_opening_uses_its_own_cutoff")
        fixture.setUp()
        snapshot = fixture._snapshot()
        fixture._incomplete_history()
        cache = {}
        first = fixture._read(cache=cache)[1][fixture.line.pk]["snapshot_boundary_evidence"]["raw_signature"]
        snapshot.raw_payload["row"][8] = "2026-10-01T02:18:45.487"
        snapshot.save(update_fields=["raw_payload"])
        second = fixture._read(cache=cache)[1][fixture.line.pk]["snapshot_boundary_evidence"]["raw_signature"]
        self.assertNotEqual(first, second)
