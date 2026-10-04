from datetime import date, datetime, timedelta, timezone as dt_timezone
from decimal import Decimal
from unittest.mock import patch

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from pos_bridge.models import (
    PointBranch, PointProduct, PointSyncJob, PointExtractionLog,
    PointInventorySnapshot, PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine, PointProductHistoryImport,
)
from pos_bridge.services.audit_stock_history_service import PointHistoryReconciliation
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

    def test_existing_incomplete_canonical_is_never_hidden_by_snapshot(self):
        self._snapshot()
        PointProductHistoryImport.objects.create(
            file_hash="incomplete", source_filename="history", report_path="/Stock/GetHistorial",
            product_name=self.product.name, point_branch=self.branch, point_product=self.product,
            raw_metadata={"source": "POINT_STOCK_HISTORY_API"},
        )
        values, proofs, unproven = self._read()
        self.assertEqual(values, {})
        self.assertEqual(unproven, (self.line,))
        self.assertEqual(proofs[self.line.pk]["coverage_status"], "INCOMPLETE")

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
        self.assertLessEqual(len(queries), 5)
        with CaptureQueriesContext(connection) as cached:
            self._read(lines=lines, cache=cache)
        self.assertEqual(len(cached), 0)

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

    def _assert_consumer_snapshot_proof(self, proof, snapshot):
        self.assertEqual(Decimal(proof["effective_stock"]), snapshot.stock)
        self.assertEqual(proof["coverage_status"], "MISSING")
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
