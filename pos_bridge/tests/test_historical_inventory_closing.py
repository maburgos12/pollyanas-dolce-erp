from datetime import date, datetime, timezone
from decimal import Decimal

from django.db import IntegrityError
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.db import connection

from core.models import Sucursal
from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointInventorySnapshot,
    PointProduct,
    PointProductHistoryRow,
    PointSyncJob,
)
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from pos_bridge.services.branch_inventory_traceability_service import BranchInventoryTraceabilityService
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from recetas.models import Receta


class HistoricalInventoryClosingTests(TestCase):
    def setUp(self):
        self.recipe = Receta.objects.create(
            nombre="Pastel histórico exacto",
            codigo_point="HIST-1",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="pastel-historico-exacto",
        )
        self.product = PointProduct.objects.create(external_id="857", sku="HIST-1", name=self.recipe.nombre)
        self.branches = []
        for index in (1, 2):
            erp = Sucursal.objects.create(codigo=f"HIST-{index}", nombre=f"Histórica {index}")
            self.branches.append(PointBranch.objects.create(
                external_id=str(index), name=erp.nombre, erp_branch=erp,
            ))

    def closing(self, *, status=PointHistoricalInventoryClosing.STATUS_VERIFIED):
        return PointHistoricalInventoryClosing.objects.create(
            operational_date=date(2026, 7, 31),
            status=status,
            source=PointHistoricalInventoryClosing.SOURCE_OFFICIAL_REPORT,
            source_fingerprint="a" * 64,
            expected_branch_ids=[branch.id for branch in self.branches],
            expected_product_ids=[self.product.id],
        )

    def test_verified_closing_is_a_separate_exact_date_source_for_month_opening(self):
        closing = self.closing()
        for branch, stock in zip(self.branches, (Decimal("3"), Decimal("2"))):
            PointHistoricalInventoryClosingLine.objects.create(
                closing=closing,
                branch=branch,
                product=self.product,
                stock=stock,
                evidence={"method": "anchored_stock_history"},
            )

        values, meta, unresolved = MonthlyPointProductBalanceService()._load_opening(
            snapshot_date=date(2026, 7, 31)
        )

        self.assertEqual(PointInventorySnapshot.objects.count(), 0)
        self.assertEqual(values[self.recipe.id], (Decimal("5"), 2))
        self.assertEqual(meta["source"], "PointHistoricalInventoryClosing")
        self.assertEqual(meta["effective_date"], date(2026, 7, 31))
        self.assertTrue(meta["authoritative"])
        self.assertEqual(unresolved, [])

    def test_draft_or_incomplete_closing_is_not_used_as_authoritative_inventory(self):
        closing = self.closing(status=PointHistoricalInventoryClosing.STATUS_DRAFT)
        PointHistoricalInventoryClosingLine.objects.create(
            closing=closing, branch=self.branches[0], product=self.product, stock=Decimal("3")
        )

        values, meta, _ = MonthlyPointProductBalanceService()._load_opening(
            snapshot_date=date(2026, 7, 31)
        )

        self.assertEqual(values, {})
        self.assertFalse(meta["source_present"])

    def test_branch_product_line_is_unique_inside_a_closing(self):
        closing = self.closing()
        PointHistoricalInventoryClosingLine.objects.create(
            closing=closing, branch=self.branches[0], product=self.product, stock=Decimal("3")
        )
        with self.assertRaises(IntegrityError):
            PointHistoricalInventoryClosingLine.objects.create(
                closing=closing, branch=self.branches[0], product=self.product, stock=Decimal("4")
            )

    def stock_boundaries(self, *, fetched_at="2026-10-02T07:00:00+00:00", broken=False):
        branch = self.branches[0]
        closings = []
        for day, stock in ((date(2026, 8, 31), "10"), (date(2026, 9, 30), "5")):
            closing = PointHistoricalInventoryClosing.objects.create(
                operational_date=day, status="VERIFIED",
                source=PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY,
                source_fingerprint=day.isoformat(), expected_branch_ids=[branch.pk],
                expected_product_ids=[self.product.pk],
            )
            PointHistoricalInventoryClosingLine.objects.create(
                closing=closing, branch=branch, product=self.product, stock=stock,
                evidence={"method": "anchored_stock_history", "original": "preserve"},
            )
            closings.append(closing)
        service = AuditStockHistoryService()
        record = service._canonical_import(branch, self.product)
        record.raw_metadata = {"source": "POINT_STOCK_HISTORY_API", "fetched_rows": 2,
            "history_limit": 500, "fetched_at": fetched_at}
        record.save()
        for movement_id, stamp, quantity, previous, new in (
            (101, "2026-09-01T08:00:00", 1, 7, 6),
            (102, "2026-10-01T01:00:00", 5, 9 if broken else 6, 4 if broken else 1),
        ):
            raw = {"FK_Movimiento": movement_id, "Movimiento": "VENTA", "Fecha": stamp,
                "Cantidad": quantity, "Existencia_anterior": previous, "Existencia_nueva": new,
                "Cancelado": False}
            row_id, defaults = service._parse_row(raw)
            PointProductHistoryRow.objects.create(import_record=record, row_number=row_id, **defaults)
        return closings

    def test_stock_boundaries_are_reinterpreted_from_complete_raw_utc_without_writes(self):
        opening, closing = self.stock_boundaries()
        service = MonthlyPointProductBalanceService()
        values, meta, missing = service._load_historical_closing(
            snapshot_date=date(2026, 8, 31), source="opening_snapshot")
        self.assertEqual(values[self.recipe.pk], (Decimal("7"), 1))
        self.assertTrue(meta["authoritative"])
        self.assertEqual(missing, [])
        values, meta, missing = service._load_historical_closing(
            snapshot_date=date(2026, 9, 30), source="closing_snapshot")
        self.assertEqual(values[self.recipe.pk], (Decimal("1"), 1))
        self.assertEqual(meta["historical_boundary_evidence"][0]["movement_ids"], (101, 102))
        branch_service = BranchInventoryTraceabilityService()
        key = (self.branches[0].pk, self.product.pk)
        self.assertEqual(branch_service._load_closing(opening, month=date(2026, 9, 1), boundary="opening")[key][0], Decimal("7"))
        self.assertEqual(branch_service._load_closing(closing)[key][0], Decimal("1"))
        self.assertEqual(list(opening.lines.values_list("stock", flat=True)), [Decimal("10")])
        self.assertEqual(list(closing.lines.values_list("stock", flat=True)), [Decimal("5")])
        self.assertEqual(opening.lines.get().evidence["original"], "preserve")

    def test_stock_unproven_coverage_never_uses_legacy_quantity_or_snapshot_fallback(self):
        opening, _ = self.stock_boundaries(fetched_at="2026-10-01T01:52:00+00:00")
        PointInventorySnapshot.objects.create(branch=self.branches[0], product=self.product,
            stock="999", captured_at=datetime(2026, 8, 31, 19, tzinfo=timezone.utc),
            sync_job=PointSyncJob.objects.create(job_type="INVENTORY"))
        values, meta, missing = MonthlyPointProductBalanceService()._load_month_end_closing(
            snapshot_date=date(2026, 8, 31), source="opening_snapshot")
        self.assertEqual(values, {})
        self.assertFalse(meta["authoritative"])
        self.assertEqual(missing[0].issue, "SOURCE_INCOMPLETE")
        service = BranchInventoryTraceabilityService()
        self.assertEqual(service._load_closing(opening, month=date(2026, 9, 1), boundary="opening"), {})

    def test_discontinuous_stock_history_does_not_grant_documentary_boundary(self):
        self.stock_boundaries(broken=True)
        values, meta, missing = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 9, 30), source="closing_snapshot")
        self.assertEqual(values, {})
        self.assertFalse(meta["authoritative"])
        self.assertEqual(missing[0].issue, "SOURCE_INCOMPLETE")

    def test_two_boundary_reads_reuse_bulk_history_and_preserve_original_evidence(self):
        opening, closing = self.stock_boundaries()
        service = BranchInventoryTraceabilityService()
        with CaptureQueriesContext(connection) as first:
            service._load_closing(opening, month=date(2026, 9, 1), boundary="opening")
        with CaptureQueriesContext(connection) as second:
            service._load_closing(closing, month=date(2026, 9, 1), boundary="closing")
        self.assertEqual(len(first), 3)  # closing lines + two bulk canonical history reads
        self.assertEqual(len(second), 1)  # no repeated history / per-line queries
        proof = service._historical_boundary_evidence[(self.branches[0].pk, self.product.pk)]
        self.assertEqual(proof["opening"]["original_stock"], "10.000")
        self.assertEqual(Decimal(proof["closing"]["effective_stock"]), Decimal("1"))

    def test_empty_stock_history_zero_requires_explicit_original_zero_evidence(self):
        opening, _ = self.stock_boundaries()
        line = opening.lines.get()
        record = AuditStockHistoryService()._existing_import(self.branches[0], self.product)
        record.rows.all().delete()
        record.raw_metadata["fetched_rows"] = 0
        record.save()
        line.stock = Decimal("0")
        line.evidence = {}
        line.save()
        values, meta, _ = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 8, 31), source="opening_snapshot")
        self.assertEqual(values, {})
        self.assertFalse(meta["authoritative"])
        line.evidence = {"method": "no_history_current_zero"}
        line.save()
        values, meta, missing = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 8, 31), source="opening_snapshot")
        self.assertEqual(values[self.recipe.pk], (Decimal("0"), 1))
        self.assertTrue(meta["authoritative"])
        self.assertEqual(missing, [])

    def test_nonzero_stock_without_month_events_is_documented_by_exact_old_capture(self):
        opening, closing = self.stock_boundaries()
        record = AuditStockHistoryService()._existing_import(self.branches[0], self.product)
        record.rows.all().delete()
        raw = {"FK_Movimiento": 301, "Movimiento": "ENTRADA", "Fecha": "2025-12-01T08:00:00",
            "Cantidad": 4, "Existencia_anterior": 0, "Existencia_nueva": 4, "Cancelado": False}
        row_id, defaults = AuditStockHistoryService._parse_row(raw)
        PointProductHistoryRow.objects.create(import_record=record, row_number=row_id, **defaults)
        record.raw_metadata.update({"fetched_rows": 1, "fetched_movement_ids": [301]})
        record.save()
        service = BranchInventoryTraceabilityService()
        key = (self.branches[0].pk, self.product.pk)
        with self.assertNumQueries(5):
            self.assertEqual({key: value[0] for key, value in service._load_closing(
                opening, month=date(2026, 9, 1), boundary="opening").items()}, {key: Decimal("4")})
        with self.assertNumQueries(1):
            self.assertEqual({key: value[0] for key, value in service._load_closing(
                closing, month=date(2026, 9, 1)).items()}, {key: Decimal("4")})
        proof = service._historical_boundary_evidence[key]
        self.assertEqual(proof["closing"]["movement_ids"], (301,))
        self.assertEqual(proof["closing"]["original_stock"], "5.000")
        # Legacy full batches without IDs are usable only when all canonical
        # rows are exactly that complete, sub-limit batch.
        record.raw_metadata.pop("fetched_movement_ids")
        record.row_count = 1
        record.save()
        values, meta, _ = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 9, 30), source="closing_snapshot")
        self.assertEqual(values, {self.recipe.pk: (Decimal("4"), 1)})
        self.assertTrue(meta["authoritative"])
        record.row_count = 2
        record.save()
        values, meta, _ = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 9, 30), source="closing_snapshot")
        self.assertEqual(values, {})
        self.assertFalse(meta["authoritative"])

    def test_retained_old_row_is_not_a_boundary_of_a_different_latest_capture(self):
        opening, _ = self.stock_boundaries()
        record = AuditStockHistoryService()._existing_import(self.branches[0], self.product)
        record.rows.all().delete()
        for movement_id, stamp, previous, new in (
            (301, "2025-12-01T08:00:00", 0, 4),
            (302, "2026-10-03T08:00:00", 9, 10),
        ):
            raw = {"FK_Movimiento": movement_id, "Movimiento": "ENTRADA", "Fecha": stamp,
                "Cantidad": new - previous, "Existencia_anterior": previous,
                "Existencia_nueva": new, "Cancelado": False}
            row_id, defaults = AuditStockHistoryService._parse_row(raw)
            PointProductHistoryRow.objects.create(import_record=record, row_number=row_id, **defaults)
        record.raw_metadata.update({"fetched_rows": 1, "fetched_movement_ids": [302],
            "fetched_at": "2026-10-04T07:00:00+00:00"})
        record.save()
        values, meta, missing = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 8, 31), source="opening_snapshot")
        self.assertEqual(values, {self.recipe.pk: (Decimal("9"), 1)})
        self.assertTrue(meta["authoritative"])
        self.assertEqual(missing, [])
        self.assertEqual(meta["historical_boundary_evidence"][0]["movement_ids"], (302,))
        record.raw_metadata["fetched_at"] = "2026-10-02T07:00:00+00:00"
        record.save()
        values, meta, _ = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 8, 31), source="opening_snapshot")
        self.assertEqual(values, {})
        self.assertFalse(meta["authoritative"])
        # A claimed latest batch with missing IDs cannot borrow retained rows.
        record.raw_metadata.update({"fetched_rows": 500, "fetched_movement_ids": [301],
            "earliest_movement_at": "2025-12-01T15:00:00+00:00"})
        record.save()
        values, meta, missing = MonthlyPointProductBalanceService()._load_historical_closing(
            snapshot_date=date(2026, 8, 31), source="opening_snapshot")
        self.assertEqual(values, {})
        self.assertFalse(meta["authoritative"])
        self.assertEqual(missing[0].issue, "SOURCE_INCOMPLETE")
