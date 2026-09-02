from datetime import date, datetime
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from django.db import connection, connections, transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from pos_bridge.services.product_month_closure_service import ProductMonthClosureService, ProductMonthClosureError
from pos_bridge.services.product_month_source_mutex import PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE
from recetas.models import ProductoMonthClosure, ProductoMonthClosureLine, Receta


@override_settings(TIME_ZONE="America/Mazatlan", PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS=3)
class MonthlyOperationalDateTests(TestCase):
    def setUp(self):
        self.recipe = Receta.objects.create(nombre="Producto cierre exacto", codigo_point="EXACTO", hash_contenido="exacto")
        self.product = PointProduct.objects.create(external_id="EXACTO", sku="EXACTO", name=self.recipe.nombre)
        self.branch = PointBranch.objects.create(
            external_id="EXACTA", name="Sucursal exacta",
            erp_branch=Sucursal.objects.create(codigo="EXACTA", nombre="Sucursal exacta"),
        )
        self.job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)

    def snapshot(self, when, stock):
        return PointInventorySnapshot.objects.create(
            product=self.product, branch=self.branch, sync_job=self.job,
            captured_at=timezone.make_aware(when), stock=stock,
        )

    def previous_close(self, effective_date="2026-07-31", contract="POINT_PRODUCT_BALANCE_V1"):
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2026, 7, 1), month_end=date(2026, 7, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            built_at=timezone.make_aware(datetime(2026, 8, 1, 1)),
            metadata={
                "balance": {"contract": contract, "closing_by_recipe": {
                    str(self.recipe.pk): {"quantity": "5", "snapshot_rows": 1},
                }},
                "closing_inventory_meta": {"effective_date": effective_date, "authoritative": True},
            },
        )
        ProductoMonthClosureLine.objects.create(
            closure=closure, receta_padre=self.recipe,
            inventario_final_point_total=Decimal("20"), source_closing_snapshot_count=1,
        )
        return closure

    def test_nearby_august_capture_is_not_july_closing(self):
        for when in (datetime(2026, 7, 30, 23), datetime(2026, 8, 1, 0), datetime(2026, 8, 3, 12)):
            self.snapshot(when, 99)
        self.snapshot(datetime(2026, 8, 31, 23, 8), 22)
        balance = MonthlyPointProductBalanceService().build("2026-08")
        self.assertIsNone(balance.rows[self.recipe.pk].opening_point)
        self.assertIsNone(balance.effective_snapshot_dates["opening"])
        self.assertEqual(balance.rows[self.recipe.pk].closing_point, Decimal("22"))

    def test_month_ends_are_exact_across_year_and_leap_month(self):
        for month, before, end, later in (
            ("2026-01", datetime(2025, 12, 30, 23), datetime(2026, 1, 31, 23), datetime(2026, 2, 1, 0)),
            ("2024-03", datetime(2024, 2, 28, 23), datetime(2024, 3, 31, 23), datetime(2024, 4, 1, 0)),
        ):
            with self.subTest(month=month):
                self.snapshot(before, 99)
                self.snapshot(end, 22)
                self.snapshot(later, 88)
                balance = MonthlyPointProductBalanceService().build(month)
                self.assertIsNone(balance.rows[self.recipe.pk].opening_point)
                self.assertEqual(balance.rows[self.recipe.pk].closing_point, Decimal("22"))

    def test_exact_previous_close_is_carried_despite_next_day_generation(self):
        closure = self.previous_close()
        self.snapshot(datetime(2026, 7, 31, 23), 88)
        self.snapshot(datetime(2026, 8, 31, 23), 22)
        balance = MonthlyPointProductBalanceService().build("2026-08")
        self.assertEqual(balance.rows[self.recipe.pk].opening_point, Decimal("5"))
        self.assertEqual(balance.sources["opening_snapshot"]["previous_closure_id"], closure.pk)
        self.assertEqual(balance.effective_snapshot_dates["opening"], date(2026, 7, 31))

    def test_previous_close_from_another_day_is_rejected(self):
        self.previous_close(effective_date="2026-07-20")
        self.snapshot(datetime(2026, 8, 31, 23), 22)
        balance = MonthlyPointProductBalanceService().build("2026-08")
        self.assertIsNone(balance.rows[self.recipe.pk].opening_point)

    def test_carried_close_keeps_unmatched_product_warning_and_known_quantity(self):
        previous = self.previous_close()
        previous.metadata["closing_inventory_meta"]["unresolved_rows"] = 2
        previous.save(update_fields=["metadata"])
        opening, meta, _ = MonthlyPointProductBalanceService()._load_opening(snapshot_date=date(2026, 7, 31))
        self.assertEqual(opening[self.recipe.pk][0], Decimal("5"))
        self.assertEqual(meta["unresolved_rows"], 2)
        self.assertFalse(meta["authoritative"])
        balance = MonthlyPointProductBalanceService().build("2026-08")
        self.assertEqual(balance.source_counts["opening_snapshot_unresolved"], 2)
        self.assertTrue(any("2 registro(s) sin homologar" in warning for warning in balance.warnings))

    def test_legacy_equivalent_units_are_not_carried_as_exact_product_units(self):
        self.previous_close(contract="legacy")
        self.snapshot(datetime(2026, 8, 31, 23), 22)
        balance = MonthlyPointProductBalanceService().build("2026-08")
        self.assertIsNone(balance.rows[self.recipe.pk].opening_point)

    def test_carried_json_coverage_is_hashable_and_not_lost_to_compaction(self):
        previous = self.previous_close()
        key = [self.branch.pk, self.product.pk]
        previous.metadata["balance"]["closing_coverage"] = {
            "applied_branch_ids": [self.branch.pk], "applied_coverage_keys": [key],
        }
        previous.metadata["closing_inventory_meta"].update({
            "applied_branch_ids": [self.branch.pk],
            "applied_coverage_keys": [key],
        })
        previous.save(update_fields=["metadata"])
        self.snapshot(datetime(2026, 8, 31, 23), 22)
        balance = MonthlyPointProductBalanceService().build("2026-08")
        self.assertNotIn("SNAPSHOT_PRODUCT_COVERAGE_INCOMPLETE", balance.issues)
        previous.metadata["closing_inventory_meta"]["applied_coverage_keys"] = {
            "count": 1, "hash": "compacted", "sample": [key],
        }
        previous.save(update_fields=["metadata"])
        balance = MonthlyPointProductBalanceService().build("2026-08")
        self.assertNotIn("SNAPSHOT_PRODUCT_COVERAGE_INCOMPLETE", balance.issues)

    def test_source_fingerprint_ignores_adjacent_days(self):
        self.snapshot(datetime(2026, 8, 31, 23), 22)
        before = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))
        self.snapshot(datetime(2026, 9, 1, 1), 99)
        after = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))
        self.assertEqual(before, after)

    def test_source_fingerprint_includes_previous_close(self):
        previous = self.previous_close()
        before = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))
        previous.metadata["balance"]["closing_by_recipe"][str(self.recipe.pk)]["quantity"] = "6"
        previous.save(update_fields=["metadata"])
        after = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))
        self.assertNotEqual(before, after)


class MonthlyOpeningSourceMutexTests(TransactionTestCase):
    def test_build_protects_previous_month_while_reading_opening(self):
        def competing_writer():
            try:
                with transaction.atomic(), connection.cursor() as cursor:
                    cursor.execute("SELECT pg_try_advisory_xact_lock(%s, %s)", [
                        PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE, 202607,
                    ])
                    return cursor.fetchone()[0]
            finally:
                connections.close_all()

        def inspect_during_preview(**kwargs):
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertFalse(pool.submit(competing_writer).result(timeout=10))
            raise ProductMonthClosureError("stop after read-only contention check")

        service = ProductMonthClosureService()
        with patch.object(service, "preview", side_effect=inspect_during_preview):
            with self.assertRaisesMessage(ProductMonthClosureError, "stop after"):
                service.build(month="2026-08")

    def test_lock_acquires_source_mutex_before_validation(self):
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2026, 8, 1), month_end=date(2026, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
        )
        service = ProductMonthClosureService()
        with patch.object(service, "_lock_canonical_source_month", wraps=service._lock_canonical_source_month) as lock:
            with self.assertRaises(ProductMonthClosureError):
                service.lock(closure=closure)
        lock.assert_called_once_with(date(2026, 8, 1))
