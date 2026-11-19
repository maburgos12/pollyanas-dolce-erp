"""Recorded-time comparisons require a complete, consistent original history."""
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext
from pos_bridge.models import PointBranch
from reportes.tests_inventory_audit_agent import InventoryAuditAgentFixtures
from inventario.conteos_conciliacion import cierre_dia
from reportes.services_physical_recorded_time import compare_recorded_time, add_recorded_time_comparisons


class RecordedTimeComparisonTests(SimpleTestCase):
    def setUp(self):
        self.stamp = datetime(2026, 9, 1, 2, tzinfo=timezone.utc)
        self.case = SimpleNamespace(branch_id=1, product_id=2, month=date(2026, 8, 1),
                                    source_trace={"closing": [1]})
        self.record = SimpleNamespace(point_branch_id=1, point_product_id=2)
        self.reading = {"counted_at": self.stamp, "unit": "PZA", "quantity": "10"}
        self.comparison = {"closing": Decimal("9")}
        self.proof = {"response_fingerprint": "a" * 64, "raw_sha256": "b" * 64,
                      "retrieved_at": "2026-09-02T12:00:00+00:00", "original_count": 2, "history_limit": 500}
        self.reconciliation = SimpleNamespace(coverage_status="COMPLETE", unknown_movement_ids=(),
            documentary_closing=Decimal("9"), original_batch_evidence=self.proof)
        self.raw = {"FK_Movimiento": 1, "Fecha": "2026-09-01 03:00:00", "Movimiento": "VENTA",
                    "Cantidad": "1", "Existencia_anterior": "10", "Existencia_nueva": "9", "Cancelado": False, "Unidad": "PZA"}
        self.batch = {"evidence": deepcopy(self.proof), "stock_chain_gaps": [], "unique_rows": [self.raw]}

    def compare(self):
        with patch("reportes.services_physical_recorded_time.AuditStockHistoryService._original_batch", return_value=self.batch):
            return compare_recorded_time(self.case, self.reading, self.comparison,
                self.record, self.reconciliation, cierre_dia(date(2026, 8, 31)))

    def test_sale_after_count_reconstructs_point_at_the_recorded_time(self):
        result = self.compare()
        self.assertEqual(result["point"], Decimal("10"))
        self.assertEqual(result["difference"], Decimal("0"))
        self.assertEqual(result["movement_ids"], [1])
        self.assertEqual(result["variation"], Decimal("-1"))
        self.assertEqual(self.comparison["closing"], Decimal("9"))

    def test_equal_closing_with_later_movement_remains_only_a_reference(self):
        self.comparison["closing"] = Decimal("10")
        self.reconciliation.documentary_closing = Decimal("10")
        self.raw.update(Existencia_anterior="11", Existencia_nueva="10")
        self.assertEqual(self.compare()["difference"], Decimal("-1"))
        case = {"id": 1, "physical_comparison": {"closing": Decimal("10"),
                "delta": Decimal("0"), "summary": "coinciden"},
                "physical_evidence": [{**self.reading, "round": 1}]}
        with patch("reportes.services_physical_recorded_time.AuditStockHistoryService.reconcile_many") as histories:
            add_recorded_time_comparisons([case], date(2026, 8, 31))
        histories.assert_not_called()
        comparison = case["physical_comparison"]
        self.assertNotIn("at_recorded_time", comparison)
        self.assertEqual(comparison["label"], "Coincide con cierre declarado")
        self.assertIn("no se verificó", comparison["provisional"])
        self.assertIn("31/08/2026 (Mazatlán)", comparison["basis"])

    def test_units_require_explicit_original_evidence(self):
        self.raw["Unidad"] = "KG"
        self.assertIn("unidad", self.compare()["reason"])
        del self.raw["Unidad"]
        self.assertIn("unidad", self.compare()["reason"])
        self.raw["Unidad"] = "U"
        self.assertIn("unidad", self.compare()["reason"])
        self.reading["unit"] = "U"
        self.assertEqual(self.compare()["point"], Decimal("10"))
        self.reading["unit"] = "KG"
        self.raw["Unidad"] = "KG"
        self.assertEqual(self.compare()["point"], Decimal("10"))

    def test_zero_is_a_real_quantity(self):
        self.reading["quantity"] = "0"
        self.assertEqual(self.compare()["difference"], Decimal("-10"))

    def test_missing_closing_history_or_identity_cannot_be_accredited(self):
        for field, value in (("branch_id", 7), ("product_id", 7)):
            with self.subTest(field=field):
                old = getattr(self.case, field)
                setattr(self.case, field, value)
                self.assertIn("reason", self.compare())
                setattr(self.case, field, old)
        self.reconciliation.coverage_status = "INCOMPLETE"
        self.assertIn("reason", self.compare())
        self.reconciliation.coverage_status = "COMPLETE"
        self.case.source_trace = {}
        self.assertIn("reason", self.compare())

    def test_history_closing_absent_is_not_a_numerical_contradiction(self):
        self.reconciliation.documentary_closing = None
        self.assertIn("cierre", self.compare()["reason"])
        self.batch["stock_chain_gaps"] = [{"movement_at": "2026-08-22T20:00:00+00:00"}]
        self.assertIn("salto previo", self.compare()["reason"])
        self.batch["stock_chain_gaps"] = []
        self.reconciliation.documentary_closing = Decimal("8")
        self.assertIn("contradice", self.compare()["reason"])

    def test_new_source_receipt_unknown_gaps_or_same_second_fail_closed(self):
        variants = ("fingerprint", "receipt", "unknown", "gap", "same-second", "unit", "future", "month", "truncated", "invalid-effect", "before-month")
        for variant in variants:
            with self.subTest(variant=variant):
                self.setUp()
                if variant == "fingerprint":
                    self.batch["evidence"]["response_fingerprint"] = "c" * 64
                elif variant == "receipt":
                    self.batch["evidence"]["retrieved_at"] = "2026-08-31T01:00:00+00:00"
                elif variant == "unknown":
                    self.reconciliation.unknown_movement_ids = (3,)
                elif variant == "gap":
                    self.batch["stock_chain_gaps"] = [{"movement_at": "2026-09-01T04:00:00+00:00"}]
                elif variant == "same-second":
                    self.raw["Fecha"] = "2026-09-01 02:00:00"
                elif variant == "unit":
                    self.reading["unit"] = "KG"
                elif variant == "month":
                    self.case.month = date(2026, 7, 1)
                elif variant == "truncated":
                    self.batch["evidence"]["original_count"] = 500
                elif variant == "invalid-effect":
                    self.raw["Cantidad"] = "2"
                elif variant == "before-month":
                    self.reading["counted_at"] = datetime(2026, 7, 31, 21, tzinfo=timezone.utc)
                    self.batch["unique_rows"].append({**self.raw, "FK_Movimiento": 2,
                        "Fecha": "2026-07-31 22:00:00", "Unidad": "KG"})
                else:
                    self.reading["counted_at"] = datetime(2026, 10, 1, tzinfo=timezone.utc)
                self.assertIn("reason", self.compare())


class RecordedTimeBulkTests(InventoryAuditAgentFixtures, TestCase):
    def test_missing_histories_use_constant_queries_and_preserve_reference(self):
        first = self.make_case()
        branch = PointBranch.objects.create(external_id="bulk-time-2", name="Otra sucursal")
        second = self.make_case(branch=branch)
        def entry(case):
            return {"id": case.pk, "physical_comparison": {"closing": Decimal("9"),
                    "delta": Decimal("1"), "summary": "con diferencia"}, "physical_evidence": [{
                    "quantity": "10", "round": 1, "counted_at": datetime(2026, 8, 31, 23, tzinfo=timezone.utc),
                    "unit": "PZA"}]}
        query_counts = []
        for cases in ([entry(first)], [entry(first), entry(second)]):
            with CaptureQueriesContext(connection) as queries:
                add_recorded_time_comparisons(cases, date(2026, 8, 31))
            query_counts.append(len(queries))
            for case in cases:
                self.assertEqual(case["physical_comparison"]["delta"], Decimal("1"))
                self.assertIn("Falta historial original", case["physical_comparison"]["provisional"])
                self.assertNotIn("at_recorded_time", case["physical_comparison"])
        self.assertEqual(query_counts[0], query_counts[1])
        self.assertLessEqual(query_counts[0], 3)
