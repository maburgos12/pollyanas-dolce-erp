from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointProduct,
)
from pos_bridge.services.branch_inventory_traceability_service import (
    BranchInventoryTraceabilityService,
)


class BranchInventoryTraceabilityServiceTests(TestCase):
    def setUp(self):
        self.service = BranchInventoryTraceabilityService()
        self.centro = PointBranch.objects.create(external_id="CENTRO", name="Centro")
        self.plaza = PointBranch.objects.create(external_id="PLAZA", name="Plaza")
        self.product = PointProduct.objects.create(
            external_id="PASTEL-001",
            sku="PASTEL-001",
            name="Pastel de prueba",
        )

    def _closing(
        self,
        operational_date,
        stocks,
        *,
        fingerprint_suffix="",
        expected_branch_ids=None,
        expected_product_ids=None,
    ):
        return self._closing_lines(
            operational_date,
            {(branch, self.product): stock for branch, stock in stocks.items()},
            fingerprint_suffix=fingerprint_suffix,
            expected_branch_ids=expected_branch_ids,
            expected_product_ids=expected_product_ids,
        )

    def _closing_lines(
        self,
        operational_date,
        stocks,
        *,
        fingerprint_suffix="",
        expected_branch_ids=None,
        expected_product_ids=None,
    ):
        closing = PointHistoricalInventoryClosing.objects.create(
            operational_date=operational_date,
            status=PointHistoricalInventoryClosing.STATUS_VERIFIED,
            source=PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY,
            source_fingerprint=f"closing-{operational_date.isoformat()}{fingerprint_suffix}",
            expected_branch_ids=(
                sorted({branch.id for branch, _product in stocks})
                if expected_branch_ids is None
                else expected_branch_ids
            ),
            expected_product_ids=(
                sorted({product.id for _branch, product in stocks})
                if expected_product_ids is None
                else expected_product_ids
            ),
        )
        for (branch, product), stock in stocks.items():
            PointHistoricalInventoryClosingLine.objects.create(
                closing=closing,
                branch=branch,
                product=product,
                stock=stock,
            )
        return closing

    def test_latest_verified_closing_is_selected_without_summing_older_batches(self):
        older = self._closing(
            date(2026, 7, 31),
            {self.centro: Decimal("4")},
            fingerprint_suffix="-older",
        )
        latest = self._closing(
            date(2026, 7, 31),
            {self.centro: Decimal("10")},
            fingerprint_suffix="-latest",
        )
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})

        result = self.service.build(month=date(2026, 8, 1))

        line = result.lines[0]
        self.assertEqual(line.opening, Decimal("10"))
        self.assertEqual(line.difference, Decimal("0"))
        self.assertEqual(
            line.source_trace["opening"],
            tuple(latest.lines.values_list("id", flat=True)),
        )
        self.assertNotIn(older.lines.get().id, line.source_trace["opening"])

    def test_incomplete_opening_manifest_stops_calculation(self):
        self._closing(
            date(2026, 7, 31),
            {self.centro: Decimal("4")},
            expected_branch_ids=[self.centro.id, self.plaza.id],
        )
        self._closing(date(2026, 8, 31), {self.centro: Decimal("4")})

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        self.assertIn("2026-07-31", result.global_issues[0].message)

    def test_incomplete_closing_manifest_stops_calculation(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("4")})
        self._closing(
            date(2026, 8, 31),
            {self.centro: Decimal("4")},
            expected_product_ids=[self.product.id, self.product.id + 999],
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        self.assertIn("2026-08-31", result.global_issues[0].message)

    def test_equal_company_total_does_not_hide_branch_differences(self):
        self._closing(
            date(2026, 7, 31),
            {self.centro: Decimal("10"), self.plaza: Decimal("10")},
        )
        self._closing(
            date(2026, 8, 31),
            {self.centro: Decimal("9"), self.plaza: Decimal("11")},
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {
            (line.branch.external_id, line.product.external_id): line
            for line in result.lines
        }
        self.assertEqual(by_branch[("CENTRO", "PASTEL-001")].difference, Decimal("-1"))
        self.assertEqual(by_branch[("PLAZA", "PASTEL-001")].difference, Decimal("1"))
        self.assertEqual(result.company_difference, Decimal("0"))
        self.assertEqual(result.exception_count, 2)

    def test_build_normalizes_month_to_first_day(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("3")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("3")})

        result = self.service.build(month=date(2026, 8, 19))

        self.assertEqual(result.month, date(2026, 8, 1))

    def test_only_verified_closings_on_exact_operational_dates_are_accepted(self):
        self._closing(date(2026, 7, 30), {self.centro: Decimal("3")})
        draft = self._closing(date(2026, 8, 31), {self.centro: Decimal("3")})
        draft.status = PointHistoricalInventoryClosing.STATUS_DRAFT
        draft.save(update_fields=["status"])

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        self.assertEqual(
            [issue.code for issue in result.global_issues],
            ["SOURCE_INCOMPLETE", "SOURCE_INCOMPLETE"],
        )
        self.assertIn("2026-07-31", result.global_issues[0].message)
        self.assertIn("2026-08-31", result.global_issues[1].message)

    def test_missing_required_close_stops_before_loading_balances(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("3")})

        with patch.object(self.service, "_load_closing") as load_closing:
            result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        self.assertEqual(result.company_difference, Decimal("0"))
        self.assertEqual(result.exception_count, 0)
        load_closing.assert_not_called()

    def test_decimal_values_and_closing_line_evidence_are_preserved(self):
        opening = self._closing(date(2026, 7, 31), {self.centro: Decimal("1.125")})
        closing = self._closing(date(2026, 8, 31), {self.centro: Decimal("2.375")})

        result = self.service.build(month=date(2026, 8, 1))

        line = result.lines[0]
        self.assertEqual(line.opening, Decimal("1.125"))
        self.assertEqual(line.point_closing, Decimal("2.375"))
        self.assertEqual(line.expected_closing, Decimal("1.125"))
        self.assertEqual(line.difference, Decimal("1.250"))
        self.assertEqual(
            line.source_trace,
            {
                "opening": tuple(opening.lines.values_list("id", flat=True)),
                "closing": tuple(closing.lines.values_list("id", flat=True)),
            },
        )

    def test_source_trace_rejects_runtime_mutation(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("1")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("1")})

        line = self.service.build(month=date(2026, 8, 1)).lines[0]

        with self.assertRaises(TypeError):
            line.source_trace["opening"] = ()

    def test_key_universe_keeps_opening_only_and_closing_only_products(self):
        closing_only_product = PointProduct.objects.create(
            external_id="PASTEL-002",
            sku="PASTEL-002",
            name="Pastel sólo cierre",
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("2")})
        self._closing_lines(
            date(2026, 8, 31),
            {(self.centro, closing_only_product): Decimal("5")},
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_product = {line.product.external_id: line for line in result.lines}
        self.assertEqual(by_product["PASTEL-001"].opening, Decimal("2"))
        self.assertEqual(by_product["PASTEL-001"].point_closing, Decimal("0"))
        self.assertEqual(by_product["PASTEL-001"].difference, Decimal("-2"))
        self.assertEqual(by_product["PASTEL-002"].opening, Decimal("0"))
        self.assertEqual(by_product["PASTEL-002"].point_closing, Decimal("5"))
        self.assertEqual(by_product["PASTEL-002"].difference, Decimal("5"))

    def test_multiple_products_remain_isolated_by_location(self):
        second_product = PointProduct.objects.create(
            external_id="PASTEL-002",
            sku="PASTEL-002",
            name="Pastel adicional",
        )
        opening_stocks = {
            (self.centro, self.product): Decimal("10"),
            (self.centro, second_product): Decimal("20"),
            (self.plaza, self.product): Decimal("30"),
            (self.plaza, second_product): Decimal("40"),
        }
        closing_stocks = {
            key: stock + Decimal("1") for key, stock in opening_stocks.items()
        }
        self._closing_lines(date(2026, 7, 31), opening_stocks)
        self._closing_lines(date(2026, 8, 31), closing_stocks)

        result = self.service.build(month=date(2026, 8, 1))

        self.assertEqual(len(result.lines), 4)
        self.assertTrue(all(line.difference == Decimal("1") for line in result.lines))
        self.assertEqual(result.company_difference, Decimal("4"))
        self.assertEqual(result.exception_count, 4)

    def test_query_count_does_not_grow_with_product_location_lines(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("1")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("1")})
        with CaptureQueriesContext(connection) as baseline_queries:
            baseline = self.service.build(month=date(2026, 8, 1))

        extra_products = [
            PointProduct.objects.create(
                external_id=f"PASTEL-{number:03d}",
                sku=f"PASTEL-{number:03d}",
                name=f"Pastel {number}",
            )
            for number in range(2, 8)
        ]
        products = [self.product, *extra_products]
        stocks = {
            (branch, product): Decimal("1")
            for branch in (self.centro, self.plaza)
            for product in products
        }
        self._closing_lines(date(2026, 7, 31), stocks, fingerprint_suffix="-expanded")
        self._closing_lines(date(2026, 8, 31), stocks, fingerprint_suffix="-expanded")

        with CaptureQueriesContext(connection) as expanded_queries:
            expanded = self.service.build(month=date(2026, 8, 1))

        self.assertEqual(len(baseline.lines), 1)
        self.assertEqual(len(expanded.lines), 14)
        self.assertEqual(len(expanded_queries), len(baseline_queries))
        self.assertLessEqual(len(expanded_queries), 6)
