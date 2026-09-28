from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase

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

    def _closing(self, operational_date, stocks):
        closing = PointHistoricalInventoryClosing.objects.create(
            operational_date=operational_date,
            status=PointHistoricalInventoryClosing.STATUS_VERIFIED,
            source=PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY,
            source_fingerprint=f"closing-{operational_date.isoformat()}",
            expected_branch_ids=[self.centro.id, self.plaza.id],
            expected_product_ids=[self.product.id],
        )
        for branch, stock in stocks.items():
            PointHistoricalInventoryClosingLine.objects.create(
                closing=closing,
                branch=branch,
                product=self.product,
                stock=stock,
            )
        return closing

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
