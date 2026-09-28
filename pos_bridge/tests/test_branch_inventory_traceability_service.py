from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointDailySale,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointProduct,
    PointProductionLine,
    PointWasteLine,
)
from pos_bridge.services.branch_inventory_traceability_service import (
    BranchInventoryTraceabilityService,
)
from ventas.services.sales_canonical_source import OFFICIAL_POINT_SOURCE


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

    def _sale(self, *, branch=None, product=None, quantity="1"):
        return PointDailySale.objects.create(
            branch=branch or self.centro,
            product=product or self.product,
            sale_date=date(2026, 8, PointDailySale.objects.count() + 1),
            quantity=Decimal(quantity),
            source_endpoint=OFFICIAL_POINT_SOURCE,
        )

    def _production(self, *, branch=None, item_code=None, item_name=None, quantity="1"):
        return PointProductionLine.objects.create(
            branch=branch or self.centro,
            production_external_id=f"production-{PointProductionLine.objects.count() + 1}",
            detail_external_id=f"detail-{PointProductionLine.objects.count() + 1}",
            source_hash=f"production-hash-{PointProductionLine.objects.count() + 1}",
            production_date=date(2026, 8, 11),
            item_code=self.product.external_id if item_code is None else item_code,
            item_name=self.product.name if item_name is None else item_name,
            produced_quantity=Decimal(quantity),
        )

    def _waste(
        self,
        *,
        branch=None,
        item_code=None,
        item_name=None,
        quantity="1",
        responsible="",
    ):
        sequence = PointWasteLine.objects.count() + 1
        return PointWasteLine.objects.create(
            branch=branch or self.centro,
            movement_external_id=f"waste-{sequence}",
            source_hash=f"waste-hash-{sequence}",
            movement_at=datetime(
                2026,
                8,
                12,
                12,
                0,
                tzinfo=timezone.get_current_timezone(),
            ),
            item_code=self.product.external_id if item_code is None else item_code,
            item_name=self.product.name if item_name is None else item_name,
            quantity=Decimal(quantity),
            responsible=responsible,
        )

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
                "sales": (),
                "production": (),
                "waste": (),
                "transfers": (),
                "conversions": (),
                "adjustments": (),
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

    def test_sale_decreases_only_the_branch_where_it_occurred(self):
        stocks = {
            (self.centro, self.product): Decimal("10"),
            (self.plaza, self.product): Decimal("10"),
        }
        self._closing_lines(date(2026, 7, 31), stocks)
        self._closing_lines(date(2026, 8, 31), stocks)
        sale = self._sale(branch=self.centro, quantity="3")

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in result.lines}
        self.assertEqual(by_branch["CENTRO"].sales, Decimal("3"))
        self.assertEqual(by_branch["CENTRO"].expected_closing, Decimal("7"))
        self.assertEqual(by_branch["PLAZA"].sales, Decimal("0"))
        self.assertEqual(by_branch["PLAZA"].expected_closing, Decimal("10"))
        self.assertEqual(by_branch["CENTRO"].source_trace["sales"], (sale.id,))
        self.assertEqual(by_branch["PLAZA"].source_trace["sales"], ())

    def test_production_increases_only_the_recorded_location(self):
        stocks = {
            (self.centro, self.product): Decimal("10"),
            (self.plaza, self.product): Decimal("10"),
        }
        self._closing_lines(date(2026, 7, 31), stocks)
        self._closing_lines(date(2026, 8, 31), stocks)
        production = self._production(branch=self.plaza, quantity="4")

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in result.lines}
        self.assertEqual(by_branch["CENTRO"].production, Decimal("0"))
        self.assertEqual(by_branch["CENTRO"].expected_closing, Decimal("10"))
        self.assertEqual(by_branch["PLAZA"].production, Decimal("4"))
        self.assertEqual(by_branch["PLAZA"].expected_closing, Decimal("14"))
        self.assertEqual(
            by_branch["PLAZA"].source_trace["production"], (production.id,)
        )

    def test_waste_decreases_recorded_location_regardless_of_responsible_text(self):
        stocks = {
            (self.centro, self.product): Decimal("10"),
            (self.plaza, self.product): Decimal("10"),
        }
        self._closing_lines(date(2026, 7, 31), stocks)
        self._closing_lines(date(2026, 8, 31), stocks)
        waste = self._waste(
            branch=self.plaza,
            quantity="2.5",
            responsible="Vendedora de Centro",
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in result.lines}
        self.assertEqual(by_branch["CENTRO"].waste, Decimal("0"))
        self.assertEqual(by_branch["PLAZA"].waste, Decimal("2.5"))
        self.assertEqual(by_branch["PLAZA"].expected_closing, Decimal("7.5"))
        self.assertEqual(by_branch["PLAZA"].source_trace["waste"], (waste.id,))

    def test_unresolved_product_is_global_issue_and_not_added_to_product_lines(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        unresolved = self._production(
            item_code="UNKNOWN-999",
            item_name="Producto que no existe",
            quantity="6",
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(len(result.lines), 1)
        self.assertEqual(result.lines[0].production, Decimal("0"))
        issue = result.global_issues[0]
        self.assertEqual(issue.code, "UNRESOLVED_PRODUCT")
        self.assertEqual(issue.branch_id, self.centro.id)
        self.assertEqual(issue.source_ids, (unresolved.id,))

    def test_ambiguous_normalized_name_is_explicit_and_not_assigned(self):
        PointProduct.objects.create(
            external_id="PASTEL-DUPLICADO-1",
            sku="DUPLICADO-1",
            name="Pastel Único",
        )
        PointProduct.objects.create(
            external_id="PASTEL-DUPLICADO-2",
            sku="DUPLICADO-2",
            name="  pastel unico  ",
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        ambiguous = self._waste(
            item_code="",
            item_name="PASTEL UNICO",
            quantity="2",
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(len(result.lines), 1)
        self.assertEqual(result.lines[0].waste, Decimal("0"))
        issue = result.global_issues[0]
        self.assertEqual(issue.code, "AMBIGUOUS_PRODUCT")
        self.assertEqual(issue.branch_id, self.centro.id)
        self.assertEqual(issue.source_ids, (ambiguous.id,))

    def test_resolution_precedence_and_movement_only_products_expand_key_universe(self):
        sku_product = PointProduct.objects.create(
            external_id="EXTERNAL-SKU-TARGET",
            sku="ONLY-SKU-MATCH",
            name="Producto por SKU",
        )
        name_product = PointProduct.objects.create(
            external_id="EXTERNAL-NAME-TARGET",
            sku="NAME-TARGET-SKU",
            name="Producto Único por Nombre",
        )
        misleading_name_product = PointProduct.objects.create(
            external_id="MISLEADING-NAME",
            sku="MISLEADING-NAME-SKU",
            name="Nombre que no debe ganar",
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        external_match = self._production(
            item_code=self.product.external_id,
            item_name=misleading_name_product.name,
            quantity="2",
        )
        sku_match = self._waste(
            item_code=sku_product.sku,
            item_name="Nombre sin coincidencia",
            quantity="3",
        )
        name_match = self._production(
            item_code="",
            item_name=" producto unico por nombre ",
            quantity="4",
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_product = {line.product.id: line for line in result.lines}
        self.assertEqual(
            set(by_product), {self.product.id, sku_product.id, name_product.id}
        )
        self.assertEqual(by_product[self.product.id].production, Decimal("2"))
        self.assertEqual(
            by_product[self.product.id].source_trace["production"],
            (external_match.id,),
        )
        self.assertEqual(by_product[sku_product.id].waste, Decimal("3"))
        self.assertEqual(
            by_product[sku_product.id].source_trace["waste"], (sku_match.id,)
        )
        self.assertEqual(by_product[name_product.id].production, Decimal("4"))
        self.assertEqual(
            by_product[name_product.id].source_trace["production"],
            (name_match.id,),
        )
        self.assertNotIn(misleading_name_product.id, by_product)

    def test_direct_source_ids_and_empty_future_sources_are_immutable_tuples(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        sale = self._sale(quantity="2")
        production = self._production(quantity="5")
        waste = self._waste(quantity="1")

        line = self.service.build(month=date(2026, 8, 1)).lines[0]

        self.assertEqual(line.sales, Decimal("2"))
        self.assertEqual(line.production, Decimal("5"))
        self.assertEqual(line.waste, Decimal("1"))
        self.assertEqual(line.expected_closing, Decimal("12"))
        self.assertEqual(line.difference, Decimal("-2"))
        self.assertEqual(line.source_trace["sales"], (sale.id,))
        self.assertEqual(line.source_trace["production"], (production.id,))
        self.assertEqual(line.source_trace["waste"], (waste.id,))
        self.assertEqual(
            set(line.source_trace),
            {
                "opening",
                "closing",
                "sales",
                "production",
                "waste",
                "transfers",
                "conversions",
                "adjustments",
            },
        )
        for source_ids in line.source_trace.values():
            self.assertIsInstance(source_ids, tuple)

    def test_query_count_does_not_grow_with_product_location_lines(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("1")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("1")})
        self._sale()
        self._production()
        self._waste()
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
        for branch in (self.centro, self.plaza):
            for product in products:
                self._sale(branch=branch, product=product)
                self._production(
                    branch=branch,
                    item_code=product.external_id,
                    item_name=product.name,
                )
                self._waste(
                    branch=branch,
                    item_code=product.external_id,
                    item_name=product.name,
                )

        with CaptureQueriesContext(connection) as expanded_queries:
            expanded = self.service.build(month=date(2026, 8, 1))

        self.assertEqual(len(baseline.lines), 1)
        self.assertEqual(len(expanded.lines), 14)
        self.assertEqual(len(expanded_queries), len(baseline_queries))
        self.assertLessEqual(len(expanded_queries), 10)
