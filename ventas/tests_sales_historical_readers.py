from datetime import date
from decimal import Decimal

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from reportes.services_reparto_historico import ventas_con_identidad_historica
from ventas.services.sales_read_service import (
    point_sales_evidence_months,
    point_sales_with_historical_branch_identity,
)


class HistoricalPointSalesReadersTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.crucero = Sucursal.objects.create(codigo="CRUCERO", nombre="Crucero", activa=False)
        cls.bamoa = Sucursal.objects.create(codigo="BAMOA", nombre="Bamoa")
        cls.point = PointBranch.objects.create(external_id="2", name="Point 2", erp_branch=cls.bamoa)
        cls.other = PointBranch.objects.create(external_id="other", name="Other", erp_branch=cls.bamoa)
        cls.unmapped = PointBranch.objects.create(external_id="unmapped", name="Unmapped")
        cls.product = PointProduct.objects.create(external_id="historical-reader", name="Pastel")

    def sale(self, day, *, branch=None, quantity="-2.500", gross="-15.50", source="historical-test"):
        return PointDailySale.objects.create(
            branch=branch or self.point, product=self.product, sale_date=day,
            quantity=Decimal(quantity), gross_amount=Decimal(gross),
            discount_amount=Decimal("-1.25"), total_amount=Decimal("-14.25"),
            net_amount=Decimal("-12.00"), tax_amount=Decimal("-2.25"),
            source_endpoint=source,
        )

    def test_identity_date_boundaries_preserve_all_values_and_current_mapping(self):
        days = [date(2025, 12, 31), date(2026, 1, 1), date(2026, 6, 30), date(2026, 7, 1), date(2027, 1, 1)]
        for day in days:
            self.sale(day)
        other = self.sale(date(2026, 1, 1), branch=self.other, quantity="0", gross="0")
        unmapped = self.sale(date(2026, 6, 30), branch=self.unmapped)
        before = list(PointDailySale.objects.order_by("pk").values())
        branches_before = list(PointBranch.objects.order_by("pk").values())
        with CaptureQueriesContext(connection) as queries:
            rows = list(point_sales_with_historical_branch_identity(
                start_date=days[0], end_date=days[-1],
            ).order_by("pk").values())
        self.assertTrue(all(q["sql"].lstrip().upper().startswith("SELECT") for q in queries))
        for row in rows:
            historical = row.pop("sucursal_historica_id")
            expected = self.bamoa.pk
            if row["branch_id"] == self.point.pk and date(2026, 1, 1) <= row["sale_date"] <= date(2026, 6, 30):
                expected = self.crucero.pk
            elif row["id"] == unmapped.pk:
                expected = None
            self.assertEqual(historical, expected)
        self.assertEqual(rows, before)
        self.assertEqual(before, list(PointDailySale.objects.order_by("pk").values()))
        self.assertEqual(branches_before, list(PointBranch.objects.order_by("pk").values()))
        self.assertIn(other.pk, [row["id"] for row in rows])

    def test_public_report_reader_preserves_inclusive_range(self):
        self.sale(date(2025, 12, 31))
        first = self.sale(date(2026, 1, 1))
        last = self.sale(date(2026, 6, 30))
        self.sale(date(2026, 7, 1))
        actual = list(ventas_con_identidad_historica(date(2026, 1, 1), date(2026, 6, 30))
                      .order_by("sale_date").values_list("pk", "sucursal_historica_id"))
        self.assertEqual(actual, [(first.pk, self.crucero.pk), (last.pk, self.crucero.pk)])

    def test_missing_crucero_is_null_without_falling_back_to_current_branch(self):
        self.crucero.delete()
        historical = self.sale(date(2026, 6, 30))
        current = self.sale(date(2026, 7, 1))
        actual = dict(point_sales_with_historical_branch_identity(
            start_date=date(2026, 6, 30), end_date=date(2026, 7, 1),
        ).values_list("pk", "sucursal_historica_id"))
        self.assertEqual(actual, {historical.pk: None, current.pk: self.bamoa.pk})

    def test_evidence_months_include_zero_negative_unmapped_and_all_sources(self):
        for day in [date(2025, 12, 31), date(2026, 1, 31), date(2026, 2, 1), date(2026, 2, 28), date(2026, 3, 1), date(2026, 4, 1)]:
            self.sale(day, branch=self.unmapped, quantity="0", gross="0", source="/unrecognized-source")
        self.sale(date(2026, 2, 15), source="/Report/PrintReportes?idreporte=3")
        before = list(PointDailySale.objects.order_by("pk").values())
        months = list(point_sales_evidence_months(start_date=date(2026, 1, 31), end_date=date(2026, 3, 1)))
        self.assertEqual(months, [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)])
        self.assertEqual(list(point_sales_evidence_months(start_date=date(2026, 3, 2), end_date=date(2026, 3, 31))), [])
        self.assertEqual(before, list(PointDailySale.objects.order_by("pk").values()))
