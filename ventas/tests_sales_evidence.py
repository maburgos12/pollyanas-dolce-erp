from datetime import date
from decimal import Decimal
from pathlib import Path

from django.test import SimpleTestCase, TestCase

from orquestacion.services.pointdailysale_guard import (
    is_allowed_pointdailysale_path,
    scan_pointdailysale_usage,
)
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from ventas.services import sales_read_service


class SalesEvidenceTests(TestCase):
    def setUp(self):
        self.branch = PointBranch.objects.create(external_id="evidence", name="Evidence")
        self.product = PointProduct.objects.create(external_id="evidence", name="Evidence")
        self.sales = [
            PointDailySale.objects.create(
                branch=self.branch,
                product=self.product,
                sale_date=date(2026, 8, day),
                quantity=Decimal(quantity),
                source_endpoint=source,
            )
            for day, quantity, source in (
                (1, "4.250", sales_read_service.OFFICIAL_POINT_SOURCE),
                (2, "0", sales_read_service.LEGACY_POINT_SOURCE),
                (3, "-1.500", sales_read_service.OFFICIAL_POINT_SOURCE),
            )
        ]

    def reader(self):
        reader = getattr(sales_read_service, "point_sales_evidence_by_ids", None)
        self.assertIsNotNone(reader, "La evidencia debe leerse mediante el servicio de ventas.")
        return reader

    def test_ids_preserve_evidence_without_source_selection_or_aggregation(self):
        reader = self.reader()
        selected = self.sales[:2]
        ids = [selected[1].pk, selected[0].pk, selected[1].pk, 999999]
        with self.assertNumQueries(1):
            actual = {
                row.pk: (row.sale_date, row.quantity, row.branch.name, row.source_endpoint)
                for row in reader(sale_ids=ids)
            }
        self.assertEqual(actual, {
            row.pk: (row.sale_date, row.quantity, self.branch.name, row.source_endpoint)
            for row in selected
        })

    def test_empty_ids_do_not_read_unrelated_sales(self):
        reader = self.reader()
        with self.assertNumQueries(0):
            self.assertEqual(list(reader(sale_ids=[])), [])

    def test_negative_quantity_is_preserved(self):
        reader = self.reader()
        self.assertEqual(
            [row.quantity for row in reader(sale_ids=[self.sales[2].pk])],
            [Decimal("-1.500")],
        )


class InventoryEvidenceArchitectureTests(SimpleTestCase):
    def test_inventory_view_uses_the_sales_boundary_without_an_allowlist_exception(self):
        view_path = "reportes/views_inventory_traceability.py"
        self.assertFalse(is_allowed_pointdailysale_path(view_path))
        result = scan_pointdailysale_usage(base_dir=Path(__file__).resolve().parents[1])
        self.assertEqual(
            [item for item in result.violations if item.relative_path == view_path], []
        )
