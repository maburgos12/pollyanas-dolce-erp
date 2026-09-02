from datetime import date
from decimal import Decimal

from django.test import TestCase

from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from ventas.services import sales_canonical_source


class PointStageEvidenceReadTests(TestCase):
    def setUp(self):
        self.branch = PointBranch.objects.create(
            external_id="CANONICAL-READ-BRANCH",
            name="Sucursal lectura canónica",
        )
        self.product = PointProduct.objects.create(
            external_id="CANONICAL-READ-PRODUCT",
            name="Producto lectura canónica",
        )

    def _sale(self, *, sale_date: date, source_endpoint: str, quantity: str = "1"):
        return PointDailySale.objects.create(
            branch=self.branch,
            product=self.product,
            sale_date=sale_date,
            quantity=Decimal(quantity),
            source_endpoint=source_endpoint,
        )

    def test_official_stage_read_preserves_range_source_and_legacy_count(self):
        official_reader = getattr(sales_canonical_source, "official_point_sales_rows_for_range", None)
        legacy_counter = getattr(sales_canonical_source, "legacy_point_sales_row_count_for_range", None)
        self.assertIsNotNone(official_reader, "Falta la API canónica para leer ventas oficiales persistidas.")
        self.assertIsNotNone(legacy_counter, "Falta la API canónica para detectar ventas legacy persistidas.")

        expected = self._sale(
            sale_date=date(2026, 8, 15),
            source_endpoint=sales_canonical_source.OFFICIAL_POINT_SOURCE,
            quantity="3",
        )
        self._sale(
            sale_date=date(2026, 8, 16),
            source_endpoint=sales_canonical_source.RECENT_POINT_SOURCE,
        )
        self._sale(
            sale_date=date(2026, 7, 31),
            source_endpoint=sales_canonical_source.OFFICIAL_POINT_SOURCE,
        )

        rows = list(official_reader(start_date=date(2026, 8, 1), end_date=date(2026, 8, 31)))

        self.assertEqual([row.id for row in rows], [expected.id])
        self.assertEqual(
            legacy_counter(start_date=date(2026, 8, 1), end_date=date(2026, 8, 31)),
            1,
        )

    def test_canonical_sales_evidence_months_exposes_point_stage_months(self):
        evidence_months = getattr(sales_canonical_source, "canonical_sales_evidence_months", None)
        self.assertIsNotNone(evidence_months, "Falta la API canónica de meses con evidencia de ventas.")

        self._sale(
            sale_date=date(2026, 7, 15),
            source_endpoint=sales_canonical_source.OFFICIAL_POINT_SOURCE,
        )
        self._sale(
            sale_date=date(2026, 6, 20),
            source_endpoint=sales_canonical_source.RECENT_POINT_SOURCE,
        )

        self.assertEqual(evidence_months(), ("2026-07", "2026-06"))
