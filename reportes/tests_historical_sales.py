from datetime import date
from decimal import Decimal

from django.core.cache import cache
from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from reportes.executive_panels import _sales_fact_daily_map, build_monthly_yoy_panel
from reportes.models import FactVentaDiaria
from ventas.models import VentaAutoritativaPoint


class HistoricalSalesComparisonTests(TestCase):
    def setUp(self):
        cache.clear()
        self.crucero = Sucursal.objects.create(codigo="CRUCERO-HIST", nombre="Crucero", activa=False)
        self.bamoa = Sucursal.objects.create(codigo="BAMOA-HIST", nombre="Bamoa", activa=True)
        self.point_branch = PointBranch.objects.create(external_id="BAMOA-HIST", name="Bamoa", erp_branch=self.bamoa)
        self.product = PointProduct.objects.create(external_id="HIST-116", name="Bollo Chocolate")

    def test_daily_history_keeps_inactive_branch(self):
        day = date(2025, 5, 10)
        FactVentaDiaria.objects.create(fecha=day, sucursal=self.crucero,
            producto_clave="0116", venta_total=Decimal("1000"), cantidad=Decimal("10"))
        self.assertEqual(_sales_fact_daily_map(start_date=day, end_date=day).get(day),
                         (Decimal("1000"), Decimal("10")))
        self.crucero.refresh_from_db()
        self.assertFalse(self.crucero.activa)

    def test_both_historical_years_include_crucero_at_full_and_partial_cutoffs(self):
        for year in (2024, 2025):
            VentaAutoritativaPoint.objects.create(branch=self.crucero, sale_date=date(year, 5, 10),
                product_code="0116", total_amount=Decimal("1000"), quantity=Decimal("10"))
        PointDailySale.objects.create(branch=self.point_branch, product=self.product,
            sale_date=date(2026, 5, 10), total_amount=Decimal("900"), quantity=Decimal("9"),
            source_endpoint="/Report/PrintReportes?idreporte=3")
        for last_day in (15, 31):
            with self.subTest(last_day=last_day):
                row = build_monthly_yoy_panel(latest_date=date(2026, 5, last_day), months=1)["rows"][0]
                self.assertEqual(row["amount"], Decimal("900"))
                self.assertEqual(row["prev_amount"], Decimal("1000"))
                self.assertEqual(row["prev2_amount"], Decimal("1000"))
                self.assertEqual(row["amount_delta_pct"], Decimal("-10"))
        self.assertEqual(VentaAutoritativaPoint.objects.filter(branch=self.crucero).count(), 2)
        self.crucero.refresh_from_db()
        self.assertFalse(self.crucero.activa)

    def test_previous_year_uses_point_instead_of_duplicated_analytic_copy(self):
        branch = self.point_branch
        for year, amount in ((2026, "1000"), (2027, "900")):
            PointDailySale.objects.create(branch=branch, product=self.product,
                sale_date=date(year, 3, 10), total_amount=Decimal(amount), quantity=Decimal("10"),
                source_endpoint="/Report/PrintReportes?idreporte=3")
        for code in ("0116", "116"):
            VentaAutoritativaPoint.objects.create(branch=self.bamoa, sale_date=date(2026, 3, 10),
                product_code=code, total_amount=Decimal("1000"), quantity=Decimal("10"))
            FactVentaDiaria.objects.create(fecha=date(2026, 3, 10), sucursal=self.bamoa,
                producto_clave=code, venta_total=Decimal("1000"), cantidad=Decimal("10"))
        row = build_monthly_yoy_panel(latest_date=date(2027, 3, 31), months=1)["rows"][0]
        self.assertEqual(row["prev_amount"], Decimal("1000"))
        self.assertEqual(row["amount_delta_pct"], Decimal("-10"))
        self.assertEqual(VentaAutoritativaPoint.objects.count(), 2)
