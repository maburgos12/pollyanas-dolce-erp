from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct, PointSyncJob
from pos_bridge.services.sales_extractor import ExtractedBranchDailySales
from pos_bridge.services.sync_service import PointSyncService
from recetas.models import Receta, VentaHistorica


class FakeSalesExtractor:
    def iter_extract(self, *, start_date, end_date, branch_filter=None, excluded_ranges=None, max_days=None):
        del start_date, end_date, branch_filter, excluded_ranges, max_days
        yield ExtractedBranchDailySales(
            branch={"external_id": "9", "name": "Almacen", "status": "ACTIVE", "metadata": {}},
            sale_date=date(2025, 12, 31),
            sales_rows=[
                {
                    "external_id": "2",
                    "sku": "0002",
                    "name": "Pay de Queso Mediano",
                    "category": "Pay Mediano",
                    "family": "Pay",
                    "quantity": Decimal("40"),
                    "tickets": 0,
                    "gross_amount": Decimal("15800"),
                    "discount_amount": Decimal("30.02"),
                    "total_amount": Decimal("15769.98"),
                    "tax_amount": Decimal("0"),
                    "net_amount": Decimal("15769.98"),
                    "raw_payload": {"Codigo": "0002"},
                },
                {
                    "external_id": "659",
                    "sku": "7186/2",
                    "name": "VELA METALICA No. 2 DORADO",
                    "category": "Granmark",
                    "family": "Velas",
                    "quantity": Decimal("3"),
                    "tickets": 0,
                    "gross_amount": Decimal("105"),
                    "discount_amount": Decimal("0"),
                    "total_amount": Decimal("105"),
                    "tax_amount": Decimal("14.48"),
                    "net_amount": Decimal("90.52"),
                    "raw_payload": {"Codigo": "7186/2"},
                },
            ],
            captured_at=timezone.now(),
            raw_export_path="/tmp/point_sales.json",
            metadata={"row_count": 2},
        )

    def extract(self, *, start_date, end_date, branch_filter=None, excluded_ranges=None, max_days=None):
        return list(
            self.iter_extract(
                start_date=start_date,
                end_date=end_date,
                branch_filter=branch_filter,
                excluded_ranges=excluded_ranges,
                max_days=max_days,
            )
        )


class PointSalesSyncServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_sales_bridge",
            email="admin_sales_bridge@example.com",
            password="test12345",
        )
        self.sucursal = Sucursal.objects.create(codigo="9", nombre="Almacen", activa=True)
        self.receta = Receta.objects.create(
            nombre="Pay de Queso Mediano",
            codigo_point="0002",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-sales-pay-mediano",
        )

    def test_run_sales_sync_persists_staging_and_historical_sales(self):
        service = PointSyncService(sales_extractor=FakeSalesExtractor())
        sync_job = service.run_sales_sync(
            start_date=date(2025, 12, 31),
            end_date=date(2025, 12, 31),
            triggered_by=self.user,
        )

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(PointBranch.objects.count(), 1)
        self.assertEqual(PointProduct.objects.count(), 2)
        self.assertEqual(PointDailySale.objects.count(), 2)
        self.assertEqual(VentaHistorica.objects.filter(fuente=PointSyncService.SALES_HISTORY_SOURCE).count(), 1)

        historial = VentaHistorica.objects.get(fuente=PointSyncService.SALES_HISTORY_SOURCE)
        self.assertEqual(historial.receta_id, self.receta.id)
        self.assertEqual(historial.sucursal_id, self.sucursal.id)
        self.assertEqual(historial.fecha.isoformat(), "2025-12-31")
        self.assertEqual(historial.cantidad, Decimal("40"))

    def test_run_sales_sync_is_idempotent_for_historical_sales_source(self):
        service = PointSyncService(sales_extractor=FakeSalesExtractor())
        first_job = service.run_sales_sync(start_date=date(2025, 12, 31), end_date=date(2025, 12, 31), triggered_by=self.user)
        self.assertEqual(first_job.status, PointSyncJob.STATUS_SUCCESS)

        service.run_sales_sync(start_date=date(2025, 12, 31), end_date=date(2025, 12, 31), triggered_by=self.user)

        self.assertEqual(PointDailySale.objects.count(), 2)
        self.assertEqual(VentaHistorica.objects.filter(fuente=PointSyncService.SALES_HISTORY_SOURCE).count(), 1)
