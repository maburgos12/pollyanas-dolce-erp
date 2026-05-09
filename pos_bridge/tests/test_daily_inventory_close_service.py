from __future__ import annotations

from datetime import datetime, timezone as datetime_timezone
from decimal import Decimal
from io import BytesIO
from zoneinfo import ZoneInfo

from django.test import TestCase
from django.utils import timezone
from openpyxl import load_workbook

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.daily_inventory_close_service import DailyInventoryCloseService


class DailyInventoryCloseServiceTests(TestCase):
    def setUp(self):
        PointInventorySnapshot.objects.all().delete()
        PointBranch.objects.all().delete()
        PointProduct.objects.all().delete()
        PointSyncJob.objects.all().delete()
        Sucursal.objects.all().update(activa=False)
        self.matriz, _ = Sucursal.objects.update_or_create(
            codigo="MATRIZ",
            defaults={"nombre": "Matriz", "activa": True},
        )
        self.cedis, _ = Sucursal.objects.update_or_create(
            codigo="CEDIS",
            defaults={"nombre": "CEDIS", "activa": False},
        )
        self.inactive, _ = Sucursal.objects.update_or_create(
            codigo="DEVOLUCIONES",
            defaults={"nombre": "Devoluciones", "activa": False},
        )
        self.matriz_branch = PointBranch.objects.create(external_id="1", name="MATRIZ", erp_branch=self.matriz)
        self.cedis_branch = PointBranch.objects.create(external_id="8", name="CEDIS", erp_branch=self.cedis)
        self.inactive_branch = PointBranch.objects.create(
            external_id="12",
            name="DEVOLUCIONES",
            erp_branch=self.inactive,
        )
        self.product = PointProduct.objects.create(external_id="P1", sku="P1", name="Pastel Chocolate")
        self.job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY)

    def _captured_at(self, value: str):
        return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("America/Mazatlan")).astimezone(datetime_timezone.utc)

    def _snapshot(self, *, branch, stock, captured_at):
        return PointInventorySnapshot.objects.create(
            branch=branch,
            product=self.product,
            stock=Decimal(str(stock)),
            min_stock=Decimal("0"),
            max_stock=Decimal("0"),
            captured_at=self._captured_at(captured_at),
            sync_job=self.job,
        )

    def test_build_close_uses_latest_snapshot_for_active_branches_plus_cedis_only(self):
        self._snapshot(branch=self.matriz_branch, stock="3", captured_at="2026-05-08T21:00:00")
        self._snapshot(branch=self.matriz_branch, stock="4.5", captured_at="2026-05-08T23:00:00")
        self._snapshot(branch=self.cedis_branch, stock="10", captured_at="2026-05-08T23:05:00")
        self._snapshot(branch=self.inactive_branch, stock="99", captured_at="2026-05-08T23:10:00")
        self._snapshot(branch=self.matriz_branch, stock="50", captured_at="2026-05-07T23:00:00")

        payload = DailyInventoryCloseService().build_close(fecha_operacion=datetime(2026, 5, 8).date())

        self.assertEqual([branch["code"] for branch in payload["branches"]], ["MATRIZ", "CEDIS"])
        self.assertEqual(payload["rows"][0]["stocks"]["MATRIZ"], Decimal("4.500"))
        self.assertEqual(payload["rows"][0]["stocks"]["CEDIS"], Decimal("10.000"))
        self.assertEqual(payload["rows"][0]["total_stock"], Decimal("14.500"))
        self.assertEqual(payload["missing_branch_codes"], [])

    def test_build_workbook_exports_same_matrix(self):
        self._snapshot(branch=self.matriz_branch, stock="4", captured_at="2026-05-08T23:00:00")
        self._snapshot(branch=self.cedis_branch, stock="10", captured_at="2026-05-08T23:05:00")

        payload = DailyInventoryCloseService().build_close(fecha_operacion=datetime(2026, 5, 8).date())
        workbook = DailyInventoryCloseService().build_workbook(payload)
        output = BytesIO()
        workbook.save(output)
        output.seek(0)
        sheet = load_workbook(output, data_only=True).active

        self.assertEqual(sheet["A1"].value, "Inventario final al cierre")
        self.assertEqual(sheet["A5"].value, "SKU")
        self.assertEqual(sheet["D5"].value, "MATRIZ")
        self.assertEqual(sheet["E5"].value, "CEDIS")
        self.assertEqual(sheet["F5"].value, "Total cierre")
        self.assertEqual(sheet["D6"].value, 4.0)
        self.assertEqual(sheet["E6"].value, 10.0)
        self.assertEqual(sheet["F6"].value, 14.0)
