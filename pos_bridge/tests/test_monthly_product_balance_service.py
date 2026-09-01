from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointConversionLine, PointDailySale, PointProduct, PointSyncJob
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from recetas.models import Receta, RecetaEquivalencia


class MonthlyProductBalanceConversionTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="CONV", nombre="Sucursal Conversiones")
        self.branch = PointBranch.objects.create(
            external_id="CONV",
            name="Sucursal Conversiones",
            erp_branch=self.sucursal,
        )
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
        )
        self.parent = self._recipe("Pastel Chocolate Mediano", "CHO-M")
        self.slice = self._recipe("Pastel Chocolate Rebanada", "CHO-R")

    def _recipe(self, name: str, point_code: str) -> Receta:
        return Receta.objects.create(
            nombre=name,
            codigo_point=point_code,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"test-{point_code}",
        )

    def _conversion(self, *, quantity: str, when: datetime, **overrides) -> PointConversionLine:
        values = {
            "branch": self.branch,
            "erp_branch": self.sucursal,
            "receta": self.slice,
            "sync_job": self.sync_job,
            "movement_external_id": f"movement-{PointConversionLine.objects.count() + 1}",
            "source_hash": f"conversion-{PointConversionLine.objects.count() + 1}",
            "movement_at": timezone.make_aware(when, timezone.get_current_timezone()),
            "item_name": self.slice.nombre,
            "item_code": self.slice.codigo_point,
            "quantity": Decimal(quantity),
        }
        values.update(overrides)
        return PointConversionLine.objects.create(**values)

    def test_configured_equivalence_projects_real_point_conversion(self):
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("8"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._conversion(quantity="10", when=datetime(2026, 8, 1, 0, 0))
        self._conversion(quantity="6", when=datetime(2026, 8, 15, 12, 0))
        self._conversion(quantity="99", when=datetime(2026, 9, 1, 0, 0))

        balance = MonthlyPointProductBalanceService().build("2026-08")

        self.assertEqual(balance.month_start, date(2026, 8, 1))
        self.assertEqual(balance.month_end, date(2026, 8, 31))
        slice_row = balance.row_for(self.slice.id)
        parent_row = balance.row_for(self.parent.id)
        self.assertEqual(slice_row.conversion_in, Decimal("16"))
        self.assertEqual(parent_row.conversion_out, Decimal("2"))
        self.assertEqual(slice_row.conversion_origin, "EQUIVALENCIA_CONFIGURADA")
        self.assertEqual(slice_row.source_counts, {"EQUIVALENCIA_CONFIGURADA": 2})

    def test_sales_without_point_conversion_do_not_create_conversion_movements(self):
        product = PointProduct.objects.create(
            external_id="slice-product",
            sku=self.slice.codigo_point,
            name=self.slice.nombre,
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=product,
            receta=self.slice,
            sync_job=self.sync_job,
            sale_date=date(2026, 8, 20),
            quantity=Decimal("16"),
            tickets=1,
            gross_amount=Decimal("160"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("160"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("160"),
        )
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("8"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )

        balance = MonthlyPointProductBalanceService().build(date(2026, 8, 20))

        self.assertEqual(balance.row_for(self.slice.id).conversion_in, Decimal("0"))
        self.assertEqual(balance.row_for(self.parent.id).conversion_out, Decimal("0"))

    def test_unresolved_origin_preserves_destination_entry_and_records_issue(self):
        self._conversion(quantity="16", when=datetime(2026, 8, 15, 12, 0))

        balance = MonthlyPointProductBalanceService().build("2026-08")

        slice_row = balance.row_for(self.slice.id)
        self.assertEqual(slice_row.conversion_in, Decimal("16"))
        self.assertEqual(slice_row.conversion_out, Decimal("0"))
        self.assertIn("CONVERSION_ORIGIN_UNRESOLVED", slice_row.issues)
        self.assertEqual(slice_row.source_counts, {"UNRESOLVED": 1})
        self.assertEqual(sum(row.conversion_out for row in balance.rows), Decimal("0"))

    def test_explicit_point_source_takes_priority_over_configured_parent(self):
        explicit_parent = self._recipe("Pastel Chocolate Grande", "CHO-G")
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("8"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._conversion(
            quantity="16",
            when=datetime(2026, 8, 15, 12, 0),
            source_item_code=explicit_parent.codigo_point,
            source_item_name=explicit_parent.nombre,
        )

        balance = MonthlyPointProductBalanceService().build("2026-08")

        self.assertEqual(balance.row_for(self.slice.id).conversion_origin, "POINT")
        self.assertEqual(balance.row_for(self.slice.id).source_counts, {"POINT": 1})
        self.assertEqual(balance.row_for(explicit_parent.id).conversion_out, Decimal("2"))
        self.assertEqual(balance.row_for(self.parent.id).conversion_out, Decimal("0"))
