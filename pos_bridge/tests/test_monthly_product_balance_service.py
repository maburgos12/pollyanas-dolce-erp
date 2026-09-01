from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

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
        slice_row = balance.rows[self.slice.id]
        parent_row = balance.rows[self.parent.id]
        self.assertEqual(slice_row.conversion_in, Decimal("16"))
        self.assertEqual(parent_row.conversion_out, Decimal("2"))
        self.assertEqual(slice_row.conversion_origin, "EQUIVALENCIA_CONFIGURADA")
        self.assertEqual(slice_row.source_counts, {"conversion_in_rows": 2, "conversion_out_rows": 0})
        self.assertEqual(balance.source_counts, {"conversion_rows_read": 2, "conversion_rows_applied": 2})

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

        self.assertNotIn(self.slice.id, balance.rows)
        self.assertNotIn(self.parent.id, balance.rows)
        with self.assertRaises(KeyError):
            balance.rows[self.slice.id]
        with self.assertRaises(TypeError):
            balance.rows[self.slice.id] = None

    def test_unresolved_origin_preserves_destination_entry_and_records_issue(self):
        self._conversion(quantity="16", when=datetime(2026, 8, 15, 12, 0))

        balance = MonthlyPointProductBalanceService().build("2026-08")

        slice_row = balance.rows[self.slice.id]
        self.assertEqual(slice_row.conversion_in, Decimal("16"))
        self.assertEqual(slice_row.conversion_out, Decimal("0"))
        self.assertIn("CONVERSION_ORIGIN_UNRESOLVED", slice_row.issues)
        self.assertEqual(slice_row.source_counts, {"conversion_in_rows": 1, "conversion_out_rows": 0})
        self.assertEqual(sum(row.conversion_out for row in balance.rows.values()), Decimal("0"))

    def test_explicit_point_source_uses_factor_only_for_same_configured_parent(self):
        explicit_parent = self._recipe("Pastel Chocolate Grande", "CHO-G")
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=explicit_parent,
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

        self.assertEqual(balance.rows[self.slice.id].conversion_origin, "POINT")
        self.assertEqual(balance.rows[explicit_parent.id].conversion_out, Decimal("2"))
        self.assertNotIn(self.parent.id, balance.rows)

    def test_supplied_unmatched_point_source_does_not_fallback_to_equivalence(self):
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
            source_item_code="UNKNOWN-PARENT",
            source_item_name="Pastel inexistente",
        )

        balance = MonthlyPointProductBalanceService().build("2026-08")

        row = balance.rows[self.slice.id]
        self.assertEqual(row.conversion_in, Decimal("16"))
        self.assertIn("POINT_CONVERSION_SOURCE_UNRESOLVED", row.issues)
        self.assertNotIn(self.parent.id, balance.rows)

    def test_explicit_point_source_with_different_configured_parent_has_no_exit(self):
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
        )

        balance = MonthlyPointProductBalanceService().build("2026-08")

        self.assertIn("CONVERSION_SOURCE_FACTOR_MISMATCH", balance.rows[self.slice.id].issues)
        self.assertNotIn(explicit_parent.id, balance.rows)
        self.assertNotIn(self.parent.id, balance.rows)

    def test_unhomologated_destination_is_preserved_as_top_level_issue(self):
        conversion = self._conversion(
            quantity="7",
            when=datetime(2026, 8, 15, 12, 0),
            receta=None,
            item_code="UNKNOWN-SLICE",
            item_name="Rebanada desconocida",
        )

        balance = MonthlyPointProductBalanceService().build("2026-08")

        self.assertEqual(balance.rows, {})
        self.assertEqual(len(balance.unresolved_conversions), 1)
        unresolved = balance.unresolved_conversions[0]
        self.assertEqual(unresolved.movement_external_id, conversion.movement_external_id)
        self.assertEqual(unresolved.source_hash, conversion.source_hash)
        self.assertEqual(unresolved.item_code, "UNKNOWN-SLICE")
        self.assertEqual(unresolved.item_name, "Rebanada desconocida")
        self.assertEqual(unresolved.quantity, Decimal("7"))
        self.assertEqual(unresolved.issue, "CONVERSION_DESTINATION_UNRESOLVED")

    def test_repeated_point_source_identity_is_resolved_once(self):
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("8"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._conversion(
            quantity="10",
            when=datetime(2026, 8, 10, 12, 0),
            source_item_code=self.parent.codigo_point,
            source_item_name=self.parent.nombre,
        )
        self._conversion(
            quantity="6",
            when=datetime(2026, 8, 11, 12, 0),
            source_item_code=self.parent.codigo_point,
            source_item_name=self.parent.nombre,
        )
        calls = []

        def resolve_recipe(**identity):
            calls.append(identity)
            return self.parent

        service = MonthlyPointProductBalanceService(
            identity_service=SimpleNamespace(resolve_recipe=resolve_recipe)
        )

        with self.assertNumQueries(2):
            balance = service.build("2026-08")

        self.assertEqual(len(calls), 1)
        self.assertEqual(balance.rows[self.parent.id].conversion_out, Decimal("2"))

    def test_datetime_month_is_rejected_instead_of_silently_relocalized(self):
        with self.assertRaisesRegex(ValueError, "datetime"):
            MonthlyPointProductBalanceService().build(datetime(2026, 8, 15, 12, 0))

    def test_invalid_configured_factor_has_distinct_issue_and_no_exit(self):
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("0"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._conversion(quantity="16", when=datetime(2026, 8, 15, 12, 0))

        balance = MonthlyPointProductBalanceService().build("2026-08")

        self.assertIn("CONVERSION_FACTOR_INVALID", balance.rows[self.slice.id].issues)
        self.assertNotIn(self.parent.id, balance.rows)

    def test_negative_conversion_reverses_both_sides_and_zero_is_not_applied(self):
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("8"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._conversion(quantity="-16", when=datetime(2026, 8, 15, 12, 0))
        self._conversion(quantity="0", when=datetime(2026, 8, 16, 12, 0))

        balance = MonthlyPointProductBalanceService().build("2026-08")

        self.assertEqual(balance.rows[self.slice.id].conversion_in, Decimal("-16"))
        self.assertEqual(balance.rows[self.parent.id].conversion_out, Decimal("-2"))
        self.assertEqual(balance.source_counts, {"conversion_rows_read": 2, "conversion_rows_applied": 1})
        self.assertEqual(
            balance.rows[self.slice.id].source_counts,
            {"conversion_in_rows": 1, "conversion_out_rows": 0},
        )
        self.assertEqual(
            balance.rows[self.parent.id].source_counts,
            {"conversion_in_rows": 0, "conversion_out_rows": 1},
        )
