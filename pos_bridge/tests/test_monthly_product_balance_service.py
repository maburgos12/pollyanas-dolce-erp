from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from types import MappingProxyType, SimpleNamespace

from django.test import TestCase, override_settings
from django.utils import timezone

from control.models import MermaMensualSucursal
from core.models import Sucursal
from pos_bridge.models import (
    PointBranch,
    PointConversionLine,
    PointDailySale,
    PointInventorySnapshot,
    PointProduct,
    PointProductionLine,
    PointSyncJob,
    PointWasteLine,
)
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from recetas.models import Receta, RecetaEquivalencia, VentaHistorica
from reportes.models import FactProduccionDiaria


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

    def _service(self, **kwargs):
        kwargs.setdefault("official_sales_report_service", _EmptyOfficialSalesReportService())
        return MonthlyPointProductBalanceService(**kwargs)

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

        balance = self._service().build("2026-08")

        self.assertEqual(balance.month_start, date(2026, 8, 1))
        self.assertEqual(balance.month_end, date(2026, 8, 31))
        slice_row = balance.rows[self.slice.id]
        parent_row = balance.rows[self.parent.id]
        self.assertEqual(slice_row.conversion_in, Decimal("16"))
        self.assertEqual(parent_row.conversion_out, Decimal("2"))
        self.assertEqual(slice_row.conversion_origin, "EQUIVALENCIA_CONFIGURADA")
        self.assertEqual(slice_row.source_counts["conversion_in_rows"], 2)
        self.assertEqual(slice_row.source_counts["conversion_out_rows"], 0)
        self.assertEqual(balance.source_counts["conversion_rows_read"], 2)
        self.assertEqual(balance.source_counts["conversion_destination_rows_applied"], 2)

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

        balance = self._service().build(date(2026, 8, 20))

        self.assertNotIn(self.slice.id, balance.rows)
        self.assertNotIn(self.parent.id, balance.rows)
        with self.assertRaises(KeyError):
            balance.rows[self.slice.id]
        with self.assertRaises(TypeError):
            balance.rows[self.slice.id] = None

    def test_unresolved_origin_preserves_destination_entry_and_records_issue(self):
        self._conversion(quantity="16", when=datetime(2026, 8, 15, 12, 0))

        balance = self._service().build("2026-08")

        slice_row = balance.rows[self.slice.id]
        self.assertEqual(slice_row.conversion_in, Decimal("16"))
        self.assertEqual(slice_row.conversion_out, Decimal("0"))
        self.assertIn("CONVERSION_ORIGIN_UNRESOLVED", slice_row.issues)
        self.assertEqual(slice_row.source_counts["conversion_in_rows"], 1)
        self.assertEqual(slice_row.source_counts["conversion_out_rows"], 0)
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

        balance = self._service().build("2026-08")

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

        balance = self._service().build("2026-08")

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

        balance = self._service().build("2026-08")

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

        balance = self._service().build("2026-08")

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

        service = self._service(
            identity_service=SimpleNamespace(resolve_recipe=resolve_recipe)
        )

        balance = service.build("2026-08")

        self.assertEqual(len(calls), 1)
        self.assertEqual(balance.rows[self.parent.id].conversion_out, Decimal("2"))

    def test_datetime_month_is_rejected_instead_of_silently_relocalized(self):
        with self.assertRaisesRegex(ValueError, "datetime"):
            self._service().build(datetime(2026, 8, 15, 12, 0))

    def test_invalid_configured_factor_has_distinct_issue_and_no_exit(self):
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("0"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._conversion(quantity="16", when=datetime(2026, 8, 15, 12, 0))

        balance = self._service().build("2026-08")

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

        balance = self._service().build("2026-08")

        self.assertEqual(balance.rows[self.slice.id].conversion_in, Decimal("-16"))
        self.assertEqual(balance.rows[self.parent.id].conversion_out, Decimal("-2"))
        self.assertEqual(balance.source_counts["conversion_rows_read"], 2)
        self.assertEqual(balance.source_counts["conversion_destination_rows_applied"], 1)
        self.assertEqual(balance.rows[self.slice.id].source_counts["conversion_in_rows"], 1)
        self.assertEqual(balance.rows[self.slice.id].source_counts["conversion_out_rows"], 0)
        self.assertEqual(balance.rows[self.parent.id].source_counts["conversion_in_rows"], 0)
        self.assertEqual(balance.rows[self.parent.id].source_counts["conversion_out_rows"], 1)

    def test_explicit_source_without_equivalence_has_factor_missing_issue(self):
        self._conversion(
            quantity="16",
            when=datetime(2026, 8, 15, 12, 0),
            source_item_code=self.parent.codigo_point,
            source_item_name=self.parent.nombre,
        )

        balance = self._service().build("2026-08")

        self.assertIn("CONVERSION_FACTOR_MISSING", balance.rows[self.slice.id].issues)
        unresolved = next(item for item in balance.unresolved_movements if item.source == "conversion_source")
        self.assertEqual(unresolved.movement_id, "movement-1")
        self.assertEqual(unresolved.item_code, self.parent.codigo_point)

    def test_complete_snapshots_with_conversion_issue_force_review_status(self):
        product = PointProduct.objects.create(
            external_id="conversion-status-slice",
            sku=self.slice.codigo_point,
            name=self.slice.nombre,
        )
        for stock, captured_at in (
            ("5", datetime(2026, 7, 31, 8)),
            ("6", datetime(2026, 8, 31, 8)),
        ):
            PointInventorySnapshot.objects.create(
                branch=self.branch,
                product=product,
                stock=Decimal(stock),
                captured_at=timezone.make_aware(captured_at, timezone.get_current_timezone()),
                sync_job=self.sync_job,
            )
        scenarios = (
            ({}, "CONVERSION_ORIGIN_UNRESOLVED"),
            (
                {
                    "source_item_code": self.parent.codigo_point,
                    "source_item_name": self.parent.nombre,
                },
                "CONVERSION_FACTOR_MISSING",
            ),
        )

        for index, (overrides, expected_issue) in enumerate(scenarios, start=1):
            with self.subTest(issue=expected_issue):
                PointConversionLine.objects.all().delete()
                self._conversion(
                    quantity="1",
                    when=datetime(2026, 8, 15, 12),
                    movement_external_id=f"status-{index}",
                    source_hash=f"status-hash-{index}",
                    **overrides,
                )

                row = self._service().build("2026-08").rows[self.slice.id]

                self.assertEqual(row.difference_point, Decimal("0"))
                self.assertIn(expected_issue, row.issues)
                self.assertEqual(row.status, "REVISAR_FUENTE")

    def test_complete_snapshots_with_factor_mismatch_force_review_status(self):
        configured_parent = self._recipe("Pastel configurado", "CFG-PARENT")
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=configured_parent,
            factor_conversion=Decimal("8"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        product = PointProduct.objects.create(
            external_id="conversion-status-mismatch",
            sku=self.slice.codigo_point,
            name=self.slice.nombre,
        )
        for stock, captured_at in (
            ("5", datetime(2026, 7, 31, 8)),
            ("6", datetime(2026, 8, 31, 8)),
        ):
            PointInventorySnapshot.objects.create(
                branch=self.branch,
                product=product,
                stock=Decimal(stock),
                captured_at=timezone.make_aware(captured_at, timezone.get_current_timezone()),
                sync_job=self.sync_job,
            )
        self._conversion(
            quantity="1",
            when=datetime(2026, 8, 15, 12),
            source_item_code=self.parent.codigo_point,
            source_item_name=self.parent.nombre,
        )

        row = self._service().build("2026-08").rows[self.slice.id]

        self.assertEqual(row.difference_point, Decimal("0"))
        self.assertIn("CONVERSION_SOURCE_FACTOR_MISMATCH", row.issues)
        self.assertEqual(row.status, "REVISAR_FUENTE")


class _EmptyOfficialSalesReportService:
    def fetch_report(self, **kwargs):
        return SimpleNamespace(report_path="/tmp/empty-point-report.xls", request_url="https://point.invalid/report")

    def parse_report(self, *, report_path):
        return SimpleNamespace(rows=[], summary={})


class _OfficialSalesReportService:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def fetch_report(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(report_path="/tmp/point-report.xls", request_url="https://point.invalid/report")

    def parse_report(self, *, report_path):
        return SimpleNamespace(rows=self.rows, summary={"venta": Decimal("100")})


class _FailingOfficialSalesReportService:
    def fetch_report(self, **kwargs):
        raise RuntimeError("Point unavailable")


class _UnexpectedOfficialSalesReportService:
    def __init__(self):
        self.calls = 0

    def fetch_report(self, **kwargs):
        self.calls += 1
        raise AssertionError("build local no debe consultar Point")


class _MalformedOfficialSalesReportService:
    def __init__(self):
        self.calls = 0

    def fetch_report(self, **kwargs):
        self.calls += 1
        return SimpleNamespace(report_path="/tmp/malformed-point-report.xls", request_url="https://point.invalid/report")

    def parse_report(self, *, report_path):
        return SimpleNamespace(rows={"Codigo": "not-a-list"}, summary={})


@override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="AUTO")
class MonthlyProductBalanceLedgerTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="LEDGER", nombre="Sucursal Ledger")
        self.branch = PointBranch.objects.create(
            external_id="LEDGER",
            name="Sucursal Ledger",
            erp_branch=self.sucursal,
        )
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
        )
        self.parent = self._recipe("Pastel Fresa Mediano", "FRE-M")
        self.slice = self._recipe("Pastel Fresa Rebanada", "FRE-R")
        self.parent_product = self._product(self.parent, "parent")
        self.slice_product = self._product(self.slice, "slice")

    def _recipe(self, name, code):
        return Receta.objects.create(
            nombre=name,
            codigo_point=code,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"ledger-{code}",
        )

    def _product(self, recipe, suffix):
        return PointProduct.objects.create(
            external_id=f"ledger-{suffix}",
            sku=recipe.codigo_point,
            name=recipe.nombre,
        )

    def _snapshot(self, product, stock, when):
        return PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=product,
            stock=Decimal(stock),
            captured_at=timezone.make_aware(when, timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )

    def _production(self, recipe, quantity, production_date, suffix):
        return PointProductionLine.objects.create(
            branch=self.branch,
            erp_branch=self.sucursal,
            receta=recipe,
            production_external_id=f"prod-{suffix}",
            detail_external_id=f"detail-{suffix}",
            source_hash=f"prod-hash-{suffix}",
            production_date=production_date,
            item_name=recipe.nombre,
            item_code=recipe.codigo_point,
            produced_quantity=Decimal(quantity),
        )

    def _waste(self, recipe, quantity, when, suffix):
        return PointWasteLine.objects.create(
            branch=self.branch,
            erp_branch=self.sucursal,
            receta=recipe,
            movement_external_id=f"waste-{suffix}",
            source_hash=f"waste-hash-{suffix}",
            movement_at=timezone.make_aware(when, timezone.get_current_timezone()),
            item_name=recipe.nombre,
            item_code=recipe.codigo_point,
            quantity=Decimal(quantity),
        )

    def _daily_sale(self, recipe, product, quantity, sale_date, suffix, source_endpoint="/Report/PrintReportes?idreporte=3"):
        return PointDailySale.objects.create(
            branch=self.branch,
            product=product,
            receta=recipe,
            sync_job=self.sync_job,
            sale_date=sale_date,
            quantity=Decimal(quantity),
            source_endpoint=source_endpoint,
        )

    def _service(self, rows=()):
        official = _OfficialSalesReportService(list(rows))
        return MonthlyPointProductBalanceService(
            official_sales_report_service=official,
            refresh_official_sales=bool(rows),
        ), official

    def test_exact_recipe_snapshots_and_formula_do_not_roll_slice_into_parent(self):
        RecetaEquivalencia.objects.create(
            receta_porcion=self.slice,
            receta_padre=self.parent,
            factor_conversion=Decimal("8"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.slice_product, "24", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "12", datetime(2026, 7, 31, 8))
        self._snapshot(self.slice_product, "17", datetime(2026, 7, 31, 8))
        self._production(self.slice, "4", date(2026, 7, 10), "july")
        self._waste(self.slice, "1", datetime(2026, 7, 20, 12), "july")
        service, _official = self._service(
            [{"Codigo": self.slice.codigo_point, "Nombre": self.slice.nombre, "Cantidad": Decimal("10")}]
        )

        balance = service.build("2026-07")

        parent = balance.rows[self.parent.id]
        slice_row = balance.rows[self.slice.id]
        self.assertEqual(parent.opening_point, Decimal("10"))
        self.assertEqual(slice_row.opening_point, Decimal("24"))
        self.assertEqual(slice_row.production, Decimal("4"))
        self.assertEqual(slice_row.sales, Decimal("10"))
        self.assertEqual(slice_row.waste, Decimal("1"))
        self.assertEqual(slice_row.calculated_closing, Decimal("17"))
        self.assertEqual(slice_row.closing_point, Decimal("17"))
        self.assertEqual(slice_row.difference_point, Decimal("0"))
        self.assertEqual(slice_row.status, "COINCIDE")
        self.assertIsInstance(balance.rows, MappingProxyType)
        with self.assertRaises(TypeError):
            balance.rows[self.slice.id] = slice_row

    def test_difference_sign_and_generic_august_month(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 9))
        self._snapshot(self.parent_product, "12.02", datetime(2026, 8, 31, 9))
        service, official = self._service(
            [{"Codigo": self.parent.codigo_point, "Nombre": self.parent.nombre, "Cantidad": Decimal("1")}]
        )
        self._production(self.parent, "3", date(2026, 8, 15), "august")

        balance = service.build("2026-08")

        row = balance.rows[self.parent.id]
        self.assertEqual(row.calculated_closing, Decimal("12"))
        self.assertEqual(row.difference_point, Decimal("0.02"))
        self.assertEqual(row.status, "POINT_MAYOR")
        self.assertEqual(official.calls[0]["start_date"], date(2026, 8, 1))
        self.assertEqual(official.calls[0]["end_date"], date(2026, 8, 31))

        self._snapshot(self.parent_product, "11.98", datetime(2026, 8, 31, 10))
        lower = service.build("2026-08").rows[self.parent.id]
        self.assertEqual(lower.difference_point, Decimal("-0.02"))
        self.assertEqual(lower.status, "POINT_MENOR")

    def test_december_uses_half_open_year_rollover_boundary(self):
        self._snapshot(self.parent_product, "5", datetime(2026, 11, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 12, 31, 8))
        self._production(self.parent, "2", date(2026, 12, 31), "december")
        self._production(self.parent, "99", date(2027, 1, 1), "january")
        service, official = self._service()

        row = service.build("2026-12").rows[self.parent.id]

        self.assertEqual(row.production, Decimal("2"))
        self.assertEqual(row.calculated_closing, Decimal("7"))
        self.assertEqual(official.calls, [])

    def test_missing_snapshot_is_non_authoritative_and_warned(self):
        self._production(self.parent, "3", date(2026, 7, 15), "missing-opening")
        self._snapshot(self.parent_product, "3", datetime(2026, 7, 31, 8))
        service, _official = self._service()

        balance = service.build("2026-07")

        row = balance.rows[self.parent.id]
        self.assertIsNone(row.opening_point)
        self.assertIsNone(row.calculated_closing)
        self.assertEqual(row.closing_point, Decimal("3"))
        self.assertIsNone(row.difference_point)
        self.assertEqual(row.status, "REVISAR_FUENTE")
        self.assertTrue(any("inicial" in warning.lower() for warning in balance.warnings))

    def test_missing_closing_snapshot_is_non_authoritative_and_warned(self):
        self._snapshot(self.parent_product, "3", datetime(2026, 6, 30, 8))
        service, _official = self._service()

        balance = service.build("2026-07")

        row = balance.rows[self.parent.id]
        self.assertEqual(row.opening_point, Decimal("3"))
        self.assertIsNone(row.closing_point)
        self.assertIsNone(row.difference_point)
        self.assertEqual(row.status, "REVISAR_FUENTE")
        self.assertTrue(any("final" in warning.lower() for warning in balance.warnings))

    def test_recipe_missing_from_selected_snapshot_emits_warning(self):
        self._snapshot(self.parent_product, "3", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "3", datetime(2026, 7, 31, 8))
        self._production(self.slice, "1", date(2026, 7, 10), "slice-without-snapshot")
        service, _official = self._service()

        balance = service.build("2026-07")

        self.assertEqual(balance.rows[self.slice.id].status, "REVISAR_FUENTE")
        self.assertTrue(any("receta" in warning.lower() and "inicial" in warning.lower() for warning in balance.warnings))
        self.assertTrue(any("receta" in warning.lower() and "final" in warning.lower() for warning in balance.warnings))

    def test_latest_snapshot_per_branch_product_and_fallback_day_are_exposed(self):
        self._snapshot(self.parent_product, "1", datetime(2026, 6, 29, 8))
        self._snapshot(self.parent_product, "8", datetime(2026, 6, 29, 18))
        self._snapshot(self.parent_product, "8", datetime(2026, 7, 30, 18))
        service, _official = self._service()

        balance = service.build("2026-07")

        row = balance.rows[self.parent.id]
        self.assertEqual(row.opening_point, Decimal("8"))
        self.assertEqual(row.closing_point, Decimal("8"))
        self.assertEqual(balance.effective_snapshot_dates["opening"], date(2026, 6, 29))
        self.assertEqual(balance.effective_snapshot_dates["closing"], date(2026, 7, 30))
        self.assertTrue(any("alternativa" in warning.lower() for warning in balance.warnings))

    def test_exact_snapshot_calendar_day_wins_over_adjacent_late_timestamp(self):
        self._snapshot(self.parent_product, "99", datetime(2026, 6, 29, 23))
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "88", datetime(2026, 7, 30, 23))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        service, _official = self._service()

        balance = service.build("2026-07")

        self.assertEqual(balance.rows[self.parent.id].opening_point, Decimal("10"))
        self.assertEqual(balance.rows[self.parent.id].closing_point, Decimal("10"))
        self.assertEqual(balance.effective_snapshot_dates["opening"], date(2026, 6, 30))
        self.assertEqual(balance.effective_snapshot_dates["closing"], date(2026, 7, 31))
        self.assertTrue(balance.sources["opening_snapshot"]["authoritative"])
        self.assertEqual(balance.sources["opening_snapshot"]["selected_rows"], 1)
        self.assertEqual(balance.sources["opening_snapshot"]["applied_rows"], 1)

    def test_snapshot_branch_coverage_is_distinct_from_product_rows_and_respects_tolerance(self):
        second_branch = PointBranch.objects.create(
            external_id="LEDGER-2",
            name="Sucursal Ledger Dos",
            erp_branch=self.sucursal,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.slice_product, "20", datetime(2026, 6, 30, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 6, 30, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 8, 5, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 8, 5, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )
        service, _official = self._service()

        balance = service.build("2026-07")

        opening = balance.sources["opening_snapshot"]
        closing = balance.sources["closing_snapshot"]
        self.assertEqual(opening["selected_rows"], 3)
        self.assertEqual(opening["selected_branch_count"], 2)
        self.assertEqual(opening["applied_branch_count"], 2)
        self.assertEqual(closing["selected_rows"], 0)
        self.assertEqual(closing["selected_branch_count"], 0)
        self.assertEqual(closing["applied_rows"], 0)
        self.assertEqual(closing["applied_branch_count"], 0)

    def test_snapshot_selection_keeps_prior_day_for_other_branch_when_one_branch_is_exact(self):
        second_branch = PointBranch.objects.create(
            external_id="LEDGER-PRIOR",
            name="Sucursal Ledger Previa",
            erp_branch=self.sucursal,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("7"),
            captured_at=timezone.make_aware(datetime(2026, 6, 29, 18), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("7"),
            captured_at=timezone.make_aware(datetime(2026, 7, 31, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )

        balance = self._service()[0].build("2026-07")

        self.assertEqual(balance.rows[self.parent.id].opening_point, Decimal("17"))
        self.assertEqual(balance.sources["opening_snapshot"]["applied_branch_count"], 2)
        self.assertEqual(balance.sources["opening_snapshot"]["applied_coverage_key_count"], 2)
        self.assertEqual(balance.rows[self.parent.id].status, "COINCIDE")

    def test_missing_closing_branch_coverage_blocks_all_rows(self):
        second_branch = PointBranch.objects.create(
            external_id="LEDGER-MISSING",
            name="Sucursal Ledger Faltante",
            erp_branch=self.sucursal,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 6, 30, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )
        self._snapshot(self.parent_product, "18", datetime(2026, 7, 31, 8))

        balance = self._service()[0].build("2026-07")

        self.assertIn("SNAPSHOT_BRANCH_COVERAGE_INCOMPLETE", balance.issues)
        self.assertIn("MONTH_SOURCE_INCOMPLETE", balance.issues)
        self.assertEqual(balance.sources["opening_snapshot"]["missing_in_closing_branch_ids"], (second_branch.id,))
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_missing_product_coverage_with_same_branches_blocks_all_rows(self):
        second_branch = PointBranch.objects.create(
            external_id="LEDGER-PRODUCT",
            name="Sucursal Ledger Producto",
            erp_branch=self.sucursal,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.slice_product, "4", datetime(2026, 6, 30, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 6, 30, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.slice_product,
            stock=Decimal("3"),
            captured_at=timezone.make_aware(datetime(2026, 6, 30, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 7, 31, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )

        balance = self._service()[0].build("2026-07")

        self.assertNotIn("SNAPSHOT_BRANCH_COVERAGE_INCOMPLETE", balance.issues)
        self.assertIn("SNAPSHOT_PRODUCT_COVERAGE_INCOMPLETE", balance.issues)
        self.assertTrue(balance.sources["opening_snapshot"]["missing_in_closing_coverage_keys"])
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_snapshot_coverage_keeps_distinct_point_products_that_map_to_one_recipe(self):
        alias_product = PointProduct.objects.create(
            external_id="ledger-parent-alias",
            sku="LEDGER-PARENT-ALIAS",
            name=self.parent.nombre,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(alias_product, "2", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))

        balance = self._service()[0].build("2026-07")

        opening = balance.sources["opening_snapshot"]
        self.assertEqual(opening["applied_coverage_key_count"], 2)
        self.assertIn((self.branch.id, self.parent_product.id), opening["applied_coverage_keys"])
        self.assertIn((self.branch.id, alias_product.id), opening["applied_coverage_keys"])
        self.assertIn("SNAPSHOT_PRODUCT_COVERAGE_INCOMPLETE", balance.issues)
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_snapshot_tolerance_is_evaluated_per_branch_product_key(self):
        second_branch = PointBranch.objects.create(
            external_id="LEDGER-STALE",
            name="Sucursal Ledger Fuera Tolerancia",
            erp_branch=self.sucursal,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 6, 26, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        PointInventorySnapshot.objects.create(
            branch=second_branch,
            product=self.parent_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 7, 31, 8), timezone.get_current_timezone()),
            sync_job=self.sync_job,
        )

        balance = self._service()[0].build("2026-07")

        opening = balance.sources["opening_snapshot"]
        self.assertEqual(balance.rows[self.parent.id].opening_point, Decimal("10"))
        self.assertEqual(opening["selected_branch_count"], 1)
        self.assertEqual(opening["applied_branch_count"], 1)
        self.assertEqual(opening["out_of_tolerance_key_count"], 0)
        self.assertIn("SNAPSHOT_BRANCH_COVERAGE_INCOMPLETE", balance.issues)

    def test_fact_priority_preserves_exact_recipe_for_production_and_waste(self):
        self._snapshot(self.slice_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.slice_product, "12", datetime(2026, 7, 31, 8))
        self._production(self.slice, "99", date(2026, 7, 10), "ignored")
        self._waste(self.slice, "99", datetime(2026, 7, 10, 12), "ignored")
        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 10),
            sucursal=self.sucursal,
            receta=self.slice,
            producido=Decimal("5"),
            merma=Decimal("3"),
        )
        service, _official = self._service()

        row = service.build("2026-07").rows[self.slice.id]

        self.assertEqual(row.production, Decimal("5"))
        self.assertEqual(row.waste, Decimal("3"))
        self.assertNotIn(self.parent.id, service.build("2026-07").rows)

    def test_zero_fact_rows_still_take_priority_over_bridge_rows(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        self._production(self.parent, "99", date(2026, 7, 10), "ignored-zero-fact")
        self._waste(self.parent, "99", datetime(2026, 7, 10, 12), "ignored-zero-fact")
        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 10),
            sucursal=self.sucursal,
            receta=self.parent,
            producido=Decimal("0"),
            merma=Decimal("0"),
        )
        service, _official = self._service()

        balance = service.build("2026-07")

        self.assertEqual(balance.rows[self.parent.id].production, Decimal("0"))
        self.assertEqual(balance.rows[self.parent.id].waste, Decimal("0"))
        self.assertEqual(balance.sources["production"]["source"], "FactProduccionDiaria")
        self.assertEqual(balance.sources["waste"]["source"], "FactProduccionDiaria")

    def test_unmapped_zero_facts_do_not_block_per_field_fallbacks_but_mapped_zero_stays_authoritative(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 10),
            sucursal=self.sucursal,
            receta=None,
            producido=Decimal("0"),
            vendido=Decimal("0"),
            merma=Decimal("0"),
        )
        self._production(self.parent, "2", date(2026, 7, 10), "after-unmapped-zero")
        self._waste(self.parent, "1", datetime(2026, 7, 10, 12), "after-unmapped-zero")
        VentaHistorica.objects.create(
            receta=self.parent,
            sucursal=self.sucursal,
            fecha=date(2026, 7, 10),
            cantidad=Decimal("3"),
            fuente="POINT_BRIDGE_SALES",
        )
        service, _official = self._service()

        fallback_balance = service.build("2026-07")

        self.assertEqual(fallback_balance.rows[self.parent.id].production, Decimal("2"))
        self.assertEqual(fallback_balance.rows[self.parent.id].waste, Decimal("1"))
        self.assertEqual(fallback_balance.rows[self.parent.id].sales, Decimal("3"))
        self.assertEqual(fallback_balance.sources["production"]["source"], "PointProductionLine")
        self.assertEqual(fallback_balance.sources["waste"]["source"], "PointWasteLine")
        self.assertEqual(fallback_balance.sources["sales"]["mode"], "bridge_history")

        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 11),
            sucursal=self.sucursal,
            receta=self.parent,
            producido=Decimal("0"),
        )

        mapped_zero_balance = service.build("2026-07")

        self.assertEqual(mapped_zero_balance.rows[self.parent.id].production, Decimal("0"))
        self.assertEqual(mapped_zero_balance.rows[self.parent.id].waste, Decimal("0"))
        self.assertEqual(mapped_zero_balance.rows[self.parent.id].sales, Decimal("0"))
        self.assertEqual(mapped_zero_balance.sources["production"]["source"], "FactProduccionDiaria")
        self.assertEqual(mapped_zero_balance.sources["waste"]["source"], "FactProduccionDiaria")
        self.assertEqual(mapped_zero_balance.sources["sales"]["mode"], "production_facts")

    def test_monthly_waste_is_last_resort_fallback(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "8", datetime(2026, 7, 31, 8))
        MermaMensualSucursal.objects.create(
            periodo=date(2026, 7, 1),
            sucursal=self.sucursal,
            receta=self.parent,
            nombre_producto=self.parent.nombre,
            unidades_merma=Decimal("2"),
        )
        service, _official = self._service()

        balance = service.build("2026-07")

        self.assertEqual(balance.rows[self.parent.id].waste, Decimal("2"))
        self.assertEqual(balance.sources["waste"]["source"], "MermaMensualSucursal")

    def test_official_sales_priority_preserves_recipe_and_unmatched_rows(self):
        self._snapshot(self.slice_product, "20", datetime(2026, 6, 30, 8))
        self._snapshot(self.slice_product, "16", datetime(2026, 7, 31, 8))
        self._daily_sale(self.slice, self.slice_product, "999", date(2026, 7, 3), "ignored")
        service, _official = self._service(
            [
                {"Codigo": self.slice.codigo_point, "Nombre": self.slice.nombre, "Cantidad": Decimal("4")},
                {"Codigo": "UNKNOWN", "Nombre": "Producto no homologado", "Cantidad": Decimal("2")},
            ]
        )

        balance = service.build("2026-07")

        self.assertEqual(balance.rows[self.slice.id].sales, Decimal("4"))
        self.assertNotIn(self.parent.id, balance.rows)
        unresolved = next(item for item in balance.unresolved_movements if item.source == "official_sales")
        self.assertEqual(unresolved.item_code, "UNKNOWN")
        self.assertEqual(unresolved.quantity, Decimal("2"))
        self.assertEqual(balance.sources["sales"]["mode"], "official_monthly_report")
        self.assertEqual(balance.rows[self.slice.id].status, "REVISAR_FUENTE")
        with self.assertRaises(TypeError):
            balance.sources["sales"]["summary"]["venta"] = "mutada"

    def test_sales_falls_back_to_official_daily_then_facts(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "3", date(2026, 7, 3), "daily")
        service = MonthlyPointProductBalanceService(
            official_sales_report_service=_FailingOfficialSalesReportService()
        )

        daily_balance = service.build("2026-07")

        self.assertEqual(daily_balance.rows[self.parent.id].sales, Decimal("3"))
        self.assertEqual(daily_balance.sources["sales"]["mode"], "official_point_daily_sales")

        PointDailySale.objects.all().delete()
        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 4),
            sucursal=self.sucursal,
            receta=self.parent,
            vendido=Decimal("2"),
        )

        fact_balance = service.build("2026-07")

        self.assertEqual(fact_balance.rows[self.parent.id].sales, Decimal("2"))
        self.assertEqual(fact_balance.sources["sales"]["mode"], "production_facts")

    def test_auto_sales_mode_uses_persisted_daily_until_remote_refresh_is_requested(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "3", date(2026, 7, 3), "auto-daily")
        remote = _OfficialSalesReportService(
            [{"Codigo": self.parent.codigo_point, "Nombre": self.parent.nombre, "Cantidad": Decimal("2")}]
        )
        service = MonthlyPointProductBalanceService(official_sales_report_service=remote)

        persisted = service.build("2026-07")
        refreshed = service.build("2026-07", refresh_official_sales=True)

        self.assertEqual(remote.calls, [{"start_date": date(2026, 7, 1), "end_date": date(2026, 7, 31), "branch_external_id": None, "branch_display_name": None, "credito": None}])
        self.assertEqual(persisted.rows[self.parent.id].sales, Decimal("3"))
        self.assertEqual(persisted.sources["sales"]["configured_source_mode"], "AUTO")
        self.assertEqual(persisted.sources["sales"]["selection_reason"], "persisted_official_daily_sales")
        self.assertEqual(refreshed.rows[self.parent.id].sales, Decimal("2"))
        self.assertEqual(refreshed.sources["sales"]["selection_reason"], "remote_monthly_report")

    @override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="OFFICIAL_MONTHLY_REPORT")
    def test_strict_official_sales_mode_requires_refresh_and_marks_fallback_non_authoritative(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "3", date(2026, 7, 3), "strict-daily")
        remote = _OfficialSalesReportService(
            [{"Codigo": self.parent.codigo_point, "Nombre": self.parent.nombre, "Cantidad": Decimal("2")}]
        )
        service = MonthlyPointProductBalanceService(official_sales_report_service=remote)

        unrefreshed = service.build("2026-07")
        refreshed = service.build("2026-07", refresh_official_sales=True)

        self.assertEqual(remote.calls, [{"start_date": date(2026, 7, 1), "end_date": date(2026, 7, 31), "branch_external_id": None, "branch_display_name": None, "credito": None}])
        self.assertIn("OFFICIAL_SALES_REFRESH_REQUIRED", unrefreshed.issues)
        self.assertFalse(unrefreshed.sources["sales"]["authoritative"])
        self.assertEqual(unrefreshed.rows[self.parent.id].status, "REVISAR_FUENTE")
        self.assertTrue(refreshed.sources["sales"]["authoritative"])
        self.assertEqual(refreshed.sources["sales"]["mode"], "official_monthly_report")

    @override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="OFFICIAL_MONTHLY_REPORT")
    def test_strict_official_sales_mode_keeps_remote_failure_blocking_after_persisted_fallback(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "3", date(2026, 7, 3), "strict-failure-daily")
        service = MonthlyPointProductBalanceService(
            official_sales_report_service=_FailingOfficialSalesReportService(),
            refresh_official_sales=True,
        )

        balance = service.build("2026-07")

        self.assertEqual(balance.sources["sales"]["mode"], "official_point_daily_sales")
        self.assertFalse(balance.sources["sales"]["authoritative"])
        self.assertIn("OFFICIAL_SALES_REPORT_INVALID", balance.issues)
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    @override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="BRIDGE_HISTORY")
    def test_bridge_history_mode_skips_daily_sales_and_prefers_facts(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "8", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "99", date(2026, 7, 3), "daily-skipped")
        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 3),
            sucursal=self.sucursal,
            receta=self.parent,
            vendido=Decimal("2"),
        )
        remote = _UnexpectedOfficialSalesReportService()

        balance = MonthlyPointProductBalanceService(official_sales_report_service=remote).build("2026-07")

        self.assertEqual(remote.calls, 0)
        self.assertEqual(balance.rows[self.parent.id].sales, Decimal("2"))
        self.assertEqual(balance.sources["sales"]["mode"], "production_facts")
        self.assertEqual(balance.sources["sales"]["configured_source_mode"], "BRIDGE_HISTORY")

    def test_official_daily_sales_preserve_mixed_unmatched_rows(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "3", date(2026, 7, 3), "matched")
        unknown_product = PointProduct.objects.create(
            external_id="daily-unmatched",
            sku="DAILY-UNKNOWN",
            name="Venta diaria desconocida",
        )
        self._daily_sale(None, unknown_product, "4", date(2026, 7, 4), "unmatched")
        service = MonthlyPointProductBalanceService(
            official_sales_report_service=_FailingOfficialSalesReportService()
        )

        balance = service.build("2026-07")

        self.assertEqual(balance.rows[self.parent.id].sales, Decimal("3"))
        self.assertEqual(balance.sources["sales"]["mode"], "official_point_daily_sales")
        unresolved = next(item for item in balance.unresolved_movements if item.source == "official_daily_sales")
        self.assertEqual(unresolved.item_code, "DAILY-UNKNOWN")
        self.assertEqual(unresolved.item_name, "Venta diaria desconocida")
        self.assertEqual(unresolved.branch_external_id, self.branch.external_id)
        self.assertEqual(unresolved.movement_date, date(2026, 7, 4))
        self.assertEqual(unresolved.quantity, Decimal("4"))
        self.assertEqual(unresolved.issue, "SALES_DESTINATION_UNRESOLVED")
        self.assertEqual(balance.source_counts["official_daily_sales_unresolved"], 1)
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")
        self.assertIn("MONTH_SOURCE_INCOMPLETE", balance.rows[self.parent.id].issues)

    def test_all_unmatched_official_daily_sales_do_not_fall_through_to_facts(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        unknown_product = PointProduct.objects.create(
            external_id="daily-all-unmatched",
            sku="DAILY-ONLY-UNKNOWN",
            name="Única venta desconocida",
        )
        self._daily_sale(None, unknown_product, "5", date(2026, 7, 5), "only-unmatched")
        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 5),
            sucursal=self.sucursal,
            receta=self.parent,
            vendido=Decimal("99"),
        )
        service = MonthlyPointProductBalanceService(
            official_sales_report_service=_FailingOfficialSalesReportService()
        )

        balance = service.build("2026-07")

        self.assertEqual(balance.rows[self.parent.id].sales, Decimal("0"))
        self.assertEqual(balance.sources["sales"]["mode"], "official_point_daily_sales")
        self.assertEqual(balance.source_counts["official_daily_sales_unresolved"], 1)
        self.assertTrue(any(item.source == "official_daily_sales" for item in balance.unresolved_movements))

    def test_default_build_never_fetches_remote_official_report(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        remote = _UnexpectedOfficialSalesReportService()
        service = MonthlyPointProductBalanceService(official_sales_report_service=remote)

        balance = service.build("2026-07")

        self.assertEqual(remote.calls, 0)
        self.assertEqual(balance.rows[self.parent.id].status, "COINCIDE")

    def test_explicit_empty_remote_report_is_blocking_and_falls_back(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "3", date(2026, 7, 3), "daily-after-empty")
        remote = _OfficialSalesReportService([])
        service = MonthlyPointProductBalanceService(
            official_sales_report_service=remote,
            refresh_official_sales=True,
        )

        balance = service.build("2026-07")

        self.assertEqual(len(remote.calls), 1)
        self.assertEqual(balance.sources["sales"]["mode"], "official_point_daily_sales")
        self.assertIn("OFFICIAL_SALES_REPORT_EMPTY", balance.issues)
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_explicit_malformed_remote_report_is_blocking_and_falls_back(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "7", datetime(2026, 7, 31, 8))
        self._daily_sale(self.parent, self.parent_product, "3", date(2026, 7, 3), "daily-after-malformed")
        remote = _MalformedOfficialSalesReportService()
        service = MonthlyPointProductBalanceService(
            official_sales_report_service=remote,
            refresh_official_sales=True,
        )

        balance = service.build("2026-07")

        self.assertEqual(remote.calls, 1)
        self.assertEqual(balance.sources["sales"]["mode"], "official_point_daily_sales")
        self.assertIn("OFFICIAL_SALES_REPORT_INVALID", balance.issues)
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_unmatched_fact_movements_are_preserved_and_block_all_rows(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        FactProduccionDiaria.objects.create(
            fecha=date(2026, 7, 10),
            sucursal=self.sucursal,
            receta=None,
            producido=Decimal("4"),
            merma=Decimal("2"),
        )
        service, _official = self._service()

        balance = service.build("2026-07")

        fact_sources = {item.source for item in balance.unresolved_movements if item.issue == "FACT_RECIPE_UNRESOLVED"}
        self.assertEqual(fact_sources, {"fact_production", "fact_waste"})
        production_issue = next(item for item in balance.unresolved_movements if item.source == "fact_production")
        self.assertEqual(production_issue.branch_external_id, self.sucursal.codigo)
        self.assertEqual(production_issue.movement_date, date(2026, 7, 10))
        self.assertEqual(production_issue.quantity, Decimal("4"))
        self.assertTrue(balance.sources["production"]["source_present"])
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_all_unmatched_point_production_is_source_present_and_blocking(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        PointProductionLine.objects.create(
            branch=self.branch,
            erp_branch=self.sucursal,
            receta=None,
            production_external_id="prod-unmatched",
            detail_external_id="detail-unmatched",
            source_hash="prod-unmatched-hash",
            production_date=date(2026, 7, 10),
            item_name="Producción desconocida",
            item_code="PROD-UNKNOWN",
            produced_quantity=Decimal("4"),
        )
        service, _official = self._service()

        balance = service.build("2026-07")

        unresolved = next(item for item in balance.unresolved_movements if item.source == "point_production")
        self.assertEqual(unresolved.issue, "PRODUCTION_RECIPE_UNRESOLVED")
        self.assertEqual(unresolved.item_code, "PROD-UNKNOWN")
        self.assertEqual(unresolved.branch_external_id, self.branch.external_id)
        self.assertTrue(balance.sources["production"]["source_present"])
        self.assertEqual(balance.sources["production"]["rows_read"], 1)
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_all_unmatched_authoritative_source_exposes_global_incomplete_issue(self):
        PointProductionLine.objects.create(
            branch=self.branch,
            erp_branch=self.sucursal,
            receta=None,
            production_external_id="prod-only-unmatched",
            detail_external_id="detail-only-unmatched",
            source_hash="prod-only-unmatched-hash",
            production_date=date(2026, 7, 10),
            item_name="Producción sin receta",
            item_code="PROD-ONLY-UNKNOWN",
            produced_quantity=Decimal("4"),
        )
        service, _official = self._service()

        balance = service.build("2026-07")

        self.assertEqual(balance.rows, {})
        self.assertTrue(balance.sources["production"]["source_present"])
        self.assertIn("PRODUCTION_RECIPE_UNRESOLVED", balance.issues)
        self.assertIn("MONTH_SOURCE_INCOMPLETE", balance.issues)

    def test_all_unmatched_point_and_monthly_waste_are_preserved(self):
        self._snapshot(self.parent_product, "10", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "10", datetime(2026, 7, 31, 8))
        PointWasteLine.objects.create(
            branch=self.branch,
            erp_branch=self.sucursal,
            receta=None,
            movement_external_id="waste-unmatched",
            source_hash="waste-unmatched-hash",
            movement_at=timezone.make_aware(datetime(2026, 7, 10, 12), timezone.get_current_timezone()),
            item_name="Merma desconocida",
            item_code="WASTE-UNKNOWN",
            quantity=Decimal("2"),
        )
        service, _official = self._service()

        point_balance = service.build("2026-07")

        point_issue = next(item for item in point_balance.unresolved_movements if item.source == "point_waste")
        self.assertEqual(point_issue.issue, "WASTE_RECIPE_UNRESOLVED")
        self.assertTrue(point_balance.sources["waste"]["source_present"])
        self.assertEqual(point_balance.rows[self.parent.id].status, "REVISAR_FUENTE")

        PointWasteLine.objects.all().delete()
        MermaMensualSucursal.objects.create(
            periodo=date(2026, 7, 1),
            sucursal=self.sucursal,
            receta=None,
            nombre_producto="Merma mensual desconocida",
            unidades_merma=Decimal("3"),
        )

        monthly_balance = service.build("2026-07")

        monthly_issue = next(item for item in monthly_balance.unresolved_movements if item.source == "monthly_waste")
        self.assertEqual(monthly_issue.issue, "WASTE_RECIPE_UNRESOLVED")
        self.assertEqual(monthly_issue.item_name, "Merma mensual desconocida")
        self.assertTrue(monthly_balance.sources["waste"]["source_present"])
        self.assertEqual(monthly_balance.rows[self.parent.id].status, "REVISAR_FUENTE")

    def test_unmatched_snapshot_remains_visible_in_immutable_diagnostics(self):
        unknown = PointProduct.objects.create(external_id="unknown", sku="UNKNOWN", name="Producto desconocido")
        self._snapshot(unknown, "5", datetime(2026, 6, 30, 8))
        self._snapshot(self.parent_product, "2", datetime(2026, 7, 31, 8))
        service, _official = self._service()

        balance = service.build("2026-07")

        unresolved = next(item for item in balance.unresolved_movements if item.source == "opening_snapshot")
        self.assertEqual(unresolved.item_code, "UNKNOWN")
        self.assertEqual(unresolved.quantity, Decimal("5"))
        self.assertEqual(unresolved.branch_external_id, self.branch.external_id)
        self.assertEqual(balance.source_counts["opening_snapshot_unresolved"], 1)
        self.assertEqual(balance.sources["opening_snapshot"]["selected_rows"], 1)
        self.assertEqual(balance.sources["opening_snapshot"]["applied_rows"], 0)
        self.assertEqual(balance.rows[self.parent.id].status, "REVISAR_FUENTE")
        self.assertIn("MONTH_SOURCE_INCOMPLETE", balance.rows[self.parent.id].issues)
        with self.assertRaises(TypeError):
            balance.sources["sales"] = {}
