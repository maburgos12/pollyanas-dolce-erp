from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.db import connection, models
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import Sucursal
from maestros.models import Insumo
from pos_bridge.models import (
    PointBranch,
    PointConversionLine,
    PointDailySale,
    PointExtractionLog,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointProduct,
    PointProductionLine,
    PointSyncJob,
    PointTransferLine,
    PointWasteLine,
)
from pos_bridge.services.branch_inventory_traceability_service import (
    BranchInventoryTraceabilityService,
)
from pos_bridge.utils.dates import iter_business_dates
from recetas.models import Receta, RecetaEquivalencia
from ventas.services.sales_canonical_source import (
    OFFICIAL_POINT_SOURCE,
    RECENT_POINT_SOURCE,
)


class BranchInventoryTraceabilityServiceTests(TestCase):
    def setUp(self):
        self.service = BranchInventoryTraceabilityService()
        self.centro_erp = Sucursal.objects.create(codigo="CENTRO", nombre="Centro")
        self.plaza_erp = Sucursal.objects.create(codigo="PLAZA", nombre="Plaza")
        self.centro = PointBranch.objects.create(
            external_id="CENTRO", name="Centro", erp_branch=self.centro_erp
        )
        self.plaza = PointBranch.objects.create(
            external_id="PLAZA", name="Plaza", erp_branch=self.plaza_erp
        )
        self.product = PointProduct.objects.create(
            external_id="PASTEL-001",
            sku="PASTEL-001",
            name="Pastel de prueba",
        )
        self.sales_job = self._sales_job()
        self.production_job = self._movement_job("production")
        self.waste_job = self._movement_job("waste")
        self.transfer_job = self._movement_job("transfers")
        self.conversion_job = self._movement_job("conversions")

    def _movement_job(
        self,
        family,
        *,
        status=PointSyncJob.STATUS_SUCCESS,
        branch_filter="",
        rows_seen=0,
    ):
        job_type, count_key = {
            "production": (
                PointSyncJob.JOB_TYPE_PRODUCTION,
                "production_lines_seen",
            ),
            "waste": (PointSyncJob.JOB_TYPE_WASTE, "waste_lines_seen"),
            "transfers": (
                PointSyncJob.JOB_TYPE_TRANSFERS,
                "transfer_lines_seen",
            ),
            "conversions": (
                PointSyncJob.JOB_TYPE_INVENTORY,
                "total_rows",
            ),
        }[family]
        parameters = {
            "start_date": "2026-08-01",
            "end_date": "2026-08-31",
            "branch_filter": branch_filter,
        }
        result_summary = {count_key: rows_seen}
        if family == "transfers":
            result_summary.update(
                {
                    "transfer_lines_created": rows_seen,
                    "transfer_lines_updated": 0,
                }
            )
        if family == "conversions":
            parameters = {
                "source": "point_conversion_lines",
                "date_from": "2026-08-01",
                "date_to": "2026-08-31",
                "branch_filter": branch_filter,
            }
            result_summary.update(
                {
                    "created": rows_seen,
                    "skipped": 0,
                    "relinked": 0,
                    "skipped_unmatched_branch": 0,
                    "invalid_rows": 0,
                    "report_pk": f"report-{PointSyncJob.objects.count() + 1}",
                }
            )
        return PointSyncJob.objects.create(
            job_type=job_type,
            status=status,
            parameters=parameters,
            result_summary=result_summary,
        )

    def _sales_job(self, *, status=PointSyncJob.STATUS_SUCCESS, branch_filter=""):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=status,
            parameters={
                "source": "POINT_OFFICIAL_REPORT",
                "start_date": "2026-08-01",
                "end_date": "2026-08-31",
                "branch_filter": branch_filter,
                "credito_scopes": ["null"],
                "excluded_ranges": [],
                "max_days": None,
            },
            result_summary={},
        )
        self._refresh_sales_evidence(job)
        return job

    def _refresh_sales_evidence(self, job):
        month_days = iter_business_dates(date(2026, 8, 1), date(2026, 8, 31))
        branches = (self.centro, self.plaza)
        rows = PointDailySale.objects.filter(
            sync_job=job,
            source_endpoint=OFFICIAL_POINT_SOURCE,
            sale_date__gte=date(2026, 8, 1),
            sale_date__lte=date(2026, 8, 31),
        )
        counts = {
            (branch_id, sale_date): count
            for branch_id, sale_date, count in rows.values_list(
                "branch_id", "sale_date"
            ).annotate(count=models.Count("id"))
        }
        PointExtractionLog.objects.filter(sync_job=job).delete()
        logs = []
        for sale_date in month_days:
            for branch in branches:
                logs.append(
                    PointExtractionLog(
                        sync_job=job,
                        level=PointExtractionLog.LEVEL_INFO,
                        message=(
                            f"Backfill oficial {branch.external_id} "
                            f"{sale_date.isoformat()}"
                        ),
                        context={
                            "branch": branch.name,
                            "branch_external_id": branch.external_id,
                            "sale_date": sale_date.isoformat(),
                            "rows_imported": counts.get((branch.id, sale_date), 0),
                            "rows_deleted": 0,
                            "reports_downloaded": 1,
                        },
                    )
                )
        PointExtractionLog.objects.bulk_create(logs)
        job.result_summary = {
            "branch_days_processed": len(logs),
            "failed_branch_days": 0,
            "rows_imported": rows.count(),
            "rows_deleted": 0,
            "indicator_rows_created": 0,
            "indicator_rows_updated": 0,
            "reports_downloaded": len(logs),
            "raw_exports": [],
            "failures": [],
        }
        job.save(update_fields=["result_summary", "updated_at"])

    @staticmethod
    def _increment_movement_job(job, count_key):
        summary = dict(job.result_summary or {})
        summary[count_key] = int(summary.get(count_key) or 0) + 1
        job.result_summary = summary
        job.save(update_fields=["result_summary", "updated_at"])

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

    def _sale(
        self,
        *,
        branch=None,
        product=None,
        quantity="1",
        sale_date=None,
        source_endpoint=OFFICIAL_POINT_SOURCE,
        sync_job=...,
    ):
        sale_date = sale_date or date(2026, 8, PointDailySale.objects.count() + 1)
        if sync_job is ...:
            sync_job = (
                self.sales_job
                if source_endpoint == OFFICIAL_POINT_SOURCE
                and date(2026, 8, 1) <= sale_date <= date(2026, 8, 31)
                else None
            )
        row = PointDailySale.objects.create(
            branch=branch or self.centro,
            product=product or self.product,
            sync_job=sync_job,
            sale_date=sale_date,
            quantity=Decimal(quantity),
            source_endpoint=source_endpoint,
        )
        if sync_job == self.sales_job:
            self._refresh_sales_evidence(self.sales_job)
        return row

    def _production(
        self,
        *,
        branch=None,
        item_code=None,
        item_name=None,
        quantity="1",
        production_date=date(2026, 8, 11),
        is_insumo=False,
        sync_job=...,
    ):
        if sync_job is ...:
            sync_job = (
                self.production_job
                if date(2026, 8, 1) <= production_date <= date(2026, 8, 31)
                else None
            )
        row = PointProductionLine.objects.create(
            branch=branch or self.centro,
            sync_job=sync_job,
            production_external_id=f"production-{PointProductionLine.objects.count() + 1}",
            detail_external_id=f"detail-{PointProductionLine.objects.count() + 1}",
            source_hash=f"production-hash-{PointProductionLine.objects.count() + 1}",
            production_date=production_date,
            item_code=self.product.external_id if item_code is None else item_code,
            item_name=self.product.name if item_name is None else item_name,
            produced_quantity=Decimal(quantity),
            is_insumo=is_insumo,
        )
        if sync_job == self.production_job:
            self._increment_movement_job(self.production_job, "production_lines_seen")
        return row

    def _waste(
        self,
        *,
        branch=None,
        item_code=None,
        item_name=None,
        quantity="1",
        responsible="",
        movement_at=None,
        receta=None,
        insumo=None,
        sync_job=...,
    ):
        sequence = PointWasteLine.objects.count() + 1
        movement_at = movement_at or datetime(
            2026, 8, 12, 12, 0, tzinfo=timezone.get_current_timezone()
        )
        local_date = timezone.localtime(movement_at).date()
        if sync_job is ...:
            sync_job = (
                self.waste_job
                if date(2026, 8, 1) <= local_date <= date(2026, 8, 31)
                else None
            )
        row = PointWasteLine.objects.create(
            branch=branch or self.centro,
            sync_job=sync_job,
            receta=receta,
            insumo=insumo,
            movement_external_id=f"waste-{sequence}",
            source_hash=f"waste-hash-{sequence}",
            movement_at=movement_at,
            item_code=self.product.external_id if item_code is None else item_code,
            item_name=self.product.name if item_name is None else item_name,
            quantity=Decimal(quantity),
            responsible=responsible,
        )
        if sync_job == self.waste_job:
            self._increment_movement_job(self.waste_job, "waste_lines_seen")
        return row

    def _transfer(
        self,
        *,
        origin=None,
        destination=None,
        sent_quantity="4",
        received_quantity="4",
        sent_at=...,
        received_at=...,
        registered_at=None,
        is_received=True,
        is_cancelled=False,
        is_finalized=True,
        is_current_snapshot=True,
        is_insumo=False,
        sync_job=...,
        transfer_external_id=None,
    ):
        sequence = PointTransferLine.objects.count() + 1
        registered_at = registered_at or datetime(
            2026, 8, 10, 8, 0, tzinfo=timezone.get_current_timezone()
        )
        if sent_at is ...:
            sent_at = datetime(
                2026, 8, 10, 9, 0, tzinfo=timezone.get_current_timezone()
            )
        if received_at is ...:
            received_at = (
                datetime(2026, 8, 10, 12, 0, tzinfo=timezone.get_current_timezone())
                if is_received
                else None
            )
        if sync_job is ...:
            sync_job = self.transfer_job
        row = PointTransferLine.objects.create(
            origin_branch=origin or self.centro,
            destination_branch=destination or self.plaza,
            sync_job=sync_job,
            transfer_external_id=transfer_external_id or f"transfer-{sequence}",
            detail_external_id=f"transfer-detail-{sequence}",
            source_hash=f"transfer-hash-{sequence}",
            registered_at=registered_at,
            sent_at=sent_at,
            received_at=received_at,
            item_name=self.product.name,
            item_code=self.product.external_id,
            sent_quantity=Decimal(sent_quantity),
            received_quantity=Decimal(received_quantity),
            is_received=is_received,
            is_cancelled=is_cancelled,
            is_finalized=is_finalized,
            is_current_snapshot=is_current_snapshot,
            is_insumo=is_insumo,
        )
        if sync_job == self.transfer_job:
            self._increment_movement_job(self.transfer_job, "transfer_lines_seen")
            self._increment_movement_job(self.transfer_job, "transfer_lines_created")
        return row

    def _conversion(
        self,
        *,
        branch=None,
        product=None,
        quantity="12",
        source_item_code="",
        source_item_name="",
        movement_at=None,
        sync_job=...,
    ):
        sequence = PointConversionLine.objects.count() + 1
        product = product or self.product
        movement_at = movement_at or datetime(
            2026, 8, 15, 12, 0, tzinfo=timezone.get_current_timezone()
        )
        if sync_job is ...:
            sync_job = self.conversion_job
        row = PointConversionLine.objects.create(
            branch=branch or self.centro,
            sync_job=sync_job,
            movement_external_id=f"conversion-{sequence}",
            source_hash=f"conversion-hash-{sequence}",
            movement_at=movement_at,
            item_name=product.name,
            item_code=product.external_id,
            quantity=Decimal(quantity),
            source_item_code=source_item_code,
            source_item_name=source_item_name,
        )
        if sync_job == self.conversion_job:
            summary = dict(self.conversion_job.result_summary)
            summary["total_rows"] += 1
            summary["created"] += 1
            self.conversion_job.result_summary = summary
            self.conversion_job.save(update_fields=["result_summary", "updated_at"])
        return row

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

    def test_production_input_row_is_excluded_without_product_issue(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        production_input = self._production(
            item_code=self.product.external_id,
            item_name=self.product.name,
            quantity="7",
            is_insumo=True,
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertTrue(result.source_complete)
        self.assertEqual(result.global_issues, ())
        self.assertEqual(result.lines[0].production, Decimal("0"))
        self.assertNotIn(
            production_input.id, result.lines[0].source_trace["production"]
        )

    def test_ingredient_waste_is_excluded_but_finished_product_waste_is_applied(self):
        recipe = Receta.objects.create(
            nombre=self.product.name,
            codigo_point=self.product.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="traceability-finished-product",
        )
        ingredient = Insumo.objects.create(
            nombre="Ingrediente interno",
            nombre_point=self.product.name,
            codigo_point=self.product.external_id,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        ingredient_waste = self._waste(
            item_code=self.product.external_id,
            item_name=self.product.name,
            quantity="6",
            insumo=ingredient,
        )
        finished_waste = self._waste(
            item_code=self.product.external_id,
            item_name=self.product.name,
            quantity="2",
            receta=recipe,
        )

        result = self.service.build(month=date(2026, 8, 1))

        line = result.lines[0]
        self.assertTrue(result.source_complete)
        self.assertEqual(result.global_issues, ())
        self.assertEqual(line.waste, Decimal("2"))
        self.assertEqual(line.source_trace["waste"], (finished_waste.id,))
        self.assertNotIn(ingredient_waste.id, line.source_trace["waste"])

    def test_dual_mapped_waste_uses_exact_point_product_evidence(self):
        recipe = Receta.objects.create(
            nombre=self.product.name,
            codigo_point=self.product.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="traceability-dual-mapped-product",
        )
        synchronizer_input = Insumo.objects.create(
            nombre=self.product.name,
            nombre_point=self.product.name,
            codigo_point=self.product.external_id,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("8")})
        dual_mapped_waste = self._waste(
            item_code=self.product.external_id,
            item_name=self.product.name,
            quantity="2",
            receta=recipe,
            insumo=synchronizer_input,
        )

        result = self.service.build(month=date(2026, 8, 1))

        line = result.lines[0]
        self.assertTrue(result.source_complete)
        self.assertEqual(result.global_issues, ())
        self.assertEqual(line.waste, Decimal("2"))
        self.assertEqual(line.expected_closing, Decimal("8"))
        self.assertEqual(line.difference, Decimal("0"))
        self.assertEqual(line.source_trace["waste"], (dual_mapped_waste.id,))

    def test_unresolved_product_is_global_issue_and_not_added_to_product_lines(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        unresolved = self._production(
            item_code="UNKNOWN-999",
            item_name="Producto que no existe",
            quantity="6",
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertTrue(result.source_complete)
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

        self.assertTrue(result.source_complete)
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
        sku_issue = by_product[sku_product.id].issues[0]
        self.assertEqual(sku_issue.code, "PRODUCT_RESOLVED_BY_SKU")
        self.assertEqual(sku_issue.branch_id, self.centro.id)
        self.assertEqual(sku_issue.product_id, sku_product.id)
        self.assertEqual(sku_issue.source_ids, (sku_match.id,))
        self.assertEqual(by_product[name_product.id].production, Decimal("4"))
        self.assertEqual(
            by_product[name_product.id].source_trace["production"],
            (name_match.id,),
        )
        name_issue = by_product[name_product.id].issues[0]
        self.assertEqual(name_issue.code, "PRODUCT_RESOLVED_BY_NAME")
        self.assertEqual(name_issue.branch_id, self.centro.id)
        self.assertEqual(name_issue.product_id, name_product.id)
        self.assertEqual(name_issue.source_ids, (name_match.id,))
        self.assertEqual(by_product[self.product.id].issues, ())
        self.assertTrue(result.source_complete)
        self.assertNotIn(misleading_name_product.id, by_product)

    def test_noncanonical_sales_row_makes_required_sales_source_incomplete(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        self._sale(
            quantity="50",
            sale_date=date(2026, 8, 11),
            source_endpoint=RECENT_POINT_SOURCE,
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        self.assertEqual(result.global_issues[0].code, "SOURCE_INCOMPLETE")
        self.assertIn("SALES_SOURCE_MIXED", result.global_issues[0].message)

    def test_rows_outside_requested_month_are_excluded(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        included_sale = self._sale(quantity="2", sale_date=date(2026, 8, 10))
        self._sale(quantity="60", sale_date=date(2026, 7, 31))
        self._sale(quantity="70", sale_date=date(2026, 9, 1))
        included_production = self._production(
            quantity="3", production_date=date(2026, 8, 1)
        )
        self._production(quantity="80", production_date=date(2026, 7, 31))
        self._production(quantity="90", production_date=date(2026, 9, 1))
        included_waste = self._waste(
            quantity="1",
            movement_at=datetime(
                2026, 8, 31, 23, 59, tzinfo=timezone.get_current_timezone()
            ),
        )
        self._waste(
            quantity="100",
            movement_at=datetime(
                2026, 7, 31, 23, 59, tzinfo=timezone.get_current_timezone()
            ),
        )
        self._waste(
            quantity="110",
            movement_at=datetime(
                2026, 9, 1, 0, 0, tzinfo=timezone.get_current_timezone()
            ),
        )

        line = self.service.build(month=date(2026, 8, 1)).lines[0]

        self.assertEqual(line.sales, Decimal("2"))
        self.assertEqual(line.production, Decimal("3"))
        self.assertEqual(line.waste, Decimal("1"))
        self.assertEqual(line.source_trace["sales"], (included_sale.id,))
        self.assertEqual(line.source_trace["production"], (included_production.id,))
        self.assertEqual(line.source_trace["waste"], (included_waste.id,))

    def test_missing_required_movement_authority_stops_calculation(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        PointSyncJob.objects.all().delete()

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        self.assertTrue(result.global_issues)
        self.assertTrue(
            all(issue.code == "SOURCE_INCOMPLETE" for issue in result.global_issues)
        )
        messages = " ".join(issue.message for issue in result.global_issues)
        self.assertIn("SALES_SYNC_JOB_MISSING", messages)
        self.assertIn("PRODUCTION_SYNC_JOB_MISSING", messages)
        self.assertIn("WASTE_SYNC_JOB_MISSING", messages)

    def test_failed_partial_restricted_and_count_mismatched_sources_are_rejected(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        cases = (
            (
                self.sales_job,
                "status",
                PointSyncJob.STATUS_FAILED,
                "SALES_SYNC_JOB_FAILED",
            ),
            (
                self.production_job,
                "status",
                PointSyncJob.STATUS_PARTIAL,
                "PRODUCTION_SYNC_JOB_PARTIAL",
            ),
            (
                self.waste_job,
                "branch_filter",
                self.centro.external_id,
                "WASTE_SYNC_JOB_RESTRICTED",
            ),
            (
                self.production_job,
                "rows_seen",
                1,
                "PRODUCTION_SYNC_COUNT_MISMATCH",
            ),
        )
        for job, field, value, expected_issue in cases:
            with self.subTest(expected_issue=expected_issue):
                original_status = job.status
                original_parameters = dict(job.parameters or {})
                original_summary = dict(job.result_summary or {})
                if field == "status":
                    job.status = value
                elif field == "branch_filter":
                    job.parameters = {**original_parameters, "branch_filter": value}
                else:
                    job.result_summary = {
                        **original_summary,
                        "production_lines_seen": value,
                    }
                job.save()

                result = self.service.build(month=date(2026, 8, 1))

                self.assertFalse(result.source_complete)
                self.assertEqual(result.lines, ())
                self.assertIn(
                    expected_issue,
                    " ".join(issue.message for issue in result.global_issues),
                )
                job.status = original_status
                job.parameters = original_parameters
                job.result_summary = original_summary
                job.save()

    def test_incomplete_sales_coverage_and_mixed_production_jobs_are_rejected(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        PointExtractionLog.objects.filter(sync_job=self.sales_job).first().delete()

        incomplete_sales = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(incomplete_sales.source_complete)
        self.assertIn(
            "SALES_SYNC_COVERAGE_UNPROVEN",
            " ".join(issue.message for issue in incomplete_sales.global_issues),
        )

        self._refresh_sales_evidence(self.sales_job)
        foreign_job = self._movement_job("production", rows_seen=1)
        self._production(quantity="1", sync_job=self.production_job)
        foreign_job.started_at = timezone.now() + timedelta(seconds=1)
        foreign_job.save(update_fields=["started_at", "updated_at"])

        mixed_production = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(mixed_production.source_complete)
        self.assertIn(
            "PRODUCTION_SYNC_JOB_MIXED",
            " ".join(issue.message for issue in mixed_production.global_issues),
        )

    def test_authoritative_zero_month_requires_and_accepts_canonical_evidence(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})

        result = self.service.build(month=date(2026, 8, 1))

        self.assertTrue(result.source_complete)
        self.assertEqual(result.global_issues, ())
        self.assertEqual(result.lines[0].sales, Decimal("0"))
        self.assertEqual(result.lines[0].production, Decimal("0"))
        self.assertEqual(result.lines[0].waste, Decimal("0"))

    def test_high_volume_source_evidence_is_frozen_once_as_tuple(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        rows = PointProductionLine.objects.bulk_create(
            [
                PointProductionLine(
                    branch=self.centro,
                    sync_job=self.production_job,
                    production_external_id=f"bulk-{number}",
                    detail_external_id=f"bulk-detail-{number}",
                    source_hash=f"bulk-hash-{number}",
                    production_date=date(2026, 8, 15),
                    item_code=self.product.external_id,
                    item_name=self.product.name,
                    produced_quantity=Decimal("1"),
                )
                for number in range(250)
            ]
        )
        self.production_job.result_summary = {"production_lines_seen": len(rows)}
        self.production_job.save(update_fields=["result_summary", "updated_at"])

        line = self.service.build(month=date(2026, 8, 1)).lines[0]

        self.assertEqual(line.production, Decimal("250"))
        self.assertEqual(line.source_trace["production"], tuple(row.id for row in rows))

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

    def test_complete_transfer_moves_stock_between_point_locations_without_company_change(self):
        stocks = {
            (self.centro, self.product): Decimal("10"),
            (self.plaza, self.product): Decimal("10"),
        }
        self._closing_lines(date(2026, 7, 31), stocks)
        self._closing_lines(
            date(2026, 8, 31),
            {
                (self.centro, self.product): Decimal("6"),
                (self.plaza, self.product): Decimal("14"),
            },
        )
        transfer = self._transfer(
            origin=self.centro,
            destination=self.plaza,
            sent_quantity="4",
            received_quantity="4",
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in result.lines}
        self.assertEqual(by_branch["CENTRO"].transfer_out, Decimal("4"))
        self.assertEqual(by_branch["CENTRO"].transfer_in, Decimal("0"))
        self.assertEqual(by_branch["CENTRO"].expected_closing, Decimal("6"))
        self.assertEqual(by_branch["PLAZA"].transfer_in, Decimal("4"))
        self.assertEqual(by_branch["PLAZA"].transfer_out, Decimal("0"))
        self.assertEqual(by_branch["PLAZA"].expected_closing, Decimal("14"))
        self.assertEqual(by_branch["CENTRO"].source_trace["transfers"], (transfer.id,))
        self.assertEqual(by_branch["PLAZA"].source_trace["transfers"], (transfer.id,))
        self.assertEqual(result.company_difference, Decimal("0"))

    def test_devoluciones_remains_auditable_point_location_and_is_not_waste(self):
        devoluciones = PointBranch.objects.create(
            external_id="DEVOLUCIONES",
            name="  Devoluciónes  ",
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("8")})
        transfer = self._transfer(
            origin=self.centro,
            destination=devoluciones,
            sent_quantity="2",
            received_quantity="2",
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in result.lines}
        self.assertEqual(by_branch["CENTRO"].transfer_out, Decimal("2"))
        self.assertEqual(by_branch["DEVOLUCIONES"].transfer_in, Decimal("2"))
        self.assertEqual(by_branch["DEVOLUCIONES"].waste, Decimal("0"))
        self.assertEqual(by_branch["DEVOLUCIONES"].source_trace["transfers"], (transfer.id,))
        self.assertIsNone(by_branch["DEVOLUCIONES"].branch.erp_branch_id)

    def test_incomplete_transfer_records_only_origin_and_auditable_issue(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("6")})
        transfer = self._transfer(
            sent_quantity="4",
            received_quantity="0",
            is_received=False,
            received_at=None,
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in result.lines}
        self.assertEqual(by_branch["CENTRO"].transfer_out, Decimal("4"))
        self.assertNotIn("PLAZA", by_branch)
        issue = by_branch["CENTRO"].issues[0]
        self.assertEqual(issue.code, "INCOMPLETE_TRANSFER")
        self.assertEqual(issue.source_ids, (transfer.id,))
        self.assertIn(transfer.transfer_external_id, issue.message)
        self.assertIn(transfer.detail_external_id, issue.message)

    def test_finalized_transfer_without_sent_date_uses_registered_date_with_issue(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("7")})
        transfer = self._transfer(
            sent_quantity="3",
            received_quantity="0",
            sent_at=None,
            received_at=None,
            is_received=False,
            is_finalized=True,
        )

        line = self.service.build(month=date(2026, 8, 1)).lines[0]

        self.assertEqual(line.transfer_out, Decimal("3"))
        self.assertEqual(
            {issue.code for issue in line.issues},
            {"INCOMPLETE_TRANSFER", "TRANSFER_DATE_FALLBACK"},
        )
        self.assertTrue(all(issue.source_ids == (transfer.id,) for issue in line.issues))

    def test_cancelled_stale_and_ingredient_transfer_rows_only_count_for_authority(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        rows = (
            self._transfer(is_cancelled=True),
            self._transfer(is_current_snapshot=False),
            self._transfer(is_insumo=True),
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertTrue(result.source_complete)
        self.assertEqual(result.lines[0].transfer_in, Decimal("0"))
        self.assertEqual(result.lines[0].transfer_out, Decimal("0"))
        self.assertTrue(
            all(row.id not in result.lines[0].source_trace["transfers"] for row in rows)
        )

    def test_transfer_legs_use_their_own_operational_month(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("6")})
        transfer = self._transfer(
            sent_quantity="4",
            received_quantity="4",
            sent_at=datetime(2026, 8, 31, 23, 0, tzinfo=timezone.get_current_timezone()),
            received_at=datetime(2026, 9, 1, 0, 5, tzinfo=timezone.get_current_timezone()),
        )

        august = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in august.lines}
        self.assertEqual(by_branch["CENTRO"].transfer_out, Decimal("4"))
        self.assertNotIn("PLAZA", by_branch)
        self.assertEqual(by_branch["CENTRO"].source_trace["transfers"], (transfer.id,))

    def test_exact_month_transfer_contract_is_independent_of_later_operational_legs(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        self._transfer(
            sent_at=datetime(2026, 9, 2, 9, 0, tzinfo=timezone.get_current_timezone()),
            received_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.get_current_timezone()),
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertTrue(result.source_complete)
        self.assertEqual(result.lines[0].transfer_in, Decimal("0"))
        self.assertEqual(result.lines[0].transfer_out, Decimal("0"))

    def test_cross_month_origin_accepts_row_rebound_to_another_full_transfer_job(self):
        september_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_SUCCESS,
            parameters={
                "start_date": "2026-09-01",
                "end_date": "2026-09-30",
                "branch_filter": "",
            },
            result_summary={
                "transfer_lines_seen": 1,
                "transfer_lines_created": 0,
                "transfer_lines_updated": 1,
            },
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("6")})
        transfer = self._transfer(
            sent_at=datetime(2026, 8, 31, 23, 0, tzinfo=timezone.get_current_timezone()),
            received_at=datetime(2026, 9, 1, 0, 5, tzinfo=timezone.get_current_timezone()),
            sync_job=september_job,
        )

        result = self.service.build(month=date(2026, 8, 1))

        self.assertTrue(result.source_complete)
        line = result.lines[0]
        self.assertEqual(line.transfer_out, Decimal("4"))
        self.assertEqual(line.source_trace["transfers"], (transfer.id,))

    def test_operational_transfer_without_sync_provenance_blocks_calculation(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("6")})
        transfer = self._transfer(sync_job=None)

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        issue = next(
            issue
            for issue in result.global_issues
            if "TRANSFER_ROW_PROVENANCE_MISSING" in issue.message
        )
        self.assertEqual(issue.source_ids, (transfer.id,))

    def test_received_flag_without_received_date_keeps_origin_incomplete(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("6")})
        transfer = self._transfer(
            is_received=True,
            received_at=None,
            sent_quantity="4",
            received_quantity="4",
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_branch = {line.branch.external_id: line for line in result.lines}
        self.assertEqual(by_branch["CENTRO"].transfer_out, Decimal("4"))
        self.assertNotIn("PLAZA", by_branch)
        issue = next(
            issue
            for issue in by_branch["CENTRO"].issues
            if issue.code == "INCOMPLETE_TRANSFER"
        )
        self.assertEqual(issue.source_ids, (transfer.id,))

    def test_conversion_uses_configured_equivalence_for_both_product_legs(self):
        whole = PointProduct.objects.create(
            external_id="WHOLE-001", sku="WHOLE-001", name="Pastel entero"
        )
        slice_product = PointProduct.objects.create(
            external_id="SLICE-001", sku="SLICE-001", name="Rebanada de pastel"
        )
        whole_recipe = Receta.objects.create(
            nombre=whole.name,
            codigo_point=whole.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="trace-whole",
        )
        slice_recipe = Receta.objects.create(
            nombre=slice_product.name,
            codigo_point=slice_product.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="trace-slice",
        )
        RecetaEquivalencia.objects.create(
            receta_porcion=slice_recipe,
            receta_padre=whole_recipe,
            factor_conversion=Decimal("12"),
            tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            activo=True,
        )
        self._closing_lines(
            date(2026, 7, 31),
            {
                (self.centro, whole): Decimal("5"),
                (self.centro, slice_product): Decimal("0"),
            },
        )
        self._closing_lines(
            date(2026, 8, 31),
            {
                (self.centro, whole): Decimal("4"),
                (self.centro, slice_product): Decimal("12"),
            },
        )
        conversion = self._conversion(
            product=slice_product,
            quantity="12",
            source_item_code="",
            source_item_name=whole.name,
        )

        result = self.service.build(month=date(2026, 8, 1))

        by_product = {line.product.external_id: line for line in result.lines}
        self.assertEqual(by_product["WHOLE-001"].conversion_out, Decimal("1"))
        self.assertEqual(by_product["SLICE-001"].conversion_in, Decimal("12"))
        self.assertEqual(by_product["WHOLE-001"].source_trace["conversions"], (conversion.id,))
        self.assertEqual(by_product["SLICE-001"].source_trace["conversions"], (conversion.id,))
        self.assertEqual(result.company_difference, Decimal("0"))

    def test_conversion_missing_origin_keeps_destination_leg_and_does_not_invent_exit(self):
        slice_product = PointProduct.objects.create(
            external_id="SLICE-002", sku="SLICE-002", name="Rebanada sin origen"
        )
        slice_recipe = Receta.objects.create(
            nombre=slice_product.name,
            codigo_point=slice_product.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="trace-slice-no-origin",
        )
        whole_recipe = Receta.objects.create(
            nombre="Entero configurado",
            codigo_point="WHOLE-002",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="trace-whole-configured",
        )
        RecetaEquivalencia.objects.create(
            receta_porcion=slice_recipe,
            receta_padre=whole_recipe,
            factor_conversion=Decimal("12"),
            activo=True,
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("0")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("0")})
        conversion = self._conversion(product=slice_product, quantity="12")

        result = self.service.build(month=date(2026, 8, 1))

        line = next(line for line in result.lines if line.product == slice_product)
        self.assertEqual(line.conversion_in, Decimal("12"))
        self.assertEqual(line.conversion_out, Decimal("0"))
        self.assertEqual(line.issues[0].code, "MISSING_CONVERSION_ORIGIN")
        self.assertEqual(line.issues[0].source_ids, (conversion.id,))
        self.assertEqual(
            sum((item.conversion_out for item in result.lines), Decimal("0")),
            Decimal("0"),
        )

    def test_conversion_equivalence_mismatch_keeps_result_without_inventing_origin_quantity(self):
        configured_whole = PointProduct.objects.create(
            external_id="WHOLE-CONFIGURED",
            sku="WHOLE-CONFIGURED",
            name="Entero configurado",
        )
        reported_whole = PointProduct.objects.create(
            external_id="WHOLE-REPORTED",
            sku="WHOLE-REPORTED",
            name="Entero reportado",
        )
        slice_product = PointProduct.objects.create(
            external_id="SLICE-MISMATCH",
            sku="SLICE-MISMATCH",
            name="Rebanada con conflicto",
        )
        configured_recipe = Receta.objects.create(
            nombre=configured_whole.name,
            codigo_point=configured_whole.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="trace-configured-whole",
        )
        Receta.objects.create(
            nombre=reported_whole.name,
            codigo_point=reported_whole.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="trace-reported-whole",
        )
        slice_recipe = Receta.objects.create(
            nombre=slice_product.name,
            codigo_point=slice_product.external_id,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="trace-mismatched-slice",
        )
        RecetaEquivalencia.objects.create(
            receta_porcion=slice_recipe,
            receta_padre=configured_recipe,
            factor_conversion=Decimal("12"),
            activo=True,
        )
        self._closing(date(2026, 7, 31), {self.centro: Decimal("0")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("0")})
        conversion = self._conversion(
            product=slice_product,
            source_item_code=reported_whole.external_id,
            source_item_name=reported_whole.name,
        )

        result = self.service.build(month=date(2026, 8, 1))

        line = next(line for line in result.lines if line.product == slice_product)
        self.assertEqual(line.conversion_in, Decimal("12"))
        self.assertEqual(line.issues[0].code, "CONVERSION_EQUIVALENCE_MISMATCH")
        self.assertEqual(line.issues[0].source_ids, (conversion.id,))
        self.assertEqual(
            sum((item.conversion_out for item in result.lines), Decimal("0")),
            Decimal("0"),
        )

    def test_conversion_missing_destination_is_auditable_without_inventory_leg(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        unknown = PointProduct(
            external_id="UNKNOWN-CONVERSION",
            sku="UNKNOWN-CONVERSION",
            name="Resultado desconocido",
        )
        conversion = self._conversion(product=unknown, quantity="9")

        result = self.service.build(month=date(2026, 8, 1))

        self.assertEqual(len(result.lines), 1)
        self.assertEqual(result.lines[0].conversion_in, Decimal("0"))
        issue = result.global_issues[0]
        self.assertEqual(issue.code, "MISSING_CONVERSION_DESTINATION")
        self.assertEqual(issue.source_ids, (conversion.id,))

    def test_transfer_and_conversion_authority_failures_stop_calculation(self):
        self._closing(date(2026, 7, 31), {self.centro: Decimal("10")})
        self._closing(date(2026, 8, 31), {self.centro: Decimal("10")})
        self.transfer_job.result_summary = {
            "transfer_lines_seen": 1,
            "transfer_lines_created": 0,
            "transfer_lines_updated": 0,
        }
        self.transfer_job.save(update_fields=["result_summary", "updated_at"])
        self.conversion_job.status = PointSyncJob.STATUS_PARTIAL
        self.conversion_job.save(update_fields=["status", "updated_at"])

        result = self.service.build(month=date(2026, 8, 1))

        self.assertFalse(result.source_complete)
        self.assertEqual(result.lines, ())
        messages = " ".join(issue.message for issue in result.global_issues)
        self.assertIn("TRANSFER_SYNC_COUNT_MISMATCH", messages)
        self.assertIn("CONVERSION_SYNC_JOB_PARTIAL", messages)

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
        self.assertLessEqual(len(expanded_queries), 23)
