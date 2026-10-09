from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import (
    PointBranch,
    PointConversionLine,
    PointInventorySnapshot,
    PointProduct,
    PointProductionLine,
    PointSyncJob,
    PointTransferLine,
)
from pos_bridge.services.daily_inventory_break_service import (
    DailyInventoryBreakService,
    DailyBreakStatus,
    DailyMovement,
    StockCheckpoint,
    project_checkpoints,
)
from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditRun


class DailyInventoryBreakProjectionTests(SimpleTestCase):
    @staticmethod
    def aware(day, hour):
        return datetime(2026, 8, day, hour, tzinfo=ZoneInfo("America/Mazatlan"))

    def test_finds_first_mismatch_after_exact_checkpoint(self):
        result = project_checkpoints(
            opening=Decimal("10"),
            movements=(
                DailyMovement("production", date(2026, 8, 1), Decimal("2"), (1,)),
                DailyMovement("sales", date(2026, 8, 2), Decimal("-3"), (2,)),
            ),
            checkpoints=(
                StockCheckpoint(self.aware(1, 23), Decimal("12")),
                StockCheckpoint(self.aware(2, 23), Decimal("8")),
            ),
        )

        self.assertEqual(result.status, DailyBreakStatus.FOUND)
        self.assertEqual(result.last_matching_checkpoint.observed, Decimal("12"))
        self.assertEqual(result.first_mismatch_checkpoint.difference, Decimal("-1"))

    def test_same_day_date_only_rows_form_compatible_range(self):
        result = project_checkpoints(
            opening=Decimal("10"),
            movements=(
                DailyMovement("production", date(2026, 8, 1), Decimal("5"), (1,)),
                DailyMovement("sales", date(2026, 8, 1), Decimal("-4"), (2,)),
            ),
            checkpoints=(StockCheckpoint(self.aware(1, 18), Decimal("8")),),
        )

        self.assertEqual(result.status, DailyBreakStatus.INCONCLUSIVE)
        self.assertEqual((result.minimum, result.maximum), (Decimal("6"), Decimal("15")))

    def test_exact_timestamp_after_checkpoint_is_excluded(self):
        result = project_checkpoints(
            opening=Decimal("10"),
            movements=(
                DailyMovement("waste", self.aware(1, 19), Decimal("-2"), (1,)),
            ),
            checkpoints=(StockCheckpoint(self.aware(1, 18), Decimal("10")),),
        )

        self.assertEqual(result.status, DailyBreakStatus.NOT_FOUND)

    def test_first_checkpoint_can_be_the_first_mismatch(self):
        result = project_checkpoints(
            opening=Decimal("10"),
            movements=(),
            checkpoints=(StockCheckpoint(self.aware(1, 18), Decimal("9")),),
        )

        self.assertEqual(result.status, DailyBreakStatus.FOUND)
        self.assertIsNone(result.last_matching_checkpoint)
        self.assertEqual(result.first_mismatch_checkpoint.difference, Decimal("-1"))

    def test_without_snapshots_is_insufficient_evidence(self):
        result = project_checkpoints(opening=Decimal("10"), movements=(), checkpoints=())

        self.assertEqual(result.status, DailyBreakStatus.INSUFFICIENT_EVIDENCE)


class DailyInventoryBreakServiceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.month = date(2026, 8, 1)
        cls.local_tz = ZoneInfo("America/Mazatlan")
        cls.erp_branch = Sucursal.objects.create(codigo="DIARIO", nombre="Diario")
        cls.branch = PointBranch.objects.create(
            external_id="DIARIO-1",
            name="Diario",
            erp_branch=cls.erp_branch,
        )
        cls.alias = PointBranch.objects.create(
            external_id="DIARIO-2",
            name="Diario alias",
            erp_branch=cls.erp_branch,
        )
        cls.product = PointProduct.objects.create(
            external_id="DAILY-PRODUCT",
            sku="DAILY-001",
            name="Producto diario",
        )
        cls.job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
        )
        cls.audit_run = ProductInventoryAuditRun.objects.create(
            month=cls.month,
            status=ProductInventoryAuditRun.Status.READY,
            calculation_fingerprint="a" * 64,
        )

    def make_case(self, **overrides):
        values = {
            "run": self.audit_run,
            "month": self.month,
            "branch": self.branch,
            "product": self.product,
            "opening_point": Decimal("10"),
            "production": Decimal("0"),
            "sales": Decimal("0"),
            "waste": Decimal("0"),
            "transfer_in": Decimal("0"),
            "transfer_out": Decimal("0"),
            "conversion_in": Decimal("0"),
            "conversion_out": Decimal("0"),
            "identified_adjustment": Decimal("0"),
            "expected_closing": Decimal("10"),
            "point_closing": Decimal("10"),
            "difference": Decimal("0"),
            "calculation_fingerprint": "b" * 64,
            "rebuilt_at": timezone.now(),
            "source_trace": {},
        }
        values.update(overrides)
        return ProductInventoryAuditCase.objects.create(**values)

    def snapshot(self, *, day, stock, branch=None, product=None):
        return PointInventorySnapshot.objects.create(
            branch=branch or self.alias,
            product=product or self.product,
            stock=Decimal(stock),
            captured_at=datetime(2026, 8, day, 22, tzinfo=self.local_tz),
            sync_job=self.job,
        )

    def test_build_month_uses_trace_rows_and_branch_alias_snapshots(self):
        production = PointProductionLine.objects.create(
            branch=self.branch,
            production_external_id="PROD-1",
            detail_external_id="PROD-1-1",
            source_hash="1" * 64,
            production_date=date(2026, 8, 1),
            item_name=self.product.name,
            item_code=self.product.sku,
            produced_quantity=Decimal("2"),
        )
        case = self.make_case(
            source_trace={"production": [production.id]},
            production=Decimal("2"),
            expected_closing=Decimal("12"),
            point_closing=Decimal("11"),
            difference=Decimal("-1"),
        )
        self.snapshot(day=2, stock="11")

        result = DailyInventoryBreakService().build_month(self.month, [case])[case.id]

        self.assertEqual(result.status, DailyBreakStatus.FOUND)
        self.assertEqual(result.first_mismatch_checkpoint.difference, Decimal("-1"))
        self.assertEqual(result.movement_ids_by_source, {"production": (production.id,)})

    def test_partial_transfer_return_is_an_origin_entry(self):
        transfer = PointTransferLine.objects.create(
            origin_branch=self.branch,
            destination_branch=PointBranch.objects.create(
                external_id="DAILY-DEST", name="Destino diario"
            ),
            transfer_external_id="TRANSFER-1",
            detail_external_id="TRANSFER-1-1",
            source_hash="2" * 64,
            registered_at=datetime(2026, 8, 1, 9, tzinfo=self.local_tz),
            sent_at=datetime(2026, 8, 1, 10, tzinfo=self.local_tz),
            received_at=datetime(2026, 8, 1, 12, tzinfo=self.local_tz),
            item_name=self.product.name,
            item_code=self.product.sku,
            sent_quantity=Decimal("2"),
            received_quantity=Decimal("1"),
            is_received=True,
            is_finalized=False,
        )
        case = self.make_case(
            source_trace={
                "transfers": [transfer.id],
                "transfer_in": [transfer.id],
                "transfer_out": [transfer.id],
            },
            transfer_in=Decimal("1"),
            transfer_out=Decimal("2"),
            expected_closing=Decimal("9"),
            point_closing=Decimal("9"),
        )
        self.snapshot(day=1, stock="9")

        result = DailyInventoryBreakService().build_month(self.month, [case])[case.id]

        self.assertEqual(result.status, DailyBreakStatus.NOT_FOUND)
        self.assertEqual(
            result.movement_ids_by_source,
            {"transfer_out": (transfer.id,), "transfer_return": (transfer.id,)},
        )

    def test_conversion_out_uses_persisted_trace_impact(self):
        conversion = PointConversionLine.objects.create(
            branch=self.branch,
            movement_external_id="CONV-1",
            source_hash="3" * 64,
            movement_at=datetime(2026, 8, 1, 10, tzinfo=self.local_tz),
            item_name="Rebanada",
            item_code="REB-1",
            quantity=Decimal("10"),
            source_item_name=self.product.name,
            source_item_code=self.product.sku,
        )
        case = self.make_case(
            source_trace={
                "conversions": [conversion.id],
                "conversion_out": [conversion.id],
                "conversion_out_impacts": {str(conversion.id): "0.8333"},
            },
            conversion_out=Decimal("0.8333"),
            expected_closing=Decimal("9.1667"),
            point_closing=Decimal("9"),
            difference=Decimal("-0.1667"),
        )
        self.snapshot(day=1, stock="9")

        result = DailyInventoryBreakService().build_month(self.month, [case])[case.id]

        self.assertEqual(result.status, DailyBreakStatus.FOUND)
        self.assertEqual(result.first_mismatch_checkpoint.minimum, Decimal("9.1667"))

    def test_month_loader_does_not_query_per_case(self):
        first = self.make_case()
        self.snapshot(day=1, stock="10")
        with CaptureQueriesContext(connection) as single_queries:
            DailyInventoryBreakService().build_month(self.month, [first])

        second_product = PointProduct.objects.create(
            external_id="DAILY-PRODUCT-2",
            sku="DAILY-002",
            name="Producto diario 2",
        )
        second = self.make_case(product=second_product)
        self.snapshot(day=1, stock="10", product=second_product)
        with CaptureQueriesContext(connection) as doubled_queries:
            DailyInventoryBreakService().build_month(self.month, [first, second])

        self.assertEqual(len(single_queries), len(doubled_queries))
        self.assertLessEqual(len(doubled_queries), 7)

    def test_open_transfer_snapshot_without_event_time_is_insufficient(self):
        case = self.make_case(
            source_trace={"open_transfer_snapshot_out": [999]},
            transfer_out=Decimal("1"),
            expected_closing=Decimal("9"),
            point_closing=Decimal("9"),
        )
        self.snapshot(day=1, stock="9")

        result = DailyInventoryBreakService().build_month(self.month, [case])[case.id]

        self.assertEqual(result.status, DailyBreakStatus.INSUFFICIENT_EVIDENCE)
        self.assertTrue(any("hora" in warning for warning in result.warnings))

    def test_missing_referenced_row_is_insufficient_instead_of_false_break(self):
        case = self.make_case(source_trace={"production": [999]})
        self.snapshot(day=1, stock="9")

        result = DailyInventoryBreakService().build_month(self.month, [case])[case.id]

        self.assertEqual(result.status, DailyBreakStatus.INSUFFICIENT_EVIDENCE)
        self.assertTrue(any("#999" in warning for warning in result.warnings))
