from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.test import SimpleTestCase

from pos_bridge.services.daily_inventory_break_service import (
    DailyBreakStatus,
    DailyMovement,
    StockCheckpoint,
    project_checkpoints,
)


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
