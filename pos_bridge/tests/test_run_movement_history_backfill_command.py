from datetime import date

from django.test import SimpleTestCase

from pos_bridge.management.commands.run_movement_history_backfill import DateChunk, iter_date_chunks


class MovementHistoryBackfillCommandTests(SimpleTestCase):
    def test_iter_date_chunks_day_mode_returns_one_chunk_per_day(self):
        chunks = list(iter_date_chunks(date(2026, 3, 1), date(2026, 3, 3), chunk_mode="day"))

        self.assertEqual(
            chunks,
            [
                DateChunk(start_date=date(2026, 3, 1), end_date=date(2026, 3, 1)),
                DateChunk(start_date=date(2026, 3, 2), end_date=date(2026, 3, 2)),
                DateChunk(start_date=date(2026, 3, 3), end_date=date(2026, 3, 3)),
            ],
        )

    def test_iter_date_chunks_month_mode_groups_month_boundaries(self):
        chunks = list(iter_date_chunks(date(2026, 1, 10), date(2026, 3, 5), chunk_mode="month"))

        self.assertEqual(
            chunks,
            [
                DateChunk(start_date=date(2026, 1, 10), end_date=date(2026, 1, 31)),
                DateChunk(start_date=date(2026, 2, 1), end_date=date(2026, 2, 28)),
                DateChunk(start_date=date(2026, 3, 1), end_date=date(2026, 3, 5)),
            ],
        )

    def test_iter_date_chunks_month_mode_respects_chunk_size(self):
        chunks = list(iter_date_chunks(date(2026, 1, 10), date(2026, 5, 4), chunk_mode="month", chunk_size=2))

        self.assertEqual(
            chunks,
            [
                DateChunk(start_date=date(2026, 1, 10), end_date=date(2026, 2, 28)),
                DateChunk(start_date=date(2026, 3, 1), end_date=date(2026, 4, 30)),
                DateChunk(start_date=date(2026, 5, 1), end_date=date(2026, 5, 4)),
            ],
        )

    def test_iter_date_chunks_quarter_mode_groups_quarter_boundaries(self):
        chunks = list(iter_date_chunks(date(2026, 2, 10), date(2026, 8, 3), chunk_mode="quarter"))

        self.assertEqual(
            chunks,
            [
                DateChunk(start_date=date(2026, 2, 10), end_date=date(2026, 3, 31)),
                DateChunk(start_date=date(2026, 4, 1), end_date=date(2026, 6, 30)),
                DateChunk(start_date=date(2026, 7, 1), end_date=date(2026, 8, 3)),
            ],
        )
