from datetime import date

from django.test import SimpleTestCase

from pos_bridge.utils.dates import iter_business_dates, resolve_incremental_window


class PosBridgeDateUtilsTests(SimpleTestCase):
    def test_iter_business_dates_skips_excluded_ranges(self):
        result = iter_business_dates(
            date(2026, 1, 1),
            date(2026, 1, 5),
            excluded_ranges=[(date(2026, 1, 2), date(2026, 1, 4))],
        )
        self.assertEqual(result, [date(2026, 1, 1), date(2026, 1, 5)])

    def test_resolve_incremental_window_uses_lookback_and_lag(self):
        start_date, end_date = resolve_incremental_window(
            anchor_date=date(2026, 3, 15),
            lookback_days=3,
            lag_days=1,
        )
        self.assertEqual(start_date, date(2026, 3, 12))
        self.assertEqual(end_date, date(2026, 3, 14))

    def test_resolve_incremental_window_clamps_invalid_values(self):
        start_date, end_date = resolve_incremental_window(
            anchor_date=date(2026, 3, 15),
            lookback_days=0,
            lag_days=-5,
        )
        self.assertEqual(start_date, date(2026, 3, 15))
        self.assertEqual(end_date, date(2026, 3, 15))
