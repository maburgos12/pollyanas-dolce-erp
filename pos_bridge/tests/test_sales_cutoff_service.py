from django.test import SimpleTestCase

from pos_bridge.services.sales_cutoff_service import build_probe_delta, summarize_probe_series


class PointSalesCutoffServiceTests(SimpleTestCase):
    def test_build_probe_delta_marks_changed_fields(self):
        previous_snapshot = {
            "row_count": 10,
            "branch_count": 2,
            "total_quantity": "100",
            "total_tickets": 20,
            "gross_amount": "1000",
            "discount_amount": "5",
            "total_amount": "995",
            "tax_amount": "0",
            "net_amount": "995",
        }
        current_snapshot = {
            "row_count": 12,
            "branch_count": 2,
            "total_quantity": "101.5",
            "total_tickets": 20,
            "gross_amount": "1015",
            "discount_amount": "5",
            "total_amount": "1010",
            "tax_amount": "0",
            "net_amount": "1010",
        }

        result = build_probe_delta(previous_snapshot, current_snapshot)

        self.assertEqual(result["status"], "CHANGED")
        self.assertEqual(
            result["changed_fields"],
            ["row_count", "total_quantity", "gross_amount", "total_amount", "net_amount"],
        )
        self.assertEqual(result["delta"]["row_count"], 2)
        self.assertEqual(result["delta"]["total_quantity"], "1.5")

    def test_summarize_probe_series_marks_stable_after_unchanged_tail(self):
        probes = [
            {"comparison": {"status": "BASELINE"}, "captured_at_local": "2026-03-17T23:00:00-07:00"},
            {"comparison": {"status": "CHANGED"}, "captured_at_local": "2026-03-18T00:00:00-07:00"},
            {"comparison": {"status": "UNCHANGED"}, "captured_at_local": "2026-03-18T01:00:00-07:00"},
            {"comparison": {"status": "UNCHANGED"}, "captured_at_local": "2026-03-18T02:00:00-07:00"},
        ]

        result = summarize_probe_series(probes, stable_after=2)

        self.assertEqual(result["latest_status"], "UNCHANGED")
        self.assertEqual(result["trailing_unchanged_probes"], 2)
        self.assertTrue(result["is_stable"])
