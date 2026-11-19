from decimal import Decimal
from dataclasses import replace
from types import SimpleNamespace

from django.test import SimpleTestCase

from pos_bridge.services.audit_stock_history_service import PointHistoryReconciliation
from reportes.services_operational_reconciliation import verify_operational_balance


class OperationalReconciliationContractTests(SimpleTestCase):
    def setUp(self):
        self.snapshot = SimpleNamespace(stock=Decimal("0"))
        self.flows = {field: Decimal("0") for field in ("production", "sales", "waste",
            "transfer_in", "transfer_out", "conversion_in", "conversion_out")}
        self.history = PointHistoryReconciliation(coverage_status="COMPLETE")

    def verify(self, **kwargs):
        return verify_operational_balance(self.snapshot, self.flows, Decimal("0"), self.history, **kwargs)

    def test_complete_zero_is_credited_as_no_activity(self):
        result = self.verify()
        self.assertTrue(result["verified"])
        self.assertTrue(result["no_activity"])
        self.assertEqual(result["difference"], 0)

    def test_vainilla_eighteen_entries_and_sales_preserve_existing_equation(self):
        self.flows.update(transfer_in=Decimal("18"), sales=Decimal("18"))
        self.history = replace(self.history, transfer_in=Decimal("18"), sales=Decimal("18"), movement_ids=(10, 11))
        result = self.verify()
        self.assertFalse(result["no_activity"])
        self.assertEqual(result["calculated"], 0)
        self.assertEqual(result["flows"]["sales"], 18)

    def test_new_or_changed_flow_and_stock_remove_verification(self):
        self.flows["sales"] = Decimal("1")
        self.assertIsNone(self.verify())
        self.flows["sales"] = Decimal("0")
        self.history = replace(self.history, transfer_in=Decimal("1"))
        self.assertIsNone(self.verify())
        self.history = replace(self.history, transfer_in=Decimal("0"))
        self.history = replace(self.history, identified_adjustment=Decimal("1"))
        self.assertIsNone(self.verify())

    def test_unknown_partial_missing_or_pre_capture_evidence_does_not_close(self):
        self.assertIsNone(self.verify(blocked=True))
        self.history = replace(self.history, coverage_status="INCOMPLETE")
        self.assertIsNone(self.verify())
        self.history = replace(self.history, coverage_status="COMPLETE")
        self.history = replace(self.history, unknown_movement_ids=(100,))
        self.assertIsNone(self.verify())
        self.assertIsNone(verify_operational_balance(self.snapshot, self.flows, None, self.history))
        self.assertIsNone(verify_operational_balance(self.snapshot, self.flows, 0, None))

    def test_offsetting_movements_are_not_called_no_activity(self):
        self.history = replace(self.history, movement_ids=(10, 11))
        self.assertFalse(self.verify()["no_activity"])
