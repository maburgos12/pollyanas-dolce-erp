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


class TransferEvidenceIdentityTests(SimpleTestCase):
    def setUp(self):
        from datetime import datetime, timezone
        from pos_bridge.models import PointProduct
        from pos_bridge.services.branch_inventory_traceability_service import BranchInventoryTraceabilityService
        self.capture = datetime(2026, 10, 3, 17, 48, tzinfo=timezone.utc)
        self.lower = datetime(2026, 10, 1, 7, tzinfo=timezone.utc)
        self.service = BranchInventoryTraceabilityService()
        self.indexes = self.service._build_product_indexes([
            PointProduct(pk=99, external_id="700", sku="SKU", name="Prueba")])

    def rows(self, real_before=False, member_before=False, member_missing=False):
        from datetime import timedelta
        from pos_bridge.models import PointTransferLine, PointOpenTransferSnapshotMember
        shared = dict(origin_branch_id=28, destination_branch_id=1, item_code="700", item_name="Prueba",
            sent_quantity=Decimal("1"), received_quantity=Decimal("0"), is_insumo=False, is_cancelled=False)
        real = PointTransferLine(pk=7, transfer_external_id="real", detail_external_id="detail-real",
            sent_at=self.capture + timedelta(hours=-1 if real_before else 1), **shared)
        # Same PK in a DIFFERENT table and a third source_line_id. Neither can
        # substitute for a transfer primary key or suppress a real transfer.
        member = PointOpenTransferSnapshotMember(pk=7, source_line_id=999,
            transfer_external_id="frozen", detail_external_id="detail-frozen",
            sent_at=None if member_missing else self.capture + timedelta(hours=-1 if member_before else 1), **shared)
        return real, member

    def blocked(self, real, member):
        from reportes.services_operational_reconciliation import transfer_temporal_blockers
        return transfer_temporal_blockers([real], [member], service=self.service, indexes=self.indexes,
            locations={28}, product_ids={99}, lower=self.lower, capture=self.capture)

    def test_member_before_capture_is_not_hidden_by_after_capture_transfer_with_same_pk(self):
        self.assertEqual(self.blocked(*self.rows(member_before=True)), {99})

    def test_real_before_capture_is_not_removed_by_frozen_member_pk_collision(self):
        self.assertEqual(self.blocked(*self.rows(real_before=True)), {99})

    def test_known_after_capture_dates_are_eligible_but_missing_frozen_date_is_not(self):
        self.assertEqual(self.blocked(*self.rows()), set())
        self.assertEqual(self.blocked(*self.rows(member_missing=True)), {99})

    def test_frozen_business_reference_overrides_mutable_version_without_using_pk(self):
        real, member = self.rows(real_before=True)
        member.pk = 888
        member.transfer_external_id = real.transfer_external_id
        member.detail_external_id = real.detail_external_id
        self.assertEqual(self.blocked(real, member), set())


from django.test import TransactionTestCase


class OperationalReconciliationLockTests(TransactionTestCase):
    def test_busy_monthly_mutex_returns_unavailable_and_keeps_transaction_healthy(self):
        from django.db import connection, transaction
        from reportes.services_operational_reconciliation import read_operational_reconciliation
        from pos_bridge.services.product_month_source_mutex import PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE
        peer = connection.copy(alias="operational-guard-test")
        try:
            with peer.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(%s, %s)", [PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE, 202610])
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout='500ms'")
                self.assertIsNone(read_operational_reconciliation([SimpleNamespace()], {}))
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    self.assertEqual(cursor.fetchone(), (1,))
        finally:
            with peer.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s, %s)", [PRODUCT_MONTH_SOURCE_LOCK_NAMESPACE, 202610])
            peer.close()
