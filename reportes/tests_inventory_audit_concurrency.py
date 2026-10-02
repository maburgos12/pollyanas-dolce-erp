from unittest.mock import patch

from django.db import DatabaseError, connection, transaction
from django.test import TransactionTestCase

from reportes.services_inventory_audit_agent import InventoryAuditAgent
from reportes.services_inventory_traceability import InventoryAuditMaterializer
from reportes.tests_inventory_audit_agent import InventoryAuditAgentFixtures
from reportes.views_inventory_traceability import _locked_case


class InventoryAuditCaseLockTests(InventoryAuditAgentFixtures, TransactionTestCase):
    def setUp(self):
        self.setUpTestData()
        self.case = self.make_case()

    def assert_only_case_is_locked(self):
        # A second PostgreSQL session probes the locks while the caller's
        # transaction remains open; NOWAIT makes accidental master locks fail.
        probe = connection.copy(alias="inventory_audit_lock_probe")
        try:
            for instance in (self.product, self.branch, self.audit_run):
                with probe.cursor() as cursor:
                    cursor.execute("BEGIN")
                    try:
                        cursor.execute(
                            f'SELECT id FROM "{instance._meta.db_table}" '
                            "WHERE id = %s FOR UPDATE NOWAIT",
                            [instance.pk],
                        )
                    except DatabaseError as exc:
                        self.fail(f"Auditor blocked unrelated {instance._meta.db_table}: {exc}")
                    finally:
                        cursor.execute("ROLLBACK")
            with probe.cursor() as cursor:
                cursor.execute("BEGIN")
                try:
                    with self.assertRaises(DatabaseError) as blocked:
                        cursor.execute(
                            f'SELECT id FROM "{self.case._meta.db_table}" '
                            "WHERE id = %s FOR UPDATE NOWAIT",
                            [self.case.pk],
                        )
                    cause = blocked.exception.__cause__
                    self.assertEqual(
                        getattr(cause, "sqlstate", getattr(cause, "pgcode", None)),
                        "55P03",
                    )
                finally:
                    cursor.execute("ROLLBACK")
        finally:
            probe.close()

    def test_agent_locks_case_without_blocking_shared_point_records(self):
        agent = InventoryAuditAgent()
        investigate = agent.investigate_case

        def checked_investigation(case):
            self.assert_only_case_is_locked()
            return investigate(case)

        with patch.object(agent, "investigate_case", side_effect=checked_investigation):
            self.assertEqual(agent.run_month(self.month)["total"], 1)
        self.assertEqual(agent.run_month(self.month)["updated"], 0)

    def test_rebuild_locks_only_canonical_cases(self):
        with transaction.atomic():
            cases = InventoryAuditMaterializer()._canonical_existing_cases(
                month=self.month,
                branch_aliases={self.branch.pk: self.branch.pk},
                prepared_lines=[],
                dry_run=True,
            )
            self.assertEqual(list(cases.values()), [self.case])
            self.assert_only_case_is_locked()

    def test_history_reconciliation_locks_only_cases(self):
        def checked_history(cases, month):
            self.assertEqual([case.pk for case in cases], [self.case.pk])
            self.assert_only_case_is_locked()
            return {}

        with patch(
            "reportes.services_inventory_traceability.AuditStockHistoryService.reconcile_many",
            side_effect=checked_history,
        ):
            result = InventoryAuditMaterializer().reconcile_existing_cases_from_point_history(
                self.month
            )
        self.assertEqual(result, {"selected": 1, "reconciled": 0, "pending": 1})

    def test_operator_review_locks_only_case(self):
        with transaction.atomic():
            self.assertEqual(_locked_case(self.case.pk), self.case)
            self.assert_only_case_is_locked()
