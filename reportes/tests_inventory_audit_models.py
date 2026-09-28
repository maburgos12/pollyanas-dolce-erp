from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.exceptions import ValidationError
from django.db import IntegrityError, connection, transaction
from django.test import TestCase
from django.utils import timezone

from pos_bridge.models import PointBranch, PointProduct
from reportes.models import (
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)


class ProductInventoryAuditModelsTests(TestCase):
    def setUp(self):
        self.branch = PointBranch.objects.create(
            external_id="branch-audit-1",
            name="Sucursal auditoría",
        )
        self.product = PointProduct.objects.create(
            external_id="product-audit-1",
            sku="AUDIT-1",
            name="Producto auditoría",
        )
        self.actor = get_user_model().objects.create_user(
            username="inventory.auditor",
        )

    def _run(self, **overrides):
        values = {
            "month": date(2026, 8, 1),
            "status": ProductInventoryAuditRun.Status.READY,
            "calculation_fingerprint": "a" * 64,
        }
        values.update(overrides)
        return ProductInventoryAuditRun.objects.create(**values)

    def _case(self, **overrides):
        run = overrides.pop("run", None) or self._run()
        values = {
            "run": run,
            "month": run.month,
            "branch": self.branch,
            "product": self.product,
            "point_closing_status": ProductInventoryAuditCase.PointClosingStatus.AVAILABLE,
            "movement_status": ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
            "physical_status": ProductInventoryAuditCase.PhysicalStatus.NOT_AVAILABLE,
            "opening_point": Decimal("0"),
            "production": Decimal("0"),
            "sales": Decimal("0"),
            "waste": Decimal("0"),
            "transfer_in": Decimal("0"),
            "transfer_out": Decimal("0"),
            "conversion_in": Decimal("0"),
            "conversion_out": Decimal("0"),
            "identified_adjustment": Decimal("0"),
            "expected_closing": Decimal("0"),
            "point_closing": Decimal("0"),
            "difference": Decimal("0"),
            "calculation_fingerprint": "b" * 64,
            "rebuilt_at": timezone.now(),
        }
        values.update(overrides)
        return ProductInventoryAuditCase.objects.create(**values)

    def test_run_normalizes_month_during_validation_and_month_is_unique(self):
        run = ProductInventoryAuditRun(
            month=date(2026, 8, 17),
            status=ProductInventoryAuditRun.Status.READY,
            calculation_fingerprint="a" * 64,
        )

        run.full_clean()
        self.assertEqual(run.month, date(2026, 8, 1))
        run.save()

        duplicate = ProductInventoryAuditRun(
            month=date(2026, 8, 29),
            status=ProductInventoryAuditRun.Status.READY,
            calculation_fingerprint="b" * 64,
        )
        duplicate.full_clean(exclude={"month"}, validate_unique=False)
        duplicate.month = date(2026, 8, 1)
        with self.assertRaises(IntegrityError), transaction.atomic():
            duplicate.save()

    def test_run_persists_source_state_summary_and_rebuild_timestamps(self):
        started_at = timezone.now()
        rebuilt_at = timezone.now()
        successful_at = timezone.now()
        run = self._run(
            status=ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE,
            source_issues=[
                {
                    "code": "POINT_CLOSING_MISSING",
                    "message": "Falta cierre Point verificado.",
                    "branch_id": self.branch.pk,
                    "product_id": None,
                    "source_ids": [17, 18],
                }
            ],
            summary={
                "created": 4,
                "updated": 3,
                "unchanged": 2,
                "reopened": 1,
                "balanced": 5,
                "exceptions": 4,
                "source_incomplete": 1,
            },
            calculation_fingerprint="c" * 64,
            started_at=started_at,
            rebuilt_at=rebuilt_at,
            last_successful_rebuild_at=successful_at,
        )

        run.refresh_from_db()
        self.assertEqual(run.status, ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE)
        self.assertEqual(run.source_issues[0]["code"], "POINT_CLOSING_MISSING")
        self.assertEqual(run.summary["reopened"], 1)
        self.assertEqual(run.calculation_fingerprint, "c" * 64)
        self.assertEqual(run.started_at, started_at)
        self.assertEqual(run.rebuilt_at, rebuilt_at)
        self.assertEqual(run.last_successful_rebuild_at, successful_at)

    def test_run_summary_requires_exact_non_negative_integer_counters(self):
        valid_summary = {
            "created": 1,
            "updated": 2,
            "unchanged": 3,
            "reopened": 4,
            "balanced": 5,
            "exceptions": 6,
            "source_incomplete": 7,
        }
        valid = ProductInventoryAuditRun(
            month=date(2026, 8, 1),
            status=ProductInventoryAuditRun.Status.READY,
            summary=valid_summary,
            calculation_fingerprint="e" * 64,
        )
        valid.full_clean()

        invalid_summaries = (
            {key: value for key, value in valid_summary.items() if key != "created"},
            {**valid_summary, "unknown": 0},
            {**valid_summary, "created": -1},
            {**valid_summary, "created": True},
            {**valid_summary, "created": 1.5},
        )
        for summary in invalid_summaries:
            with self.subTest(summary=summary):
                invalid = ProductInventoryAuditRun(
                    month=date(2026, 8, 1),
                    status=ProductInventoryAuditRun.Status.READY,
                    summary=summary,
                    calculation_fingerprint="f" * 64,
                )
                with self.assertRaisesMessage(ValidationError, "summary"):
                    invalid.full_clean()

    def test_run_source_issues_require_known_typed_fields(self):
        valid_issue = {
            "code": "SOURCE_INCOMPLETE",
            "message": "Falta evidencia de cierre.",
            "branch_id": self.branch.pk,
            "product_id": self.product.pk,
            "source_ids": [11, 12],
        }
        valid = ProductInventoryAuditRun(
            month=date(2026, 8, 1),
            status=ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE,
            source_issues=[valid_issue],
            calculation_fingerprint="1" * 64,
        )
        valid.full_clean()

        invalid_issues = (
            [{key: value for key, value in valid_issue.items() if key != "message"}],
            [{**valid_issue, "unknown": "value"}],
            [{**valid_issue, "code": ""}],
            [{**valid_issue, "branch_id": "1"}],
            [{**valid_issue, "product_id": True}],
            [{**valid_issue, "source_ids": [11, "12"]}],
        )
        for source_issues in invalid_issues:
            with self.subTest(source_issues=source_issues):
                invalid = ProductInventoryAuditRun(
                    month=date(2026, 8, 1),
                    status=ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE,
                    source_issues=source_issues,
                    calculation_fingerprint="2" * 64,
                )
                with self.assertRaisesMessage(ValidationError, "source_issues"):
                    invalid.full_clean()

    def test_run_rejects_invalid_json_on_save_and_mass_operations(self):
        with self.assertRaisesMessage(ValidationError, "summary"):
            ProductInventoryAuditRun.objects.create(
                month=date(2026, 8, 1),
                summary={"created": -1},
                calculation_fingerprint="3" * 64,
            )
        with self.assertRaisesMessage(ValidationError, "source_issues"):
            ProductInventoryAuditRun.objects.create(
                month=date(2026, 8, 1),
                source_issues=[{"code": "SOURCE_INCOMPLETE"}],
                calculation_fingerprint="4" * 64,
            )

        run = self._run()
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditRun.objects.filter(pk=run.pk).update(
                summary={"created": -1}
            )
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditRun.objects.bulk_create(
                [
                    ProductInventoryAuditRun(
                        month=date(2026, 9, 1),
                        calculation_fingerprint="5" * 64,
                    )
                ]
            )
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditRun.objects.bulk_update(
                [run],
                ["summary"],
            )

    def test_run_rejects_invalid_status_in_orm_and_database(self):
        with self.assertRaisesMessage(ValidationError, "status"):
            ProductInventoryAuditRun.objects.create(
                month=date(2026, 8, 1),
                status="INVALID",
                calculation_fingerprint="6" * 64,
            )

        run = self._run()
        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE reportes_productinventoryauditrun SET status = %s WHERE id = %s",
                    ["INVALID", run.pk],
                )

    def test_case_is_unique_per_month_branch_product_and_keeps_separate_statuses(self):
        run = self._run()
        case = self._case(
            run=run,
            point_closing_status=ProductInventoryAuditCase.PointClosingStatus.PROTECTED,
            movement_status=ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
            opening_point=Decimal("10.2500"),
            production=Decimal("8.0000"),
            sales=Decimal("3.0000"),
            waste=Decimal("0.2500"),
            transfer_in=Decimal("1.0000"),
            transfer_out=Decimal("2.0000"),
            conversion_in=Decimal("0.5000"),
            conversion_out=Decimal("0.7500"),
            identified_adjustment=Decimal("1.2500"),
            expected_closing=Decimal("15.0000"),
            point_closing=Decimal("14.5000"),
            difference=Decimal("-0.5000"),
            issue_codes=["POINT_CLOSING_PROTECTED"],
            source_trace={"point_closing": {"status": "PROTECTED"}},
        )

        self.assertEqual(case.point_closing_status, ProductInventoryAuditCase.PointClosingStatus.PROTECTED)
        self.assertEqual(case.movement_status, ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL)
        self.assertEqual(case.physical_status, ProductInventoryAuditCase.PhysicalStatus.NOT_AVAILABLE)
        self.assertEqual(case.difference, Decimal("-0.5000"))
        self.assertEqual(case.source_trace["point_closing"]["status"], "PROTECTED")

        with self.assertRaises(IntegrityError), transaction.atomic():
            self._case(run=run)

    def test_case_normalizes_month_during_validation_and_matches_run_month(self):
        run = self._run()
        case = ProductInventoryAuditCase(
            run=run,
            month=date(2026, 8, 23),
            branch=self.branch,
            product=self.product,
            opening_point=Decimal("0"),
            production=Decimal("0"),
            sales=Decimal("0"),
            waste=Decimal("0"),
            transfer_in=Decimal("0"),
            transfer_out=Decimal("0"),
            conversion_in=Decimal("0"),
            conversion_out=Decimal("0"),
            identified_adjustment=Decimal("0"),
            expected_closing=Decimal("0"),
            point_closing=Decimal("0"),
            difference=Decimal("0"),
            calculation_fingerprint="d" * 64,
            rebuilt_at=timezone.now(),
        )

        case.full_clean()
        self.assertEqual(case.month, date(2026, 8, 1))

        case.month = date(2026, 9, 4)
        with self.assertRaisesMessage(ValidationError, "mes de la corrida"):
            case.full_clean()

    def test_case_rejects_invalid_json_on_save_and_mass_operations(self):
        run = self._run()
        with self.assertRaisesMessage(ValidationError, "issue_codes"):
            self._case(run=run, issue_codes={"not": "a list"})

        case = self._case(run=run)
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditCase.objects.filter(pk=case.pk).update(
                source_trace=[]
            )
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditCase.objects.bulk_create([])
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditCase.objects.bulk_update(
                [case],
                ["source_trace"],
            )

    def test_case_rejects_invalid_statuses_in_orm_and_database(self):
        run = self._run()
        invalid_statuses = (
            ("point_closing_status", "INVALID"),
            ("movement_status", "INVALID"),
            ("physical_status", "INVALID"),
        )
        for field, value in invalid_statuses:
            with self.subTest(layer="orm", field=field):
                with self.assertRaisesMessage(ValidationError, field):
                    self._case(run=run, **{field: value})

        case = self._case(run=run)
        for field, value in invalid_statuses:
            with self.subTest(layer="database", field=field):
                with self.assertRaises(IntegrityError), transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"UPDATE reportes_productinventoryauditcase SET {field} = %s WHERE id = %s",
                            [value, case.pk],
                        )

    def test_case_rejects_month_different_from_run_in_orm_and_database(self):
        run = self._run()
        with self.assertRaisesMessage(ValidationError, "mes de la corrida"):
            self._case(run=run, month=date(2026, 9, 1))

        case = self._case(run=run)
        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE reportes_productinventoryauditcase SET month = %s WHERE id = %s",
                    [date(2026, 9, 1), case.pk],
                )
        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE reportes_productinventoryauditrun SET month = %s WHERE id = %s",
                    [date(2026, 9, 1), run.pk],
                )

    def test_event_persists_audit_history_and_is_append_only(self):
        case = self._case()
        explanation = ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="TRANSFER_TIMING",
            notes="Transferencia recibida al inicio del mes siguiente.",
            evidence="reportes/inventory-audit/2026/08/evidence.pdf",
            actor=self.actor,
            metadata={"source": "manual_review"},
        )
        approval = ProductInventoryAuditEvent(
            case=case,
            action=ProductInventoryAuditEvent.Action.APPROVE,
            reason_code="EXPLANATION_ACCEPTED",
            notes="Evidencia conciliada.",
            actor=self.actor,
            related_event=explanation,
        )
        approval.full_clean()
        approval.save()

        self.assertIsNotNone(approval.created_at)
        self.assertEqual(approval.related_event, explanation)
        self.assertEqual(explanation.metadata, {"source": "manual_review"})

        approval.notes = "Intento de edición"
        with self.assertRaisesMessage(ValidationError, "inmutables"):
            approval.save()
        with self.assertRaisesMessage(ValidationError, "no pueden eliminarse"):
            explanation.delete()

    def test_approval_or_rejection_requires_explanation_from_same_case(self):
        first_case = self._case()
        second_product = PointProduct.objects.create(
            external_id="product-audit-2",
            sku="AUDIT-2",
            name="Otro producto",
        )
        second_case = self._case(run=first_case.run, product=second_product)
        explanation = ProductInventoryAuditEvent.objects.create(
            case=first_case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="COUNT_TIMING",
            actor=self.actor,
        )

        for action in (
            ProductInventoryAuditEvent.Action.APPROVE,
            ProductInventoryAuditEvent.Action.REJECT,
        ):
            with self.subTest(action=action):
                event = ProductInventoryAuditEvent(
                    case=second_case,
                    action=action,
                    reason_code="REVIEWED",
                    actor=self.actor,
                    related_event=explanation,
                )
                with self.assertRaisesMessage(ValidationError, "explicación del mismo caso"):
                    event.full_clean()

    def test_event_queryset_rejects_update_delete_and_bulk_create(self):
        case = self._case()
        event = ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="COUNT_TIMING",
            actor=self.actor,
        )

        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditEvent.objects.filter(pk=event.pk).update(
                notes="Intento masivo"
            )
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditEvent.objects.filter(pk=event.pk).update(actor=None)
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditEvent.objects.filter(pk=event.pk).delete()
        with self.assertRaisesMessage(ValidationError, "operaciones masivas"):
            ProductInventoryAuditEvent.objects.bulk_create(
                [
                    ProductInventoryAuditEvent(
                        case=case,
                        action=ProductInventoryAuditEvent.Action.EXPLAIN,
                        reason_code="BULK_ATTEMPT",
                    )
                ]
            )

        event.refresh_from_db()
        self.assertEqual(event.notes, "")

    def test_event_database_rejects_approval_without_related_explanation(self):
        case = self._case()
        event = ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="COUNT_TIMING",
            actor=self.actor,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE reportes_productinventoryauditevent
                    SET action = %s, related_event_id = NULL
                    WHERE id = %s
                    """,
                    [ProductInventoryAuditEvent.Action.APPROVE, event.pk],
                )

    def test_event_database_rejects_cross_case_or_non_explanation_relation(self):
        first_case = self._case()
        second_product = PointProduct.objects.create(
            external_id="product-audit-trigger-2",
            sku="AUDIT-TRIGGER-2",
            name="Producto trigger",
        )
        second_case = self._case(run=first_case.run, product=second_product)
        explanation = ProductInventoryAuditEvent.objects.create(
            case=first_case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="COUNT_TIMING",
        )
        reopening = ProductInventoryAuditEvent.objects.create(
            case=second_case,
            action=ProductInventoryAuditEvent.Action.REOPEN,
            reason_code="SOURCE_CHANGED",
        )

        invalid_relations = (
            (second_case.pk, explanation.pk),
            (second_case.pk, reopening.pk),
        )
        for case_id, related_event_id in invalid_relations:
            with self.subTest(case_id=case_id, related_event_id=related_event_id):
                with self.assertRaises(IntegrityError), transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO reportes_productinventoryauditevent
                                (case_id, action, reason_code, notes, evidence,
                                 actor_id, created_at, related_event_id, metadata)
                            VALUES (%s, %s, %s, '', NULL, NULL, %s, %s, '{}'::jsonb)
                            """,
                            [
                                case_id,
                                ProductInventoryAuditEvent.Action.APPROVE,
                                "RAW_INVALID_REVIEW",
                                timezone.now(),
                                related_event_id,
                            ],
                        )

    def test_event_database_rejects_raw_update_and_delete(self):
        case = self._case()
        event = ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="COUNT_TIMING",
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE reportes_productinventoryauditevent
                    SET notes = %s
                    WHERE id = %s
                    """,
                    ["Intento SQL", event.pk],
                )
        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM reportes_productinventoryauditevent WHERE id = %s",
                    [event.pk],
                )

    def test_event_actor_is_set_null_when_user_is_deleted(self):
        case = self._case()
        event = ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="COUNT_TIMING",
            actor=self.actor,
        )

        self.actor.delete()

        event.refresh_from_db()
        self.assertIsNone(event.actor_id)

    def test_event_database_rejects_actor_null_without_deleting_user(self):
        case = self._case()
        event = ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="COUNT_TIMING",
            actor=self.actor,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE reportes_productinventoryauditevent
                    SET actor_id = NULL
                    WHERE id = %s
                    """,
                    [event.pk],
                )
                cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    def test_event_base_manager_bulk_create_cannot_bypass_database_contract(self):
        case = self._case()
        invalid_events = (
            ProductInventoryAuditEvent(
                case=case,
                action="INVALID",
                reason_code="INVALID_ACTION",
            ),
            ProductInventoryAuditEvent(
                case=case,
                action=ProductInventoryAuditEvent.Action.EXPLAIN,
                reason_code="   ",
            ),
            ProductInventoryAuditEvent(
                case=case,
                action=ProductInventoryAuditEvent.Action.REOPEN,
                reason_code="SOURCE_CHANGED",
                metadata=[],
            ),
        )

        for event in invalid_events:
            with self.subTest(action=event.action, reason=event.reason_code):
                with self.assertRaises(IntegrityError), transaction.atomic():
                    ProductInventoryAuditEvent._base_manager.bulk_create([event])

    def test_custom_approval_permission_exists(self):
        permission = Permission.objects.get(
            content_type__app_label="reportes",
            codename="approve_product_inventory_audit",
        )

        self.assertEqual(permission.name, "Puede aprobar auditorías de inventario")
