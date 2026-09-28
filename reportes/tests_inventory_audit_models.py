from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
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
            source_issues=[{"code": "POINT_CLOSING_MISSING", "branch_id": self.branch.pk}],
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

    def test_custom_approval_permission_exists(self):
        permission = Permission.objects.get(
            content_type__app_label="reportes",
            codename="approve_product_inventory_audit",
        )

        self.assertEqual(permission.name, "Puede aprobar auditorías de inventario")
