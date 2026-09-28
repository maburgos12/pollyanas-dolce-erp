from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import UserModuleAccess
from pos_bridge.models import PointBranch, PointProduct
from reportes.models import (
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)


class InventoryTraceabilityViewsTests(TestCase):
    def setUp(self):
        self.branch = PointBranch.objects.create(
            external_id="audit-view-branch",
            name="Sucursal auditoría vistas",
        )
        self.product = PointProduct.objects.create(
            external_id="audit-view-product",
            sku="AUDIT-VIEW-1",
            name="Producto auditoría vistas",
        )
        self.run = ProductInventoryAuditRun.objects.create(
            month=date(2026, 8, 1),
            calculation_fingerprint="a" * 64,
        )
        self.case = self._case()

        user_model = get_user_model()
        self.viewer = user_model.objects.create_user(username="audit.viewer")
        self.explainer = user_model.objects.create_user(username="audit.explainer")
        self.approver = user_model.objects.create_user(username="audit.approver")
        self.outsider = user_model.objects.create_user(username="audit.outsider")
        for user in (self.viewer, self.explainer, self.approver):
            UserModuleAccess.objects.create(
                user=user,
                module="reportes",
                access=UserModuleAccess.ACCESS_VIEW,
            )

        change_permission = Permission.objects.get(
            codename="change_productinventoryauditcase"
        )
        approve_permission = Permission.objects.get(
            codename="approve_product_inventory_audit"
        )
        self.explainer.user_permissions.add(change_permission)
        self.explainer.user_permissions.add(approve_permission)
        self.approver.user_permissions.add(approve_permission)

    def _case(self, **overrides):
        values = {
            "run": self.run,
            "month": self.run.month,
            "branch": self.branch,
            "product": self.product,
            "opening_point": Decimal("10"),
            "production": Decimal("5"),
            "sales": Decimal("4"),
            "waste": Decimal("1"),
            "transfer_in": Decimal("0"),
            "transfer_out": Decimal("0"),
            "conversion_in": Decimal("0"),
            "conversion_out": Decimal("0"),
            "identified_adjustment": Decimal("0"),
            "expected_closing": Decimal("10"),
            "point_closing": Decimal("9"),
            "difference": Decimal("-1"),
            "movement_status": ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
            "calculation_fingerprint": "b" * 64,
            "rebuilt_at": timezone.now(),
        }
        values.update(overrides)
        return ProductInventoryAuditCase.objects.create(**values)

    def _explain(self, *, user=None, reason_code="TRANSFER_PENDING", notes="Falta recepción."):
        self.client.force_login(user or self.explainer)
        return self.client.post(
            reverse("reportes:inventory_audit_explain", args=[self.case.pk]),
            {"reason_code": reason_code, "notes": notes},
        )

    def test_report_viewer_can_read_materialized_dashboard_and_detail_without_rebuild(self):
        other_run = ProductInventoryAuditRun.objects.create(
            month=date(2026, 7, 1),
            calculation_fingerprint="c" * 64,
        )
        other_case = self._case(
            run=other_run,
            month=other_run.month,
            calculation_fingerprint="d" * 64,
        )
        self.client.force_login(self.viewer)

        with patch(
            "reportes.services_inventory_traceability.InventoryAuditMaterializer.rebuild"
        ) as rebuild:
            dashboard = self.client.get(
                reverse("reportes:inventory_audit"), {"month": "2026-08"}
            )
            detail = self.client.get(
                reverse("reportes:inventory_audit_case", args=[self.case.pk])
            )

        self.assertEqual(dashboard.status_code, 200)
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(
            [row["id"] for row in dashboard.json()["cases"]], [self.case.pk]
        )
        self.assertNotIn(other_case.pk, [row["id"] for row in dashboard.json()["cases"]])
        self.assertEqual(detail.json()["case"]["id"], self.case.pk)
        rebuild.assert_not_called()

    def test_user_without_report_access_cannot_read_dashboard_or_detail(self):
        self.client.force_login(self.outsider)

        dashboard = self.client.get(reverse("reportes:inventory_audit"))
        detail = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk])
        )

        self.assertEqual(dashboard.status_code, 403)
        self.assertEqual(detail.status_code, 403)

    def test_change_permission_can_explain_and_traditional_post_returns_stable_fragment(self):
        response = self._explain()

        self.assertRedirects(
            response,
            f"{reverse('reportes:inventory_audit_case', args=[self.case.pk])}"
            f"#inventory-audit-case-{self.case.pk}",
            fetch_redirect_response=False,
        )
        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        )
        event = self.case.events.get()
        self.assertEqual(event.action, ProductInventoryAuditEvent.Action.EXPLAIN)
        self.assertEqual(event.actor, self.explainer)
        self.assertEqual(event.reason_code, "TRANSFER_PENDING")
        self.assertEqual(event.notes, "Falta recepción.")

    def test_explanation_accepts_optional_evidence(self):
        self.client.force_login(self.explainer)
        response = self.client.post(
            reverse("reportes:inventory_audit_explain", args=[self.case.pk]),
            {
                "reason_code": "PHYSICAL_EVIDENCE",
                "notes": "Se adjunta evidencia de custodia.",
                "evidence": SimpleUploadedFile("evidencia.txt", b"evidencia"),
            },
        )

        self.assertEqual(response.status_code, 302)
        evidence = self.case.events.get().evidence
        self.assertIn("reportes/inventory-audit/", evidence.name)
        self.assertEqual(evidence.read(), b"evidencia")

    def test_explanation_requires_change_permission(self):
        response = self._explain(user=self.viewer)

        self.assertEqual(response.status_code, 403)
        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
        )
        self.assertFalse(self.case.events.exists())

    def test_invalid_explanation_preserves_fields_and_returns_400_in_both_formats(self):
        self.client.force_login(self.explainer)
        url = reverse("reportes:inventory_audit_explain", args=[self.case.pk])

        html_response = self.client.post(
            url,
            {"reason_code": "", "notes": "Comentario conservado"},
        )
        json_response = self.client.post(
            url,
            {"reason_code": "CAUSA", "notes": ""},
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(html_response.status_code, 400)
        self.assertContains(
            html_response, "Comentario conservado", status_code=400
        )
        self.assertEqual(json_response.status_code, 400)
        self.assertEqual(json_response.json()["fields"]["reason_code"], "CAUSA")
        self.assertEqual(json_response.json()["fields"]["notes"], "")
        self.assertEqual(self.case.events.count(), 0)

    def test_approver_needs_custom_permission_and_cannot_be_explainer(self):
        self._explain()
        approve_url = reverse(
            "reportes:inventory_audit_approve", args=[self.case.pk]
        )

        self.client.force_login(self.viewer)
        without_permission = self.client.post(
            approve_url, {"reason_code": "REVIEWED", "notes": "Validado"}
        )
        self.client.force_login(self.explainer)
        self_approval = self.client.post(
            approve_url, {"reason_code": "REVIEWED", "notes": "Validado"}
        )

        self.assertEqual(without_permission.status_code, 403)
        self.assertEqual(self_approval.status_code, 409)
        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        )
        self.assertEqual(self.case.events.count(), 1)

    def test_different_approver_resolves_against_current_explanation(self):
        self._explain()
        explanation = self.case.events.get()
        self.client.force_login(self.approver)

        response = self.client.post(
            reverse("reportes:inventory_audit_approve", args=[self.case.pk]),
            {"reason_code": "REVIEWED", "notes": "Cadena comprobada"},
        )

        self.assertEqual(response.status_code, 302)
        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.RESOLVED,
        )
        approval = self.case.events.get(action=ProductInventoryAuditEvent.Action.APPROVE)
        self.assertEqual(approval.related_event, explanation)
        self.assertEqual(approval.actor, self.approver)

    def test_reject_adds_history_and_returns_case_to_needs_explanation(self):
        self._explain()
        explanation = self.case.events.get()
        self.client.force_login(self.approver)

        response = self.client.post(
            reverse("reportes:inventory_audit_reject", args=[self.case.pk]),
            {"reason_code": "INSUFFICIENT", "notes": "Falta la contraparte"},
        )

        self.assertEqual(response.status_code, 302)
        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
        )
        self.assertEqual(self.case.events.count(), 2)
        rejection = self.case.events.get(action=ProductInventoryAuditEvent.Action.REJECT)
        self.assertEqual(rejection.related_event, explanation)

    def test_async_action_returns_toast_target_and_updated_case_fragment(self):
        self.client.force_login(self.explainer)

        response = self.client.post(
            reverse("reportes:inventory_audit_explain", args=[self.case.pk]),
            {"reason_code": "TRANSFER_PENDING", "notes": "En revisión"},
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["toast"]["type"], "success")
        self.assertEqual(payload["target"], f"#inventory-audit-case-{self.case.pk}")
        self.assertIn(f'id="inventory-audit-case-{self.case.pk}"', payload["html"])
        self.assertIn("PENDING_APPROVAL", payload["html"])

    def test_invalid_or_duplicate_transitions_return_conflict_without_extra_events(self):
        self.case.movement_status = ProductInventoryAuditCase.MovementStatus.BALANCED
        self.case.save(update_fields=["movement_status"])
        invalid_explain = self._explain()
        self.assertEqual(invalid_explain.status_code, 409)
        self.assertFalse(self.case.events.exists())

        self.case.movement_status = ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION
        self.case.save(update_fields=["movement_status"])
        self._explain()
        self.client.force_login(self.approver)
        url = reverse("reportes:inventory_audit_approve", args=[self.case.pk])
        first = self.client.post(
            url, {"reason_code": "REVIEWED", "notes": "Validado"}
        )
        second = self.client.post(
            url, {"reason_code": "REVIEWED", "notes": "Validado"}
        )

        self.assertEqual(first.status_code, 302)
        self.assertEqual(second.status_code, 409)
        self.assertEqual(self.case.events.count(), 2)
