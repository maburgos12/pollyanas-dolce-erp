from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.exceptions import ImproperlyConfigured, SuspiciousFileOperation
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import Client, TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal, UserModuleAccess, UserProfile
from pos_bridge.models import (
    PointBranch,
    PointConversionLine,
    PointDailySale,
    PointOpenTransferSnapshotMember,
    PointProduct,
    PointSyncJob,
    PointTransferLine,
)
from pos_bridge.services.open_transfer_sync_service import persist_open_transfer_snapshot
from reportes.models import (
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)


class InventoryTraceabilityViewsTests(TestCase):
    def test_missing_close_is_not_labeled_as_a_proven_zero_or_available_close(self):
        from reportes.views_inventory_traceability import _case_status_context, _case_payload
        self.case.movement_status = "SOURCE_INCOMPLETE"
        self.case.source_trace = {"opening": [], "closing": []}
        self.assertEqual(_case_status_context(self.case)["point"], "Sin cierre comprobado")
        self.assertEqual(_case_status_context(self.case)["balance"], "Saldo no comprobado")
        self.assertIsNone(_case_payload(self.case)["difference"])

    def setUp(self):
        self.private_evidence_directory = TemporaryDirectory()
        self.addCleanup(self.private_evidence_directory.cleanup)
        private_settings = override_settings(
            INVENTORY_AUDIT_PRIVATE_ROOT=self.private_evidence_directory.name,
            DATA_UPLOAD_MAX_MEMORY_SIZE=12 * 1024 * 1024,
        )
        private_settings.enable()
        self.addCleanup(private_settings.disable)
        self.erp_branch = Sucursal.objects.create(
            codigo="AUD-VIEW-1",
            nombre="Sucursal auditoría vistas",
        )
        self.other_erp_branch = Sucursal.objects.create(
            codigo="AUD-VIEW-2",
            nombre="Otra sucursal auditoría",
        )
        self.branch = PointBranch.objects.create(
            external_id="audit-view-branch",
            name="Sucursal auditoría vistas",
            erp_branch=self.erp_branch,
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
        UserProfile.objects.update_or_create(
            user=self.explainer,
            defaults={"sucursal": self.erp_branch},
        )
        UserProfile.objects.update_or_create(
            user=self.approver,
            defaults={"sucursal": self.erp_branch},
        )

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

    def test_case_detail_includes_history_from_point_alias_of_same_erp_branch(self):
        alias = PointBranch.objects.create(
            external_id="alias-audit-history",
            name="Alias histórico",
            erp_branch=self.erp_branch,
        )
        alias_case = self._case(
            branch=alias,
            calculation_fingerprint="e" * 64,
        )
        legacy_event = ProductInventoryAuditEvent.objects.create(
            case=alias_case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="OTHER",
            notes="Explicación histórica conservada.",
            actor=self.explainer,
        )
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk])
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            legacy_event.id,
            [event["id"] for event in response.json()["case"]["events"]],
        )

    def test_alias_history_does_not_replace_current_explanation_for_review(self):
        current_explanation = ProductInventoryAuditEvent.objects.create(
            case=self.case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="OTHER",
            notes="Explicación vigente.",
            actor=self.explainer,
        )
        self.case.movement_status = (
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
        )
        self.case.save(update_fields=["movement_status", "updated_at"])
        alias = PointBranch.objects.create(
            external_id="alias-reviewed-history",
            name="Alias con revisión histórica",
            erp_branch=self.erp_branch,
        )
        alias_case = self._case(
            branch=alias,
            calculation_fingerprint="f" * 64,
        )
        alias_explanation = ProductInventoryAuditEvent.objects.create(
            case=alias_case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="OTHER",
            notes="Explicación de alias.",
            actor=self.explainer,
        )
        ProductInventoryAuditEvent.objects.create(
            case=alias_case,
            action=ProductInventoryAuditEvent.Action.APPROVE,
            reason_code="REVIEWED",
            related_event=alias_explanation,
            actor=self.viewer,
        )
        self.client.force_login(self.approver)

        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["latest_explanation"], current_explanation)
        self.assertTrue(response.context["can_review"])
        self.assertContains(response, "Explicación de alias.")

    def test_browser_dashboard_prioritizes_exceptions_and_exposes_operational_filters(self):
        self.case.conversion_in = Decimal("8")
        self.case.issue_codes = ["CONVERSION_ORIGIN_UNRESOLVED"]
        self.case.save(update_fields=["conversion_in", "issue_codes"])
        balanced_product = PointProduct.objects.create(
            external_id="audit-view-balanced-product",
            sku="AUDIT-BALANCED",
            name="Producto conciliado",
        )
        balanced_case = self._case(
            product=balanced_product,
            difference=Decimal("0"),
            movement_status=ProductInventoryAuditCase.MovementStatus.BALANCED,
            calculation_fingerprint="7" * 64,
        )
        pending_product = PointProduct.objects.create(
            external_id="audit-view-pending-product",
            sku="AUDIT-PENDING",
            name="Producto por aprobar",
        )
        pending_case = self._case(
            product=pending_product,
            movement_status=ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
            calculation_fingerprint="8" * 64,
        )
        self.client.force_login(self.viewer)

        with patch(
            "reportes.services_inventory_traceability.InventoryAuditMaterializer.rebuild"
        ) as rebuild, CaptureQueriesContext(connection) as queries:
            response = self.client.get(
                reverse("reportes:inventory_audit"),
                {"month": "2026-08"},
                HTTP_ACCEPT="text/html",
            )

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "reportes/auditoria_inventario.html")
        self.assertContains(response, "Cuadran con Point")
        self.assertContains(response, "Pendientes")
        self.assertContains(response, "Pendientes de aprobación")
        self.assertContains(
            response,
            "Origen de conversión por identificar",
        )
        self.assertContains(response, 'name="month"')
        self.assertContains(response, 'name="branch"')
        self.assertContains(response, 'name="status"')
        self.assertContains(response, '<colgroup>', html=False)
        self.assertContains(response, "inventory-audit-table-wrap")
        self.assertContains(response, 'class="inventory-audit-table"')
        content = response.content.decode()
        self.assertEqual(content.count("<main"), 1)
        self.assertContains(response, 'aria-current="page">Excepciones')
        self.assertContains(response, 'tab=balanced')
        self.assertContains(response, "Excepciones <span>2</span>", html=True)
        self.assertContains(response, "Conciliados <span>1</span>", html=True)
        self.assertContains(response, self.product.name)
        self.assertContains(response, pending_product.name)
        self.assertNotContains(response, balanced_product.name)
        self.assertEqual(response.context["kpis"]["balanced"], 1)
        self.assertEqual(response.context["kpis"]["pending"], 1)
        self.assertEqual(response.context["kpis"]["pending_approval"], 1)
        stylesheet = Path("static/css/inventory_audit_v1.css").read_text()
        self.assertContains(response, "css/inventory_audit_v1.css")
        self.assertIn(".inventory-audit-table thead th", stylesheet)
        self.assertIn("position: sticky", stylesheet)
        global_stylesheet = Path("static/css/styles.css").read_text()
        guardrail_stylesheet = Path("static/css/hallmark_guardrails.css").read_text()

        def rule_body(css, selector):
            selector_start = css.index(selector)
            block_start = css.index("{", selector_start)
            block_end = css.index("}", block_start)
            return css[block_start + 1 : block_end]

        global_table_rule = rule_body(global_stylesheet, ".table-responsive > table,")
        audit_table_rule = rule_body(
            stylesheet,
            ".inventory-audit-table-wrap > .inventory-audit-table",
        )
        audit_cell_rule = rule_body(
            stylesheet,
            ".inventory-audit-table-wrap > .inventory-audit-table th,",
        )
        audit_wrap_rule = rule_body(stylesheet, ".inventory-audit-table-wrap {")
        guardrail_table_rule = rule_body(
            guardrail_stylesheet,
            ".main-content[data-hallmark-scope=\"erp\"] :is(\n"
            "  .table-responsive > table,",
        )
        self.assertIn("table-layout: auto", global_table_rule)
        self.assertIn("width: max-content", global_table_rule)
        self.assertIn("overflow: hidden", global_table_rule)
        self.assertGreater((0, 2, 0), (0, 1, 1))
        self.assertIn("table-layout: fixed", audit_table_rule)
        self.assertIn("width: 100%", audit_table_rule)
        self.assertIn("min-width: 1050px", audit_table_rule)
        self.assertIn("overflow: visible !important", audit_table_rule)
        self.assertIn("--table-min-width: 1050px", audit_wrap_rule)
        self.assertIn(
            "min-width: max(100%, var(--table-min-width, 0px))",
            guardrail_table_rule,
        )
        self.assertIn("white-space: normal", audit_cell_rule)
        self.assertIn("overflow-wrap: anywhere", audit_cell_rule)
        forbidden_live_sources = (
            "pos_bridge_daily_sales",
            "pos_bridge_production_lines",
            "pos_bridge_waste_lines",
            "pos_bridge_transfer_lines",
            "pos_bridge_conversion_lines",
            "pos_bridge_sync_jobs",
        )
        self.assertTrue(
            all(
                all(source not in query["sql"] for source in forbidden_live_sources)
                for query in queries.captured_queries
            ),
            queries.captured_queries,
        )
        rebuild.assert_not_called()

        balanced_response = self.client.get(
            reverse("reportes:inventory_audit"),
            {"month": "2026-08", "tab": "balanced"},
            HTTP_ACCEPT="text/html",
        )
        self.assertContains(balanced_response, 'aria-current="page">Conciliados')
        self.assertContains(balanced_response, balanced_product.name)
        self.assertNotContains(balanced_response, self.product.name)
        self.assertNotContains(balanced_response, pending_product.name)
        self.assertEqual(balanced_response.content.decode().count("<table"), 1)

    def test_browser_dashboard_names_transfer_mismatch_without_declaring_merma(self):
        self.case.issue_codes = ["TRANSFER_QUANTITY_MISMATCH"]
        self.case.transfer_out = Decimal("4")
        self.case.save(update_fields=["issue_codes", "transfer_out"])
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit"),
            {"month": "2026-08"},
            HTTP_ACCEPT="text/html",
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Transferencia por conciliar")
        self.assertNotContains(response, "Merma registrada o pendiente")

    def test_browser_dashboard_filters_attention_and_shows_compact_owner(self):
        self.case.attention_level = ProductInventoryAuditCase.AttentionLevel.HIGH
        self.case.responsible_area = ProductInventoryAuditCase.ResponsibleArea.LOGISTICS
        self.case.assigned_to = self.viewer
        self.case.investigation_summary = {
            "facts": ["Transferencia ligada a ruta RUT-202608-0029."],
            "hypotheses": [],
            "missing": ["Confirmar recepción."],
        }
        self.case.save(
            update_fields=[
                "attention_level",
                "responsible_area",
                "assigned_to",
                "investigation_summary",
                "updated_at",
            ]
        )
        other_product = PointProduct.objects.create(
            external_id="audit-normal-product",
            sku="AUDIT-NORMAL",
            name="Producto de atención normal",
        )
        self._case(
            product=other_product,
            attention_level=ProductInventoryAuditCase.AttentionLevel.NORMAL,
            calculation_fingerprint="1" * 64,
        )
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit"),
            {"month": "2026-08", "attention": "HIGH"},
            HTTP_ACCEPT="text/html",
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'name="attention"')
        self.assertContains(response, "Atención inmediata")
        self.assertContains(response, "Logística · audit.viewer")
        self.assertContains(response, self.product.name)
        self.assertNotContains(response, other_product.name)

        detail = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )
        self.assertContains(detail, "Investigación del agente auditor")
        self.assertContains(detail, "Comprobado")
        self.assertContains(detail, "Transferencia ligada a ruta RUT-202608-0029.")
        self.assertContains(detail, "Confirmar recepción.")

    def test_browser_dashboard_hides_legacy_point_alias_case_for_same_erp_branch(self):
        self.branch.external_id = "1"
        self.branch.save(update_fields=["external_id", "updated_at"])
        alias = PointBranch.objects.create(
            external_id="Sucursal auditoría vistas",
            name="Alias nominal que no debe duplicarse",
            erp_branch=self.erp_branch,
        )
        self._case(
            branch=alias,
            calculation_fingerprint="9" * 64,
        )
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit"),
            {"month": "2026-08"},
            HTTP_ACCEPT="text/html",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["result_count"], 1)
        self.assertNotContains(response, "Alias nominal que no debe duplicarse")

    def test_browser_dashboard_filters_materialized_rows_without_loading_other_months(self):
        other_branch = PointBranch.objects.create(
            external_id="audit-view-filter-branch",
            name="Devoluciones",
        )
        other_case = self._case(
            branch=other_branch,
            calculation_fingerprint="6" * 64,
        )
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit"),
            {
                "month": "2026-08",
                "branch": str(other_branch.pk),
                "status": ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
            },
            HTTP_ACCEPT="text/html",
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Devoluciones")
        self.assertContains(response, other_case.product.name)
        self.assertEqual([case.pk for case in response.context["cases"]], [other_case.pk])

    def test_browser_dashboard_paginates_rows_and_preserves_filters(self):
        products = PointProduct.objects.bulk_create(
            [
                PointProduct(
                    external_id=f"audit-page-{index}",
                    sku=f"PAGE-{index:03d}",
                    name=f"Producto paginado {index:03d}",
                )
                for index in range(60)
            ]
        )
        for index, product in enumerate(products, start=1):
            self._case(
                product=product,
                difference=Decimal("-1"),
                calculation_fingerprint=f"{index:064x}",
            )
        self.client.force_login(self.viewer)

        with CaptureQueriesContext(connection) as queries:
            response = self.client.get(
                reverse("reportes:inventory_audit"),
                {
                    "month": "2026-08",
                    "branch": str(self.branch.pk),
                    "status": ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
                    "tab": "exceptions",
                    "page": "2",
                },
                HTTP_ACCEPT="text/html",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["result_count"], 61)
        self.assertEqual(len(response.context["cases"]), 11)
        self.assertContains(response, "Página 2 de 2")
        self.assertContains(response, "month=2026-08")
        self.assertContains(response, f"branch={self.branch.pk}")
        self.assertContains(response, "status=NEEDS_EXPLANATION")
        self.assertEqual(response.content.decode().count("<table"), 1)
        audit_queries = [
            query["sql"]
            for query in queries.captured_queries
            if "reportes_productinventoryaudit" in query["sql"]
        ]
        self.assertLessEqual(len(audit_queries), 7)
        self.assertLessEqual(len(queries), 35)

    def test_invalid_month_uses_html_error_for_navigation_and_json_for_async(self):
        self.client.force_login(self.viewer)
        url = reverse("reportes:inventory_audit")

        html_response = self.client.get(
            url,
            {"month": "2026-13"},
            HTTP_ACCEPT="text/html",
        )
        json_response = self.client.get(
            url,
            {"month": "2026-13"},
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(html_response.status_code, 400)
        self.assertTrue(html_response["Content-Type"].startswith("text/html"))
        self.assertContains(html_response, "Mes no válido", status_code=400)
        self.assertEqual(json_response.status_code, 400)
        self.assertEqual(json_response.json()["ok"], False)

    def test_browser_detail_explains_balance_sequence_and_source_evidence(self):
        sale = PointDailySale.objects.create(
            branch=self.branch,
            product=self.product,
            sale_date=date(2026, 8, 15),
            quantity=Decimal("4"),
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )
        self.case.source_trace = {
            "sales": [sale.pk, 999999],
            "opening": [],
            "closing": [],
            "production": [],
            "waste": [],
            "transfers": [],
            "conversions": [],
            "adjustments": [],
        }
        self.case.save(update_fields=["source_trace"])
        self.client.force_login(self.explainer)

        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "reportes/auditoria_inventario_caso.html")
        content = response.content.decode()
        self.assertEqual(content.count("<main"), 1)
        balance_content = content[content.index('<ol class="inventory-audit-balance-list">'):]
        labels = [
            "Inventario inicial Point",
            "Producción",
            "Transferencias recibidas",
            "Conversiones de entrada",
            "Ventas",
            "Merma",
            "Transferencias enviadas",
            "Conversiones de salida",
            "Ajustes identificados",
            "Inventario esperado",
            "Cierre Point",
            "Diferencia",
        ]
        positions = [balance_content.index(label) for label in labels]
        self.assertEqual(positions, sorted(positions))
        sales_step = content[
            content.index('data-balance-step="sales"') : content.index(
                'data-balance-step="waste"'
            )
        ]
        self.assertIn("15/08/2026", sales_step)
        self.assertIn("Evidencia ya no disponible en la fuente", sales_step)
        self.assertNotContains(response, "Evidencia de origen")
        self.assertContains(response, self.branch.name)
        self.assertContains(response, "4")
        self.assertContains(response, f"Point #{sale.pk}")
        self.assertContains(response, "No informado por Point")
        self.assertContains(response, "Evidencia ya no disponible en la fuente")
        self.assertContains(response, 'data-async-action')
        self.assertContains(response, 'data-pending-label="Procesando…"')
        self.assertContains(response, "Cierre Point")
        self.assertContains(response, "Movimientos")
        self.assertContains(response, "Conteo físico")

    def test_browser_detail_groups_transfer_and_conversion_evidence_by_balance_direction(self):
        self.product.sku = ""
        self.product.save(update_fields=["sku"])
        other_branch = PointBranch.objects.create(
            external_id="audit-evidence-other-branch",
            name="CEDIS auditoría",
        )
        incoming_transfer = PointTransferLine.objects.create(
            origin_branch=other_branch,
            destination_branch=self.branch,
            transfer_external_id="TR-IN",
            detail_external_id="TR-IN-1",
            source_hash="1" * 64,
            registered_at=timezone.now(),
            received_at=timezone.now(),
            item_name=self.product.name,
            item_code=self.product.sku,
            received_quantity=Decimal("3"),
            received_by="Carolina",
        )
        outgoing_transfer = PointTransferLine.objects.create(
            origin_branch=self.branch,
            destination_branch=other_branch,
            transfer_external_id="TR-OUT",
            detail_external_id="TR-OUT-1",
            source_hash="2" * 64,
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            item_name=self.product.name,
            item_code=self.product.sku,
            sent_quantity=Decimal("2"),
            sent_by="Johana",
        )
        conversion_in = PointConversionLine.objects.create(
            branch=self.branch,
            movement_external_id="CONV-IN",
            source_hash="3" * 64,
            movement_at=timezone.now(),
            item_name=self.product.name,
            item_code=self.product.sku,
            quantity=Decimal("8"),
            source_item_name="Pastel entero",
            source_item_code="ENTERO-1",
            raw_payload={"responsable": "Carolina"},
        )
        conversion_out = PointConversionLine.objects.create(
            branch=self.branch,
            movement_external_id="CONV-OUT",
            source_hash="4" * 64,
            movement_at=timezone.now(),
            item_name="Rebanada",
            item_code="REB-1",
            quantity=Decimal("10"),
            source_item_name=self.product.name,
            source_item_code=self.product.sku,
            raw_payload={"responsable": "Carolina"},
        )
        self.case.source_trace = {
            "transfer_in": [incoming_transfer.pk, 991001],
            "transfer_out": [outgoing_transfer.pk, 991002],
            "conversion_in": [conversion_in.pk, 992001],
            "conversion_out": [conversion_out.pk, 992002],
            "transfers": [incoming_transfer.pk, outgoing_transfer.pk, 999001],
            "conversions": [conversion_in.pk, conversion_out.pk, 999002],
            "conversion_in_impacts": {str(conversion_in.pk): "8.0000"},
            "conversion_out_impacts": {str(conversion_out.pk): "0.8333"},
        }
        self.case.save(update_fields=["source_trace"])
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )

        content = response.content.decode()
        transfer_in_step = content[
            content.index('data-balance-step="transfer_in"') : content.index(
                'data-balance-step="conversion_in"'
            )
        ]
        transfer_out_step = content[
            content.index('data-balance-step="transfer_out"') : content.index(
                'data-balance-step="conversion_out"'
            )
        ]
        conversion_in_step = content[
            content.index('data-balance-step="conversion_in"') : content.index(
                'data-balance-step="sales"'
            )
        ]
        conversion_out_step = content[
            content.index('data-balance-step="conversion_out"') : content.index(
                'data-balance-step="identified_adjustment"'
            )
        ]
        self.assertIn("TR-IN", transfer_in_step)
        self.assertNotIn("TR-OUT", transfer_in_step)
        self.assertIn("Point #991001", transfer_in_step)
        self.assertNotIn("Point #991002", transfer_in_step)
        self.assertIn("TR-OUT", transfer_out_step)
        self.assertNotIn("TR-IN", transfer_out_step)
        self.assertIn("Point #991002", transfer_out_step)
        self.assertNotIn("Point #991001", transfer_out_step)
        self.assertIn("CONV-IN", conversion_in_step)
        self.assertNotIn("CONV-OUT", conversion_in_step)
        self.assertIn("Point #992001", conversion_in_step)
        self.assertNotIn("Point #992002", conversion_in_step)
        self.assertIn("CONV-OUT", conversion_out_step)
        self.assertNotIn("CONV-IN", conversion_out_step)
        self.assertIn("Point #992002", conversion_out_step)
        self.assertNotIn("Point #992001", conversion_out_step)
        self.assertIn("Cantidad", conversion_in_step)
        self.assertIn("8", conversion_in_step)
        self.assertNotIn("Cantidad registrada en Point", conversion_in_step)
        self.assertIn(
            "Cantidad registrada en Point (producto destino)",
            conversion_out_step,
        )
        self.assertIn("0.8333", conversion_out_step)
        self.assertIn("10", conversion_out_step)
        self.assertNotContains(response, "Evidencia sin dirección disponible")

    def test_browser_detail_keeps_legacy_flat_trace_explicitly_undirected(self):
        self.case.source_trace = {
            "transfers": [993001],
            "conversions": [993002],
        }
        self.case.save(update_fields=["source_trace"])
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )

        self.assertContains(response, "Evidencia sin dirección disponible")
        self.assertContains(response, "Point #993001")
        self.assertContains(response, "Point #993002")

    def test_browser_detail_resolves_immutable_open_transfer_snapshot_evidence(self):
        other_branch = PointBranch.objects.create(
            external_id="audit-snapshot-other-branch",
            name="CEDIS histórico",
        )
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_SUCCESS,
        )
        transfer = PointTransferLine.objects.create(
            origin_branch=self.branch,
            destination_branch=other_branch,
            sync_job=job,
            transfer_external_id="TR-SNAPSHOT",
            detail_external_id="TR-SNAPSHOT-1",
            source_hash="9" * 64,
            registered_at=datetime(
                2026, 8, 31, 20, 0, tzinfo=timezone.get_current_timezone()
            ),
            sent_at=datetime(
                2026, 8, 31, 21, 0, tzinfo=timezone.get_current_timezone()
            ),
            item_name=self.product.name,
            item_code=self.product.external_id,
            sent_quantity=Decimal("2"),
            sent_by="Johana",
            is_open=True,
            is_finalized=True,
        )
        snapshot, _manifest = persist_open_transfer_snapshot(
            sync_job=job,
            lines=[transfer],
            operational_date=date(2026, 8, 31),
            captured_at=datetime(
                2026, 9, 1, 2, 4, tzinfo=timezone.get_current_timezone()
            ),
        )
        member = PointOpenTransferSnapshotMember.objects.get(snapshot=snapshot)
        transfer.delete()
        self.case.source_trace = {
            "open_transfer_snapshot_out": [member.pk],
        }
        self.case.save(update_fields=["source_trace"])
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )

        content = response.content.decode()
        transfer_out_step = content[
            content.index('data-balance-step="transfer_out"') : content.index(
                'data-balance-step="conversion_out"'
            )
        ]
        self.assertIn("TR-SNAPSHOT", transfer_out_step)
        self.assertIn("Sucursal auditoría vistas → CEDIS histórico", transfer_out_step)
        self.assertIn("2", transfer_out_step)
        self.assertNotIn("Evidencia ya no disponible", transfer_out_step)

    def test_browser_detail_maps_reason_codes_without_exposing_internal_tokens(self):
        ProductInventoryAuditEvent.objects.create(
            case=self.case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="UNKNOWN_INTERNAL_TOKEN",
            notes="Explicación histórica",
            actor=self.explainer,
        )
        self.client.force_login(self.viewer)

        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )

        self.assertContains(response, "Causa registrada")
        self.assertNotContains(response, "UNKNOWN_INTERNAL_TOKEN")
        stylesheet = Path("static/css/inventory_audit_v1.css").read_text()
        self.assertIn(".inventory-audit-heading h1", stylesheet)
        self.assertIn("font-family: 'Playfair Display', serif", stylesheet)
        scenarios = (
            (
                {
                    "status": "FOUND",
                    "last_matching_label": "10/08/2026 22:31",
                    "first_mismatch_label": "11/08/2026 22:31",
                    "unlocated_quantity": "2",
                    "movement_labels": [
                        "2 movimiento(s) de producciones",
                        "3 movimiento(s) de salidas por transferencia",
                    ],
                    "warnings": [],
                },
                ("Primer corte con diferencia", "11/08/2026 22:31", "2 unidades continúan sin localizar"),
            ),
            (
                {
                    "status": "INCONCLUSIVE",
                    "minimum": "6",
                    "maximum": "15",
                    "movement_labels": [],
                    "warnings": [],
                },
                ("Point no informa el orden", "saldo entre", "<strong>6</strong>", "<strong>15</strong>"),
            ),
            (
                {
                    "status": "INSUFFICIENT_EVIDENCE",
                    "movement_labels": [],
                    "warnings": ["No existen cortes intermedios de Point para este caso."],
                },
                ("No existe un corte intermedio suficiente",),
            ),
        )
        for daily_break, expected in scenarios:
            with self.subTest(status=daily_break["status"]):
                self.case.investigation_summary = {
                    "facts": [],
                    "hypotheses": [],
                    "missing": [],
                    "daily_break": daily_break,
                }
                self.case.difference = Decimal("-2")
                self.case.save(
                    update_fields=["investigation_summary", "difference", "updated_at"]
                )
                response = self.client.get(
                    reverse("reportes:inventory_audit_case", args=[self.case.pk]),
                    HTTP_ACCEPT="text/html",
                )
                content = response.content.decode()
                for text in expected:
                    self.assertIn(text, content)
                self.assertNotIn(f">{daily_break['status']}<", content)
                if daily_break["status"] == "FOUND":
                    daily_block = content[
                        content.index("inventory-audit-daily-break") : content.index("balance-title")
                    ]
                    self.assertNotIn("transfer_out", daily_block)

    def test_browser_detail_prefetches_event_actors_without_query_per_event(self):
        for index in range(8):
            ProductInventoryAuditEvent.objects.create(
                case=self.case,
                action=ProductInventoryAuditEvent.Action.EXPLAIN,
                reason_code="OTHER",
                notes=f"Evidencia {index}",
                actor=self.explainer,
            )
        self.client.force_login(self.viewer)

        with CaptureQueriesContext(connection) as queries:
            response = self.client.get(
                reverse("reportes:inventory_audit_case", args=[self.case.pk]),
                HTTP_ACCEPT="text/html",
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.explainer.username, count=8)
        event_queries = [
            query["sql"]
            for query in queries.captured_queries
            if "reportes_productinventoryauditevent" in query["sql"]
        ]
        self.assertEqual(len(event_queries), 1)
        self.assertIn("JOIN", event_queries[0])
        self.assertLessEqual(len(queries), 30)

    def test_browser_detail_hides_actions_outside_permission_and_custody(self):
        self.client.force_login(self.viewer)
        response = self.client.get(
            reverse("reportes:inventory_audit_case", args=[self.case.pk]),
            HTTP_ACCEPT="text/html",
        )

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'action="%s"' % reverse(
            "reportes:inventory_audit_explain", args=[self.case.pk]
        ))
        self.assertNotContains(response, "Aprobar explicación")

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
        evidence_content = b"%PDF-1.4\nPrivate inventory audit evidence"
        self.client.force_login(self.explainer)
        response = self.client.post(
            reverse("reportes:inventory_audit_explain", args=[self.case.pk]),
            {
                "reason_code": "OTHER",
                "notes": "Se adjunta evidencia de custodia.",
                "evidence": SimpleUploadedFile(
                    "evidencia.pdf",
                    evidence_content,
                    content_type="application/pdf",
                ),
            },
        )

        self.assertEqual(response.status_code, 302)
        event = self.case.events.get()
        stored_name = event.evidence.name
        self.assertNotEqual(stored_name, "evidencia.pdf")
        self.assertEqual(Path(stored_name).parent, Path("."))
        self.assertEqual(event.metadata["evidence_original_name"], "evidencia.pdf")
        stored_path = Path(self.private_evidence_directory.name, stored_name)
        self.assertTrue(stored_path.is_file())
        self.assertEqual(stored_path.read_bytes(), evidence_content)

        download = self.client.get(
            reverse(
                "reportes:inventory_audit_evidence",
                args=[self.case.pk, event.pk],
            )
        )
        self.assertEqual(download.status_code, 200)
        self.assertEqual(b"".join(download.streaming_content), evidence_content)
        self.assertIn(
            'attachment; filename="evidencia.pdf"',
            download["Content-Disposition"],
        )
        self.assertEqual(download["X-Content-Type-Options"], "nosniff")
        self.assertEqual(download["Cache-Control"], "private, no-store")
        self.assertEqual(self.client.get(f"/media/{stored_name}").status_code, 404)

    def test_evidence_filefield_uses_private_storage_without_public_url(self):
        storage = ProductInventoryAuditEvent._meta.get_field("evidence").storage
        private_root = Path(self.private_evidence_directory.name).resolve()

        self.assertEqual(Path(storage.path("sample.pdf")).parent, private_root)
        with self.assertRaises(NotImplementedError):
            storage.url("sample.pdf")
        with self.assertRaises(SuspiciousFileOperation):
            storage.open("../escape.pdf")

    def test_private_storage_rejects_media_root_and_symlink_into_it(self):
        storage = ProductInventoryAuditEvent._meta.get_field("evidence").storage
        media_root = Path(self.private_evidence_directory.name, "public-media")
        media_root.mkdir()
        linked_private = Path(self.private_evidence_directory.name, "linked-private")
        linked_private.symlink_to(media_root, target_is_directory=True)

        for invalid_private_root in (media_root / "audit", linked_private):
            with self.subTest(private_root=invalid_private_root):
                with override_settings(
                    MEDIA_ROOT=media_root,
                    INVENTORY_AUDIT_PRIVATE_ROOT=invalid_private_root,
                ):
                    with self.assertRaises(ImproperlyConfigured):
                        storage.path("sample.pdf")

    def test_evidence_rejects_disguised_or_oversized_files_without_writing(self):
        self.client.force_login(self.explainer)
        url = reverse("reportes:inventory_audit_explain", args=[self.case.pk])

        disguised = self.client.post(
            url,
            {
                "reason_code": "OTHER",
                "notes": "Archivo con extensión falsa.",
                "evidence": SimpleUploadedFile(
                    "evidencia.png",
                    b"this is not a png",
                    content_type="image/png",
                ),
            },
        )
        oversized = self.client.post(
            url,
            {
                "reason_code": "OTHER",
                "notes": "Archivo demasiado grande.",
                "evidence": SimpleUploadedFile(
                    "evidencia.pdf",
                    b"%PDF-" + (b"x" * (10 * 1024 * 1024)),
                    content_type="application/pdf",
                ),
            },
        )

        self.assertEqual(disguised.status_code, 400)
        self.assertContains(disguised, "contenido no coincide", status_code=400)
        self.assertEqual(oversized.status_code, 400)
        self.assertContains(oversized, "excede el límite de 10 MB", status_code=400)
        self.assertFalse(self.case.events.exists())
        self.assertEqual(list(Path(self.private_evidence_directory.name).iterdir()), [])

    def test_private_evidence_download_requires_report_and_case_custody(self):
        self.client.force_login(self.explainer)
        self.client.post(
            reverse("reportes:inventory_audit_explain", args=[self.case.pk]),
            {
                "reason_code": "OTHER",
                "notes": "Evidencia privada.",
                "evidence": SimpleUploadedFile(
                    "private.pdf",
                    b"%PDF-1.4\nprivate",
                    content_type="application/pdf",
                ),
            },
        )
        event = self.case.events.get()
        url = reverse(
            "reportes:inventory_audit_evidence", args=[self.case.pk, event.pk]
        )

        self.client.logout()
        self.assertEqual(self.client.get(url).status_code, 302)
        self.client.force_login(self.outsider)
        self.assertEqual(self.client.get(url).status_code, 403)
        self.client.force_login(self.viewer)
        self.assertEqual(self.client.get(url).status_code, 403)
        UserProfile.objects.update_or_create(
            user=self.viewer, defaults={"sucursal": self.other_erp_branch}
        )
        self.assertEqual(self.client.get(url).status_code, 403)

    def test_private_evidence_download_rejects_case_event_mismatch(self):
        self.client.force_login(self.explainer)
        self.client.post(
            reverse("reportes:inventory_audit_explain", args=[self.case.pk]),
            {
                "reason_code": "OTHER",
                "notes": "Evidencia del caso original.",
                "evidence": SimpleUploadedFile(
                    "private.pdf",
                    b"%PDF-1.4\nprivate",
                    content_type="application/pdf",
                ),
            },
        )
        event = self.case.events.get()
        other_product = PointProduct.objects.create(
            external_id="audit-view-other-product",
            sku="AUDIT-VIEW-2",
            name="Otro producto auditoría vistas",
        )
        other_case = self._case(
            product=other_product,
            calculation_fingerprint="9" * 64,
        )

        response = self.client.get(
            reverse(
                "reportes:inventory_audit_evidence",
                args=[other_case.pk, event.pk],
            )
        )

        self.assertEqual(response.status_code, 404)

    def test_failed_transaction_removes_private_evidence(self):
        self.client.force_login(self.explainer)
        uploaded = SimpleUploadedFile(
            "rollback.pdf",
            b"%PDF-1.4\nrollback",
            content_type="application/pdf",
        )

        with patch.object(
            ProductInventoryAuditCase,
            "save",
            side_effect=RuntimeError("forced rollback"),
        ):
            with self.assertRaises(RuntimeError):
                self.client.post(
                    reverse(
                        "reportes:inventory_audit_explain", args=[self.case.pk]
                    ),
                    {
                        "reason_code": "OTHER",
                        "notes": "Debe revertir archivo y base.",
                        "evidence": uploaded,
                    },
                )

        self.assertFalse(self.case.events.exists())
        self.assertEqual(list(Path(self.private_evidence_directory.name).iterdir()), [])

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
        self.assertContains(html_response, f'action="{url}"', status_code=400)
        self.assertContains(html_response, 'method="post"', status_code=400)
        self.assertContains(html_response, 'enctype="multipart/form-data"', status_code=400)
        self.assertContains(html_response, 'name="csrfmiddlewaretoken"', status_code=400)
        self.assertContains(html_response, 'name="evidence"', status_code=400)
        self.assertContains(html_response, 'type="submit"', status_code=400)
        self.assertContains(
            html_response,
            f'id="inventory-audit-case-{self.case.pk}"',
            status_code=400,
        )
        self.assertEqual(json_response.status_code, 400)
        self.assertEqual(json_response.json()["fields"]["reason_code"], "CAUSA")
        self.assertEqual(json_response.json()["fields"]["notes"], "")
        self.assertIn('name="reason_code"', json_response.json()["html"])
        self.assertNotIn('value="CAUSA"', json_response.json()["html"])
        self.assertIn("Causa registrada", json_response.json()["html"])
        self.assertIn('name="evidence"', json_response.json()["html"])
        self.assertEqual(self.case.events.count(), 0)

    def test_explanation_rejects_reason_code_outside_operational_allowlist(self):
        response = self._explain(
            reason_code="INTERNAL_ADMIN_OVERRIDE",
            notes="Intento manipulado.",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(self.case.events.exists())

    def test_notes_length_is_bounded_for_explanations_and_reviews(self):
        long_notes = "x" * 4001
        explanation = self._explain(notes=long_notes)
        self.assertEqual(explanation.status_code, 400)
        self.assertFalse(self.case.events.exists())

        self._explain()
        self.client.force_login(self.approver)
        review = self.client.post(
            reverse("reportes:inventory_audit_approve", args=[self.case.pk]),
            {"reason_code": "REVIEWED", "notes": long_notes},
        )
        self.assertEqual(review.status_code, 400)
        self.assertEqual(self.case.events.count(), 1)

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
            {"reason_code": "MANIPULATED", "notes": "Cadena comprobada"},
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
        self.assertEqual(approval.reason_code, "REVIEWED")

    def test_reject_adds_history_and_returns_case_to_needs_explanation(self):
        self._explain()
        explanation = self.case.events.get()
        self.client.force_login(self.approver)

        response = self.client.post(
            reverse("reportes:inventory_audit_reject", args=[self.case.pk]),
            {"reason_code": "MANIPULATED", "notes": "Falta la contraparte"},
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
        self.assertEqual(rejection.reason_code, "INSUFFICIENT")

    def test_reject_requires_nonblank_notes_for_html_and_async_requests(self):
        self._explain()
        self.client.force_login(self.approver)
        url = reverse("reportes:inventory_audit_reject", args=[self.case.pk])

        html_response = self.client.post(
            url,
            {"reason_code": "MANIPULATED", "notes": "   "},
        )
        json_response = self.client.post(
            url,
            {"reason_code": "MANIPULATED", "notes": "   "},
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(html_response.status_code, 400)
        self.assertContains(html_response, f'action="{url}"', status_code=400)
        self.assertContains(html_response, 'value="INSUFFICIENT"', status_code=400)
        self.assertEqual(json_response.status_code, 400)
        self.assertEqual(json_response.json()["fields"]["reason_code"], "INSUFFICIENT")
        self.assertEqual(json_response.json()["fields"]["notes"], "   ")
        self.assertIn('value="INSUFFICIENT"', json_response.json()["html"])
        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        )
        self.assertEqual(self.case.events.count(), 1)

    def test_review_fails_closed_when_current_explanation_has_no_actor(self):
        self.case.movement_status = ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
        self.case.save(update_fields=["movement_status"])
        ProductInventoryAuditEvent.objects.create(
            case=self.case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="LEGACY",
            notes="Explicación histórica sin actor.",
            actor=None,
        )
        self.client.force_login(self.approver)

        for route_name in (
            "inventory_audit_approve",
            "inventory_audit_reject",
        ):
            with self.subTest(route_name=route_name):
                response = self.client.post(
                    reverse(f"reportes:{route_name}", args=[self.case.pk]),
                    {"reason_code": "REVIEWED", "notes": "No debe avanzar."},
                )
                self.assertEqual(response.status_code, 409)

        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        )
        self.assertEqual(self.case.events.count(), 1)

    def test_action_endpoints_reject_get_and_enforce_csrf(self):
        self.client.force_login(self.explainer)
        action_urls = [
            reverse("reportes:inventory_audit_explain", args=[self.case.pk]),
            reverse("reportes:inventory_audit_approve", args=[self.case.pk]),
            reverse("reportes:inventory_audit_reject", args=[self.case.pk]),
        ]
        for url in action_urls:
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, 405)

        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(self.explainer)
        response = csrf_client.post(
            action_urls[0],
            {"reason_code": "CAUSE", "notes": "Sin token"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("login"))
        self.assertFalse(self.case.events.exists())

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
        self.assertIn("Pendiente de aprobación", payload["html"])
        self.assertNotIn(">PENDING_APPROVAL<", payload["html"])
        self.assertEqual(
            payload["redirect"],
            f"{reverse('reportes:inventory_audit_case', args=[self.case.pk])}"
            f"#inventory-audit-case-{self.case.pk}",
        )
        self.assertTrue(payload["reload"])

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

    def test_operational_user_cannot_explain_case_from_another_custody(self):
        other_point_branch = PointBranch.objects.create(
            external_id="audit-view-other-branch",
            name="Otra sucursal Point",
            erp_branch=self.other_erp_branch,
        )
        other_case = self._case(
            branch=other_point_branch,
            calculation_fingerprint="e" * 64,
        )
        self.client.force_login(self.explainer)

        response = self.client.post(
            reverse("reportes:inventory_audit_explain", args=[other_case.pk]),
            {"reason_code": "TRANSFER_PENDING", "notes": "No es mi custodia"},
        )

        self.assertEqual(response.status_code, 403)
        other_case.refresh_from_db()
        self.assertEqual(
            other_case.movement_status,
            ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
        )
        self.assertFalse(other_case.events.exists())

    def test_approver_cannot_review_case_from_another_custody(self):
        self._explain()
        UserProfile.objects.update_or_create(
            user=self.approver,
            defaults={"sucursal": self.other_erp_branch},
        )
        self.client.force_login(self.approver)

        response = self.client.post(
            reverse("reportes:inventory_audit_approve", args=[self.case.pk]),
            {"reason_code": "REVIEWED", "notes": "Fuera de custodia"},
        )

        self.assertEqual(response.status_code, 403)
        self.case.refresh_from_db()
        self.assertEqual(
            self.case.movement_status,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        )
        self.assertEqual(self.case.events.count(), 1)

    def test_unmapped_cedis_and_returns_fail_closed_for_ordinary_user(self):
        for suffix, name in (
            ("cedis", "CEDIS"),
            ("returns", "Devoluciones"),
        ):
            with self.subTest(location=name):
                point_branch = PointBranch.objects.create(
                    external_id=f"audit-view-{suffix}",
                    name=name,
                )
                case = self._case(
                    branch=point_branch,
                    calculation_fingerprint=("f" if suffix == "cedis" else "1") * 64,
                )
                self.client.force_login(self.explainer)

                response = self.client.post(
                    reverse("reportes:inventory_audit_explain", args=[case.pk]),
                    {"reason_code": "OTHER", "notes": "Revisión"},
                )

                self.assertEqual(response.status_code, 403)
                self.assertFalse(case.events.exists())

    def test_explicit_global_report_manager_can_act_on_unmapped_custody(self):
        global_user = get_user_model().objects.create_user(username="audit.global")
        UserModuleAccess.objects.create(
            user=global_user,
            module="reportes",
            access=UserModuleAccess.ACCESS_MANAGE,
        )
        global_user.user_permissions.add(
            Permission.objects.get(codename="change_productinventoryauditcase")
        )
        unmapped_branch = PointBranch.objects.create(
            external_id="audit-view-global-unmapped",
            name="Devoluciones sin enlace ERP",
        )
        case = self._case(
            branch=unmapped_branch,
            calculation_fingerprint="2" * 64,
        )
        self.client.force_login(global_user)

        response = self.client.post(
            reverse("reportes:inventory_audit_explain", args=[case.pk]),
            {"reason_code": "OTHER", "notes": "Revisión global"},
        )

        self.assertEqual(response.status_code, 302)
        case.refresh_from_db()
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        )

    def test_submodule_manager_is_not_treated_as_global_custody_authority(self):
        limited_manager = get_user_model().objects.create_user(
            username="audit.limited-manager"
        )
        UserModuleAccess.objects.create(
            user=limited_manager,
            module="reportes.financiero",
            access=UserModuleAccess.ACCESS_MANAGE,
        )
        limited_manager.user_permissions.add(
            Permission.objects.get(codename="change_productinventoryauditcase")
        )
        unmapped_branch = PointBranch.objects.create(
            external_id="audit-view-limited-unmapped",
            name="CEDIS fuera del submódulo",
        )
        case = self._case(
            branch=unmapped_branch,
            calculation_fingerprint="4" * 64,
        )
        self.client.force_login(limited_manager)

        response = self.client.post(
            reverse("reportes:inventory_audit_explain", args=[case.pk]),
            {"reason_code": "OTHER", "notes": "Fuera de alcance"},
        )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(case.events.exists())

    def test_superuser_can_act_on_unmapped_custody(self):
        superuser = get_user_model().objects.create_superuser(
            username="audit.superuser",
            email="audit.superuser@example.com",
            password="test12345",
        )
        unmapped_branch = PointBranch.objects.create(
            external_id="audit-view-super-unmapped",
            name="CEDIS sin enlace ERP",
        )
        case = self._case(
            branch=unmapped_branch,
            calculation_fingerprint="3" * 64,
        )
        self.client.force_login(superuser)

        response = self.client.post(
            reverse("reportes:inventory_audit_explain", args=[case.pk]),
            {"reason_code": "OTHER", "notes": "Revisión DG"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(case.events.filter(actor=superuser).exists())
