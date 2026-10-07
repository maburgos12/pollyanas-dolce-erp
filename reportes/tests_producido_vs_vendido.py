"""Report contracts against persisted audit cases, not a mocked second balance."""
from datetime import date
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import SuspiciousOperation
from django.db import connection
from django.template.loader import render_to_string
from django.test import RequestFactory, TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from openpyxl import load_workbook

from core.models import Sucursal
from pos_bridge.models import PointProduct
from recetas.models import Receta
from reportes.models import ProductInventoryAuditRun
from reportes.tests_inventory_audit_agent import InventoryAuditAgentFixtures
from reportes.views_produccion import ProducidoVsVendidoMermaView


class ProducidoVsVendidoAuditTests(InventoryAuditAgentFixtures, TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.view = ProducidoVsVendidoMermaView()
        self.recipe = Receta.objects.create(nombre=self.product.name, codigo_point=self.product.sku,
            tipo=Receta.TIPO_PRODUCTO_FINAL, categoria="Pastel Chico", pasa_modulo_produccion=True,
            hash_contenido="audit-report-test")

    def context(self, **params):
        return self.view._build_context(self.factory.get("/reportes/produccion/", {"periodo": "2026-08", **params}))

    def render(self, context):
        request = self.factory.get("/reportes/produccion/")
        request.user = AnonymousUser()
        return render_to_string(self.view.template_name, context, request=request)

    def test_all_quantities_are_the_auditors_persisted_values(self):
        self.make_case(opening_point=10, production=5, sales=4, waste=1, transfer_in=2,
            transfer_out=1, conversion_in=2, conversion_out=3, identified_adjustment=1,
            expected_closing=11, point_closing=12, difference=1)
        with patch("pos_bridge.services.monthly_product_balance_service.MonthlyPointProductBalanceService") as other:
            context = self.context()
            other.assert_not_called()
        row = context["groups"][0]["rows"][0]
        for field, value in (("inventario_inicial", 10), ("producido", 5), ("vendido", 4),
            ("merma_reportada", 1), ("transferencia_entrada", 2), ("transferencia_salida", 1),
            ("conversion_entrada", 2), ("conversion_salida", 3), ("ajuste_identificado", 1),
            ("inventario_final_teorico", 11), ("inventario_final_point_total", 12), ("diferencia_inventario", 1)):
            self.assertEqual(row[field], value, field)
        self.assertEqual(row["estado_inventario"], "Pendiente de conciliar")

    def test_balanced_stock_keeps_pending_transfer_visible(self):
        self.make_case(difference=0, movement_status="NEEDS_EXPLANATION",
                       issue_codes=["INCOMPLETE_TRANSFER"])
        context = self.context()
        row = context["groups"][0]["rows"][0]
        self.assertEqual(row["estado_inventario"], "Conciliado")
        self.assertEqual(row["estado_trazabilidad"], "Pendiente de conciliar")
        self.assertIn("Trazabilidad pendiente", self.render(context))
        self.assertIn("Saldo conciliado · trazabilidad pendiente", self.view._export_csv(context).content.decode())

    def test_missing_opening_preserves_known_closing_in_html_and_exports(self):
        self.make_case(opening_point=0, point_closing=22, movement_status="SOURCE_INCOMPLETE",
            source_trace={"opening": [], "closing": [1]}, issue_codes=["SOURCE_INCOMPLETE"])
        context = self.context()
        row = context["groups"][0]["rows"][0]
        for key in ("inventario_inicial", "inventario_final_teorico", "diferencia_inventario"):
            self.assertIsNone(row[key])
        self.assertEqual(row["inventario_final_point_total"], 22)
        self.assertIn("Falta información", self.render(context))
        for export in ("_export_csv", "_export_xlsx", "_export_pdf"):
            self.assertEqual(getattr(self.view, export)(context).status_code, 200)

    def test_unknown_slice_origin_does_not_create_fractional_parent_exit(self):
        self.make_case(conversion_in=414, conversion_out=0, issue_codes=["CONVERSION_SOURCE_UNRESOLVED"])
        row = self.context()["groups"][0]["rows"][0]
        self.assertEqual(row["conversion_entrada"], 414)
        self.assertEqual(row["conversion_salida"], 0)
        self.assertEqual(row["conversion_provenance_label"], "Origen por identificar")
        self.assertNotIn("41.4", self.view._export_csv(self.context()).content.decode())

    def test_filter_aliases_and_erp_branch_are_preserved(self):
        branch = Sucursal.objects.create(codigo="PVVAUD", nombre="Sucursal auditada")
        self.branch.erp_branch = branch
        self.branch.save()
        self.make_case()
        context = self.context(branch=str(branch.pk), familia="Pastel Chico")
        self.assertEqual(len(context["json_rows"]), 1)
        self.assertEqual(context["selected_branch"], str(branch.pk))
        self.assertIn("branch=" + str(branch.pk), self.render(context))
        self.assertEqual(self.context(categoria="Rebanada")["json_rows"], [])

    def test_invalid_branch_is_rejected(self):
        for branch in ("999999", "not-a-number"):
            with self.assertRaises(SuspiciousOperation):
                self.context(branch=branch)

    def test_zero_waste_has_zero_cost_without_known_unit_cost(self):
        self.make_case(waste=0)
        self.assertEqual(Decimal(self.context()["json_rows"][0]["costo_merma"]), 0)

    def test_only_confirmed_waste_recipes_are_costed(self):
        self.make_case(waste=2)
        with patch("reportes.views_produccion.get_total_cost_map", return_value={self.recipe.pk: Decimal("3")}) as cost:
            row = self.context()["json_rows"][0]
        cost.assert_called_once_with([self.recipe.pk])
        self.assertEqual(Decimal(row["costo_merma"]), 6)

    def test_reference_products_do_not_pollute_operational_difference(self):
        self.recipe.pasa_modulo_produccion = False
        self.recipe.save()
        self.make_case(production=3, sales=2)
        context = self.context()
        self.assertIsNone(context["grand_total"]["dif"])
        self.assertEqual(context["json_rows"][0]["dif_referencia"], "1.0000")

    def test_no_audit_does_not_claim_zero_or_complete(self):
        context = self.context()
        self.assertEqual(context["audit_metadata"]["status"], "Aún no auditado")
        self.assertEqual(context["json_rows"], [])

    def test_stale_rebuild_keeps_date_of_last_valid_result(self):
        self.make_case()
        stamp = timezone.now()
        self.audit_run.last_successful_rebuild_at = stamp
        self.audit_run.rebuilt_at = stamp + timezone.timedelta(minutes=1)
        self.audit_run.save()
        context = self.context()
        self.assertTrue(context["audit_metadata"]["stale"])
        self.assertEqual(context["audit_updated_at"], stamp)
        self.assertIn("Pendiente de actualización", self.render(context))

    def test_partial_publication_uses_its_timestamp_without_claiming_month_closed(self):
        self.make_case()
        stamp = timezone.now()
        self.audit_run.last_successful_rebuild_at = stamp
        self.audit_run.rebuilt_at = stamp + timezone.timedelta(minutes=1)
        self.audit_run.partial_published = True
        self.audit_run.status = ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE
        self.audit_run.save()
        context = self.context()
        self.assertTrue(context["audit_metadata"]["partial"])
        self.assertEqual(context["audit_updated_at"], self.audit_run.rebuilt_at)
        html = self.render(context)
        self.assertIn("Actualización parcial", html)
        self.assertIn("septiembre no está cerrado", html)
        self.assertIn("Parcial: 1 de 1 productos", html)

    def test_kpis_show_known_products_as_partial_not_zero_for_missing_product(self):
        self.make_case(production=5, sales=4, waste=0)
        missing = PointProduct.objects.create(
            external_id="MISSING-PRODUCT", sku="MISSING-PRODUCT", name="Producto sin fuente",
        )
        self.make_case(product=missing, movement_status="SOURCE_INCOMPLETE",
                       issue_codes=["CASE_MISSING_FROM_REBUILD"])
        context = self.context()
        self.assertIsNone(context["grand_total"]["vendido"])
        self.assertEqual(context["kpi_totals"]["vendido"], 4)
        self.assertEqual(context["kpi_totals"]["producido"], 5)
        self.assertEqual(context["kpi_coverage"]["vendido"], {"known": 1, "total": 2})
        self.assertIn("Parcial: 1 de 2 productos", self.render(context))

    def test_month_catalog_uses_runs_not_raw_history(self):
        ProductInventoryAuditRun.objects.create(month=date(2026, 6, 1))
        with CaptureQueriesContext(connection) as queries:
            periods = self.view._available_periods(selected="2026-07")
        self.assertIn("2026-06", periods)
        self.assertIn("2026-07", periods)
        self.assertFalse(any("pos_bridge_inventory_snapshots" in q["sql"] or "pos_bridge_daily_sales" in q["sql"] for q in queries))

    def test_clean_html_has_native_traceability_and_sticky_aligned_headers(self):
        from django.urls import reverse
        case = self.make_case()
        html = self.render(self.context())
        self.assertIn("Ver trazabilidad", html)
        self.assertIn(reverse("reportes:inventory_audit_case", args=[case.pk]), html)
        self.assertIn("Sucursal o almacén", html)
        self.assertNotIn("production-source-list", html)
        self.assertNotIn("Autoridad Point:", html)
        self.assertIn("requestSubmit()", html)
        self.assertIn("window.setInterval(refreshAudit, 60000)", html)
        self.assertIn("document.hidden || busy || submitting", html)
        self.assertEqual(html.count('scope="col"'), 15)
        css = Path("static/css/styles.css").read_text()
        self.assertIn("max-height: calc(100dvh - 112px)", css)
        self.assertIn(".production-table-wrap .table thead th.text-end", css)

    def test_light_metadata_get_does_not_build_or_cost_the_report(self):
        request = self.factory.get("/reportes/produccion/", {"periodo": "2026-08", "metadata": "1"})
        with patch("reportes.views_produccion.read_audit_report") as report:
            response = self.view.get(request)
        self.assertEqual(response.status_code, 200)
        report.assert_not_called()

    def test_csv_xlsx_pdf_keep_numbers_missing_data_and_neutral_states(self):
        self.make_case(difference=2, point_closing=12, expected_closing=10)
        context = self.context()
        csv = self.view._export_csv(context).content.decode()
        self.assertIn("Saldo calculado", csv)
        self.assertIn("Sucursal o almacén", csv)
        self.assertIn("Transferencia entrada", csv)
        self.assertIn(context["audit_metadata"]["branch_label"], csv)
        self.assertIn(",10,12,2,Pendiente de conciliar", csv)
        workbook = load_workbook(BytesIO(self.view._export_xlsx(context).content), read_only=True)
        sheet = workbook["Producido vs Vendido"]
        headers = next(sheet.iter_rows(min_row=5, max_row=5, values_only=True))
        row = next(sheet.iter_rows(min_row=7, max_row=7, values_only=True))
        self.assertEqual(row[headers.index("Dif. Point")], 2)
        self.assertEqual(row[headers.index("Sucursal o almacén")], context["audit_metadata"]["branch_label"])
        pdf = self.view._export_pdf(context).content.decode("latin-1")
        self.assertIn("Dif. Point 2", pdf)
        self.assertIn("Pendiente de conciliar", pdf)
