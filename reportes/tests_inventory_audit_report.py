from decimal import Decimal
from importlib import import_module, util

from django.test import TestCase
from django.test import RequestFactory
from unittest.mock import patch

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointProduct, PointProductCategory
from reportes.models import ProductInventoryAuditCase
from reportes.tests_inventory_audit_agent import InventoryAuditAgentFixtures


class InventoryAuditReportTests(InventoryAuditAgentFixtures, TestCase):
    def test_commercial_sales_are_preserved_in_legacy_history_projection(self):
        self.make_case(sales=Decimal("0"), source_trace={"point_history": {
            "aggregate_comparison": {"sales": {
                "aggregate": "5", "point_history": "0", "difference": "-5",
            }},
        }})
        row = self.service().read_audit_report(self.month)["rows"][0]
        self.assertEqual(row["vendido"], Decimal("5"))
        self.assertEqual(ProductInventoryAuditCase.objects.get().sales, Decimal("0"))

    def test_stock_balances_without_claiming_traceability_is_closed(self):
        self.make_case(difference=Decimal('0'), point_closing=Decimal('10'),
                       issue_codes=['INCOMPLETE_TRANSFER'])
        row = self.service().read_audit_report(self.month)['rows'][0]
        self.assertEqual(row['estado_inventario'], 'Conciliado')
        self.assertEqual(row['estado_trazabilidad'], 'Pendiente de conciliar')

    def service(self):
        name = "reportes.services_inventory_audit_report"
        self.assertIsNotNone(util.find_spec(name), "Falta la lectura compartida del auditor")
        return import_module(name)

    def test_status_never_nets_pending_branches(self):
        service = self.service()
        self.assertEqual(service.audit_status(["NEEDS_EXPLANATION"] * 2), "Pendiente de conciliar")
        self.assertEqual(service.audit_status(["BALANCED", "SOURCE_INCOMPLETE"]), "Falta información")
        self.assertEqual(service.audit_status([]), "Aún no auditado")
        self.assertEqual(service.audit_status(["BALANCED", "RESOLVED"]), "Conciliado")

    def test_report_reuses_cases_and_filters_real_erp_branch(self):
        first = Sucursal.objects.create(codigo="AUD-R1", nombre="Primera")
        second = Sucursal.objects.create(codigo="AUD-R2", nombre="Segunda")
        self.branch.erp_branch = first
        self.branch.save()
        other = PointBranch.objects.create(external_id="AUD-R2", name="Segunda", erp_branch=second)
        self.make_case(difference=Decimal("2"), point_closing=Decimal("12"))
        self.make_case(branch=other, difference=Decimal("-2"), point_closing=Decimal("8"))
        report = self.service().read_audit_report(self.month)
        self.assertEqual(len(report["rows"]), 1)
        self.assertEqual(report["rows"][0]["diferencia_inventario"], 0)
        self.assertEqual(report["rows"][0]["estado_inventario"], "Pendiente de conciliar")
        selected = self.service().read_audit_report(self.month, branch=str(first.pk))
        self.assertEqual(selected["rows"][0]["diferencia_inventario"], 2)
        self.assertEqual(selected["rows"][0]["inventario_final_teorico"], 10)
        self.assertEqual(len(selected["rows"][0]["cases"]), 1)

    def test_missing_opening_is_not_zero_but_known_closing_remains_visible(self):
        self.make_case(movement_status="SOURCE_INCOMPLETE", issue_codes=["SOURCE_INCOMPLETE"],
                       source_trace={"opening": [], "closing": [22]}, opening_point=0)
        row = self.service().read_audit_report(self.month)["rows"][0]
        self.assertIsNone(row["inventario_inicial"])
        self.assertIsNone(row["inventario_final_teorico"])
        self.assertIsNone(row["diferencia_inventario"])
        self.assertEqual(row["inventario_final_point_total"], 9)
        self.assertEqual(row["estado_inventario"], "Falta información")

    def test_product_without_recipe_is_not_dropped(self):
        self.make_case()
        row = self.service().read_audit_report(self.month)["rows"][0]
        self.assertEqual(row["product_id"], self.product.pk)
        self.assertEqual(row["receta"], self.product.name)
        self.assertIsNone(row["receta_id"])

    def test_empty_month_does_not_claim_complete(self):
        from datetime import date
        result = self.service().read_audit_report(date(2026, 9, 1))
        self.assertEqual(result["audit_status"], "Aún no auditado")
        self.assertEqual(result["rows"], [])

    def test_consumption_is_separate_without_deleting_cases_or_resale_products(self):
        for name, code in (("Empaque Pastel", "EMP"), ("TOPPING FRESA", "TOP"),
                           ("Fresa para consumo", "TOP-CAT"), ("TE DEL JARDIN", "TE"),
                           ("CAJA G PARA VENTA", "CAJA")):
            product = PointProduct.objects.create(external_id=code, sku=code, name=name)
            self.make_case(product=product)
        PointProductCategory.objects.create(codigo_point="TOP-CAT", nombre="Fresa para consumo", category="TOPPING")
        report = self.service().read_audit_report(self.month)
        self.assertEqual({row["receta"] for row in report["rows"]}, {"TE DEL JARDIN", "CAJA G PARA VENTA"})
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 5)
        self.assertEqual(ProductInventoryAuditCase.objects.sold_products().count(), 2)

    def test_only_active_approved_addons_are_separate_from_sold_products(self):
        from recetas.models import Receta, RecetaAgrupacionAddon
        from pos_bridge.models.product import inventory_consumption_filter

        base = Receta.objects.create(nombre="Pay natural", codigo_point="BASE-PAY", hash_contenido="BASE-PAY")
        base_product = PointProduct.objects.create(external_id="BASE-PAY", sku="BASE-PAY", name=base.nombre)
        self.make_case(product=base_product)
        for code, status, active in (
            ("APPROVED", "APPROVED", True),
            ("DETECTED", "DETECTED", True),
            ("REJECTED", "REJECTED", True),
            ("INACTIVE", "APPROVED", False),
        ):
            recipe = Receta.objects.create(nombre=f"Sabor {code}", codigo_point=code, hash_contenido=code)
            product = PointProduct.objects.create(external_id=code, sku=code, name=recipe.nombre)
            self.make_case(product=product, sales=Decimal("14"))
            RecetaAgrupacionAddon.objects.create(
                base_receta=base, addon_receta=recipe, addon_codigo_point=code,
                addon_nombre_point=recipe.nombre, status=status, activo=active,
            )

        self.assertEqual(set(ProductInventoryAuditCase.objects.sold_products().values_list(
            "product__sku", flat=True)), {"BASE-PAY", "DETECTED", "REJECTED", "INACTIVE"})
        self.assertEqual({row["receta"] for row in self.service().read_audit_report(self.month)["rows"]},
                         {"Pay natural", "Sabor DETECTED", "Sabor REJECTED", "Sabor INACTIVE"})
        self.assertEqual(set(Receta.objects.filter(inventory_consumption_filter(
            name_field="nombre", code_field="codigo_point")).values_list("codigo_point", flat=True)), {"APPROVED"})
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 5)
        self.assertEqual(ProductInventoryAuditCase.objects.get(product__sku="APPROVED").sales, Decimal("14"))

    def test_view_and_exports_read_persisted_audit_not_another_balance(self):
        from reportes.views_produccion import ProducidoVsVendidoMermaView
        self.make_case(difference=Decimal("2"), point_closing=Decimal("12"))
        request = RequestFactory().get("/reportes/produccion/", {"periodo": "2026-08"})
        with patch("pos_bridge.services.monthly_product_balance_service.MonthlyPointProductBalanceService") as balance:
            view = ProducidoVsVendidoMermaView()
            context = view._build_context(request)
            balance.assert_not_called()
        row = context["json_rows"][0]
        self.assertEqual(row["diferencia_inventario"], "2.0000")
        self.assertEqual(row["estado_inventario"], "Pendiente de conciliar")
        for export in ("_export_csv", "_export_xlsx", "_export_pdf"):
            self.assertEqual(getattr(view, export)(context).status_code, 200)
