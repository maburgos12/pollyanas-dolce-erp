from decimal import Decimal
from datetime import datetime
from importlib import import_module, util

from django.test import TestCase
from django.test import RequestFactory
from unittest.mock import patch

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointProduct, PointProductCategory
from reportes.models import ProductInventoryAuditCase
from reportes.tests_inventory_audit_agent import InventoryAuditAgentFixtures


class InventoryAuditReportTests(InventoryAuditAgentFixtures, TestCase):
    def sales_mismatch_case(self):
        return self.make_case(movement_status="SOURCE_INCOMPLETE", issue_codes=["SOURCE_INCOMPLETE"],
            opening_point=0, point_closing=2, transfer_in=10, transfer_out=2, sales=5, waste=2,
            expected_closing=1, difference=1, source_trace={"opening": [1], "closing": [2],
                "point_history": {"coverage_status": "COMPLETE", "unknown_movement_ids": [],
                    "unexplained_remainder": "0", "unapplied_reason": "SALES_STOCK_EFFECT_UNVERIFIED",
                    "documentary_opening": "0", "point_closing": "2", "sales": "4",
                    "production": "0", "waste": "2", "transfer_in": "10", "transfer_out": "2",
                    "conversion_in": "0", "conversion_out": "0", "identified_adjustment": "0",
                    "aggregate_comparison": {"sales": {"aggregate": "5", "point_history": "4", "difference": "-1"}}}})

    def test_sales_stock_difference_shows_calculation_without_closing_case(self):
        case = self.sales_mismatch_case()
        before = ProductInventoryAuditCase.objects.values().get(pk=case.pk)
        row = self.service().read_audit_report(self.month)["rows"][0]
        self.assertEqual(row["inventario_final_teorico"], 1)
        self.assertEqual(row["inventario_final_point_total"], 2)
        self.assertEqual(row["diferencia_inventario"], 1)
        self.assertEqual(row["estado_inventario"], "Pendiente de conciliar")
        self.assertEqual(row["estado_trazabilidad"], "Falta información")
        self.assertEqual(self.service().case_balance_status(case), "SOURCE_INCOMPLETE")
        self.assertEqual(row["cases"][0]["calculated"], 1)
        self.assertIn("5", row["cases"][0]["pending_reason"])
        self.assertIn("4", row["cases"][0]["pending_reason"])
        self.assertEqual(before, ProductInventoryAuditCase.objects.values().get(pk=case.pk))

    def test_sales_difference_does_not_hide_other_missing_or_contradictory_evidence(self):
        case = self.sales_mismatch_case()
        from copy import deepcopy
        original = deepcopy(case.source_trace)
        for key, value in (("coverage_status", "INCOMPLETE"), ("unknown_movement_ids", [123]),
                           ("unexplained_remainder", "1"), ("unapplied_reason", "OTHER"),
                           ("production", "1"), ("waste", None), ("transfer_in", "NaN"),
                           ("documentary_opening", "1"), ("point_closing", "3")):
            with self.subTest(key=key):
                case.source_trace = deepcopy(original)
                case.source_trace["point_history"][key] = value
                case.save(update_fields=["source_trace"])
                row = self.service().read_audit_report(self.month)["rows"][0]
                self.assertIsNone(row["inventario_final_teorico"])
                self.assertIsNone(row["diferencia_inventario"])
        case.source_trace = original
        case.source_trace["opening"] = []
        case.save(update_fields=["source_trace"])
        self.assertIsNone(self.service().read_audit_report(self.month)["rows"][0]["inventario_final_teorico"])

    def test_retired_projection_not_in_current_report_after_full_rebuild(self):
        from django.utils import timezone
        self.audit_run.last_successful_rebuild_at = timezone.now()
        self.audit_run.save(update_fields=["last_successful_rebuild_at"])
        self.make_case(movement_status="SOURCE_INCOMPLETE", issue_codes=["CASE_MISSING_FROM_REBUILD"])
        self.assertEqual(self.service().read_audit_report(self.month)["rows"], [])
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 1)
        self.audit_run.partial_published = True
        self.audit_run.save(update_fields=["partial_published"])
        self.assertEqual(len(self.service().read_audit_report(self.month)["rows"]), 1)

    def test_sales_comparison_must_match_its_own_quantity_and_stock_equation(self):
        from copy import deepcopy
        case = self.sales_mismatch_case()
        original = deepcopy(case.source_trace)
        for key, value in (("point_history", "3"), ("difference", "0"), ("aggregate", "NaN")):
            with self.subTest(key=key):
                case.source_trace = deepcopy(original)
                case.source_trace["point_history"]["aggregate_comparison"]["sales"][key] = value
                case.save(update_fields=["source_trace"])
                self.assertIsNone(self.service().read_audit_report(self.month)["rows"][0]["inventario_final_teorico"])
        case.source_trace = original
        case.transfer_in = 11
        case.source_trace["point_history"]["transfer_in"] = "11"
        case.save(update_fields=["source_trace", "transfer_in"])
        self.assertIsNone(self.service().read_audit_report(self.month)["rows"][0]["inventario_final_teorico"])

    def test_retired_wrong_product_source_excluded_even_from_partial_report(self):
        from django.utils import timezone
        from pos_bridge.models import PointTransferLine
        accessory = PointProduct.objects.create(external_id="1001", sku="875", name="VELA INDIVIDUAL", category="Alegría")
        transfer = PointTransferLine.objects.create(origin_branch=self.branch, destination_branch=self.branch,
            registered_at=timezone.make_aware(datetime(2026,8,10)), transfer_external_id="1", detail_external_id="1", source_hash="c"*64,
            raw_payload={"detail": {"FK_articulo": 1001, "Articulo": accessory.name, "isInsumo": False}})
        self.audit_run.partial_published = True
        self.audit_run.save(update_fields=["partial_published"])
        case = self.make_case(movement_status="SOURCE_INCOMPLETE", issue_codes=["CASE_MISSING_FROM_REBUILD"],
            source_trace={"transfers": [transfer.pk]})
        self.assertEqual(self.service().read_audit_report(self.month)["rows"], [])
        self.assertTrue(ProductInventoryAuditCase.objects.filter(pk=case.pk).exists())
        transfer.raw_payload["detail"]["FK_articulo"] = 99999
        transfer.save(update_fields=["raw_payload"])
        self.assertEqual(len(self.service().read_audit_report(self.month)["rows"]), 1)
        for field, value in (("FK_articulo", True), ("Articulo", "Otra vela"), ("isInsumo", True)):
            with self.subTest(field=field):
                transfer.raw_payload = {"detail": {"FK_articulo": 1001, "Articulo": accessory.name, "isInsumo": False}}
                transfer.raw_payload["detail"][field] = value
                transfer.save(update_fields=["raw_payload"])
                self.assertEqual(len(self.service().read_audit_report(self.month)["rows"]), 1)
        transfer.raw_payload = {"detail": {"FK_articulo": 1001, "Articulo": accessory.name, "isInsumo": False}}
        transfer.registered_at = timezone.make_aware(datetime(2025,8,10))
        transfer.save(update_fields=["raw_payload", "registered_at"])
        self.assertEqual(len(self.service().read_audit_report(self.month)["rows"]), 1)

    def test_sales_mismatch_equation_and_reason_reach_html_and_exports(self):
        from django.contrib.auth.models import AnonymousUser
        from django.template.loader import render_to_string
        from reportes.views_produccion import ProducidoVsVendidoMermaView
        self.sales_mismatch_case()
        request = RequestFactory().get('/reportes/produccion/', {'periodo': '2026-08'})
        request.user = AnonymousUser()
        view = ProducidoVsVendidoMermaView()
        context = view._build_context(request)
        html = render_to_string(view.template_name, context, request=request)
        self.assertIn('Saldo calculado: 1', html)
        self.assertIn('Final Point: 2', html)
        self.assertIn('Ventas registradas: 5; salidas por venta en inventario: 4.', html)
        self.assertEqual(context['groups'][0]['rows'][0]['inventario_final_teorico'], 1)
        for export in ('_export_csv', '_export_xlsx', '_export_pdf'):
            self.assertEqual(getattr(view, export)(context).status_code, 200)

    def test_dot_cake_manufactured_trials_are_included_without_sales(self):
        from recetas.models import Receta
        for code, name, produced in (("4358", "Vaso Dot Cake Chocolate", 2),
                                     ("8734", "Vaso Dot Cake Vainilla", 3)):
            product = PointProduct.objects.create(external_id=code, sku=code, name=name,
                                                 category="Vasos Grande")
            Receta.objects.create(nombre=name, codigo_point=code, hash_contenido=code,
                                  tipo=Receta.TIPO_PRODUCTO_FINAL, categoria="Vasos Preparados Grande",
                                  modo_costeo=Receta.MODO_COSTEO_FABRICADO, pasa_modulo_produccion=True)
            self.make_case(product=product, sales=0, production=produced, waste=produced)
        rows = self.service().read_audit_report(self.month)["rows"]
        self.assertEqual({row["receta"] for row in rows},
                         {"Vaso Dot Cake Chocolate", "Vaso Dot Cake Vainilla"})
        self.assertEqual(sum(row["producido"] for row in rows), 5)
        self.assertEqual(sum(row["merma_reportada"] for row in rows), 5)
        self.assertEqual(sum(row["vendido"] for row in rows), 0)

    def test_dot_cake_exception_requires_manufactured_recipe_and_original_cup_category(self):
        from recetas.models import Receta
        product = PointProduct.objects.create(external_id="DOT", sku="4358", name="Vaso Dot Cake Chocolate",
                                             category="Vasos Grande")
        recipe = Receta.objects.create(nombre=product.name, codigo_point="4358", hash_contenido="DOT",
                                       tipo=Receta.TIPO_PRODUCTO_FINAL, categoria="Vasos Preparados Grande",
                                       modo_costeo=Receta.MODO_COSTEO_REVENTA)
        self.make_case(product=product, production=2)
        self.assertEqual(self.service().read_audit_report(self.month)["rows"], [])
        recipe.modo_costeo = Receta.MODO_COSTEO_FABRICADO
        recipe.save(update_fields=["modo_costeo"])
        product.category = "Coca-cola"
        product.save(update_fields=["category"])
        self.assertEqual(self.service().read_audit_report(self.month)["rows"], [])

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
        PointProduct.objects.filter(sku="TE").update(category="TE")
        PointProduct.objects.filter(sku="CAJA").update(category="Industrias lec")
        report = self.service().read_audit_report(self.month)
        self.assertEqual(report["rows"], [])
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 5)
        self.assertEqual(ProductInventoryAuditCase.objects.sold_products().count(), 2)

    def test_returns_location_does_not_hide_store_balances(self):
        returns_erp, _ = Sucursal.objects.get_or_create(codigo="DEVOLUCIONES", defaults={"nombre": "Devoluciones"})
        returns = PointBranch.objects.create(external_id="12", name="Devoluciones")
        self.make_case(difference=0, point_closing=10, movement_status="BALANCED")
        self.make_case(branch=returns, movement_status="SOURCE_INCOMPLETE",
                       source_trace={"opening": [], "closing": []})
        report = self.service().read_audit_report(self.month)
        row = report["rows"][0]
        self.assertEqual(row["inventario_inicial"], 10)
        self.assertEqual(row["inventario_final_teorico"], 10)
        self.assertEqual(row["inventario_final_point_total"], 10)
        self.assertEqual(row["diferencia_inventario"], 0)
        self.assertEqual(row["point_coverage"]["inventario_inicial"],
                         {"known": 1, "total": 1, "known_sum": Decimal("10")})
        self.assertEqual(report["audit_status"], "Conciliado")
        self.assertNotIn(returns_erp, report["branches"])
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 2)

    def test_nonproduction_categories_are_excluded_even_when_recipe_category_is_wrong(self):
        from recetas.models import Receta
        categories = ("Accesorios de repostería", "Alegría", "Cake Topper", "Coca-cola",
                      "D-rigaldi", "Granmark", "Industrias lec", "Plásticos", "REGALOS",
                      "TE", "Vela Sparklers", "Velas", "Café", "Otros postres",
                      "Vaso Preparado Mini", "Vasos Mini", "Vasos Grande", "Vasos Preparados Grande")
        for index, category in enumerate(categories):
            code = f"EXCLUDED-{index}"
            product = PointProduct.objects.create(external_id=code, sku=code,
                                                 name=f"Artículo {category}", category=category)
            Receta.objects.create(nombre=product.name, codigo_point=code, hash_contenido=code,
                                  tipo=Receta.TIPO_PRODUCTO_FINAL, categoria="Pastel Chico")
            self.make_case(product=product, production=2, sales=1)
        self.make_case()
        report = self.service().read_audit_report(self.month)
        self.assertEqual([r["product_id"] for r in report["rows"]], [self.product.id])
        self.assertEqual(sum(report["counts"].values()), 1)
        self.assertEqual(ProductInventoryAuditCase.objects.count(), len(categories) + 1)

    def test_recipe_production_scope_and_inactive_rosca(self):
        from recetas.models import Receta
        for index, overrides in enumerate((
            {"modo_costeo": Receta.MODO_COSTEO_REVENTA},
            {"modo_costeo": Receta.MODO_COSTEO_SERVICIO},
        )):
            code = f"NON-PRODUCTION-{index}"
            product = PointProduct.objects.create(external_id=code, sku=code, name=code)
            Receta.objects.create(nombre=code, codigo_point=code, hash_contenido=code,
                                  tipo=Receta.TIPO_PRODUCTO_FINAL, **overrides)
            self.make_case(product=product)
        rosca = PointProduct.objects.create(external_id="ROSCA", sku="ROSCA",
                                            name="Rosca de Dulce de Leche", category="Rosca")
        case = self.make_case(product=rosca)
        self.assertEqual(self.service().read_audit_report(self.month)["rows"], [])
        case.production = 2
        case.save(update_fields=["production"])
        self.assertEqual([r["product_id"] for r in self.service().read_audit_report(self.month)["rows"]],
                         [rosca.id])

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
