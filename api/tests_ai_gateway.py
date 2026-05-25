from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from compras.models import OrdenCompra, SolicitudCompra
from core.access import ROLE_ALMACEN, ROLE_COMPRAS, ROLE_DG, ROLE_LECTURA, ROLE_PRODUCCION
from core.models import AuditLog, Sucursal, UserProfile
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from orquestacion.models import AgentExecutionLink, AgentSuggestion
from pos_bridge.models import (
    PointBranch,
    PointDailyBranchIndicator,
    PointDailySale,
    PointInventorySnapshot,
    PointProduct,
    PointSyncJob,
)
from recetas.models import Receta, RecetaCostoVersion
from reportes.models import FactVentaDiaria


class AIGatewayApiTests(APITestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user_lectura = user_model.objects.create_user(
            username="lectura_ai_gateway",
            email="lectura_ai_gateway@example.com",
            password="test12345",
        )
        lectura_group, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.user_lectura.groups.add(lectura_group)

        self.user_dg = user_model.objects.create_user(
            username="dg_ai_gateway",
            email="dg_ai_gateway@example.com",
            password="test12345",
        )
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user_dg.groups.add(dg_group)

        self.user_branch = user_model.objects.create_user(
            username="branch_ai_gateway",
            email="branch_ai_gateway@example.com",
            password="test12345",
        )
        almacen_group, _ = Group.objects.get_or_create(name=ROLE_ALMACEN)
        self.user_branch.groups.add(almacen_group)

        self.user_compras = user_model.objects.create_user(
            username="compras_ai_gateway",
            email="compras_ai_gateway@example.com",
            password="test12345",
        )
        compras_group, _ = Group.objects.get_or_create(name=ROLE_COMPRAS)
        self.user_compras.groups.add(compras_group)

        self.user_produccion = user_model.objects.create_user(
            username="produccion_ai_gateway",
            email="produccion_ai_gateway@example.com",
            password="test12345",
        )
        produccion_group, _ = Group.objects.get_or_create(name=ROLE_PRODUCCION)
        self.user_produccion.groups.add(produccion_group)

        self.user_plain = user_model.objects.create_user(
            username="plain_ai_gateway",
            email="plain_ai_gateway@example.com",
            password="test12345",
        )

        self.sucursal_1 = Sucursal.objects.create(codigo="SUC1", nombre="Sucursal 1", activa=True)
        self.sucursal_2 = Sucursal.objects.create(codigo="SUC2", nombre="Sucursal 2", activa=True)
        UserProfile.objects.create(
            user=self.user_branch,
            sucursal=self.sucursal_1,
            modo_captura_sucursal=True,
        )

        self.point_branch_1 = PointBranch.objects.create(external_id="1", name="Sucursal 1", erp_branch=self.sucursal_1)
        self.point_branch_2 = PointBranch.objects.create(external_id="2", name="Sucursal 2", erp_branch=self.sucursal_2)
        self.product = PointProduct.objects.create(external_id="PROD-1", sku="PROD-1", name="Pastel Vainilla", category="PASTELES")
        self.proveedor = Proveedor.objects.create(nombre="Proveedor AI", lead_time_dias=2, activo=True)
        self.unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=Decimal("1000"))
        self.insumo = Insumo.objects.create(
            nombre="Harina AI",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.receta = Receta.objects.create(nombre="Receta AI", hash_contenido="hash-receta-ai-001", codigo_point="REC-AI")
        self.cost_version = RecetaCostoVersion.objects.create(
            receta=self.receta,
            version_num=1,
            hash_snapshot="hash-snapshot-ai-001",
            costo_mp=Decimal("100"),
            costo_mo=Decimal("20"),
            costo_indirecto=Decimal("10"),
            costo_total=Decimal("130"),
            fuente="AUTO",
        )
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            started_at=timezone.now(),
        )
        self.solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="Comprador",
            insumo=self.insumo,
            cantidad=Decimal("4"),
            fecha_requerida=timezone.localdate(),
        )
        self.orden = OrdenCompra.objects.create(
            solicitud=self.solicitud,
            proveedor=self.proveedor,
            monto_estimado=Decimal("500"),
        )

        PointDailySale.objects.create(
            branch=self.point_branch_1,
            product=self.product,
            sale_date=timezone.localdate(),
            quantity=Decimal("5"),
            tickets=3,
            total_amount=Decimal("500.00"),
            net_amount=Decimal("500.00"),
            sync_job=self.sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=self.point_branch_1,
            product=self.product,
            stock=Decimal("2"),
            min_stock=Decimal("5"),
            max_stock=Decimal("12"),
            sync_job=self.sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=self.point_branch_2,
            product=self.product,
            stock=Decimal("1"),
            min_stock=Decimal("4"),
            max_stock=Decimal("10"),
            sync_job=self.sync_job,
        )

    def _invoke_url(self, tool_key: str) -> str:
        return reverse("api_ai_gateway_tool_invoke", kwargs={"tool_key": tool_key})

    def _approval_request_url(self, tool_key: str) -> str:
        return reverse("api_ai_gateway_tool_request_approval", kwargs={"tool_key": tool_key})

    def _detail_url(self, tool_key: str) -> str:
        return reverse("api_ai_gateway_tool_detail", kwargs={"tool_key": tool_key})

    def test_tools_list_respects_role(self):
        self.client.force_authenticate(self.user_lectura)
        response = self.client.get(reverse("api_ai_gateway_tools"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        tool_keys = {row["key"] for row in response.data["tools"]}
        self.assertIn("erp.get_dashboard", tool_keys)
        self.assertIn("erp.get_sales_summary", tool_keys)
        self.assertIn("erp.get_inventory_low_stock", tool_keys)
        self.assertIn("erp.get_current_input_cost", tool_keys)
        self.assertIn("erp.get_purchase_requests", tool_keys)
        self.assertIn("erp.get_purchase_orders", tool_keys)
        self.assertIn("erp.get_recipe_cost_history", tool_keys)
        self.assertNotIn("erp.get_audit_logs", tool_keys)
        self.assertNotIn("erp.get_sync_jobs", tool_keys)
        self.assertNotIn("erp.create_purchase_request_draft", tool_keys)
        self.assertNotIn("erp.create_production_plan_draft", tool_keys)
        sales_summary = next(row for row in response.data["tools"] if row["key"] == "erp.get_sales_summary")
        self.assertIn("argument_schema", sales_summary)
        self.assertIn("endpoints", sales_summary)
        self.assertIn("/api/ai-gateway/tools/erp.get_sales_summary/invoke/", sales_summary["endpoints"]["invoke_path"])

    def test_manage_purchase_tool_visible_for_compras_role(self):
        self.client.force_authenticate(self.user_compras)
        response = self.client.get(reverse("api_ai_gateway_tools"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        tool_keys = {row["key"] for row in response.data["tools"]}
        self.assertIn("erp.create_purchase_request_draft", tool_keys)

    def test_manage_purchase_tool_visible_for_dg_role(self):
        self.client.force_authenticate(self.user_dg)
        response = self.client.get(reverse("api_ai_gateway_tools"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        tool_keys = {row["key"] for row in response.data["tools"]}
        self.assertIn("erp.create_purchase_request_draft", tool_keys)

    def test_manage_production_tool_visible_for_produccion_role(self):
        self.client.force_authenticate(self.user_produccion)
        response = self.client.get(reverse("api_ai_gateway_tools"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        tool_keys = {row["key"] for row in response.data["tools"]}
        self.assertIn("erp.create_production_plan_draft", tool_keys)

    def test_manifest_exposes_safe_gateway_contract(self):
        self.client.force_authenticate(self.user_dg)
        response = self.client.get(reverse("api_ai_gateway_manifest"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["gateway"]["version"], "v1")
        self.assertEqual(response.data["gateway"]["name"], "pollyana_erp_ai_gateway")
        self.assertEqual(response.data["gateway"]["display_name"], "Pollyana ERP AI Gateway")
        self.assertEqual(response.data["auth"]["type"], "token")
        self.assertTrue(response.data["approval_workflow"]["required_for_execute_safe_action"])
        tool_keys = {row["key"] for row in response.data["tools"]}
        self.assertIn("erp.trigger_sync_jobs", tool_keys)
        self.assertGreaterEqual(response.data["count"], 1)
        for tool in response.data["tools"]:
            self.assertRegex(tool["name"], r"^[a-zA-Z0-9_-]+$")
            self.assertTrue(tool["display_name"])

    def test_openapi_spec_exposes_importable_actions_contract(self):
        self.client.force_authenticate(self.user_dg)
        response = self.client.get(reverse("api_ai_gateway_openapi"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["openapi"], "3.0.3")
        self.assertIn("TokenAuth", response.data["components"]["securitySchemes"])
        self.assertIn("/api/ai-gateway/tools/erp.get_sales_summary/invoke/", response.data["paths"])
        self.assertIn("/api/ai-gateway/tools/erp.trigger_sync_jobs/request-approval/", response.data["paths"])
        self.assertNotIn("/api/ai-gateway/tools/erp.get_sales_summary/request-approval/", response.data["paths"])
        self.assertEqual(response.data["paths"]["/api/ai-gateway/tools/erp.get_sales_summary/invoke/"]["post"]["operationId"], "erp_get_sales_summary_invoke")

    def test_openapi_spec_can_filter_by_profile(self):
        self.client.force_authenticate(self.user_dg)
        response = self.client.get(reverse("api_ai_gateway_openapi"), {"profile": "compras"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("/api/ai-gateway/tools/erp.get_purchase_requests/invoke/", response.data["paths"])
        self.assertIn("/api/ai-gateway/tools/erp.create_purchase_request_draft/request-approval/", response.data["paths"])
        self.assertNotIn("/api/ai-gateway/tools/erp.get_audit_logs/invoke/", response.data["paths"])
        self.assertNotIn("/api/ai-gateway/tools/erp.create_production_plan_draft/request-approval/", response.data["paths"])
        self.assertNotIn("/api/ai-gateway/approvals/", response.data["paths"])

    def test_openapi_spec_can_filter_by_tool_keys(self):
        self.client.force_authenticate(self.user_dg)
        response = self.client.get(reverse("api_ai_gateway_openapi"), {"tool_keys": "erp.get_audit_logs,erp.get_discrepancies"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("/api/ai-gateway/tools/erp.get_audit_logs/invoke/", response.data["paths"])
        self.assertIn("/api/ai-gateway/tools/erp.get_discrepancies/invoke/", response.data["paths"])
        self.assertNotIn("/api/ai-gateway/tools/erp.get_sales_summary/invoke/", response.data["paths"])

    def test_openapi_spec_rejects_unknown_profile(self):
        self.client.force_authenticate(self.user_dg)
        response = self.client.get(reverse("api_ai_gateway_openapi"), {"profile": "desconocido"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    @override_settings(AI_GATEWAY_OPENAPI_SERVER_URL="http://host.docker.internal:8011")
    def test_openapi_spec_uses_configured_server_url_when_present(self):
        self.client.force_authenticate(self.user_dg)
        response = self.client.get(reverse("api_ai_gateway_openapi"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["servers"], [{"url": "http://host.docker.internal:8011"}])

    def test_tool_detail_exposes_schema_and_paths(self):
        self.client.force_authenticate(self.user_lectura)
        response = self.client.get(self._detail_url("erp.get_recipe_cost_history"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["key"], "erp.get_recipe_cost_history")
        self.assertEqual(response.data["name"], "erp_get_recipe_cost_history")
        self.assertEqual(response.data["display_name"], "Costo historico de receta")
        self.assertEqual(response.data["argument_schema"]["required"], ["receta_id"])
        self.assertIn("/api/ai-gateway/tools/erp.get_recipe_cost_history/", response.data["endpoints"]["detail_path"])

    def test_invoke_current_input_cost_resolves_fresa_fresca_as_insumo(self):
        fresa = Insumo.objects.create(
            nombre="Fresa Fresca",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        mermelada = Insumo.objects.create(
            nombre="Mermelada Fresa",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        CostoInsumo.objects.create(
            insumo=fresa,
            proveedor=self.proveedor,
            fecha=timezone.localdate(),
            costo_unitario=Decimal("78.500000"),
            source_hash="ai-gateway-fresa-fresca-cost",
        )
        CostoInsumo.objects.create(
            insumo=mermelada,
            proveedor=self.proveedor,
            fecha=timezone.localdate(),
            costo_unitario=Decimal("42.000000"),
            source_hash="ai-gateway-mermelada-fresa-cost",
        )

        self.client.force_authenticate(self.user_lectura)
        response = self.client.post(
            self._invoke_url("erp.get_current_input_cost"),
            {"arguments": {"q": "fresa fresca"}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.data["result"]["payload"]
        self.assertEqual(response.data["result"]["sources"], ["maestros.CostoInsumo", "maestros.Insumo"])
        self.assertEqual(payload["insumo"], "Fresa Fresca")
        self.assertEqual(payload["unidad_base"], "kg")
        self.assertEqual(payload["costo_unitario"], 78.5)
        self.assertEqual(payload["costo_source_insumo"], "Fresa Fresca")

    def test_ticket_amount_threshold_reports_exact_count_unavailable_from_aggregates(self):
        PointDailyBranchIndicator.objects.create(
            branch=self.point_branch_1,
            indicator_date=timezone.datetime(2026, 4, 1).date(),
            total_amount=Decimal("1200.00"),
            total_tickets=3,
            total_avg_ticket=Decimal("400.00"),
        )
        PointDailyBranchIndicator.objects.create(
            branch=self.point_branch_2,
            indicator_date=timezone.datetime(2026, 4, 1).date(),
            total_amount=Decimal("800.00"),
            total_tickets=2,
            total_avg_ticket=Decimal("400.00"),
        )

        self.client.force_authenticate(self.user_lectura)
        response = self.client.post(
            self._invoke_url("erp.get_ticket_amount_threshold"),
            {"arguments": {"start_date": "2026-04-01", "end_date": "2026-04-30", "threshold_amount": 500}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result = response.data["result"]
        payload = result["payload"]
        self.assertEqual(result["status"], "not_available_exact")
        self.assertFalse(payload["exact_count_available"])
        self.assertIsNone(payload["exact_count"])
        self.assertTrue(payload["do_not_infer_zero"])
        self.assertEqual(payload["total_sales"], 2000.0)
        self.assertEqual(payload["total_tickets"], 5)
        self.assertEqual(payload["avg_ticket"], 400.0)
        self.assertEqual(payload["upper_bound_if_all_qualifying_tickets_were_at_least_threshold"], 4)

    def test_invoke_promotion_profitability_returns_financial_decision_table(self):
        today = timezone.localdate()
        products = [
            ("FCM", "Fresas con crema mediana", Decimal("120.00"), Decimal("10"), Decimal("1200.00"), Decimal("450.00")),
            ("FCG", "Fresas con crema grande", Decimal("180.00"), Decimal("6"), Decimal("1080.00"), Decimal("510.00")),
            ("REB", "Rebanada de pastel fresa", Decimal("65.00"), Decimal("18"), Decimal("1170.00"), Decimal("540.00")),
        ]
        for sku, name, price, quantity, sales, cost in products:
            product = PointProduct.objects.create(
                external_id=f"{sku}-PROMO",
                sku=sku,
                name=name,
                category="PROMO",
                precio=price,
            )
            receta = Receta.objects.create(
                nombre=name,
                hash_contenido=f"hash-{sku.lower()}-promo",
                codigo_point=sku,
                tipo=Receta.TIPO_PRODUCTO_FINAL,
            )
            FactVentaDiaria.objects.create(
                fecha=today,
                sucursal=self.sucursal_1,
                receta=receta,
                point_product=product,
                producto_clave=sku,
                producto_nombre=name,
                categoria="PROMO",
                cantidad=quantity,
                tickets=int(quantity),
                venta_bruta=sales,
                descuento=Decimal("0.00"),
                venta_total=sales,
                venta_neta=sales,
                costo_estimado=cost,
                margen=sales - cost,
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )

        self.client.force_authenticate(self.user_dg)
        response = self.client.post(
            self._invoke_url("erp.analyze_promotion_profitability"),
            {
                "arguments": {
                    "promotion_type": "3x2",
                    "event_name": "Día del Estudiante",
                    "product_queries": [
                        "fresas con crema mediana",
                        "fresas con crema grande",
                        "rebanada de pastel",
                    ],
                    "expected_uplift_pct": 60,
                    "marketing_budget": 300,
                    "lookback_days": 30,
                }
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result = response.data["result"]
        self.assertEqual(result["sources"], ["pos_bridge.PointProduct", "reportes.FactVentaDiaria", "recetas.Receta"])
        payload = result["payload"]
        self.assertEqual(payload["promotion_type"], "3x2")
        self.assertEqual(payload["event_name"], "Día del Estudiante")
        self.assertEqual(payload["expected_uplift_pct"], 60.0)
        self.assertEqual(payload["returned"], 3)
        self.assertIn("finance", payload)
        self.assertIn("marketing", payload)
        self.assertIn("operations", payload)
        self.assertIn("accounting", payload)

        mediana = next(row for row in payload["items"] if row["product_sku"] == "FCM")
        self.assertEqual(mediana["normal_unit_price"], 120.0)
        self.assertEqual(mediana["unit_cost"], 45.0)
        self.assertEqual(mediana["promo_effective_unit_price"], 80.0)
        self.assertEqual(mediana["normal_margin_per_unit"], 75.0)
        self.assertEqual(mediana["promo_margin_per_unit"], 35.0)
        self.assertEqual(mediana["baseline_units"], 10.0)
        self.assertEqual(mediana["expected_units"], 16.0)
        self.assertEqual(mediana["baseline_profit"], 750.0)
        self.assertEqual(mediana["promo_profit"], 560.0)
        self.assertEqual(mediana["profit_delta"], -190.0)
        self.assertEqual(mediana["break_even_units"], 22)
        self.assertEqual(mediana["recommendation"], "NO_CONVIENE")
        self.assertGreaterEqual(len(payload["decision_summary"]["ranked_products"]), 3)

    def test_invoke_promotion_profitability_resolves_vasos_and_rebanada_mix(self):
        today = timezone.localdate()

        fixtures = [
            ("PFCM", "Pastel de Fresas Con Crema Mediano", "Pastel Mediano", Decimal("490.00"), Decimal("4"), Decimal("1960.00"), Decimal("600.00")),
            ("VFM", "Vaso Fresas con Crema Mediano", "Vasos Grande", None, Decimal("12"), Decimal("1200.00"), Decimal("420.00")),
            ("VFG", "Vaso Fresas con Crema Grande", "Vasos Grande", None, Decimal("8"), Decimal("1040.00"), Decimal("360.00")),
            ("REB1", "Pastel de 3 Leches Rebanada", "Rebanada", Decimal("70.00"), Decimal("10"), Decimal("700.00"), Decimal("220.00")),
            ("REB2", "Pastel de Snickers Rebanada", "Rebanada", Decimal("70.00"), Decimal("20"), Decimal("1400.00"), Decimal("500.00")),
            ("SAB1", "Sabor Fresa Rebanada Pay", "Rebanada", None, Decimal("100"), Decimal("0.00"), Decimal("100.00")),
        ]
        for index in range(60):
            PointProduct.objects.create(
                external_id=f"FILLER-{index}",
                sku=f"FILLER-{index}",
                name=f"Pastel Fresas con Crema Relleno {index:02d}",
                category="Pastel Mediano",
                precio=Decimal("490.00"),
            )
        for sku, name, category, price, quantity, sales, cost in fixtures:
            product = PointProduct.objects.create(
                external_id=f"{sku}-PROMO2",
                sku=sku,
                name=name,
                category=category,
                precio=price,
            )
            if sku in {"VFM", "VFG"}:
                receta = Receta.objects.create(
                    nombre=name,
                    hash_contenido=f"hash-{sku.lower()}-recipe-promo2",
                    codigo_point=sku,
                    tipo=Receta.TIPO_PRODUCTO_FINAL,
                )
                RecetaCostoVersion.objects.create(
                    receta=receta,
                    version_num=1,
                    hash_snapshot=f"hash-{sku.lower()}-recipe-cost-promo2",
                    costo_mp=Decimal("0.00"),
                    costo_mo=Decimal("0.00"),
                    costo_indirecto=Decimal("0.00"),
                    costo_total=Decimal("32.00") if sku == "VFM" else Decimal("36.00"),
                )
            FactVentaDiaria.objects.create(
                fecha=today,
                sucursal=self.sucursal_1,
                point_product=product,
                producto_clave=sku,
                producto_nombre=name,
                categoria=category,
                cantidad=quantity,
                tickets=int(quantity),
                venta_bruta=sales,
                descuento=Decimal("0.00"),
                venta_total=sales,
                venta_neta=sales,
                costo_estimado=cost,
                margen=sales - cost,
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )

        self.client.force_authenticate(self.user_dg)
        response = self.client.post(
            self._invoke_url("erp.analyze_promotion_profitability"),
            {
                "arguments": {
                    "promotion_type": "3x2",
                    "product_queries": [
                        "vaso fresas con crema mediana",
                        "vaso fresas con crema grnde",
                        "revoltura de rebanadas de pastel",
                    ],
                    "expected_uplift_pct": 60,
                    "lookback_days": 30,
                }
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        items = response.data["result"]["payload"]["items"]
        mediano = next(row for row in items if row["query"] == "vaso fresas con crema mediana")
        grande = next(row for row in items if row["query"] == "vaso fresas con crema grnde")
        mix = next(row for row in items if row["query"] == "revoltura de rebanadas de pastel")

        self.assertEqual(mediano["product_sku"], "VFM")
        self.assertEqual(mediano["product_name"], "Vaso Fresas con Crema Mediano")
        self.assertEqual(mediano["normal_unit_price"], 100.0)
        self.assertEqual(mediano["receta"], "Vaso Fresas con Crema Mediano")
        self.assertEqual(mediano["unit_cost"], 32.0)
        self.assertEqual(mediano["cost_source"], "receta_costo_vigente")
        self.assertEqual(mediano["observed_unit_cost"], 35.0)
        self.assertEqual(grande["product_sku"], "VFG")
        self.assertEqual(grande["product_name"], "Vaso Fresas con Crema Grande")
        self.assertEqual(grande["normal_unit_price"], 130.0)
        self.assertEqual(grande["receta"], "Vaso Fresas con Crema Grande")
        self.assertEqual(grande["unit_cost"], 36.0)
        self.assertEqual(grande["cost_source"], "receta_costo_vigente")
        self.assertEqual(grande["observed_unit_cost"], 45.0)
        self.assertEqual(mix["item_type"], "product_group")
        self.assertEqual(mix["product_sku"], "GRUPO_REBANADAS")
        self.assertEqual(mix["product_count"], 2)
        self.assertEqual(mix["baseline_units"], 30.0)
        self.assertEqual(mix["normal_unit_price"], 70.0)
        self.assertEqual(mix["unit_cost"], 24.0)

    def test_invoke_sales_summary_logs_audit(self):
        self.client.force_authenticate(self.user_lectura)
        response = self.client.post(
            self._invoke_url("erp.get_sales_summary"),
            {"arguments": {"branch": "SUC1"}},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["result"]["status"], "ok")
        self.assertEqual(response.data["result"]["payload"]["total_tickets"], 3)
        self.assertEqual(response.data["result"]["payload"]["branches_count"], 1)
        audit = AuditLog.objects.filter(action="AI_GATEWAY_TOOL_INVOKE", object_id="erp.get_sales_summary").first()
        self.assertIsNotNone(audit)
        self.assertEqual(audit.user_id, self.user_lectura.id)

    def test_invoke_sales_summary_prefers_canonical_fact_over_staging_sale(self):
        today = timezone.localdate()
        FactVentaDiaria.objects.create(
            fecha=today,
            sucursal=self.sucursal_1,
            receta=self.receta,
            point_product=self.product,
            producto_clave="REC-AI",
            producto_nombre=self.receta.nombre,
            categoria="PASTELES",
            cantidad=Decimal("7"),
            tickets=4,
            venta_bruta=Decimal("700.00"),
            descuento=Decimal("0"),
            venta_total=Decimal("700.00"),
            venta_neta=Decimal("700.00"),
            costo_estimado=Decimal("0"),
            margen=Decimal("0"),
            source_kind=FactVentaDiaria.SOURCE_V2,
        )

        self.client.force_authenticate(self.user_lectura)
        response = self.client.post(
            self._invoke_url("erp.get_sales_summary"),
            {"arguments": {"branch": "SUC1", "start_date": today.isoformat(), "end_date": today.isoformat()}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["result"]["payload"]["source"], "PointSalesDailyFact")
        self.assertEqual(response.data["result"]["payload"]["source_status"], "OFFICIAL")
        self.assertEqual(response.data["result"]["payload"]["total_sales"], 700.0)

    def test_invoke_sales_by_branch_prefers_canonical_fact_over_staging_sale(self):
        today = timezone.localdate()
        FactVentaDiaria.objects.create(
            fecha=today,
            sucursal=self.sucursal_1,
            receta=self.receta,
            point_product=self.product,
            producto_clave="REC-AI",
            producto_nombre=self.receta.nombre,
            categoria="PASTELES",
            cantidad=Decimal("8"),
            tickets=5,
            venta_bruta=Decimal("800.00"),
            descuento=Decimal("0"),
            venta_total=Decimal("800.00"),
            venta_neta=Decimal("800.00"),
            costo_estimado=Decimal("0"),
            margen=Decimal("0"),
            source_kind=FactVentaDiaria.SOURCE_V2,
        )

        self.client.force_authenticate(self.user_lectura)
        response = self.client.post(
            self._invoke_url("erp.get_sales_by_branch"),
            {"arguments": {"start_date": today.isoformat(), "end_date": today.isoformat()}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.data["result"]["payload"]
        self.assertEqual(payload["source"], "PointSalesDailyFact")
        self.assertEqual(payload["source_status"], "OFFICIAL")
        self.assertEqual(payload["items"][0]["branch_external_id"], "SUC1")
        self.assertEqual(payload["items"][0]["total_sales"], 800.0)

    def test_audit_logs_tool_requires_dg_or_admin(self):
        self.client.force_authenticate(self.user_lectura)
        forbidden = self.client.post(self._invoke_url("erp.get_audit_logs"), {"arguments": {}}, format="json")
        self.assertEqual(forbidden.status_code, status.HTTP_403_FORBIDDEN)

        self.client.force_authenticate(self.user_dg)
        allowed = self.client.post(self._invoke_url("erp.get_audit_logs"), {"arguments": {"limit": 10}}, format="json")
        self.assertEqual(allowed.status_code, status.HTTP_200_OK)
        self.assertEqual(allowed.data["result"]["status"], "ok")

    def test_branch_capture_scope_blocks_other_branch_and_defaults_own_branch(self):
        self.client.force_authenticate(self.user_branch)

        forbidden = self.client.post(
            self._invoke_url("erp.get_inventory_low_stock"),
            {"arguments": {"branch": "SUC2"}},
            format="json",
        )
        self.assertEqual(forbidden.status_code, status.HTTP_403_FORBIDDEN)

        allowed = self.client.post(
            self._invoke_url("erp.get_inventory_low_stock"),
            {"arguments": {}},
            format="json",
        )
        self.assertEqual(allowed.status_code, status.HTTP_200_OK)
        items = allowed.data["result"]["payload"]["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["erp_branch_code"], "SUC1")

    def test_safe_action_requires_approval_then_can_execute(self):
        self.client.force_authenticate(self.user_dg)
        request_response = self.client.post(
            self._approval_request_url("erp.trigger_sync_jobs"),
            {
                "arguments": {"job_type": "inventory", "branch_filter": "SUC1"},
                "summary": "Refresh inventario SUC1",
                "rationale": "Sincronizacion manual controlada para piloto.",
            },
            format="json",
        )
        self.assertEqual(request_response.status_code, status.HTTP_201_CREATED)
        suggestion_id = request_response.data["approval"]["suggestion_id"]
        suggestion = AgentSuggestion.objects.get(id=suggestion_id)
        self.assertEqual(suggestion.decision_status, AgentSuggestion.DECISION_PENDING)
        self.assertEqual(suggestion.details_json["tool_key"], "erp.trigger_sync_jobs")

        approvals_response = self.client.get(reverse("api_ai_gateway_approvals"))
        self.assertEqual(approvals_response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(approvals_response.data["count"], 1)

        approve_response = self.client.post(
            reverse("api_ai_gateway_approval_decision", kwargs={"suggestion_id": suggestion_id, "decision": "approve"}),
            {"comment": "Aprobado para piloto"},
            format="json",
        )
        self.assertEqual(approve_response.status_code, status.HTTP_200_OK)
        suggestion.refresh_from_db()
        self.assertEqual(suggestion.decision_status, AgentSuggestion.DECISION_APPROVED)

        mocked_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            started_at=timezone.now(),
            triggered_by=self.user_dg,
        )
        with patch("api.ai_gateway_services.run_inventory_sync", return_value=mocked_job):
            execute_response = self.client.post(
                reverse("api_ai_gateway_approval_execute", kwargs={"suggestion_id": suggestion_id}),
                {},
                format="json",
            )
        self.assertEqual(execute_response.status_code, status.HTTP_200_OK)
        suggestion.refresh_from_db()
        self.assertEqual(suggestion.decision_status, AgentSuggestion.DECISION_EXECUTED)
        execution = AgentExecutionLink.objects.get(suggestion=suggestion)
        self.assertEqual(execution.execution_status, AgentExecutionLink.STATUS_SUCCESS)
        self.assertEqual(execute_response.data["result"]["status"], "ok")

    def test_purchase_request_draft_safe_action_requires_approval_then_creates_borrador(self):
        self.client.force_authenticate(self.user_compras)
        request_response = self.client.post(
            self._approval_request_url("erp.create_purchase_request_draft"),
            {
                "arguments": {
                    "area": "Compras",
                    "insumo_id": self.insumo.id,
                    "cantidad": "7.500",
                    "fecha_requerida": timezone.localdate().isoformat(),
                },
                "summary": "Crear borrador de compra para Harina AI",
                "rationale": "Reposicion preventiva controlada para piloto.",
            },
            format="json",
        )
        self.assertEqual(request_response.status_code, status.HTTP_201_CREATED)
        suggestion_id = request_response.data["approval"]["suggestion_id"]

        self.client.force_authenticate(self.user_dg)
        approve_response = self.client.post(
            reverse("api_ai_gateway_approval_decision", kwargs={"suggestion_id": suggestion_id, "decision": "approve"}),
            {"comment": "Aprobado para crear borrador controlado"},
            format="json",
        )
        self.assertEqual(approve_response.status_code, status.HTTP_200_OK)

        before_count = SolicitudCompra.objects.count()
        execute_response = self.client.post(
            reverse("api_ai_gateway_approval_execute", kwargs={"suggestion_id": suggestion_id}),
            {},
            format="json",
        )
        self.assertEqual(execute_response.status_code, status.HTTP_200_OK)
        self.assertEqual(execute_response.data["result"]["status"], "ok")
        self.assertEqual(SolicitudCompra.objects.count(), before_count + 1)

        solicitud = SolicitudCompra.objects.order_by("-id").first()
        self.assertIsNotNone(solicitud)
        self.assertEqual(solicitud.estatus, SolicitudCompra.STATUS_BORRADOR)
        self.assertEqual(solicitud.insumo_id, self.insumo.id)
        self.assertEqual(solicitud.solicitante, self.user_compras.username)

    def test_production_plan_draft_safe_action_requires_approval_then_creates_borrador(self):
        self.client.force_authenticate(self.user_produccion)
        request_response = self.client.post(
            self._approval_request_url("erp.create_production_plan_draft"),
            {
                "arguments": {
                    "nombre": "Plan piloto AI",
                    "fecha_produccion": timezone.localdate().isoformat(),
                    "notas": "Borrador generado por flujo seguro",
                    "items": [
                        {
                            "receta_id": self.receta.id,
                            "cantidad": "3.000",
                            "notas": "Linea inicial",
                        }
                    ],
                },
                "summary": "Crear borrador de plan de produccion",
                "rationale": "Preparacion controlada para piloto.",
            },
            format="json",
        )
        self.assertEqual(request_response.status_code, status.HTTP_201_CREATED)
        suggestion_id = request_response.data["approval"]["suggestion_id"]

        self.client.force_authenticate(self.user_dg)
        approve_response = self.client.post(
            reverse("api_ai_gateway_approval_decision", kwargs={"suggestion_id": suggestion_id, "decision": "approve"}),
            {"comment": "Aprobado para crear plan borrador"},
            format="json",
        )
        self.assertEqual(approve_response.status_code, status.HTTP_200_OK)

        from recetas.models import PlanProduccion

        before_count = PlanProduccion.objects.count()
        execute_response = self.client.post(
            reverse("api_ai_gateway_approval_execute", kwargs={"suggestion_id": suggestion_id}),
            {},
            format="json",
        )
        self.assertEqual(execute_response.status_code, status.HTTP_200_OK)
        self.assertEqual(execute_response.data["result"]["status"], "ok")
        self.assertEqual(PlanProduccion.objects.count(), before_count + 1)

        plan = PlanProduccion.objects.order_by("-id").first()
        self.assertIsNotNone(plan)
        self.assertEqual(plan.estado, PlanProduccion.ESTADO_BORRADOR)
        self.assertEqual(plan.creado_por_id, self.user_dg.id)
        self.assertEqual(plan.items.count(), 1)

    def test_non_dg_cannot_list_or_decide_approvals(self):
        self.client.force_authenticate(self.user_lectura)
        list_response = self.client.get(reverse("api_ai_gateway_approvals"))
        self.assertEqual(list_response.status_code, status.HTTP_403_FORBIDDEN)

    def test_purchase_requests_tool_requires_purchase_access(self):
        self.client.force_authenticate(self.user_plain)
        forbidden = self.client.post(self._invoke_url("erp.get_purchase_requests"), {"arguments": {}}, format="json")
        self.assertEqual(forbidden.status_code, status.HTTP_403_FORBIDDEN)

        self.client.force_authenticate(self.user_lectura)
        allowed = self.client.post(self._invoke_url("erp.get_purchase_requests"), {"arguments": {"limit": 10}}, format="json")
        self.assertEqual(allowed.status_code, status.HTTP_200_OK)
        self.assertEqual(allowed.data["result"]["status"], "ok")
        self.assertGreaterEqual(allowed.data["result"]["payload"]["returned"], 1)

    def test_purchase_orders_tool_returns_orders_for_allowed_roles(self):
        self.client.force_authenticate(self.user_lectura)
        response = self.client.post(
            self._invoke_url("erp.get_purchase_orders"),
            {"arguments": {"limit": 10}},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["result"]["status"], "ok")
        self.assertGreaterEqual(response.data["result"]["payload"]["returned"], 1)
        self.assertEqual(response.data["result"]["payload"]["items"][0]["proveedor"], "Proveedor AI")

    def test_recipe_cost_history_tool_returns_versions(self):
        self.client.force_authenticate(self.user_lectura)
        response = self.client.post(
            self._invoke_url("erp.get_recipe_cost_history"),
            {"arguments": {"receta_id": self.receta.id, "limit": 5}},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["result"]["status"], "ok")
        self.assertEqual(response.data["result"]["payload"]["receta"], "Receta AI")
        self.assertEqual(response.data["result"]["payload"]["items"][0]["costo_total"], 130.0)
