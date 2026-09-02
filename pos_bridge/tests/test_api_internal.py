from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model
from django.test import override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointDailySale, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.product_closure_projection import project_product_closure_line
from recetas.models import LineaReceta, ProductoMonthClosure, ProductoMonthClosureLine, Receta


class PosBridgeInternalApiTests(APITestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="pos_api_user",
            email="pos_api_user@example.com",
            password="test12345",
            is_staff=True,
        )
        self.client.force_authenticate(self.user)

        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.branch = PointBranch.objects.create(
            external_id="1",
            name="MATRIZ",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.sucursal,
        )
        self.product = PointProduct.objects.create(
            external_id="100",
            sku="0100",
            name="Pastel de Fresas Con Crema Mediano",
            category="PASTEL MEDIANO",
        )
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            triggered_by=self.user,
        )
        PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=self.product,
            stock=Decimal("3"),
            min_stock=Decimal("1"),
            max_stock=Decimal("8"),
            captured_at=timezone.now() - timedelta(hours=2),
            sync_job=self.sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=self.product,
            stock=Decimal("10"),
            min_stock=Decimal("1"),
            max_stock=Decimal("8"),
            captured_at=timezone.now(),
            sync_job=self.sync_job,
        )

        self.sales_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_SUCCESS,
            triggered_by=self.user,
        )
        self.receta = Receta.objects.create(
            nombre="Pastel Fresas Con Crema - Mediano",
            codigo_point="0100",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        unidad = UnidadMedida.objects.create(
            codigo="pza",
            nombre="Pieza",
            tipo=UnidadMedida.TIPO_PIEZA,
            factor_to_base=Decimal("1"),
        )
        insumo = Insumo.objects.create(nombre="Caja pastel mediano", unidad_base=unidad, activo=True)
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=insumo,
            insumo_texto="Caja pastel mediano",
            cantidad=Decimal("1"),
            unidad=unidad,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("12"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.product,
            receta=self.receta,
            sync_job=self.sales_job,
            sale_date=timezone.localdate(),
            quantity=Decimal("4"),
            tickets=2,
            gross_amount=Decimal("600"),
            discount_amount=Decimal("50"),
            total_amount=Decimal("550"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("550"),
        )
        self.product_closure = ProductoMonthClosure.objects.create(
            month_start=timezone.localdate().replace(day=1),
            month_end=timezone.localdate(),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            opening_reference_date=timezone.localdate() - timedelta(days=1),
            built_at=timezone.now(),
            is_locked=False,
        )
        ProductoMonthClosureLine.objects.create(
            closure=self.product_closure,
            receta_padre=self.receta,
            inventario_inicial_teorico=Decimal("12"),
            produccion_mes=Decimal("8"),
            venta_directa_enteros=Decimal("4"),
            venta_total_equivalente=Decimal("4"),
            merma_total_equivalente=Decimal("1"),
            inventario_final_teorico=Decimal("15"),
        )

    def test_inventory_current_uses_latest_snapshot(self):
        response = self.client.get("/api/pos-bridge/inventory/current/")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        row = response.data["results"][0]
        self.assertEqual(row["product_sku"], "0100")
        self.assertEqual(row["total_stock"], "10.000")

    def test_sales_summary_returns_aggregates(self):
        response = self.client.get("/api/pos-bridge/sales/summary/")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_sales"], "550.00")
        self.assertEqual(response.data["branches_count"], 1)
        self.assertEqual(response.data["products_count"], 1)

    def test_product_recipe_returns_bom(self):
        response = self.client.get(f"/api/pos-bridge/products/{self.product.id}/recipe/")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["receta_id"], self.receta.id)
        self.assertEqual(len(response.data["bom"]), 1)
        self.assertEqual(response.data["bom"][0]["insumo"], "Caja pastel mediano")

    @override_settings(
        PICKUP_AVAILABILITY_FRESHNESS_MINUTES=20,
        PICKUP_STOCK_BUFFER_DEFAULT="1",
        PICKUP_LOW_STOCK_THRESHOLD="2",
    )
    def test_inventory_availability_exposes_latest_stock(self):
        response = self.client.get("/api/pos-bridge/inventory/availability/?sku=0100")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        row = response.data["results"][0]
        self.assertEqual(row["sku"], "0100")
        self.assertEqual(row["total_stock"], "10.000")
        self.assertTrue(row["available"])

    def test_sync_job_trigger_inventory_returns_job_payload(self):
        fake_job = SimpleNamespace(
            id=999,
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            started_at=timezone.now(),
            finished_at=timezone.now(),
            error_message="",
            parameters={},
            result_summary={"branches_processed": 1},
            artifacts={},
            attempt_count=1,
            triggered_by=self.user,
            created_at=timezone.now(),
        )
        with patch("pos_bridge.api.views.sync_jobs.run_inventory_sync", return_value=fake_job) as run_mock:
            response = self.client.post("/api/pos-bridge/sync-jobs/trigger/", {"job_type": "inventory"}, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["id"], 999)
        run_mock.assert_called_once()

    def test_agent_query_returns_recipe_summary(self):
        response = self.client.post(
            "/api/pos-bridge/agent/query/",
            {"query": "Dame la receta de Pastel de Fresas Con Crema Mediano"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["query_type"], "recipe")
        self.assertEqual(response.data["data"]["receta_id"], self.receta.id)

    def test_product_closures_list_returns_month_summary(self):
        response = self.client.get("/api/pos-bridge/product-closures/")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        row = response.data["results"][0]
        self.assertEqual(row["line_count"], 1)
        self.assertEqual(row["total_opening_inventory"], "12.000000")
        self.assertEqual(row["total_ending_inventory"], "15.000000")

    def test_product_closures_detail_returns_lines(self):
        response = self.client.get(f"/api/pos-bridge/product-closures/{self.product_closure.id}/")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.product_closure.id)
        self.assertEqual(len(response.data["lines"]), 1)
        self.assertEqual(response.data["lines"][0]["receta_padre"], self.receta.id)

    def test_product_closure_detail_distinguishes_canonical_zero_from_missing_placeholder(self):
        line = self.product_closure.lines.get()
        line.inventario_inicial_teorico = Decimal("0")
        line.produccion_mes = Decimal("0")
        line.venta_directa_enteros = Decimal("0")
        line.venta_derivada_equivalente = Decimal("0")
        line.venta_total_equivalente = Decimal("0")
        line.merma_total_equivalente = Decimal("0")
        line.inventario_final_teorico = Decimal("0")
        line.inventario_final_point_total = Decimal("0")
        line.metadata = {
            "balance_contract": "POINT_PRODUCT_BALANCE_V1",
            "issues": [],
            "sales_source_available": True,
            "opening_source_authoritative": True,
            "sales_source_authoritative": True,
            "production_source_authoritative": True,
            "waste_source_authoritative": True,
            "conversion_source_authoritative": True,
            "closing_source_authoritative": True,
            "point_final_scopes_available": True,
            "point_conversion_in": "0",
            "point_conversion_out": "0",
            "point_difference": "0",
            "point_status": "COINCIDE",
            "conversion_origins": ["POINT"],
            "projection_sources": ["DIRECTA"],
        }
        line.save()

        response = self.client.get(f"/api/pos-bridge/product-closures/{self.product_closure.id}/")

        row = response.data["lines"][0]
        self.assertEqual(row["opening_point"], "0.000000")
        self.assertEqual(row["sales_total"], "0.000000")
        self.assertEqual(row["point_conversion_in"], "0.000000")
        self.assertEqual(row["closing_point"], "0.000000")
        self.assertEqual(row["point_difference"], "0.000000")
        self.assertEqual(row["point_status"], "COINCIDE")
        self.assertEqual(row["conversion_origins"], ["POINT"])
        self.assertEqual(row["projection_sources"], ["DIRECTA"])

        line.metadata = {
            **line.metadata,
            "issues": [
                "OPENING_SNAPSHOT_MISSING",
                "SALES_SOURCE_MISSING",
                "CALCULATED_CLOSING_MISSING",
                "CLOSING_SNAPSHOT_MISSING",
            ],
            "sales_source_available": False,
        }
        line.save(update_fields=["metadata", "updated_at"])
        missing = self.client.get(f"/api/pos-bridge/product-closures/{self.product_closure.id}/").data["lines"][0]
        self.assertIsNone(missing["opening_point"])
        self.assertIsNone(missing["sales_total"])
        self.assertIsNone(missing["calculated_closing"])
        self.assertIsNone(missing["closing_point"])
        self.assertIsNone(missing["point_difference"])
        self.assertEqual(missing["point_status"], "REVISAR_FUENTE")

    def test_product_closure_list_totals_are_none_when_any_canonical_line_is_missing(self):
        line = self.product_closure.lines.get()
        line.metadata = {
            "balance_contract": "POINT_PRODUCT_BALANCE_V1",
            "issues": [],
            "sales_source_available": True,
            "opening_source_authoritative": True,
            "sales_source_authoritative": True,
            "production_source_authoritative": True,
            "waste_source_authoritative": True,
            "conversion_source_authoritative": True,
            "closing_source_authoritative": True,
            "point_final_scopes_available": True,
            "point_difference": "0",
        }
        line.save(update_fields=["metadata", "updated_at"])
        missing_recipe = Receta.objects.create(
            nombre="Pastel prueba faltante",
            codigo_point="MISSING-1",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        ProductoMonthClosureLine.objects.create(
            closure=self.product_closure,
            receta_padre=missing_recipe,
            produccion_mes=Decimal("0"),
            metadata={
                "balance_contract": "POINT_PRODUCT_BALANCE_V1",
                "issues": ["PRODUCTION_SOURCE_MISSING", "CALCULATED_CLOSING_MISSING"],
                "sales_source_available": True,
                "opening_source_authoritative": True,
                "sales_source_authoritative": True,
                "production_source_authoritative": False,
                "waste_source_authoritative": True,
                "conversion_source_authoritative": True,
                "closing_source_authoritative": True,
            },
        )

        response = self.client.get("/api/pos-bridge/product-closures/")

        summary = response.data["results"][0]
        self.assertIsNone(summary["total_production"])
        self.assertIsNone(summary["total_ending_inventory"])
        self.assertEqual(summary["total_sales"], "4.000000")

    def test_product_closure_api_preserves_historical_inventory_semantics(self):
        line = self.product_closure.lines.get()
        line.inventario_final_teorico = Decimal("21")
        line.inventario_final_point_total = Decimal("18")
        # The real historical importer persists theoretical - physical count.
        line.diferencia_teorico_vs_point = (
            line.inventario_final_teorico - line.inventario_final_point_total
        )
        line.estado_auditoria = ProductoMonthClosureLine.AUDIT_STATUS_SOBRANTE_FISICO
        line.metadata = {"historical_excel": True}
        line.save()
        self.product_closure.metadata = {"historical_excel_import": {"source_file": "historico.xlsx"}}
        self.product_closure.save(update_fields=["metadata", "updated_at"])

        response = self.client.get(f"/api/pos-bridge/product-closures/{self.product_closure.id}/")

        row = response.data["lines"][0]
        self.assertTrue(row["is_historical_inventory"])
        self.assertIsNone(row["closing_point"])
        self.assertIsNone(row["point_difference"])
        self.assertEqual(row["historical_count"], "18.000000")
        self.assertEqual(row["historical_difference"], "3.000000")
        self.assertEqual(row["point_status"], ProductoMonthClosureLine.AUDIT_STATUS_SOBRANTE_FISICO)
        self.assertIsNone(response.data["total_closing_point"])
        self.assertIsNone(response.data["total_point_difference"])
        self.assertEqual(response.data["total_historical_count"], "18.000000")
        self.assertEqual(response.data["total_historical_difference"], "3.000000")

    def test_product_closure_present_but_non_authoritative_sources_are_null_not_zero(self):
        line = self.product_closure.lines.get()
        line.inventario_inicial_teorico = Decimal("12")
        line.produccion_mes = Decimal("8")
        line.venta_total_equivalente = Decimal("4")
        line.merma_total_equivalente = Decimal("1")
        line.inventario_final_teorico = Decimal("15")
        line.inventario_final_point_total = Decimal("15")
        line.metadata = {
            "balance_contract": "POINT_PRODUCT_BALANCE_V1",
            "issues": ["MONTH_SOURCE_INCOMPLETE"],
            "sales_source_available": True,
            "opening_source_authoritative": False,
            "sales_source_authoritative": False,
            "production_source_authoritative": False,
            "waste_source_authoritative": False,
            "conversion_source_authoritative": False,
            "closing_source_authoritative": False,
            "point_conversion_in": "0",
            "point_conversion_out": "0",
            "point_difference": "0",
            "point_status": "REVISAR_FUENTE",
        }
        line.save()

        detail = self.client.get(f"/api/pos-bridge/product-closures/{self.product_closure.id}/").data
        row = detail["lines"][0]

        for field in (
            "opening_point",
            "production",
            "sales_total",
            "waste_total",
            "point_conversion_in",
            "point_conversion_out",
            "calculated_closing",
            "closing_point",
            "point_difference",
        ):
            self.assertIsNone(row[field], field)
        self.assertEqual(
            row["source_authority"],
            {
                "opening": False,
                "sales": False,
                "production": False,
                "waste": False,
                "conversions": False,
                "closing": False,
            },
        )
        summary = self.client.get("/api/pos-bridge/product-closures/").data["results"][0]
        self.assertIsNone(summary["total_opening_inventory"])
        self.assertIsNone(summary["total_sales"])
        self.assertIsNone(summary["total_ending_inventory"])

    def test_product_closure_old_canonical_metadata_without_authority_flags_is_conservative(self):
        line = self.product_closure.lines.get()
        line.inventario_final_point_total = Decimal("15")
        line.metadata = {
            "balance_contract": "POINT_PRODUCT_BALANCE_V1",
            "issues": [],
            "sales_source_available": True,
            "point_conversion_in": "0",
            "point_conversion_out": "0",
            "point_difference": "0",
            "point_status": "COINCIDE",
        }
        line.save()

        response = self.client.get(f"/api/pos-bridge/product-closures/{self.product_closure.id}/")
        row = response.data["lines"][0]

        for field in (
            "opening_point",
            "production",
            "sales_total",
            "waste_total",
            "point_conversion_in",
            "point_conversion_out",
            "calculated_closing",
            "closing_point",
            "point_difference",
        ):
            self.assertIsNone(row[field], field)
        self.assertEqual(row["point_status"], "REVISAR_FUENTE")
        self.assertEqual(row["point_status_label"], "Revisar fuente")
        self.assertFalse(any(row["source_authority"].values()))

    def test_product_closure_summary_projects_each_line_once_for_all_totals(self):
        with patch(
            "pos_bridge.api.serializers.closures.project_product_closure_line",
            wraps=project_product_closure_line,
        ) as projection:
            response = self.client.get("/api/pos-bridge/product-closures/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(projection.call_count, self.product_closure.lines.count())

    def test_product_closure_detail_reuses_total_projection_for_line_fields(self):
        with patch(
            "pos_bridge.api.serializers.closures.project_product_closure_line",
            wraps=project_product_closure_line,
        ) as projection:
            response = self.client.get(
                f"/api/pos-bridge/product-closures/{self.product_closure.id}/"
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(projection.call_count, self.product_closure.lines.count())

    def test_product_closure_api_exposes_source_authority_and_issues(self):
        self.product_closure.metadata = {
            "opening_meta": {"authoritative": True, "effective_date": "2026-07-31"},
            "sales_meta": {"authoritative": False, "authority_issues": ["SALES_SYNC_JOB_PARTIAL"]},
            "production_meta": {"authoritative": True},
            "waste_meta": {"authoritative": True},
            "conversion_meta": {"authoritative": True},
            "closing_inventory_meta": {"authoritative": True, "effective_date": "2026-08-31"},
            "balance": {"issues": ["MONTH_SOURCE_INCOMPLETE"]},
            "validation": {"blocking_issues": ["SALES_SYNC_JOB_PARTIAL"], "lock_ready": False},
        }
        self.product_closure.save(update_fields=["metadata", "updated_at"])

        response = self.client.get(f"/api/pos-bridge/product-closures/{self.product_closure.id}/")

        self.assertTrue(response.data["source_authority"]["opening"]["authoritative"])
        self.assertFalse(response.data["source_authority"]["sales"]["authoritative"])
        self.assertIn("SALES_SYNC_JOB_PARTIAL", response.data["source_issues"])
        self.assertIn("MONTH_SOURCE_INCOMPLETE", response.data["source_issues"])

    def test_product_closure_source_authority_never_exposes_private_source_metadata(self):
        self.product_closure.metadata = {
            "opening_meta": {
                "source": "PointInventorySnapshot",
                "source_present": True,
                "authoritative": True,
                "selected_sync_job_ids": [11],
                "snapshot_rows": 9,
                "effective_date": "2026-07-31",
                "request_url": "https://point.invalid/private?token=secret",
                "report_path": "/tmp/private-report.xlsx",
                "raw_samples": [{"password": "secret"}],
            },
            "sales_meta": {
                "selected_source": "official_point_daily_sales",
                "authoritative": False,
                "job_status": "PARTIAL",
                "authority_issues": ["SALES_SYNC_JOB_PARTIAL"],
                "job_id": 22,
                "row_count": 31,
                "request_url": "https://point.invalid/private-sales",
                "nested": {"report_path": "/tmp/also-private.xlsx"},
            },
        }
        self.product_closure.save(update_fields=["metadata", "updated_at"])

        authority = self.client.get(
            f"/api/pos-bridge/product-closures/{self.product_closure.id}/"
        ).data["source_authority"]

        self.assertEqual(authority["opening"]["selected_sync_job_ids"], [11])
        self.assertEqual(authority["sales"]["job_id"], 22)

        def all_keys(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    yield key
                    yield from all_keys(child)
            elif isinstance(value, (list, tuple)):
                for child in value:
                    yield from all_keys(child)

        exposed_keys = set(all_keys(authority))
        self.assertTrue(
            {"request_url", "report_path", "raw_samples", "password"}.isdisjoint(exposed_keys)
        )

    def test_product_closures_build_endpoint_creates_month(self):
        PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=self.product,
            stock=Decimal("9"),
            sync_job=self.sync_job,
            captured_at=timezone.make_aware(datetime(2025, 8, 31, 23, 0, 0), timezone.get_current_timezone()),
        )
        response = self.client.post(
            "/api/pos-bridge/product-closures/build/",
            {"month": "2025-09"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["month"], "2025-09")
        self.assertIn("validation", response.data)

    def test_product_closures_lock_endpoint_locks_clean_closure(self):
        response = self.client.post(
            f"/api/pos-bridge/product-closures/{self.product_closure.id}/lock/",
            {"approval_note": "Cierre aprobado desde API"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.product_closure.refresh_from_db()
        self.assertTrue(self.product_closure.is_locked)
        self.assertEqual(self.product_closure.metadata["lock_event"]["channel"], "api")

    def test_product_closures_build_requires_operator_permission_for_non_staff(self):
        non_staff = get_user_model().objects.create_user(
            username="product_closure_viewer",
            email="product_closure_viewer@example.com",
            password="test12345",
        )
        lectura_group, _ = Group.objects.get_or_create(name="LECTURA")
        non_staff.groups.add(lectura_group)
        self.client.force_authenticate(non_staff)

        response = self.client.post(
            "/api/pos-bridge/product-closures/build/",
            {"month": "2025-09"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_product_closures_existing_month_requires_explicit_rebuild(self):
        original_line = self.product_closure.lines.get()
        response = self.client.post(
            "/api/pos-bridge/product-closures/build/",
            {"month": self.product_closure.month_start.strftime("%Y-%m")},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("rebuild", response.data["detail"])
        self.assertTrue(ProductoMonthClosureLine.objects.filter(pk=original_line.pk).exists())

    def test_product_closures_production_operator_cannot_request_rebuild(self):
        operator = get_user_model().objects.create_user(
            username="product_closure_production",
            email="product_closure_production@example.com",
            password="test12345",
        )
        production_group, _ = Group.objects.get_or_create(name="PRODUCCION")
        operator.groups.add(production_group)
        self.client.force_authenticate(operator)

        response = self.client.post(
            "/api/pos-bridge/product-closures/build/",
            {"month": self.product_closure.month_start.strftime("%Y-%m"), "rebuild": True},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_product_closures_production_operator_cannot_lock_after_build(self):
        operator = get_user_model().objects.create_user(username="closure_builder_only", password="test12345")
        production_group, _ = Group.objects.get_or_create(name="PRODUCCION")
        operator.groups.add(production_group)
        self.client.force_authenticate(operator)

        with patch("pos_bridge.api.views.closures.ProductMonthClosureService.build") as build:
            response = self.client.post(
                "/api/pos-bridge/product-closures/build/",
                {"month": "2025-09", "lock_after_build": True},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        build.assert_not_called()

    def test_product_closures_dg_can_build_and_lock_in_one_request(self):
        director = get_user_model().objects.create_user(username="closure_dg", password="test12345")
        dg_group, _ = Group.objects.get_or_create(name="DG")
        director.groups.add(dg_group)
        self.client.force_authenticate(director)

        with patch(
            "pos_bridge.api.views.closures.ProductMonthClosureService.build",
            return_value=self.product_closure,
        ) as build:
            response = self.client.post(
                "/api/pos-bridge/product-closures/build/",
                {"month": "2025-09", "lock_after_build": True},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertTrue(build.call_args.kwargs["lock_after_build"])
