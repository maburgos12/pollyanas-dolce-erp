from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointDailySale, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.models import LineaReceta, Receta


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
