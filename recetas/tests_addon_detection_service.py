from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct, PointSyncJob
from recetas.models import Receta
from recetas.utils.addon_detection_service import PointAddonDetectionService


class PointAddonDetectionServiceTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.branch = PointBranch.objects.create(
            external_id="1",
            name="MATRIZ",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.sucursal,
        )
        self.job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_SALES, status=PointSyncJob.STATUS_SUCCESS)
        self.base_product = PointProduct.objects.create(
            external_id="1",
            sku="0001",
            name="Pay de Queso Grande",
            category="Pay Grande",
        )
        self.addon_product = PointProduct.objects.create(
            external_id="824",
            sku="SFRESAG",
            name="Sabor Fresa Grande Pay",
            category="Pay Grande",
        )
        self.base_recipe = Receta.objects.create(
            nombre="Pay de Queso Grande",
            codigo_point="0001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        self.addon_recipe = Receta.objects.create(
            nombre="Sabor Fresa Grande Pay",
            codigo_point="SFRESAG",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        today = timezone.localdate()
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.base_product,
            receta=self.base_recipe,
            sync_job=self.job,
            sale_date=today,
            quantity=Decimal("10"),
            tickets=4,
            gross_amount=Decimal("1000"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("1000"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("1000"),
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.addon_product,
            receta=self.addon_recipe,
            sync_job=self.job,
            sale_date=today,
            quantity=Decimal("10"),
            tickets=0,
            gross_amount=Decimal("0"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("0"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("0"),
        )

    def test_detect_and_stage_creates_detected_rule(self):
        service = PointAddonDetectionService(recipe_sync_service=None)
        service.recipe_sync_service = type("Noop", (), {"sync": lambda *a, **k: None})()

        report = service.detect_and_stage(auto_sync_missing=False, top_per_addon=1)

        self.assertEqual(len(report["candidates"]), 1)
        self.assertEqual(report["candidates"][0]["sku"], "SFRESAG")
        self.assertEqual(len(report["detected_rules"]), 1)
        self.assertEqual(report["detected_rules"][0]["base_codigo_point"], "0001")
