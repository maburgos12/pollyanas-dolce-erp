from __future__ import annotations

import json
from decimal import Decimal
from io import StringIO
from uuid import uuid4

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointDailySale, PointProduct, PointSyncJob
from recetas.models import LineaReceta, Receta, RecetaAgrupacionAddon
from recetas.utils.costeo_versionado import asegurar_version_costeo


class ApprovePointAddonsSafeCommandTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.branch = PointBranch.objects.create(
            external_id="1",
            name="MATRIZ",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.sucursal,
        )
        self.job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_SALES, status=PointSyncJob.STATUS_SUCCESS)
        unit_pza = UnidadMedida.objects.create(
            codigo="pza",
            nombre="Pieza",
            tipo=UnidadMedida.TIPO_PIEZA,
            factor_to_base=Decimal("1"),
        )
        self.unit_pza = unit_pza
        self.base_input = Insumo.objects.create(nombre="Base", unidad_base=unit_pza, activo=True)
        self.addon_input = Insumo.objects.create(nombre="Addon", unidad_base=unit_pza, activo=True)

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
        LineaReceta.objects.create(
            receta=self.base_recipe,
            posicion=1,
            insumo=self.base_input,
            insumo_texto="Base",
            cantidad=Decimal("1"),
            unidad=unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("10"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        LineaReceta.objects.create(
            receta=self.addon_recipe,
            posicion=1,
            insumo=self.addon_input,
            insumo_texto="Addon",
            cantidad=Decimal("1"),
            unidad=unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("2"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        asegurar_version_costeo(self.base_recipe, fuente="TEST")
        asegurar_version_costeo(self.addon_recipe, fuente="TEST")

        self.base_product = PointProduct.objects.create(
            external_id="1",
            sku="0001",
            name="Pay de Queso Grande",
            category="Pay Grande",
        )
        self.addon_product = PointProduct.objects.create(
            external_id="2",
            sku="SFRESAG",
            name="Sabor Fresa Grande Pay",
            category="Pay Grande",
        )
        today = timezone.localdate()
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.base_product,
            receta=self.base_recipe,
            sync_job=self.job,
            sale_date=today,
            quantity=Decimal("5"),
            tickets=2,
            gross_amount=Decimal("100"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("100"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("100"),
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.addon_product,
            receta=self.addon_recipe,
            sync_job=self.job,
            sale_date=today,
            quantity=Decimal("5"),
            tickets=0,
            gross_amount=Decimal("0"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("0"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("0"),
        )

        self.ambiguous_recipe = Receta.objects.create(
            nombre="TOPPING ZANAHORIA C",
            codigo_point="1254",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        PointProduct.objects.create(
            external_id="3",
            sku="1254",
            name="TOPPING ZANAHORIA C",
            category="Pastel Chico",
        )
        PointProduct.objects.create(
            external_id="4",
            sku="1254",
            name="TOPPING CRUNCH MINI",
            category="Pastel Mini",
        )

    def test_command_approves_safe_pair(self):
        out = StringIO()
        call_command("approve_point_addons_safe", stdout=out)
        payload = json.loads(out.getvalue())

        rule = RecetaAgrupacionAddon.objects.get(base_receta=self.base_recipe, addon_codigo_point="SFRESAG")
        self.assertEqual(rule.status, RecetaAgrupacionAddon.STATUS_APPROVED)
        self.assertTrue(any(item["addon_codigo_point"] == "SFRESAG" for item in payload["approved"]))

    def test_command_skips_duplicate_sku(self):
        out = StringIO()
        call_command("approve_point_addons_safe", stdout=out)
        payload = json.loads(out.getvalue())

        self.assertFalse(RecetaAgrupacionAddon.objects.filter(addon_codigo_point="1254").exists())
        self.assertTrue(any(item["addon_codigo_point"] == "1254" for item in payload["skipped"]))

    def test_command_allows_smanzanareb_when_erp_recipe_is_canonical(self):
        base_input = Insumo.objects.create(nombre="Base Rebanada", unidad_base=self.unit_pza, activo=True)
        base_rebanada = Receta.objects.create(
            nombre="Pay de Queso Rebanada",
            codigo_point="0003",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=base_rebanada,
            posicion=1,
            insumo=base_input,
            insumo_texto="Base Rebanada",
            cantidad=Decimal("1"),
            unidad=self.unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("8"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        asegurar_version_costeo(base_rebanada, fuente="TEST")
        addon_input = Insumo.objects.create(nombre="Addon Manzana", unidad_base=self.unit_pza, activo=True)
        addon_recipe = Receta.objects.create(
            nombre="Sabor Pay Manzana Rebanada",
            codigo_point="SMANZANAREB",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=addon_recipe,
            posicion=1,
            insumo=addon_input,
            insumo_texto="Addon Manzana",
            cantidad=Decimal("1"),
            unidad=self.unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("3"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        asegurar_version_costeo(addon_recipe, fuente="TEST")
        PointProduct.objects.create(
            external_id="5",
            sku="SMANZANAREB",
            name="Sabor Mazana Rebanada",
            category="Rebanada",
        )
        PointProduct.objects.create(
            external_id="6",
            sku="SMANZANAREB",
            name="Sabor Pay Manzana Rebanada",
            category="Rebanada",
        )

        out = StringIO()
        call_command("approve_point_addons_safe", stdout=out)

        rule = RecetaAgrupacionAddon.objects.get(base_receta=base_rebanada, addon_codigo_point="SMANZANAREB")
        addon_recipe.refresh_from_db()
        self.assertEqual(rule.status, RecetaAgrupacionAddon.STATUS_APPROVED)
        self.assertEqual(addon_recipe.temporalidad, Receta.TEMPORALIDAD_TEMPORAL)
