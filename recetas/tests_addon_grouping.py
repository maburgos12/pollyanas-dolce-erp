from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointDailySale, PointProduct, PointSyncJob
from pos_bridge.services.agent_query_service import PosAgentQueryService
from recetas.models import LineaReceta, Receta, RecetaAgrupacionAddon
from recetas.utils.addon_grouping import calculate_grouped_addon_cost, upsert_addon_rule
from recetas.utils.costeo_versionado import asegurar_version_costeo


class RecetaAddonGroupingTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="addon_agent",
            email="addon_agent@example.com",
            password="test12345",
            is_staff=True,
        )
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.branch = PointBranch.objects.create(
            external_id="1",
            name="MATRIZ",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.sucursal,
        )
        self.sales_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_SUCCESS,
            triggered_by=self.user,
        )
        self.unit_pza = UnidadMedida.objects.create(
            codigo="pza",
            nombre="Pieza",
            tipo=UnidadMedida.TIPO_PIEZA,
            factor_to_base=Decimal("1"),
        )
        self.unit_g = UnidadMedida.objects.create(
            codigo="g",
            nombre="Gramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1"),
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
        self.base_input = Insumo.objects.create(nombre="Base queso", unidad_base=self.unit_pza, activo=True)
        self.addon_input = Insumo.objects.create(nombre="Fresa fresca", unidad_base=self.unit_g, activo=True)
        LineaReceta.objects.create(
            receta=self.base_recipe,
            posicion=1,
            insumo=self.base_input,
            insumo_texto="Base queso",
            cantidad=Decimal("1"),
            unidad=self.unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("150"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        LineaReceta.objects.create(
            receta=self.addon_recipe,
            posicion=1,
            insumo=self.addon_input,
            insumo_texto="Fresa fresca",
            cantidad=Decimal("400"),
            unidad=self.unit_g,
            unidad_texto="g",
            costo_unitario_snapshot=Decimal("0.09"),
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
            external_id="824",
            sku="SFRESAG",
            name="Sabor Fresa Grande Pay",
            category="Pay Grande",
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.base_product,
            receta=self.base_recipe,
            sync_job=self.sales_job,
            sale_date=timezone.localdate(),
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
            sync_job=self.sales_job,
            sale_date=timezone.localdate(),
            quantity=Decimal("10"),
            tickets=0,
            gross_amount=Decimal("0"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("0"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("0"),
        )

    def test_upsert_addon_rule_builds_evidence_and_grouped_cost(self):
        rule = upsert_addon_rule(
            base_receta=self.base_recipe,
            addon_receta=self.addon_recipe,
            addon_codigo_point="SFRESAG",
            addon_nombre_point="Sabor Fresa Grande Pay",
            addon_categoria="Pay Grande",
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )

        self.assertEqual(rule.cooccurrence_days, 1)
        self.assertEqual(rule.cooccurrence_branches, 1)
        self.assertEqual(rule.cooccurrence_qty, Decimal("10"))
        grouped = calculate_grouped_addon_cost(rule=rule)
        self.assertEqual(grouped.base_cost, Decimal("150.000000"))
        self.assertEqual(grouped.addon_cost, Decimal("36.000000"))
        self.assertEqual(grouped.grouped_cost, Decimal("186.000000"))

    def test_agent_recipe_query_returns_grouped_cost_for_base_plus_addon(self):
        upsert_addon_rule(
            base_receta=self.base_recipe,
            addon_receta=self.addon_recipe,
            addon_codigo_point="SFRESAG",
            addon_nombre_point="Sabor Fresa Grande Pay",
            addon_categoria="Pay Grande",
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )

        result = PosAgentQueryService().process_query(
            query="Dame la receta de Pay de Queso Grande y Sabor Fresa Grande Pay",
            user=self.user,
        )

        self.assertEqual(result["query_type"], "recipe")
        self.assertEqual(result["data"]["base_receta"], "Pay de Queso Grande")
        self.assertEqual(result["data"]["addon_receta"], "Sabor Fresa Grande Pay")
        self.assertEqual(result["data"]["grouped_cost"], "186.000000")

    def test_agent_recipe_query_keeps_recipe_intent_for_topping_word(self):
        topping_recipe = Receta.objects.create(
            nombre="TOPPING FRESA M",
            codigo_point="SFRESAPM",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=topping_recipe,
            posicion=1,
            insumo=self.addon_input,
            insumo_texto="Fresa fresca",
            cantidad=Decimal("200"),
            unidad=self.unit_g,
            unidad_texto="g",
            costo_unitario_snapshot=Decimal("0.09"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        asegurar_version_costeo(topping_recipe, fuente="TEST")
        PointProduct.objects.create(
            external_id="923",
            sku="SFRESAPM",
            name="TOPPING FRESA M",
            category="Pastel Mediano",
        )
        upsert_addon_rule(
            base_receta=self.base_recipe,
            addon_receta=topping_recipe,
            addon_codigo_point="SFRESAPM",
            addon_nombre_point="TOPPING FRESA M",
            addon_categoria="Pastel Mediano",
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )

        result = PosAgentQueryService().process_query(
            query="Dame la receta de Pay de Queso Grande y TOPPING FRESA M",
            user=self.user,
        )

        self.assertEqual(result["query_type"], "recipe")
