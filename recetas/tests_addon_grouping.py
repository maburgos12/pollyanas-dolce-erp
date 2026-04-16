from __future__ import annotations

from decimal import Decimal
from io import StringIO
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointDailySale, PointProduct, PointSyncJob
from pos_bridge.services.agent_query_service import PosAgentQueryService
from recetas.models import LineaReceta, Receta, RecetaAgrupacionAddon, RecetaPresentacionDerivada
from recetas.utils.addon_grouping import calculate_grouped_addon_cost, upsert_addon_rule
from recetas.utils.commercial_composition import (
    RULE_COMPLEMENTO_OBLIGATORIO,
    RULE_BLOQUEADO_POR_AMBIGUEDAD,
    RULE_HISTORICO_LEGADO,
    RULE_PRODUCTO_BASE_DIRECTO,
    classify_commercial_recipe,
    ensure_curated_commercial_mappings,
    get_commercial_total_cost_map,
    iter_commercial_validation_rows,
    resolve_commercial_recipe,
)
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

    def test_curated_fresa_rebanada_pay_resolves_as_base_plus_addon(self):
        unit_ml = UnidadMedida.objects.create(
            codigo="ml",
            nombre="Mililitro",
            tipo=UnidadMedida.TIPO_VOLUMEN,
            factor_to_base=Decimal("0.001"),
        )
        spoon = Insumo.objects.create(nombre="CUCHARA CH", unidad_base=self.unit_pza, activo=True)
        label_ch = Insumo.objects.create(nombre="ETIQUETA CH", unidad_base=self.unit_pza, activo=True)
        triangle = Insumo.objects.create(nombre="Rebanada Triangular RP25", unidad_base=self.unit_pza, activo=True)
        cookie = Insumo.objects.create(nombre="Galleta Para Pay", unidad_base=self.unit_g, activo=True)
        jam = Insumo.objects.create(nombre="Mermelada Fresa", unidad_base=unit_ml, activo=True)
        fresh_strawberry = Insumo.objects.create(nombre="Fresa", unidad_base=self.unit_g, activo=True)
        derived_base = Receta.objects.create(
            nombre="Pay de Queso Rebanada",
            codigo_point="0003",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        RecetaPresentacionDerivada.objects.create(
            receta_padre=self.base_recipe,
            receta_derivada=derived_base,
            codigo_point_derivado="0003",
            nombre_derivado=derived_base.nombre,
            unidades_por_padre=Decimal("8"),
            requiere_componentes_directos=True,
        )
        addon_recipe = Receta.objects.create(
            nombre="Sabor Fresa Rebanada Pay",
            codigo_point="03SPFREB",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        for insumo, cost in [
            (fresh_strawberry, Decimal("0.09")),
            (spoon, Decimal("0.50")),
            (label_ch, Decimal("0.26")),
            (triangle, Decimal("0.90")),
            (cookie, Decimal("0.10")),
            (jam, Decimal("0.12")),
        ]:
            CostoInsumo.objects.create(
                insumo=insumo,
                costo_unitario=cost,
                source_hash=str(uuid4()),
            )
        PointProduct.objects.create(
            external_id="911",
            sku="0003",
            name="Pay de Queso Rebanada",
            category="Rebanada",
        )
        PointProduct.objects.create(
            external_id="912",
            sku="03SPFREB",
            name="Sabor Fresa Rebanada Pay",
            category="Rebanada",
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=PointProduct.objects.get(sku="0003"),
            receta=derived_base,
            sync_job=self.sales_job,
            sale_date=timezone.localdate(),
            quantity=Decimal("6"),
            tickets=2,
            gross_amount=Decimal("300"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("300"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("300"),
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=PointProduct.objects.get(sku="03SPFREB"),
            receta=addon_recipe,
            sync_job=self.sales_job,
            sale_date=timezone.localdate(),
            quantity=Decimal("6"),
            tickets=0,
            gross_amount=Decimal("0"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("0"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("0"),
        )

        costs = get_commercial_total_cost_map({addon_recipe.id})
        resolution = resolve_commercial_recipe(addon_recipe)
        addon_recipe.refresh_from_db()

        self.assertEqual(resolution.resolution_kind, "BASE_PLUS_ADDON")
        self.assertEqual([component.codigo_point for component in resolution.component_recetas], ["0003", "03SPFREB"])
        self.assertEqual(
            addon_recipe.lineas.exclude(match_status=LineaReceta.STATUS_REJECTED).count(),
            6,
        )

    def test_classify_commercial_recipe_distinguishes_history_complement_and_direct(self):
        ensure_curated_commercial_mappings()
        history_recipe = Receta.objects.create(
            nombre="Sabor Guayaba Rebanada",
            codigo_point="03SPGREB",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        base_medium = Receta.objects.create(
            nombre="Pastel de Crunch Mediano",
            codigo_point="0060",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=base_medium,
            posicion=1,
            insumo=self.base_input,
            insumo_texto="Base queso",
            cantidad=Decimal("1"),
            unidad=self.unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("100"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        complement_recipe = Receta.objects.create(
            nombre="TOPPING CRUNCH M",
            codigo_point="21125",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=complement_recipe,
            posicion=1,
            insumo=self.addon_input,
            insumo_texto="Fresa fresca",
            cantidad=Decimal("1"),
            unidad=self.unit_g,
            unidad_texto="g",
            costo_unitario_snapshot=Decimal("1"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        direct_recipe = Receta.objects.create(
            nombre="Pastel Directo Control",
            codigo_point="DIRTEST01",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )

        history = classify_commercial_recipe(history_recipe)
        complement = classify_commercial_recipe(complement_recipe)
        direct = classify_commercial_recipe(direct_recipe)

        self.assertEqual(history.clasificacion, RULE_HISTORICO_LEGADO)
        self.assertEqual(history.sku_historico, "0011")
        self.assertEqual(complement.clasificacion, RULE_COMPLEMENTO_OBLIGATORIO)
        self.assertEqual(complement.sku_base, "0060")
        self.assertEqual(direct.clasificacion, RULE_PRODUCTO_BASE_DIRECTO)

    def test_classify_commercial_recipe_blocks_ambiguous_candidates(self):
        ambiguous_recipe = Receta.objects.create(
            nombre="Sabor Mango Grande Pay",
            codigo_point="SMANGOG",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        direct_recipe = Receta.objects.create(
            nombre="Pastel Directo Control",
            codigo_point="DIRTEST02",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )

        ambiguous = classify_commercial_recipe(ambiguous_recipe)
        direct = classify_commercial_recipe(direct_recipe)

        self.assertEqual(ambiguous.clasificacion, RULE_BLOQUEADO_POR_AMBIGUEDAD)
        self.assertEqual(ambiguous.estado, "BLOQUEADO")
        self.assertEqual(ambiguous.confianza, "BAJA")
        self.assertIn("ambig", ambiguous.nota_negocio.lower())
        self.assertEqual(direct.clasificacion, RULE_PRODUCTO_BASE_DIRECTO)

    def test_exportar_matriz_relaciones_comerciales_generates_outputs(self):
        base_medium = Receta.objects.create(
            nombre="Pastel de Crunch Mediano",
            codigo_point="0060",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=base_medium,
            posicion=1,
            insumo=self.base_input,
            insumo_texto="Base queso",
            cantidad=Decimal("1"),
            unidad=self.unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("100"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        complement_recipe = Receta.objects.create(
            nombre="TOPPING CRUNCH M",
            codigo_point="21125",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=complement_recipe,
            posicion=1,
            insumo=self.addon_input,
            insumo_texto="Fresa fresca",
            cantidad=Decimal("1"),
            unidad=self.unit_g,
            unidad_texto="g",
            costo_unitario_snapshot=Decimal("1"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        out = StringIO()
        call_command("exportar_matriz_relaciones_comerciales", stdout=out)
        output = out.getvalue()

        self.assertIn("matriz_validacion_relaciones_point_erp_", output)
        rows = iter_commercial_validation_rows()
        self.assertTrue(any(row.clasificacion == RULE_COMPLEMENTO_OBLIGATORIO for row in rows))

    def test_curated_crunch_slice_alias_uses_point_recipe_cost(self):
        medium_input = Insumo.objects.create(nombre="Base crunch", unidad_base=self.unit_pza, activo=True)
        medium_recipe = Receta.objects.create(
            nombre="Pastel de Crunch Mediano",
            codigo_point="0060",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=medium_recipe,
            posicion=1,
            insumo=medium_input,
            insumo_texto="Base crunch",
            cantidad=Decimal("1"),
            unidad=self.unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("200"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        slice_recipe = Receta.objects.create(
            nombre="Pastel de Crunch R",
            codigo_point="0063",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        RecetaPresentacionDerivada.objects.create(
            receta_padre=medium_recipe,
            receta_derivada=slice_recipe,
            codigo_point_derivado="0063",
            nombre_derivado=slice_recipe.nombre,
            unidades_por_padre=Decimal("10"),
            requiere_componentes_directos=True,
        )
        alias_recipe = Receta.objects.create(
            nombre="Pastel Crunch - Rebanada",
            codigo_point="",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )

        costs = get_commercial_total_cost_map({slice_recipe.id, alias_recipe.id})
        resolution = resolve_commercial_recipe(alias_recipe)

        self.assertEqual(resolution.resolution_kind, "ALIASED_RECIPE")
        self.assertEqual(resolution.component_recetas[0].codigo_point, "0063")
        self.assertEqual(costs[alias_recipe.id], costs[slice_recipe.id])

    def test_curated_mapping_sync_is_idempotent(self):
        unit_ml = UnidadMedida.objects.create(
            codigo="ml",
            nombre="Mililitro",
            tipo=UnidadMedida.TIPO_VOLUMEN,
            factor_to_base=Decimal("0.001"),
        )
        derived_base = Receta.objects.create(
            nombre="Pay de Queso Rebanada",
            codigo_point="0003",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        RecetaPresentacionDerivada.objects.create(
            receta_padre=self.base_recipe,
            receta_derivada=derived_base,
            codigo_point_derivado="0003",
            nombre_derivado=derived_base.nombre,
            unidades_por_padre=Decimal("8"),
            requiere_componentes_directos=True,
        )
        addon_recipe = Receta.objects.create(
            nombre="Sabor Fresa Rebanada Pay",
            codigo_point="03SPFREB",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        for insumo_name, unit in [
            ("CUCHARA CH", self.unit_pza),
            ("ETIQUETA CH", self.unit_pza),
            ("Rebanada Triangular RP25", self.unit_pza),
            ("Fresa", self.unit_g),
            ("Galleta Para Pay", self.unit_g),
        ]:
            Insumo.objects.create(nombre=insumo_name, unidad_base=unit, activo=True)
        Insumo.objects.create(nombre="Mermelada Fresa", unidad_base=unit_ml, activo=True)

        ensure_curated_commercial_mappings()
        ensure_curated_commercial_mappings()

        self.assertEqual(
            RecetaAgrupacionAddon.objects.filter(base_receta=derived_base, addon_codigo_point="03SPFREB").count(),
            1,
        )
        self.assertEqual(
            addon_recipe.lineas.exclude(match_status=LineaReceta.STATUS_REJECTED).count(),
            6,
        )
