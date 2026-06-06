from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from recetas.models import LineaReceta, Receta, RecetaAgrupacionAddon, RecetaCostoSemanal
from recetas.utils.costeo_semanal import snapshot_weekly_costs
from reportes.models import RecetaCostoHistoricoMensual


class WeeklyCostSnapshotTests(TestCase):
    def setUp(self):
        self.unit = UnidadMedida.objects.create(codigo="pza", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA)
        self.base_input = Insumo.objects.create(nombre="Base", unidad_base=self.unit, activo=True)
        self.addon_input = Insumo.objects.create(nombre="Addon", unidad_base=self.unit, activo=True)
        self.base_recipe = Receta.objects.create(
            nombre="Pay de Queso Grande",
            codigo_point="0001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pay",
            categoria="Pay Grande",
            hash_contenido=f"hash-{uuid4()}",
        )
        self.addon_recipe = Receta.objects.create(
            nombre="Sabor Fresa Grande Pay",
            codigo_point="SFRESAG",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pay",
            categoria="Pay Grande",
            hash_contenido=f"hash-{uuid4()}",
        )
        LineaReceta.objects.create(
            receta=self.base_recipe,
            posicion=1,
            insumo=self.base_input,
            insumo_texto="Base",
            cantidad=Decimal("1"),
            unidad=self.unit,
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
            unidad=self.unit,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("2"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        self.rule = RecetaAgrupacionAddon.objects.create(
            base_receta=self.base_recipe,
            addon_receta=self.addon_recipe,
            addon_codigo_point="SFRESAG",
            addon_nombre_point="Sabor Fresa Grande Pay",
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
            activo=True,
        )

    def test_snapshot_creates_recipe_and_grouped_addon_rows(self):
        summary = snapshot_weekly_costs(anchor_date=date(2026, 3, 23))

        self.assertEqual(summary.recipes_created, 2)
        self.assertEqual(summary.addons_created, 1)
        self.assertTrue(RecetaCostoSemanal.objects.filter(identity_key=f"RECIPE:{self.base_recipe.id}").exists())
        self.assertTrue(RecetaCostoSemanal.objects.filter(identity_key=f"GROUPED_ADDON:{self.rule.id}").exists())

    def test_snapshot_computes_delta_vs_previous_week(self):
        snapshot_weekly_costs(anchor_date=date(2026, 3, 16))
        linea = self.base_recipe.lineas.first()
        linea.costo_unitario_snapshot = Decimal("12")
        linea.save(update_fields=["costo_unitario_snapshot"])

        snapshot_weekly_costs(anchor_date=date(2026, 3, 23))
        latest = RecetaCostoSemanal.objects.get(identity_key=f"RECIPE:{self.base_recipe.id}", week_start=date(2026, 3, 23))
        self.assertEqual(latest.delta_total, Decimal("2.000000"))

    def test_snapshot_ignores_subsection_cost_in_recipe_total(self):
        LineaReceta.objects.create(
            receta=self.base_recipe,
            posicion=2,
            tipo_linea=LineaReceta.TIPO_SUBSECCION,
            etapa="Decorado",
            insumo=None,
            insumo_texto="Decorado",
            cantidad=Decimal("0.100000"),
            unidad=self.unit,
            unidad_texto="pza",
            costo_linea_excel=Decimal("99"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_SUBSECTION,
        )

        snapshot_weekly_costs(anchor_date=date(2026, 3, 23))
        latest = RecetaCostoSemanal.objects.get(identity_key=f"RECIPE:{self.base_recipe.id}", week_start=date(2026, 3, 23))

        self.assertEqual(latest.costo_mp, Decimal("10.000000"))
        self.assertEqual(latest.costo_total, Decimal("10.000000"))

    def test_snapshot_prefers_complete_monthly_historical_cost(self):
        linea = self.base_recipe.lineas.first()
        linea.costo_unitario_snapshot = Decimal("37120.844267")
        linea.save(update_fields=["costo_unitario_snapshot"])
        RecetaCostoHistoricoMensual.objects.create(
            periodo=date(2026, 4, 1),
            receta=self.base_recipe,
            costo_total=Decimal("17.380640"),
            costo_por_unidad_rendimiento=Decimal("17.380640"),
            lineas_costeadas=1,
            lineas_totales=1,
            coverage_pct=Decimal("100.000000"),
            metadata={"source": "test"},
        )

        snapshot_weekly_costs(anchor_date=date(2026, 4, 27), receta_ids=[self.base_recipe.id], include_addons=False)
        latest = RecetaCostoSemanal.objects.get(
            identity_key=f"RECIPE:{self.base_recipe.id}",
            week_start=date(2026, 4, 27),
        )

        self.assertEqual(latest.costo_mp, Decimal("17.380640"))
        self.assertEqual(latest.costo_total, Decimal("17.380640"))
        self.assertIsNone(latest.version_receta)
        self.assertEqual(latest.metadata["cost_source"], "HISTORICAL_MONTHLY")
