from __future__ import annotations

from django.test import TestCase
from unittest.mock import patch

from maestros.models import Insumo, InsumoAlias, UnidadMedida
from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureService, PointInventoryCostRow


class PointInventoryCostCaptureServiceTests(TestCase):
    def test_expand_search_scope_uses_aliases_and_point_code_from_resolved_insumo(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1000)
        insumo = Insumo.objects.create(
            codigo_point="009",
            nombre_point="SUSTITUTO DE CREMA",
            nombre="SUSTITUTO DE CREMA",
            unidad_base=unidad,
        )
        InsumoAlias.objects.create(nombre="Polvo Dream Whip", insumo=insumo)
        InsumoAlias.objects.create(nombre="Polvo para Dream Whip", insumo=insumo)

        service = PointInventoryCostCaptureService()

        queries, point_codes = service._expand_search_scope(
            queries=["Polvo Dream Whip"],
            point_codes=[],
        )

        self.assertIn("Polvo Dream Whip", queries)
        self.assertIn("Polvo para Dream Whip", queries)
        self.assertIn("SUSTITUTO DE CREMA", queries)
        self.assertIn("009", point_codes)

    def test_capture_and_persist_all_summarizes_created_existing_and_skipped_rows(self):
        unidad = UnidadMedida.objects.create(codigo="pz", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA, factor_to_base=1)
        insumo = Insumo.objects.create(
            codigo_point="009",
            nombre_point="SUSTITUTO DE CREMA",
            nombre="SUSTITUTO DE CREMA",
            unidad_base=unidad,
        )
        Insumo.objects.create(
            codigo_point="011",
            nombre_point="COSTO CERO",
            nombre="COSTO CERO",
            unidad_base=unidad,
        )
        service = PointInventoryCostCaptureService()
        rows = [
            PointInventoryCostRow(
                branch_name="ALMACEN",
                category_name="Insumos",
                point_internal_id="1",
                point_code="009",
                point_name="SUSTITUTO DE CREMA",
                point_category="Insumos",
                quantity=1,
                unit="kg",
                unit_cost=10,
                total_cost=10,
                last_movement="2026-04-14T10:00:00",
                raw_row=[],
            ),
            PointInventoryCostRow(
                branch_name="ALMACEN",
                category_name="Insumos",
                point_internal_id="2",
                point_code="010",
                point_name="SIN MATCH",
                point_category="Insumos",
                quantity=1,
                unit="kg",
                unit_cost=5,
                total_cost=5,
                last_movement="2026-04-14T10:00:00",
                raw_row=[],
            ),
            PointInventoryCostRow(
                branch_name="ALMACEN",
                category_name="Empaque",
                point_internal_id="3",
                point_code="011",
                point_name="COSTO CERO",
                point_category="Empaque",
                quantity=1,
                unit="pz",
                unit_cost=0,
                total_cost=0,
                last_movement="2026-04-14T10:00:00",
                raw_row=[],
            ),
        ]

        with patch.object(service, "capture_all_rows", return_value=rows):
            result = service.capture_and_persist_all(branch_hint="ALMACEN")
            second = service.capture_and_persist_all(branch_hint="ALMACEN")

        self.assertEqual(insumo.codigo_point, "009")
        self.assertEqual(result.rows_seen, 3)
        self.assertEqual(result.costs_created, 1)
        self.assertEqual(result.costs_existing, 0)
        self.assertEqual(result.unresolved_matches, 1)
        self.assertEqual(result.zero_cost_matches, 1)
        self.assertEqual(second.costs_created, 0)
        self.assertEqual(second.costs_existing, 1)
