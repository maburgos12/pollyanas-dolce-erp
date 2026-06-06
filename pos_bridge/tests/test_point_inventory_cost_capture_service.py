from __future__ import annotations

from django.test import TestCase
from unittest.mock import patch

from maestros.models import CostoInsumo, Insumo, InsumoAlias, UnidadMedida
from pos_bridge.models import PointProduct
from pos_bridge.services.point_cost_validation import point_unit_code, point_unit_type
from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureService, PointInventoryCostRow
from reportes.models import ProductoReventaCosto


class PointInventoryCostCaptureServiceTests(TestCase):
    def test_company_point_units_are_recognized_by_cost_validator(self):
        self.assertEqual(point_unit_code("GLI"), "gli")
        self.assertEqual(point_unit_type("GLI"), "VOLUME")
        self.assertEqual(point_unit_code("Gfn"), "gfn")
        self.assertEqual(point_unit_type("Gfn"), "VOLUME")
        self.assertEqual(point_unit_code("CJA"), "cja")
        self.assertEqual(point_unit_type("CJA"), "UNIT")

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
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1000)
        pieza = UnidadMedida.objects.create(codigo="pz", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA, factor_to_base=1)
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
            unidad_base=pieza,
        )
        PointProduct.objects.create(
            external_id="009",
            sku="009",
            name="SUSTITUTO DE CREMA",
            category="Insumos",
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
        self.assertEqual(result.resale_costs_created, 1)
        self.assertEqual(result.resale_costs_existing, 0)
        self.assertEqual(result.unresolved_matches, 1)
        self.assertEqual(result.zero_cost_matches, 1)
        self.assertEqual(second.costs_created, 0)
        self.assertEqual(second.costs_existing, 1)
        self.assertEqual(second.resale_costs_created, 0)
        self.assertEqual(second.resale_costs_existing, 1)
        self.assertEqual(ProductoReventaCosto.objects.count(), 1)

    def test_resale_product_match_preserves_leading_zero_point_code(self):
        tea = PointProduct.objects.create(
            external_id="402",
            sku="0313",
            name="TE DEL JARDIN",
            category="Te",
        )
        oreo = PointProduct.objects.create(
            external_id="137",
            sku="0402",
            name="Galleta Oreo Base",
            category="Galletas",
        )
        service = PointInventoryCostCaptureService()
        row = PointInventoryCostRow(
            branch_name="ALMACEN",
            category_name="GALLETAS",
            point_internal_id="137",
            point_code="0402",
            point_name="Galleta Oreo Base",
            point_category="GALLETAS",
            quantity=1,
            unit="KG",
            unit_cost=117.72,
            total_cost=117.72,
            last_movement="2026-04-24T10:00:00",
            raw_row=[],
        )

        product = service._resolve_point_product(row)

        self.assertEqual(product, oreo)
        self.assertNotEqual(product, tea)

    def test_resale_product_match_does_not_strip_leading_zero_to_external_id(self):
        PointProduct.objects.create(
            external_id="402",
            sku="0313",
            name="TE DEL JARDIN",
            category="Te",
        )
        service = PointInventoryCostCaptureService()
        row = PointInventoryCostRow(
            branch_name="ALMACEN",
            category_name="GALLETAS",
            point_internal_id="137",
            point_code="0402",
            point_name="Galleta Oreo Base",
            point_category="GALLETAS",
            quantity=1,
            unit="KG",
            unit_cost=117.72,
            total_cost=117.72,
            last_movement="2026-04-24T10:00:00",
            raw_row=[],
        )

        product = service._resolve_point_product(row)

        self.assertIsNone(product)

    def test_persist_cost_row_rejects_reused_point_code_with_different_name(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1000)
        Insumo.objects.create(
            codigo_point="50161813",
            nombre_point="CRUNCH ROCKS",
            nombre="CRUNCH ROCKS",
            unidad_base=unidad,
        )
        service = PointInventoryCostCaptureService()
        row = PointInventoryCostRow(
            branch_name="ALMACEN",
            category_name="Insumos",
            point_internal_id="1",
            point_code="50161813",
            point_name="HERSHEYS MINIATURA",
            point_category="Insumos",
            quantity=1,
            unit="PZA",
            unit_cost=2.92,
            total_cost=2.92,
            last_movement="2026-04-14T10:00:00",
            raw_row=[],
        )

        cost, created, status = service.persist_cost_row(row)

        self.assertIsNone(cost)
        self.assertFalse(created)
        self.assertIn("NOMBRE_POINT_NO_COINCIDE", status)
        self.assertIn("UNIDAD_INCOMPATIBLE", status)
        self.assertEqual(CostoInsumo.objects.count(), 0)

    def test_persist_cost_row_accepts_positive_cost_with_non_positive_quantity(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1000)
        Insumo.objects.create(
            codigo_point="50161813",
            nombre_point="CRUNCH ROCKS",
            nombre="CRUNCH ROCKS",
            unidad_base=unidad,
        )
        service = PointInventoryCostCaptureService()
        row = PointInventoryCostRow(
            branch_name="ALMACEN",
            category_name="Insumos",
            point_internal_id="1",
            point_code="50161813",
            point_name="CRUNCH ROCKS",
            point_category="Insumos",
            quantity=0,
            unit="kg",
            unit_cost=319.83,
            total_cost=0,
            last_movement="2026-04-14T10:00:00",
            raw_row=[],
        )

        cost, created, status = service.persist_cost_row(row)

        self.assertIsNotNone(cost)
        self.assertTrue(created)
        self.assertEqual(status, "CREATED")
        self.assertEqual(CostoInsumo.objects.count(), 1)

    def test_capture_and_persist_all_reports_validation_rejections(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1000)
        Insumo.objects.create(
            codigo_point="50161813",
            nombre_point="CRUNCH ROCKS",
            nombre="CRUNCH ROCKS",
            unidad_base=unidad,
        )
        service = PointInventoryCostCaptureService()
        rows = [
            PointInventoryCostRow(
                branch_name="ALMACEN",
                category_name="Insumos",
                point_internal_id="1",
                point_code="50161813",
                point_name="CRUNCH ROCKS",
                point_category="Insumos",
                quantity=1,
                unit="kg",
                unit_cost=319.83,
                total_cost=319.83,
                last_movement="2026-04-14T10:00:00",
                raw_row=[],
            ),
            PointInventoryCostRow(
                branch_name="ALMACEN",
                category_name="Insumos",
                point_internal_id="2",
                point_code="50161813",
                point_name="HERSHEYS MINIATURA",
                point_category="Insumos",
                quantity=0,
                unit="PZA",
                unit_cost=2.92,
                total_cost=0,
                last_movement="2026-04-14T10:00:00",
                raw_row=[],
            ),
        ]

        with patch.object(service, "capture_all_rows", return_value=rows):
            result = service.capture_and_persist_all(branch_hint="ALMACEN")

        self.assertEqual(result.costs_created, 1)
        self.assertEqual(result.rejected_matches, 1)
        self.assertEqual(result.rejected_samples[0]["point_code"], "50161813")
        self.assertIn("NOMBRE_POINT_NO_COINCIDE", result.rejected_samples[0]["status"])
        self.assertEqual(CostoInsumo.objects.count(), 1)
