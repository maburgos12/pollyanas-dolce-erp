from __future__ import annotations

from django.test import TestCase

from maestros.models import Insumo, InsumoAlias, UnidadMedida
from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureService


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
