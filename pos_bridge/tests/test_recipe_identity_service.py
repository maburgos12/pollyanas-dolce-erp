from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService


class PointRecipeIdentityServiceTests(TestCase):
    def test_resolve_unit_supports_company_point_abbreviations(self):
        galon = UnidadMedida.objects.get(codigo="GLI")
        garrafon = UnidadMedida.objects.get(codigo="Gfn")
        caja = UnidadMedida.objects.get(codigo="CJA")
        service = PointRecipeIdentityService()

        self.assertEqual(service.resolve_unit("GLI").id, galon.id)
        self.assertEqual(service.resolve_unit({"Abreviacion": "Gfn"}).id, garrafon.id)
        self.assertEqual(service.resolve_unit("caja").id, caja.id)

    def test_resolve_insumo_by_exact_point_code_does_not_canonicalize_to_other_duplicate(self):
        wrong = Insumo.objects.create(
            nombre="TAPA VASO 16OZ NESCAFE",
            codigo_point="51152103",
            tipo_item=Insumo.TIPO_EMPAQUE,
            categoria="DESECHABLES",
            activo=True,
        )
        correct = Insumo.objects.create(
            nombre="VASO 16OZ NESCAFE",
            codigo_point="52152102",
            tipo_item=Insumo.TIPO_EMPAQUE,
            categoria="DESECHABLES",
            activo=True,
        )

        resolved = PointRecipeIdentityService().resolve_insumo(
            point_code="52152102",
            point_name="VASO 16OZ NESCAFE",
        )

        self.assertEqual(resolved.method, "POINT_CODE")
        self.assertEqual(resolved.insumo.id, correct.id)
        self.assertNotEqual(resolved.insumo.id, wrong.id)
