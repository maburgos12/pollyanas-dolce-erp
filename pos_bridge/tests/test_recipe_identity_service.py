from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService


class PointRecipeIdentityServiceTests(TestCase):
    def test_active_point_code_wins_over_older_inactive_record(self):
        old = Insumo.objects.create(nombre="Rosca Lotus", codigo_point="LOTUS", activo=False)
        active = Insumo.objects.create(nombre="Pan Lotus Rosca", codigo_point="LOTUS")
        resolved = PointRecipeIdentityService().resolve_insumo(point_code="LOTUS")
        self.assertEqual(resolved.insumo.id, active.id)
        old.refresh_from_db()
        self.assertFalse(old.activo)

    def test_inactive_code_does_not_fall_back_to_another_active_name(self):
        Insumo.objects.create(nombre="Retirado", codigo_point="OLD", activo=False)
        Insumo.objects.create(nombre="Similar activo", codigo_point="NEW")
        resolved = PointRecipeIdentityService().resolve_insumo(point_code="OLD", point_name="Similar activo")
        self.assertIsNone(resolved.insumo)
        self.assertEqual(resolved.method, "INACTIVE_POINT_CODE")

    def test_ambiguous_normalized_codes_are_not_selected_arbitrarily(self):
        Insumo.objects.create(nombre="Primero", codigo_point="AB-1")
        Insumo.objects.create(nombre="Segundo", codigo_point="AB1")
        resolved = PointRecipeIdentityService().resolve_insumo(point_code="ab1")
        self.assertIsNone(resolved.insumo)
        self.assertEqual(resolved.method, "AMBIGUOUS_POINT_CODE")

    def test_internal_creation_preserves_inactive_and_ambiguous_codes(self):
        Insumo.objects.create(nombre="Retirado", codigo_point="OLD", activo=False)
        Insumo.objects.create(nombre="Primero", codigo_point="AB-1")
        Insumo.objects.create(nombre="Segundo", codigo_point="AB1")
        service = PointRecipeIdentityService()
        for code in ("OLD", "AB1"):
            with self.subTest(code=code), self.assertRaises(ValueError):
                service.get_or_create_internal_insumo(point_code=code, point_name="Nuevo")
        self.assertEqual(Insumo.objects.count(), 3)

    def test_resolve_unit_supports_company_point_abbreviations(self):
        galon, _ = UnidadMedida.objects.get_or_create(codigo="GLI", defaults={"nombre": "Galón"})
        garrafon, _ = UnidadMedida.objects.get_or_create(codigo="Gfn", defaults={"nombre": "Garrafón"})
        caja, _ = UnidadMedida.objects.get_or_create(codigo="CJA", defaults={"nombre": "Caja"})
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
