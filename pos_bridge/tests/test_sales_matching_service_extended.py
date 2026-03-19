from django.test import TestCase

from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import Receta


class PointSalesMatchingServiceExtendedTests(TestCase):
    def test_is_descriptive_product_name_accepts_multiword_name(self):
        service = PointSalesMatchingService()
        self.assertTrue(service.is_descriptive_product_name(point_name="Pastel de Ciruela R", family="Pastel"))

    def test_is_descriptive_product_name_rejects_numeric_code_name(self):
        service = PointSalesMatchingService()
        self.assertFalse(service.is_descriptive_product_name(point_name="800", family=""))

    def test_create_missing_product_recipe_creates_producto_final_with_alias(self):
        service = PointSalesMatchingService()
        receta = service.create_missing_product_recipe(
            codigo_point="SFRESAM",
            point_name="Sabor Fresa Mediano Pay",
            category="Pay Mediano",
            family="Pay",
        )

        self.assertIsNotNone(receta)
        receta_db = Receta.objects.get(pk=receta.id)
        self.assertEqual(receta_db.nombre, "Sabor Fresa Mediano Pay")
        self.assertEqual(receta_db.codigo_point, "SFRESAM")
        self.assertEqual(receta_db.tipo, Receta.TIPO_PRODUCTO_FINAL)
        self.assertEqual(receta_db.sheet_name, "AUTO_POINT_SALES")
        self.assertEqual(receta_db.familia, "Pay")
        self.assertEqual(receta_db.categoria, "Pay Mediano")
        self.assertTrue(receta_db.codigos_point_aliases.filter(codigo_point_normalizado="sfresam").exists())
