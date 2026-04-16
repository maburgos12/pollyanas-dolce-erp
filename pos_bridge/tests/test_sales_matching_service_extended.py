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

    def test_is_descriptive_product_name_accepts_single_token_when_category_present(self):
        service = PointSalesMatchingService()
        self.assertTrue(service.is_descriptive_product_name(point_name="Americano", family="", category="Café"))

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

    def test_create_missing_product_recipe_sets_servicio_for_pillines(self):
        service = PointSalesMatchingService()
        receta = service.create_missing_product_recipe(
            codigo_point="0179",
            point_name="Letrero Chispas Felicidades",
            category="Pillines",
            family="Accesorios",
        )

        self.assertEqual(receta.modo_costeo, Receta.MODO_COSTEO_SERVICIO)

    def test_create_missing_product_recipe_sets_reventa_for_beverage_rows(self):
        service = PointSalesMatchingService()
        receta = service.create_missing_product_recipe(
            codigo_point="TE01",
            point_name="Te Chai",
            category="Te",
            family="Bebidas",
        )

        self.assertEqual(receta.modo_costeo, Receta.MODO_COSTEO_REVENTA)

    def test_create_missing_product_recipe_keeps_service_for_extra_even_if_not_otherwise_recipe(self):
        service = PointSalesMatchingService()
        receta = service.create_missing_product_recipe(
            codigo_point="0320",
            point_name="Extra 100",
            category="Otros postres",
            family="",
        )

        self.assertEqual(receta.modo_costeo, Receta.MODO_COSTEO_SERVICIO)

    def test_infer_non_recipe_bucket_separates_resale_service_and_accessory(self):
        service = PointSalesMatchingService()

        self.assertEqual(
            service.infer_non_recipe_bucket(
                {"sku": "TE01", "name": "Te Chai", "category": "Te", "family": "Bebidas"}
            ),
            "REVENTA",
        )
        self.assertEqual(
            service.infer_non_recipe_bucket(
                {"sku": "SVC01", "name": "Servicio de domicilio", "category": "Servicios", "family": ""}
            ),
            "SERVICIO",
        )
        self.assertEqual(
            service.infer_non_recipe_bucket(
                {"sku": "VELA1", "name": "Letrero Chispas", "category": "Pillines", "family": "Accesorios"}
            ),
            "ACCESORIO",
        )

    def test_infer_non_recipe_bucket_marks_catalog_outside_resale_items(self):
        service = PointSalesMatchingService()

        self.assertEqual(
            service.infer_non_recipe_bucket(
                {"sku": "0237", "name": "Agua Clarita 500ml", "category": "Clarita", "family": ""}
            ),
            "REVENTA",
        )
