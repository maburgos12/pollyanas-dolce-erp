from django.test import TestCase

from recetas.models import Receta, RecetaCodigoPointAlias
from pos_bridge.services.sales_matching_service import PointSalesMatchingService


class PointSalesMatchingServiceTests(TestCase):
    def setUp(self):
        self.receta = Receta.objects.create(
            nombre="Pay de Queso Mediano",
            codigo_point="0002",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pay-queso-mediano",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=self.receta,
            codigo_point="PAYQUESO-M",
            nombre_point="Pay de Queso Mediano",
            activo=True,
        )
        self.service = PointSalesMatchingService()

    def test_resolve_receta_matches_alias_code(self):
        receta = self.service.resolve_receta(codigo_point="PAYQUESO-M", point_name="")
        self.assertIsNotNone(receta)
        self.assertEqual(receta.id, self.receta.id)

    def test_is_non_recipe_sale_row_detects_accessories(self):
        self.assertTrue(
            self.service.is_non_recipe_sale_row(
                {
                    "family": "Velas",
                    "category": "Granmark",
                    "name": "VELA METALICA No. 2 DORADO",
                    "sku": "7186/2",
                }
            )
        )
