from django.test import SimpleTestCase

from pos_bridge.services.normalizer_service import PointNormalizerService


class PointNormalizerServiceTests(SimpleTestCase):
    def setUp(self):
        self.service = PointNormalizerService()

    def test_normalize_inventory_row_uses_sku_as_fallback_external_id(self):
        result = self.service.normalize_inventory_row(
            {
                "external_id": "",
                "sku": "SKU-001",
                "name": "Pastel Chocolate",
                "stock": "10.5",
                "min_stock": "2",
                "max_stock": "20",
            }
        )
        self.assertEqual(result["external_id"], "SKU-001")
        self.assertEqual(str(result["stock"]), "10.5")
        self.assertEqual(str(result["min_stock"]), "2")
        self.assertEqual(str(result["max_stock"]), "20")
