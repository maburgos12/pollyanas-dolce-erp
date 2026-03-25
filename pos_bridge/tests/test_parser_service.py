from django.test import SimpleTestCase

from pos_bridge.services.parser_service import PointInventoryParserService


class PointInventoryParserServiceTests(SimpleTestCase):
    def test_parse_inventory_table_handles_point_hidden_columns(self):
        parser = PointInventoryParserService()

        payload = {
            "headers": [
                "Código",
                "Producto",
                "Cantidad",
                "Unidad",
                "Costo Unitario",
                "Costo Total",
                "Último Movimiento",
                "Opciones",
            ],
            "rows": [
                [
                    "879",
                    "VHAZUL",
                    "VELA HUMO AZUL",
                    "Vela Sparklers",
                    "-1",
                    "PZA",
                    "0",
                    "0",
                    "2025-09-20T23:42:00.27",
                    "false",
                ],
                [
                    "463",
                    "VHROSA",
                    "VELA HUMO ROSA",
                    "Vela Sparklers",
                    "20",
                    "PZA",
                    "0",
                    "0",
                    "2025-10-10T18:53:02.133",
                    "false",
                ],
            ],
        }

        parsed = parser.parse_inventory_table(payload, branch_external_id="2", branch_name="CRUCERO")

        self.assertEqual(parsed["items"][0]["external_id"], "879")
        self.assertEqual(parsed["items"][0]["sku"], "VHAZUL")
        self.assertEqual(parsed["items"][0]["name"], "VELA HUMO AZUL")
        self.assertEqual(parsed["items"][0]["category"], "Vela Sparklers")
        self.assertEqual(parsed["items"][0]["stock"], "-1")
        self.assertEqual(parsed["items"][1]["stock"], "20")

    def test_parse_inventory_table_keeps_standard_layout(self):
        parser = PointInventoryParserService()

        payload = {
            "headers": ["Código", "Producto", "Cantidad", "Mínimo", "Máximo"],
            "rows": [["0001", "Pay de Queso Grande", "7", "1", "10"]],
        }

        parsed = parser.parse_inventory_table(payload, branch_external_id="1", branch_name="MATRIZ")

        self.assertEqual(parsed["items"][0]["external_id"], "0001")
        self.assertEqual(parsed["items"][0]["sku"], "0001")
        self.assertEqual(parsed["items"][0]["name"], "Pay de Queso Grande")
        self.assertEqual(parsed["items"][0]["stock"], "7")
        self.assertEqual(parsed["items"][0]["min_stock"], "1")
        self.assertEqual(parsed["items"][0]["max_stock"], "10")
