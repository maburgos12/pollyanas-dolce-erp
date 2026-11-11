from types import SimpleNamespace
from unittest.mock import Mock, call

from django.test import SimpleTestCase

from pos_bridge.services.inventory_extractor import PointInventoryExtractor
from pos_bridge.utils.exceptions import ExtractionError


class PointInventoryCategoryExtractionTests(SimpleTestCase):
    def setUp(self):
        self.extractor = PointInventoryExtractor(
            bridge_settings=SimpleNamespace(timeout_ms=30000)
        )
        self.page = Mock()
        self.page.list_category_options.return_value = [
            {"value": "21", "label": "OTROS POSTRES"},
            {"value": "22", "label": "PASTEL CHICO"},
        ]
        headers = ["Código", "Producto", "Cantidad", "Unidad", "Costo Unitario",
                   "Costo Total", "Último Movimiento", "Opciones"]
        # Point exposes the category in the hidden cells of each DataTables row.
        self.previous = {
            "headers": headers,
            "rows": [["734", "M0212", "GRAGEA CH", "Otros postres", "0"]],
        }
        self.current = {
            "headers": headers,
            "rows": [["924", "SFRESAPC", "TOPPING FRESA C", "Pastel Chico", "0"]],
        }

    def test_waits_for_requested_category_and_keeps_zero_stock(self):
        self.page.extract_inventory_table.side_effect = [self.previous, self.previous, self.current]

        rows, audit = self.extractor._extract_product_rows_by_category(self.page)

        self.assertEqual([(row["external_id"], row["stock"]) for row in rows],
                         [("734", 0), ("924", 0)])
        self.assertEqual([category["rows"] for category in audit], [1, 1])
        self.assertEqual(self.page.select_category.call_count, 2)

    def test_rejects_cut_if_table_never_switches(self):
        self.page.extract_inventory_table.return_value = self.previous

        self.page.page.url = "https://point.example/Stock/Existencias"

        def select(value, *, kind):
            if self.page.page.on.call_args is None:
                return
            listener = self.page.page.on.call_args.args[1]
            for url in (
                f"https://point.example/Stock/Productos?categoria={value}&token=private",
                "https://other.example/Stock/Productos?categoria=999",
                "https://point.example/Auth/Session?token=private",
            ):
                listener(SimpleNamespace(
                    url=url, status=200, request=SimpleNamespace(resource_type="xhr"),
                ))

        self.page.select_category.side_effect = select
        with self.assertRaisesMessage(ExtractionError, "PASTEL CHICO") as failure:
            self.extractor._extract_product_rows_by_category(self.page)

        evidence = failure.exception.context["category_trace"]
        self.assertEqual(evidence["requested_id"], "22")
        self.assertEqual([row["category_id"] for row in evidence["responses"]], ["22", "21", "22"])
        self.assertEqual([row["observed_categories"] for row in evidence["reads"]],
                         [["otros postres"]] * 8)
        self.assertNotIn("private", str(evidence))
        self.assertEqual(self.page.page.remove_listener.call_count, 2)

    def test_reselects_filter_if_point_keeps_previous_category(self):
        self.page.extract_inventory_table.side_effect = [self.previous] * 5 + [self.current]

        rows, audit = self.extractor._extract_product_rows_by_category(self.page)

        self.assertEqual([row["external_id"] for row in rows], ["734", "924"])
        self.assertEqual([category["rows"] for category in audit], [1, 1])
        self.assertEqual(self.page.select_category.call_args_list, [
            call("21", kind="products"), call("22", kind="products"),
            call("21", kind="products"), call("22", kind="products"),
        ])

    def test_accepts_point_selector_aliases_confirmed_in_actual_cuts(self):
        pairs = [
            ("VASO PREPARADO MINI", "Vasos Mini"),
            ("VASOS PREPARADOS GRANDE", "Vasos Grande"),
            ("VELAS SPARKLERS", "Vela Sparklers"),
        ]
        for selected, row_category in pairs:
            with self.subTest(selected=selected, row_category=row_category):
                self.page.list_category_options.return_value = [{"value": "10", "label": selected}]
                self.page.extract_inventory_table.return_value = {
                    "headers": self.current["headers"],
                    "rows": [["500", "00500", "Producto de prueba", row_category, "0"]],
                }
                rows, audit = self.extractor._extract_product_rows_by_category(self.page)
                self.assertEqual([(row["category"], row["stock"]) for row in rows], [(row_category, 0)])
                self.assertEqual(audit, [{"category": selected, "rows": 1}])

    def test_waits_again_when_category_is_initially_empty(self):
        self.page.extract_inventory_table.side_effect = [
            self.previous, {"headers": self.current["headers"], "rows": []}, self.current,
        ]

        rows, audit = self.extractor._extract_product_rows_by_category(self.page)

        self.assertEqual([row["external_id"] for row in rows], ["734", "924"])
        self.assertEqual(audit[1]["rows"], 1)
        self.assertEqual([read["rows"] for read in audit[1]["category_trace"]["reads"]], [0, 1])

    def test_accepts_empty_category_without_conflicting_rows(self):
        self.page.extract_inventory_table.side_effect = [
            self.previous, {"headers": self.current["headers"], "rows": []},
            {"headers": self.current["headers"], "rows": []},
        ]

        rows, audit = self.extractor._extract_product_rows_by_category(self.page)

        self.assertEqual([row["external_id"] for row in rows], ["734"])
        self.assertEqual([category["rows"] for category in audit], [1, 0])
