from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from pos_bridge.browser.inventory_page import PointInventoryPage


class PointInventoryPageTests(SimpleTestCase):
    def test_list_branches_excludes_placeholder_option(self):
        page = Mock()
        settings = SimpleNamespace(selector_overrides={})
        inventory_page = PointInventoryPage(page, settings)
        branch_locator = Mock()
        branch_locator.locator.return_value.evaluate_all.return_value = [
            {"value": "", "label": "SELECCIONE UNA SUCURSAL"},
            {"value": "1", "label": "MATRIZ"},
        ]

        with patch("pos_bridge.browser.inventory_page.find_first", return_value=branch_locator):
            branches = inventory_page.list_branches()

        self.assertEqual(branches, [{"value": "1", "label": "MATRIZ"}])
