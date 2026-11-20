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

    def test_confirmed_branch_uses_the_selected_option_read_from_point(self):
        page = PointInventoryPage(Mock(), SimpleNamespace(selector_overrides={}, timeout_ms=30000))
        locator = Mock()
        locator.evaluate.return_value = {"value": "1", "label": "Matriz"}
        with patch("pos_bridge.browser.inventory_page.find_first", return_value=locator):
            selected = page.select_branch({"value": "1", "label": "Solicitud"}, require_confirmation=True)

        locator.select_option.assert_called_once_with(value="1")
        locator.evaluate.assert_called_once()
        self.assertEqual(selected, {"value": "1", "label": "Matriz"})
