from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from pos_bridge.browser.workspace_page import PointWorkspacePage
from pos_bridge.utils.exceptions import NavigationError


class PointWorkspacePageTests(SimpleTestCase):
    def _settings(self):
        return SimpleNamespace(timeout_ms=30000, selector_overrides={})

    def test_select_workspace_executes_point_workspace_function(self):
        page = Mock()
        page.url = "https://app.pointmeup.com/Account/workSpaces"

        def _evaluate(script, arg):
            if "eval(targetOnclick)" in script:
                page.url = "https://app.pointmeup.com/Home/Index"
                return None
            raise AssertionError(f"Unexpected evaluate call: {script}")

        page.evaluate.side_effect = _evaluate

        workspace_page = PointWorkspacePage(page, self._settings())
        workspace_page.wait_until_loaded = Mock()
        workspace_page.list_workspaces = Mock(
            return_value=[
                {
                    "onclick": 'selWS("83852AED-D4FB-E611-814F-06B55B5505BA",1,"Matriz")',
                    "text": "Matriz",
                    "containerText": "Matriz",
                }
            ]
        )

        result = workspace_page.select_workspace(branch_hint="MATRIZ")

        self.assertEqual(result["workspace_label"], "Matriz")
        page.wait_for_load_state.assert_called_once()
        page.wait_for_function.assert_called_once()
        self.assertIn("eval(targetOnclick)", page.evaluate.call_args.args[0])

    def test_select_workspace_raises_if_point_does_not_change_context(self):
        page = Mock()
        page.url = "https://app.pointmeup.com/Account/workSpaces"
        page.wait_for_function.side_effect = TimeoutError("no navigation")

        workspace_page = PointWorkspacePage(page, self._settings())
        workspace_page.wait_until_loaded = Mock()
        workspace_page.list_workspaces = Mock(
            return_value=[
                {
                    "onclick": 'selWS("83852AED-D4FB-E611-814F-06B55B5505BA",1,"Matriz")',
                    "text": "Matriz",
                    "containerText": "Matriz",
                }
            ]
        )

        with self.assertRaises(NavigationError):
            workspace_page.select_workspace(branch_hint="MATRIZ")
