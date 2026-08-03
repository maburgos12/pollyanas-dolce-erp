from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from django.test import SimpleTestCase

from pos_bridge.browser.login_page import PointLoginPage


class PointLoginPageTests(SimpleTestCase):
    def test_login_closes_visible_sat_modal_before_submit(self):
        page = Mock()
        modal = Mock()
        close_button = Mock()
        settings = SimpleNamespace(selector_overrides={}, timeout_ms=5000)
        login_page = PointLoginPage(page, settings)
        events = Mock()
        events.attach_mock(close_button.click, "close_modal")

        with (
            patch("pos_bridge.browser.login_page.fill_first"),
            patch("pos_bridge.browser.login_page.find_first", side_effect=[modal, close_button, None]),
            patch("pos_bridge.browser.login_page.click_first") as submit,
            patch("pos_bridge.browser.login_page.wait_for_any"),
        ):
            events.attach_mock(submit, "submit")
            login_page.login("usuario", "secreto")

        close_button.click.assert_called_once_with()
        modal.wait_for.assert_called_once_with(state="hidden", timeout=5000)
        submit.assert_called_once()
        self.assertEqual(events.mock_calls[:2], [call.close_modal(), call.submit(page, submit.call_args.args[1], "botón de login Point")])

    def test_login_retries_submit_when_sat_modal_appears_late(self):
        page = Mock()
        modal = Mock()
        close_button = Mock()
        settings = SimpleNamespace(selector_overrides={}, timeout_ms=5000)
        login_page = PointLoginPage(page, settings)

        with (
            patch("pos_bridge.browser.login_page.fill_first"),
            patch(
                "pos_bridge.browser.login_page.find_first",
                side_effect=[None, modal, close_button, None],
            ),
            patch(
                "pos_bridge.browser.login_page.click_first",
                side_effect=[RuntimeError("modal intercepts pointer events"), Mock()],
            ) as submit,
            patch("pos_bridge.browser.login_page.wait_for_any"),
        ):
            login_page.login("usuario", "secreto")

        self.assertEqual(submit.call_count, 2)
        close_button.click.assert_called_once_with()
        modal.wait_for.assert_called_once_with(state="hidden", timeout=5000)
