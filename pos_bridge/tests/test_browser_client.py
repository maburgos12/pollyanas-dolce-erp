from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from pos_bridge.browser.client import PlaywrightBrowserClient


class PlaywrightBrowserClientTests(SimpleTestCase):
    def test_start_stops_playwright_when_browser_launch_fails(self):
        playwright = SimpleNamespace(
            chromium=SimpleNamespace(launch=Mock(side_effect=RuntimeError("missing browser"))),
            stop=Mock(),
        )
        sync_playwright = Mock(return_value=SimpleNamespace(start=Mock(return_value=playwright)))
        sync_api = ModuleType("playwright.sync_api")
        sync_api.sync_playwright = sync_playwright

        with patch.dict(sys.modules, {"playwright.sync_api": sync_api}):
            client = PlaywrightBrowserClient()
            with self.assertRaisesRegex(RuntimeError, "missing browser"):
                client.start()

        playwright.stop.assert_called_once_with()
        self.assertIsNone(client._playwright)
        self.assertIsNone(client._browser)

    def test_docker_image_installs_playwright_browser_by_default(self):
        dockerfile = Path(__file__).resolve().parents[2] / "Dockerfile"

        self.assertIn("ARG INSTALL_PLAYWRIGHT_BROWSER=1", dockerfile.read_text())
