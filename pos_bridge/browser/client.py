from __future__ import annotations

from typing import Any

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.utils.exceptions import ConfigurationError


class PlaywrightBrowserClient:
    def __init__(self, bridge_settings: PointBridgeSettings | None = None):
        self.settings = bridge_settings or load_point_bridge_settings()
        self._playwright = None
        self._browser = None

    def start(self):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise ConfigurationError(
                "Playwright no está instalado en este entorno. Ejecuta `pip install -r requirements.txt` y "
                "`python -m playwright install chromium`.",
            ) from exc

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.settings.headless,
            slow_mo=self.settings.browser_slow_mo_ms,
        )
        return self._browser

    def stop(self) -> None:
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None

    def new_context(self) -> Any:
        browser = self._browser or self.start()
        context = browser.new_context(ignore_https_errors=True)
        return context
