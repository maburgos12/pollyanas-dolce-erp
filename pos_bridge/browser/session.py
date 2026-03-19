from __future__ import annotations

from dataclasses import dataclass

from pos_bridge.browser.client import PlaywrightBrowserClient


@dataclass
class BrowserSession:
    client: PlaywrightBrowserClient
    context: object
    page: object


class BrowserSessionManager:
    def __init__(self, client: PlaywrightBrowserClient):
        self.client = client
        self._context = None
        self._page = None

    def __enter__(self) -> BrowserSession:
        self._context = self.client.new_context()
        self._page = self._context.new_page()
        self._page.set_default_timeout(self.client.settings.timeout_ms)
        return BrowserSession(client=self.client, context=self._context, page=self._page)

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._context is not None:
                self._context.close()
        finally:
            self.client.stop()
