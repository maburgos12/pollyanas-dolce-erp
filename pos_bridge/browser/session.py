from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass

from pos_bridge.browser.client import PlaywrightBrowserClient


@dataclass
class BrowserSession:
    client: PlaywrightBrowserClient
    context: object
    page: object


class BrowserSessionManager:
    """Sesión de navegador contra Point, serializada por cuenta.

    Point invalida la sesión anterior cuando la misma cuenta inicia sesión de nuevo,
    así que el candado se toma aquí y no en cada caller: la exclusión tiene que durar
    lo que dura la sesión, no solo el login, y depender de que siete puntos de entrada
    se acuerden de tomarlo ya falló en producción (2026-09-05, una extracción de
    compras murió en el login porque asistencias y ventas intradía arrancaron ambas
    en la misma hora en punto).

    El candado es reentrante dentro del mismo backend de PostgreSQL, así que los
    callers que ya lo tomaban siguen siendo correctos.
    """

    def __init__(self, client: PlaywrightBrowserClient):
        self.client = client
        self._context = None
        self._page = None
        self._stack: ExitStack | None = None

    def __enter__(self) -> BrowserSession:
        from pos_bridge.services.point_account_session_lock import point_account_session_lock

        stack = ExitStack()
        try:
            # Antes de abrir el navegador: si otra sincronización tiene la cuenta, esta
            # espera en vez de robarle la sesión a media corrida.
            stack.enter_context(point_account_session_lock(wait=True))
            self._context = self.client.new_context()
            self._page = self._context.new_page()
            self._page.set_default_timeout(self.client.settings.timeout_ms)
        except BaseException:
            stack.close()
            raise
        self._stack = stack
        return BrowserSession(client=self.client, context=self._context, page=self._page)

    def __exit__(self, exc_type, exc, tb):
        try:
            try:
                if self._context is not None:
                    self._context.close()
            finally:
                self.client.stop()
        finally:
            # El candado se suelta al final: mientras el navegador siga vivo, la cuenta
            # sigue ocupada.
            if self._stack is not None:
                self._stack.close()
                self._stack = None
