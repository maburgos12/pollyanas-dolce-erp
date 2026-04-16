from __future__ import annotations

from pos_bridge.browser.login_page import PointLoginPage
from pos_bridge.browser.workspace_page import PointWorkspacePage


class PointAuthService:
    def __init__(self, bridge_settings):
        self.settings = bridge_settings

    def login(self, session, *, branch_hint: str | None = None) -> dict:
        login_page = PointLoginPage(session.page, self.settings)
        login_page.open()
        login_page.login(self.settings.username, self.settings.password)
        # Point puede dejar la navegación en una pantalla intermedia tras el login.
        # Forzamos la vista de workspaces antes de intentar seleccionar sucursal.
        if self.settings.base_url:
            session.page.goto(f"{self.settings.base_url.rstrip('/')}/Account/workSpaces", wait_until="domcontentloaded")
        workspace_page = PointWorkspacePage(session.page, self.settings)
        return workspace_page.select_workspace(branch_hint=branch_hint)
