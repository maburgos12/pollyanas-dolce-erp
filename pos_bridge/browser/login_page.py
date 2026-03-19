from __future__ import annotations

from pos_bridge.selectors.login_selectors import (
    ERROR_BANNERS,
    PASSWORD_INPUTS,
    SUBMIT_BUTTONS,
    SUCCESS_LANDMARKS,
    USERNAME_INPUTS,
)
from pos_bridge.browser.waits import click_first, fill_first, find_first, wait_for_any
from pos_bridge.utils.exceptions import AuthenticationError
from pos_bridge.utils.helpers import select_candidates


class PointLoginPage:
    def __init__(self, page, bridge_settings):
        self.page = page
        self.settings = bridge_settings

    def open(self) -> None:
        if not self.settings.base_url:
            raise AuthenticationError("Falta POINT_BASE_URL para abrir el portal Point.")
        self.page.goto(self.settings.base_url, wait_until="domcontentloaded")

    def login(self, username: str, password: str) -> None:
        if not username or not password:
            raise AuthenticationError("Faltan POINT_USERNAME y/o POINT_PASSWORD.")

        fill_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "login.username_input", USERNAME_INPUTS),
            username,
            "input de usuario Point",
        )
        fill_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "login.password_input", PASSWORD_INPUTS),
            password,
            "input de contraseña Point",
        )
        click_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "login.submit_button", SUBMIT_BUTTONS),
            "botón de login Point",
        )
        try:
            self.page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
        except Exception:
            pass

        error_banner = find_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "login.error_banner", ERROR_BANNERS),
            timeout_ms=500,
        )
        if error_banner is not None:
            raise AuthenticationError(
                "Point rechazó el login.",
                context={"error_text": error_banner.inner_text()},
            )

        wait_for_any(
            self.page,
            select_candidates(self.settings.selector_overrides, "login.success_landmark", SUCCESS_LANDMARKS),
            "indicador de sesión autenticada",
            timeout_ms=self.settings.timeout_ms,
        )
