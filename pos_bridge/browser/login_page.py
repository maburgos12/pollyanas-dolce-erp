from __future__ import annotations

from pos_bridge.selectors.login_selectors import (
    BLOCKING_MODAL_CONTAINERS,
    BLOCKING_MODAL_DISMISS_BUTTONS,
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

    def _dismiss_blocking_modal(self) -> bool:
        modal = find_first(self.page, BLOCKING_MODAL_CONTAINERS, timeout_ms=500)
        if modal is None:
            return False

        dismiss_button = find_first(self.page, BLOCKING_MODAL_DISMISS_BUTTONS, timeout_ms=500)
        if dismiss_button is None:
            raise AuthenticationError(
                "Point mostró un aviso que bloquea el login y no se encontró cómo cerrarlo.",
                context={"modal_selectors": BLOCKING_MODAL_CONTAINERS},
            )
        try:
            dismiss_button.click()
            modal.wait_for(state="hidden", timeout=self.settings.timeout_ms)
        except Exception as exc:
            raise AuthenticationError(
                "No se pudo cerrar el aviso que bloquea el login de Point.",
                context={"error": str(exc)},
            ) from exc
        return True

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
        self._dismiss_blocking_modal()
        submit_selectors = select_candidates(
            self.settings.selector_overrides,
            "login.submit_button",
            SUBMIT_BUTTONS,
        )
        try:
            click_first(self.page, submit_selectors, "botón de login Point")
        except Exception:
            if not self._dismiss_blocking_modal():
                raise
            click_first(self.page, submit_selectors, "botón de login Point")
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
