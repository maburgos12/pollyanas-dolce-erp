"""La sesión de navegador contra Point debe estar serializada por cuenta."""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from pos_bridge.browser.session import BrowserSessionManager


class _FakeClient:
    def __init__(self, eventos):
        self.eventos = eventos
        self.settings = SimpleNamespace(timeout_ms=1000)

    def new_context(self):
        self.eventos.append("abre_navegador")
        return SimpleNamespace(
            new_page=lambda: SimpleNamespace(set_default_timeout=lambda _ms: None),
            close=lambda: self.eventos.append("cierra_contexto"),
        )

    def stop(self):
        self.eventos.append("detiene_navegador")


class BrowserSessionLockTests(TestCase):
    def _patch_lock(self, eventos):
        @contextmanager
        def candado_espia(*, wait):
            eventos.append(("candado", wait))
            try:
                yield True
            finally:
                eventos.append("liberado")

        return patch(
            "pos_bridge.services.point_account_session_lock.point_account_session_lock",
            candado_espia,
        )

    def test_el_candado_envuelve_toda_la_sesion(self):
        eventos = []
        with self._patch_lock(eventos):
            with BrowserSessionManager(_FakeClient(eventos)):
                eventos.append("trabajo")

        # El candado se toma ANTES de abrir el navegador y se suelta DESPUÉS de
        # cerrarlo: mientras el navegador viva, la cuenta Point sigue ocupada.
        self.assertEqual(
            eventos,
            [
                ("candado", True),
                "abre_navegador",
                "trabajo",
                "cierra_contexto",
                "detiene_navegador",
                "liberado",
            ],
        )

    def test_libera_el_candado_si_el_trabajo_falla(self):
        eventos = []
        with self._patch_lock(eventos):
            with self.assertRaises(RuntimeError):
                with BrowserSessionManager(_FakeClient(eventos)):
                    raise RuntimeError("falla a media sesión")

        self.assertEqual(eventos[-1], "liberado")

    def test_libera_el_candado_si_el_navegador_no_abre(self):
        eventos = []

        class ClienteRoto(_FakeClient):
            def new_context(self):
                raise RuntimeError("no hay navegador")

        with self._patch_lock(eventos):
            with self.assertRaises(RuntimeError):
                with BrowserSessionManager(ClienteRoto(eventos)):
                    pass

        self.assertEqual(eventos, [("candado", True), "liberado"])
