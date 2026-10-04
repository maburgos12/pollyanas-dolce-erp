"""Recuperación privada del cliente; no modifica datos operativos."""
import json
import re
import subprocess
from types import SimpleNamespace

from django.conf import settings
from django.template.loader import render_to_string
from django.test import RequestFactory, SimpleTestCase


class CapturaRecuperacionClienteTests(SimpleTestCase):
    def test_motor_real_recupera_formulario_con_csrf_vigente_y_adjuntos_identicos(self):
        result = subprocess.run(
            ["node", "activos/tests_capturas_web_harness.js"],
            cwd=settings.BASE_DIR, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_contexto_privado_contiene_actor_y_superficie_sin_token(self):
        request = RequestFactory().get("/activos/ordenes/")
        request.user = SimpleNamespace(pk=37)
        html = render_to_string(
            "activos/_captura_contexto.html",
            {"captura_superficie": "ordenes", "request": request},
        )
        match = re.search(r'<script id="captura-contexto" type="application/json">(.*?)</script>', html)
        self.assertEqual(json.loads(match.group(1)), {"actor": "37", "superficie": "ordenes"})
        self.assertNotIn("csrfmiddlewaretoken", html)
