"""Regresiones del JavaScript real de las capturas móviles, sin usar datos operativos."""
import subprocess
from django.conf import settings
from django.test import SimpleTestCase


class FechaCapturaPWAJavaScriptTests(SimpleTestCase):
    def test_fecha_erp_en_capturas_y_reintentos(self):
        result = subprocess.run(
            ["node", "mantenimiento/tests_fecha_pwa_harness.js"],
            cwd=settings.BASE_DIR, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_restauracion_con_dom_instalado_y_captura_congelada(self):
        result = subprocess.run(
            ["node", "mantenimiento/tests_capturas_pwa_harness.js"],
            cwd=settings.BASE_DIR, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
