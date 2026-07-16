from pathlib import Path

from django.test import SimpleTestCase


class LogisticaPwaCopyTests(SimpleTestCase):
    def test_ruta_planeada_indica_accion_del_chofer_para_activar_gps(self):
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")

        self.assertIn(
            "Ruta en planeación. Termina de revisar la carga e inicia la ruta con tu turno para activar el GPS.",
            html,
        )
        self.assertNotIn("cuando logística libere la ruta", html)
