from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase


class LogisticaPwaTicketPhotoTemplateTests(SimpleTestCase):
    def test_combustible_ticket_photo_can_be_changed_before_upload(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "pwa.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn("function setFotoCargaCombustible(input)", source)
        self.assertIn("function limpiarFotoCargaCombustible()", source)
        self.assertIn('draft.carga_foto_ticket = null;', source)
        self.assertIn("Cambiar foto", source)
        self.assertIn("Quitar foto", source)
        self.assertIn('payload.append("foto_ticket", draft.carga_foto_ticket);', source)
