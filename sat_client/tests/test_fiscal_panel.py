from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.test import TestCase

from sat_client.models import SolicitudDocumentoSat


class SatFiscalPanelTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_sat_fiscal",
            email="sat-fiscal@example.com",
            password="x",
        )
        self.client.force_login(self.user)

    def test_panel_renders_sat_document_automation_actions(self):
        response = self.client.get("/sat/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Automatizaciones SAT")
        self.assertContains(response, "Descargar constancia actual")
        self.assertContains(response, "Descargar opinión")
        self.assertContains(response, "Revisar buzón")
        self.assertContains(response, "Ver conciliación")
        self.assertNotContains(response, "Actualizar CFDI")
        self.assertNotContains(response, "Guardar PDF")

    def test_document_request_button_marks_missing_configuration_as_error(self):
        response = self.client.post(
            "/sat/",
            {"tipo": SolicitudDocumentoSat.TIPO_CONSTANCIA},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        solicitud = SolicitudDocumentoSat.objects.get()
        self.assertEqual(solicitud.tipo, SolicitudDocumentoSat.TIPO_CONSTANCIA)
        self.assertEqual(solicitud.estado, SolicitudDocumentoSat.ESTADO_ERROR)
        self.assertContains(response, solicitud.mensaje)

    @override_settings(
        SAT_EFIRMA_CER_PATH="/tmp/fake.cer",
        SAT_EFIRMA_KEY_PATH="/tmp/fake.key",
        SAT_EFIRMA_PASSWORD="secret",
        SAT_RFC="PDO000000000",
    )
    def test_document_request_button_marks_pending_connector_as_pending(self):
        response = self.client.post(
            "/sat/",
            {"tipo": SolicitudDocumentoSat.TIPO_CONSTANCIA},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        solicitud = SolicitudDocumentoSat.objects.get()
        self.assertEqual(solicitud.tipo, SolicitudDocumentoSat.TIPO_CONSTANCIA)
        self.assertEqual(solicitud.estado, SolicitudDocumentoSat.ESTADO_PENDIENTE)
        self.assertContains(response, "Pendiente")
        self.assertContains(response, solicitud.mensaje)
