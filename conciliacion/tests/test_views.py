from __future__ import annotations

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings

from conciliacion.models import ImportacionBancaria
from sat_client.models import LogDescargaSat
from syncfy_client.models import CuentaBancaria, MovimientoBancario


class ConciliacionBancariaViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_conciliacion_view",
            email="admin-view@example.com",
            password="x",
        )
        self.cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BBVA,
            nombre_display="BBVA Empresas",
            id_site_syncfy="site-bbva",
            numero_cuenta="00741744000120753084",
        )
        self.client.force_login(self.user)

    def test_get_bancaria_renders_upload_screen(self):
        response = self.client.get("/conciliacion/bancaria/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Conciliacion bancaria")
        self.assertContains(response, "BBVA Empresas")

    @override_settings(SAT_DESCARGA_ENABLED=True)
    def test_get_bancaria_shows_sat_error_status_when_last_log_failed(self):
        LogDescargaSat.objects.create(nivel=LogDescargaSat.NIVEL_ERROR, mensaje="Error SAT: HTTP 500")

        response = self.client.get("/conciliacion/bancaria/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Descarga SAT con error")
        self.assertNotContains(response, "Descarga SAT activa")

    def test_preview_and_confirm_import_movements(self):
        archivo = SimpleUploadedFile(
            "bbva.csv",
            "Fecha,Descripcion,Monto,Referencia\n2026-06-09,DEPOSITO CLIENTE,900.00,R1\n".encode("utf-8"),
            content_type="text/csv",
        )

        preview_response = self.client.post(
            "/conciliacion/bancaria/",
            {"action": "preview", "cuenta": self.cuenta.pk, "archivo": archivo},
        )
        self.assertEqual(preview_response.status_code, 200)
        self.assertContains(preview_response, "DEPOSITO CLIENTE")

        confirm_response = self.client.post("/conciliacion/bancaria/", {"action": "confirm"})

        self.assertEqual(confirm_response.status_code, 302)
        self.assertEqual(MovimientoBancario.objects.count(), 1)
        self.assertEqual(ImportacionBancaria.objects.count(), 1)
