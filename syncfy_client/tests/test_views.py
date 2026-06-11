from __future__ import annotations

import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from syncfy_client.models import CuentaBancaria


class SyncfyBancosViewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser(
            username="admin_syncfy",
            email="admin_syncfy@example.com",
            password="x",
        )
        self.user = user_model.objects.create_user(
            username="operativo_syncfy",
            email="operativo_syncfy@example.com",
            password="x",
        )
        self.bbva = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BBVA,
            nombre_display="BBVA Empresas",
            id_site_syncfy="66cdeccb04d89a0ea654d887",
        )
        self.banbajio = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="BanBajio Empresas",
            id_site_syncfy="66cdedb80f446e12e023441d",
        )

    def test_bancos_requires_login(self):
        response = self.client.get("/syncfy/bancos/")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response["Location"])

    def test_bancos_rejects_non_admin_user(self):
        self.client.force_login(self.user)

        response = self.client.get("/syncfy/bancos/")

        self.assertEqual(response.status_code, 403)

    @patch("syncfy_client.views.obtener_token", return_value="token-123")
    def test_bancos_admin_loads_widget_context(self, _token):
        self.client.force_login(self.admin)

        response = self.client.get("/syncfy/bancos/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "BBVA Empresas")
        self.assertContains(response, "66cdeccb04d89a0ea654d887")
        self.assertContains(response, "token-123")
        self.assertNotContains(response, "American Express Business Gold")

    def test_guardar_credential_updates_expected_bank(self):
        self.client.force_login(self.admin)

        response = self.client.post(
            "/syncfy/bancos/bbva/credential/",
            data=json.dumps({"id_credential": "cred-bbva-1", "id_site": self.bbva.id_site_syncfy}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.bbva.refresh_from_db()
        self.assertEqual(self.bbva.id_credential, "cred-bbva-1")
        self.assertEqual(response.json()["cuenta"]["estado_syncfy"], "Credencial guardada")

    def test_guardar_credential_rejects_site_mismatch(self):
        self.client.force_login(self.admin)

        response = self.client.post(
            "/syncfy/bancos/bbva/credential/",
            data=json.dumps({"id_credential": "cred-wrong", "id_site": self.banbajio.id_site_syncfy}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.bbva.refresh_from_db()
        self.assertFalse(self.bbva.id_credential)

    @patch("syncfy_client.views.obtener_token", return_value="token-123")
    @patch(
        "syncfy_client.views.listar_credenciales",
        return_value=[
            {
                "id_credential": "cred-old",
                "id_site": "66cdeccb04d89a0ea654d887",
                "is_authorized": 0,
                "dt_authorized": 10,
            },
            {
                "id_credential": "cred-bbva-ok",
                "id_site": "66cdeccb04d89a0ea654d887",
                "is_authorized": 1,
                "code": 200,
                "dt_authorized": 20,
            },
        ],
    )
    def test_sincronizar_credenciales_links_best_site_match(self, _credentials, _token):
        self.client.force_login(self.admin)

        response = self.client.post("/syncfy/bancos/sincronizar-credenciales/")

        self.assertEqual(response.status_code, 200)
        self.bbva.refresh_from_db()
        self.assertEqual(self.bbva.id_credential, "cred-bbva-ok")
        self.assertEqual(response.json()["actualizadas"], 1)
        self.assertEqual(response.json()["autorizadas"], 1)

    @patch("syncfy_client.views.obtener_token", return_value="token-123")
    @patch(
        "syncfy_client.views.listar_credenciales",
        return_value=[
            {
                "id_credential": "cred-bbva-denied",
                "id_site": "66cdeccb04d89a0ea654d887",
                "is_authorized": 0,
                "code": 401,
            },
        ],
    )
    def test_sincronizar_credenciales_reports_unauthorized_status(self, _credentials, _token):
        self.client.force_login(self.admin)

        response = self.client.post("/syncfy/bancos/sincronizar-credenciales/")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["no_autorizadas"], 1)
        bbva_payload = next(cuenta for cuenta in payload["cuentas"] if cuenta["banco"] == "bbva")
        self.assertEqual(bbva_payload["estado_syncfy"], "No autorizado (401)")
