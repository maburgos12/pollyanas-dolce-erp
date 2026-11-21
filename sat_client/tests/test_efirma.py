from datetime import timedelta
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.utils import timezone
from lxml import etree


RFC = "AAA010101AAA"
PASSWORD = "synthetic-key-password"


def certificate_pair(*, rfc=RFC, starts=None, ends=None, key=None):
    key = key or rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = timezone.now()
    name = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "Prueba sintética"),
        x509.NameAttribute(NameOID.X500_UNIQUE_IDENTIFIER, rfc),
    ])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(starts or now - timedelta(days=1))
            .not_valid_after(ends or now + timedelta(days=30)).sign(key, hashes.SHA256()))
    return cert.public_bytes(serialization.Encoding.DER), key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(PASSWORD.encode()))


@override_settings(SAT_RFC=RFC, SAT_EFIRMA_CER_PATH="", SAT_EFIRMA_KEY_PATH="", SAT_EFIRMA_PASSWORD="")
class EfirmaTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.cer, cls.key = certificate_pair()
        cls.dg = get_user_model().objects.create_user(username="direccion", password="x")
        cls.dg.groups.add(Group.objects.get_or_create(name="DG")[0])

    def setUp(self):
        self.client.force_login(self.dg)

    def data(self, cer=None, key=None, password=PASSWORD):
        return {"accion": "actualizar_efirma", "password": password,
                "certificado": SimpleUploadedFile("firma.cer", cer or self.cer),
                "llave": SimpleUploadedFile("firma.key", key or self.key)}

    def test_direccion_has_form(self):
        response = self.client.get("/sat/")
        self.assertContains(response, "Validar y activar e.firma")
        self.assertContains(response, 'enctype="multipart/form-data"')

    def test_admin_staff_and_readers_cannot_manage_even_by_post(self):
        for group, staff in [("ADMIN", True), ("LECTURA", False), ("ADMIN", False)]:
            user = get_user_model().objects.create_user(username=f"{group}{staff}", is_staff=staff)
            user.groups.add(Group.objects.get_or_create(name=group)[0])
            self.client.force_login(user)
            with self.subTest(group=group, staff=staff):
                self.assertNotContains(self.client.get("/sat/"), "Validar y activar e.firma")
                self.assertEqual(self.client.post("/sat/", self.data()).status_code, 403)

    @patch("sat_client.services.efirma.obtener_token", return_value="private-token")
    def test_activate_encrypts_and_shared_loader_and_signer_use_uploaded_credentials(self, token):
        from sat_client.models import ConfiguracionEfirma
        from sat_client.services.base import get_sat_credentials
        from sat_client.services.autenticacion import build_autenticacion_envelope
        response = self.client.post("/sat/", self.data(), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        saved = ConfiguracionEfirma.objects.get(pk=1)
        self.assertNotIn(PASSWORD.encode(), bytes(saved.contenido_cifrado))
        self.assertNotIn(self.key, bytes(saved.contenido_cifrado))
        credentials = get_sat_credentials()
        self.assertEqual(credentials.rfc, RFC)
        self.assertEqual(credentials.cer_data, self.cer)
        self.assertEqual(credentials.key_data, self.key)
        self.assertIn(b"SignatureValue", etree.tostring(build_autenticacion_envelope()))
        self.assertNotIn(PASSWORD, repr(credentials))
        self.assertNotIn("private-token", response.content.decode())
        self.assertIn("<details open>", response.json()["html"])
        self.assertEqual(saved.actualizado_por, self.dg)
        token.assert_called_once()

    @patch("sat_client.services.efirma.obtener_token", return_value="token")
    def test_invalid_candidates_do_not_call_sat_or_replace_active_credentials(self, token):
        from sat_client.models import ConfiguracionEfirma
        self.client.post("/sat/", self.data())
        original = bytes(ConfiguracionEfirma.objects.get().contenido_cifrado)
        token.reset_mock()
        expired, _ = certificate_pair(ends=timezone.now() - timedelta(hours=1))
        future, _ = certificate_pair(starts=timezone.now() + timedelta(days=1))
        other, _ = certificate_pair(rfc="BBB010101BBB")
        for payload in [self.data(password="wrong"), self.data(cer=b"invalid"),
                        self.data(cer=expired), self.data(cer=future), self.data(cer=other),
                        self.data(key=certificate_pair()[1]), self.data(cer=b"x" * 20000)]:
            with self.subTest():
                response = self.client.post("/sat/", payload, HTTP_ACCEPT="application/json")
                self.assertEqual(response.status_code, 400)
                self.assertFalse(response.json()["ok"])
                self.assertEqual(bytes(ConfiguracionEfirma.objects.get().contenido_cifrado), original)
                self.assertNotIn(PASSWORD, response.content.decode())
        token.assert_not_called()

    @patch("sat_client.services.efirma.obtener_token", return_value="token")
    def test_sat_failure_keeps_previous_and_does_not_echo_fault_secrets(self, token):
        from sat_client.models import ConfiguracionEfirma
        from sat_client.services.base import SatServiceError
        self.client.post("/sat/", self.data())
        original = bytes(ConfiguracionEfirma.objects.get().contenido_cifrado)
        token.side_effect = SatServiceError("Certificado Revocado o Caduco " + PASSWORD)
        response = self.client.post("/sat/", self.data(), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("revocado", response.json()["toast"]["message"].lower())
        self.assertNotIn(PASSWORD, response.content.decode())
        self.assertEqual(bytes(ConfiguracionEfirma.objects.get().contenido_cifrado), original)

    @patch("sat_client.services.efirma.obtener_token", return_value="token")
    def test_test_current_credentials_records_failure_without_replacing(self, token):
        from sat_client.models import ConfiguracionEfirma
        from sat_client.services.base import SatServiceError
        self.client.post("/sat/", self.data())
        token.side_effect = SatServiceError("HTTP 503")
        response = self.client.post("/sat/", {"accion": "probar_efirma"}, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(ConfiguracionEfirma.objects.get().ultima_prueba_ok)
        page = self.client.get("/sat/")
        self.assertFalse(page.context["efirma"]["ultima_prueba_ok"])
        self.assertContains(page, "Última prueba SAT fallida")

    def test_csrf_required(self):
        from django.test import Client
        from sat_client.models import ConfiguracionEfirma
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.dg)
        self.assertRedirects(client.post("/sat/", self.data()), "/login/", fetch_redirect_response=False)
        self.assertFalse(ConfiguracionEfirma.objects.exists())

    @override_settings(FILE_UPLOAD_MAX_MEMORY_SIZE=1)
    @patch("sat_client.services.efirma.obtener_token", return_value="token")
    def test_upload_never_spools_private_key_to_disk_and_valid_csrf_works(self, token):
        from django.test import Client
        from sat_client.models import ConfiguracionEfirma
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.dg)
        client.get("/sat/")
        data = self.data()
        data["csrfmiddlewaretoken"] = client.cookies["csrftoken"].value
        with patch("django.core.files.uploadhandler.TemporaryUploadedFile", side_effect=AssertionError("secret on disk")):
            response = client.post("/sat/", data, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(ConfiguracionEfirma.objects.exists())

    @patch("sat_client.services.efirma.obtener_token", return_value="token")
    def test_corrupt_cipher_fails_closed_and_secret_rotation_uses_fallback(self, token):
        from sat_client.models import ConfiguracionEfirma
        from sat_client.services.base import SatConfigurationError, get_sat_credentials
        with override_settings(SECRET_KEY="old-test-only"):
            self.client.force_login(self.dg)
            self.client.post("/sat/", self.data())
        with override_settings(SECRET_KEY="new-test-only", SECRET_KEY_FALLBACKS=["old-test-only"]):
            self.assertEqual(get_sat_credentials().cer_data, self.cer)
        with override_settings(SECRET_KEY="new-test-only", SECRET_KEY_FALLBACKS=[]):
            with self.assertRaises(SatConfigurationError):
                get_sat_credentials()
        ConfiguracionEfirma.objects.update(contenido_cifrado=b"corrupt")
        with self.assertRaises(SatConfigurationError):
            get_sat_credentials()

    @patch("sat_client.services.efirma.obtener_token", return_value="token")
    def test_environment_test_tracks_certificate_without_activating_or_copying_secrets(self, token):
        import tempfile
        from pathlib import Path
        from sat_client.models import ConfiguracionEfirma
        from sat_client.services.efirma import estado_efirma
        with tempfile.TemporaryDirectory() as directory:
            cer = Path(directory) / "synthetic.cer"
            key = Path(directory) / "synthetic.key"
            cer.write_bytes(self.cer)
            key.write_bytes(self.key)
            with override_settings(SAT_EFIRMA_CER_PATH=str(cer), SAT_EFIRMA_KEY_PATH=str(key), SAT_EFIRMA_PASSWORD=PASSWORD):
                response = self.client.post("/sat/", {"accion": "probar_efirma"}, HTTP_ACCEPT="application/json")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["target"], "#sat-efirma-status")
                self.assertNotIn('type="file"', response.json()["html"])
                self.assertTrue(estado_efirma()["ultima_prueba_ok"])
                self.assertNotIn(PASSWORD, response.content.decode())
        self.assertFalse(ConfiguracionEfirma.objects.exists())

    @patch("sat_client.services.efirma.obtener_token", return_value="token")
    def test_html_fallback_returns_to_anchor_and_does_not_repopulate_password(self, token):
        response = self.client.post("/sat/", self.data(password="wrong"))
        self.assertContains(response, "no se pudo abrir")
        self.assertNotContains(response, 'value="wrong"')
        response = self.client.post("/sat/", self.data())
        self.assertRedirects(response, "/sat/#sat-efirma")
