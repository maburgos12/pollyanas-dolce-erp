from __future__ import annotations

from io import BytesIO
from unittest.mock import patch

from django.core.mail import EmailMessage
from django.test import SimpleTestCase, override_settings

from config.email_backends import ResendEmailBackend


class FakeResponse:
    def __init__(self, body: bytes, status_code: int = 200):
        self.body = BytesIO(body)
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def getcode(self):
        return self.status_code

    def read(self):
        return self.body.read()


@override_settings(RESEND_API_KEY="test-key", RESEND_FROM="ERP <erp@pollyanasdolce.com>")
class ResendEmailBackendTests(SimpleTestCase):
    def test_send_message_stores_resend_response_on_message(self):
        message = EmailMessage(
            subject="Prueba",
            body="Contenido",
            from_email="erp@pollyanasdolce.com",
            to=["produccion.carolina@pollyanasdolce.com"],
        )
        backend = ResendEmailBackend()

        with patch(
            "config.email_backends.urllib.request.urlopen",
            return_value=FakeResponse(b'{"id":"email_123"}'),
        ):
            sent_count = backend.send_messages([message])

        self.assertEqual(sent_count, 1)
        self.assertEqual(message.resend_email_id, "email_123")
        self.assertEqual(message.resend_response, {"id": "email_123"})
