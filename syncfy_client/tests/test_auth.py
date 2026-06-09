from __future__ import annotations

from django.test import SimpleTestCase

from syncfy_client.services.auth import obtener_token
from syncfy_client.services.base import SyncfyClient, SyncfyConfig
from syncfy_client.services.usuarios import crear_usuario_pollyanas


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append({"method": method, "url": url, **kwargs})
        return self.response


class SyncfyAuthTests(SimpleTestCase):
    def test_obtener_token_posts_api_key_and_user(self):
        session = FakeSession(FakeResponse({"response": {"token": "token-123"}}))
        client = SyncfyClient(
            config=SyncfyConfig(api_key="api-key", id_user="user-1", base_url="https://syncfy.test/v1"),
            session=session,
        )

        token = obtener_token(client=client)

        self.assertEqual(token, "token-123")
        self.assertEqual(session.requests[0]["method"], "POST")
        self.assertEqual(session.requests[0]["url"], "https://syncfy.test/v1/sessions")
        self.assertEqual(session.requests[0]["json"], {"api_key": "api-key", "id_user": "user-1"})

    def test_crear_usuario_uses_api_key_authorization(self):
        session = FakeSession(FakeResponse({"response": {"id_user": "user-1", "name": "pollyanas_dolce"}}))
        client = SyncfyClient(
            config=SyncfyConfig(api_key="api-key", id_user="", base_url="https://syncfy.test/v1"),
            session=session,
        )

        id_user = crear_usuario_pollyanas(client=client)

        self.assertEqual(id_user, "user-1")
        self.assertEqual(session.requests[0]["headers"]["Authorization"], "api_key api-key")
        self.assertEqual(session.requests[0]["json"], {"name": "pollyanas_dolce"})
