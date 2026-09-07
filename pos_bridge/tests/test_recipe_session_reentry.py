"""Lost Point sessions must resume recipe reads, never masquerade as empty data."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import requests
from django.test import SimpleTestCase

from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.utils.exceptions import ExtractionError


class RecipeSessionReentryTests(SimpleTestCase):
    reads = (
        ("get_product_detail", {"product_id": 824}, {"Codigo": "SFRESAG"}),
        ("get_articulos", {"search": "DC0508", "category": 7}, [{"PK_Articulo": 1}]),
        ("get_articulo_detail", {"articulo_id": 1}, {"Codigo_Articulo": "DC0508"}),
    )

    def make_client(self):
        client = PointHttpSessionClient(SimpleNamespace(
            base_url="https://example.invalid", username="test", password="test",
            timeout_ms=5000, retry_attempts=2,
        ))
        client._last_branch_hint = "Guamuchil"
        self.addCleanup(client.close)
        return client

    def response(self, payload, *, html=False):
        response = requests.Response()
        response.status_code = 200
        response._content = payload.encode() if html else json.dumps(payload).encode()
        return response

    def test_each_recipe_read_reenters_and_repeats_identical_query(self):
        for method, kwargs, expected in self.reads:
            for expired in (
                self.response("<html>Sesión Expirada</html>", html=True),
                self.response({"error": "session expired"}),
                self.response({"redirectToUrl": "/Account/SignIn"}),
            ):
                with self.subTest(method=method, expired=expired.text):
                    client = self.make_client()
                    old_session = client.session
                    with patch.object(client, "_request", side_effect=[expired, self.response(expected)]) as request, patch.object(client, "login") as login:
                        self.assertEqual(getattr(client, method)(**kwargs), expected)
                    login.assert_called_once_with(branch_hint="Guamuchil")
                    self.assertIsNot(client.session, old_session)
                    self.assertEqual(request.call_count, 2)
                    self.assertEqual(request.call_args_list[0], request.call_args_list[1])

    def test_http_unauthorized_also_reenters(self):
        for method, kwargs, expected in self.reads:
            with self.subTest(method=method):
                client = self.make_client()
                response = self.response({})
                response.status_code = 401
                error = requests.HTTPError(response=response)
                with patch.object(client, "_request", side_effect=[error, self.response(expected)]), patch.object(client, "login") as login:
                    self.assertEqual(getattr(client, method)(**kwargs), expected)
                login.assert_called_once_with(branch_hint="Guamuchil")

    def test_expired_session_never_becomes_empty_success_or_unlimited_retry(self):
        for method, kwargs, _ in self.reads:
            with self.subTest(method=method):
                client = self.make_client()
                with patch.object(client, "_request", return_value=self.response({"error": "session expired"})) as request, patch.object(client, "login") as login:
                    with self.assertRaises(ExtractionError):
                        getattr(client, method)(**kwargs)
                self.assertEqual(request.call_count, 2)
                self.assertEqual(login.call_count, 1)

    def test_valid_empty_article_search_does_not_reauthenticate(self):
        client = self.make_client()
        with patch.object(client, "_request", return_value=self.response([])), patch.object(client, "login") as login:
            self.assertEqual(client.get_articulos(search="unknown"), [])
        login.assert_not_called()

    def test_reads_do_not_swallow_job_deadline(self):
        for method, kwargs, _ in self.reads:
            with self.subTest(method=method):
                client = self.make_client()
                with patch.object(client, "_request", side_effect=TimeoutError("deadline")), patch.object(client, "login") as login:
                    with self.assertRaises(TimeoutError):
                        getattr(client, method)(**kwargs)
                login.assert_not_called()
