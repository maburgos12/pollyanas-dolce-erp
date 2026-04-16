from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests
from django.test import SimpleTestCase

from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.utils.exceptions import ExtractionError


class PointHttpSessionClientTests(SimpleTestCase):
    def _settings(self, *, retry_attempts: int = 3):
        return SimpleNamespace(
            base_url="https://app.pointmeup.com",
            username="user@example.com",
            password="secret",
            timeout_ms=30000,
            retry_attempts=retry_attempts,
        )

    @patch("pos_bridge.services.point_http_client.time.sleep", return_value=None)
    def test_request_retries_transient_request_exception(self, _sleep):
        audit_callback = Mock()
        client = PointHttpSessionClient(self._settings(retry_attempts=3), audit_callback=audit_callback)
        response = Mock(status_code=200)
        response.raise_for_status = Mock()
        client.session.request = Mock(
            side_effect=[
                requests.exceptions.ConnectionError("Connection aborted."),
                response,
            ]
        )

        returned = client._request("GET", "/Catalogos/get_productos")

        self.assertIs(returned, response)
        self.assertEqual(client.session.request.call_count, 2)
        audit_callback.assert_called_once()
        self.assertEqual(audit_callback.call_args.kwargs["event"], "point_http_retry")

    @patch("pos_bridge.services.point_http_client.time.sleep", return_value=None)
    def test_request_retries_server_error_before_success(self, _sleep):
        audit_callback = Mock()
        client = PointHttpSessionClient(self._settings(retry_attempts=3), audit_callback=audit_callback)
        error_response = Mock(status_code=500)
        error_response.raise_for_status = Mock()
        ok_response = Mock(status_code=200)
        ok_response.raise_for_status = Mock()
        client.session.request = Mock(side_effect=[error_response, ok_response])

        returned = client._request("GET", "/Catalogos/get_productos")

        self.assertIs(returned, ok_response)
        self.assertEqual(client.session.request.call_count, 2)
        audit_callback.assert_called_once()
        self.assertEqual(audit_callback.call_args.kwargs["event"], "point_http_retry")

    @patch("pos_bridge.services.point_http_client.time.sleep", return_value=None)
    def test_login_retries_when_point_returns_session_expired_html(self, _sleep):
        audit_callback = Mock()
        client = PointHttpSessionClient(self._settings(retry_attempts=3), audit_callback=audit_callback)
        first_error = ExtractionError(
            "Point devolvió una respuesta no JSON en workspaces Point.",
            context={"body_preview": "<title>Sesión Expirada - Point</title>"},
        )

        with patch.object(client, "_login_once", side_effect=[first_error, {"branch_name": "Matriz"}]) as login_once:
            with patch.object(client, "_reset_session") as reset_session:
                result = client.login(branch_hint="MATRIZ")

        self.assertEqual(result, {"branch_name": "Matriz"})
        self.assertEqual(login_once.call_count, 2)
        reset_session.assert_called_once()
        audit_callback.assert_called_once()
        self.assertEqual(audit_callback.call_args.kwargs["event"], "point_relogin")
