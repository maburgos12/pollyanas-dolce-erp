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

    @patch("pos_bridge.services.point_http_client.time.sleep", return_value=None)
    def test_login_reenters_when_workspace_token_returns_server_error(self, _sleep):
        audit_callback = Mock()
        client = PointHttpSessionClient(self._settings(retry_attempts=3), audit_callback=audit_callback)
        response = requests.Response()
        response.status_code = 500
        response.url = "https://app.pointmeup.com/Account/get_acctok"
        first_error = requests.HTTPError("500 Server Error", response=response)

        with patch.object(client, "_login_once", side_effect=[first_error, {"branch_name": "Matriz"}]) as login_once:
            with patch.object(client, "_reset_session") as reset_session:
                result = client.login(branch_hint="MATRIZ")

        self.assertEqual(result, {"branch_name": "Matriz"})
        self.assertEqual(login_once.call_count, 2)
        reset_session.assert_called_once()
        self.assertEqual(audit_callback.call_args.kwargs["event"], "point_relogin")
        self.assertEqual(audit_callback.call_args.kwargs["context"]["status_code"], 500)

    def test_get_product_stock_uses_stock_endpoint(self):
        client = PointHttpSessionClient(self._settings())
        response = Mock()
        response.json.return_value = [{"PK_Sucursal": 1, "Sucursal": "Matriz", "Cantidad": 18}]

        with patch.object(client, "_request", return_value=response) as request:
            stock = client.get_product_stock(2, timeout=2)

        self.assertEqual(stock[0]["Cantidad"], 18)
        request.assert_called_once_with("GET", "/Stock/get_productos_existencia", params={"pk": 2}, timeout=2)

    def test_get_stock_history_uses_complete_point_filter_contract(self):
        client = PointHttpSessionClient(self._settings())
        response = Mock()
        response.json.return_value = [{"FK_Movimiento": 123, "Existencia_nueva": 3}]

        with patch.object(client, "_request", return_value=response) as request:
            history = client.get_stock_history("857", "1")

        self.assertEqual(history[0]["FK_Movimiento"], 123)
        request.assert_called_once_with(
            "GET",
            "/Stock/GetHistorial",
            params={
                "tipo": "false",
                "almacen": "1",
                "pkproducto": "857",
                "movimientos": "500",
                "tipoMovimiento": "",
            },
        )

    def test_get_insumo_categories_uses_official_stock_catalog(self):
        client = PointHttpSessionClient(self._settings())
        response = Mock()
        response.json.return_value = [{"PK_Categoria_insumo": 12, "Categoria": "FRUTAS"}]

        with patch.object(client, "_request", return_value=response) as request:
            categories = client.get_insumo_categories(timeout=2)

        self.assertEqual(categories[0]["PK_Categoria_insumo"], 12)
        request.assert_called_once_with("GET", "/Catalogos/get_insumos", timeout=2)

    def test_get_branch_insumos_parses_double_encoded_official_payload(self):
        client = PointHttpSessionClient(self._settings())
        response = Mock()
        response.json.return_value = (
            '[{"PK_articulo":17,"Codigo":"017","Nombre":"Fresa Fresca",'
            '"Cantidad":53.47099999999971,"Unidad":"KG"}]'
        )

        with patch.object(client, "_request", return_value=response) as request:
            stock = client.get_branch_insumos(branch_id=2, category_id=12, timeout=2)

        self.assertEqual(stock[0]["Codigo"], "017")
        self.assertEqual(stock[0]["Cantidad"], 53.47099999999971)
        request.assert_called_once_with(
            "GET",
            "/Stock/GetInsumosPA",
            params={"almacen": 2, "categoria": 12},
            timeout=2,
        )
