from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch
import json

import requests
from django.test import SimpleTestCase

from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.utils.exceptions import ExtractionError


class PointHttpSessionClientTests(SimpleTestCase):
    def test_full_catalog_uses_families_without_losing_products_above_150(self):
        client = PointHttpSessionClient(self._settings())
        client._ENUM_SEEDS = "x"
        client._ENUM_REFINE = "x"
        catalog = [{"PK_Producto": i, "ID_Familia": 1 if i < 100 else 2} for i in range(220)]
        def request(method, path, **kwargs):
            if path == "/Catalogos/get_familias":
                data = [{"ID_Familia": 1}, {"ID_Familia": 2}]
            else:
                params = kwargs.get("params", {})
                family = params.get("familia")
                data = [r for r in catalog if family is None or str(r["ID_Familia"]) == str(family)][:150]
            response = requests.Response()
            response.status_code = 200
            response._content = json.dumps(data).encode()
            return response
        with patch.object(client, "_request", side_effect=request) as fetch:
            rows = client.get_all_products()
            self.assertEqual(len(rows), 220)
            self.assertLessEqual(fetch.call_count, 4)
            self.assertEqual(client.get_all_products(), rows)
            self.assertLessEqual(fetch.call_count, 4)

    def test_product_catalog_recovers_expired_session_in_same_workspace(self):
        client = PointHttpSessionClient(self._settings())
        client._last_branch_hint = "MATRIZ"
        expired = requests.Response()
        expired.status_code = 200
        expired._content = b"<title>Session Expired</title>"
        valid = requests.Response()
        valid.status_code = 200
        valid._content = b'[{"PK_Producto": 7}]'
        with patch.object(client, "_request", side_effect=[expired, valid]), patch.object(client, "login") as login:
            self.assertEqual(client.get_products(text_art="NUEVO"), [{"PK_Producto": 7}])
        login.assert_called_once_with(branch_hint="MATRIZ")

    def test_product_catalog_rejects_json_error_object(self):
        client = PointHttpSessionClient(self._settings(retry_attempts=1))
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"error":"session expired"}'
        with patch.object(client, "_request", return_value=response), self.assertRaises(ExtractionError):
            client.get_products()

    def test_catalog_recovery_has_bounded_attempts(self):
        client = PointHttpSessionClient(self._settings(retry_attempts=2))
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        with patch.object(client, "_request", return_value=response) as request, patch.object(client, "login"):
            with self.assertRaises(ExtractionError):
                client.get_products()
        self.assertEqual(request.call_count, 2)

    def test_saturated_family_keeps_initial_rows_when_refining(self):
        client = PointHttpSessionClient(self._settings())
        initial = [{"PK_Producto": i, "ID_Familia": 1} for i in range(150)]
        second = [{"PK_Producto": i, "ID_Familia": 2} for i in range(150, 300)]
        extra = {"PK_Producto": 300, "ID_Familia": 2}
        def products(**kwargs):
            return second if kwargs.get("familia") == "2" else initial
        def enumerate_rows(fetch, **kwargs):
            return initial if kwargs["label"].endswith("1") else [extra]
        with patch.object(client, "get_products", side_effect=products), patch.object(client, "_catalog_rows", return_value=[{"ID_Familia": 1}, {"ID_Familia": 2}]), patch.object(client, "_enumerate_catalog", side_effect=enumerate_rows):
            self.assertEqual(len(client.get_all_products()), 301)

    def test_product_bom_recovers_session_without_treating_html_as_empty(self):
        client = PointHttpSessionClient(self._settings(retry_attempts=2))
        expired = requests.Response()
        expired.status_code = 200
        expired._content = b"<html>Session Expired</html>"
        valid = requests.Response()
        valid.status_code = 200
        valid._content = b'[{"PK_Articulo": 1, "Cantidad": 2}]'
        with patch.object(client, "_request", side_effect=[expired, valid]), patch.object(client, "login"):
            self.assertEqual(client.get_product_bom(7), [{"PK_Articulo": 1, "Cantidad": 2}])

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
