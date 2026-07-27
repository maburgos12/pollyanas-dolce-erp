from __future__ import annotations

import json
import tempfile
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import requests
from django.core.management import CommandError, call_command
from django.test import SimpleTestCase

from pos_bridge.management.commands.probe_point_note_detail import discover_note_detail


VALID_HEADER = [
    {
        "Folio": "18452",
        "Cliente": "Nombre privado",
        "FK_Cliente": "customer-1",
        "FK_Sucursal": "branch-1",
        "Sucursal": "Matriz",
        "Importe": 565,
        "MotivoCancelacion": "texto libre privado",
        "unexpected_pii": "dato inesperado",
    }
]
VALID_DETAIL = [
    {
        "Codigo": "P001",
        "Producto": "Producto operativo",
        "Cantidad": 1,
        "Precio_Venta": 565,
        "Total": 565,
        "autoriza": "Nombre privado",
        "jsonDescuentos": '{"observacion":"texto libre"}',
        "unknown": "no debe salir",
    }
]


class _FakeResponse:
    def __init__(self, payload, *, text_plain=False, status_error=None):
        self.payload = payload
        self.status_error = status_error
        self.text = json.dumps(payload)
        self.text_plain = text_plain

    def raise_for_status(self):
        if self.status_error:
            raise self.status_error

    def json(self):
        if self.text_plain:
            raise requests.exceptions.JSONDecodeError("invalid", self.text, 0)
        return self.payload


class _FakeSession:
    def __init__(self, header=VALID_HEADER, detail=VALID_DETAIL, *, detail_text_plain=False):
        self.header = header
        self.detail = detail
        self.detail_text_plain = detail_text_plain
        self.requests = []
        self.closed = False
        self.response_errors = {}

    def get(self, url, *, params, timeout):
        self.requests.append({"method": "GET", "url": url, "params": params, "timeout": timeout})
        is_header = url.endswith("/get_encabezado_nota/")
        error = self.response_errors.get("header" if is_header else "detail")
        return _FakeResponse(
            self.header if is_header else self.detail,
            text_plain=self.detail_text_plain and not is_header,
            status_error=error,
        )

    def close(self):
        self.closed = True


def _service_class_for(session):
    class FakeHttpSessionService:
        def __init__(self, _settings):
            return None

        def create(self, **_kwargs):
            return SimpleNamespace(session=session)

    return FakeHttpSessionService


class ProbePointNoteDetailCommandTests(SimpleTestCase):
    def _settings(self, timeout_ms=30000):
        return SimpleNamespace(base_url="https://point.test", timeout_ms=timeout_ms)

    def _run_command(self, session, output, **kwargs):
        with (
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
                _service_class_for(session),
            ),
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings",
                return_value=self._settings(),
            ),
        ):
            call_command(
                "probe_point_note_detail",
                pk_nota="900001",
                folio="18452",
                sucursal="Matriz",
                output=str(output),
                stdout=StringIO(),
                **kwargs,
            )

    def test_probe_requires_explicit_note_id(self):
        with self.assertRaisesRegex(CommandError, "--pk-nota"):
            call_command("probe_point_note_detail", output="/tmp/unused.json")

    def test_probe_uses_existing_session_and_fail_closed_allowlist(self):
        session = _FakeSession()
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "detail.json"
            self._run_command(session, output)
            payload = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(payload["header"][0]["Sucursal"], "Matriz")
        self.assertEqual(payload["detail"][0]["Producto"], "Producto operativo")
        self.assertEqual(payload["header"][0]["Cliente"], "***")
        self.assertEqual(payload["header"][0]["MotivoCancelacion"], "***")
        self.assertEqual(payload["header"][0]["unexpected_pii"], "***")
        self.assertEqual(payload["detail"][0]["autoriza"], "***")
        self.assertEqual(payload["detail"][0]["jsonDescuentos"], "***")
        self.assertEqual(payload["detail"][0]["unknown"], "***")
        self.assertEqual(session.requests[0]["params"]["id_nota"], "900001")
        self.assertEqual(session.requests[0]["timeout"], 30)
        self.assertTrue(session.closed)

    def test_probe_parses_text_plain_json_detail(self):
        session = _FakeSession(detail_text_plain=True)
        with (
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
                _service_class_for(session),
            ),
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings",
                return_value=self._settings(),
            ),
        ):
            result = discover_note_detail(pk_nota="900001")

        self.assertEqual(result["classification"], "DIRECT_API")
        self.assertEqual(result["detail"][0]["Codigo"], "P001")

    def test_probe_uses_configured_timeout_without_arbitrary_minimum(self):
        session = _FakeSession()
        with (
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
                _service_class_for(session),
            ),
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings",
                return_value=self._settings(timeout_ms=2500),
            ),
        ):
            discover_note_detail(pk_nota="900001")

        self.assertEqual(session.requests[0]["timeout"], 2.5)

    def test_probe_rejects_invalid_header_shape_and_names_endpoint(self):
        session = _FakeSession(header={"Folio": "18452"})
        with (
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
                _service_class_for(session),
            ),
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings",
                return_value=self._settings(),
            ),
            self.assertRaisesRegex(CommandError, "cabecera.*get_encabezado_nota"),
        ):
            discover_note_detail(pk_nota="900001")

        self.assertTrue(session.closed)

    def test_probe_rejects_null_string_or_empty_detail(self):
        for invalid_detail in (None, "session expired", {}, [], [None], [VALID_DETAIL[0], {"Codigo": "P2"}]):
            with self.subTest(detail=invalid_detail):
                session = _FakeSession(detail=invalid_detail)
                with (
                    patch(
                        "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
                        _service_class_for(session),
                    ),
                    patch(
                        "pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings",
                        return_value=self._settings(),
                    ),
                    self.assertRaisesRegex(CommandError, "detalle.*get_detalle_nota"),
                ):
                    discover_note_detail(pk_nota="900001")

    def test_probe_names_detail_endpoint_on_http_error(self):
        session = _FakeSession()
        session.response_errors["detail"] = requests.HTTPError("500 Server Error")
        with (
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
                _service_class_for(session),
            ),
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings",
                return_value=self._settings(),
            ),
            self.assertRaisesRegex(CommandError, "detalle.*get_detalle_nota"),
        ):
            discover_note_detail(pk_nota="900001")

    def test_probe_rejects_invalid_json_and_names_detail_endpoint(self):
        session = _FakeSession(detail_text_plain=True)

        def invalid_detail_get(url, *, params, timeout):
            session.requests.append({"method": "GET", "url": url, "params": params, "timeout": timeout})
            if url.endswith("/get_encabezado_nota/"):
                return _FakeResponse(VALID_HEADER)
            response = _FakeResponse(VALID_DETAIL, text_plain=True)
            response.text = "<html>session expired</html>"
            return response

        session.get = invalid_detail_get
        with (
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
                _service_class_for(session),
            ),
            patch(
                "pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings",
                return_value=self._settings(),
            ),
            self.assertRaisesRegex(CommandError, "JSON inválido.*detalle.*get_detalle_nota"),
        ):
            discover_note_detail(pk_nota="900001")

        self.assertTrue(session.closed)

    @patch("pos_bridge.management.commands.probe_point_note_detail.os.replace")
    def test_atomic_output_preserves_existing_file_and_cleans_temp_on_failure(self, replace):
        replace.side_effect = OSError("replace failed")
        session = _FakeSession()
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "detail.json"
            output.write_text("previous", encoding="utf-8")

            with self.assertRaisesRegex(CommandError, "atómica"):
                self._run_command(session, output)

            self.assertEqual(output.read_text(encoding="utf-8"), "previous")
            self.assertEqual(list(Path(temp_dir).glob(".detail.json.*.tmp")), [])
