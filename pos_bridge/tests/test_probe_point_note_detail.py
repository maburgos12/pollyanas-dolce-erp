from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.core.management import CommandError, call_command
from django.test import SimpleTestCase


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200
        self.text = json.dumps(payload)

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def __init__(self):
        self.requests = []
        self.closed = False

    def get(self, url, *, params, timeout):
        self.requests.append({"method": "GET", "url": url, "params": params, "timeout": timeout})
        if url.endswith("/get_encabezado_nota/"):
            return _FakeResponse(
                [
                    {
                        "Folio": "18452",
                        "Cliente": "Nombre privado",
                        "FK_Cliente": "customer-1",
                        "cookie": "session-secret",
                        "access_token": "access-secret",
                    }
                ]
            )
        return _FakeResponse(
            [
                {
                    "Codigo": "P001",
                    "Producto": "Producto sanitizado",
                    "Cantidad": 1,
                    "authorization": "Bearer secret",
                }
            ]
        )

    def close(self):
        self.closed = True


class _FakeHttpSessionService:
    session = _FakeSession()

    def __init__(self, _settings):
        return None

    def create(self, **_kwargs):
        return SimpleNamespace(session=self.session)


class ProbePointNoteDetailCommandTests(SimpleTestCase):
    def test_probe_requires_explicit_note_id(self):
        with self.assertRaisesRegex(CommandError, "--pk-nota"):
            call_command("probe_point_note_detail", output="/tmp/unused.json")

    @patch(
        "pos_bridge.management.commands.probe_point_note_detail.PointHttpSessionService",
        _FakeHttpSessionService,
    )
    @patch("pos_bridge.management.commands.probe_point_note_detail.load_point_bridge_settings")
    def test_probe_uses_existing_point_session_and_writes_only_sanitized_json(self, load_settings):
        load_settings.return_value = SimpleNamespace(base_url="https://point.test", timeout_ms=30000)
        output = Path(self._temp_dir()) / "detail.json"
        stdout = StringIO()

        call_command(
            "probe_point_note_detail",
            pk_nota="900001",
            folio="18452",
            sucursal="Matriz",
            output=str(output),
            stdout=stdout,
        )

        payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(payload["header"][0]["Cliente"], "***")
        self.assertEqual(payload["header"][0]["cookie"], "***")
        self.assertEqual(payload["header"][0]["access_token"], "***")
        self.assertEqual(payload["detail"][0]["authorization"], "***")
        self.assertNotIn("session-secret", output.read_text(encoding="utf-8"))
        self.assertNotIn("access-secret", output.read_text(encoding="utf-8"))
        self.assertNotIn("Bearer secret", output.read_text(encoding="utf-8"))
        self.assertNotIn("Nombre privado", output.read_text(encoding="utf-8"))
        self.assertNotIn("session-secret", stdout.getvalue())
        self.assertEqual(_FakeHttpSessionService.session.requests[0]["params"]["id_nota"], "900001")
        self.assertEqual(_FakeHttpSessionService.session.requests[1]["params"]["id_nota"], "900001")
        self.assertTrue(_FakeHttpSessionService.session.closed)

    def _temp_dir(self):
        import tempfile

        return tempfile.mkdtemp(prefix="probe-point-note-detail-")
