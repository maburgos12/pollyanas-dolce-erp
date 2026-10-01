from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from pos_bridge.services.production_entry_extractor import PointProductionEntryExtractor


class PointProductionEntryExtractorTests(SimpleTestCase):
    @patch("pos_bridge.services.production_entry_extractor.write_json_file")
    def test_retries_transient_empty_list_and_production_detail_error(self, _write_json_file):
        production = {
            "FK_Produccion": 23220,
            "Sucursal": "CEDIS",
            "Fecha": "2026-09-30T00:00:00",
            "Usuario": "Julissa Angulo",
        }
        detail = {
            "PK_Produccion_detalle": 88012,
            "Codigo": "01SGM01",
            "Nombre": "Surtido Galletas Mini",
            "Unidad": "PZA",
            "Precio_default": 13.59,
            "Cantidad_solicitada": 15,
            "Cantidad_producida": 15,
            "IsInsumo": False,
        }
        sessions = [Mock(), Mock(), Mock()]
        sessions[0].get.return_value = Mock(text=json.dumps([]), raise_for_status=Mock())
        sessions[1].get.side_effect = [
            Mock(text=json.dumps([production]), raise_for_status=Mock()),
            Mock(
                text=json.dumps(
                    {
                        "error": True,
                        "message": "The value's length for key 'initial catalog' exceeds its limit of '128'.",
                        "data": None,
                    }
                ),
                raise_for_status=Mock(),
            ),
        ]
        sessions[2].get.return_value = Mock(text=json.dumps([detail]), raise_for_status=Mock())
        session_service = Mock()
        session_service.create.side_effect = [SimpleNamespace(session=session) for session in sessions]
        settings = SimpleNamespace(
            base_url="https://app.pointmeup.com",
            timeout_ms=30000,
            retry_attempts=2,
            raw_exports_dir=Path("/tmp"),
        )

        rows = PointProductionEntryExtractor(
            bridge_settings=settings,
            http_session_service=session_service,
        ).extract(start_date=date(2026, 9, 1), end_date=date(2026, 9, 30))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].production_external_id, "23220")
        self.assertEqual(rows[0].detail_external_id, "88012")
        self.assertEqual(session_service.create.call_count, 3)
