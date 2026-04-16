from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from pos_bridge.services.point_file_download_service import PointFileDownloadService
from pos_bridge.utils.exceptions import ExtractionError


class PointFileDownloadServiceTests(SimpleTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.settings = SimpleNamespace(
            base_url="https://point.example.com",
            timeout_ms=5000,
            raw_exports_dir=Path(self.temp_dir.name),
        )

    def test_download_writes_file_and_closes_session(self):
        response = Mock()
        response.content = b"excel-bytes"
        response.headers = {"Content-Type": "application/vnd.ms-excel"}
        response.raise_for_status = Mock()

        requests_session = Mock()
        requests_session.get.return_value = response
        requests_session.close = Mock()
        auth_session = SimpleNamespace(session=requests_session)

        service = PointFileDownloadService(
            bridge_settings=self.settings,
            http_session_service=SimpleNamespace(create=Mock(return_value=auth_session)),
        )

        result = service.download(
            path_or_url="/Report/PrintReportes/",
            params={"idreporte": "3", "ext": "Excel"},
            branch_external_id="1",
            output_name="ventas.xls",
        )

        self.assertTrue(Path(result.output_path).exists())
        self.assertEqual(Path(result.output_path).read_bytes(), b"excel-bytes")
        self.assertIn("idreporte=3", result.request_url)
        self.assertEqual(result.content_type, "application/vnd.ms-excel")
        requests_session.close.assert_called_once()

    def test_download_rejects_different_host(self):
        service = PointFileDownloadService(
            bridge_settings=self.settings,
            http_session_service=SimpleNamespace(create=Mock()),
        )

        with self.assertRaises(ExtractionError):
            service.download(path_or_url="https://otro-host.example.com/file.xls")
