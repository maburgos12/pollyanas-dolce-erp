from __future__ import annotations

import json
from datetime import date, timezone
from types import SimpleNamespace

import requests
from django.test import SimpleTestCase

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.waste_extractor import PointWasteExtractor


class _FakeResponse:
    def __init__(self, payload):
        self.text = json.dumps(payload)

    def raise_for_status(self):
        return None


class _FakeSession:
    def get(self, url, params=None, timeout=None):
        if url.endswith("/Mermas/get_mermas"):
            return _FakeResponse(
                [
                    {
                        "PK_Movimiento": 1511362,
                        "Fecha": "2026-03-21T02:13:03.94",
                        "Sucursal": "EL TUNEL",
                        "Sucursal_corto": "EL TUNEL",
                        "Responsable": "Cesar Gastelum",
                        "Costo": 9.44,
                    }
                ]
            )
        if url.endswith("/Mermas/get_justificacion"):
            return _FakeResponse(
                [
                    {
                        "Fecha": "2026-03-21T02:13:03.94",
                        "Sucursal": "EL TUNEL",
                        "Costo": 9.44,
                        "Justificacion": "Merma desde la caja",
                    }
                ]
            )
        if url.endswith("/Mermas/get_detalle"):
            return _FakeResponse(
                [
                    {
                        "Articulo": "Bollo Zanahoria",
                        "Cantidad": 1.0,
                        "Unidad": "PZA",
                        "Costo_unitario": 9.435706,
                        "Costo_total": 9.44,
                    }
                ]
            )
        raise AssertionError(f"URL inesperada: {url}")


class _FakeHttpSessionService:
    def create(self):
        return SimpleNamespace(session=_FakeSession())


class _RetrySession(_FakeSession):
    def __init__(self, *, invalid_detail=False):
        self.invalid_detail = invalid_detail

    def get(self, url, params=None, timeout=None):
        if self.invalid_detail and url.endswith("/Mermas/get_detalle"):
            return _FakeResponse({"redirectToUrl": "/Account/Login"})
        return super().get(url, params=params, timeout=timeout)

    def close(self):
        return None


class _RetryHttpSessionService:
    def __init__(self):
        self.create_count = 0

    def create(self):
        self.create_count += 1
        return SimpleNamespace(
            session=_RetrySession(invalid_detail=self.create_count == 1),
        )


class _ReloginFailureHttpSessionService(_RetryHttpSessionService):
    def create(self):
        self.create_count += 1
        if self.create_count == 2:
            raise requests.HTTPError("Point no pudo seleccionar la cuenta")
        return SimpleNamespace(
            session=_RetrySession(invalid_detail=self.create_count == 1),
        )


class PointWasteExtractorTests(SimpleTestCase):
    def test_extract_treats_naive_point_timestamp_as_utc(self):
        extractor = PointWasteExtractor(
            bridge_settings=load_point_bridge_settings(),
            http_session_service=_FakeHttpSessionService(),
        )

        rows = extractor.extract(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].movement_at.tzinfo, timezone.utc)
        self.assertEqual(rows[0].movement_at.isoformat(), "2026-03-21T02:13:03.940000+00:00")

    def test_extract_reauthenticates_when_point_returns_non_list_payload(self):
        http_session_service = _RetryHttpSessionService()
        extractor = PointWasteExtractor(
            bridge_settings=load_point_bridge_settings(),
            http_session_service=http_session_service,
        )

        rows = extractor.extract(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(http_session_service.create_count, 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].item_name, "Bollo Zanahoria")

    def test_extract_retries_when_reauthentication_temporarily_fails(self):
        http_session_service = _ReloginFailureHttpSessionService()
        extractor = PointWasteExtractor(
            bridge_settings=load_point_bridge_settings(),
            http_session_service=http_session_service,
        )

        rows = extractor.extract(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(http_session_service.create_count, 3)
        self.assertEqual(len(rows), 1)
