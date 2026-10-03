from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import requests
from django.test import SimpleTestCase
from django.utils import timezone as django_timezone

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.waste_extractor import PointWasteExtractor
from pos_bridge.utils.exceptions import ExtractionError


class _FakeResponse:
    def __init__(self, payload):
        self.text = json.dumps(payload)

    def raise_for_status(self):
        return None


class _FakeSession:
    def close(self):
        return None

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


class _TrackedSession(_FakeSession):
    def __init__(self, movements, *, fail_path=None):
        self.movements = movements
        self.fail_path = fail_path
        self.calls = []
        self.close_count = 0

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if self.fail_path and url.endswith(self.fail_path):
            raise requests.HTTPError("Point no devolvió el reporte")
        if url.endswith("/Mermas/get_mermas"):
            movements = self.movements(params) if callable(self.movements) else self.movements
            return _FakeResponse(movements)
        return super().get(url, params=params, timeout=timeout)

    def close(self):
        self.close_count += 1


class _TrackedSessionService:
    def __init__(self, *sessions):
        self.sessions = iter(sessions)
        self.create_count = 0

    def create(self):
        self.create_count += 1
        session = next(self.sessions)
        if isinstance(session, Exception):
            raise session
        return SimpleNamespace(session=session)


@patch("pos_bridge.services.waste_extractor.write_json_file")
class PointWasteExtractorBoundaryTests(SimpleTestCase):
    def _movement(self, pk, timestamp):
        return {
            "PK_Movimiento": pk,
            "Fecha": timestamp,
            "Sucursal": "EL TUNEL",
            "Sucursal_corto": "EL TUNEL",
            "Responsable": "Cesar Gastelum",
            "Costo": 9.44,
        }

    def _extractor(self, *sessions, attempts=1):
        settings = load_point_bridge_settings()
        settings = replace(settings, retry_attempts=attempts)
        service = _TrackedSessionService(*sessions)
        return PointWasteExtractor(bridge_settings=settings, http_session_service=service)

    def test_recovers_initial_day_by_querying_calendar_margin(self, write_json):
        movement = self._movement(1683114, "2026-09-27T19:01:00")
        threshold = int(datetime(2026, 9, 26, 7, tzinfo=timezone.utc).timestamp() * 1000)
        session = _TrackedSession(lambda params: [movement] if int(params["fechaini"]) <= threshold else [])
        extractor = self._extractor(session)

        rows = extractor.extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 28))

        self.assertEqual([row.movement_external_id for row in rows], ["1683114"])
        params = session.calls[0][1]
        self.assertEqual(params["fechaini"], str(extractor._to_epoch_ms(date(2026, 9, 26))))
        self.assertEqual(params["fechafin"], str(extractor._to_epoch_ms(date(2026, 9, 29))))

    def test_filters_operational_dates_before_fetching_details(self, write_json):
        session = _TrackedSession([
            self._movement(1, "2026-09-27T06:59:59Z"),
            self._movement(2, "2026-09-27T07:00:00Z"),
            self._movement(3, "2026-09-29T06:59:59Z"),
            self._movement(4, "2026-09-29T07:00:00Z"),
        ])

        rows = self._extractor(session).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 28))

        self.assertEqual([row.movement_external_id for row in rows], ["2", "3"])
        detail_ids = [params["pk_movimiento"] for url, params in session.calls if url.endswith("/get_detalle")]
        self.assertEqual(detail_ids, ["2", "3"])
        exported = write_json.call_args.args[1]["movements"]
        self.assertEqual([item["movement"]["PK_Movimiento"] for item in exported], [2, 3])

    def test_aware_timestamp_filters_using_mazatlan_even_with_active_utc(self, write_json):
        session = _TrackedSession([
            self._movement(1, "2026-09-27T23:59:59-07:00"),
            self._movement(2, "2026-09-28T00:00:00-07:00"),
        ])
        with django_timezone.override("UTC"):
            rows = self._extractor(session).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        self.assertEqual([row.movement_external_id for row in rows], ["1"])
        self.assertEqual(rows[0].movement_at.isoformat(), "2026-09-27T23:59:59-07:00")

    def test_duplicate_movement_pk_only_fetches_once(self, write_json):
        movement = self._movement(1, "2026-09-27T19:01:00")
        session = _TrackedSession([movement, dict(movement)])
        rows = self._extractor(session).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        self.assertEqual(len(rows), 1)
        self.assertEqual(len([url for url, _ in session.calls if url.endswith("/get_detalle")]), 1)

    def test_invalid_date_aborts_without_writing_partial_export(self, write_json):
        session = _TrackedSession([self._movement(1, "2026-09-27T19:01:00"), self._movement(2, "not-a-date")])
        with self.assertRaises(Exception) as raised:
            self._extractor(session).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        self.assertIsInstance(raised.exception, ExtractionError)
        write_json.assert_not_called()
        self.assertEqual(session.close_count, 1)

    def test_conflicting_duplicate_pk_aborts_without_export(self, write_json):
        first = self._movement(1, "2026-09-27T19:01:00")
        second = dict(first, Costo=123)
        session = _TrackedSession([first, second])
        with self.assertRaises(ExtractionError):
            self._extractor(session).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        write_json.assert_not_called()

    def test_inverted_range_rejected_before_authentication(self, write_json):
        extractor = self._extractor(_TrackedSession([]))
        with self.assertRaises(ExtractionError):
            extractor.extract(start_date=date(2026, 9, 28), end_date=date(2026, 9, 27))
        self.assertEqual(extractor.http_session_service.create_count, 0)

    def test_success_closes_session(self, write_json):
        session = _TrackedSession([])
        self._extractor(session).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        self.assertEqual(session.close_count, 1)

    def test_terminal_failure_closes_reauthenticated_session(self, write_json):
        first = _TrackedSession([], fail_path="/get_mermas")
        second = _TrackedSession([], fail_path="/get_mermas")
        with self.assertRaises(ExtractionError):
            self._extractor(first, second, attempts=2).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        self.assertEqual(first.close_count, 1)
        self.assertEqual(second.close_count, 1)

    def test_success_closes_original_and_reauthenticated_sessions(self, write_json):
        first = _TrackedSession([], fail_path="/get_mermas")
        second = _TrackedSession([self._movement(1, "2026-09-27T19:01:00")])
        rows = self._extractor(first, second, attempts=2).extract(
            start_date=date(2026, 9, 27), end_date=date(2026, 9, 27)
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(first.close_count, 1)
        self.assertEqual(second.close_count, 1)

    def test_export_failure_closes_session(self, write_json):
        session = _TrackedSession([])
        write_json.side_effect = OSError("disk unavailable")
        with self.assertRaises(OSError):
            self._extractor(session).extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        self.assertEqual(session.close_count, 1)

    def test_relogin_failure_closes_original_session(self, write_json):
        first = _TrackedSession([], fail_path="/get_mermas")
        extractor = self._extractor(first, requests.HTTPError("login failed"), requests.HTTPError("login failed"), attempts=2)
        with patch("pos_bridge.services.waste_extractor.time_module.sleep"):
            with self.assertRaises(ExtractionError):
                extractor.extract(start_date=date(2026, 9, 27), end_date=date(2026, 9, 27))
        self.assertEqual(first.close_count, 1)


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
