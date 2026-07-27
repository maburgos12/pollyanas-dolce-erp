from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone as datetime_timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import requests
from django.test import SimpleTestCase
from django.utils import timezone

from pos_bridge.services.point_note_detail_service import (
    PointNoteContractError,
    PointNoteDetailError,
    PointNoteDetailService,
    PointNoteIntegrityError,
    PointNoteNotFoundError,
    PointNoteUnavailableError,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "point_note_detail.json"
_UNSET = object()


class _FakeResponse:
    def __init__(self, payload, *, text_plain=False):
        self._payload = payload
        self.text = json.dumps(payload)
        self.status_code = 200
        self._text_plain = text_plain

    def raise_for_status(self):
        return None

    def json(self):
        if self._text_plain:
            raise requests.exceptions.JSONDecodeError("invalid", self.text, 0)
        return self._payload


class _FakeSession:
    def __init__(self, header, detail):
        self.header = header
        self.detail = detail
        self.requests = []
        self.closed = False
        self.invalid_detail_json = False

    def get(self, url, *, params, timeout):
        self.requests.append({"url": url, "params": params, "timeout": timeout})
        if url.endswith("/get_encabezado_nota/"):
            return _FakeResponse(self.header)
        response = _FakeResponse(self.detail, text_plain=True)
        if self.invalid_detail_json:
            response.text = "<html>session expired</html>"
        return response

    def close(self):
        self.closed = True


class _FakeHttpSessionService:
    def __init__(self, session):
        self.session = session

    def create(self):
        return SimpleNamespace(session=self.session)


class PointNoteDetailServiceTests(SimpleTestCase):
    def setUp(self):
        fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        self.header = fixture["header"]
        self.detail = fixture["detail"]

    def _service(self, *, header=_UNSET, detail=_UNSET, timeout_ms=2500):
        session = _FakeSession(
            deepcopy(self.header if header is _UNSET else header),
            deepcopy(self.detail if detail is _UNSET else detail),
        )
        settings = SimpleNamespace(base_url="https://point.test", timeout_ms=timeout_ms)
        service = PointNoteDetailService(
            bridge_settings=settings,
            http_session_service=_FakeHttpSessionService(session),
        )
        return service, session

    def test_fetch_returns_immutable_canonical_note_from_confirmed_endpoints(self):
        service, session = self._service()

        note = service.fetch(pk_nota="900001")

        self.assertEqual(note.pk_nota, "900001")
        self.assertEqual(note.folio, "18452")
        self.assertEqual(note.branch_name, "Matriz")
        self.assertTrue(timezone.is_aware(note.sold_at))
        self.assertEqual(timezone.localtime(note.sold_at).replace(tzinfo=None), datetime(2026, 7, 27, 12, 40))
        self.assertEqual(note.total, Decimal("375.01"))
        self.assertEqual(note.total.as_tuple().exponent, -2)
        self.assertFalse(note.invoiced)
        self.assertEqual(note.payment_type, "CONTADO")
        self.assertEqual(note.customer_external_id, "customer-001")
        self.assertEqual(note.lines[0].point_code, "P001")
        self.assertEqual(note.lines[0].discount, Decimal("19.99"))
        self.assertEqual(note.lines[0].line_total, Decimal("375.01"))
        self.assertEqual(note.lines[0].unit_price.as_tuple().exponent, -2)
        self.assertEqual(
            note.source_endpoint,
            "/Clientes/get_detalle_nota/",
        )
        self.assertEqual(
            [request["url"] for request in session.requests],
            [
                "https://point.test/Clientes/get_encabezado_nota/",
                "https://point.test/Clientes/get_detalle_nota/",
            ],
        )
        self.assertTrue(all(request["params"] == {"id_nota": "900001"} for request in session.requests))
        self.assertTrue(all(request["timeout"] == 2.5 for request in session.requests))
        self.assertTrue(session.closed)
        self.assertFalse(hasattr(note.lines[0], "autoriza"))
        self.assertFalse(hasattr(note.lines[0], "json_descuentos"))
        with self.assertRaises(FrozenInstanceError):
            note.folio = "changed"
        with self.assertRaises(FrozenInstanceError):
            note.lines[0].quantity = Decimal("2")

    def test_fetch_rejects_total_mismatch_against_sum_of_point_line_totals(self):
        header = deepcopy(self.header)
        header[0]["Importe"] = "999.00"
        service, session = self._service(header=header)

        with self.assertRaisesRegex(PointNoteIntegrityError, "total"):
            service.fetch(pk_nota="900001")

        self.assertTrue(session.closed)

    def test_fetch_rejects_invalid_json_and_invalid_structure(self):
        service, session = self._service()
        session.invalid_detail_json = True
        with self.assertRaisesRegex(PointNoteContractError, "JSON.*detalle"):
            service.fetch(pk_nota="900001")
        self.assertTrue(session.closed)

        for invalid_detail in (None, {}, "expired", [None]):
            with self.subTest(invalid_detail=invalid_detail):
                service, session = self._service(detail=invalid_detail)
                with self.assertRaisesRegex(PointNoteContractError, "estructura.*detalle"):
                    service.fetch(pk_nota="900001")
                self.assertTrue(session.closed)

    def test_fetch_treats_empty_detail_as_note_not_found(self):
        service, session = self._service(detail=[])

        with self.assertRaisesRegex(PointNoteNotFoundError, "no existe"):
            service.fetch(pk_nota="missing")

        self.assertTrue(session.closed)

    def test_fetch_treats_empty_header_and_detail_as_note_not_found(self):
        service, session = self._service(header=[], detail=[])

        with self.assertRaisesRegex(PointNoteNotFoundError, "no existe"):
            service.fetch(pk_nota="missing")

        self.assertTrue(session.closed)

    def test_fetch_rejects_missing_required_field(self):
        detail = deepcopy(self.detail)
        del detail[0]["Total"]
        service, _session = self._service(detail=detail)

        with self.assertRaisesRegex(PointNoteContractError, "Total"):
            service.fetch(pk_nota="900001")

    def test_fetch_rejects_invalid_decimal_and_invalid_quantity_price_or_discount(self):
        invalid_values = (
            ("Cantidad", "not-a-number"),
            ("Cantidad", "0"),
            ("Precio_Venta", "-1"),
            ("Descuento", "-0.01"),
            ("Descuento_p", "101"),
            ("Total", "-1"),
        )
        for field, value in invalid_values:
            with self.subTest(field=field, value=value):
                detail = deepcopy(self.detail)
                detail[0][field] = value
                service, _session = self._service(detail=detail)
                with self.assertRaises(PointNoteContractError):
                    service.fetch(pk_nota="900001")

    def test_fetch_closes_session_when_http_request_fails(self):
        service, session = self._service()

        def fail_get(*_args, **_kwargs):
            raise requests.Timeout("late")

        session.get = fail_get
        with self.assertRaisesRegex(PointNoteUnavailableError, "Point"):
            service.fetch(pk_nota="900001")
        self.assertTrue(session.closed)

    def test_fetch_uses_configured_timeout_without_arbitrary_floor(self):
        service, session = self._service(timeout_ms=750)

        service.fetch(pk_nota="900001")

        self.assertEqual([request["timeout"] for request in session.requests], [0.75, 0.75])

    def test_fetch_classifies_http_failure_as_point_unavailable(self):
        service, session = self._service()

        def fail_get(*_args, **_kwargs):
            raise requests.HTTPError("503 Server Error")

        session.get = fail_get
        with self.assertRaises(PointNoteUnavailableError):
            service.fetch(pk_nota="900001")
        self.assertTrue(session.closed)

    def test_fetch_localizes_naive_point_datetime_to_project_timezone(self):
        service, _session = self._service()

        sold_at = service.fetch(pk_nota="900001").sold_at

        self.assertTrue(timezone.is_aware(sold_at))
        self.assertEqual(sold_at.tzinfo, timezone.get_default_timezone())
        self.assertEqual(sold_at.replace(tzinfo=None), datetime(2026, 7, 27, 12, 40))

    def test_fetch_normalizes_offset_datetime_preserving_the_instant(self):
        header = deepcopy(self.header)
        header[0]["Fecha_Hora"] = "2026-07-27T19:40:00+00:00"
        service, _session = self._service(header=header)

        sold_at = service.fetch(pk_nota="900001").sold_at

        self.assertTrue(timezone.is_aware(sold_at))
        self.assertEqual(
            sold_at.astimezone(datetime_timezone.utc),
            datetime(2026, 7, 27, 19, 40, tzinfo=datetime_timezone.utc),
        )
        self.assertEqual(timezone.localtime(sold_at).hour, 12)

    def test_fetch_canonicalizes_real_point_subcent_discount_values(self):
        service, _session = self._service()

        note = service.fetch(pk_nota="900001")

        self.assertEqual(note.total, Decimal("375.01"))
        self.assertEqual(note.lines[0].unit_price, Decimal("395.00"))
        self.assertEqual(note.lines[0].discount, Decimal("19.99"))
        self.assertEqual(note.lines[0].line_total, Decimal("375.01"))

    def test_fetch_reconciles_header_against_raw_line_totals_before_canonical_rounding(self):
        header = deepcopy(self.header)
        header[0]["Importe"] = "4.01"
        detail = [deepcopy(self.detail[0]), deepcopy(self.detail[0])]
        for index, line in enumerate(detail):
            line["Codigo"] = f"P{index}"
            line["Precio_Venta"] = "2.005"
            line["Descuento"] = "0"
            line["Descuento_p"] = "0"
            line["Total"] = "2.005"
        service, _session = self._service(header=header, detail=detail)

        note = service.fetch(pk_nota="900001")

        self.assertEqual(note.total, Decimal("4.01"))
        self.assertEqual([line.line_total for line in note.lines], [Decimal("2.01"), Decimal("2.01")])

    def test_fetch_normalizes_boolean_variants(self):
        truthy_header = deepcopy(self.header)
        truthy_header[0]["isFacturado"] = 1
        service, _session = self._service(header=truthy_header)
        self.assertTrue(service.fetch(pk_nota="900001").invoiced)

        invalid_header = deepcopy(self.header)
        invalid_header[0]["isFacturado"] = "quizá"
        service, _session = self._service(header=invalid_header)
        with self.assertRaisesRegex(PointNoteDetailError, "isFacturado"):
            service.fetch(pk_nota="900001")
