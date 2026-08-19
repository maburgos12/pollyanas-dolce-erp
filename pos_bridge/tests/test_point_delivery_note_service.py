from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import requests
from django.test import SimpleTestCase
from django.utils import timezone

from pos_bridge.services.point_delivery_note_service import (
    PointDeliveryContractError,
    PointDeliveryNoteService,
    PointDeliveryUnavailableError,
)
from pos_bridge.services.point_note_detail_service import PointNote, PointNoteLine


class _FakeResponse:
    def __init__(self, payload, *, invalid_json=False):
        self._payload = payload
        self.text = json.dumps(payload)
        self.invalid_json = invalid_json

    def raise_for_status(self):
        return None

    def json(self):
        if self.invalid_json:
            raise requests.exceptions.JSONDecodeError("invalid", self.text, 0)
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.closed = False

    def get(self, url, *, params, timeout):
        self.requests.append({"url": url, "params": params, "timeout": timeout})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return _FakeResponse(response)

    def close(self):
        self.closed = True


class _FakeHttpSessionService:
    def __init__(self, session):
        self.session = session
        self.create_calls = 0
        self.create_kwargs = []

    def create(self, **kwargs):
        self.create_calls += 1
        self.create_kwargs.append(kwargs)
        return SimpleNamespace(session=self.session)


class _FakeDetailService:
    def __init__(self, note):
        self.note = note
        self.calls = []

    def fetch_with_session(self, session, *, pk_nota):
        self.calls.append((session, pk_nota))
        if isinstance(self.note, dict):
            return self.note[pk_nota]
        return self.note


def _note(*, pk_nota="887410", folio="15616"):
    sold_at = timezone.make_aware(datetime(2026, 8, 5, 14, 16, 50))
    return PointNote(
        pk_nota=pk_nota,
        folio=folio,
        branch_name="Matriz",
        sold_at=sold_at,
        total=Decimal("514.00"),
        invoiced=False,
        payment_type="CONTADO",
        point_channel="Mostrador",
        lines=(
            PointNoteLine(
                point_code="0304",
                description="Servicio Domicilio 2",
                quantity=Decimal("1"),
                unit_price=Decimal("15.00"),
                discount=Decimal("0.00"),
                line_total=Decimal("15.00"),
            ),
        ),
        source_endpoint="/Clientes/get_detalle_nota/",
        customer_external_id="321",
    )


class PointDeliveryNoteServiceTests(SimpleTestCase):
    def _service(self, responses, *, note=None):
        session = _FakeSession(responses)
        http = _FakeHttpSessionService(session)
        detail = _FakeDetailService(note or _note())
        settings = SimpleNamespace(base_url="https://point.test", timeout_ms=2500)
        service = PointDeliveryNoteService(
            bridge_settings=settings,
            http_session_service=http,
            note_detail_service=detail,
        )
        return service, session, http, detail

    def test_fetch_range_uses_official_delivery_tray_and_one_session_for_all_calls(self):
        tray = [{
            "PK_Nota": 887410,
            "Folio": 15616,
            "Sucursal": "Matriz",
            "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
            "Total": 514,
            "Facturado": False,
        }]
        delivery_customer = {
            "PK_Cliente": 321,
            "Cliente": "Cliente Prueba",
            "Calle": "Av. Principal",
            "NoExterior": "101",
            "NoInterior": "",
            "Colonia": "Centro",
            "EntreCalles": "Primera y Segunda",
            "Observaciones": "Portón gris",
            "PK_Direccion_Entrega": 88,
        }
        catalog = [{
            "Pk_cliente": 321,
            "Cliente": "Cliente Prueba",
            "telefono1": "",
            "telefono2": "687 123 4567",
            "Correo": "cliente@example.test",
        }]
        service, session, http, detail = self._service(
            [
                {"hasError": False, "data": tray},
                {"hasError": False, "data": delivery_customer},
                catalog,
            ],
        )

        result = service.fetch_range(
            start_date=date(2026, 8, 4),
            end_date=date(2026, 8, 5),
            branch_external_id="10",
            branch_display_name="Matriz",
        )

        self.assertEqual(http.create_calls, 1)
        self.assertEqual(
            http.create_kwargs,
            [
                {
                    "branch_external_id": "10",
                    "branch_display_name": "Matriz",
                    "strict_branch": True,
                },
            ],
        )
        self.assertEqual(detail.calls, [(session, "887410")])
        self.assertTrue(session.closed)
        self.assertEqual(result[0].note.pk_nota, "887410")
        self.assertEqual(result[0].customer_name, "Cliente Prueba")
        self.assertEqual(result[0].customer_phone, "6871234567")
        self.assertEqual(result[0].customer_email, "cliente@example.test")
        self.assertEqual(result[0].address, "Av. Principal 101, Colonia Centro")
        self.assertEqual(
            result[0].references,
            "Entre calles: Primera y Segunda. Observaciones: Portón gris",
        )
        self.assertEqual(
            session.requests[0]["url"],
            "https://point.test/Ventas/getNotasByDateServicioDomicilio",
        )
        self.assertEqual(session.requests[0]["params"]["id_sucursal"], "10")
        self.assertIsInstance(session.requests[0]["params"]["fecha_inicio"], int)
        self.assertIsInstance(session.requests[0]["params"]["fecha_final"], int)
        self.assertEqual(
            [request["url"] for request in session.requests[1:]],
            [
                "https://point.test/Ventas/getDataClienteById",
                "https://point.test/Clientes/get_clientes_byActivo",
            ],
        )
        self.assertEqual(
            session.requests[2]["params"],
            {
                "activo": True,
                "id_sucursal": "null",
                "texto": "Cliente Prueba",
                "id_tipo_membresia": "null",
            },
        )
        with self.assertRaises(FrozenInstanceError):
            result[0].customer_name = "changed"

    def test_fetch_range_accepts_missing_optional_delivery_customer_fields(self):
        tray = [{
            "PK_Nota": 887410,
            "Folio": 15616,
            "Sucursal": "Matriz",
            "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
            "Total": 514,
            "Facturado": False,
        }]
        delivery_customer = {
            "PK_Cliente": 321,
            "Cliente": "Cliente Prueba",
            "Calle": "Av. Principal",
            "NoExterior": "101",
            "Colonia": "Centro",
            "PK_Direccion_Entrega": 88,
        }
        service, _session, _http, _detail = self._service(
            [
                {"hasError": False, "data": tray},
                {"hasError": False, "data": delivery_customer},
                [],
            ],
        )

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
        )

        self.assertEqual(len(result.notes), 1)
        self.assertEqual(result.failures, ())
        self.assertEqual(result[0].address, "Av. Principal 101, Colonia Centro")
        self.assertEqual(result[0].references, "")

    def test_fetch_range_rejects_invalid_delivery_customer_envelopes(self):
        tray = [{
            "PK_Nota": 887410,
            "Folio": 15616,
            "Sucursal": "Matriz",
            "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
            "Total": 514,
            "Facturado": False,
        }]
        customer = {
            "PK_Cliente": 321,
            "Cliente": "Cliente Prueba",
            "Calle": "Av. Principal",
        }
        invalid_envelopes = {
            "point_error": {"hasError": True, "data": customer},
            "missing_data": {"hasError": False},
            "multiple_rows": {"hasError": False, "data": [customer, customer]},
            "scalar_data": {"hasError": False, "data": "invalid"},
        }

        for label, envelope in invalid_envelopes.items():
            with self.subTest(label=label):
                service, session, _http, _detail = self._service([tray, envelope])

                result = service.fetch_range(
                    start_date=date(2026, 8, 5),
                    end_date=date(2026, 8, 5),
                )

                self.assertEqual(result.notes, ())
                self.assertEqual(len(result.failures), 1)
                self.assertEqual(result.failures[0].error_code, "POINT_CONTRACT")
                self.assertEqual(len(session.requests), 2)
                self.assertTrue(session.closed)

    def test_fetch_range_normalizes_invalid_catalog_email_to_blank(self):
        tray = [{
            "PK_Nota": 887410,
            "Folio": 15616,
            "Sucursal": "Matriz",
            "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
            "Total": 514,
            "Facturado": False,
        }]
        delivery_customer = {
            "PK_Cliente": 321,
            "Cliente": "Cliente Prueba",
            "Calle": "Calle Uno",
            "NoExterior": "1",
            "NoInterior": "",
            "Colonia": "Centro",
            "EntreCalles": "",
            "Observaciones": "",
            "PK_Direccion_Entrega": 88,
        }
        catalog = [{
            "Pk_cliente": 321,
            "telefono1": "6871234567",
            "Correo": "SIN CORREO",
        }]
        service, _session, _http, _detail = self._service(
            [tray, delivery_customer, catalog],
        )

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
            branch_external_id="10",
            branch_display_name="Matriz",
        )

        self.assertEqual(result[0].customer_email, "")

    def test_fetch_range_uses_first_nonempty_phone_including_alternate_phone(self):
        tray = [{
            "PK_Nota": 887410,
            "Folio": 15616,
            "Sucursal": "Matriz",
            "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
            "Total": 514,
            "Facturado": False,
        }]
        delivery_customer = {
            "PK_Cliente": 321,
            "Cliente": "Cliente Prueba",
            "Calle": "Calle Uno",
            "NoExterior": "1",
            "NoInterior": "",
            "Colonia": "Centro",
            "EntreCalles": "",
            "Observaciones": "",
            "PK_Direccion_Entrega": 88,
        }
        catalog = [{
            "Pk_cliente": 321,
            "telefono1": None,
            "telefono2": "(687) 765-4321",
            "correo": "",
        }]
        service, _session, _http, _detail = self._service(
            [tray, delivery_customer, catalog],
        )

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
        )

        self.assertEqual(result[0].customer_phone, "6877654321")

    def test_fetch_range_rejects_generic_note_that_is_not_in_delivery_tray(self):
        service, session, _http, detail = self._service([[]])

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
        )

        self.assertEqual(result.seen_count, 0)
        self.assertEqual(tuple(result), ())
        self.assertEqual(detail.calls, [])
        self.assertTrue(session.closed)

    def test_fetch_range_rejects_missing_tray_contract_fields(self):
        service, session, _http, _detail = self._service([[{"PK_Nota": 887410}]])

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
        )

        self.assertEqual(len(result), 0)
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(result.failures[0].error_code, "POINT_CONTRACT")
        self.assertTrue(session.closed)

    def test_fetch_range_rejects_error_delivery_tray_envelope(self):
        service, session, _http, _detail = self._service(
            [{"hasError": True, "data": []}],
        )

        with self.assertRaises(PointDeliveryContractError):
            service.fetch_range(
                start_date=date(2026, 8, 5),
                end_date=date(2026, 8, 5),
            )

        self.assertTrue(session.closed)

    def test_fetch_range_classifies_point_http_failure_and_closes_session(self):
        service, session, _http, _detail = self._service(
            [requests.Timeout("late")],
        )

        with self.assertRaises(PointDeliveryUnavailableError):
            service.fetch_range(
                start_date=date(2026, 8, 5),
                end_date=date(2026, 8, 5),
            )

        self.assertTrue(session.closed)

    def test_fetch_range_keeps_delivery_without_catalog_match_for_intake(self):
        tray = [{
            "PK_Nota": 887410,
            "Folio": 15616,
            "Sucursal": "Matriz",
            "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
            "Total": 514,
            "Facturado": False,
        }]
        delivery_customer = {
            "PK_Cliente": 321,
            "Cliente": "Cliente Prueba",
            "Calle": "Calle Uno",
            "NoExterior": "1",
            "NoInterior": "",
            "Colonia": "Centro",
            "EntreCalles": "",
            "Observaciones": "",
            "PK_Direccion_Entrega": 88,
        }
        catalog = [{"Pk_cliente": 999, "telefono1": "6871234567"}]
        service, _session, _http, _detail = self._service(
            [tray, delivery_customer, catalog],
        )

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
        )

        self.assertEqual(result[0].customer_external_id, "321")
        self.assertEqual(result[0].customer_phone, "")
        self.assertEqual(result[0].customer_email, "")

    def test_fetch_range_isolates_invalid_row_between_valid_notes(self):
        tray = [
            {
                "PK_Nota": 887410,
                "Folio": 15616,
                "Sucursal": "Matriz",
                "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
                "Total": 514,
                "Facturado": False,
            },
            {"PK_Nota": 999999},
            {
                "PK_Nota": 887411,
                "Folio": 15617,
                "Sucursal": "Matriz",
                "Fecha_Hora_Cierre": "2026-08-05T14:17:50",
                "Total": 514,
                "Facturado": False,
            },
        ]
        customer = {
            "PK_Cliente": 321,
            "Cliente": "Cliente Prueba",
            "Calle": "Calle Uno",
            "NoExterior": "1",
            "NoInterior": "",
            "Colonia": "Centro",
            "EntreCalles": "",
            "Observaciones": "",
            "PK_Direccion_Entrega": 88,
        }
        catalog = [{"Pk_cliente": 321, "telefono1": "6871234567"}]
        service, _session, _http, detail = self._service(
            [tray, customer, catalog, customer, catalog],
            note={
                "887410": _note(pk_nota="887410", folio="15616"),
                "887411": _note(pk_nota="887411", folio="15617"),
            },
        )

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
        )

        self.assertEqual([item.note.pk_nota for item in result], ["887410", "887411"])
        self.assertEqual(detail.calls, [
            (detail.calls[0][0], "887410"),
            (detail.calls[0][0], "887411"),
        ])
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(result.failures[0].error_code, "POINT_CONTRACT")

    def test_fetch_range_skips_known_note_before_loading_detail_or_customer(self):
        tray = [{
            "PK_Nota": 887410,
            "Folio": 15616,
            "Sucursal": "Matriz",
            "Fecha_Hora_Cierre": "2026-08-05T14:16:50",
            "Total": 514,
            "Facturado": False,
        }]
        service, session, _http, detail = self._service([tray])

        result = service.fetch_range(
            start_date=date(2026, 8, 5),
            end_date=date(2026, 8, 5),
            exclude_note_ids={"887410"},
        )

        self.assertEqual(result.seen_count, 1)
        self.assertEqual(tuple(result), ())
        self.assertEqual(detail.calls, [])
        self.assertEqual(len(session.requests), 1)
