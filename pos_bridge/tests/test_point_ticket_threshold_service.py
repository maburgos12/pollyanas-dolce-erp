from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch
from pos_bridge.services.point_ticket_threshold_service import PointTicketThresholdService


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.requests = []
        self.closed = False

    def get(self, url, *, params, timeout):
        self.requests.append({"url": url, "params": params, "timeout": timeout})
        return _FakeResponse(self.payload)

    def close(self):
        self.closed = True


class _FakeHttpSessionService:
    def __init__(self, session):
        self.session = session

    def create(self):
        return SimpleNamespace(session=self.session)


class PointTicketThresholdServiceTests(TestCase):
    def _service(self, payload):
        session = _FakeSession(payload)
        settings = SimpleNamespace(base_url="https://point.test", timeout_ms=30000)
        return PointTicketThresholdService(
            bridge_settings=settings,
            http_session_service=_FakeHttpSessionService(session),
        ), session

    def test_fetch_threshold_count_counts_point_notes_by_amount(self):
        service, session = self._service(
            [
                {"SUCURSAL": "Matriz", "MONTO": 499.99},
                {"SUCURSAL": "Matriz", "MONTO": 500},
                {"SUCURSAL": "Leyva", "MONTO": 900},
            ]
        )

        result = service.fetch_threshold_count(
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 30),
            threshold_amount=Decimal("500"),
        )

        self.assertEqual(result.exact_count, 2)
        self.assertEqual(result.total_notes, 3)
        self.assertEqual(result.total_amount, Decimal("1899.99"))
        self.assertEqual(result.branch_results[0].branch_name, "Leyva")
        self.assertEqual(result.branch_results[0].exact_count, 1)
        self.assertTrue(session.closed)
        self.assertEqual(session.requests[0]["url"], "https://point.test/Report/NotasByPlaza")
        parsed = parse_qs(urlparse(result.request_url).query)
        self.assertEqual(parsed["sucursal"], ["null"])
        self.assertEqual(parsed["credito"], ["null"])

    def test_fetch_threshold_count_applies_branch_scope_after_point_response(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=sucursal)
        service, _session = self._service(
            [
                {"SUCURSAL": "Matriz", "MONTO": 700},
                {"SUCURSAL": "Leyva", "MONTO": 800},
            ]
        )

        result = service.fetch_threshold_count(
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 30),
            threshold_amount=Decimal("500"),
            branch_ids=[sucursal.id],
        )

        self.assertEqual(result.exact_count, 1)
        self.assertEqual(result.total_notes, 1)
        self.assertEqual(result.branch_results[0].branch_name, "Matriz")
