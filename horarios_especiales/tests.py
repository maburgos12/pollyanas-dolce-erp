from __future__ import annotations

from datetime import date
from unittest.mock import patch

from django.contrib.auth.models import Group, User
from django.test import TestCase

from core.models import Sucursal
from horarios_especiales.models import (
    HorarioEspecialDetalle,
    SolicitudHorarioEspecial,
    SucursalAlias,
    SucursalPlataformaExterna,
)
from horarios_especiales.services.command_parser import build_preview_from_command
from horarios_especiales.services.execution import execute_request
from horarios_especiales.services.requests import approve_request, create_request_from_text, validate_request


class SpecialHoursParsingTests(TestCase):
    def setUp(self):
        self.matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payan", activa=True)
        SucursalAlias.objects.create(sucursal=self.payan, alias="paya")

    def test_preview_parses_locations_date_and_hours(self):
        with patch("horarios_especiales.services.command_parser.timezone.localdate", return_value=date(2026, 4, 16)):
            preview = build_preview_from_command(
                "matriz y paya el dia 19 de abril abriran a las 12 pm y cerraran a las 5 pm"
            )
        self.assertEqual(preview.canonical_payload["effective_date"], "2026-04-19")
        self.assertEqual([row["branch_code"] for row in preview.canonical_payload["locations"]], ["MATRIZ", "PAYAN"])
        self.assertEqual(preview.canonical_payload["time_windows"][0], {"open": "12:00", "close": "17:00"})
        self.assertEqual(preview.validation_errors, [])


class SpecialHoursWorkflowTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="dg_special_hours", password="secret")
        Group.objects.get_or_create(name="DG")[0].user_set.add(self.user)
        self.branch = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        SucursalPlataformaExterna.objects.create(
            sucursal=self.branch,
            platform=SucursalPlataformaExterna.PLATFORM_GOOGLE,
            external_location_name="locations/123456789",
            is_active=True,
        )

    def test_create_validate_and_approve_request(self):
        with patch("horarios_especiales.services.command_parser.timezone.localdate", return_value=date(2026, 4, 16)):
            request_obj, payload = create_request_from_text(
                raw_text="matriz el dia 19 de abril abrira a las 12 pm y cerrara a las 5 pm",
                actor=self.user,
                source_channel=SolicitudHorarioEspecial.SOURCE_API,
            )
        self.assertEqual(request_obj.status, SolicitudHorarioEspecial.STATUS_BORRADOR)
        self.assertEqual(payload["validation_errors"], [])
        self.assertEqual(request_obj.details.count(), 1)

        errors = validate_request(request_obj=request_obj, actor=self.user)
        self.assertEqual(errors, [])
        request_obj.refresh_from_db()
        self.assertEqual(request_obj.status, SolicitudHorarioEspecial.STATUS_VALIDADO)

        approve_request(request_obj=request_obj, actor=self.user)
        request_obj.refresh_from_db()
        self.assertEqual(request_obj.status, SolicitudHorarioEspecial.STATUS_APROBADO)
        self.assertEqual(request_obj.approved_by, self.user)

    @patch("horarios_especiales.services.execution.GoogleBusinessProfilePublisher")
    def test_execute_request_marks_success(self, publisher_cls):
        with patch("horarios_especiales.services.command_parser.timezone.localdate", return_value=date(2026, 4, 16)):
            request_obj, _ = create_request_from_text(
                raw_text="matriz el dia 19 de abril abrira a las 12 pm y cerrara a las 5 pm",
                actor=self.user,
                source_channel=SolicitudHorarioEspecial.SOURCE_API,
            )
        approve_request(request_obj=request_obj, actor=self.user)
        publisher = publisher_cls.return_value
        publisher.publish_detail.return_value = {
            "noop": False,
            "request_payload": {"specialHours": {"specialHourPeriods": []}},
            "response_payload": {"name": "locations/123456789"},
            "operation_id": "locations/123456789",
        }

        summary = execute_request(request_obj=request_obj, actor=self.user)

        request_obj.refresh_from_db()
        detail = HorarioEspecialDetalle.objects.get(request=request_obj)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(request_obj.status, SolicitudHorarioEspecial.STATUS_EJECUTADO)
        self.assertEqual(detail.execution_status, HorarioEspecialDetalle.EXEC_STATUS_SUCCESS)
