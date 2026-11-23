from __future__ import annotations

from datetime import date
from unittest.mock import patch

from django.contrib.auth.models import Group, User
from django.core.management import call_command
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
from horarios_especiales.services.requests import approve_request, cancel_request, create_request_from_text, validate_request


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

    @patch("horarios_especiales.services.execution.GoogleBusinessProfilePublisher")
    def test_legacy_execution_rejects_cancelled_or_unapproved_even_with_approved_status(self, publisher):
        from django.utils import timezone
        for approved_at, cancelled_at in ((timezone.now(), timezone.now()), (None, None)):
            obj = SolicitudHorarioEspecial.objects.create(raw_command="fixture", status="APROBADO",
                                                          approved_at=approved_at, cancelled_at=cancelled_at)
            HorarioEspecialDetalle.objects.create(request=obj, sucursal=self.branch, target_date=date(2026, 11, 23))
            with self.assertRaises(ValueError):
                execute_request(request_obj=obj)
        publisher.assert_not_called()

    def test_stale_retry_does_not_resurrect_cancelled_legacy_request(self):
        from rest_framework.test import APIClient
        from django.utils import timezone
        obj = SolicitudHorarioEspecial.objects.create(raw_command="fixture", status="APROBADO", approved_at=timezone.now())
        HorarioEspecialDetalle.objects.create(request=obj, sucursal=self.branch, target_date=date(2026, 11, 23))
        stale = SolicitudHorarioEspecial.objects.get(pk=obj.pk)
        cancel_request(request_obj=obj)
        client = APIClient()
        client.force_authenticate(self.user)
        with patch("api.special_hours_views.get_object_or_404", return_value=stale), patch(
            "api.special_hours_views.execute_special_hours_request_task.delay",
        ) as queued:
            queued.return_value.id = "stale-retry"
            response = client.post(f"/api/integraciones/horarios-especiales/{obj.pk}/retry/", {}, format="json")
        self.assertEqual(response.status_code, 202)
        obj.refresh_from_db()
        self.assertEqual(obj.status, "CANCELADO")
        self.assertIsNotNone(obj.cancelled_at)
        with patch("horarios_especiales.services.execution.GoogleBusinessProfilePublisher") as publisher:
            with self.assertRaises(ValueError):
                execute_request(request_obj=stale)
            publisher.assert_not_called()

    @patch("horarios_especiales.services.execution.GoogleBusinessProfilePublisher")
    def test_legacy_failed_approved_request_retries_without_resetting_approval(self, publisher):
        from django.utils import timezone
        obj = SolicitudHorarioEspecial.objects.create(raw_command="fixture", status="FALLIDO", approved_at=timezone.now())
        HorarioEspecialDetalle.objects.create(request=obj, sucursal=self.branch, target_date=date(2026, 11, 23))
        publisher.return_value.publish_detail.return_value = {"noop": False, "receipt": "API_ACCEPTED"}
        result = execute_request(request_obj=obj)
        self.assertEqual(result["failed"], 0)
        publisher.return_value.publish_detail.assert_called_once()


class SpecialHoursBootstrapCommandTests(TestCase):
    def setUp(self):
        self.matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payan", activa=True)

    def test_bootstrap_catalog_creates_aliases_and_platform_config(self):
        call_command("bootstrap_special_hours_catalog", "--apply")

        self.assertTrue(SucursalAlias.objects.filter(sucursal=self.matriz, alias="matriz", is_active=True).exists())
        self.assertTrue(SucursalAlias.objects.filter(sucursal=self.payan, alias="paya", is_active=True).exists())

        matriz_config = SucursalPlataformaExterna.objects.get(
            sucursal=self.matriz,
            platform=SucursalPlataformaExterna.PLATFORM_GOOGLE,
        )
        self.assertTrue(matriz_config.is_active)
        self.assertEqual(
            matriz_config.settings_json["google_business_profile"]["expected_store_name"],
            "Pollyana's Dolce",
        )
        self.assertEqual(matriz_config.external_account_id, "113534506057208552108")
        self.assertEqual(matriz_config.external_location_id, "17542525418474435119")
        self.assertEqual(matriz_config.external_location_name, "locations/17542525418474435119")
