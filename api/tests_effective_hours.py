from datetime import date
import json
from unittest.mock import Mock, patch

import requests
from django.contrib.auth.models import User
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from rest_framework.test import APIClient

from core.models import Sucursal
from horarios_especiales.models import HorarioEspecialDetalle, SolicitudHorarioEspecial


URL = "/api/integraciones/horarios-especiales/effective/"
WEBSITE = "https://www.pollyanasdolce.com/api/branches/"


class EffectiveHoursTests(TestCase):
    def setUp(self):
        self.branch = Sucursal.objects.create(codigo="CRUCERO", nombre="Sucursal Bamoa")
        self.user = User.objects.create_user(username="maya_hours_reader")
        self.client = APIClient()
        self.client.force_authenticate(self.user)
        self.row = {"id": 11, "name": "Sucursal Bamoa", "erp_branch_code": "BAMOA",
                    "slug": "sucursal-bamoa", "schedule": json.dumps({
                        "lun-sab": "9:00 a.m. a 7:30 p.m.", "dom": "10:00 a.m. a 6:00 p.m."})}
        self.network = patch("requests.get")
        self.get = self.network.start()
        self.addCleanup(self.network.stop)
        self.response = Mock(status_code=200)
        self.response.json.return_value = [self.row]
        self.get.return_value = self.response

    def read(self, **kwargs):
        return self.client.get(URL, {"branch_name": "Bamoa", "target_date": "2026-09-30", **kwargs})

    def special(self, *, status="APROBADO", windows=None, closed=False, target=date(2026, 9, 30), approved=True):
        request = SolicitudHorarioEspecial.objects.create(
            raw_command="fixture", status=status, approved_at=timezone.now() if approved else None)
        return HorarioEspecialDetalle.objects.create(
            request=request, sucursal=self.branch, target_date=target, closed_all_day=closed,
            time_windows_json=windows if windows is not None else [{"open": "11:30", "close": "19:30"}])

    def test_regular_website_is_not_effective_opening_proof(self):
        response = self.read()
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["branch_id"], str(self.branch.id))
        self.assertEqual(data["branch_code"], "CRUCERO")
        self.assertEqual(data["public_branch_code"], "BAMOA")
        self.assertEqual(data["target_date"], "2026-09-30")
        self.assertEqual(data["timezone"], "America/Mazatlan")
        self.assertIsNotNone(timezone.datetime.fromisoformat(data["checked_at"]).tzinfo)
        self.assertEqual(data["regular"], {"status": "VERIFIED", "source": "OFFICIAL_WEBSITE",
            "source_url": WEBSITE, "windows": [{"open": "09:00", "close": "19:30"}]})
        self.assertEqual(data["effective"]["status"], "REGULAR_ONLY")
        self.assertIsNone(data["effective"]["closed_all_day"])
        self.assertNotIn("is_open", data)
        self.get.assert_called_once_with(WEBSITE, timeout=5, allow_redirects=False)

    def test_requires_authentication_and_only_get(self):
        self.client.force_authenticate(None)
        self.assertEqual(self.read().status_code, 401)
        self.client.force_authenticate(self.user)
        self.assertEqual(self.client.post(URL, {}).status_code, 405)
        self.get.assert_not_called()

    def test_invalid_or_missing_inputs(self):
        for args in ({}, {"branch_name": "Bamoa"}, {"branch_name": "", "target_date": "2026-09-30"},
                     {"branch_name": "Bamoa", "target_date": "2026-02-30"},
                     {"branch_name": "Bamoa", "target_date": "20260930"},
                     {"branch_name": "a" * 121, "target_date": "2026-09-30"}):
            with self.subTest(args=args):
                self.assertEqual(self.client.get(URL, args).status_code, 400)
        self.get.assert_not_called()

    def test_unknown_or_ambiguous_branch(self):
        self.assertEqual(self.read(branch_name="No existe").status_code, 400)
        Sucursal.objects.create(codigo="BAMOA2", nombre="Sucursal Bamoa Dos")
        self.assertEqual(self.read().status_code, 400)
        self.get.assert_not_called()

    def test_approved_exception_and_get_has_zero_sql_writes(self):
        self.special()
        with CaptureQueriesContext(connection) as queries:
            data = self.read().json()
        self.assertEqual(data["effective"], {"status": "VERIFIED", "source": "ERP_APPROVED_SPECIAL_HOURS",
            "closed_all_day": False, "windows": [{"open": "11:30", "close": "19:30"}], "reason": None})
        self.assertFalse(any(q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for q in queries))

    def test_explicit_closed_and_executed_exception(self):
        detail = self.special(status="EJECUTADO", windows=[], closed=True)
        detail.execution_status = "EXITOSO"
        detail.save()
        effective = self.read().json()["effective"]
        self.assertEqual(effective["status"], "VERIFIED")
        self.assertTrue(effective["closed_all_day"])
        self.assertEqual(effective["windows"], [])

    def test_drafts_cancelled_and_other_dates_do_not_override(self):
        for status in ("BORRADOR", "VALIDADO", "CANCELADO"):
            self.special(status=status)
        self.special(target=date(2026, 9, 29))
        self.assertEqual(self.read().json()["effective"]["status"], "REGULAR_ONLY")

    def test_failed_approved_publication_and_conflicts_are_unknown(self):
        failed = self.special(status="FALLIDO")
        self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")
        failed.request.delete()
        self.special()
        self.special(windows=[{"open": "12:00", "close": "19:30"}])
        self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")

    def test_invalid_approved_detail_cannot_become_regular_proof(self):
        cases = [([], False), ([{"open": "19:30", "close": "11:30"}], False),
                 ([{"open": "9:00", "close": "19:30"}], False),
                 ([{"open": "09:00", "close": "19:30"}], True),
                 ([{"open": "09:00", "close": "12:00"}, {"open": "11:00", "close": "19:30"}], False)]
        for windows, closed in cases:
            with self.subTest(windows=windows, closed=closed):
                detail = self.special(windows=windows, closed=closed)
                self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")
                detail.request.delete()
        detail = self.special(approved=False)
        self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")
        detail.request.delete()
        detail = self.special()
        detail.validation_errors_json = ["invalid"]
        detail.save()
        self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")

    def test_same_exception_deduplicates_and_cancelled_detail_ignored(self):
        self.special()
        self.special()
        detail = self.special(windows=[{"open": "13:00", "close": "14:00"}])
        detail.execution_status = "CANCELADO"
        detail.save()
        self.assertEqual(self.read().json()["effective"]["status"], "VERIFIED")

    def test_network_failure_preserves_approved_erp_exception(self):
        self.special()
        self.get.side_effect = requests.Timeout()
        data = self.read().json()
        self.assertEqual(data["regular"]["status"], "UNKNOWN")
        self.assertEqual(data["effective"]["status"], "VERIFIED")
        self.assertIsNone(data["public_branch_code"])

    def test_redirect_bad_json_and_oversize_payload_are_unknown(self):
        for status, payload in ((301, [self.row]), (503, [self.row]), (200, {}), (200, [self.row] * 101)):
            with self.subTest(status=status, payload_type=type(payload)):
                self.response.status_code = status
                self.response.json.return_value = payload
                self.assertEqual(self.read().json()["regular"]["status"], "UNKNOWN")
        self.response.status_code = 200
        self.response.json.side_effect = ValueError("bad json")
        self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")

    def test_public_duplicate_and_historical_crucero_are_rejected(self):
        for rows in ([self.row, self.row], [{**self.row, "name": "Sucursal Crucero", "erp_branch_code": "CRUCERO"}],
                     [{**self.row, "erp_branch_code": "MATRIZ"}],
                     [self.row, {**self.row, "erp_branch_code": "MATRIZ", "id": 12}]):
            with self.subTest(rows=rows):
                self.response.json.return_value = rows
                self.assertEqual(self.read().json()["regular"]["status"], "UNKNOWN")
        self.branch.codigo = "MATRIZ"
        self.branch.save()
        self.response.json.return_value = [self.row]
        self.assertEqual(self.read().json()["regular"]["status"], "UNKNOWN")

    def test_all_real_day_groups_and_noon_midnight(self):
        cases = [({"lun-sab": "9:00 a.m. a 7:30 p.m.", "dom": "10:00 a.m. a 6:00 p.m."}, "2026-10-04", "10:00", "18:00"),
                 ({"lun-mar,jue-sab": "9:00 a.m. a 7:30 p.m.", "mie": "11:30 a.m. a 7:30 p.m."}, "2026-09-30", "11:30", "19:30"),
                 ({"lun-mie,vie-sab": "9:00 a.m. a 7:30 p.m.", "jue": "11:30 a.m. a 7:30 p.m."}, "2026-10-01", "11:30", "19:30"),
                 ({"lun,mie-sab": "11:00 a.m. a 9:00 p.m.", "mar,dom": "1:00 p.m. a 9:00 p.m."}, "2026-09-29", "13:00", "21:00"),
                 ({"lun-sab": "11:30 a.m. a 7:30 p.m."}, "2026-09-30", "11:30", "19:30"),
                 ({"lun-sab": "12:00 p.m. a 10:00 p.m."}, "2026-09-30", "12:00", "22:00"),
                 ({"mie": "12:00 a.m. a 12:00 p.m."}, "2026-09-30", "00:00", "12:00"),
                 ({"mie": "09:00-19:30"}, "2026-09-30", "09:00", "19:30")]
        for schedule, target, opened, closed in cases:
            with self.subTest(schedule=schedule):
                self.row["schedule"] = json.dumps(schedule)
                self.assertEqual(self.read(target_date=target).json()["regular"]["windows"], [{"open": opened, "close": closed}])

    def test_malformed_regular_schedule_unknown_not_closed(self):
        for schedule in ({"mie": "closed"}, {"mie": "19:30-09:00"}, {"mie": "09:00-19:30", "lun-sab": "10:00-18:00"},
                         {"notday": "09:00-19:30"}, [], "bad json", {"mie": "13:00 p.m. a 19:00 p.m."}):
            with self.subTest(schedule=schedule):
                self.row["schedule"] = schedule
                data = self.read().json()
                self.assertEqual(data["regular"]["status"], "UNKNOWN")
                self.assertIsNone(data["effective"]["closed_all_day"])

    def test_failed_child_in_executed_request_is_unknown(self):
        detail = self.special(status="EJECUTADO")
        detail.execution_status = "FALLIDO"
        detail.save()
        self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")

    def test_pending_child_in_executed_request_is_unknown(self):
        self.special(status="EJECUTADO")
        self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")

    def test_unknown_child_execution_status_cannot_confirm_approved_hours(self):
        for execution_status in ("UNKNOWN", "INVALID"):
            with self.subTest(execution_status=execution_status):
                detail = self.special()
                detail.execution_status = execution_status
                detail.save()
                self.assertEqual(self.read().json()["effective"]["status"], "UNKNOWN")
                detail.request.delete()

    def test_duplicate_json_day_keys_cannot_confirm_regular_hours(self):
        self.row["schedule"] = '{"mie":"09:00-19:30","mie":"11:30-18:00"}'
        data = self.read().json()
        self.assertEqual(data["regular"]["status"], "UNKNOWN")
        self.assertEqual(data["effective"]["status"], "UNKNOWN")

    def test_all_nine_official_branch_identities(self):
        for code, name in (("MATRIZ", "Matriz"), ("PAYAN", "Payán"), ("LAS_GLORIAS", "Plaza Las Glorias"),
                           ("PLAZA_NIO", "Plaza Nío"), ("LEYVA", "Leyva"), ("COLOSIO", "Colosio"),
                           ("EL_TUNEL", "El Túnel"), ("BAMOA", "Bamoa"), ("GUAMUCHIL", "Guamúchil")):
            with self.subTest(code=code):
                self.branch.codigo, self.branch.nombre = code, f"Sucursal {name}"
                self.branch.save()
                self.row.update(name=f"Sucursal {name}", erp_branch_code=code)
                data = self.read(branch_name=name).json()
                self.assertEqual(data["public_branch_code"], code)
                self.assertEqual(data["regular"]["status"], "VERIFIED")
