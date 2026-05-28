from __future__ import annotations

from datetime import date, datetime, time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointSyncJob
from pos_bridge.services.attendance_sync_service import PointAttendanceSyncService
from rrhh.models import AsistenciaEmpleado, Empleado, Turno


class FakeSettings:
    base_url = "https://app.pointmeup.test"
    timeout_ms = 30000

    def safe_dict(self):
        return {"base_url": self.base_url, "password": "***"}


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.text = ""

    def json(self):
        return self.payload

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, *, attendance_rows, absence_rows=None):
        self.attendance_rows = attendance_rows
        self.absence_rows = absence_rows or []
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params or {}, "timeout": timeout})
        if url.endswith("/Home/Get_Sucursales_ByZona"):
            return FakeResponse(
                [
                    {
                        "Plaza": "Plaza principal",
                        "Sucursales": [
                            {"PK_Sucursal": "2", "Sucursal": "Crucero"},
                            {"PK_Sucursal": "3", "Sucursal": "Las Glorias"},
                            {"PK_Sucursal": "10", "Sucursal": "Produccion Crucero"},
                        ],
                    }
                ]
            )
        if url.endswith("/Attendance/AsistenciaDiaria"):
            return FakeResponse(self.attendance_rows)
        if url.endswith("/Attendance/Inasistencias"):
            return FakeResponse(self.absence_rows)
        return FakeResponse([])


class FakeHttpSessionService:
    def __init__(self, session):
        self.session = session

    def create(self):
        return SimpleNamespace(session=self.session)


class PointAttendanceSyncServiceTests(TestCase):
    def test_run_sync_persists_point_attendance(self):
        sucursal = Sucursal.objects.create(codigo="CRUCERO", nombre="Crucero", activa=True)
        empleado = Empleado.objects.create(codigo="0010", nombre="Empleado Crucero", sucursal="Crucero")
        turno = Turno.objects.create(nombre="Matutino", hora_entrada=time(8, 0), hora_salida=time(16, 0))
        PointBranch.objects.create(
            external_id="2",
            name="Crucero anterior",
            metadata={"sales_source": "daily_sync"},
        )
        session = FakeSession(
            attendance_rows=[
                {
                    "Codigo": "0010",
                    "Empleado": "Empleado Crucero",
                    "Puesto": "cajero",
                    "Entrada": "2026-05-27T07:37:45",
                    "Salida": "2026-05-27T16:00:14",
                    "H_Entrada": "08:00:00",
                    "H_Salida": "16:00:00",
                    "Horas_Trabajo": 9.0,
                    "IDX": 17677,
                    "Nombre_Corto": "Crucero",
                    "Retardo": False,
                    "Falta": False,
                    "fuera_rango": False,
                }
            ]
        )
        service = PointAttendanceSyncService(
            bridge_settings=FakeSettings(),
            http_session_service=FakeHttpSessionService(session),
        )

        job = service.run_sync(start_date=date(2026, 5, 27), end_date=date(2026, 5, 27), branch_filter="Crucero")

        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha=date(2026, 5, 27))
        self.assertEqual(asistencia.fuente, AsistenciaEmpleado.FUENTE_POINT)
        self.assertEqual(asistencia.sucursal, sucursal)
        self.assertEqual(asistencia.turno, turno)
        self.assertEqual(asistencia.minutos_trabajados, 502)
        self.assertIn("IDX=17677", asistencia.observacion)
        point_branch = PointBranch.objects.get(external_id="2")
        self.assertEqual(point_branch.erp_branch, sucursal)
        self.assertEqual(point_branch.metadata["sales_source"], "daily_sync")
        self.assertEqual(point_branch.metadata["attendance_source"], "attendance")
        attendance_branch_calls = [
            call["params"].get("sucursal")
            for call in session.calls
            if "Attendance/" in call["url"]
        ]
        self.assertEqual(attendance_branch_calls, ["2", "2"])

    def test_run_sync_marks_partial_for_unmapped_employee(self):
        Sucursal.objects.create(codigo="CRUCERO", nombre="Crucero", activa=True)
        session = FakeSession(
            attendance_rows=[
                {
                    "Codigo": "NO-MAP",
                    "Empleado": "Empleado sin mapa",
                    "Entrada": "2026-05-27T08:00:00",
                    "Salida": "2026-05-27T16:00:00",
                    "H_Entrada": "08:00:00",
                    "H_Salida": "16:00:00",
                    "IDX": 1,
                }
            ]
        )
        service = PointAttendanceSyncService(
            bridge_settings=FakeSettings(),
            http_session_service=FakeHttpSessionService(session),
        )

        job = service.run_sync(start_date=date(2026, 5, 27), end_date=date(2026, 5, 27), branch_filter="Crucero")

        self.assertEqual(job.status, PointSyncJob.STATUS_PARTIAL)
        self.assertEqual(job.result_summary["missing_employee"], 1)
        self.assertEqual(AsistenciaEmpleado.objects.count(), 0)

    def test_persist_payload_updates_existing_open_attendance(self):
        empleado = Empleado.objects.create(codigo="0010", nombre="Empleado Crucero")
        AsistenciaEmpleado.objects.create(
            empleado=empleado,
            fecha=date(2026, 5, 27),
            entrada=datetime(2026, 5, 27, 8, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            fuente=AsistenciaEmpleado.FUENTE_POINT,
        )
        session = FakeSession(
            attendance_rows=[
                {
                    "Codigo": "0010",
                    "Empleado": "Empleado Crucero",
                    "Entrada": "2026-05-27T08:00:00",
                    "Salida": "2026-05-27T17:00:00",
                    "H_Entrada": "08:00:00",
                    "H_Salida": "16:00:00",
                    "IDX": 2,
                }
            ]
        )
        service = PointAttendanceSyncService(
            bridge_settings=FakeSettings(),
            http_session_service=FakeHttpSessionService(session),
        )

        job = service.run_sync(start_date=date(2026, 5, 27), end_date=date(2026, 5, 27), branch_filter="Crucero")

        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(AsistenciaEmpleado.objects.count(), 1)
        asistencia = AsistenciaEmpleado.objects.get()
        self.assertIsNotNone(asistencia.salida)
        self.assertEqual(asistencia.minutos_trabajados, 540)

    def test_run_sync_never_renames_or_creates_employees_from_point_names(self):
        empleado = Empleado.objects.create(codigo="0010", nombre="Nombre ERP Oficial")
        session = FakeSession(
            attendance_rows=[
                {
                    "Codigo": "0010",
                    "Empleado": "Nombre distinto en Point",
                    "Entrada": "2026-05-27T08:00:00",
                    "Salida": "2026-05-27T16:00:00",
                    "IDX": 3,
                },
                {
                    "Codigo": "9999",
                    "Empleado": "Persona solo Point",
                    "Entrada": "2026-05-27T08:00:00",
                    "Salida": "2026-05-27T16:00:00",
                    "IDX": 4,
                },
            ]
        )
        service = PointAttendanceSyncService(
            bridge_settings=FakeSettings(),
            http_session_service=FakeHttpSessionService(session),
        )

        job = service.run_sync(start_date=date(2026, 5, 27), end_date=date(2026, 5, 27), branch_filter="Crucero")

        self.assertEqual(job.status, PointSyncJob.STATUS_PARTIAL)
        empleado.refresh_from_db()
        self.assertEqual(empleado.nombre, "Nombre ERP Oficial")
        self.assertEqual(Empleado.objects.count(), 1)
        self.assertEqual(AsistenciaEmpleado.objects.filter(empleado=empleado).count(), 1)
        self.assertEqual(job.result_summary["missing_employee"], 1)

    def test_run_sync_can_link_by_unique_erp_name_without_creating_employee(self):
        empleado = Empleado.objects.create(codigo="ERP-001", nombre="Laura Torres")
        session = FakeSession(
            attendance_rows=[
                {
                    "Codigo": "2543",
                    "Empleado": "  laura   torres ",
                    "Entrada": "2026-05-27T08:00:00",
                    "Salida": "2026-05-27T16:00:00",
                    "IDX": 5,
                }
            ]
        )
        service = PointAttendanceSyncService(
            bridge_settings=FakeSettings(),
            http_session_service=FakeHttpSessionService(session),
        )

        job = service.run_sync(start_date=date(2026, 5, 27), end_date=date(2026, 5, 27), branch_filter="Crucero")

        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        empleado.refresh_from_db()
        self.assertEqual(empleado.codigo, "ERP-001")
        self.assertEqual(empleado.nombre, "Laura Torres")
        self.assertEqual(Empleado.objects.count(), 1)
        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha=date(2026, 5, 27))
        self.assertIn("match=name", asistencia.observacion)
        self.assertIn("codigo_point=2543", asistencia.observacion)
        self.assertEqual(job.result_summary["attendance_matched_by_name"], 1)

    def test_run_sync_can_link_short_point_name_to_unique_full_erp_name(self):
        empleado = Empleado.objects.create(codigo="ERP-001", nombre="TORRES BURGUEÑO LAURA ELENA")
        session = FakeSession(
            attendance_rows=[
                {
                    "Codigo": "2543",
                    "Empleado": "Laura Torres",
                    "Entrada": "2026-05-27T08:00:00",
                    "Salida": "2026-05-27T16:00:00",
                    "IDX": 7,
                }
            ]
        )
        service = PointAttendanceSyncService(
            bridge_settings=FakeSettings(),
            http_session_service=FakeHttpSessionService(session),
        )

        job = service.run_sync(start_date=date(2026, 5, 27), end_date=date(2026, 5, 27), branch_filter="Crucero")

        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(Empleado.objects.count(), 1)
        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha=date(2026, 5, 27))
        self.assertIn("match=name_tokens", asistencia.observacion)
        self.assertEqual(job.result_summary["attendance_matched_by_name"], 1)

    def test_run_sync_skips_ambiguous_erp_name(self):
        Empleado.objects.create(codigo="ERP-001", nombre="Laura Torres")
        Empleado.objects.create(codigo="ERP-002", nombre="Laura Torres")
        session = FakeSession(
            attendance_rows=[
                {
                    "Codigo": "2543",
                    "Empleado": "Laura Torres",
                    "Entrada": "2026-05-27T08:00:00",
                    "Salida": "2026-05-27T16:00:00",
                    "IDX": 6,
                }
            ]
        )
        service = PointAttendanceSyncService(
            bridge_settings=FakeSettings(),
            http_session_service=FakeHttpSessionService(session),
        )

        job = service.run_sync(start_date=date(2026, 5, 27), end_date=date(2026, 5, 27), branch_filter="Crucero")

        self.assertEqual(job.status, PointSyncJob.STATUS_PARTIAL)
        self.assertEqual(Empleado.objects.count(), 2)
        self.assertEqual(AsistenciaEmpleado.objects.count(), 0)
        self.assertEqual(job.result_summary["ambiguous_employee"], 1)
