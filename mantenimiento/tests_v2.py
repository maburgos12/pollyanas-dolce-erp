from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.base import ContentFile
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.files.storage import default_storage
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from activos.models import Activo, OrdenMantenimiento, SolicitudFalla
from core.models import Sucursal, UserModuleAccess, UserProfile
from fallas.models import BitacoraFalla, CategoriaFalla, EvidenciaSeguimientoFalla, ReporteFalla
from logistica.models import ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad
from mantenimiento.services_access import (
    authorized_fallas,
    authorized_orders,
    authorized_repairs,
    authorized_unit_reports,
    authorized_unit_services,
    can_view_costs,
)
from mantenimiento.services_history import canonical_status, period_bounds
from mantenimiento.evidence_validation import EvidenceValidationError, validate_evidence_files


class EvidenceValidationTests(SimpleTestCase):
    def upload(self, name="foto.jpg", content=b"\xff\xd8\xffimagen", content_type="image/jpeg"):
        return SimpleUploadedFile(name, content, content_type=content_type)

    def test_accepts_supported_real_signatures_and_resets_stream(self):
        files = [
            self.upload("foto.jpg"),
            self.upload("foto.png", b"\x89PNG\r\n\x1a\nresto", "image/png"),
            self.upload("foto.webp", b"RIFFxxxxWEBPresto", "image/webp"),
            self.upload("manual.pdf", b"%PDF-1.7 resto", "application/pdf"),
        ]
        validated = validate_evidence_files(files)
        self.assertEqual([item.name for item in validated], ["foto.jpg", "foto.png", "foto.webp", "manual.pdf"])
        self.assertTrue(all(item.tell() == 0 for item in validated))

    def test_rejects_mime_signature_extension_size_count_and_unsafe_name(self):
        invalid_cases = [
            [self.upload("ataque.svg", b"<svg>", "image/svg+xml")],
            [self.upload("pagina.jpg", b"<html>", "image/jpeg")],
            [self.upload("foto.png", b"\xff\xd8\xffimagen", "image/png")],
            [self.upload("foto.jpg", b"\xff\xd8\xffimagen", "application/octet-stream")],
            [self.upload("programa.exe", b"MZ...", "application/octet-stream")],
            [SimpleUploadedFile("grande.jpg", b"\xff\xd8\xff", content_type="image/jpeg")],
            [self.upload(f"{index}.jpg") for index in range(6)],
        ]
        invalid_cases[5][0].size = 10 * 1024 * 1024 + 1
        for files in invalid_cases:
            with self.subTest(files=[file.name for file in files]):
                with self.assertRaises(EvidenceValidationError):
                    validate_evidence_files(files)

        safe = self.upload("../mi foto.jpg")
        self.assertEqual(validate_evidence_files([safe])[0].name, "mi_foto.jpg")

    def test_images_only_rejects_pdf_and_long_name_keeps_extension(self):
        with self.assertRaises(EvidenceValidationError):
            validate_evidence_files([self.upload("manual.pdf", b"%PDF-1.7", "application/pdf")], images_only=True)
        uploaded = self.upload(f"{'a' * 300}.jpeg")
        self.assertEqual(len(validate_evidence_files([uploaded])[0].name), 255)
        self.assertTrue(uploaded.name.endswith(".jpeg"))


@override_settings(MEDIA_ROOT="/tmp/mantenimiento-evidence-validation")
class EvidenceWritePathTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user("evidence-writer", password="test")
        Group.objects.create(name="mantenimiento").user_set.add(cls.user)
        cls.branch = Sucursal.objects.create(codigo="EVI", nombre="Evidencias")
        cls.category = CategoriaFalla.objects.create(nombre="Evidencias", tipo=CategoriaFalla.TIPO_EQUIPO)
        cls.report = ReporteFalla.objects.create(
            sucursal=cls.branch, categoria=cls.category, titulo="Original", descripcion="Original",
            foto_evidencia="fallas/evidencias/original.jpg", reportado_por=cls.user,
        )

    def setUp(self):
        self.client.force_login(self.user)

    def invalid_html(self, name="ataque.jpg"):
        return SimpleUploadedFile(name, b"<html>ataque</html>", content_type="image/jpeg")

    def test_followup_api_rejects_all_files_before_updating_or_creating_rows(self):
        before_rows = BitacoraFalla.objects.filter(reporte=self.report).count()
        response = self.client.post(
            f"/api/mantenimiento/bandeja/falla/{self.report.pk}/actualizar/",
            {"estatus": ReporteFalla.ESTATUS_RESUELTO, "comentario": "No persistir", "evidencias_seguimiento": [self.invalid_html()]},
        )
        self.report.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertTrue(response.json()["evidencias"])
        self.assertEqual(self.report.estatus, ReporteFalla.ESTATUS_ABIERTO)
        self.assertEqual(BitacoraFalla.objects.filter(reporte=self.report).count(), before_rows)
        self.assertFalse(EvidenciaSeguimientoFalla.objects.filter(bitacora__reporte=self.report).exists())

    def test_initial_form_rejects_invalid_photo_before_creating_report_and_shows_message(self):
        before = ReporteFalla.objects.count()
        response = self.client.post("/mantenimiento/nueva-falla/", {
            "sucursal": self.branch.pk, "categoria": self.category.pk, "titulo": "Nueva",
            "descripcion": "Descripción", "foto_evidencia": self.invalid_html(),
        }, follow=True)
        self.assertEqual(ReporteFalla.objects.count(), before)
        self.assertContains(response, "contenido no coincide")

    def test_initial_form_and_mobile_api_reject_pdf_as_photo(self):
        payload = {"sucursal": self.branch.pk, "categoria": self.category.pk, "titulo": "Nueva", "descripcion": "Descripción"}
        pdf = SimpleUploadedFile("manual.pdf", b"%PDF-1.7", content_type="application/pdf")
        before = ReporteFalla.objects.count()
        self.client.post("/mantenimiento/nueva-falla/", {**payload, "foto_evidencia": pdf})
        self.assertEqual(ReporteFalla.objects.count(), before)
        pdf = SimpleUploadedFile("manual.pdf", b"%PDF-1.7", content_type="application/pdf")
        response = self.client.post("/api/mantenimiento/fallas/", {**payload, "foto_evidencia": pdf})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(ReporteFalla.objects.count(), before)

    def test_second_evidence_failure_rolls_back_database_and_removes_first_blob(self):
        original_save = EvidenciaSeguimientoFalla.save
        calls = 0
        first_blob = None
        def fail_second(instance, *args, **kwargs):
            nonlocal calls, first_blob
            calls += 1
            if calls == 2:
                instance.archivo.save(instance.archivo.name, instance.archivo.file, save=False)
                raise RuntimeError("fallo inyectado")
            try:
                return original_save(instance, *args, **kwargs)
            finally:
                first_blob = instance.archivo
        files = [
            SimpleUploadedFile("uno.jpg", b"\xff\xd8\xffuno", content_type="image/jpeg"),
            SimpleUploadedFile("dos.jpg", b"\xff\xd8\xffdos", content_type="image/jpeg"),
        ]
        before_rows = BitacoraFalla.objects.filter(reporte=self.report).count()
        with patch.object(EvidenciaSeguimientoFalla, "save", new=fail_second):
            with self.assertRaises(RuntimeError):
                self.client.post(f"/api/mantenimiento/bandeja/falla/{self.report.pk}/actualizar/", {
                    "estatus": ReporteFalla.ESTATUS_RESUELTO, "evidencias_seguimiento": files,
                })
        self.report.refresh_from_db()
        self.assertEqual(self.report.estatus, ReporteFalla.ESTATUS_ABIERTO)
        self.assertEqual(BitacoraFalla.objects.filter(reporte=self.report).count(), before_rows)
        self.assertFalse(EvidenciaSeguimientoFalla.objects.filter(bitacora__reporte=self.report).exists())
        self.assertIsNotNone(first_blob)
        self.assertFalse(first_blob.name)

    def test_initial_photo_is_removed_when_bitacora_creation_fails_in_form_and_api(self):
        payload = {"sucursal": self.branch.pk, "categoria": self.category.pk, "titulo": "Nueva", "descripcion": "Descripción"}
        media = Path("/tmp/mantenimiento-evidence-validation")
        before_files = set(media.rglob("*")) if media.exists() else set()
        for url in ("/mantenimiento/nueva-falla/", "/api/mantenimiento/fallas/"):
            photo = SimpleUploadedFile("atomica.jpg", b"\xff\xd8\xfffoto", content_type="image/jpeg")
            before_rows = ReporteFalla.objects.count()
            with patch.object(BitacoraFalla.objects, "create", side_effect=RuntimeError("bitácora falla")):
                with self.assertRaises(RuntimeError):
                    self.client.post(url, {**payload, "foto_evidencia": photo})
            self.assertEqual(ReporteFalla.objects.count(), before_rows)
            current_files = set(media.rglob("*")) if media.exists() else set()
            self.assertEqual({path for path in current_files if path.is_file()}, {path for path in before_files if path.is_file()})

    def test_storage_failure_does_not_delete_preexisting_homonymous_blob(self):
        existing_name = default_storage.save("fallas/evidencias/segura.jpg", ContentFile(b"existente"))
        before_rows = ReporteFalla.objects.count()
        payload = {"sucursal": self.branch.pk, "categoria": self.category.pk, "titulo": "Nueva", "descripcion": "Descripción"}
        photo = SimpleUploadedFile("segura.jpg", b"\xff\xd8\xffnueva", content_type="image/jpeg")
        storage_class = default_storage._wrapped.__class__
        with patch.object(storage_class, "save", side_effect=OSError("storage no disponible")):
            with self.assertRaises(OSError):
                self.client.post("/mantenimiento/nueva-falla/", {**payload, "foto_evidencia": photo})
        self.assertEqual(ReporteFalla.objects.count(), before_rows)
        self.assertTrue(default_storage.exists(existing_name))
        self.assertEqual(default_storage.open(existing_name).read(), b"existente")
        default_storage.delete(existing_name)


class MaintenanceHistoryDomainTests(SimpleTestCase):
    def test_30d_uses_mazatlan_inclusive_start_exclusive_end(self):
        now = datetime(2026, 7, 11, 15, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("30d", now=now)

        self.assertEqual(start.isoformat(), "2026-06-12T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-12T00:00:00-07:00")

    def test_90d_uses_inclusive_start_and_exclusive_end(self):
        now = datetime(2026, 7, 11, 15, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("90d", now=now)

        self.assertEqual(start.isoformat(), "2026-04-13T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-12T00:00:00-07:00")

    def test_week_starts_on_monday_and_ends_next_monday(self):
        now = datetime(2026, 7, 8, 12, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("semana", now=now)

        self.assertEqual(start.isoformat(), "2026-07-06T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-13T00:00:00-07:00")

    def test_week_on_sunday_keeps_same_monday_boundaries(self):
        now = datetime(2026, 7, 12, 23, 59, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("semana", now=now)

        self.assertEqual(start.isoformat(), "2026-07-06T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-13T00:00:00-07:00")

    def test_month_uses_first_day_and_next_month_exclusive(self):
        now = datetime(2026, 7, 31, 23, 59, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("mes", now=now)

        self.assertEqual(start.isoformat(), "2026-07-01T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-08-01T00:00:00-07:00")

    def test_todo_has_no_start_and_ends_after_current_local_day(self):
        now = datetime(2026, 7, 11, 15, 0, tzinfo=ZoneInfo("America/Mazatlan"))

        start, end = period_bounds("todo", now=now)

        self.assertIsNone(start)
        self.assertEqual(end.isoformat(), "2026-07-12T00:00:00-07:00")

    def test_invalid_period_raises_value_error(self):
        with self.assertRaisesMessage(ValueError, "Periodo no soportado"):
            period_bounds("trimestre")

    def test_source_statuses_map_without_losing_programmed(self):
        self.assertEqual(canonical_status("orden", "PENDIENTE"), "abierto")
        self.assertEqual(canonical_status("orden", "EN_PROCESO"), "en_proceso")
        self.assertEqual(canonical_status("orden", "CERRADA"), "cerrado")
        self.assertEqual(canonical_status("orden", "CANCELADA"), "cancelado")
        self.assertEqual(canonical_status("reporte_unidad", "ABIERTO"), "abierto")
        self.assertEqual(canonical_status("reporte_unidad", "EN_PROCESO"), "en_proceso")
        self.assertEqual(canonical_status("reporte_unidad", "PROGRAMADO"), "programado")
        self.assertEqual(canonical_status("reporte_unidad", "CERRADO"), "cerrado")
        self.assertEqual(canonical_status("reporte_unidad", "CANCELADO"), "cancelado")


class MaintenanceAccessTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        user_model = get_user_model()
        cls.own_branch = Sucursal.objects.create(codigo="OWN", nombre="Propia")
        cls.other_branch = Sucursal.objects.create(codigo="OTHER", nombre="Ajena")
        cls.limited_user = user_model.objects.create_user("limited", password="test")
        cls.no_scope_user = user_model.objects.create_user("no-scope", password="test")
        cls.no_permission_user = user_model.objects.create_user("no-permission", password="test")
        cls.inactive_user = user_model.objects.create_user("inactive", password="test", is_active=False)
        cls.manager = user_model.objects.create_user("manager", password="test")
        cls.maintenance_group_user = user_model.objects.create_user("maintenance-group", password="test")
        cls.assets_view_user = user_model.objects.create_user("assets-view", password="test")
        cls.inbox_manage_user = user_model.objects.create_user("inbox-manage", password="test")
        cls.app_view_user = user_model.objects.create_user("app-view", password="test")
        cls.dashboard_view_user = user_model.objects.create_user("dashboard-view", password="test")
        cls.dg_user = user_model.objects.create_user("dg-user", password="test")
        cls.admin_group_user = user_model.objects.create_user("admin-group-user", password="test")
        cls.admin = user_model.objects.create_superuser("global", "global@example.com", "test")
        cls.dg_user.groups.add(Group.objects.create(name="DG"))
        cls.admin_group_user.groups.add(Group.objects.create(name="ADMIN"))
        cls.maintenance_group_user.groups.add(Group.objects.create(name="mantenimiento"))
        UserProfile.objects.create(user=cls.limited_user, sucursal=cls.own_branch)
        UserProfile.objects.create(user=cls.no_scope_user)
        UserProfile.objects.create(user=cls.no_permission_user, sucursal=cls.own_branch)
        UserProfile.objects.create(user=cls.inactive_user, sucursal=cls.own_branch)
        UserModuleAccess.objects.create(user=cls.limited_user, module="mantenimiento", access="view")
        UserModuleAccess.objects.create(user=cls.manager, module="mantenimiento", access="manage")
        UserModuleAccess.objects.create(user=cls.assets_view_user, module="activos", access="view")
        UserModuleAccess.objects.create(user=cls.inbox_manage_user, module="mantenimiento.bandeja", access="manage")
        UserModuleAccess.objects.create(user=cls.app_view_user, module="mantenimiento.app", access="view")
        UserModuleAccess.objects.create(user=cls.dashboard_view_user, module="mantenimiento.dashboard", access="view")

        category = CategoriaFalla.objects.create(nombre="General")
        cls.own_report = ReporteFalla.objects.create(
            sucursal=cls.own_branch, categoria=category, titulo="Propia", descripcion="x",
            foto_evidencia="fallas/evidencias/own.jpg", reportado_por=cls.limited_user,
        )
        cls.other_report = ReporteFalla.objects.create(
            sucursal=cls.other_branch, categoria=category, titulo="Ajena", descripcion="x",
            foto_evidencia="fallas/evidencias/other.jpg", reportado_por=cls.limited_user,
        )
        own_asset = Activo.objects.create(nombre="Horno propio", sucursal=cls.own_branch)
        other_asset = Activo.objects.create(nombre="Horno ajeno", sucursal=cls.other_branch)
        cls.own_order = OrdenMantenimiento.objects.create(activo_ref=own_asset)
        cls.other_order = OrdenMantenimiento.objects.create(activo_ref=other_asset)
        own_unit = Unidad.objects.create(codigo="U-OWN", descripcion="Propia", sucursal=cls.own_branch)
        other_unit = Unidad.objects.create(codigo="U-OTHER", descripcion="Ajena", sucursal=cls.other_branch)
        cls.own_unit_report = ReporteUnidad.objects.create(unidad=own_unit, tipo="falla", descripcion="x")
        cls.other_unit_report = ReporteUnidad.objects.create(unidad=other_unit, tipo="falla", descripcion="x")
        service_type = TipoServicioUnidad.objects.create(nombre="Servicio")
        cls.own_service = ServicioRealizadoUnidad.objects.create(unidad=own_unit, tipo_servicio=service_type, fecha_servicio="2026-07-11")
        cls.other_service = ServicioRealizadoUnidad.objects.create(unidad=other_unit, tipo_servicio=service_type, fecha_servicio="2026-07-11")
        cls.own_repair = ReparacionUnidad.objects.create(unidad=own_unit, fecha_ingreso="2026-07-11", descripcion_falla="x")
        cls.other_repair = ReparacionUnidad.objects.create(unidad=other_unit, fecha_ingreso="2026-07-11", descripcion_falla="x")

    def test_limited_user_only_resolves_objects_from_profile_branch_fk(self):
        pairs = [
            (authorized_fallas, self.own_report, self.other_report),
            (authorized_orders, self.own_order, self.other_order),
            (authorized_unit_reports, self.own_unit_report, self.other_unit_report),
            (authorized_repairs, self.own_repair, self.other_repair),
            (authorized_unit_services, self.own_service, self.other_service),
        ]
        for authorize, own, other in pairs:
            with self.subTest(authorize=authorize.__name__):
                qs = authorize(self.limited_user)
                self.assertTrue(qs.filter(pk=own.pk).exists())
                self.assertFalse(qs.filter(pk=other.pk).exists())

    def test_global_and_mantenimiento_manager_see_all_branches(self):
        for user in (self.admin, self.dg_user, self.admin_group_user, self.manager):
            with self.subTest(user=user.username):
                self.assertEqual(authorized_fallas(user).count(), 2)
                self.assertEqual(authorized_orders(user).count(), 2)
                self.assertEqual(authorized_unit_reports(user).count(), 2)
                self.assertEqual(authorized_repairs(user).count(), 2)
                self.assertEqual(authorized_unit_services(user).count(), 2)

    def test_non_global_user_without_branch_scope_sees_no_data(self):
        UserModuleAccess.objects.create(user=self.no_scope_user, module="activos", access="view")
        self._assert_no_authorized_objects(self.no_scope_user)

    def test_user_with_branch_but_without_mantenimiento_permission_sees_nothing(self):
        self._assert_no_authorized_objects(self.no_permission_user)

    def test_inactive_user_with_branch_sees_nothing(self):
        self._assert_no_authorized_objects(self.inactive_user)

    def test_every_real_read_gate_returns_endpoint_data(self):
        users = (
            self.maintenance_group_user,
            self.assets_view_user,
            self.inbox_manage_user,
            self.app_view_user,
            self.dashboard_view_user,
        )
        for user in users:
            with self.subTest(user=user.username):
                self.client.force_login(user)
                response = self.client.get("/api/mantenimiento/v2/bandeja/", {"periodo": "todo"})
                self.assertEqual(response.status_code, 200)
                self.assertGreater(response.json()["pagination"]["total"], 0)

    def test_user_without_any_real_read_gate_gets_403(self):
        self.client.force_login(self.no_permission_user)
        response = self.client.get("/api/mantenimiento/v2/bandeja/")
        self.assertEqual(response.status_code, 403)

    def _assert_no_authorized_objects(self, user):
        self.assertFalse(authorized_fallas(user).exists())
        self.assertFalse(authorized_orders(user).exists())
        self.assertFalse(authorized_unit_reports(user).exists())
        self.assertFalse(authorized_repairs(user).exists())
        self.assertFalse(authorized_unit_services(user).exists())

    def test_only_global_or_mantenimiento_manager_can_view_costs(self):
        self.assertTrue(can_view_costs(self.admin))
        self.assertTrue(can_view_costs(self.dg_user))
        self.assertTrue(can_view_costs(self.admin_group_user))
        self.assertTrue(can_view_costs(self.manager))
        self.assertFalse(can_view_costs(self.limited_user))
        self.assertFalse(can_view_costs(self.no_scope_user))
        self.assertFalse(can_view_costs(self.no_permission_user))
        self.assertFalse(can_view_costs(self.inactive_user))


@override_settings(TIME_ZONE="America/Mazatlan")
class MaintenanceInboxV2Tests(TestCase):
    @classmethod
    def setUpTestData(cls):
        user_model = get_user_model()
        cls.branch = Sucursal.objects.create(codigo="V2", nombre="Sucursal V2")
        cls.other_branch = Sucursal.objects.create(codigo="V2-OTHER", nombre="Sucursal ajena")
        cls.user = user_model.objects.create_superuser("v2-admin", "v2@example.com", "test")
        cls.reporter = user_model.objects.create_user("v2-reporter", password="test")
        cls.category = CategoriaFalla.objects.create(nombre="General V2")
        cls.asset = Activo.objects.create(nombre="Horno V2", sucursal=cls.branch)
        cls.other_asset = Activo.objects.create(nombre="Horno ajeno V2", sucursal=cls.other_branch)
        cls.unit = Unidad.objects.create(codigo="U-V2", descripcion="Unidad V2", sucursal=cls.branch)

    def setUp(self):
        self.client.force_login(self.user)

    def _closed_falla(self, days_ago, *, branch=None, priority=ReporteFalla.PRIORIDAD_MEDIA):
        event = timezone.now() - timedelta(days=days_ago)
        report = ReporteFalla.objects.create(
            sucursal=branch or self.branch,
            categoria=self.category,
            titulo=f"Falla {days_ago}",
            descripcion="x",
            prioridad=priority,
            estatus=ReporteFalla.ESTATUS_CERRADO,
            foto_evidencia=f"fallas/evidencias/{days_ago}.jpg",
            reportado_por=self.reporter,
            fecha_cierre=event,
        )
        ReporteFalla.objects.filter(pk=report.pk).update(fecha_reporte=event)
        return report

    def _closed_order(self, days_ago):
        event = timezone.localdate() - timedelta(days=days_ago)
        return OrdenMantenimiento.objects.create(
            activo_ref=self.asset,
            estatus=OrdenMantenimiento.ESTATUS_CERRADA,
            fecha_cierre=event,
            descripcion=f"Orden {days_ago}",
        )

    def _closed_unit_report(self, days_ago, *, reported_days_ago=None, with_close_date=True):
        report = ReporteUnidad.objects.create(
            unidad=self.unit,
            tipo=ReporteUnidad.TIPO_FALLA,
            descripcion=f"Unidad {days_ago}",
            estatus=ReporteUnidad.ESTATUS_CERRADO,
        )
        updates = {"fecha_reporte": timezone.now() - timedelta(days=reported_days_ago or days_ago)}
        if with_close_date:
            updates["fecha_cierre"] = timezone.now() - timedelta(days=days_ago)
        else:
            updates["fecha_cierre"] = None
        ReporteUnidad.objects.filter(pk=report.pk).update(**updates)
        return report

    def test_closed_count_is_independent_from_page_size_and_includes_all_sources(self):
        self._closed_falla(2)
        self._closed_order(3)
        self._closed_unit_report(4)

        response = self.client.get("/api/mantenimiento/v2/bandeja/", {
            "estado": "cerrados", "periodo": "30d", "page_size": 1,
        })

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["schema_version"], 2)
        self.assertEqual(payload["counts"]["cerrados"], 3)
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(payload["pagination"]["total"], 3)
        self.assertTrue(payload["pagination"]["has_next"])

    def test_unit_report_uses_real_close_date_not_old_report_date(self):
        report = self._closed_unit_report(2, reported_days_ago=45)

        payload = self.client.get("/api/mantenimiento/v2/bandeja/", {
            "estado": "cerrados", "periodo": "30d", "origen": "logistica",
        }).json()

        self.assertEqual(payload["counts"]["cerrados"], 1)
        self.assertEqual(payload["results"][0]["uid"], f"reporte_unidad:{report.pk}")

    def test_legacy_closed_unit_without_close_date_is_excluded_from_30d(self):
        self._closed_unit_report(2, with_close_date=False)

        payload = self.client.get("/api/mantenimiento/v2/bandeja/", {
            "estado": "cerrados", "periodo": "30d", "origen": "logistica",
        }).json()

        self.assertEqual(payload["counts"]["cerrados"], 0)
        self.assertEqual(payload["results"], [])

    def test_unit_close_date_is_set_once_and_cleared_on_reopen(self):
        report = ReporteUnidad.objects.create(
            unidad=self.unit, tipo=ReporteUnidad.TIPO_FALLA, descripcion="Transiciones",
        )
        report.estatus = ReporteUnidad.ESTATUS_CERRADO
        report.save(update_fields=["estatus", "actualizado_en"])
        first_close = report.fecha_cierre
        self.assertIsNotNone(first_close)

        report.descripcion = "Editado después del cierre"
        report.save(update_fields=["descripcion", "actualizado_en"])
        report.refresh_from_db()
        self.assertEqual(report.fecha_cierre, first_close)

        report.estatus = ReporteUnidad.ESTATUS_EN_PROCESO
        report.save(update_fields=["estatus", "actualizado_en"])
        report.refresh_from_db()
        self.assertIsNone(report.fecha_cierre)

    def test_stale_instance_saving_other_field_does_not_reopen_or_clear_close_date(self):
        report = ReporteUnidad.objects.create(
            unidad=self.unit, tipo=ReporteUnidad.TIPO_FALLA, descripcion="Original",
        )
        stale = ReporteUnidad.objects.get(pk=report.pk)
        report.estatus = ReporteUnidad.ESTATUS_CERRADO
        report.save(update_fields=["estatus", "actualizado_en"])
        close_date = report.fecha_cierre

        stale.descripcion = "Edición concurrente"
        stale.save(update_fields=["descripcion", "actualizado_en"])

        stale.refresh_from_db()
        self.assertEqual(stale.estatus, ReporteUnidad.ESTATUS_CERRADO)
        self.assertEqual(stale.fecha_cierre, close_date)

    def test_maintenance_write_path_persists_real_unit_close_date(self):
        report = ReporteUnidad.objects.create(
            unidad=self.unit, tipo=ReporteUnidad.TIPO_FALLA, descripcion="Cierre desde mantenimiento",
        )

        response = self.client.post(
            f"/api/mantenimiento/bandeja/unidad/{report.pk}/actualizar/",
            {"estatus": ReporteUnidad.ESTATUS_CERRADO},
        )

        self.assertEqual(response.status_code, 200)
        report.refresh_from_db()
        self.assertIsNotNone(report.fecha_cierre)

    def test_30d_includes_29_days_but_excludes_31_days_cancelled_and_other_branch(self):
        included = self._closed_falla(29)
        self._closed_falla(31)
        cancelled = self._closed_falla(2)
        ReporteFalla.objects.filter(pk=cancelled.pk).update(estatus=ReporteFalla.ESTATUS_CANCELADO)
        self._closed_falla(2, branch=self.other_branch)
        limited = get_user_model().objects.create_user("v2-limited", password="test")
        UserProfile.objects.create(user=limited, sucursal=self.branch)
        UserModuleAccess.objects.create(user=limited, module="mantenimiento", access="view")
        self.client.force_login(limited)

        payload = self.client.get("/api/mantenimiento/v2/bandeja/", {
            "estado": "cerrados", "periodo": "30d",
        }).json()

        self.assertEqual(payload["counts"]["cerrados"], 1)
        self.assertEqual([row["uid"] for row in payload["results"]], [f"falla:{included.pk}"])

    def test_counts_are_for_filtered_unpaginated_set(self):
        self._closed_falla(1, priority=ReporteFalla.PRIORIDAD_CRITICA)
        ReporteFalla.objects.create(
            sucursal=self.branch, categoria=self.category, titulo="Abierta", descripcion="x",
            prioridad=ReporteFalla.PRIORIDAD_CRITICA, foto_evidencia="fallas/evidencias/open.jpg",
            reportado_por=self.reporter, estatus=ReporteFalla.ESTATUS_PROCESO,
        )

        payload = self.client.get("/api/mantenimiento/v2/bandeja/", {
            "estado": "todos", "periodo": "30d", "page_size": 1,
        }).json()

        self.assertEqual(payload["counts"], {"abiertos": 1, "en_proceso": 1, "criticos": 2, "cerrados": 1})
        self.assertEqual(payload["pagination"]["total"], 2)

    def test_invalid_enums_and_pagination_are_rejected(self):
        cases = [("estado", "x"), ("periodo", "x"), ("origen", "x"), ("page", "0"), ("page_size", "0")]
        for key, value in cases:
            with self.subTest(key=key):
                response = self.client.get("/api/mantenimiento/v2/bandeja/", {key: value})
                self.assertEqual(response.status_code, 400)

    def test_page_size_is_capped_and_results_have_deterministic_order(self):
        first = self._closed_falla(1)
        second = self._closed_falla(1)

        payload = self.client.get("/api/mantenimiento/v2/bandeja/", {
            "estado": "cerrados", "periodo": "30d", "page_size": 500,
        }).json()

        self.assertEqual(payload["pagination"]["page_size"], 100)
        self.assertEqual([row["uid"] for row in payload["results"]], [f"falla:{second.pk}", f"falla:{first.pk}"])

    def test_query_count_does_not_grow_with_result_count(self):
        self._closed_falla(1)
        with self.assertNumQueries(9):
            self.client.get("/api/mantenimiento/v2/bandeja/", {"estado": "cerrados", "periodo": "30d"})
        for index in range(19):
            self._closed_falla(1)
        with self.assertNumQueries(9):
            self.client.get("/api/mantenimiento/v2/bandeja/", {"estado": "cerrados", "periodo": "30d"})


class MaintenanceUnifiedHistoryV2Tests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_superuser("history-admin", "history@example.com", "test")
        cls.actor = get_user_model().objects.create_user("history-actor", first_name="Isaac")
        cls.branch = Sucursal.objects.create(codigo="HIS", nombre="Historial")
        cls.asset = Activo.objects.create(nombre="Horno historial", sucursal=cls.branch)
        cls.unit = Unidad.objects.create(codigo="HIS-01", descripcion="Camioneta historial", sucursal=cls.branch)
        cls.category = CategoriaFalla.objects.create(nombre="Historial")
        cls.service_type = TipoServicioUnidad.objects.create(nombre="Afinación")

    def setUp(self):
        self.client.force_login(self.user)

    def test_unifies_all_sources_without_duplicate_uids_and_keeps_real_relationships(self):
        falla = ReporteFalla.objects.create(
            sucursal=self.branch, categoria=self.category, titulo="Falla", descripcion="x",
            reportado_por=self.actor, foto_evidencia="fallas/evidencias/history.jpg",
        )
        order = OrdenMantenimiento.objects.create(
            activo_ref=self.asset, origen=OrdenMantenimiento.ORIGEN_EMERGENCIA,
            creado_por=self.actor, descripcion="Emergencia", numero_factura="OM-FACT",
            factura_archivo="activos/facturas/orden.pdf", costo_otros="120.00",
        )
        report = ReporteUnidad.objects.create(unidad=self.unit, tipo="falla", descripcion="Motor")
        repair = ReparacionUnidad.objects.create(
            unidad=self.unit, reporte_origen=report, fecha_ingreso=timezone.localdate(),
            descripcion_falla="Motor", registrado_por=self.actor,
            archivo_factura="reparaciones_unidad/reparacion.pdf", costo_total="250.00",
        )
        service = ServicioRealizadoUnidad.objects.create(
            unidad=self.unit, tipo_servicio=self.service_type, fecha_servicio=timezone.localdate(),
            registrado_por=None, archivo_factura="servicios_unidad/factura.pdf",
        )

        payload = self.client.get("/api/mantenimiento/v2/historial/", {"periodo": "todo"}).json()

        uids = [row["uid"] for row in payload["results"]]
        self.assertEqual(len(uids), len(set(uids)))
        rows = {row["uid"]: row for row in payload["results"]}
        self.assertEqual(len(rows), payload["pagination"]["total"])
        self.assertTrue({f"falla:{falla.pk}", f"orden:{order.pk}", f"reporte_unidad:{report.pk}",
                         f"reparacion:{repair.pk}", f"servicio_unidad:{service.pk}"}.issubset(rows))
        self.assertEqual(rows[f"orden:{order.pk}"]["origen"], "sin_reporte")
        self.assertTrue(rows[f"orden:{order.pk}"]["captura_directa"])
        self.assertEqual(rows[f"reparacion:{repair.pk}"]["parent_uid"], f"reporte_unidad:{report.pk}")
        self.assertFalse(rows[f"reparacion:{repair.pk}"]["captura_directa"])
        self.assertEqual(rows[f"servicio_unidad:{service.pk}"]["actor"], {"id": None, "label": "Sin autor registrado"})
        for uid, kind in ((f"orden:{order.pk}", "orden_factura"),
                          (f"reparacion:{repair.pk}", "reparacion_factura"),
                          (f"servicio_unidad:{service.pk}", "servicio_unidad_factura")):
            self.assertIn(f"/evidencias/{kind}/", rows[uid]["factura"]["url"])
        self.assertEqual(rows[f"orden:{order.pk}"]["costo"], "120.00")

        direct_repair = ReparacionUnidad.objects.create(
            unidad=self.unit, fecha_ingreso=timezone.localdate(), descripcion_falla="Directa",
            registrado_por=None,
        )
        payload = self.client.get("/api/mantenimiento/v2/historial/", {"periodo": "todo"}).json()
        direct = next(row for row in payload["results"] if row["uid"] == f"reparacion:{direct_repair.pk}")
        self.assertTrue(direct["captura_directa"])
        self.assertIsNone(direct["parent_uid"])

        limited = get_user_model().objects.create_user("history-limited", password="test")
        UserProfile.objects.create(user=limited, sucursal=self.branch)
        UserModuleAccess.objects.create(user=limited, module="mantenimiento", access="view")
        self.client.force_login(limited)
        hidden = self.client.get("/api/mantenimiento/v2/historial/", {"periodo": "todo"}).json()
        self.assertTrue(all(row["costo"] is None for row in hidden["results"]))

    def test_filters_type_state_scope_search_period_and_stable_pagination(self):
        recent = ServicioRealizadoUnidad.objects.create(
            unidad=self.unit, tipo_servicio=self.service_type,
            fecha_servicio=timezone.localdate() - timedelta(days=29), registrado_por=self.actor,
        )
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unit, tipo_servicio=self.service_type,
            fecha_servicio=timezone.localdate() - timedelta(days=31), registrado_por=self.actor,
        )
        first = self.client.get("/api/mantenimiento/v2/historial/", {
            "tipo": "servicio_unidad", "estado": "cerrado", "periodo": "30d",
            "sucursal": self.branch.pk, "unidad": self.unit.pk, "q": "afinación",
            "page": 1, "page_size": 1,
        }).json()
        second = self.client.get("/api/mantenimiento/v2/historial/", {
            "tipo": "servicio_unidad", "estado": "cerrado", "periodo": "30d",
            "sucursal": self.branch.pk, "unidad": self.unit.pk, "q": "afinación",
            "page": 1, "page_size": 1,
        }).json()
        self.assertEqual(first["pagination"]["total"], 1)
        self.assertEqual(first["results"][0]["uid"], f"servicio_unidad:{recent.pk}")
        self.assertEqual(first["results"], second["results"])

    def test_rejects_invalid_history_filters(self):
        for key, value in (("tipo", "x"), ("estado", "x"), ("periodo", "x"), ("page", "0")):
            with self.subTest(key=key):
                self.assertEqual(self.client.get("/api/mantenimiento/v2/historial/", {key: value}).status_code, 400)

    def test_scope_linked_orders_exclusive_types_and_multi_page_stability(self):
        linked = OrdenMantenimiento.objects.create(
            activo_ref=self.asset, origen=OrdenMantenimiento.ORIGEN_EMERGENCIA,
            creado_por=None, descripcion="Orden con solicitud",
        )
        SolicitudFalla.objects.create(
            activo_ref=self.asset, descripcion="Solicitud real", reportado_por=self.actor,
            orden_atencion=linked,
        )
        other_branch = Sucursal.objects.create(codigo="HIS-OTHER", nombre="Historial ajeno")
        other_unit = Unidad.objects.create(codigo="HIS-OTHER", descripcion="Ajena", sucursal=other_branch)
        ServicioRealizadoUnidad.objects.create(
            unidad=other_unit, tipo_servicio=self.service_type, fecha_servicio=timezone.localdate(),
            registrado_por=None,
        )
        for days in range(7):
            ServicioRealizadoUnidad.objects.create(
                unidad=self.unit, tipo_servicio=self.service_type,
                fecha_servicio=timezone.localdate() - timedelta(days=days), registrado_por=None,
            )
        limited = get_user_model().objects.create_user("history-page-limited", password="test")
        UserProfile.objects.create(user=limited, sucursal=self.branch)
        UserModuleAccess.objects.create(user=limited, module="mantenimiento", access="view")
        self.client.force_login(limited)

        all_rows = self.client.get("/api/mantenimiento/v2/historial/", {"periodo": "todo", "page_size": 100}).json()["results"]
        self.assertTrue(all(row["sucursal"]["id"] == self.branch.pk for row in all_rows))
        linked_row = next(row for row in all_rows if row["uid"] == f"orden:{linked.pk}")
        self.assertNotEqual(linked_row["origen"], "sin_reporte")
        self.assertFalse(linked_row["captura_directa"])

        typed_uids = {}
        for kind in ("reporte", "orden", "reparacion", "servicio_unidad", "sin_reporte"):
            response = self.client.get("/api/mantenimiento/v2/historial/", {
                "tipo": kind, "periodo": "todo", "page_size": 100,
            }).json()
            typed_uids[kind] = {row["uid"] for row in response["results"]}
        self.assertFalse(typed_uids["orden"] & typed_uids["reporte"])
        self.assertFalse(typed_uids["sin_reporte"] & typed_uids["orden"])

        params = {"tipo": "servicio_unidad", "periodo": "todo", "page_size": 3}
        page1 = self.client.get("/api/mantenimiento/v2/historial/", {**params, "page": 1}).json()
        page2 = self.client.get("/api/mantenimiento/v2/historial/", {**params, "page": 2}).json()
        uids1 = [row["uid"] for row in page1["results"]]
        uids2 = [row["uid"] for row in page2["results"]]
        self.assertEqual(page1["pagination"]["total"], 7)
        self.assertFalse(set(uids1) & set(uids2))
        self.assertEqual(uids1, [row["uid"] for row in self.client.get(
            "/api/mantenimiento/v2/historial/", {**params, "page": 1}
        ).json()["results"]])


@override_settings(MEDIA_ROOT="/tmp/mantenimiento-v2-test-media")
class MaintenanceDetailV2Tests(TestCase):
    @classmethod
    def setUpTestData(cls):
        users = get_user_model()
        cls.branch = Sucursal.objects.create(codigo="DET", nombre="Detalle")
        cls.other_branch = Sucursal.objects.create(codigo="DET2", nombre="Ajena")
        cls.user = users.objects.create_user("detail", password="test", first_name="Lector")
        cls.denied = users.objects.create_user("denied", password="test")
        cls.no_profile = users.objects.create_user("no-profile", password="test")
        cls.reporter = users.objects.create_user("reporter", password="test", first_name="Reportante", last_name="QA")
        UserProfile.objects.create(user=cls.user, sucursal=cls.branch)
        UserProfile.objects.create(user=cls.denied, sucursal=cls.branch)
        UserModuleAccess.objects.create(user=cls.user, module="mantenimiento", access="view")
        UserModuleAccess.objects.create(user=cls.no_profile, module="activos", access="view")
        category = CategoriaFalla.objects.create(nombre="Electricidad", tipo=CategoriaFalla.TIPO_EQUIPO)
        asset = Activo.objects.create(nombre="Horno", sucursal=cls.branch)
        cls.report = ReporteFalla.objects.create(
            sucursal=cls.branch, categoria=category, activo_relacionado=asset, area=ReporteFalla.AREA_PRODUCCION,
            titulo="No enciende", descripcion="Sin corriente", prioridad=ReporteFalla.PRIORIDAD_ALTA,
            foto_evidencia="fallas/evidencias/inicial.jpg", reportado_por=cls.reporter,
        )
        cls.other_report = ReporteFalla.objects.create(
            sucursal=cls.other_branch, categoria=category, titulo="Ajena", descripcion="x",
            foto_evidencia="fallas/evidencias/ajena.jpg", reportado_por=cls.reporter,
        )
        cls.old = BitacoraFalla.objects.create(
            reporte=cls.report, usuario=cls.reporter, estatus_anterior="abierto",
            estatus_nuevo="en_proceso", comentario="Primero",
        )
        cls.new = BitacoraFalla.objects.create(
            reporte=cls.report, usuario=cls.user, estatus_anterior="en_proceso",
            estatus_nuevo="resuelto", comentario="Después",
        )
        EvidenciaSeguimientoFalla.objects.create(
            bitacora=cls.old, archivo="fallas/seguimiento/avance.jpg", nombre="avance.jpg", subido_por=cls.reporter,
        )
        EvidenciaSeguimientoFalla.objects.create(
            bitacora=cls.old, archivo="fallas/seguimiento/dos.pdf", nombre="dos.pdf", subido_por=cls.reporter,
        )

    def test_falla_detail_contains_identity_nulls_initial_photo_and_ordered_timeline(self):
        self.client.force_login(self.user)
        data = self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.pk}/").json()
        self.assertEqual(data["uid"], f"falla:{self.report.pk}")
        self.assertEqual(data["estado"]["codigo"], "abierto")
        self.assertEqual(data["prioridad"], {"codigo": "alta", "etiqueta": "Alta"})
        self.assertEqual(data["reporte_inicial"]["reportado_por"]["nombre"], "Reportante QA")
        self.assertEqual(data["reporte_inicial"]["foto"]["url"], f"/api/mantenimiento/v2/evidencias/falla_inicial/{self.report.pk}/")
        self.assertIsNone(data["fechas"]["asignacion"])
        self.assertIsNone(data["responsables"]["asignado_a"])
        self.assertEqual([row["id"] for row in data["seguimiento"]], [self.old.pk, self.new.pk])
        self.assertEqual([row["nombre"] for row in data["seguimiento"][0]["evidencias"]], ["avance.jpg", "dos.pdf"])
        self.assertNotIn("notas_internas", data)

    def test_detail_rejects_anonymous_permissionless_other_branch_unknown_type_and_missing_id(self):
        self.client.logout()
        self.assertIn(self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.pk}/").status_code, {401, 403})
        self.client.force_login(self.denied)
        self.assertEqual(self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.pk}/").status_code, 403)
        self.client.force_login(self.user)
        self.assertEqual(self.client.get(f"/api/mantenimiento/v2/items/falla/{self.other_report.pk}/").status_code, 404)
        self.assertEqual(self.client.get(f"/api/mantenimiento/v2/items/desconocido/{self.report.pk}/").status_code, 404)
        self.assertEqual(self.client.get("/api/mantenimiento/v2/items/falla/999999/").status_code, 404)
        self.client.force_login(self.no_profile)
        self.assertEqual(self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.pk}/").status_code, 404)

    def test_all_declared_detail_types_resolve_authorized_objects(self):
        asset = Activo.objects.create(nombre="Batidora", sucursal=self.branch)
        unit = Unidad.objects.create(codigo="DET-U", descripcion="Unidad", sucursal=self.branch)
        service_type = TipoServicioUnidad.objects.create(nombre="Afinación")
        objects = {
            "orden": OrdenMantenimiento.objects.create(activo_ref=asset),
            "reporte_unidad": ReporteUnidad.objects.create(unidad=unit, tipo="falla", descripcion="Motor"),
            "reparacion": ReparacionUnidad.objects.create(unidad=unit, fecha_ingreso="2026-07-11", descripcion_falla="Motor"),
            "servicio_unidad": ServicioRealizadoUnidad.objects.create(unidad=unit, tipo_servicio=service_type, fecha_servicio="2026-07-11"),
        }
        self.client.force_login(self.user)
        for kind, obj in objects.items():
            with self.subTest(kind=kind):
                payload = self.client.get(f"/api/mantenimiento/v2/items/{kind}/{obj.pk}/").json()
                self.assertEqual(payload["uid"], f"{kind}:{obj.pk}")
                self.assertIn("detalle", payload)

    def test_multiple_timeline_rows_keep_fixed_query_budget(self):
        self.client.force_login(self.user)
        with self.assertNumQueries(10):
            self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.pk}/")
        for index in range(5):
            row = BitacoraFalla.objects.create(reporte=self.report, usuario=self.user, comentario=str(index))
            EvidenciaSeguimientoFalla.objects.create(bitacora=row, archivo=f"fallas/seguimiento/{index}.jpg", subido_por=self.user)
        with self.assertNumQueries(10):
            self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.pk}/")


@override_settings(MEDIA_ROOT="/tmp/mantenimiento-v2-test-media")
class MaintenanceEvidenceV2Tests(MaintenanceDetailV2Tests):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.user)
        self.report.foto_evidencia.save("initial.jpg", ContentFile(b"\xff\xd8\xffjpeg-data"), save=True)
        self.evidence = self.old.evidencias.first()
        self.evidence.archivo.save("avance.jpg", ContentFile(b"\xff\xd8\xffimage-data"), save=True)

    def test_get_and_head_serve_private_inline_evidence_without_public_path(self):
        url = f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{self.evidence.pk}/"
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Cache-Control"], "private, no-store")
        self.assertEqual(response["X-Content-Type-Options"], "nosniff")
        self.assertIn("inline", response["Content-Disposition"])
        self.assertNotIn(self.evidence.archivo.path, response["Content-Disposition"])
        head = self.client.head(url)
        self.assertEqual(head.status_code, 200)
        self.assertEqual(b"".join(head.streaming_content), b"")

    def test_initial_photo_is_authorized_through_parent(self):
        response = self.client.get(f"/api/mantenimiento/v2/evidencias/falla_inicial/{self.report.pk}/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("inline", response["Content-Disposition"])

    def test_evidence_rejects_anonymous_permissionless_other_branch_missing_and_unknown(self):
        url = f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{self.evidence.pk}/"
        self.client.logout()
        self.assertIn(self.client.get(url).status_code, {401, 403})
        self.client.force_login(self.denied)
        self.assertEqual(self.client.get(url).status_code, 403)
        self.client.force_login(self.user)
        other = EvidenciaSeguimientoFalla.objects.create(
            bitacora=BitacoraFalla.objects.create(reporte=self.other_report, usuario=self.reporter),
            archivo="fallas/seguimiento/other.jpg", subido_por=self.reporter,
        )
        self.assertEqual(self.client.get(f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{other.pk}/").status_code, 404)
        self.assertEqual(self.client.get("/api/mantenimiento/v2/evidencias/seguimiento_falla/999999/").status_code, 404)
        self.assertEqual(self.client.get(f"/api/mantenimiento/v2/evidencias/desconocida/{self.evidence.pk}/").status_code, 404)

    def test_head_enforces_auth_permission_parent_scope_and_missing_file(self):
        own_url = f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{self.evidence.pk}/"
        other = EvidenciaSeguimientoFalla.objects.create(
            bitacora=BitacoraFalla.objects.create(reporte=self.other_report, usuario=self.reporter),
            archivo="fallas/seguimiento/other-head.jpg", subido_por=self.reporter,
        )
        other.archivo.save("other-head.jpg", ContentFile(b"other"), save=True)
        missing = EvidenciaSeguimientoFalla.objects.create(
            bitacora=self.old, archivo="fallas/seguimiento/not-on-disk.jpg", subido_por=self.reporter,
        )

        self.client.logout()
        self.assertIn(self.client.head(own_url).status_code, {401, 403})
        self.client.force_login(self.denied)
        self.assertEqual(self.client.head(own_url).status_code, 403)
        self.client.force_login(self.user)
        self.assertEqual(
            self.client.head(f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{other.pk}/").status_code,
            404,
        )
        self.assertEqual(
            self.client.head(f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{missing.pk}/").status_code,
            404,
        )
        self.client.force_login(self.no_profile)
        self.assertEqual(self.client.head(own_url).status_code, 404)

    def test_active_or_deceptive_content_is_attachment_with_safe_mime_and_nosniff(self):
        cases = [
            ("vector.svg", b'<svg xmlns="http://www.w3.org/2000/svg"><script/></svg>'),
            ("parece-foto.jpg", b"<html><script>alert(1)</script></html>"),
            ("desconocido.bin", b"unknown"),
        ]
        for name, content in cases:
            evidence = EvidenciaSeguimientoFalla.objects.create(
                bitacora=self.old, nombre=name, subido_por=self.reporter,
            )
            evidence.archivo.save(name, ContentFile(content), save=True)
            with self.subTest(name=name):
                response = self.client.get(
                    f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{evidence.pk}/"
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("attachment", response["Content-Disposition"])
                self.assertEqual(response["Content-Type"], "application/octet-stream")
                self.assertEqual(response["X-Content-Type-Options"], "nosniff")

    def test_every_declared_evidence_type_is_private_and_scoped_through_its_parent(self):
        other_timeline_evidence = EvidenciaSeguimientoFalla.objects.create(
            bitacora=BitacoraFalla.objects.create(reporte=self.other_report, usuario=self.reporter),
            archivo="fallas/seguimiento/scoped-other.jpg", subido_por=self.reporter,
        )
        other_timeline_evidence.archivo.save("scoped-other.jpg", ContentFile(b"image"), save=True)
        self.other_report.foto_evidencia.save("initial-other.jpg", ContentFile(b"image"), save=True)
        own_asset = Activo.objects.create(nombre="Activo evidencia", sucursal=self.branch)
        other_asset = Activo.objects.create(nombre="Activo evidencia ajena", sucursal=self.other_branch)
        own_unit = Unidad.objects.create(codigo="EV-OWN", descripcion="Evidencia", sucursal=self.branch)
        other_unit = Unidad.objects.create(codigo="EV-OTHER", descripcion="Evidencia ajena", sucursal=self.other_branch)
        service_type = TipoServicioUnidad.objects.create(nombre="Servicio evidencia")

        own_order = OrdenMantenimiento.objects.create(activo_ref=own_asset)
        other_order = OrdenMantenimiento.objects.create(activo_ref=other_asset)
        own_order.factura_archivo.save("orden.pdf", ContentFile(b"%PDF-order"), save=True)
        other_order.factura_archivo.save("orden-ajena.pdf", ContentFile(b"%PDF-other"), save=True)
        own_unit_report = ReporteUnidad.objects.create(unidad=own_unit, tipo="falla", descripcion="x")
        other_unit_report = ReporteUnidad.objects.create(unidad=other_unit, tipo="falla", descripcion="x")
        own_unit_report.foto.save("reporte.jpg", ContentFile(b"image"), save=True)
        other_unit_report.foto.save("reporte-ajeno.jpg", ContentFile(b"image"), save=True)
        own_repair = ReparacionUnidad.objects.create(unidad=own_unit, fecha_ingreso="2026-07-11", descripcion_falla="x")
        other_repair = ReparacionUnidad.objects.create(unidad=other_unit, fecha_ingreso="2026-07-11", descripcion_falla="x")
        own_repair.archivo_factura.save("reparacion.pdf", ContentFile(b"%PDF-repair"), save=True)
        own_repair.foto_nota.save("nota.jpg", ContentFile(b"image"), save=True)
        other_repair.archivo_factura.save("reparacion-ajena.pdf", ContentFile(b"%PDF-other"), save=True)
        other_repair.foto_nota.save("nota-ajena.jpg", ContentFile(b"image"), save=True)
        own_service = ServicioRealizadoUnidad.objects.create(
            unidad=own_unit, tipo_servicio=service_type, fecha_servicio="2026-07-11",
        )
        other_service = ServicioRealizadoUnidad.objects.create(
            unidad=other_unit, tipo_servicio=service_type, fecha_servicio="2026-07-11",
        )
        own_service.archivo_factura.save("servicio.pdf", ContentFile(b"%PDF-service"), save=True)
        other_service.archivo_factura.save("servicio-ajeno.pdf", ContentFile(b"%PDF-other"), save=True)

        cases = [
            ("seguimiento_falla", self.evidence.pk, other_timeline_evidence.pk),
            ("falla_inicial", self.report.pk, self.other_report.pk),
            ("reporte_unidad", own_unit_report.pk, other_unit_report.pk),
            ("orden_factura", own_order.pk, other_order.pk),
            ("reparacion_factura", own_repair.pk, other_repair.pk),
            ("reparacion_foto", own_repair.pk, other_repair.pk),
            ("servicio_unidad_factura", own_service.pk, other_service.pk),
        ]
        for kind, own_pk, other_pk in cases:
            with self.subTest(kind=kind, access="own"):
                response = self.client.get(f"/api/mantenimiento/v2/evidencias/{kind}/{own_pk}/")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response["Cache-Control"], "private, no-store")
                self.assertIn(response["Content-Disposition"].split(";")[0], {"inline", "attachment"})
            with self.subTest(kind=kind, access="other"):
                response = self.client.get(f"/api/mantenimiento/v2/evidencias/{kind}/{other_pk}/")
                self.assertEqual(response.status_code, 404)

    def test_missing_file_is_404_and_malicious_name_is_sanitized(self):
        missing = EvidenciaSeguimientoFalla.objects.create(
            bitacora=self.old, archivo="fallas/seguimiento/missing.pdf", nombre="../../malicioso\r\nX-Evil: yes.pdf",
            subido_por=self.reporter,
        )
        url = f"/api/mantenimiento/v2/evidencias/seguimiento_falla/{missing.pk}/"
        self.assertEqual(self.client.get(url).status_code, 404)
        missing.archivo.save("safe.pdf", ContentFile(b"%PDF-1.4"), save=True)
        response = self.client.get(url)
        self.assertNotIn("..", response["Content-Disposition"])
        self.assertNotIn("X-Evil", response["Content-Disposition"])
        self.assertIn("attachment", response["Content-Disposition"])
