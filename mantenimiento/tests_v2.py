from datetime import datetime
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import SimpleTestCase, TestCase

from activos.models import Activo, OrdenMantenimiento
from core.models import Sucursal, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
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
        cls.manager = user_model.objects.create_user("manager", password="test")
        cls.dg_user = user_model.objects.create_user("dg-user", password="test")
        cls.admin_group_user = user_model.objects.create_user("admin-group-user", password="test")
        cls.admin = user_model.objects.create_superuser("global", "global@example.com", "test")
        cls.dg_user.groups.add(Group.objects.create(name="DG"))
        cls.admin_group_user.groups.add(Group.objects.create(name="ADMIN"))
        UserProfile.objects.create(user=cls.limited_user, sucursal=cls.own_branch)
        UserProfile.objects.create(user=cls.no_scope_user)
        UserModuleAccess.objects.create(user=cls.manager, module="mantenimiento", access="manage")

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

    def test_user_without_canonical_branch_scope_sees_nothing(self):
        self.assertFalse(authorized_fallas(self.no_scope_user).exists())
        self.assertFalse(authorized_orders(self.no_scope_user).exists())
        self.assertFalse(authorized_unit_reports(self.no_scope_user).exists())
        self.assertFalse(authorized_repairs(self.no_scope_user).exists())
        self.assertFalse(authorized_unit_services(self.no_scope_user).exists())

    def test_only_global_or_mantenimiento_manager_can_view_costs(self):
        self.assertTrue(can_view_costs(self.admin))
        self.assertTrue(can_view_costs(self.dg_user))
        self.assertTrue(can_view_costs(self.admin_group_user))
        self.assertTrue(can_view_costs(self.manager))
        self.assertFalse(can_view_costs(self.limited_user))
        self.assertFalse(can_view_costs(self.no_scope_user))
