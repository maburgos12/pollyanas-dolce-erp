"""UI boundary tests: authorization, private birth years, verified capture."""
from datetime import date, timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import AuditLog
from .models import Empleado


class CumpleanosViewsTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="cumpleanos-ui")
        self.employee = Empleado.objects.create(codigo="CUMP-UI-1", nombre="Persona del equipo", fecha_nacimiento=date(1987, 9, 17), departamento="PRODUCCION", activo=True)
        self.client.force_login(self.user)
        self.scope_patch = patch("rrhh.views_cumpleanos.empleados_visibles", side_effect=lambda user: Empleado.objects.filter(pk=self.employee.pk, activo=True))
        self.scope_patch.start()
        self.addCleanup(self.scope_patch.stop)
        self.view_patch = patch("rrhh.views_cumpleanos.puede_ver_cumpleanos", return_value=True)
        self.view_patch.start()
        self.addCleanup(self.view_patch.stop)
        self.global_patch = patch("rrhh.views_cumpleanos.vista_global_cumpleanos", return_value=False)
        self.global_patch.start()
        self.addCleanup(self.global_patch.stop)
        self.manage_patch = patch("rrhh.views_cumpleanos.puede_gestionar_cumpleanos", return_value=False)
        self.manage = self.manage_patch.start()
        self.addCleanup(self.manage_patch.stop)

    def test_jefatura_get_never_renders_birth_year_or_capture(self):
        response = self.client.get(reverse("rrhh:rrhh_cumpleanos"), {"mes": "2026-09", "empleado": self.employee.pk})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.employee.nombre)
        self.assertNotContains(response, "1987")
        self.assertNotContains(response, "fecha_nacimiento")
        self.assertNotContains(response, "Fechas pendientes")

    def test_denied_get_and_post_cannot_mutate(self):
        with patch("rrhh.views_cumpleanos.puede_ver_cumpleanos", return_value=False):
            self.assertEqual(self.client.get(reverse("rrhh:rrhh_cumpleanos")).status_code, 403)
        self.assertEqual(self.client.post(reverse("rrhh:rrhh_cumpleanos_guardar"), {"empleado_id": self.employee.pk, "fecha_nacimiento": "1988-01-02", "motivo": "Documento"}).status_code, 403)
        self.employee.refresh_from_db()
        self.assertEqual(self.employee.fecha_nacimiento, date(1987, 9, 17))
        self.assertFalse(AuditLog.objects.filter(model="rrhh.Empleado").exists())

    def test_success_records_previous_date_and_verification(self):
        self.manage.return_value = True
        response = self.client.post(reverse("rrhh:rrhh_cumpleanos_guardar"), {"empleado_id": self.employee.pk, "fecha_nacimiento": "1988-02-29", "motivo": "Confirmada con documento", "mes": "2026-09", "departamento": "PRODUCCION"}, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertIn("#captura-cumpleanos", response.json()["redirect"])
        self.employee.refresh_from_db()
        self.assertEqual(self.employee.fecha_nacimiento, date(1988, 2, 29))
        audit = AuditLog.objects.get(model="rrhh.Empleado", object_id=str(self.employee.pk))
        self.assertEqual(audit.user, self.user)
        self.assertEqual(audit.payload["anterior"], "1987-09-17")
        self.assertEqual(audit.payload["motivo"], "Confirmada con documento")

    def test_invalid_capture_keeps_data_and_has_json_toast(self):
        self.manage.return_value = True
        for birth, reason in [("1988-01-02", " "), ("1988-02-30", "Documento"), ((timezone.localdate() + timedelta(days=1)).isoformat(), "Documento"), ("19880102", "Documento")]:
            with self.subTest(birth=birth, reason=reason):
                response = self.client.post(reverse("rrhh:rrhh_cumpleanos_guardar"), {"empleado_id": self.employee.pk, "fecha_nacimiento": birth, "motivo": reason}, HTTP_ACCEPT="application/json")
                self.assertEqual(response.status_code, 400)
                self.assertFalse(response.json()["ok"])
                self.assertEqual(response.json()["toast"]["type"], "error")
        self.employee.refresh_from_db()
        self.assertEqual(self.employee.fecha_nacimiento, date(1987, 9, 17))
        self.assertFalse(AuditLog.objects.filter(model="rrhh.Empleado").exists())

    def test_traditional_error_renders_bound_form(self):
        self.manage.return_value = True
        response = self.client.post(reverse("rrhh:rrhh_cumpleanos_guardar"), {"empleado_id": self.employee.pk, "fecha_nacimiento": "1988-01-02", "motivo": "", "mes": "2026-09"})
        self.assertEqual(response.status_code, 400)
        self.assertContains(response, 'value="1988-01-02"', status_code=400)
        self.assertContains(response, 'id="captura-cumpleanos"', status_code=400)

    def test_out_of_scope_or_inactive_employee_is_404(self):
        self.manage.return_value = True
        outside = Empleado.objects.create(codigo="CUMP-UI-2", nombre="Fuera del alcance", activo=True)
        for employee_id in [outside.pk, "invalid"]:
            self.assertEqual(self.client.post(reverse("rrhh:rrhh_cumpleanos_guardar"), {"empleado_id": employee_id, "fecha_nacimiento": "1988-01-02", "motivo": "Documento"}).status_code, 404)
        self.employee.activo = False
        self.employee.save(update_fields=["activo"])
        self.assertEqual(self.client.get(reverse("rrhh:rrhh_cumpleanos"), {"empleado": self.employee.pk}).status_code, 404)

    def test_invalid_month_has_safe_current_month_fallback(self):
        response = self.client.get(reverse("rrhh:rrhh_cumpleanos"), {"mes": "9999-13"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["mes"], timezone.localdate().replace(day=1))

    def test_next_seven_days_includes_new_year_outside_selected_month(self):
        self.employee.fecha_nacimiento = date(1987, 1, 2)
        self.employee.save(update_fields=["fecha_nacimiento"])
        with patch("rrhh.views_cumpleanos.timezone.localdate", return_value=date(2026, 12, 30)):
            response = self.client.get(reverse("rrhh:rrhh_cumpleanos"), {"mes": "2026-09"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["total_mes"], 0)
        self.assertEqual(response.context["total_semana"], 1)
        self.assertEqual(response.context["eventos_proximos"][0]["fecha"], date(2027, 1, 2))

    def test_global_read_only_pending_and_mail_history(self):
        from .models import AvisoCumpleanos
        self.employee.fecha_nacimiento = None
        self.employee.save(update_fields=["fecha_nacimiento"])
        AvisoCumpleanos.objects.create(usuario=self.user, tipo="hoy", fecha_referencia=date(2026, 9, 17), estado_correo="enviado", correo_destino="verificacion@example.test")
        with patch("rrhh.views_cumpleanos.vista_global_cumpleanos", return_value=True):
            response = self.client.get(reverse("rrhh:rrhh_cumpleanos"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Fechas pendientes")
        self.assertContains(response, self.employee.nombre)
        self.assertContains(response, "Aceptado por el proveedor")
        self.assertContains(response, "verificacion@example.test")
        self.assertNotContains(response, "Capturar fecha →")
        self.assertNotContains(response, "fecha_nacimiento")
