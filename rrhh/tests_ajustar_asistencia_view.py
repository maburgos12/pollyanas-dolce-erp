from __future__ import annotations

from datetime import date, timedelta

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse

from .models import AjusteAsistencia, AsistenciaEmpleado, Empleado


class AjustarAsistenciaViewTests(TestCase):
    def setUp(self):
        self.admin = User.objects.create_superuser("admin-test", "a@test.local", "x")
        self.empleado = Empleado.objects.create(
            nombre="Empleada Prueba", area="HORNOS", fecha_ingreso=date(2026, 1, 1)
        )
        self.url = reverse("rrhh:rrhh_asistencias_ajustar")
        self.ayer = date.today() - timedelta(days=1)

    def _post(self, user=None, progressive=True, **extra):
        self.client.force_login(user or self.admin)
        data = {
            "empleado_id": self.empleado.id,
            "fecha": self.ayer.isoformat(),
            "entrada": "08:00",
            "salida": "16:00",
            "motivo": "Checada perdida del checador",
            "next_query": "mes=2026-07",
        }
        data.update(extra)
        kwargs = {"HTTP_X_REQUESTED_WITH": "XMLHttpRequest"} if progressive else {}
        return self.client.post(self.url, data, **kwargs)

    def test_captura_entrada_y_salida_con_bitacora(self):
        response = self._post()
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertIn("#asistencia-", payload["redirect"])
        asistencia = AsistenciaEmpleado.objects.get(empleado=self.empleado, fecha=self.ayer)
        self.assertEqual(asistencia.entrada.astimezone().strftime("%H:%M"), "08:00")
        self.assertEqual(asistencia.salida.astimezone().strftime("%H:%M"), "16:00")
        ajustes = AjusteAsistencia.objects.filter(empleado=self.empleado, fecha=self.ayer)
        self.assertEqual(ajustes.count(), 2)
        self.assertTrue(all(a.estado == AjusteAsistencia.ESTADO_APLICADO for a in ajustes))

    def test_motivo_obligatorio(self):
        response = self._post(motivo="")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertFalse(AsistenciaEmpleado.objects.filter(empleado=self.empleado).exists())

    def test_fecha_futura_rechazada(self):
        manana = date.today() + timedelta(days=1)
        response = self._post(fecha=manana.isoformat())
        self.assertEqual(response.status_code, 400)

    def test_sin_horarios_rechazado(self):
        response = self._post(entrada="", salida="")
        self.assertEqual(response.status_code, 400)

    def test_usuario_sin_manage_recibe_403(self):
        plain = User.objects.create_user("plain", "p@test.local", "x")
        response = self._post(user=plain)
        self.assertEqual(response.status_code, 403)

    def test_fallback_sin_js_redirige_con_fragmento(self):
        response = self._post(progressive=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("mes=2026-07", response.url)
        self.assertIn(f"#asistencia-{self.empleado.id}-{self.ayer.isoformat()}", response.url)
