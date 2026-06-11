from __future__ import annotations

from datetime import date, datetime, time

from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from rrhh.models import AsistenciaEmpleado, Empleado, IncidenciaAsistencia


def dt_local(fecha: date, hora: time) -> datetime:
    return timezone.make_aware(datetime.combine(fecha, hora), timezone.get_current_timezone())


class ReporteAsistenciaTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula", password="test")
        self.user.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        self.sin_permiso = User.objects.create_user(username="sinpermiso", password="test")
        self.empleado = Empleado.objects.create(
            codigo="RPT-001",
            nombre="Empleado Reporte",
            puesto="Auxiliar",
            sucursal="Matriz",
            departamento=Empleado.DEP_PRODUCCION,
        )
        self.fecha = date(2026, 6, 10)
        AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=self.fecha,
            entrada=dt_local(self.fecha, time(8, 0)),
            salida=dt_local(self.fecha, time(16, 0)),
            minutos_comida=75,
            minutos_trabajados=480,
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )
        self.incidencia = IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=self.fecha,
            tipo=IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
            minutos=15,
            detalle="Comida excedida por 15 minutos",
        )
        self.url = reverse("rrhh:rrhh_reporte_asistencia")

    def test_vista_responde_y_resume_comida_excedida(self):
        self.client.force_login(self.user)

        response = self.client.get(
            self.url,
            {
                "fecha_inicio": "2026-06-10",
                "fecha_fin": "2026-06-10",
                "empleado": str(self.empleado.id),
            },
        )

        self.assertEqual(response.status_code, 200)
        reportes = response.context["reportes"]
        self.assertEqual(len(reportes), 1)
        self.assertEqual(reportes[0]["resumen"]["comida_excedida"], 1)
        self.assertEqual(reportes[0]["filas"][0]["incidencias"][0]["tipo"], "Comida excedida")

    def test_export_csv_devuelve_fila_de_incidencia(self):
        self.client.force_login(self.user)

        response = self.client.get(
            self.url,
            {
                "fecha_inicio": "2026-06-10",
                "fecha_fin": "2026-06-10",
                "empleado": str(self.empleado.id),
                "export": "csv",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response["Content-Type"].startswith("text/csv"))
        content = response.content.decode("utf-8")
        self.assertIn("RPT-001,Empleado Reporte,Matriz,2026-06-10,Comida excedida", content)
        self.assertIn("Comida excedida por 15 minutos", content)

    def test_usuario_sin_permiso_recibe_403(self):
        self.client.force_login(self.sin_permiso)

        response = self.client.get(
            self.url,
            {
                "fecha_inicio": "2026-06-10",
                "fecha_fin": "2026-06-10",
                "empleado": str(self.empleado.id),
            },
        )

        self.assertEqual(response.status_code, 403)
