from __future__ import annotations

from datetime import date, datetime, time, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.test import TestCase

from .models import Empleado, PermisoSalida
from .services_permisos import resolver_permiso_jefe


TZ = ZoneInfo("America/Mazatlan")


class PermisoReevaluaAsistenciaTests(TestCase):
    def setUp(self):
        self.superuser = User.objects.create_superuser("dg", "dg@test.local", "x")
        self.empleado = Empleado.objects.create(
            nombre="Empleada Produccion", area="HORNOS", fecha_ingreso=date(2026, 1, 1)
        )

    def _crear_permiso_retroactivo(self) -> PermisoSalida:
        ayer = date.today() - timedelta(days=1)
        inicio = datetime.combine(ayer, time(13, 0), tzinfo=TZ)
        return PermisoSalida.objects.create(
            empleado=self.empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=inicio,
            fecha_fin=inicio + timedelta(hours=2),
            motivo="Salida temprana: se terminó la producción",
        )

    def test_aprobar_permiso_retroactivo_reevalua_el_dia(self):
        permiso = self._crear_permiso_retroactivo()
        ayer = date.today() - timedelta(days=1)

        with patch("rrhh.services_asistencia_reglas.evaluar_rango_asistencia") as evaluar:
            with self.captureOnCommitCallbacks(execute=True):
                resolver_permiso_jefe(permiso, self.superuser, aprobar=True)

        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        evaluar.assert_called_once_with(ayer, ayer, empleados=[self.empleado])

    def test_permiso_futuro_no_dispara_reevaluacion(self):
        manana = date.today() + timedelta(days=1)
        inicio = datetime.combine(manana, time(13, 0), tzinfo=TZ)
        permiso = PermisoSalida.objects.create(
            empleado=self.empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=inicio,
            motivo="Cita médica",
        )

        with patch("rrhh.services_asistencia_reglas.evaluar_rango_asistencia") as evaluar:
            with self.captureOnCommitCallbacks(execute=True):
                resolver_permiso_jefe(permiso, self.superuser, aprobar=True)

        evaluar.assert_not_called()
