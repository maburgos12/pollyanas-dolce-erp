from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rrhh.models import AsistenciaEmpleado, Empleado, IncidenciaAsistencia, SuspensionEmpleado, Turno
from rrhh.services_asistencia_reglas import evaluar_dia_empleado


TZ = ZoneInfo("America/Mazatlan")


def dt_local(fecha: date, hora: time) -> datetime:
    return datetime(fecha.year, fecha.month, fecha.day, hora.hour, hora.minute, tzinfo=TZ)


class SuspensionesRRHHTests(TestCase):
    def setUp(self):
        self.turno = Turno.objects.create(
            nombre="Matutino",
            hora_entrada=time(8, 0),
            hora_salida=time(16, 0),
            tolerancia_minutos=10,
        )
        # fecha_ingreso explícita: el default es hoy y el motor descarta días
        # anteriores al ingreso, lo que rompía estos tests al pasar el tiempo.
        self.empleado = Empleado.objects.create(
            nombre="Empleado Suspendido",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2026, 1, 1),
        )
        User = get_user_model()
        self.rrhh_user = User.objects.create_superuser(
            username="rrhh",
            email="rrhh@example.com",
            password="testpass",
        )
        self.sin_permiso = User.objects.create_user(
            username="sin_permiso",
            email="sin_permiso@example.com",
            password="testpass",
        )

    def _crear_asistencia(self, fecha: date):
        return AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            entrada=dt_local(fecha, time(8, 0)),
            salida=dt_local(fecha, time(16, 0)),
            minutos_trabajados=480,
            turno=self.turno,
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )

    def test_dia_laborable_con_suspension_activa_genera_suspension_y_no_falta(self):
        fecha = date(2026, 6, 1)
        suspension = SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=fecha,
            fecha_fin=fecha,
            motivo="Medida disciplinaria",
            con_goce=False,
            aplicada_por=self.rrhh_user,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_SUSPENSION,
        )
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)
        self.assertEqual(incidencia.severidad, IncidenciaAsistencia.SEVERIDAD_MEDIA)
        self.assertIs(incidencia.goce_sueldo, False)
        self.assertEqual(incidencia.minutos, 0)
        self.assertEqual(incidencia.metadata, {"suspension_id": suspension.id, "con_goce": False})
        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha,
                tipo=IncidenciaAsistencia.TIPO_FALTA,
            ).exists()
        )

    def test_suspension_no_suma_al_conteo_de_despido(self):
        inicio = date(2026, 6, 1)
        for offset in range(2):
            IncidenciaAsistencia.objects.create(
                empleado=self.empleado,
                fecha=inicio + timedelta(days=offset),
                tipo=IncidenciaAsistencia.TIPO_FALTA,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
                severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            )
        SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=inicio + timedelta(days=2),
            fecha_fin=inicio + timedelta(days=2),
            motivo="Medida disciplinaria",
            aplicada_por=self.rrhh_user,
        )
        evaluar_dia_empleado(self.empleado, inicio + timedelta(days=2))
        fecha_siguiente = inicio + timedelta(days=3)
        self._crear_asistencia(fecha_siguiente)

        evaluar_dia_empleado(self.empleado, fecha_siguiente)

        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha_siguiente,
                tipo=IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS,
            ).exists()
        )

    def test_crear_suspension_por_post_reevalua_y_resuelve_falta_previa(self):
        fecha = date(2026, 6, 1)
        evaluar_dia_empleado(self.empleado, fecha)
        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:rrhh_suspension_crear"),
            {
                "empleado": self.empleado.id,
                "fecha_inicio": fecha.isoformat(),
                "fecha_fin": fecha.isoformat(),
                "motivo": "Medida disciplinaria",
                "con_goce": "on",
            },
        )

        self.assertEqual(response.status_code, 302)
        falta.refresh_from_db()
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_RESUELTO)
        suspension = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_SUSPENSION,
        )
        self.assertEqual(suspension.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)
        self.assertIs(suspension.goce_sueldo, True)

    def test_cancelar_suspension_exige_comentario_y_reevalua_falta(self):
        fecha = date(2026, 6, 1)
        suspension = SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=fecha,
            fecha_fin=fecha,
            motivo="Medida disciplinaria",
            aplicada_por=self.rrhh_user,
        )
        evaluar_dia_empleado(self.empleado, fecha)
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:rrhh_suspension_cancelar", args=[suspension.id]),
            {"comentario_cancelacion": ""},
        )

        self.assertEqual(response.status_code, 302)
        suspension.refresh_from_db()
        self.assertEqual(suspension.estado, SuspensionEmpleado.ESTADO_ACTIVA)
        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha,
                tipo=IncidenciaAsistencia.TIPO_FALTA,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            ).exists()
        )

        response = self.client.post(
            reverse("rrhh:rrhh_suspension_cancelar", args=[suspension.id]),
            {"comentario_cancelacion": "Se cancela por revisión de RRHH."},
        )

        self.assertEqual(response.status_code, 302)
        suspension.refresh_from_db()
        self.assertEqual(suspension.estado, SuspensionEmpleado.ESTADO_CANCELADA)
        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)

    def test_usuario_sin_permiso_no_puede_crear_suspension(self):
        fecha = date(2026, 6, 1)
        self.client.force_login(self.sin_permiso)

        response = self.client.post(
            reverse("rrhh:rrhh_suspension_crear"),
            {
                "empleado": self.empleado.id,
                "fecha_inicio": fecha.isoformat(),
                "fecha_fin": fecha.isoformat(),
                "motivo": "Medida disciplinaria",
            },
        )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(SuspensionEmpleado.objects.filter(empleado=self.empleado).exists())
