from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.test import TestCase
from django.utils import timezone

from rrhh.models import (
    AsistenciaEmpleado,
    Empleado,
    HoraExtra,
    IncidenciaAsistencia,
    PermisoSalida,
    SolicitudVacaciones,
    Turno,
)
from rrhh.services_asistencia_reglas import evaluar_dia_empleado


TZ = ZoneInfo("America/Mazatlan")


def dt_local(fecha: date, hora: time) -> datetime:
    return datetime(fecha.year, fecha.month, fecha.day, hora.hour, hora.minute, tzinfo=TZ)


class ReglasAsistenciaRRHHTests(TestCase):
    def setUp(self):
        self.turno = Turno.objects.create(
            nombre="Matutino",
            hora_entrada=time(8, 0),
            hora_salida=time(16, 0),
            tolerancia_minutos=10,
        )
        self.empleado = Empleado.objects.create(nombre="Empleado Reglas", salario_diario=Decimal("400.00"))

    def crear_asistencia(
        self,
        fecha: date,
        entrada: time,
        salida: time | None = time(16, 0),
        minutos=480,
        fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
    ):
        return AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            entrada=dt_local(fecha, entrada),
            salida=dt_local(fecha, salida) if salida else None,
            minutos_trabajados=minutos,
            turno=self.turno,
            fuente=fuente,
        )

    def test_entrada_despues_de_tolerancia_sin_permiso_genera_falta(self):
        fecha = date(2026, 6, 1)
        self.crear_asistencia(fecha, time(8, 11), minutos=469)

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        self.assertEqual(resultado.creados, 2)
        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(falta.minutos, 11)
        self.assertIn("se considera falta", falta.detalle)

    def test_retardo_se_concilia_con_permiso_aprobado(self):
        fecha = date(2026, 6, 1)
        self.crear_asistencia(fecha, time(8, 20), minutos=460)
        permiso = PermisoSalida.objects.create(
            empleado=self.empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=dt_local(fecha, time(8, 0)),
            fecha_fin=dt_local(fecha, time(8, 30)),
            motivo="Entrada autorizada",
            estado=PermisoSalida.ESTADO_APROBADO,
            estado_jefe=PermisoSalida.ESTADO_JEFE_PREAUTORIZADO,
            goce_sueldo=False,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        retardo = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_RETARDO,
        )
        self.assertEqual(retardo.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)
        self.assertEqual(retardo.permiso, permiso)
        self.assertIs(retardo.goce_sueldo, False)

    def test_quince_usos_de_tolerancia_generan_retardo_por_tolerancia(self):
        inicio = date(2026, 6, 1)
        for offset in range(15):
            fecha = inicio + timedelta(days=offset)
            self.crear_asistencia(fecha, time(8, 5), minutos=475)
            evaluar_dia_empleado(self.empleado, fecha)

        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=inicio + timedelta(days=14),
            tipo=IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
        )
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(incidencia.metadata["usos_tolerancia_15d"], 15)

    def test_tres_retardos_en_quince_dias_generan_falta(self):
        inicio = date(2026, 6, 1)
        for offset in (0, 1, 2):
            IncidenciaAsistencia.objects.create(
                empleado=self.empleado,
                fecha=inicio + timedelta(days=offset),
                tipo=IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
                severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
            )
        self.crear_asistencia(inicio + timedelta(days=3), time(8, 0))
        evaluar_dia_empleado(self.empleado, inicio + timedelta(days=3))

        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=inicio + timedelta(days=3),
            tipo=IncidenciaAsistencia.TIPO_FALTA_RETARDOS,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(falta.conteo_retardos_15d, 3)

    def test_tres_y_cuatro_faltas_en_treinta_dias_generan_alertas(self):
        inicio = date(2026, 6, 1)
        for offset in range(4):
            evaluar_dia_empleado(self.empleado, inicio + timedelta(days=offset))

        aviso = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=inicio + timedelta(days=2),
            tipo=IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS,
        )
        posible_rescision = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=inicio + timedelta(days=3),
            tipo=IncidenciaAsistencia.TIPO_POSIBLE_RESCISION,
        )
        self.assertEqual(aviso.conteo_faltas_30d, 3)
        self.assertEqual(posible_rescision.conteo_faltas_30d, 4)
        self.assertEqual(posible_rescision.severidad, IncidenciaAsistencia.SEVERIDAD_CRITICA)

    def test_falta_sin_registro_se_concilia_con_vacaciones_aprobadas(self):
        fecha = date(2026, 6, 1)
        solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=fecha,
            fecha_fin=fecha,
            dias_laborables=Decimal("1"),
            motivo="Vacaciones aprobadas",
            estado=SolicitudVacaciones.ESTADO_APROBADA,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)
        self.assertEqual(falta.solicitud_vacaciones, solicitud)

    def test_jornada_incompleta_busca_permiso_y_conserva_goce_sueldo(self):
        fecha = date(2026, 6, 1)
        self.crear_asistencia(fecha, time(8, 0), salida=time(14, 0), minutos=360)
        permiso = PermisoSalida.objects.create(
            empleado=self.empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=dt_local(fecha, time(14, 0)),
            fecha_fin=dt_local(fecha, time(16, 0)),
            motivo="Salida autorizada",
            estado=PermisoSalida.ESTADO_APROBADO,
            estado_jefe=PermisoSalida.ESTADO_JEFE_PREAUTORIZADO,
            goce_sueldo=True,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA,
        )
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)
        self.assertEqual(incidencia.permiso, permiso)
        self.assertIs(incidencia.goce_sueldo, True)

    def test_hora_extra_detectada_queda_pendiente_hasta_autorizacion(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(fecha, time(8, 0), salida=time(17, 15), minutos=555)

        evaluar_dia_empleado(self.empleado, fecha)

        hora_extra = HoraExtra.objects.get(asistencia=asistencia)
        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE,
        )
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)

        hora_extra.estado = HoraExtra.ESTADO_AUTORIZADO
        hora_extra.fecha_autorizacion_jefe = timezone.now()
        hora_extra.save(update_fields=["estado", "fecha_autorizacion_jefe"])

        evaluar_dia_empleado(self.empleado, fecha)
        incidencia.refresh_from_db()
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)

    def test_comida_de_35_minutos_no_genera_jornada_incompleta_si_esta_registrada(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(fecha, time(8, 0), salida=time(16, 0), minutos=445)
        asistencia.salida_comida = dt_local(fecha, time(12, 0))
        asistencia.regreso_comida = dt_local(fecha, time(12, 35))
        asistencia.minutos_comida = 35
        asistencia.save(update_fields=["salida_comida", "regreso_comida", "minutos_comida"])

        evaluar_dia_empleado(self.empleado, fecha)

        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha,
                tipo=IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA,
            ).exists()
        )

    def test_asistencia_point_no_infiere_tiempo_de_comida(self):
        fecha = date(2026, 6, 1)
        self.crear_asistencia(
            fecha,
            time(8, 0),
            salida=time(16, 0),
            minutos=480,
            fuente=AsistenciaEmpleado.FUENTE_POINT,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha,
                tipo=IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA,
            ).exists()
        )
