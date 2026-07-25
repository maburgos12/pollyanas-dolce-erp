from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.test import TestCase
from django.utils import timezone

from rrhh.models import (
    AplicacionGoceVacaciones,
    AsistenciaEmpleado,
    Empleado,
    HoraExtra,
    IncidenciaAsistencia,
    PermisoSalida,
    PeriodoVacacional,
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
        self.empleado = Empleado.objects.create(
            nombre="Empleado Reglas",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2026, 1, 1),
        )

    def crear_asistencia(
        self,
        fecha: date,
        entrada: time,
        salida: time | None = time(16, 0),
        minutos=480,
        fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        salida_comida: time | None = None,
        regreso_comida: time | None = None,
        minutos_comida=0,
    ):
        return AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            entrada=dt_local(fecha, entrada),
            salida_comida=dt_local(fecha, salida_comida) if salida_comida else None,
            regreso_comida=dt_local(fecha, regreso_comida) if regreso_comida else None,
            salida=dt_local(fecha, salida) if salida else None,
            minutos_comida=minutos_comida,
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

    def test_no_genera_faltas_antes_de_fecha_de_ingreso(self):
        self.empleado.fecha_ingreso = date(2026, 6, 10)
        self.empleado.save(update_fields=["fecha_ingreso"])
        fecha = date(2026, 6, 5)

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        self.assertEqual(resultado.creados, 0)
        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha,
                tipo=IncidenciaAsistencia.TIPO_FALTA,
            ).exists()
        )

    def test_resuelve_falta_automatica_si_fecha_queda_antes_del_ingreso(self):
        fecha = date(2026, 6, 5)
        falta = IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
        )
        self.empleado.fecha_ingreso = date(2026, 6, 10)
        self.empleado.save(update_fields=["fecha_ingreso"])

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        self.assertEqual(resultado.resueltos, 1)
        falta.refresh_from_db()
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_RESUELTO)

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

    def test_tres_usos_de_tolerancia_generan_retardo_por_tolerancia(self):
        inicio = date(2026, 6, 1)
        for offset in range(3):
            fecha = inicio + timedelta(days=offset)
            self.crear_asistencia(fecha, time(8, 5), minutos=475)
            evaluar_dia_empleado(self.empleado, fecha)

        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=inicio + timedelta(days=2),
            tipo=IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
        )
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(incidencia.metadata["usos_tolerancia_15d"], 3)

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
        baja = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=inicio + timedelta(days=3),
            tipo=IncidenciaAsistencia.TIPO_BAJA_FALTAS,
        )
        self.assertEqual(aviso.conteo_faltas_30d, 3)
        self.assertEqual(baja.conteo_faltas_30d, 4)
        self.assertEqual(baja.severidad, IncidenciaAsistencia.SEVERIDAD_CRITICA)
        self.assertIn("baja por faltas", baja.detalle)

    def test_falta_por_retardos_no_cuenta_para_aviso_o_baja_por_faltas(self):
        inicio = date(2026, 6, 1)
        for offset in range(2):
            IncidenciaAsistencia.objects.create(
                empleado=self.empleado,
                fecha=inicio + timedelta(days=offset),
                tipo=IncidenciaAsistencia.TIPO_FALTA,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
                severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            )
        IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=inicio + timedelta(days=2),
            tipo=IncidenciaAsistencia.TIPO_FALTA_RETARDOS,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
        )
        fecha_evaluacion = inicio + timedelta(days=3)
        self.crear_asistencia(fecha_evaluacion, time(8, 0))

        evaluar_dia_empleado(self.empleado, fecha_evaluacion)

        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha_evaluacion,
                tipo__in=[
                    IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS,
                    IncidenciaAsistencia.TIPO_BAJA_FALTAS,
                ],
            ).exists()
        )

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

    def test_vacacion_2026_aplicada_a_2025_justifica_fecha_2026(self):
        fecha = date(2026, 7, 20)
        periodo_2025 = PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2025, 1, 1),
            fecha_limite=date(2025, 7, 1),
            antiguedad_anios=1,
            dias_generados=Decimal("7.00"),
        )
        solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=fecha,
            fecha_fin=fecha,
            dias_laborables=Decimal("1.00"),
            motivo="Goce 2026 contra saldo 2025",
            estado=SolicitudVacaciones.ESTADO_APROBADA,
        )
        AplicacionGoceVacaciones.objects.create(
            solicitud=solicitud,
            periodo=periodo_2025,
            dias=Decimal("1.00"),
            estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
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

    def test_comida_mayor_a_35_minutos_genera_incidencia_por_exceso(self):
        fecha = date(2026, 6, 1)
        self.crear_asistencia(
            fecha,
            time(8, 0),
            salida=time(16, 0),
            minutos=430,
            salida_comida=time(12, 0),
            regreso_comida=time(12, 50),
            minutos_comida=50,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA,
        )
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(incidencia.severidad, IncidenciaAsistencia.SEVERIDAD_MEDIA)
        self.assertEqual(incidencia.minutos, 15)
        self.assertEqual(incidencia.metadata["minutos_comida"], 50)
        self.assertEqual(incidencia.metadata["exceso"], 15)

    def test_comida_de_35_minutos_o_menos_no_genera_incidencia_por_exceso(self):
        fecha = date(2026, 6, 1)
        self.crear_asistencia(
            fecha,
            time(8, 0),
            salida=time(16, 0),
            minutos=445,
            salida_comida=time(12, 0),
            regreso_comida=time(12, 35),
            minutos_comida=35,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado,
                fecha=fecha,
                tipo=IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA,
            ).exists()
        )

    def test_incidencia_editada_manual_no_se_pisa_ni_se_resuelve_por_recalculo(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(
            fecha,
            time(8, 0),
            salida=time(16, 0),
            minutos=430,
            salida_comida=time(12, 0),
            regreso_comida=time(12, 50),
            minutos_comida=50,
        )
        incidencia = IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            asistencia=asistencia,
            minutos=99,
            detalle="Ajuste manual de RRHH.",
            metadata={"manual": True},
            editado_manual=True,
        )

        evaluar_dia_empleado(self.empleado, fecha)
        incidencia.refresh_from_db()
        self.assertEqual(incidencia.severidad, IncidenciaAsistencia.SEVERIDAD_ALTA)
        self.assertEqual(incidencia.minutos, 99)
        self.assertEqual(incidencia.detalle, "Ajuste manual de RRHH.")
        self.assertEqual(incidencia.metadata, {"manual": True})

        asistencia.salida_comida = None
        asistencia.regreso_comida = None
        asistencia.minutos_comida = 0
        asistencia.minutos_trabajados = 480
        asistencia.save(update_fields=["salida_comida", "regreso_comida", "minutos_comida", "minutos_trabajados"])

        evaluar_dia_empleado(self.empleado, fecha)
        incidencia.refresh_from_db()
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(incidencia.detalle, "Ajuste manual de RRHH.")

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


class ConexionVacacionesAsistenciaTests(TestCase):
    """La solicitud de vacaciones (reservada o aprobada) concilia faltas del checador."""

    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleada Vacaciones",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2025, 1, 10),
        )

    def test_falta_sin_registro_se_concilia_con_solicitud_en_tramite(self):
        fecha = date(2026, 6, 1)
        solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=fecha,
            fecha_fin=fecha,
            dias_laborables=Decimal("1"),
            motivo="Reserva pendiente de aprobar",
            estado=SolicitudVacaciones.ESTADO_SOLICITADA,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)
        self.assertEqual(falta.solicitud_vacaciones, solicitud)
        self.assertIn("en tramite", falta.detalle)

    def _crear_periodo_con_saldo(self, dias="12.00"):
        return PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2026, 1, 10),
            fecha_limite=date(2026, 7, 10),
            antiguedad_anios=1,
            dias_generados=Decimal(dias),
        )

    def test_captura_retroactiva_concilia_falta_pendiente(self):
        from django.test import override_settings

        from rrhh.services_vacaciones import crear_solicitud_vacaciones

        fecha = timezone.localdate() - timedelta(days=7)
        while fecha.weekday() == 6:  # es_dia_laborable excluye domingos
            fecha -= timedelta(days=1)
        IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
        )
        self._crear_periodo_con_saldo()

        with override_settings(VACACIONES_GOCE_FIFO_ACTIVO=True):
            with self.captureOnCommitCallbacks(execute=True):
                crear_solicitud_vacaciones(
                    empleado=self.empleado,
                    fecha_inicio=fecha,
                    fecha_fin=fecha,
                    motivo="Captura retroactiva",
                )

        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)
        self.assertIsNotNone(falta.solicitud_vacaciones)

    def test_rechazo_rrhh_regresa_falta_a_pendiente(self):
        from django.contrib.auth import get_user_model
        from django.test import override_settings

        from rrhh.services_vacaciones import (
            crear_solicitud_vacaciones,
            rechazar_solicitud_vacaciones,
        )

        fecha = timezone.localdate() - timedelta(days=7)
        while fecha.weekday() == 6:
            fecha -= timedelta(days=1)
        self._crear_periodo_con_saldo()
        rrhh_user = get_user_model().objects.create_superuser(
            username="rrhh_admin", password="x", email="rrhh@test.local"
        )

        with override_settings(VACACIONES_GOCE_FIFO_ACTIVO=True):
            with self.captureOnCommitCallbacks(execute=True):
                solicitud = crear_solicitud_vacaciones(
                    empleado=self.empleado,
                    fecha_inicio=fecha,
                    fecha_fin=fecha,
                    motivo="Captura retroactiva",
                )
            falta = IncidenciaAsistencia.objects.get(
                empleado=self.empleado, fecha=fecha, tipo=IncidenciaAsistencia.TIPO_FALTA
            )
            self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)

            with self.captureOnCommitCallbacks(execute=True):
                rechazar_solicitud_vacaciones(solicitud, rrhh_user)

        falta.refresh_from_db()
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertIsNone(falta.solicitud_vacaciones)


class BarridoDiarioAsistenciaTests(TestCase):
    """La task diaria evalúa también a quien no checó (sustituye al polling ISAPI)."""

    FECHA_FIJA = date(2026, 6, 3)  # miércoles; el día evaluado es martes 2

    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleado Barrido",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2025, 1, 1),
        )

    def _correr_task(self):
        from unittest.mock import patch

        from rrhh import tasks

        with patch.object(tasks.timezone, "localdate", return_value=self.FECHA_FIJA):
            return tasks.evaluar_asistencia_diaria()

    def test_genera_falta_para_empleado_sin_checada(self):
        resultado = self._correr_task()

        self.assertTrue(resultado["ok"])
        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=date(2026, 6, 2),
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)

    def test_no_marca_falta_si_hay_vacaciones_reservadas(self):
        SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 5),
            dias_laborables=Decimal("5"),
            motivo="Vacaciones en trámite",
            estado=SolicitudVacaciones.ESTADO_SOLICITADA,
        )

        self._correr_task()

        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=date(2026, 6, 2),
            tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_CONCILIADO)


class AuditoriaVacacionesDiariaTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleada Auditoria",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2020, 1, 10),
        )
        self.periodo = PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2026, 1, 10),
            fecha_limite=date(2026, 7, 10),
            antiguedad_anios=6,
            dias_generados=Decimal("5.00"),
        )

    def _solicitud(self, inicio, fin, estado, dias="5.00"):
        return SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=inicio,
            fecha_fin=fin,
            dias_laborables=Decimal(dias),
            motivo="test",
            estado=estado,
        )

    def test_sin_anomalias_no_envia_correo(self):
        from django.core import mail

        from rrhh.tasks import auditar_vacaciones_diaria

        resultado = auditar_vacaciones_diaria()

        self.assertEqual(resultado["hallazgos"], 0)
        self.assertEqual(len(mail.outbox), 0)

    def test_detecta_cruce_de_periodo_en_reserva_pendiente(self):
        from django.core import mail

        from rrhh.tasks import auditar_vacaciones_diaria

        # vacación de 2025 reservando de la bolsa 2026 (patrón Carmina)
        solicitud = self._solicitud(
            date(2025, 9, 1), date(2025, 9, 5), SolicitudVacaciones.ESTADO_SOLICITADA
        )
        AplicacionGoceVacaciones.objects.create(
            solicitud=solicitud,
            periodo=self.periodo,
            dias=Decimal("5.00"),
            estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )

        resultado = auditar_vacaciones_diaria()

        self.assertEqual(resultado["hallazgos"], 1)
        self.assertEqual(len(mail.outbox), 1)
        self.assertIn("Cruce de periodo", mail.outbox[0].body)

    def test_detecta_bolsa_sobregirada(self):
        from rrhh.tasks import _hallazgos_auditoria_vacaciones

        solicitud = self._solicitud(
            date(2026, 2, 2), date(2026, 2, 7), SolicitudVacaciones.ESTADO_APROBADA, dias="6.00"
        )
        AplicacionGoceVacaciones.objects.create(
            solicitud=solicitud,
            periodo=self.periodo,
            dias=Decimal("6.00"),
            estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
        )

        hallazgos = _hallazgos_auditoria_vacaciones()

        self.assertTrue(any("sobregirada" in h for h in hallazgos))


class ExencionChecadorTests(TestCase):
    """Empleados exentos (remoto / oficina sin checador) no generan falta automática."""

    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleado Remoto",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2025, 1, 1),
            exento_checador=True,
            exento_checador_motivo="Trabajo remoto",
        )

    def test_exento_sin_checada_no_genera_falta(self):
        fecha = date(2026, 6, 1)

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        self.assertEqual(resultado.creados, 0)
        self.assertFalse(
            IncidenciaAsistencia.objects.filter(
                empleado=self.empleado, fecha=fecha, tipo=IncidenciaAsistencia.TIPO_FALTA
            ).exists()
        )

    def test_exento_resuelve_falta_previa_al_reevaluar(self):
        fecha = date(2026, 6, 1)
        IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
        )

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado, fecha=fecha, tipo=IncidenciaAsistencia.TIPO_FALTA
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_RESUELTO)
        self.assertGreaterEqual(resultado.resueltos, 1)

    def test_no_exento_sigue_generando_falta(self):
        self.empleado.exento_checador = False
        self.empleado.save(update_fields=["exento_checador"])
        fecha = date(2026, 6, 1)

        evaluar_dia_empleado(self.empleado, fecha)

        falta = IncidenciaAsistencia.objects.get(
            empleado=self.empleado, fecha=fecha, tipo=IncidenciaAsistencia.TIPO_FALTA
        )
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
