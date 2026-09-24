from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.test import TestCase
from django.utils import timezone

from rrhh.models import (
    AplicacionGoceVacaciones,
    AsignacionJornadaEmpleado,
    AsignacionTurnoEmpleado,
    AsistenciaEmpleado,
    Empleado,
    EmpleadoBaja,
    HoraExtra,
    IncidenciaAsistencia,
    IncidenciaAsistenciaBitacora,
    JornadaSemanal,
    JornadaSemanalDia,
    PermisoSalida,
    PeriodoVacacional,
    SolicitudVacaciones,
    Turno,
)
from rrhh.services_asistencia_reglas import evaluar_dia_empleado, evaluar_rango_asistencia


TZ = ZoneInfo("America/Mazatlan")


def dt_local(fecha: date, hora: time) -> datetime:
    return datetime(fecha.year, fecha.month, fecha.day, hora.hour, hora.minute, tzinfo=TZ)


class DescansoJornadaSemanalReglasTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            codigo="DESCANSO-REGLAS", nombre="Persona con descanso configurado",
            fecha_ingreso=date(2026, 9, 1),
        )
        self.turno = Turno.objects.create(
            nombre="Diurno descanso reglas", hora_entrada=time(8), hora_salida=time(16),
            tolerancia_minutos=10,
        )

    def asignar_semana(self, *, descansos=(2, 6)):
        jornada = JornadaSemanal.objects.create(nombre="Semana con miércoles y domingo libres")
        for dia in range(7):
            JornadaSemanalDia.objects.create(
                jornada=jornada, dia_semana=dia,
                turno=None if dia in descansos else self.turno,
            )
        return AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada, fecha_inicio=date(2026, 9, 1),
            motivo="Horario confirmado",
        )

    def test_descanso_sin_marcas_no_crea_falta_ni_extra_incluso_entre_semana(self):
        self.asignar_semana()
        for fecha in (date(2026, 9, 13), date(2026, 9, 9)):
            with self.subTest(fecha=fecha):
                resultado = evaluar_dia_empleado(self.empleado, fecha)
                self.assertEqual(resultado.creados, 0)
                self.assertFalse(IncidenciaAsistencia.objects.filter(
                    empleado=self.empleado, fecha=fecha,
                ).exists())
                self.assertFalse(HoraExtra.objects.filter(empleado=self.empleado, fecha=fecha).exists())

    def test_descanso_resuelve_falta_y_diagnostico_automaticos_sin_borrar_historial(self):
        fecha = date(2026, 9, 9)
        falta = IncidenciaAsistencia.objects.create(
            empleado=self.empleado, fecha=fecha, tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            detalle="Falta original para revisar",
        )
        diagnostico = IncidenciaAsistencia.objects.create(
            empleado=self.empleado, fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
        )
        manual = IncidenciaAsistencia.objects.create(
            empleado=self.empleado, fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            editado_manual=True,
        )
        self.asignar_semana()

        primero = evaluar_dia_empleado(self.empleado, fecha)
        segundo = evaluar_dia_empleado(self.empleado, fecha)

        falta.refresh_from_db()
        diagnostico.refresh_from_db()
        manual.refresh_from_db()
        self.assertEqual((primero.creados, primero.resueltos), (0, 2))
        self.assertEqual((segundo.creados, segundo.resueltos), (0, 0))
        self.assertEqual(falta.estado, IncidenciaAsistencia.ESTADO_RESUELTO)
        self.assertEqual(diagnostico.estado, IncidenciaAsistencia.ESTADO_RESUELTO)
        self.assertEqual(manual.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(IncidenciaAsistencia.objects.filter(empleado=self.empleado, fecha=fecha).count(), 3)
        self.assertEqual(IncidenciaAsistenciaBitacora.objects.filter(
            incidencia__in=[falta, diagnostico], campo="estado",
        ).count(), 2)
        self.assertIn("Falta original para revisar", IncidenciaAsistenciaBitacora.objects.get(
            incidencia=falta, campo="estado",
        ).comentario)

    def test_descanso_con_marcas_conserva_asistencia_y_solicita_revision_sin_turno_ni_extra(self):
        fecha = date(2026, 9, 9)
        self.asignar_semana()
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=fecha, turno=self.turno,
            entrada=dt_local(fecha, time(8)), salida=dt_local(fecha, time(12)),
            minutos_trabajados=240,
        )

        primero = evaluar_dia_empleado(self.empleado, fecha)
        segundo = evaluar_dia_empleado(self.empleado, fecha)

        asistencia.refresh_from_db()
        self.assertIsNone(asistencia.turno)
        self.assertEqual(asistencia.entrada, dt_local(fecha, time(8)))
        self.assertEqual(asistencia.salida, dt_local(fecha, time(12)))
        self.assertEqual(primero.creados, 1)
        self.assertEqual(segundo.creados, 0)
        self.assertFalse(IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, fecha=fecha, tipo=IncidenciaAsistencia.TIPO_FALTA,
        ).exists())
        diagnostico = IncidenciaAsistencia.objects.get(
            empleado=self.empleado, fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
        )
        self.assertEqual(diagnostico.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(diagnostico.asistencia, asistencia)
        self.assertEqual(diagnostico.metadata["motivo"], "descanso")
        self.assertIn("descanso", diagnostico.detalle.lower())
        self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_sin_asignacion_y_legacy_conservan_reglas_previas(self):
        lunes = date(2026, 9, 14)
        sin_registro = evaluar_dia_empleado(self.empleado, lunes)
        self.assertEqual(sin_registro.creados, 1)
        self.assertTrue(IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, fecha=lunes, tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
        ).exists())

        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.turno, fecha_inicio=date(2026, 9, 15),
        )
        martes = date(2026, 9, 15)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=martes, turno=self.turno,
            entrada=dt_local(martes, time(8, 20)), salida=dt_local(martes, time(17)),
            minutos_trabajados=520,
        )
        evaluar_dia_empleado(self.empleado, martes)
        self.assertTrue(IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, fecha=martes, tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
        ).exists())
        self.assertTrue(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_evaluacion_masiva_tambien_respeta_descanso(self):
        self.asignar_semana()
        fecha = date(2026, 9, 9)
        resultado = evaluar_rango_asistencia(fecha, fecha, empleados=[self.empleado])
        self.assertEqual(resultado.creados, 0)
        self.assertFalse(IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, fecha=fecha,
        ).exists())

    def test_domingo_laborable_semanal_sin_asistencia_genera_falta(self):
        self.asignar_semana(descansos=(2,))
        fecha = date(2026, 9, 13)

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        self.assertEqual(resultado.creados, 1)
        self.assertTrue(IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
        ).exists())

    def test_dia_laborable_semanal_conserva_turno_y_extra(self):
        self.asignar_semana()
        fecha = date(2026, 9, 14)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=fecha, turno=self.turno,
            entrada=dt_local(fecha, time(8)), salida=dt_local(fecha, time(17)),
            minutos_trabajados=540,
        )

        evaluar_dia_empleado(self.empleado, fecha)

        asistencia.refresh_from_db()
        self.assertEqual(asistencia.turno, self.turno)
        self.assertTrue(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_legacy_domingo_sin_asistencia_conserva_excepcion_del_calendario(self):
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.turno, fecha_inicio=date(2026, 9, 1),
        )
        fecha = date(2026, 9, 13)

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        self.assertEqual(resultado.creados, 0)
        self.assertFalse(IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, fecha=fecha,
        ).exists())


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

    def test_intervalo_largo_sin_turno_genera_extra_no_calculable(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(fecha, time(8), salida=time(17), minutos=540)
        asistencia.turno = None
        asistencia.save(update_fields=["turno"])
        evaluar_dia_empleado(self.empleado, fecha)
        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
        )
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(incidencia.metadata["motivo"], "sin_turno")
        self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_una_marca_de_comida_genera_marcaje_incompleto(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(fecha, time(8), salida=time(17), minutos=540)
        asistencia.salida_comida = dt_local(fecha, time(12))
        asistencia.regreso_comida = None
        asistencia.save(update_fields=["salida_comida", "regreso_comida"])
        evaluar_dia_empleado(self.empleado, fecha)
        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO,
        )
        self.assertIn("comida", incidencia.detalle.lower())
        self.assertEqual(HoraExtra.objects.get(asistencia=asistencia).horas, Decimal("1.00"))

    def test_point_sin_marcas_de_comida_no_genera_marcaje_incompleto(self):
        fecha = date(2026, 6, 1)
        self.crear_asistencia(
            fecha,
            time(8),
            salida=time(16),
            minutos=480,
            fuente=AsistenciaEmpleado.FUENTE_POINT,
        )
        evaluar_dia_empleado(self.empleado, fecha)
        self.assertFalse(IncidenciaAsistencia.objects.filter(
            empleado=self.empleado,
            fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO,
        ).exists())

    def test_extremo_faltante_genera_marcaje_incompleto_y_se_resuelve_al_corregir(self):
        for offset, campo in enumerate(("entrada", "salida")):
            with self.subTest(campo=campo):
                fecha = date(2026, 6, 1) + timedelta(days=offset)
                asistencia = self.crear_asistencia(fecha, time(8))
                valor_original = getattr(asistencia, campo)
                setattr(asistencia, campo, None)
                asistencia.save(update_fields=[campo])

                resultado = evaluar_dia_empleado(self.empleado, fecha)

                incidencia = IncidenciaAsistencia.objects.get(
                    empleado=self.empleado, fecha=fecha,
                    tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO,
                )
                self.assertEqual(resultado.creados, 1)
                self.assertTrue(incidencia.metadata["falta_entrada_o_salida"])
                self.assertIsNone(incidencia.goce_sueldo)
                self.assertEqual(incidencia.minutos, 0)
                self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())
                self.assertFalse(IncidenciaAsistencia.objects.filter(
                    empleado=self.empleado, fecha=fecha,
                    tipo=IncidenciaAsistencia.TIPO_FALTA,
                ).exists())

                setattr(asistencia, campo, valor_original)
                asistencia.save(update_fields=[campo])
                resultado = evaluar_dia_empleado(self.empleado, fecha)
                incidencia.refresh_from_db()
                self.assertEqual(resultado.resueltos, 1)
                self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_RESUELTO)

    def test_ambos_extremos_ausentes_generan_marcaje_incompleto_idempotente(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(fecha, time(8), salida=None, minutos=0)
        asistencia.entrada = None
        asistencia.save(update_fields=["entrada"])

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        incidencias = IncidenciaAsistencia.objects.filter(empleado=self.empleado, fecha=fecha)
        incidencia = incidencias.get(tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO)
        self.assertEqual(resultado.creados, 1)
        self.assertEqual(incidencias.count(), 1)
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertIs(incidencia.metadata["falta_entrada_o_salida"], True)
        self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

        resultado = evaluar_dia_empleado(self.empleado, fecha)

        self.assertEqual(resultado.creados, 0)
        self.assertEqual(resultado.resueltos, 0)
        self.assertEqual(incidencias.count(), 1)
        reevaluada = incidencias.get()
        self.assertEqual(reevaluada.pk, incidencia.pk)
        self.assertEqual(reevaluada.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_quitar_ultimo_extremo_conserva_marcaje_incompleto_pendiente(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(fecha, time(8))
        asistencia.entrada = None
        asistencia.save(update_fields=["entrada"])
        evaluar_dia_empleado(self.empleado, fecha)
        incidencias = IncidenciaAsistencia.objects.filter(empleado=self.empleado, fecha=fecha)
        incidencia = incidencias.get(tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO)
        asistencia.salida = None
        asistencia.minutos_trabajados = 0
        asistencia.save(update_fields=["salida", "minutos_trabajados"])

        for _ in range(2):
            resultado = evaluar_dia_empleado(self.empleado, fecha)
            self.assertEqual(resultado.creados, 0)
            self.assertEqual(resultado.resueltos, 0)
            self.assertEqual(incidencias.count(), 1)
            reevaluada = incidencias.get()
            self.assertEqual(reevaluada.pk, incidencia.pk)
            self.assertEqual(reevaluada.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
            self.assertIs(reevaluada.metadata["falta_entrada_o_salida"], True)
            self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_extra_no_calculable_se_resuelve_al_asignar_turno(self):
        fecha = date(2026, 6, 1)
        asistencia = self.crear_asistencia(fecha, time(8), salida=time(17), minutos=540)
        asistencia.turno = None
        asistencia.save(update_fields=["turno"])
        evaluar_dia_empleado(self.empleado, fecha)
        incidencia = IncidenciaAsistencia.objects.get(
            empleado=self.empleado, fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
        )
        self.assertEqual(incidencia.metadata["duracion_minutos"], 540)
        self.assertEqual(incidencia.detalle,
            "No se calcularon horas extra: falta asignar el turno de esta jornada.")
        self.assertIsNone(incidencia.goce_sueldo)
        self.assertEqual(incidencia.minutos, 0)

        asistencia.turno = self.turno
        asistencia.save(update_fields=["turno"])
        resultado = evaluar_dia_empleado(self.empleado, fecha)

        incidencia.refresh_from_db()
        self.assertEqual(resultado.resueltos, 1)
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_RESUELTO)
        self.assertTrue(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_extra_no_calculable_solo_para_intervalo_valido_mayor_de_490_sin_turno(self):
        casos = (
            (time(16, 10), None, False),
            (time(16, 11), None, True),
            (time(7), None, False),
            (time(17), time(12), True),
        )
        for offset, (salida, comida, esperado) in enumerate(casos):
            with self.subTest(salida=salida, comida=comida):
                fecha = date(2026, 6, 1) + timedelta(days=offset)
                asistencia = self.crear_asistencia(fecha, time(8), salida=salida, salida_comida=comida)
                asistencia.turno = None
                asistencia.save(update_fields=["turno"])
                evaluar_dia_empleado(self.empleado, fecha)
                self.assertEqual(IncidenciaAsistencia.objects.filter(
                    empleado=self.empleado, fecha=fecha,
                    tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
                ).exists(), esperado)
                self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_corregir_comida_resuelve_incidencia_salvo_edicion_manual(self):
        for offset, manual in enumerate((False, True)):
            with self.subTest(manual=manual):
                fecha = date(2026, 6, 1) + timedelta(days=offset)
                asistencia = self.crear_asistencia(fecha, time(8), salida_comida=time(12))
                evaluar_dia_empleado(self.empleado, fecha)
                incidencia = IncidenciaAsistencia.objects.get(
                    empleado=self.empleado, fecha=fecha,
                    tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO,
                )
                incidencia.editado_manual = manual
                incidencia.save(update_fields=["editado_manual"])
                asistencia.regreso_comida = dt_local(fecha, time(12, 30))
                asistencia.save(update_fields=["regreso_comida"])
                evaluar_dia_empleado(self.empleado, fecha)
                incidencia.refresh_from_db()
                self.assertEqual(incidencia.estado,
                    IncidenciaAsistencia.ESTADO_PENDIENTE if manual else IncidenciaAsistencia.ESTADO_RESUELTO)

    def test_ausencia_de_ambas_marcas_comida_no_es_marcaje_incompleto_en_ninguna_modalidad(self):
        for offset, modalidad in enumerate((Empleado.MARCAJE_DOS_MARCAS,
                Empleado.MARCAJE_CUATRO_MARCAS, Empleado.MARCAJE_RUTA)):
            with self.subTest(modalidad=modalidad):
                self.empleado.modalidad_marcaje = modalidad
                self.empleado.save(update_fields=["modalidad_marcaje"])
                fecha = date(2026, 6, 1) + timedelta(days=offset)
                self.crear_asistencia(fecha, time(8))
                evaluar_dia_empleado(self.empleado, fecha)
                self.assertFalse(IncidenciaAsistencia.objects.filter(
                    empleado=self.empleado, fecha=fecha,
                    tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO,
                ).exists())

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

    def test_no_genera_incidencias_despues_de_fecha_de_baja(self):
        self.empleado.activo = False
        self.empleado.save(update_fields=["activo"])
        EmpleadoBaja.objects.create(
            empleado=self.empleado,
            nombre=self.empleado.nombre,
            fecha_ingreso=self.empleado.fecha_ingreso,
            fecha_baja=date(2026, 6, 1),
            motivo=EmpleadoBaja.MOTIVO_ABANDONO,
        )

        resultado = evaluar_dia_empleado(self.empleado, date(2026, 6, 2))

        self.assertEqual(resultado.creados, 0)
        self.assertFalse(
            IncidenciaAsistencia.objects.filter(empleado=self.empleado, fecha=date(2026, 6, 2)).exists()
        )

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
        self.assertEqual(incidencia.metadata["usos_tolerancia_7d"], 3)

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

        from rrhh.services_vacaciones import crear_solicitud_vacaciones, es_dia_laborable

        fecha = timezone.localdate() - timedelta(days=7)
        while not es_dia_laborable(fecha):
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
            es_dia_laborable,
            rechazar_solicitud_vacaciones,
        )

        fecha = timezone.localdate() - timedelta(days=7)
        while not es_dia_laborable(fecha):
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


class EscalamientosDeduplicadosTests(TestCase):
    """Regresiones del bug de escalamientos repetidos/oscilantes (jul-2026)."""

    def setUp(self):
        self.turno = Turno.objects.create(
            nombre="Matutino",
            hora_entrada=time(8, 0),
            hora_salida=time(16, 0),
            tolerancia_minutos=10,
        )
        self.empleado = Empleado.objects.create(
            nombre="Empleado Escalamientos",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2026, 1, 1),
        )

    def crear_asistencia(self, fecha: date, entrada: time, minutos=475):
        return AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            entrada=dt_local(fecha, entrada),
            salida=dt_local(fecha, time(16, 0)),
            minutos_trabajados=minutos,
            turno=self.turno,
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )

    def _no_resueltas(self, tipo):
        return IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, tipo=tipo
        ).exclude(estado=IncidenciaAsistencia.ESTADO_RESUELTO)

    def test_aviso_y_baja_se_emiten_una_sola_vez_por_episodio(self):
        inicio = date(2026, 6, 1)
        for offset in range(10):
            evaluar_dia_empleado(self.empleado, inicio + timedelta(days=offset))

        avisos = self._no_resueltas(IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS)
        bajas = self._no_resueltas(IncidenciaAsistencia.TIPO_BAJA_FALTAS)
        self.assertEqual(avisos.count(), 1)
        self.assertEqual(avisos.get().fecha, inicio + timedelta(days=2))
        self.assertEqual(bajas.count(), 1)
        self.assertEqual(bajas.get().fecha, inicio + timedelta(days=3))

    def test_reevaluar_el_dia_del_aviso_no_lo_apaga_ni_duplica(self):
        inicio = date(2026, 6, 1)
        for offset in range(3):
            evaluar_dia_empleado(self.empleado, inicio + timedelta(days=offset))
        fecha_aviso = inicio + timedelta(days=2)

        evaluar_dia_empleado(self.empleado, fecha_aviso)
        evaluar_dia_empleado(self.empleado, fecha_aviso)

        avisos = self._no_resueltas(IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS)
        self.assertEqual(avisos.count(), 1)
        self.assertEqual(avisos.get().estado, IncidenciaAsistencia.ESTADO_PENDIENTE)

    def test_reevaluar_el_dia_no_apaga_falta_por_retardos(self):
        inicio = date(2026, 6, 1)
        for offset in range(3):
            IncidenciaAsistencia.objects.create(
                empleado=self.empleado,
                fecha=inicio + timedelta(days=offset),
                tipo=IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
                severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
            )
        fecha = inicio + timedelta(days=3)
        self.crear_asistencia(fecha, time(8, 0), minutos=480)

        evaluar_dia_empleado(self.empleado, fecha)
        evaluar_dia_empleado(self.empleado, fecha)
        evaluar_dia_empleado(self.empleado, fecha)

        faltas_retardos = self._no_resueltas(IncidenciaAsistencia.TIPO_FALTA_RETARDOS)
        self.assertEqual(faltas_retardos.count(), 1)
        self.assertEqual(faltas_retardos.get().estado, IncidenciaAsistencia.ESTADO_PENDIENTE)

    def test_usos_de_tolerancia_generan_un_retardo_por_cada_tres(self):
        inicio = date(2026, 6, 1)
        for offset in range(4):
            fecha = inicio + timedelta(days=offset)
            self.crear_asistencia(fecha, time(8, 5))
            evaluar_dia_empleado(self.empleado, fecha)

        # 4 usos = 1 retardo (no 2, como generaba el bug con >= 3)
        self.assertEqual(
            self._no_resueltas(IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA).count(), 1
        )

        for offset in (4, 5):
            fecha = inicio + timedelta(days=offset)
            self.crear_asistencia(fecha, time(8, 5))
            evaluar_dia_empleado(self.empleado, fecha)

        # 6 usos = 2 retardos
        self.assertEqual(
            self._no_resueltas(IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA).count(), 2
        )

    def test_reevaluar_el_dia_no_apaga_retardo_por_tolerancia(self):
        inicio = date(2026, 6, 1)
        for offset in range(3):
            fecha = inicio + timedelta(days=offset)
            self.crear_asistencia(fecha, time(8, 5))
            evaluar_dia_empleado(self.empleado, fecha)
        fecha_retardo = inicio + timedelta(days=2)

        evaluar_dia_empleado(self.empleado, fecha_retardo)
        evaluar_dia_empleado(self.empleado, fecha_retardo)

        retardos = self._no_resueltas(IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA)
        self.assertEqual(retardos.count(), 1)
        self.assertEqual(retardos.get().estado, IncidenciaAsistencia.ESTADO_PENDIENTE)

    def test_barrido_limpia_avisos_y_bajas_repetidos_heredados(self):
        """Los duplicados que dejó el bug en producción se resuelven al re-evaluar."""
        inicio = date(2026, 6, 1)
        for offset in range(3):
            IncidenciaAsistencia.objects.create(
                empleado=self.empleado,
                fecha=inicio + timedelta(days=offset),
                tipo=IncidenciaAsistencia.TIPO_FALTA,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
                severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            )
        # Avisos repetidos como los generaba el bug: uno por día laborable.
        for offset in range(2, 6):
            IncidenciaAsistencia.objects.create(
                empleado=self.empleado,
                fecha=inicio + timedelta(days=offset),
                tipo=IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
                severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            )

        for offset in range(2, 6):
            evaluar_dia_empleado(self.empleado, inicio + timedelta(days=offset))

        avisos = self._no_resueltas(IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS)
        self.assertEqual(avisos.count(), 1)
        self.assertEqual(avisos.get().fecha, inicio + timedelta(days=2))


class VentanaTolerancia7DiasTests(TestCase):
    """R-03: los usos de tolerancia acumulan en ventana móvil de 7 días (no 15)."""

    def setUp(self):
        self.turno = Turno.objects.create(
            nombre="Matutino",
            hora_entrada=time(8, 0),
            hora_salida=time(16, 0),
            tolerancia_minutos=10,
        )
        self.empleado = Empleado.objects.create(
            nombre="Empleado Ventana Semanal",
            salario_diario=Decimal("400.00"),
            fecha_ingreso=date(2026, 1, 1),
        )

    def _uso(self, fecha: date):
        AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=fecha,
            entrada=dt_local(fecha, time(8, 5)),
            salida=dt_local(fecha, time(16, 0)),
            minutos_trabajados=475,
            turno=self.turno,
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )
        evaluar_dia_empleado(self.empleado, fecha)

    def _retardos(self):
        return IncidenciaAsistencia.objects.filter(
            empleado=self.empleado, tipo=IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA
        ).exclude(estado=IncidenciaAsistencia.ESTADO_RESUELTO)

    def test_tres_usos_dentro_de_la_semana_generan_retardo(self):
        inicio = date(2026, 6, 1)  # lunes
        # lun, mié y sáb: tres usos dentro de la misma ventana de 7 días
        for offset in (0, 2, 5):
            self._uso(inicio + timedelta(days=offset))
        self.assertEqual(self._retardos().count(), 1)

    def test_tres_usos_repartidos_en_mas_de_siete_dias_no_generan_retardo(self):
        inicio = date(2026, 6, 1)
        # usos en días 0, 8 y 16: nunca hay 3 dentro de una ventana de 7 días
        for offset in (0, 8, 16):
            self._uso(inicio + timedelta(days=offset))
        self.assertEqual(self._retardos().count(), 0)
