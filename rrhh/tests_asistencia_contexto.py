from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from django.test import TestCase

from rrhh.models import (
    AsignacionJornadaEmpleado,
    AsistenciaEmpleado,
    Empleado,
    EmpleadoBaja,
    IncapacidadEmpleado,
    IncidenciaAsistencia,
    JornadaSemanal,
    JornadaSemanalDia,
    PermisoSalida,
    SolicitudVacaciones,
    SuspensionEmpleado,
    Turno,
)
from rrhh.services_asistencia_contexto import (
    CODIGO_ASISTENCIA,
    CODIGO_DESCANSO,
    CODIGO_FALTA,
    CODIGO_FESTIVO,
    CODIGO_INCAPACIDAD,
    CODIGO_PERMISO,
    CODIGO_POST_BAJA,
    CODIGO_PREINGRESO,
    CODIGO_RETARDO,
    CODIGO_SUSPENSION,
    CODIGO_VACACIONES,
    cargar_contexto_asistencia,
)


TZ = ZoneInfo("America/Mazatlan")


class ContextoAsistenciaTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            codigo="CTX-001",
            nombre="Contexto Empleado",
            fecha_ingreso=date(2026, 1, 1),
            area="HORNOS",
        )

    def clasificar(self, fecha):
        lote = cargar_contexto_asistencia(
            empleados=[self.empleado],
            fecha_inicio=date(2026, 8, 28),
            fecha_fin=date(2026, 9, 26),
        )
        return lote.clasificar(self.empleado, fecha)

    def test_incapacidad_cerrada_justifica_ausencia_hasta_su_fecha_fin(self):
        incapacidad = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 9, 1),
            estado=IncapacidadEmpleado.ESTADO_CERRADA,
        )

        resultado = self.clasificar(date(2026, 9, 1))

        self.assertEqual(resultado.codigo, CODIGO_INCAPACIDAD)
        self.assertFalse(resultado.es_exigible)
        self.assertFalse(resultado.falta_penalizable)
        self.assertEqual(resultado.fuente_id, incapacidad.id)

    def test_festivo_oficial_no_es_falta(self):
        resultado = self.clasificar(date(2026, 9, 16))

        self.assertEqual(resultado.codigo, CODIGO_FESTIVO)
        self.assertFalse(resultado.es_exigible)
        self.assertFalse(resultado.falta_penalizable)

    def test_descanso_de_jornada_no_es_falta(self):
        turno = Turno.objects.create(nombre="Contexto 8 a 4", hora_entrada=time(8), hora_salida=time(16))
        jornada = JornadaSemanal.objects.create(nombre="Contexto semanal")
        for dia in range(7):
            JornadaSemanalDia.objects.create(
                jornada=jornada,
                dia_semana=dia,
                turno=None if dia == 6 else turno,
            )
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado,
            jornada=jornada,
            fecha_inicio=date(2026, 8, 1),
            motivo="Horario vigente",
        )

        resultado = self.clasificar(date(2026, 9, 6))

        self.assertEqual(resultado.codigo, CODIGO_DESCANSO)
        self.assertFalse(resultado.falta_penalizable)

    def test_preingreso_y_postbaja_no_son_exigibles(self):
        self.empleado.fecha_ingreso = date(2026, 9, 2)
        self.empleado.save(update_fields=["fecha_ingreso"])
        preingreso = self.clasificar(date(2026, 9, 1))
        self.assertEqual(preingreso.codigo, CODIGO_PREINGRESO)
        self.assertFalse(preingreso.es_exigible)

        EmpleadoBaja.objects.create(
            empleado=self.empleado,
            nombre=self.empleado.nombre,
            fecha_ingreso=self.empleado.fecha_ingreso,
            fecha_baja=date(2026, 9, 10),
        )
        self.empleado.refresh_from_db()
        postbaja = self.clasificar(date(2026, 9, 11))
        self.assertEqual(postbaja.codigo, CODIGO_POST_BAJA)
        self.assertFalse(postbaja.falta_penalizable)

    def test_suspension_vacaciones_y_permiso_reutilizan_rrhh(self):
        SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 9, 3),
            fecha_fin=date(2026, 9, 3),
            motivo="Suspension vigente",
        )
        SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 9, 4),
            fecha_fin=date(2026, 9, 4),
            estado=SolicitudVacaciones.ESTADO_PREAUTORIZADA,
        )
        PermisoSalida.objects.create(
            empleado=self.empleado,
            tipo=PermisoSalida.TIPO_PERMISO_DIA,
            fecha_inicio=datetime(2026, 9, 5, 0, tzinfo=TZ),
            fecha_fin=datetime(2026, 9, 5, 23, 59, tzinfo=TZ),
            motivo="Permiso aprobado",
            estado=PermisoSalida.ESTADO_APROBADO,
        )

        suspension = self.clasificar(date(2026, 9, 3))
        vacaciones = self.clasificar(date(2026, 9, 4))
        permiso = self.clasificar(date(2026, 9, 5))

        self.assertEqual(suspension.codigo, CODIGO_SUSPENSION)
        self.assertEqual(vacaciones.codigo, CODIGO_VACACIONES)
        self.assertEqual(permiso.codigo, CODIGO_PERMISO)
        self.assertFalse(any(x.falta_penalizable for x in (suspension, vacaciones, permiso)))

    def test_asistencia_retardo_y_falta_sin_justificacion(self):
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 9, 7),
            entrada=datetime(2026, 9, 7, 8, tzinfo=TZ),
        )
        AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 9, 8),
            entrada=datetime(2026, 9, 8, 8, 20, tzinfo=TZ),
        )
        IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 9, 8),
            tipo=IncidenciaAsistencia.TIPO_RETARDO,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
        )

        presente = self.clasificar(date(2026, 9, 7))
        retardo = self.clasificar(date(2026, 9, 8))
        falta = self.clasificar(date(2026, 9, 9))

        self.assertEqual(presente.codigo, CODIGO_ASISTENCIA)
        self.assertEqual(presente.fuente_id, asistencia.id)
        self.assertEqual(retardo.codigo, CODIGO_RETARDO)
        self.assertFalse(retardo.falta_penalizable)
        self.assertEqual(falta.codigo, CODIGO_FALTA)
        self.assertTrue(falta.es_exigible)
        self.assertTrue(falta.falta_penalizable)

    def test_asistencia_real_prevalece_sobre_exencion_de_checador(self):
        self.empleado.exento_checador = True
        self.empleado.exento_checador_motivo = "Exención histórica durante incapacidad"
        self.empleado.save(update_fields=["exento_checador", "exento_checador_motivo"])
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 9, 7),
            entrada=datetime(2026, 9, 7, 8, tzinfo=TZ),
        )

        resultado = self.clasificar(date(2026, 9, 7))

        self.assertEqual(resultado.codigo, CODIGO_ASISTENCIA)
        self.assertTrue(resultado.es_exigible)
        self.assertFalse(resultado.falta_penalizable)
        self.assertEqual(resultado.fuente_id, asistencia.id)

    def test_clasificador_es_solo_lectura_y_consultas_no_crecen_por_empleado_dia(self):
        segundo = Empleado.objects.create(
            codigo="CTX-002",
            nombre="Contexto Segundo",
            fecha_ingreso=date(2026, 1, 1),
        )
        with self.assertNumQueries(9):
            lote = cargar_contexto_asistencia(
                empleados=[self.empleado, segundo],
                fecha_inicio=date(2026, 9, 1),
                fecha_fin=date(2026, 9, 3),
            )
        with self.assertNumQueries(0):
            for empleado in (self.empleado, segundo):
                for dia in range(1, 4):
                    lote.clasificar(empleado, date(2026, 9, dia))

        self.assertEqual(IncidenciaAsistencia.objects.count(), 0)
