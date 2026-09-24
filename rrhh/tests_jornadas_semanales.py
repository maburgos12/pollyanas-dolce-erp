from contextlib import nullcontext
from datetime import date, time
from queue import Queue
from threading import Event, Thread
from time import monotonic

from django.core.exceptions import ValidationError
from django.db import IntegrityError, close_old_connections, connection, connections, transaction
from django.test import TestCase, TransactionTestCase

from rrhh.models import (
    AsignacionJornadaEmpleado, AsignacionTurnoEmpleado, AsistenciaEmpleado,
    Empleado, JornadaSemanal, JornadaSemanalDia, Turno,
)
from rrhh.services_turnos import (
    ESTADO_DESCANSO, ESTADO_LABORABLE, ESTADO_SIN_ASIGNACION,
    asignar_jornada_empleado, es_jornada_historica_antes_de_asignacion,
    horario_programado_para_fecha, turno_asignado_para_fecha,
)


class JornadaSemanalResolverTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(codigo="JORNADA-RES", nombre="Persona administrativa")
        self.lunes = Turno.objects.create(
            nombre="Administrativo lunes a viernes", hora_entrada=time(8, 30),
            hora_salida=time(16, 30),
        )
        self.sabado = Turno.objects.create(
            nombre="Administrativo sábado", hora_entrada=time(8, 30),
            hora_salida=time(13, 30),
        )

    def asignar_semana(self, *, completa=True, proteger=False):
        jornada = JornadaSemanal.objects.create(nombre="Administrativa")
        for dia in range(7 if completa else 6):
            JornadaSemanalDia.objects.create(
                jornada=jornada, dia_semana=dia,
                turno=self.lunes if dia < 5 else self.sabado if dia == 5 else None,
            )
        asignacion = AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada, fecha_inicio=date(2026, 9, 1),
            motivo="Horario confirmado", proteger_reingesta_historica=proteger,
        )
        return asignacion

    def test_jornada_administrativa_resuelve_lunes_sabado_y_descanso(self):
        asignacion = self.asignar_semana()
        for fecha, turno, estado, salida in (
            (date(2026, 9, 7), self.lunes, ESTADO_LABORABLE, time(16, 30)),
            (date(2026, 9, 12), self.sabado, ESTADO_LABORABLE, time(13, 30)),
            (date(2026, 9, 13), None, ESTADO_DESCANSO, None),
        ):
            with self.subTest(fecha=fecha):
                horario = horario_programado_para_fecha(self.empleado, fecha)
                self.assertEqual(horario.estado, estado)
                self.assertEqual(horario.turno, turno)
                self.assertEqual(horario.asignacion, asignacion)
                self.assertEqual(horario.turno.hora_salida if horario.turno else None, salida)
                self.assertEqual(turno_asignado_para_fecha(self.empleado, fecha), turno)

    def test_fallback_legacy_conserva_turno_y_estado_laborable(self):
        asignacion = AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.lunes, fecha_inicio=date(2026, 9, 1),
        )
        horario = horario_programado_para_fecha(self.empleado, date(2026, 9, 7))
        self.assertEqual(horario.estado, ESTADO_LABORABLE)
        self.assertEqual(horario.turno, self.lunes)
        self.assertEqual(horario.asignacion, asignacion)
        self.assertEqual(turno_asignado_para_fecha(self.empleado, date(2026, 9, 7)), self.lunes)

    def test_sin_asignacion_se_distingue_de_descanso(self):
        horario = horario_programado_para_fecha(self.empleado, date(2026, 9, 7))
        self.assertEqual(horario.estado, ESTADO_SIN_ASIGNACION)
        self.assertIsNone(horario.turno)
        self.assertIsNone(horario.asignacion)

    def test_jornada_semanal_vigente_prevalece_sobre_legacy(self):
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.lunes, fecha_inicio=date(2026, 9, 1),
        )
        asignacion = self.asignar_semana()
        horario = horario_programado_para_fecha(self.empleado, date(2026, 9, 12))
        self.assertEqual(horario.estado, ESTADO_LABORABLE)
        self.assertEqual(horario.turno, self.sabado)
        self.assertEqual(horario.asignacion, asignacion)
        self.assertEqual(turno_asignado_para_fecha(self.empleado, date(2026, 9, 12)), self.sabado)

    def test_dos_asignaciones_semanales_vigentes_se_rechazan(self):
        primera = self.asignar_semana()
        # objects.create no ejecuta full_clean; la restricción SQL solo impide
        # fechas de inicio iguales, lo que permite simular datos ya corruptos.
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=primera.jornada,
            fecha_inicio=date(2026, 9, 2), motivo="Traslape corrupto",
        )
        with self.assertRaises(ValidationError):
            horario_programado_para_fecha(self.empleado, date(2026, 9, 7))

    def test_perfil_semanal_incompleto_se_rechaza(self):
        self.asignar_semana(completa=False)
        with self.assertRaises(ValidationError):
            horario_programado_para_fecha(self.empleado, date(2026, 9, 7))

    def test_reingesta_historica_reconoce_asignacion_semanal_protegida(self):
        self.asignar_semana(proteger=True)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=date(2026, 9, 7),
        )
        self.assertTrue(es_jornada_historica_antes_de_asignacion(asistencia))

    def test_reingesta_historica_respeta_proteccion_legacy_y_semanal(self):
        legacy = AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.lunes, fecha_inicio=date(2026, 9, 1),
            proteger_reingesta_historica=True,
        )
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=date(2026, 9, 7),
        )
        self.assertTrue(es_jornada_historica_antes_de_asignacion(asistencia))
        legacy.proteger_reingesta_historica = False
        legacy.save(update_fields=["proteger_reingesta_historica"])
        self.assertFalse(es_jornada_historica_antes_de_asignacion(asistencia))
        self.asignar_semana(proteger=False)
        self.assertFalse(es_jornada_historica_antes_de_asignacion(asistencia))


class JornadaSemanalModelTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(codigo="JORNADA-001", nombre="Persona con jornada")

    def jornada(self, nombre="Semana base"):
        return JornadaSemanal.objects.create(nombre=nombre)

    def test_vigencia_de_jornada_rechaza_fin_anterior_al_inicio(self):
        datos = {
            "nombre": "Vigencia inválida",
            "vigencia_desde": date(2026, 9, 10),
            "vigencia_hasta": date(2026, 9, 9),
        }
        with self.assertRaises(ValidationError):
            JornadaSemanal(**datos).full_clean()
        with self.assertRaises(IntegrityError), transaction.atomic():
            JornadaSemanal.objects.create(**datos)

    def test_un_dia_solo_puede_aparecer_una_vez_por_jornada(self):
        jornada = self.jornada()
        JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=0, turno=None)

        with self.assertRaises(ValidationError):
            JornadaSemanalDia(jornada=jornada, dia_semana=0, turno=None).full_clean()
        with self.assertRaises(IntegrityError), transaction.atomic():
            JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=0, turno=None)

        otro = JornadaSemanalDia(jornada=jornada, dia_semana=1, turno=None)
        otro.full_clean()
        otro.save()

    def test_dia_fuera_de_la_semana_se_rechaza(self):
        jornada = self.jornada()

        for dia in (-1, 7):
            with self.subTest(dia=dia), self.assertRaises(ValidationError):
                JornadaSemanalDia(jornada=jornada, dia_semana=dia).full_clean()

    def test_vigencias_traslapadas_se_rechazan(self):
        jornada = self.jornada()
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 14), motivo="Inicial",
        )

        for inicio, fin in (
            (date(2026, 9, 14), date(2026, 9, 20)),
            (date(2026, 8, 25), date(2026, 9, 1)),
            (date(2026, 9, 2), date(2026, 9, 10)),
            (date(2026, 8, 25), None),
        ):
            with self.subTest(inicio=inicio, fin=fin), self.assertRaises(ValidationError):
                AsignacionJornadaEmpleado(
                    empleado=self.empleado, jornada=jornada,
                    fecha_inicio=inicio, fecha_fin=fin, motivo="Cambio",
                ).full_clean()

    def test_vigencias_contiguas_y_empleados_distintos_se_permiten(self):
        jornada = self.jornada()
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 14), motivo="Inicial",
        )

        siguiente = AsignacionJornadaEmpleado(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 15), fecha_fin=None, motivo="Continuidad",
        )
        siguiente.full_clean()
        siguiente.save()

        otra_persona = Empleado.objects.create(codigo="JORNADA-002", nombre="Otra persona")
        paralelo = AsignacionJornadaEmpleado(
            empleado=otra_persona, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=None, motivo="Ingreso",
        )
        paralelo.full_clean()
        paralelo.save()

    def test_motivo_es_obligatorio(self):
        asignacion = AsignacionJornadaEmpleado(
            empleado=self.empleado, jornada=self.jornada(),
            fecha_inicio=date(2026, 9, 1), motivo="",
        )
        with self.assertRaises(ValidationError):
            asignacion.full_clean()

    def test_servicio_asigna_y_rechaza_traslape(self):
        jornada = self.jornada()
        primera = asignar_jornada_empleado(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 14),
            motivo="Horario confirmado", actor=None,
        )
        self.assertEqual(primera.motivo, "Horario confirmado")

        with self.assertRaises(ValidationError):
            asignar_jornada_empleado(
                empleado=self.empleado, jornada=jornada,
                fecha_inicio=date(2026, 9, 14), fecha_fin=None,
                motivo="Traslape", actor=None,
            )


class AsignacionJornadaConcurrenteTests(TransactionTestCase):
    def test_dos_primeras_asignaciones_simultaneas_se_serializan(self):
        empleado = Empleado.objects.create(codigo="JORNADA-CONC", nombre="Persona concurrente")
        jornada = JornadaSemanal.objects.create(nombre="Semana concurrente")
        primera_bloqueada = Event()
        liberar_primera = Event()
        segunda_terminada = Event()
        resultados = Queue()
        pid_segunda = Queue()

        def asignar(nombre):
            close_old_connections()
            pid = None
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '8s'")
                    cursor.execute("SELECT pg_backend_pid()")
                    pid = cursor.fetchone()[0]
                if nombre == "segunda":
                    pid_segunda.put(pid)

                def observar(execute, sql, params, many, context):
                    resultado = execute(sql, params, many, context)
                    if 'FROM "rrhh_empleado"' in sql and "FOR UPDATE" in sql:
                        primera_bloqueada.set()
                        if not liberar_primera.wait(10):
                            raise TimeoutError("No se liberó la primera asignación")
                    return resultado

                observacion = connection.execute_wrapper(observar) if nombre == "primera" else nullcontext()
                with observacion:
                    asignacion = asignar_jornada_empleado(
                        empleado=empleado, jornada=jornada,
                        fecha_inicio=date(2026, 9, 1), fecha_fin=None,
                        motivo="Asignación concurrente", actor=None,
                    )
                resultados.put((nombre, pid, asignacion.pk))
            except Exception as exc:
                resultados.put((nombre, pid, exc))
            finally:
                if nombre == "segunda":
                    segunda_terminada.set()
                connections.close_all()

        primera = Thread(target=asignar, args=("primera",), daemon=True)
        segunda = Thread(target=asignar, args=("segunda",), daemon=True)
        primera.start()
        try:
            self.assertTrue(primera_bloqueada.wait(5))
            segunda.start()
            pid = pid_segunda.get(timeout=5)
            bloqueada = False
            limite = monotonic() + 5
            while monotonic() < limite and not segunda_terminada.is_set():
                with connection.cursor() as cursor:
                    cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                    bloqueada = cursor.fetchone()[0]
                if bloqueada:
                    break
                segunda_terminada.wait(0.01)
            self.assertTrue(bloqueada, "La segunda asignación debe esperar la fila del empleado")
        finally:
            liberar_primera.set()
            primera.join(timeout=12)
            if segunda.ident is not None:
                segunda.join(timeout=12)

        self.assertFalse(primera.is_alive() or segunda.is_alive())
        recibidos = [resultados.get(timeout=2) for _ in range(2)]
        resultado = {nombre: (pid, valor) for nombre, pid, valor in recibidos}
        self.assertNotEqual(resultado["primera"][0], resultado["segunda"][0])
        self.assertIsInstance(resultado["primera"][1], int)
        self.assertIsInstance(resultado["segunda"][1], ValidationError)
        self.assertEqual(AsignacionJornadaEmpleado.objects.filter(empleado=empleado).count(), 1)
