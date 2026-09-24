from datetime import date

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase

from rrhh.models import AsignacionJornadaEmpleado, Empleado, JornadaSemanal, JornadaSemanalDia
from rrhh.services_turnos import asignar_jornada_empleado


class JornadaSemanalModelTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(codigo="JORNADA-001", nombre="Persona con jornada")

    def jornada(self, nombre="Semana base"):
        return JornadaSemanal.objects.create(nombre=nombre)

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
