from datetime import date
from decimal import Decimal

from django.db import IntegrityError, transaction
from django.test import TestCase

from rrhh.models import (
    AplicacionGoceVacaciones,
    Empleado,
    PeriodoVacacional,
    SolicitudVacaciones,
)


class PeriodoVacacionalModelTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleada Prueba FIFO",
            fecha_ingreso=date(2022, 3, 7),
        )
        self.solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 7, 20),
            fecha_fin=date(2026, 7, 24),
            dias_laborables=Decimal("5"),
        )
        self.periodo = PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2026, 3, 7),
            fecha_limite=date(2026, 9, 7),
            antiguedad_anios=4,
            dias_generados=Decimal("18.00"),
        )

    def test_empleado_no_duplica_aniversario(self):
        PeriodoVacacional.objects.create(
            empleado=self.empleado, aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7), antiguedad_anios=3,
            dias_generados=Decimal("16.00"),
        )
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                PeriodoVacacional.objects.create(
                    empleado=self.empleado, aniversario=date(2025, 3, 7),
                    fecha_limite=date(2025, 9, 7), antiguedad_anios=3,
                    dias_generados=Decimal("16.00"),
                )

    def test_periodo_no_admite_dias_negativos(self):
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                PeriodoVacacional.objects.create(
                    empleado=self.empleado, aniversario=date(2024, 3, 7),
                    fecha_limite=date(2024, 9, 7), antiguedad_anios=2,
                    dias_generados=Decimal("-1.00"),
                )

    def test_aplicacion_requiere_dias_positivos(self):
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                AplicacionGoceVacaciones.objects.create(
                    solicitud=self.solicitud, periodo=self.periodo,
                    dias=Decimal("0"), estado="reservada",
                )

    def test_aplicacion_no_duplica_periodo_por_solicitud(self):
        AplicacionGoceVacaciones.objects.create(
            solicitud=self.solicitud, periodo=self.periodo,
            dias=Decimal("3"), estado="reservada",
        )
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                AplicacionGoceVacaciones.objects.create(
                    solicitud=self.solicitud, periodo=self.periodo,
                    dias=Decimal("2"), estado="reservada",
                )
