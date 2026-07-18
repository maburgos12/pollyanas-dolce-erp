from datetime import date
from decimal import Decimal
from queue import Queue
from threading import Barrier, Thread

from django.core.exceptions import ValidationError
from django.db import IntegrityError, close_old_connections, connections, transaction
from django.test import TestCase, TransactionTestCase

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
        with self.assertRaises((IntegrityError, ValidationError)):
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
        with self.assertRaises((IntegrityError, ValidationError)):
            with transaction.atomic():
                AplicacionGoceVacaciones.objects.create(
                    solicitud=self.solicitud, periodo=self.periodo,
                    dias=Decimal("2"), estado="reservada",
                )

    def test_aplicacion_rechaza_solicitud_y_periodo_de_empleados_distintos(self):
        otro_empleado = Empleado.objects.create(
            nombre="Otro Empleado", fecha_ingreso=date(2021, 1, 15)
        )
        periodo_otro = PeriodoVacacional.objects.create(
            empleado=otro_empleado,
            aniversario=date(2025, 1, 15),
            fecha_limite=date(2025, 7, 15),
            antiguedad_anios=4,
            dias_generados=Decimal("18.00"),
        )
        with self.assertRaises(ValidationError):
            AplicacionGoceVacaciones.objects.create(
                solicitud=self.solicitud,
                periodo=periodo_otro,
                dias=Decimal("3"),
                estado="reservada",
            )


class SaldoPeriodoVacacionalTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleada Saldo Vacacional",
            fecha_ingreso=date(2022, 3, 7),
        )
        self.periodo = PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7),
            antiguedad_anios=3,
            dias_generados=Decimal("16.00"),
        )

    def _crear_aplicacion(self, dias, estado, indice):
        solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 8, 1 + indice),
            fecha_fin=date(2026, 8, 1 + indice),
            dias_laborables=dias,
        )
        AplicacionGoceVacaciones.objects.create(
            solicitud=solicitud,
            periodo=self.periodo,
            dias=dias,
            estado=estado,
        )

    def test_separa_reserva_y_consumo_e_ignora_liberada_y_revertida(self):
        from rrhh.services_vacaciones_saldos import saldo_periodo_vacacional

        self._crear_aplicacion(Decimal("2.00"), AplicacionGoceVacaciones.ESTADO_RESERVADA, 0)
        self._crear_aplicacion(Decimal("3.00"), AplicacionGoceVacaciones.ESTADO_CONSUMIDA, 1)
        self._crear_aplicacion(Decimal("4.00"), AplicacionGoceVacaciones.ESTADO_LIBERADA, 2)
        self._crear_aplicacion(Decimal("5.00"), AplicacionGoceVacaciones.ESTADO_REVERTIDA, 3)

        saldo = saldo_periodo_vacacional(self.periodo)

        self.assertEqual(saldo.periodo_id, self.periodo.id)
        self.assertEqual(saldo.aniversario, date(2025, 3, 7))
        self.assertEqual(saldo.dias_generados, Decimal("16.00"))
        self.assertEqual(saldo.reservado, Decimal("2.00"))
        self.assertEqual(saldo.gozado, Decimal("3.00"))
        self.assertEqual(saldo.disponible_goce, Decimal("11.00"))


class ReservarGoceFifoTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleada Reserva FIFO",
            fecha_ingreso=date(2022, 3, 7),
        )
        self.periodo_2025 = PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7),
            antiguedad_anios=3,
            dias_generados=Decimal("7.00"),
        )
        self.periodo_2026 = PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2026, 3, 7),
            fecha_limite=date(2026, 9, 7),
            antiguedad_anios=4,
            dias_generados=Decimal("18.00"),
        )
        self.solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 8, 10),
            fecha_fin=date(2026, 8, 14),
            dias_laborables=Decimal("5.00"),
        )

    def test_reserva_completa_en_el_periodo_mas_antiguo(self):
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        aplicaciones = reservar_goce_fifo(self.solicitud, Decimal("5.00"))

        self.assertEqual(len(aplicaciones), 1)
        self.assertEqual(aplicaciones[0].periodo_id, self.periodo_2025.id)
        self.assertEqual(aplicaciones[0].dias, Decimal("5.00"))
        self.assertEqual(
            aplicaciones[0].estado,
            AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )

    def test_divide_fifo_siete_de_2025_y_tres_de_2026(self):
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        aplicaciones = reservar_goce_fifo(self.solicitud, Decimal("10.00"))

        self.assertEqual(
            [(aplicacion.periodo_id, aplicacion.dias) for aplicacion in aplicaciones],
            [
                (self.periodo_2025.id, Decimal("7.00")),
                (self.periodo_2026.id, Decimal("3.00")),
            ],
        )
        self.assertEqual(
            list(
                self.solicitud.aplicaciones_goce.values_list(
                    "periodo_id", "dias", "estado"
                )
            ),
            [
                (
                    self.periodo_2025.id,
                    Decimal("7.00"),
                    AplicacionGoceVacaciones.ESTADO_RESERVADA,
                ),
                (
                    self.periodo_2026.id,
                    Decimal("3.00"),
                    AplicacionGoceVacaciones.ESTADO_RESERVADA,
                ),
            ],
        )

    def test_saldo_insuficiente_revierte_toda_aplicacion_parcial(self):
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        with self.assertRaisesMessage(ValidationError, "Faltan 1.00 días"):
            reservar_goce_fifo(self.solicitud, Decimal("26.00"))

        self.assertFalse(self.solicitud.aplicaciones_goce.exists())

    def test_rechaza_dias_no_positivos_sin_escrituras(self):
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        for dias in (Decimal("0"), Decimal("-1.00")):
            with self.subTest(dias=dias):
                with self.assertRaisesMessage(
                    ValidationError, "Los días a reservar deben ser mayores que cero"
                ):
                    reservar_goce_fifo(self.solicitud, dias)
                self.assertFalse(self.solicitud.aplicaciones_goce.exists())

    def test_rechaza_segunda_reserva_sin_duplicar_aplicaciones(self):
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        reservar_goce_fifo(self.solicitud, Decimal("5.00"))

        with self.assertRaisesMessage(
            ValidationError, "La solicitud ya tiene aplicaciones de goce"
        ):
            reservar_goce_fifo(self.solicitud, Decimal("2.00"))

        self.assertEqual(self.solicitud.aplicaciones_goce.count(), 1)
        self.assertEqual(
            self.solicitud.aplicaciones_goce.get().dias,
            Decimal("5.00"),
        )


class ReservarGoceFifoConcurrenciaTests(TransactionTestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            nombre="Empleada Concurrencia FIFO",
            fecha_ingreso=date(2022, 3, 7),
        )
        self.periodo = PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7),
            antiguedad_anios=3,
            dias_generados=Decimal("7.00"),
        )
        self.solicitudes = [
            SolicitudVacaciones.objects.create(
                empleado=self.empleado,
                fecha_inicio=date(2026, 9, 1 + indice),
                fecha_fin=date(2026, 9, 1 + indice),
                dias_laborables=Decimal("7.00"),
            )
            for indice in range(2)
        ]

    def test_dos_solicitudes_compiten_sin_sobreasignar_la_bolsa(self):
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        barrera = Barrier(2)
        resultados = Queue()

        def competir(solicitud_id):
            close_old_connections()
            try:
                solicitud = SolicitudVacaciones.objects.get(pk=solicitud_id)
                barrera.wait(timeout=10)
                aplicaciones = reservar_goce_fifo(solicitud, Decimal("7.00"))
                resultados.put(("ok", solicitud_id, [item.pk for item in aplicaciones]))
            except Exception as exc:
                resultados.put(("error", solicitud_id, exc))
            finally:
                connections.close_all()

        threads = [
            Thread(target=competir, args=(solicitud.pk,), daemon=True)
            for solicitud in self.solicitudes
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        obtenidos = [resultados.get(timeout=2) for _ in threads]
        exitos = [resultado for resultado in obtenidos if resultado[0] == "ok"]
        errores_validacion = [
            resultado
            for resultado in obtenidos
            if resultado[0] == "error" and isinstance(resultado[2], ValidationError)
        ]

        self.assertEqual(len(exitos), 1, obtenidos)
        self.assertEqual(len(errores_validacion), 1, obtenidos)

        aplicaciones_activas = AplicacionGoceVacaciones.objects.filter(
            estado__in=[
                AplicacionGoceVacaciones.ESTADO_RESERVADA,
                AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
            ]
        )
        total_activo = sum(
            aplicaciones_activas.values_list("dias", flat=True), Decimal("0")
        )
        self.assertLessEqual(total_activo, self.periodo.dias_generados)

        solicitud_perdedora_id = errores_validacion[0][1]
        self.assertFalse(
            AplicacionGoceVacaciones.objects.filter(
                solicitud_id=solicitud_perdedora_id
            ).exists()
        )
