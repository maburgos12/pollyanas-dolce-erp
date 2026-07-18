from datetime import date
from decimal import Decimal
from queue import Queue
from threading import Barrier, Event, Thread

from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.db import (
    IntegrityError,
    OperationalError,
    close_old_connections,
    connection,
    connections,
    transaction,
)
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext

from rrhh.models import (
    AplicacionGoceVacaciones,
    Empleado,
    MovimientoVacaciones,
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

    def test_rechaza_reservar_solicitud_rechazada_sin_crear_aplicaciones(self):
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        self.solicitud.estado = SolicitudVacaciones.ESTADO_RECHAZADA
        self.solicitud.save(update_fields=["estado"])

        with self.assertRaisesMessage(
            ValidationError,
            "Solo se puede reservar goce para solicitudes en estado solicitada",
        ):
            reservar_goce_fifo(self.solicitud, Decimal("5.00"))

        self.assertFalse(self.solicitud.aplicaciones_goce.exists())


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


class CicloGoceVacacionesFifoTests(TestCase):
    def setUp(self):
        self.rrhh_user = User.objects.create_user(username="paula.fifo")
        self.rrhh_user.groups.add(Group.objects.create(name="RRHH"))
        self.jefe_user = User.objects.create_user(username="carolina.jefa")
        self.jefa = Empleado.objects.create(
            nombre="Jefa FIFO",
            fecha_ingreso=date(2020, 1, 1),
            usuario_erp=self.jefe_user,
            activo=True,
        )
        self.empleada = Empleado.objects.create(
            nombre="Carolina FIFO",
            fecha_ingreso=date(2022, 3, 7),
            jefe_directo=self.jefa,
            activo=True,
        )
        self.periodo_2025 = PeriodoVacacional.objects.create(
            empleado=self.empleada,
            aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7),
            antiguedad_anios=3,
            dias_generados=Decimal("7.00"),
        )
        self.periodo_2026 = PeriodoVacacional.objects.create(
            empleado=self.empleada,
            aniversario=date(2026, 3, 7),
            fecha_limite=date(2026, 9, 7),
            antiguedad_anios=4,
            dias_generados=Decimal("18.00"),
        )

    def _crear(self, fecha_inicio, fecha_fin):
        from rrhh.services_vacaciones import crear_solicitud_vacaciones

        return crear_solicitud_vacaciones(
            empleado=self.empleada,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            motivo="Descanso FIFO",
            actor=self.rrhh_user,
        )

    def _movimientos(self, solicitud):
        return list(
            MovimientoVacaciones.objects.filter(solicitud=solicitud)
            .order_by("id")
            .values_list("tipo", "periodo_anio", "dias")
        )

    def test_caso_carolina_consume_cinco_dias_del_periodo_2025(self):
        from rrhh.services_vacaciones import aprobar_solicitud_vacaciones_rrhh
        from rrhh.services_vacaciones_saldos import saldo_periodo_vacacional

        solicitud = self._crear(date(2026, 8, 10), date(2026, 8, 14))
        aprobar_solicitud_vacaciones_rrhh(solicitud, self.rrhh_user)

        aplicacion = solicitud.aplicaciones_goce.get()
        self.assertEqual(aplicacion.periodo, self.periodo_2025)
        self.assertEqual(aplicacion.dias, Decimal("5.00"))
        self.assertEqual(aplicacion.estado, AplicacionGoceVacaciones.ESTADO_CONSUMIDA)
        self.assertEqual(
            self._movimientos(solicitud),
            [
                (MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("5.00")),
                (MovimientoVacaciones.TIPO_LIBERADO, 2025, Decimal("5.00")),
                (MovimientoVacaciones.TIPO_CONSUMIDO, 2025, Decimal("5.00")),
            ],
        )
        self.assertEqual(
            saldo_periodo_vacacional(self.periodo_2025).disponible_goce,
            Decimal("2.00"),
        )
        self.assertEqual(
            saldo_periodo_vacacional(self.periodo_2026).disponible_goce,
            Decimal("18.00"),
        )

    def test_solicitud_de_diez_dias_reserva_y_registra_movimientos_por_periodo(self):
        solicitud = self._crear(date(2026, 8, 3), date(2026, 8, 13))

        self.assertEqual(solicitud.dias_laborables, Decimal("10"))
        self.assertEqual(
            list(
                solicitud.aplicaciones_goce.order_by("periodo__aniversario", "id")
                .values_list("periodo_id", "dias", "estado")
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
        self.assertEqual(
            self._movimientos(solicitud),
            [
                (MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("7.00")),
                (MovimientoVacaciones.TIPO_RESERVADO, 2026, Decimal("3.00")),
            ],
        )

    def test_rechazo_jefe_libera_exactamente_las_dos_reservas(self):
        from rrhh.services_vacaciones import preautorizar_solicitud_vacaciones_jefe

        solicitud = self._crear(date(2026, 8, 3), date(2026, 8, 13))
        preautorizar_solicitud_vacaciones_jefe(
            solicitud,
            self.jefe_user,
            aprobar=False,
        )

        self.assertEqual(
            list(
                solicitud.aplicaciones_goce.order_by("periodo__aniversario", "id")
                .values_list("periodo_id", "dias", "estado")
            ),
            [
                (self.periodo_2025.id, Decimal("7.00"), AplicacionGoceVacaciones.ESTADO_LIBERADA),
                (self.periodo_2026.id, Decimal("3.00"), AplicacionGoceVacaciones.ESTADO_LIBERADA),
            ],
        )
        self.assertEqual(
            self._movimientos(solicitud),
            [
                (MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("7.00")),
                (MovimientoVacaciones.TIPO_RESERVADO, 2026, Decimal("3.00")),
                (MovimientoVacaciones.TIPO_LIBERADO, 2025, Decimal("7.00")),
                (MovimientoVacaciones.TIPO_LIBERADO, 2026, Decimal("3.00")),
            ],
        )

    def test_rechazo_rrhh_libera_la_reserva(self):
        from rrhh.services_vacaciones import rechazar_solicitud_vacaciones

        solicitud = self._crear(date(2026, 8, 10), date(2026, 8, 14))
        rechazar_solicitud_vacaciones(solicitud, self.rrhh_user)

        aplicacion = solicitud.aplicaciones_goce.get()
        self.assertEqual(aplicacion.estado, AplicacionGoceVacaciones.ESTADO_LIBERADA)
        self.assertEqual(
            self._movimientos(solicitud),
            [
                (MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("5.00")),
                (MovimientoVacaciones.TIPO_LIBERADO, 2025, Decimal("5.00")),
            ],
        )

    def test_saldo_insuficiente_revierte_solicitud_aplicaciones_y_movimientos(self):
        from rrhh.services_vacaciones import crear_solicitud_vacaciones

        with self.assertRaisesMessage(ValidationError, "Saldo vacacional insuficiente"):
            crear_solicitud_vacaciones(
                empleado=self.empleada,
                fecha_inicio=date(2026, 8, 3),
                fecha_fin=date(2026, 9, 5),
                motivo="Excede bolsas",
                actor=self.rrhh_user,
            )

        self.assertFalse(SolicitudVacaciones.objects.filter(empleado=self.empleada).exists())
        self.assertFalse(AplicacionGoceVacaciones.objects.filter(periodo__empleado=self.empleada).exists())
        self.assertFalse(MovimientoVacaciones.objects.filter(empleado=self.empleada).exists())

    def test_aprobacion_sin_reservas_falla_sin_cambiar_estado(self):
        from rrhh.services_vacaciones import aprobar_solicitud_vacaciones_rrhh

        solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleada,
            fecha_inicio=date(2026, 8, 10),
            fecha_fin=date(2026, 8, 14),
            dias_laborables=Decimal("5.00"),
        )

        with self.assertRaisesMessage(ValidationError, "reservas de goce"):
            aprobar_solicitud_vacaciones_rrhh(solicitud, self.rrhh_user)

        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estado, SolicitudVacaciones.ESTADO_SOLICITADA)
        self.assertFalse(MovimientoVacaciones.objects.filter(solicitud=solicitud).exists())

    def test_aprobacion_con_suma_inconsistente_falla_sin_cambiar_estado(self):
        from rrhh.services_vacaciones import aprobar_solicitud_vacaciones_rrhh

        solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleada,
            fecha_inicio=date(2026, 8, 10),
            fecha_fin=date(2026, 8, 14),
            dias_laborables=Decimal("5.00"),
        )
        aplicacion = AplicacionGoceVacaciones.objects.create(
            solicitud=solicitud,
            periodo=self.periodo_2025,
            dias=Decimal("4.00"),
            estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )

        with self.assertRaisesMessage(ValidationError, "no coincide"):
            aprobar_solicitud_vacaciones_rrhh(solicitud, self.rrhh_user)

        solicitud.refresh_from_db()
        aplicacion.refresh_from_db()
        self.assertEqual(solicitud.estado, SolicitudVacaciones.ESTADO_SOLICITADA)
        self.assertEqual(aplicacion.estado, AplicacionGoceVacaciones.ESTADO_RESERVADA)
        self.assertFalse(MovimientoVacaciones.objects.filter(solicitud=solicitud).exists())


class CicloGoceVacacionesConcurrenciaTests(TransactionTestCase):
    def setUp(self):
        self.rrhh_user = User.objects.create_user(username="paula.concurrente")
        self.rrhh_user.groups.add(Group.objects.create(name="RRHH"))
        self.empleada = Empleado.objects.create(
            nombre="Empleada Ciclo Concurrente",
            fecha_ingreso=date(2022, 3, 7),
            activo=True,
        )
        self.periodo = PeriodoVacacional.objects.create(
            empleado=self.empleada,
            aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7),
            antiguedad_anios=3,
            dias_generados=Decimal("10.00"),
        )

    def _crear_solicitud(self, fecha_inicio, fecha_fin):
        from rrhh.services_vacaciones import crear_solicitud_vacaciones

        return crear_solicitud_vacaciones(
            empleado=self.empleada,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            motivo="Concurrencia FIFO",
            actor=self.rrhh_user,
        )

    def _ejecutar_en_paralelo(self, operaciones):
        barrera = Barrier(len(operaciones))
        resultados = Queue()

        def ejecutar(nombre, operacion):
            close_old_connections()
            try:
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute("SET LOCAL lock_timeout = '3s'")
                    barrera.wait(timeout=10)
                    valor = operacion()
                resultados.put((nombre, "ok", valor))
            except Exception as exc:
                resultados.put((nombre, "error", exc))
            finally:
                connections.close_all()

        threads = [
            Thread(target=ejecutar, args=(nombre, operacion), daemon=True)
            for nombre, operacion in operaciones
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        return {resultado[0]: resultado[1:] for resultado in [resultados.get(timeout=2) for _ in threads]}

    def test_helper_bloquea_solo_aplicaciones_y_no_periodos(self):
        from rrhh.services_vacaciones_saldos import aplicaciones_reservadas_bloqueadas

        solicitud = self._crear_solicitud(date(2026, 8, 10), date(2026, 8, 14))

        with transaction.atomic(), CaptureQueriesContext(connection) as consultas:
            aplicaciones_reservadas_bloqueadas(solicitud)

        consulta_lock = next(
            consulta["sql"]
            for consulta in consultas.captured_queries
            if "FOR UPDATE" in consulta["sql"]
            and "rrhh_aplicaciongocevacaciones" in consulta["sql"]
        )
        self.assertIn(
            'FOR UPDATE OF "rrhh_aplicaciongocevacaciones"',
            consulta_lock,
        )

    def test_reserva_concurrente_con_aprobacion_termina_sin_deadlock(self):
        from rrhh.services_vacaciones import (
            aprobar_solicitud_vacaciones_rrhh,
            crear_solicitud_vacaciones,
        )

        solicitud_aprobar = self._crear_solicitud(
            date(2026, 8, 10), date(2026, 8, 14)
        )

        def reservar():
            empleado = Empleado.objects.get(pk=self.empleada.pk)
            actor = User.objects.get(pk=self.rrhh_user.pk)
            solicitud = crear_solicitud_vacaciones(
                empleado=empleado,
                fecha_inicio=date(2026, 8, 17),
                fecha_fin=date(2026, 8, 21),
                motivo="Reserva concurrente",
                actor=actor,
            )
            return solicitud.pk

        def aprobar():
            solicitud = SolicitudVacaciones.objects.get(pk=solicitud_aprobar.pk)
            actor = User.objects.get(pk=self.rrhh_user.pk)
            aprobar_solicitud_vacaciones_rrhh(solicitud, actor)
            return solicitud.pk

        resultados = self._ejecutar_en_paralelo(
            [("reservar", reservar), ("aprobar", aprobar)]
        )

        for nombre, (estado, valor) in resultados.items():
            self.assertEqual(estado, "ok", (nombre, valor))
            self.assertNotIsInstance(valor, OperationalError)
        solicitud_aprobar.refresh_from_db()
        self.assertEqual(
            solicitud_aprobar.estado,
            SolicitudVacaciones.ESTADO_APROBADA,
        )
        self.assertEqual(
            solicitud_aprobar.aplicaciones_goce.get().estado,
            AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
        )
        self.assertEqual(
            list(
                solicitud_aprobar.movimientos.order_by("id").values_list(
                    "tipo", "periodo_anio", "dias"
                )
            ),
            [
                (MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("5.00")),
                (MovimientoVacaciones.TIPO_LIBERADO, 2025, Decimal("5.00")),
                (MovimientoVacaciones.TIPO_CONSUMIDO, 2025, Decimal("5.00")),
            ],
        )
        solicitud_reservada = SolicitudVacaciones.objects.get(
            pk=resultados["reservar"][1]
        )
        self.assertEqual(
            solicitud_reservada.aplicaciones_goce.get().estado,
            AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )
        self.assertEqual(
            list(
                solicitud_reservada.movimientos.values_list(
                    "tipo", "periodo_anio", "dias"
                )
            ),
            [(MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("5.00"))],
        )

    def test_reserva_concurrente_con_rechazo_termina_con_resultado_consistente(self):
        from rrhh.services_vacaciones import (
            crear_solicitud_vacaciones,
            rechazar_solicitud_vacaciones,
        )

        self.periodo.dias_generados = Decimal("5.00")
        self.periodo.save(update_fields=["dias_generados"])
        solicitud_rechazar = self._crear_solicitud(
            date(2026, 8, 10), date(2026, 8, 14)
        )

        def reservar():
            empleado = Empleado.objects.get(pk=self.empleada.pk)
            actor = User.objects.get(pk=self.rrhh_user.pk)
            solicitud = crear_solicitud_vacaciones(
                empleado=empleado,
                fecha_inicio=date(2026, 8, 17),
                fecha_fin=date(2026, 8, 21),
                motivo="Reserva tras rechazo concurrente",
                actor=actor,
            )
            return solicitud.pk

        def rechazar():
            solicitud = SolicitudVacaciones.objects.get(pk=solicitud_rechazar.pk)
            actor = User.objects.get(pk=self.rrhh_user.pk)
            rechazar_solicitud_vacaciones(solicitud, actor)
            return solicitud.pk

        resultados = self._ejecutar_en_paralelo(
            [("reservar", reservar), ("rechazar", rechazar)]
        )

        estado_rechazo, valor_rechazo = resultados["rechazar"]
        self.assertEqual(estado_rechazo, "ok", valor_rechazo)
        self.assertNotIsInstance(valor_rechazo, OperationalError)
        solicitud_rechazar.refresh_from_db()
        self.assertEqual(
            solicitud_rechazar.estado,
            SolicitudVacaciones.ESTADO_RECHAZADA,
        )
        self.assertEqual(
            solicitud_rechazar.aplicaciones_goce.get().estado,
            AplicacionGoceVacaciones.ESTADO_LIBERADA,
        )
        self.assertEqual(
            list(
                solicitud_rechazar.movimientos.order_by("id").values_list(
                    "tipo", "periodo_anio", "dias"
                )
            ),
            [
                (MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("5.00")),
                (MovimientoVacaciones.TIPO_LIBERADO, 2025, Decimal("5.00")),
            ],
        )

        estado_reserva, valor_reserva = resultados["reservar"]
        self.assertNotIsInstance(valor_reserva, OperationalError)
        if estado_reserva == "ok":
            solicitud_reservada = SolicitudVacaciones.objects.get(pk=valor_reserva)
            self.assertEqual(
                solicitud_reservada.aplicaciones_goce.get().estado,
                AplicacionGoceVacaciones.ESTADO_RESERVADA,
            )
            self.assertEqual(
                list(
                    solicitud_reservada.movimientos.values_list(
                        "tipo", "periodo_anio", "dias"
                    )
                ),
                [(MovimientoVacaciones.TIPO_RESERVADO, 2025, Decimal("5.00"))],
            )
        else:
            self.assertIsInstance(valor_reserva, ValidationError)
            self.assertEqual(
                SolicitudVacaciones.objects.filter(
                    motivo="Reserva tras rechazo concurrente"
                ).count(),
                0,
            )

    def test_misma_solicitud_no_reserva_despues_de_commit_de_rechazo(self):
        from rrhh.services_vacaciones import rechazar_solicitud_vacaciones
        from rrhh.services_vacaciones_saldos import reservar_goce_fifo

        solicitud = SolicitudVacaciones.objects.create(
            empleado=self.empleada,
            fecha_inicio=date(2026, 8, 10),
            fecha_fin=date(2026, 8, 14),
            dias_laborables=Decimal("5.00"),
            motivo="Misma solicitud concurrente",
        )
        rechazo_confirmado = Event()
        resultados = Queue()

        def rechazar():
            close_old_connections()
            try:
                actor = User.objects.get(pk=self.rrhh_user.pk)
                objetivo = SolicitudVacaciones.objects.get(pk=solicitud.pk)
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute("SET LOCAL lock_timeout = '3s'")
                    rechazar_solicitud_vacaciones(objetivo, actor)
                resultados.put(("rechazar", "ok", objetivo.pk))
            except Exception as exc:
                resultados.put(("rechazar", "error", exc))
            finally:
                rechazo_confirmado.set()
                connections.close_all()

        def reservar():
            close_old_connections()
            try:
                if not rechazo_confirmado.wait(timeout=10):
                    raise TimeoutError("El rechazo no terminó a tiempo.")
                objetivo = SolicitudVacaciones.objects.get(pk=solicitud.pk)
                aplicaciones = reservar_goce_fifo(objetivo, Decimal("5.00"))
                resultados.put(
                    ("reservar", "ok", [aplicacion.pk for aplicacion in aplicaciones])
                )
            except Exception as exc:
                resultados.put(("reservar", "error", exc))
            finally:
                connections.close_all()

        threads = [
            Thread(target=rechazar, daemon=True),
            Thread(target=reservar, daemon=True),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        obtenidos = {
            resultado[0]: resultado[1:]
            for resultado in [resultados.get(timeout=2) for _ in threads]
        }
        self.assertEqual(obtenidos["rechazar"][0], "ok", obtenidos)
        self.assertEqual(obtenidos["reservar"][0], "error", obtenidos)
        self.assertIsInstance(obtenidos["reservar"][1], ValidationError)
        self.assertNotIsInstance(obtenidos["reservar"][1], OperationalError)

        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estado, SolicitudVacaciones.ESTADO_RECHAZADA)
        self.assertFalse(solicitud.aplicaciones_goce.exists())
        self.assertFalse(
            solicitud.movimientos.filter(
                tipo=MovimientoVacaciones.TIPO_RESERVADO
            ).exists()
        )
