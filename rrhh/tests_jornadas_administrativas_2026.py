"""Contrato de la carga administrativa de jornadas de septiembre a diciembre de 2026."""

from datetime import date, datetime, time
from decimal import Decimal
from io import StringIO
import json
from queue import Queue
from threading import Barrier, Event, Thread
from time import sleep
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import close_old_connections, connection, transaction
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import AuditLog
from rrhh.models import (
    AsignacionJornadaEmpleado, AsistenciaEmpleado, Empleado, HoraExtra,
    IncidenciaAsistencia, JornadaSemanal, JornadaSemanalDia, Turno,
)
from rrhh.services_jornadas_administrativas_2026 import (
    ConfiguracionJornadasError, configurar_jornadas_administrativas_2026,
)
from rrhh.services_extra_bloqueos import bloquear_jornadas_extra


PERSONAS = (
    (53, "EGUINO REYES LUIS OCTAVIO"),
    (4, "NORZAGARAY CONTRERAS JULIETA GUADALUPE"),
    (3, "SOTO INZUNZA YESENIA"),
    (99, "FIGUEROA SOTO JOHAN"),
    (33, "LUGO ESPINOZA PAULA ELIZABETH"),
    (8, "LOPEZ PALOS JOHANA ADELIN"),
)


def crear_personas():
    for pk, nombre in PERSONAS:
        Empleado.objects.create(pk=pk, codigo=f"JORNADA-2026-{pk}", nombre=nombre)


def crear_actor():
    return get_user_model().objects.create_user(
        username="rrhh_jornadas_2026", password="test", is_superuser=True,
    )


def aplicar(*, actor, hoy=date(2026, 9, 24)):
    preview = configurar_jornadas_administrativas_2026(hoy=hoy)
    return configurar_jornadas_administrativas_2026(
        aplicar=True, hoy=hoy, actor=actor,
        expected_fingerprint=preview["fingerprint"],
    )


class PreviewJornadasAdministrativasTests(TransactionTestCase):
    def setUp(self):
        crear_personas()

    def test_preview_reporta_seis_y_no_ejecuta_escrituras(self):
        fecha = date(2026, 9, 17)
        AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        with CaptureQueriesContext(connection) as queries:
            plan = configurar_jornadas_administrativas_2026(hoy=date(2026, 9, 24))
        self.assertEqual(plan["modo"], "preview")
        self.assertEqual(plan["personas_objetivo"], 6)
        self.assertEqual([p["id"] for p in plan["personas"]], [3, 4, 8, 33, 53, 99])
        self.assertEqual(len(plan["turnos"]), 3)
        self.assertEqual(len(plan["jornadas"]), 2)
        self.assertEqual(len(plan["asignaciones"]), 6)
        self.assertEqual(plan["pendientes_a_reconciliar"][0]["accion"], "crear")
        self.assertEqual(plan["conflictos"], [])
        json.dumps(plan)
        escrituras = [q["sql"] for q in queries if q["sql"].lstrip().upper().startswith((
            "INSERT", "UPDATE", "DELETE", "MERGE", "SELECT NEXTVAL", "SELECT SETVAL",
        ))]
        self.assertEqual(escrituras, [])
        self.assertFalse(AuditLog.objects.exists())


class ConcurrenciaJornadasAdministrativasTests(TransactionTestCase):
    def setUp(self):
        crear_personas()
        self.actor = crear_actor()

    def test_dos_apply_con_misma_huella_no_duplican_asignaciones(self):
        huella = configurar_jornadas_administrativas_2026(hoy=date(2026, 9, 24))["fingerprint"]
        inicio = Barrier(3)
        resultados = Queue()

        def ejecutar():
            close_old_connections()
            inicio.wait()
            try:
                configurar_jornadas_administrativas_2026(
                    aplicar=True, hoy=date(2026, 9, 24), actor=self.actor,
                    expected_fingerprint=huella,
                )
                resultados.put("aplicado")
            except ConfiguracionJornadasError:
                resultados.put("obsoleto")
            finally:
                close_old_connections()

        hilos = [Thread(target=ejecutar) for _ in range(2)]
        for hilo in hilos:
            hilo.start()
        inicio.wait()
        for hilo in hilos:
            hilo.join(timeout=20)
            self.assertFalse(hilo.is_alive())
        self.assertEqual(sorted([resultados.get_nowait() for _ in hilos]), ["aplicado", "obsoleto"])
        self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 6)

    def test_apply_y_guardado_normal_de_extra_no_se_bloquean_mutuamente(self):
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        huella = configurar_jornadas_administrativas_2026(hoy=fecha)["fingerprint"]
        diario_adquirido = Event()
        continuar_guardado = Event()
        resultados = Queue()

        def guardado_normal():
            close_old_connections()
            try:
                with transaction.atomic():
                    bloquear_jornadas_extra([(3, fecha)])
                    diario_adquirido.set()
                    self.assertTrue(continuar_guardado.wait(10))
                    HoraExtra.objects.create(
                        empleado_id=3, asistencia_id=asistencia.pk, fecha=fecha,
                        horas=Decimal("1.00"), notas="[Detección automática] normal",
                    )
                resultados.put("guardado")
            except Exception as exc:
                resultados.put(type(exc).__name__)
            finally:
                close_old_connections()

        def aplicar_t6():
            close_old_connections()
            try:
                configurar_jornadas_administrativas_2026(
                    aplicar=True, hoy=fecha, actor=self.actor,
                    expected_fingerprint=huella,
                )
                resultados.put("aplicado")
            except ConfiguracionJornadasError:
                resultados.put("obsoleto")
            except Exception as exc:
                resultados.put(type(exc).__name__)
            finally:
                close_old_connections()

        normal = Thread(target=guardado_normal)
        carga = Thread(target=aplicar_t6)
        normal.start()
        self.assertTrue(diario_adquirido.wait(10))
        carga.start()
        sleep(0.3)
        continuar_guardado.set()
        for hilo in (normal, carga):
            hilo.join(timeout=15)
            self.assertFalse(hilo.is_alive(), "posible deadlock entre locks diarios y filas")
        self.assertEqual(sorted([resultados.get_nowait() for _ in range(2)]), ["guardado", "obsoleto"])


class AplicacionJornadasAdministrativasTests(TestCase):
    def setUp(self):
        crear_personas()
        self.actor = crear_actor()
        self.hoy = date(2026, 9, 24)

    def test_identidad_ausente_nombre_distinto_o_inactivo_bloquea_todo(self):
        for cambio in ("ausente", "nombre", "inactivo"):
            with self.subTest(cambio=cambio):
                empleado = Empleado.objects.get(pk=53)
                original = (empleado.nombre, empleado.activo)
                if cambio == "ausente":
                    empleado.delete()
                elif cambio == "nombre":
                    empleado.nombre = "Persona distinta"
                    empleado.save(update_fields=["nombre", "nombre_normalizado"])
                else:
                    empleado.activo = False
                    empleado.save(update_fields=["activo"])
                plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
                self.assertTrue(plan["conflictos"])
                with self.assertRaises(ConfiguracionJornadasError):
                    configurar_jornadas_administrativas_2026(
                        aplicar=True, hoy=self.hoy, actor=self.actor,
                        expected_fingerprint=plan["fingerprint"],
                    )
                self.assertFalse(JornadaSemanal.objects.exists())
                if cambio == "ausente":
                    Empleado.objects.create(pk=53, codigo="JORNADA-2026-53", nombre=original[0])
                else:
                    empleado.nombre, empleado.activo = original
                    empleado.save()

    def test_apply_idempotente_y_semana_exacta_de_48_horas(self):
        Empleado.objects.create(pk=101, codigo="FUERA", nombre="Fuera de manifiesto")
        primero = aplicar(actor=self.actor)
        self.assertEqual(primero["modo"], "apply")
        self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 6)
        self.assertEqual(Turno.objects.count(), 3)
        self.assertEqual(JornadaSemanal.objects.count(), 2)
        self.assertEqual(JornadaSemanalDia.objects.count(), 14)
        self.assertFalse(AsignacionJornadaEmpleado.objects.filter(empleado_id=101).exists())
        for jornada in JornadaSemanal.objects.all():
            dias = list(jornada.dias.select_related("turno").order_by("dia_semana"))
            self.assertEqual([d.dia_semana for d in dias], list(range(7)))
            self.assertIsNone(dias[6].turno)
            self.assertEqual(sum(
                (d.turno.hora_salida.hour * 60 + d.turno.hora_salida.minute)
                - (d.turno.hora_entrada.hour * 60 + d.turno.hora_entrada.minute)
                for d in dias if d.turno
            ), 48 * 60)
        self.assertEqual(
            {(a.fecha_inicio, a.fecha_fin) for a in AsignacionJornadaEmpleado.objects.all()},
            {(date(2026, 9, 1), date(2026, 12, 31))},
        )
        segundo = aplicar(actor=self.actor)
        self.assertEqual(segundo["aplicadas"], [])
        self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 6)

    def test_traslape_y_fingerprint_obsoleto_no_escriben(self):
        previa = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        Turno.objects.create(nombre="Administrativa 2026 08:00-16:30", hora_entrada=time(7), hora_salida=time(16))
        with self.assertRaises(ConfiguracionJornadasError):
            configurar_jornadas_administrativas_2026(
                aplicar=True, hoy=self.hoy, actor=self.actor,
                expected_fingerprint=previa["fingerprint"],
            )
        self.assertFalse(AsignacionJornadaEmpleado.objects.exists())
        self.assertFalse(JornadaSemanal.objects.exists())

    def test_fingerprint_obsoleto_sin_conflicto_aborta_antes_de_escribir(self):
        previa = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        Turno.objects.create(nombre="Horario compatible", hora_entrada=time(8),
                             hora_salida=time(16, 30), deteccion_por_checada=False)
        nueva = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        self.assertNotEqual(previa["fingerprint"], nueva["fingerprint"])
        self.assertFalse(nueva["conflictos"])
        with self.assertRaisesMessage(ConfiguracionJornadasError, "huella"):
            configurar_jornadas_administrativas_2026(
                aplicar=True, hoy=self.hoy, actor=self.actor,
                expected_fingerprint=previa["fingerprint"],
            )
        self.assertFalse(JornadaSemanal.objects.exists())
        self.assertFalse(AuditLog.objects.exists())

    def test_edicion_de_incidencia_recalculable_cambia_huella_y_aborta_apply(self):
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        incidencia = IncidenciaAsistencia.objects.create(
            empleado_id=3, fecha=fecha, asistencia=asistencia,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            detalle="Sin turno anterior", metadata={"origen": "automatico"},
        )
        previa = configurar_jornadas_administrativas_2026(hoy=fecha)
        incidencia.detalle = "Comentario operativo nuevo"
        incidencia.save(update_fields=["detalle", "actualizado_en"])
        nueva = configurar_jornadas_administrativas_2026(hoy=fecha)
        self.assertNotEqual(previa["fingerprint"], nueva["fingerprint"])
        with self.assertRaisesMessage(ConfiguracionJornadasError, "huella"):
            configurar_jornadas_administrativas_2026(
                aplicar=True, hoy=fecha, actor=self.actor,
                expected_fingerprint=previa["fingerprint"],
            )
        incidencia.refresh_from_db()
        self.assertEqual(incidencia.detalle, "Comentario operativo nuevo")
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertFalse(JornadaSemanal.objects.exists())
        incidencia.editado_manual = True
        incidencia.save(update_fields=["editado_manual", "actualizado_en"])
        manual = configurar_jornadas_administrativas_2026(hoy=fecha)
        self.assertNotEqual(nueva["fingerprint"], manual["fingerprint"])
        incidencia.editado_manual = False
        incidencia.estado = IncidenciaAsistencia.ESTADO_CONCILIADO
        incidencia.save(update_fields=["editado_manual", "estado", "actualizado_en"])
        conciliada = configurar_jornadas_administrativas_2026(hoy=fecha)
        self.assertNotEqual(nueva["fingerprint"], conciliada["fingerprint"])

    def test_turno_ya_correcto_sin_extra_tambien_propone_una_hora(self):
        turno = Turno.objects.create(
            nombre="Administrativa 2026 08:00-16:30", hora_entrada=time(8),
            hora_salida=time(16, 30), deteccion_por_checada=False,
        )
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha, turno=turno,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        plan = configurar_jornadas_administrativas_2026(hoy=fecha)
        self.assertEqual(plan["asistencias_a_actualizar"], [])
        self.assertEqual([(p["accion"], p["nuevo"]) for p in plan["pendientes_a_reconciliar"]],
                         [("crear", "1.00")])
        aplicar(actor=self.actor, hoy=fecha)
        self.assertEqual(HoraExtra.objects.get(asistencia=asistencia).horas, Decimal("1.00"))

    def test_cancelacion_bajo_umbral_no_genera_review_en_segundo_apply(self):
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 19))),
        )
        [pendiente] = HoraExtra.objects.bulk_create([HoraExtra(
            empleado_id=3, asistencia=asistencia, fecha=fecha,
            horas=Decimal("1.00"), notas="[Detección automática] propuesta",
        )])
        primero = aplicar(actor=self.actor, hoy=fecha)
        pendiente.refresh_from_db()
        self.assertEqual(pendiente.estado, HoraExtra.ESTADO_CANCELADO)
        self.assertTrue(any(x["modelo"] == "HoraExtra" and x["accion"] == "cancelar"
                            for x in primero["aplicadas"]))
        self.assertEqual(primero["extras_resueltas_con_diferencia"], [])
        self.assertEqual(configurar_jornadas_administrativas_2026(hoy=fecha)["extras_resueltas_con_diferencia"], [])
        segundo = aplicar(actor=self.actor, hoy=fecha)
        self.assertEqual(segundo["aplicadas"], [])
        self.assertFalse(AuditLog.objects.filter(action="REVIEW").exists())

    def test_asistencia_cambia_solo_en_rango_y_descanso_queda_sin_turno(self):
        viejo = Turno.objects.create(nombre="Viejo", hora_entrada=time(7), hora_salida=time(15))
        fechas = [date(2026, 8, 31), date(2026, 9, 17), date(2026, 9, 19),
                  date(2026, 9, 20), date(2027, 1, 1)]
        for fecha in fechas:
            AsistenciaEmpleado.objects.create(empleado_id=3, fecha=fecha, turno=viejo)
        aplicar(actor=self.actor)
        filas = {a.fecha: a for a in AsistenciaEmpleado.objects.filter(empleado_id=3)}
        self.assertEqual(filas[date(2026, 8, 31)].turno_id, viejo.pk)
        self.assertEqual(filas[date(2027, 1, 1)].turno_id, viejo.pk)
        self.assertEqual(filas[date(2026, 9, 17)].turno.hora_entrada, time(8))
        self.assertEqual(filas[date(2026, 9, 19)].turno.hora_salida, time(13, 30))
        self.assertIsNone(filas[date(2026, 9, 20)].turno)

    def test_apply_requiere_actor_autorizado_y_fingerprint(self):
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        with self.assertRaises(ConfiguracionJornadasError):
            configurar_jornadas_administrativas_2026(
                aplicar=True, hoy=self.hoy, actor=self.actor,
            )
        self.actor.is_active = False
        self.actor.save(update_fields=["is_active"])
        with self.assertRaises(ConfiguracionJornadasError):
            configurar_jornadas_administrativas_2026(
                aplicar=True, hoy=self.hoy, actor=self.actor,
                expected_fingerprint=plan["fingerprint"],
            )
        self.assertFalse(JornadaSemanal.objects.exists())

    def test_comando_preview_y_validacion_de_apply(self):
        stdout = StringIO()
        call_command("configurar_jornadas_administrativas_2026", stdout=stdout)
        self.assertEqual(json.loads(stdout.getvalue())["personas_objetivo"], 6)
        with self.assertRaises(CommandError):
            call_command("configurar_jornadas_administrativas_2026", apply=True, stdout=StringIO())

    def test_comando_aplica_con_actor_y_huella_fresca_en_base_de_prueba(self):
        plan = configurar_jornadas_administrativas_2026()
        stdout = StringIO()
        call_command(
            "configurar_jornadas_administrativas_2026", apply=True,
            actor_username=self.actor.username,
            expected_fingerprint=plan["fingerprint"], stdout=stdout,
        )
        resultado = json.loads(stdout.getvalue())
        self.assertEqual(resultado["modo"], "apply")
        self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 6)
        self.assertTrue(AuditLog.objects.filter(user=self.actor, model="rrhh.AsignacionJornadaEmpleado").exists())

    def test_fingerprint_no_cambia_por_formato_del_nombre(self):
        anterior = configurar_jornadas_administrativas_2026(hoy=self.hoy)["fingerprint"]
        empleado = Empleado.objects.get(pk=53)
        empleado.nombre = "  Eguino   Reyes Luis  Octavio  "
        empleado.save()
        self.assertEqual(configurar_jornadas_administrativas_2026(hoy=self.hoy)["fingerprint"], anterior)

    def test_traslape_existente_aborta_sin_crear_catalogos(self):
        turno = Turno.objects.create(nombre="Previo", hora_entrada=time(8), hora_salida=time(16))
        jornada = JornadaSemanal.objects.create(nombre="Previa")
        for dia in range(7):
            JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=dia, turno=turno)
        AsignacionJornadaEmpleado.objects.create(
            empleado_id=3, jornada=jornada, fecha_inicio=date(2026, 9, 15),
            fecha_fin=date(2026, 9, 30), motivo="Existente",
        )
        previo = (Turno.objects.count(), JornadaSemanal.objects.count())
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        self.assertIn("asignacion_traslapada", [c["tipo"] for c in plan["conflictos"]])
        with self.assertRaises(ConfiguracionJornadasError):
            configurar_jornadas_administrativas_2026(
                aplicar=True, hoy=self.hoy, actor=self.actor,
                expected_fingerprint=plan["fingerprint"],
            )
        self.assertEqual((Turno.objects.count(), JornadaSemanal.objects.count()), previo)

    def test_extra_pendiente_se_cancela_bajo_umbral_y_se_crea_desde_cincuenta(self):
        fecha_baja = date(2026, 9, 17)
        fecha_alta = date(2026, 9, 18)
        def dt(fecha, hora):
            return timezone.make_aware(datetime.combine(fecha, hora))
        baja = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha_baja, entrada=dt(fecha_baja, time(8)),
            salida=dt(fecha_baja, time(17, 19)),
        )
        alta = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha_alta, entrada=dt(fecha_alta, time(8)),
            salida=dt(fecha_alta, time(17, 20)),
        )
        [pendiente] = HoraExtra.objects.bulk_create([HoraExtra(
            empleado_id=3, asistencia=baja, fecha=fecha_baja, horas=Decimal("1.00"),
            notas="[Detección automática] Propuesta anterior",
        )])
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        acciones = {p["fecha"]: p["accion"] for p in plan["pendientes_a_reconciliar"]}
        self.assertEqual(acciones, {fecha_baja.isoformat(): "cancelar", fecha_alta.isoformat(): "crear"})
        aplicar(actor=self.actor)
        pendiente.refresh_from_db()
        self.assertEqual(pendiente.estado, HoraExtra.ESTADO_CANCELADO)
        nueva = HoraExtra.objects.get(asistencia=alta)
        self.assertEqual(nueva.horas, Decimal("0.83"))
        self.assertEqual(nueva.estado, HoraExtra.ESTADO_PENDIENTE)

    def test_resueltas_y_manual_quedan_intactas_y_diferencia_se_audita(self):
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        [resuelta, manual] = HoraExtra.objects.bulk_create([
            HoraExtra(empleado_id=3, asistencia=asistencia, fecha=fecha,
                      horas=Decimal("0.50"), estado=HoraExtra.ESTADO_AUTORIZADO,
                      monto_calculado=Decimal("123.45"), notas="decisión humana"),
            HoraExtra(empleado_id=3, fecha=fecha, horas=Decimal("0.25"),
                      estado=HoraExtra.ESTADO_PENDIENTE, notas="manual"),
        ])
        antes = list(HoraExtra.objects.filter(pk__in=[resuelta.pk, manual.pk]).order_by("pk")
                     .values("id", "estado", "horas", "monto_calculado", "notas", "asistencia_id"))
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        self.assertEqual([r["id"] for r in plan["extras_resueltas_con_diferencia"]], [resuelta.pk])
        aplicar(actor=self.actor)
        despues = list(HoraExtra.objects.filter(pk__in=[resuelta.pk, manual.pk]).order_by("pk")
                       .values("id", "estado", "horas", "monto_calculado", "notas", "asistencia_id"))
        self.assertEqual(despues, antes)
        self.assertTrue(AuditLog.objects.filter(user=self.actor, action="REVIEW",
                                                object_id=str(resuelta.pk)).exists())
        conteo = AuditLog.objects.filter(action="REVIEW").count()
        aplicar(actor=self.actor)
        self.assertEqual(AuditLog.objects.filter(action="REVIEW").count(), conteo)

    def test_rollback_si_falla_despues_de_primera_escritura(self):
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        with patch("rrhh.services_jornadas_administrativas_2026._auditar", side_effect=RuntimeError("fallo")):
            with self.assertRaises(RuntimeError):
                configurar_jornadas_administrativas_2026(
                    aplicar=True, hoy=self.hoy, actor=self.actor,
                    expected_fingerprint=plan["fingerprint"],
                )
        self.assertFalse(Turno.objects.exists())
        self.assertFalse(AuditLog.objects.exists())

    def test_reutiliza_turnos_exactos_con_otro_nombre_y_sigue_idempotente(self):
        for nombre, entrada, salida in (
            ("A", time(8), time(13, 30)),
            ("B", time(8), time(16, 30)),
            ("C", time(9), time(17, 30)),
        ):
            Turno.objects.create(nombre=nombre, hora_entrada=entrada, hora_salida=salida,
                                 deteccion_por_checada=False)
        aplicar(actor=self.actor)
        self.assertEqual(Turno.objects.count(), 3)
        self.assertEqual(aplicar(actor=self.actor)["aplicadas"], [])

    def test_resuelta_se_reporta_aunque_turno_de_asistencia_ya_sea_correcto(self):
        aplicar(actor=self.actor)
        turno = Turno.objects.get(hora_entrada=time(8), hora_salida=time(16, 30))
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha, turno=turno,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        [resuelta] = HoraExtra.objects.bulk_create([HoraExtra(
            empleado_id=3, asistencia=asistencia, fecha=fecha,
            horas=Decimal("0.50"), estado=HoraExtra.ESTADO_PAGADO,
        )])
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        self.assertEqual([r["id"] for r in plan["extras_resueltas_con_diferencia"]], [resuelta.pk])

    def test_incidencia_resuelta_no_se_reabre_al_crear_propuesta(self):
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        incidencia = IncidenciaAsistencia.objects.create(
            empleado_id=3, fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE,
            estado=IncidenciaAsistencia.ESTADO_RESUELTO,
            detalle="Decisión humana", editado_manual=False,
        )
        aplicar(actor=self.actor)
        incidencia.refresh_from_db()
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_RESUELTO)
        self.assertEqual(incidencia.detalle, "Decisión humana")
        self.assertTrue(HoraExtra.objects.filter(asistencia=asistencia).exists())

    def test_yesenia_17_septiembre_deja_de_tener_incidencia_sin_turno(self):
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(16, 30))),
        )
        alerta = IncidenciaAsistencia.objects.create(
            empleado_id=3, fecha=fecha, asistencia=asistencia,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            detalle="Falta asignar el turno de esta jornada.",
        )
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        self.assertEqual([i["id"] for i in plan["incidencias_a_reconciliar"]], [alerta.pk])
        aplicar(actor=self.actor)
        alerta.refresh_from_db()
        asistencia.refresh_from_db()
        self.assertEqual(alerta.estado, IncidenciaAsistencia.ESTADO_RESUELTO)
        self.assertEqual(asistencia.turno.hora_entrada, time(8))

    def test_propuesta_nueva_crea_incidencia_pendiente_y_auditoria(self):
        fecha = date(2026, 9, 17)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=3, fecha=fecha,
            entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
            salida=timezone.make_aware(datetime.combine(fecha, time(17, 30))),
        )
        plan = configurar_jornadas_administrativas_2026(hoy=self.hoy)
        self.assertIn("crear", [i["accion"] for i in plan["incidencias_a_reconciliar"]])
        aplicar(actor=self.actor)
        incidencia = IncidenciaAsistencia.objects.get(
            empleado_id=3, fecha=fecha,
            tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE,
        )
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(incidencia.hora_extra.asistencia_id, asistencia.pk)
        self.assertTrue(AuditLog.objects.filter(user=self.actor,
                                                model="rrhh.IncidenciaAsistencia",
                                                object_id=str(incidencia.pk)).exists())

    def test_estados_resueltos_y_fechas_fuera_del_rango_quedan_iguales(self):
        fechas = [date(2026, 8, 31), date(2026, 9, 17), date(2026, 9, 18),
                  date(2026, 9, 19), date(2026, 9, 20), date(2027, 1, 1)]
        estados = [HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO,
                   HoraExtra.ESTADO_RECHAZADO, HoraExtra.ESTADO_CANCELADO]
        extras = []
        for fecha in fechas:
            asistencia = AsistenciaEmpleado.objects.create(
                empleado_id=3, fecha=fecha,
                entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
                salida=timezone.make_aware(datetime.combine(fecha, time(18))),
            )
            estado = estados[len(extras) - 1] if 1 <= len(extras) <= 4 else HoraExtra.ESTADO_PENDIENTE
            extras.append(HoraExtra(empleado_id=3, asistencia=asistencia,
                                    fecha=fecha, horas=Decimal("0.50"), estado=estado,
                                    notas=f"original-{fecha}", monto_calculado=Decimal("21.00")))
        extras = HoraExtra.objects.bulk_create(extras)
        antes = list(HoraExtra.objects.filter(pk__in=[e.pk for e in extras]).order_by("pk")
                     .values("id", "fecha", "estado", "horas", "monto_calculado", "notas", "asistencia_id"))
        aplicar(actor=self.actor)
        despues = list(HoraExtra.objects.filter(pk__in=[e.pk for e in extras]).order_by("pk")
                       .values("id", "fecha", "estado", "horas", "monto_calculado", "notas", "asistencia_id"))
        self.assertEqual(despues, antes)
