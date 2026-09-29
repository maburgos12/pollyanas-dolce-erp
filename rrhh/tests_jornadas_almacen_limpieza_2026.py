"""Contrato de la jornada 08:00-16:00 para almacén y limpieza en 2026."""

from datetime import date, datetime, time
from decimal import Decimal
from io import StringIO
import json

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone

from core.models import AuditLog
from rrhh.models import (
    AsignacionJornadaEmpleado,
    AsistenciaEmpleado,
    Empleado,
    HoraExtra,
    JornadaSemanal,
    JornadaSemanalDia,
    Turno,
)
from rrhh.services_jornadas_almacen_limpieza_2026 import (
    ConfiguracionJornadasError,
    configurar_jornadas_almacen_limpieza_2026,
)


PERSONAS = (
    (43, "GALVEZ GALVEZ JOSE ANTONIO"),
    (23, "LARA VILLANUEVA ERNESTO"),
    (15, "GARCIA HIGUERA BEATRIZ"),
    (16, "GARCIA HIGUERA CLARISELA"),
)


def marca(fecha, hora):
    return timezone.make_aware(datetime.combine(fecha, hora))


class JornadasAlmacenLimpieza2026Tests(TestCase):
    def setUp(self):
        for pk, nombre in PERSONAS:
            Empleado.objects.create(pk=pk, codigo=f"ALM-LIM-{pk}", nombre=nombre)
        self.actor = get_user_model().objects.create_user(
            username="rrhh_almacen_limpieza", password="test", is_superuser=True,
        )
        self.fecha = date(2026, 9, 17)

    def _aplicar(self):
        preview = configurar_jornadas_almacen_limpieza_2026(hoy=self.fecha)
        return configurar_jornadas_almacen_limpieza_2026(
            aplicar=True,
            hoy=self.fecha,
            actor=self.actor,
            expected_fingerprint=preview["fingerprint"],
        )

    def test_preview_es_puro_y_describe_cuatro_personas(self):
        plan = configurar_jornadas_almacen_limpieza_2026(hoy=self.fecha)

        self.assertEqual(plan["modo"], "preview")
        self.assertEqual(plan["personas_objetivo"], 4)
        self.assertEqual([p["id"] for p in plan["personas"]], [15, 16, 23, 43])
        self.assertEqual(plan["conflictos"], [])
        self.assertEqual(len(plan["turnos"]), 1)
        self.assertEqual(len(plan["jornadas"]), 1)
        self.assertEqual(len(plan["asignaciones"]), 4)
        self.assertFalse(Turno.objects.exists())
        self.assertFalse(JornadaSemanal.objects.exists())
        self.assertFalse(AsignacionJornadaEmpleado.objects.exists())
        self.assertFalse(AuditLog.objects.exists())

    def test_apply_asigna_semana_de_48_horas_y_es_idempotente(self):
        Turno.objects.create(
            nombre="Producción normal 08:00-16:00",
            hora_entrada=time(8), hora_salida=time(16), deteccion_por_checada=False,
        )
        primero = self._aplicar()

        self.assertEqual(primero["modo"], "apply")
        self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 4)
        jornada = JornadaSemanal.objects.get(nombre="Almacén y limpieza 2026")
        self.assertTrue(Turno.objects.filter(nombre="Almacén y limpieza 2026 08:00-16:00").exists())
        dias = list(JornadaSemanalDia.objects.filter(jornada=jornada)
                    .select_related("turno").order_by("dia_semana"))
        self.assertEqual(len(dias), 7)
        self.assertTrue(all(d.turno and d.turno.hora_entrada == time(8)
                            and d.turno.hora_salida == time(16) for d in dias[:6]))
        self.assertIsNone(dias[6].turno)
        self.assertEqual(sum(
            (d.turno.hora_salida.hour - d.turno.hora_entrada.hour) for d in dias if d.turno
        ), 48)

        segundo = self._aplicar()
        self.assertEqual(segundo["aplicadas"], [])
        self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 4)

    def test_regulariza_solo_extras_automaticas_pendientes(self):
        fecha_sin_propuesta = date(2026, 9, 15)
        sin_propuesta = AsistenciaEmpleado.objects.create(
            empleado_id=23, fecha=fecha_sin_propuesta,
            entrada=marca(fecha_sin_propuesta, time(8)),
            salida=marca(fecha_sin_propuesta, time(17)),
        )
        corta = AsistenciaEmpleado.objects.create(
            empleado_id=43, fecha=self.fecha,
            entrada=marca(self.fecha, time(8)), salida=marca(self.fecha, time(16, 20)),
        )
        corta_extra = HoraExtra.objects.create(
            empleado_id=43, asistencia=corta, fecha=self.fecha,
            horas=Decimal("0.33"), estado=HoraExtra.ESTADO_PENDIENTE,
            notas="[Detección automática] Sin turno asignado.",
        )
        larga = AsistenciaEmpleado.objects.create(
            empleado_id=15, fecha=self.fecha,
            entrada=marca(self.fecha, time(8)), salida=marca(self.fecha, time(17)),
        )
        larga_extra = HoraExtra.objects.create(
            empleado_id=15, asistencia=larga, fecha=self.fecha,
            horas=Decimal("0.95"), estado=HoraExtra.ESTADO_PENDIENTE,
            notas="[Detección automática] Sin turno asignado.",
        )
        fecha_resuelta = date(2026, 9, 16)
        resuelta_asistencia = AsistenciaEmpleado.objects.create(
            empleado_id=16, fecha=fecha_resuelta,
            entrada=marca(fecha_resuelta, time(8)), salida=marca(fecha_resuelta, time(17)),
        )
        resuelta = HoraExtra.objects.create(
            empleado_id=16, asistencia=resuelta_asistencia, fecha=fecha_resuelta,
            horas=Decimal("0.75"), estado=HoraExtra.ESTADO_AUTORIZADO,
            notas="Acuerdo autorizado",
        )

        self._aplicar()

        corta_extra.refresh_from_db()
        larga_extra.refresh_from_db()
        resuelta.refresh_from_db()
        self.assertEqual(corta_extra.estado, HoraExtra.ESTADO_CANCELADO)
        self.assertEqual(larga_extra.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(larga_extra.horas, Decimal("1.00"))
        self.assertEqual(resuelta.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(resuelta.horas, Decimal("0.75"))
        self.assertFalse(HoraExtra.objects.filter(asistencia=sin_propuesta).exists())
        self.assertEqual(AsistenciaEmpleado.objects.filter(turno__hora_entrada=time(8)).count(), 4)

    def test_conflicto_de_identidad_o_traslape_impide_aplicar(self):
        empleado = Empleado.objects.get(pk=23)
        empleado.nombre = "PERSONA DISTINTA"
        empleado.save()
        plan = configurar_jornadas_almacen_limpieza_2026(hoy=self.fecha)
        self.assertTrue(plan["conflictos"])
        with self.assertRaisesMessage(ConfiguracionJornadasError, "conflictos"):
            configurar_jornadas_almacen_limpieza_2026(
                aplicar=True,
                hoy=self.fecha,
                actor=self.actor,
                expected_fingerprint=plan["fingerprint"],
            )
        self.assertFalse(AsignacionJornadaEmpleado.objects.exists())

    def test_comando_preview_y_apply_exige_actor_y_huella(self):
        salida = StringIO()
        call_command("configurar_jornadas_almacen_limpieza_2026", stdout=salida)
        preview = json.loads(salida.getvalue())
        self.assertEqual(preview["personas_objetivo"], 4)
        self.assertEqual(preview["modo"], "preview")

        with self.assertRaisesMessage(CommandError, "--apply exige"):
            call_command("configurar_jornadas_almacen_limpieza_2026", "--apply")
