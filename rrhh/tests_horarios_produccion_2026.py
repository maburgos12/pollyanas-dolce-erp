import json
from datetime import date, datetime, time
from decimal import Decimal
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from rrhh.models import (
    AsignacionJornadaEmpleado, AsignacionTurnoEmpleado, AsistenciaEmpleado,
    Empleado, HoraExtra, IncidenciaAsistencia, JornadaSemanal, JornadaSemanalDia, Turno,
)
from rrhh.services_extra_conciliacion import diagnosticar_horas_extra
from rrhh.services_turnos import es_jornada_historica_antes_de_asignacion, turno_asignado_para_fecha
from rrhh.views_asistencia import _build_reporte_asistencia


class HorariosProduccion2026Tests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            codigo="255", nombre="Persona de envíos", departamento="PRODUCCION",
            fecha_ingreso=date(2026, 1, 1), activo=True,
        )
        self.fecha = date(2026, 9, 15)
        aware = lambda value: timezone.make_aware(datetime.combine(self.fecha, value))
        self.asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=self.fecha,
            entrada=aware(time(9)), salida=aware(time(19)),
            salida_comida=aware(time(12)), regreso_comida=aware(time(12, 35)),
            minutos_comida=35,
        )
        self.autorizada = HoraExtra.objects.create(
            empleado=self.empleado, asistencia=self.asistencia, fecha=self.fecha,
            horas=Decimal("2.00"), estado=HoraExtra.ESTADO_AUTORIZADO,
        )
        self.incidencias_originales = list(IncidenciaAsistencia.objects.values())
        self.manifest = {
            "fecha_inicio": "2026-01-01", "departamento": "PRODUCCION",
            "motivo": "Horario confirmado en prueba",
            "turnos": {"09-17": {
                "nombre": "Envíos prueba 09:00-17:00", "entrada": "09:00",
                "salida": "17:00", "tolerancia_minutos": 10,
            }},
            "empleados": {"255": "09-17"},
        }

    def ejecutar(self, manifest_path, *, apply=False):
        output = StringIO()
        args = {"manifest": manifest_path, "hasta": "2026-09-15", "stdout": output}
        if apply:
            args["apply"] = True
        call_command("asignar_horarios_produccion_2026", **args)
        return json.loads(output.getvalue())

    def test_preview_aplicacion_e_idempotencia_preservan_extra_e_incidencias(self):
        with TemporaryDirectory() as folder:
            path = Path(folder) / "horarios.json"
            path.write_text(json.dumps(self.manifest), encoding="utf-8")
            preview = self.ejecutar(path)
            self.assertEqual(preview["jornadas_sin_turno"], 1)
            self.assertEqual(preview["extra_detectado_minutos"], 120)
            self.assertEqual(AsignacionTurnoEmpleado.objects.count(), 0)
            self.asistencia.refresh_from_db()
            self.assertIsNone(self.asistencia.turno_id)

            applied = self.ejecutar(path, apply=True)
            self.assertEqual(applied["jornadas_sin_turno"], 1)
            self.asistencia.refresh_from_db()
            self.autorizada.refresh_from_db()
            self.assertEqual(diagnosticar_horas_extra(self.asistencia).minutos, 120)
            reportes, _ = _build_reporte_asistencia(
                self.fecha, self.fecha, str(self.empleado.pk), "",
            )
            extra_reporte = reportes[0]["filas"][0]["extra"]
            self.assertEqual(extra_reporte["detectado_minutos"], 120)
            self.assertEqual(extra_reporte["autorizado_minutos"], 120)
            self.assertEqual(extra_reporte["estado"], "Conciliado")
            self.assertEqual(self.autorizada.estado, HoraExtra.ESTADO_AUTORIZADO)
            self.assertEqual(self.autorizada.horas, Decimal("2.00"))
            self.assertEqual(HoraExtra.objects.count(), 1)
            self.assertEqual(list(IncidenciaAsistencia.objects.values()), self.incidencias_originales)
            self.assertFalse(self.asistencia.turno.deteccion_por_checada)
            self.assertEqual(turno_asignado_para_fecha(self.empleado, self.fecha).pk, self.asistencia.turno_id)

            second = self.ejecutar(path, apply=True)
            self.assertEqual(second["turnos_nuevos"], 0)
            self.assertEqual(second["vigencias_nuevas"], 0)
            self.assertEqual(second["jornadas_sin_turno"], 0)
            self.assertEqual(HoraExtra.objects.count(), 1)
            self.assertEqual(list(IncidenciaAsistencia.objects.values()), self.incidencias_originales)

    def test_horario_por_persona_no_se_infiere_por_checada(self):
        turno = Turno.objects.create(
            nombre="Envíos fijo", hora_entrada=time(9), hora_salida=time(17),
            deteccion_por_checada=False,
        )
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=turno, fecha_inicio=date(2026, 1, 1),
        )
        self.assertEqual(turno_asignado_para_fecha(self.empleado, date(2026, 12, 1)), turno)
        self.assertIsNone(turno_asignado_para_fecha(self.empleado, date(2025, 12, 31)))

    def test_reingesta_historica_con_turno_previo_equivalente_no_recalcula(self):
        turno_anterior = Turno.objects.create(
            nombre="Envíos anterior", hora_entrada=time(9), hora_salida=time(17),
        )
        turno_confirmado = Turno.objects.create(
            nombre="Envíos confirmado", hora_entrada=time(9), hora_salida=time(17),
            deteccion_por_checada=False,
        )
        self.asistencia.turno = turno_anterior
        self.asistencia.save(update_fields=["turno"])
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=turno_confirmado,
            fecha_inicio=date(2026, 1, 1), proteger_reingesta_historica=True,
        )
        self.assertTrue(es_jornada_historica_antes_de_asignacion(self.asistencia))

    def test_hik_reingesta_historica_sin_efectos_y_jornada_actual_con_efectos(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        turno = Turno.objects.create(
            nombre="Envíos de ingestión", hora_entrada=time(9), hora_salida=time(17),
            deteccion_por_checada=False,
        )
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=turno, fecha_inicio=date(2026, 1, 1),
            proteger_reingesta_historica=True,
        )

        def eventos(fecha):
            return [
                {"employee_no": "255", "name": self.empleado.nombre,
                 "attendance_status": estado, "time": f"{fecha}T{hora}:00-07:00",
                 "serial_no": serial}
                for estado, hora, serial in (("checkIn", "09:00", 7001), ("checkOut", "19:00", 7002))
            ]

        with patch("rrhh.services_hikvision.generar_horas_extra_automatico") as extra, \
             patch("rrhh.services_hikvision.evaluar_dia_empleado") as reglas, \
             patch("rrhh.services_hikvision.programar_sincronizacion_bonos_desde_checador") as bonos:
            procesar_eventos_hik(eventos(self.fecha))
            extra.assert_not_called()
            reglas.assert_not_called()
            bonos.assert_not_called()
            historica = AsistenciaEmpleado.objects.get(empleado=self.empleado, fecha=self.fecha)
            self.assertEqual(historica.turno_id, turno.pk)

            procesar_eventos_hik(eventos(timezone.localdate()))
            self.assertTrue(extra.called)
            self.assertTrue(reglas.called)
            self.assertTrue(bonos.called)

    def test_excel_historico_usa_turno_sin_recalcular(self):
        from rrhh.importers import importar_excel_hikconnect

        turno = Turno.objects.create(
            nombre="Envíos Excel", hora_entrada=time(9), hora_salida=time(17),
            deteccion_por_checada=False,
        )
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=turno, fecha_inicio=date(2026, 1, 1),
            proteger_reingesta_historica=True,
        )
        df = pd.DataFrame([{
            "id_empleado": "255", "nombre": self.empleado.nombre,
            "fecha": self.fecha, "hora_entrada": time(9), "hora_salida": time(19),
        }])
        with patch("rrhh.importers.pd.read_excel", return_value=df), \
             patch("rrhh.importers.ImportacionChecador.objects.create"), \
             patch("rrhh.importers.generar_horas_extra_automatico") as extra, \
             patch("rrhh.importers.evaluar_dia_empleado") as reglas, \
             patch("rrhh.importers.programar_sincronizacion_bonos_desde_checador") as bonos:
            result = importar_excel_hikconnect(StringIO(""), None, self.fecha, self.fecha)
            self.assertEqual(result["procesados"], 1)
            extra.assert_not_called()
            reglas.assert_not_called()
            bonos.assert_not_called()
        self.asistencia.refresh_from_db()
        self.assertEqual(self.asistencia.turno_id, turno.pk)

    def test_excel_reimportado_en_descanso_limpia_turno_previo_sin_extra(self):
        from rrhh.importers import importar_excel_hikconnect

        domingo = date(2026, 9, 13)
        turno = Turno.objects.create(
            nombre="Envíos previo Excel", hora_entrada=time(8, 30), hora_salida=time(16, 30),
        )
        jornada = JornadaSemanal.objects.create(nombre="Descanso domingo Excel")
        for dia in range(7):
            JornadaSemanalDia.objects.create(
                jornada=jornada, dia_semana=dia, turno=None if dia == 6 else turno,
            )
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada, fecha_inicio=date(2026, 9, 1),
            motivo="Jornada confirmada",
        )
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=domingo, turno=turno,
        )
        df = pd.DataFrame([{
            "id_empleado": "255", "nombre": self.empleado.nombre,
            "fecha": domingo, "hora_entrada": time(8, 30), "hora_salida": time(19),
        }])
        with patch("rrhh.importers.pd.read_excel", return_value=df), \
             patch("rrhh.importers.ImportacionChecador.objects.create"):
            result = importar_excel_hikconnect(StringIO(""), None, domingo, domingo)
        self.assertEqual(result["procesados"], 1)
        asistencia.refresh_from_db()
        self.assertIsNotNone(asistencia.entrada)
        self.assertIsNotNone(asistencia.salida)
        self.assertIsNone(asistencia.turno_id)
        self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())
