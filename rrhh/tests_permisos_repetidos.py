from datetime import date, datetime, time

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from rest_framework.test import APIClient

from .models import (
    AsignacionJornadaEmpleado, AsistenciaEmpleado, Empleado, IncidenciaAsistencia,
    JornadaSemanal, JornadaSemanalDia, PermisoSalida, Turno,
)
from .services_asistencia_reglas import evaluar_dia_empleado
from .services_reporte_asistencia import build_reporte_departamento


class PermisosRepetidosTests(TestCase):
    def test_un_folio_por_dias_laborables_y_solo_minutos_cubiertos(self):
        actor = get_user_model().objects.create_superuser("jefe-rango")
        jefe = Empleado.objects.create(nombre="Jefe", usuario_erp=actor)
        empleado = Empleado.objects.create(
            nombre="Colaboradora", jefe_directo=jefe, fecha_ingreso=date(2026, 1, 1),
        )
        turno = Turno.objects.create(nombre="Diurno", hora_entrada=time(8), hora_salida=time(16))
        jornada = JornadaSemanal.objects.create(nombre="Semana con miércoles de descanso")
        for dia in range(7):
            JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=dia, turno=turno if dia in (0, 1, 3, 4, 5) else None)
        AsignacionJornadaEmpleado.objects.create(empleado=empleado, jornada=jornada, fecha_inicio=date(2026, 1, 1))
        inicio, fin = date(2026, 10, 26), date(2026, 10, 31)
        fechas = ["2026-10-26", "2026-10-27", "2026-10-29", "2026-10-30", "2026-10-31"]
        api = APIClient()
        api.force_authenticate(actor)
        ruta = "/rrhh/api/permisos/"
        datos = {
            "empleado": empleado.pk, "tipo": "permiso_hora", "modalidad_dias": "laborables",
            "fecha_inicio": f"{inicio}T08:00", "fecha_fin": f"{fin}T09:00", "motivo": "Permiso recurrente",
        }
        preview = api.post(ruta, {**datos, "vista_previa": True}, format="json")
        self.assertEqual(preview.status_code, 200, preview.data)
        self.assertEqual(preview.data["fechas_laborables"], fechas)
        self.assertFalse(PermisoSalida.objects.exists())
        sin_confirmar = api.post(ruta, datos, format="json")
        self.assertEqual(sin_confirmar.status_code, 400)
        self.assertFalse(PermisoSalida.objects.exists())
        creado = api.post(ruta, {**datos, "confirmacion": preview.data["confirmacion"]}, format="json")
        self.assertEqual(creado.status_code, 201, creado.data)
        permiso = PermisoSalida.objects.get()
        with self.captureOnCommitCallbacks(execute=True):
            aprobado = api.post(f"{ruta}{permiso.pk}/autorizar-jefe/")
        self.assertEqual(aprobado.status_code, 200, aprobado.data)
        AsistenciaEmpleado.objects.create(
            empleado=empleado, fecha=inicio, turno=turno,
            entrada=timezone.make_aware(datetime.combine(inicio, time(9, 20))),
            salida=timezone.make_aware(datetime.combine(inicio, time(16))),
        )
        evaluar_dia_empleado(empleado, inicio)
        falta = IncidenciaAsistencia.objects.get(empleado=empleado, fecha=inicio, tipo=IncidenciaAsistencia.TIPO_FALTA)
        self.assertEqual((falta.estado, falta.minutos), (IncidenciaAsistencia.ESTADO_PENDIENTE, 20))
        self.assertEqual(falta.metadata["minutos_cubiertos_permiso"], 60)
        self.assertEqual(falta.permiso_id, permiso.pk)
        dia_sin_marcas = date(2026, 10, 27)
        evaluar_dia_empleado(empleado, dia_sin_marcas)
        falta_completa = IncidenciaAsistencia.objects.get(
            empleado=empleado, fecha=dia_sin_marcas, tipo=IncidenciaAsistencia.TIPO_FALTA,
        )
        self.assertEqual((falta_completa.estado, falta_completa.minutos), (IncidenciaAsistencia.ESTADO_PENDIENTE, 420))
        self.assertEqual(falta_completa.metadata["minutos_cubiertos_permiso"], 60)
        reporte = build_reporte_departamento(inicio, fin, empleado_id=empleado.pk)["reportes"][0]
        self.assertEqual([(p["folio"], p["minutos"]) for p in reporte["permisos"]], [(permiso.folio, 300)])
        self.assertEqual([fila["fecha"].isoformat() for fila in reporte["filas"] if fila["permiso_folios"]], fechas)
