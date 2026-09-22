from datetime import date, datetime, time
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone
from rrhh.models import Empleado, AsistenciaEmpleado, IncidenciaAsistencia, PermisoSalida, Turno
from rrhh.services_reporte_asistencia import build_reporte_departamento


def dt(day, hour=0):
    return timezone.make_aware(datetime.combine(day, time(hour)))


class ReporteDepartamentoServiceTests(TestCase):
    def setUp(self):
        self.start = date(2026, 9, 1)
        self.end = date(2026, 9, 15)
        self.employee = Empleado.objects.create(codigo='DEP-1', nombre='Persona', departamento='PRODUCCION', area='Hornos', sucursal='Matriz', fecha_ingreso=date(2026, 1, 1))

    def build(self, **kwargs):
        return build_reporte_departamento(self.start, self.end, **kwargs)

    def permiso(self, start, end=None, **kwargs):
        return PermisoSalida.objects.create(empleado=self.employee, fecha_inicio=start, fecha_fin=end, estado='aprobado', tipo=kwargs.pop('tipo', 'permiso_hora'), motivo='Prueba', **kwargs)

    def test_active_without_activity_full_days_unknown_extra(self):
        report = self.build(departamento='PRODUCCION')['reportes'][0]
        self.assertEqual(len(report['filas']), 15)
        self.assertEqual(report['resumen']['faltas'], 0)
        self.assertIsNone(report['extra_resumen']['detectado_minutos'])
        self.assertEqual(report['extra_resumen']['dias_sin_registro'], 15)
        self.assertIsNone(report['filas'][0]['tarde_minutos'])

    def test_filters_invalid_never_expand(self):
        for filters in ({'departamento': 'INVALID'}, {'area': 'INVALID'}, {'empleado_id': 'abc'}, {'sucursal': 'INVALID'}):
            with self.subTest(filters=filters), self.assertRaises(ValidationError):
                self.build(**filters)
        with self.assertRaises(ValidationError):
            build_reporte_departamento(self.start, date(2026, 10, 2))

    def test_day_without_end_not_indefinite_and_inactive_permission_visible(self):
        self.employee.activo = False
        self.employee.save(update_fields=['activo'])
        self.permiso(dt(date(2026, 8, 1)), tipo='permiso_dia')
        self.permiso(dt(self.start), tipo='permiso_dia')
        report = self.build()['reportes'][0]
        self.assertEqual(len(report['permisos']), 1)
        self.assertEqual(report['resumen']['dias_cg'], 1)
        self.assertEqual(report['filas'][1]['permiso_texto'], '')

    def test_hour_union_and_goce_conflict(self):
        self.permiso(dt(self.start, 8), dt(self.start, 10))
        self.permiso(dt(self.start, 9), dt(self.start, 11))
        self.permiso(dt(self.start, 10), dt(self.start, 12), goce_sueldo=False)
        summary = self.build()['reportes'][0]['resumen']
        self.assertEqual(summary['minutos_cg'], 180)
        self.assertEqual(summary['minutos_sg'], 120)
        self.assertEqual(summary['conflictos'], 1)

    def test_30day_only_pending_falta_and_no_preentry(self):
        for day, kind, state in [(date(2026,8,20),'falta','pendiente'), (self.start,'falta','conciliado'), (self.start,'falta_retardos','pendiente'), (date(2026,8,1),'falta','pendiente')]:
            IncidenciaAsistencia.objects.create(empleado=self.employee, fecha=day,tipo=kind,estado=state)
        report = self.build()['reportes'][0]
        self.assertEqual(report['faltas_30d'], 1)
        self.assertEqual(report['resumen']['faltas_conciliadas'],1)
        self.assertEqual(report['resumen']['falta_retardos'],1)

    def test_partial_detection_and_no_writes(self):
        turno = Turno.objects.create(nombre='Jornada 8 a 16', hora_entrada=time(8), hora_salida=time(16))
        AsistenciaEmpleado.objects.create(empleado=self.employee,fecha=self.start,turno=turno,entrada=dt(self.start,8),salida=dt(self.start,17))
        before = (AsistenciaEmpleado.objects.count(), IncidenciaAsistencia.objects.count())
        report = self.build()['reportes'][0]
        self.assertEqual(report['extra_resumen']['detectado_minutos'],60)
        self.assertTrue(report['extra_resumen']['parcial'])
        self.assertEqual(before,(AsistenciaEmpleado.objects.count(),IncidenciaAsistencia.objects.count()))

    def test_preentry_incidence_retains_evidence_without_sanction(self):
        self.employee.fecha_ingreso=date(2026,9,5)
        self.employee.save(update_fields=['fecha_ingreso'])
        IncidenciaAsistencia.objects.create(empleado=self.employee,fecha=self.start,tipo='falta')
        report=self.build()['reportes'][0]
        self.assertEqual(report['resumen']['faltas'],0)
        self.assertEqual(report['faltas_30d'],0)
        self.assertEqual(report['filas'][0]['estado_laboral_label'],'No aplica')
        self.assertEqual(len(report['filas'][0]['incidencias']),1)
        self.assertTrue(report['filas'][0]['observaciones'])

    def test_postbaja_and_active_rehire(self):
        from rrhh.models import EmpleadoBaja
        self.employee.activo=False
        self.employee.save(update_fields=['activo'])
        EmpleadoBaja.objects.create(empleado=self.employee,nombre=self.employee.nombre,fecha_ingreso=self.employee.fecha_ingreso,fecha_baja=self.start)
        AsistenciaEmpleado.objects.create(empleado=self.employee,fecha=date(2026,9,2),entrada=dt(date(2026,9,2),8),salida=dt(date(2026,9,2),16))
        report=self.build()['reportes'][0]
        self.assertTrue(report['filas'][1]['observaciones'])
        self.assertEqual(report['filas'][2]['estado_laboral_label'],'No aplica')
        self.employee.activo=True
        self.employee.save(update_fields=['activo'])
        report=self.build()['reportes'][0]
        self.assertEqual(report['filas'][2]['estado_laboral_label'],'Activo')
        self.assertFalse(report['filas'][1]['observaciones'])

    def test_hour_missing_end_one_day_unknown_and_cross_midnight_clipped(self):
        self.permiso(dt(self.start,8))
        self.permiso(dt(date(2026,8,31),23),dt(self.start,1))
        report=self.build()['reportes'][0]
        self.assertEqual(report['resumen']['minutos_cg'],60)
        self.assertEqual(report['resumen']['duracion_desconocida'],1)
        self.assertEqual(report['filas'][1]['permiso_texto'],'')

    def test_query_count_does_not_grow_with_employees(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        turno = Turno.objects.create(nombre='Jornada 8 a 16', hora_entrada=time(8), hora_salida=time(16))
        AsistenciaEmpleado.objects.create(
            empleado=self.employee, fecha=self.start, turno=turno,
            entrada=dt(self.start, 8), salida=dt(self.start, 17),
        )
        with CaptureQueriesContext(connection) as one:
            first_report = self.build()
        employees = Empleado.objects.bulk_create([Empleado(codigo=f'Q-{n}',nombre=f'Persona {n}',fecha_ingreso=self.start,departamento='PRODUCCION') for n in range(20)])
        AsistenciaEmpleado.objects.bulk_create([
            AsistenciaEmpleado(empleado=employee, fecha=self.start, turno=turno,
                entrada=dt(self.start, 8), salida=dt(self.start, 17))
            for employee in employees
        ])
        with CaptureQueriesContext(connection) as many:
            larger_report = self.build()
        self.assertEqual(len(first_report['reportes']), 1)
        self.assertEqual(len(larger_report['reportes']), 21)
        self.assertTrue(all(report['extra_resumen']['detectado_minutos'] == 60
            for report in larger_report['reportes']))
        self.assertEqual(len(one),len(many))
        self.assertLessEqual(len(many),8)

    def test_authorized_extra_without_attendance_and_multiple_incidences(self):
        from decimal import Decimal
        from rrhh.models import HoraExtra
        HoraExtra.objects.create(empleado=self.employee,fecha=self.start,horas=Decimal('1.5'),estado='autorizado')
        for kind in ('retardo','jornada_incompleta'):
            IncidenciaAsistencia.objects.create(empleado=self.employee,fecha=self.start,tipo=kind)
        report=self.build()['reportes'][0]
        self.assertEqual(len(report['filas']),15)
        self.assertEqual(len(report['filas'][0]['incidencias']),2)
        self.assertEqual(report['extra_resumen']['autorizado_minutos'],90)
        self.assertIsNone(report['extra_resumen']['detectado_minutos'])
        self.assertEqual(report['resumen']['retardos'],1)

    def test_explicit_turno_lateness_and_extra_helper(self):
        from rrhh.models import Turno
        turno=Turno.objects.create(nombre='Mañana',hora_entrada=time(8),hora_salida=time(16))
        AsistenciaEmpleado.objects.create(empleado=self.employee,fecha=self.start,turno=turno,entrada=timezone.make_aware(datetime.combine(self.start,time(8,12))),salida=dt(self.start,17))
        report=self.build()['reportes'][0]
        self.assertEqual(report['filas'][0]['tarde_minutos'],12)
        self.assertEqual(report['filas'][0]['retardos_registrados'],0)
        self.assertEqual(report['extra_resumen']['detectado_minutos'],60)

    def test_id_outside_database_range_rejected_before_query(self):
        for invalid in ('9'*100, '0', '²', '１２', str(2**63), 0):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                self.build(empleado_id=invalid)

    def test_daily_permission_duration_and_midnight_end(self):
        self.permiso(dt(self.start,8),dt(self.start,10),goce_sueldo=False)
        self.permiso(dt(self.start),dt(date(2026,9,2)),tipo='permiso_dia')
        report=self.build()['reportes'][0]
        self.assertIn('2 h 00 min SG',report['filas'][0]['permiso_texto'])
        self.assertIn('1 día CG',report['filas'][0]['permiso_texto'])
        self.assertEqual(report['filas'][1]['permiso_texto'],'')
        self.assertEqual(report['resumen']['dias_cg'],1)

    def test_postbaja_raw_incidences_not_disciplinary_totals(self):
        from rrhh.models import EmpleadoBaja
        self.employee.activo=False
        self.employee.save(update_fields=['activo'])
        EmpleadoBaja.objects.create(empleado=self.employee,nombre=self.employee.nombre,fecha_ingreso=self.employee.fecha_ingreso,fecha_baja=self.start)
        for kind in ('retardo','falta','aviso_baja_faltas'):
            IncidenciaAsistencia.objects.create(empleado=self.employee,fecha=date(2026,9,2),tipo=kind)
        report=self.build()['reportes'][0]
        self.assertEqual(report['resumen']['retardos'],0)
        self.assertEqual(report['resumen']['faltas'],0)
        self.assertEqual(report['faltas_30d'],0)
        self.assertEqual(report['resumen']['avisos_baja_30d'],0)
        self.assertEqual(report['filas'][1]['retardos_registrados'],0)
        self.assertEqual(len(report['filas'][1]['incidencias']),3)

    def test_preentry_authorization_evidence_without_extra_total(self):
        from decimal import Decimal
        from rrhh.models import HoraExtra
        self.employee.fecha_ingreso=date(2026,9,5)
        self.employee.save(update_fields=['fecha_ingreso'])
        HoraExtra.objects.create(empleado=self.employee,fecha=self.start,horas=Decimal('1'),estado='autorizado')
        report=self.build()['reportes'][0]
        self.assertEqual(report['filas'][0]['extra']['autorizado_minutos'],60)
        self.assertEqual(len(report['filas'][0]['extra']['registros']),1)
        self.assertEqual(report['extra_resumen']['autorizado_minutos'],0)

    def test_missing_checada_observations_and_30day_alert_total(self):
        IncidenciaAsistencia.objects.create(empleado=self.employee,fecha=date(2026,8,20),tipo='aviso_baja_faltas')
        self.permiso(dt(self.start,8))
        result=self.build()
        report=result['reportes'][0]
        self.assertIn('Sin checada',report['filas'][0]['observaciones'])
        self.assertIn('Sin registro; no se infiere falta ni descanso',report['filas'][1]['observaciones'])
        self.assertEqual(report['resumen']['avisos_baja_30d'],1)
        self.assertEqual(result['resumen']['avisos_baja_30d'],1)
        self.assertEqual(result['resumen']['avisos_baja'],0)

    def test_postbaja_extra_and_permission_evidence_without_time_kpis(self):
        from decimal import Decimal
        from rrhh.models import EmpleadoBaja, HoraExtra
        self.employee.activo=False
        self.employee.save(update_fields=['activo'])
        EmpleadoBaja.objects.create(empleado=self.employee,nombre=self.employee.nombre,fecha_ingreso=self.employee.fecha_ingreso,fecha_baja=self.start)
        day=date(2026,9,2)
        AsistenciaEmpleado.objects.create(empleado=self.employee,fecha=day,entrada=dt(day,8),salida=dt(day,17))
        HoraExtra.objects.create(empleado=self.employee,fecha=day,horas=Decimal('1'),estado='autorizado')
        self.permiso(dt(day,8),dt(day,10))
        self.permiso(dt(day),tipo='permiso_dia',goce_sueldo=False)
        report=self.build()['reportes'][0]
        self.assertFalse(report['filas'][1]['aplicable_laboral'])
        self.assertEqual(report['filas'][1]['extra']['autorizado_minutos'],60)
        self.assertIn('2 h 00 min CG',report['filas'][1]['permiso_texto'])
        self.assertEqual(report['extra_resumen']['autorizado_minutos'],0)
        self.assertEqual(report['resumen']['minutos_cg'],0)
        self.assertEqual(report['resumen']['dias_sg'],0)
        self.assertEqual(report['resumen']['permisos'],2)
        self.assertEqual(report['resumen']['permisos_aplicables'],0)
        self.assertTrue(report['observaciones_resumen'])
        self.assertTrue(report['permisos'][0]['observaciones'])
