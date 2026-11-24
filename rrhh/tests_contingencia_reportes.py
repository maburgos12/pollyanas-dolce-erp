from datetime import date, datetime, time
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone

from rrhh.models import BeneficiarioContingencia, Empleado, PermisoContingencia, IncidenciaAsistencia, AsistenciaEmpleado
from rrhh.services_reporte_asistencia import build_reporte_departamento
from rrhh.views_asistencia import _build_reporte_asistencia, _build_export_rows


class ReportesContingenciaTests(TestCase):
    def setUp(self):
        self.day = date(2026, 11, 21)
        self.actor = get_user_model().objects.create_superuser('cont-reportes', '', 'test')
        self.employee = Empleado.objects.create(
            codigo='CONT-REPORT', nombre='Empleado sintético', fecha_ingreso=date(2026, 1, 1),
        )
        self.permit = PermisoContingencia.objects.create(
            clave='CONT-REPORT-20261121', fecha_inicio=self.day, fecha_fin=self.day,
            motivo='seguridad', descripcion='Cierre empresarial por seguridad', evidencia=['acta:test'],
            alcance='empresa', tratamiento_salarial='conservar', estado='aprobado', creado_por=self.actor,
        )
        BeneficiarioContingencia.objects.create(
            permiso=self.permit, empleado=self.employee, fecha_inicio=self.day, fecha_fin=self.day,
        )

    def test_reporte_completo_muestra_permiso_sin_checada_ni_inferir_goce(self):
        with CaptureQueriesContext(connection) as queries:
            data = build_reporte_departamento(self.day, self.day, empleado_id=str(self.employee.pk))
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')) for q in queries))
        report = data['reportes'][0]
        row = report['filas'][0]
        self.assertIn('Permiso empresarial', row['permiso_texto'])
        self.assertIn(self.permit.clave, row['permiso_folios'])
        self.assertIsNone(row['asistencia'])
        self.assertEqual(report['resumen']['faltas'], 0)
        self.assertEqual(report['resumen']['permisos'], 1)
        self.assertEqual(report['resumen']['dias_cg'], 0)
        self.assertEqual(report['resumen']['dias_sg'], 0)

    def test_reporte_legacy_conserva_dia_de_permiso_sin_incidencias(self):
        reports, count = _build_reporte_asistencia(self.day, self.day, str(self.employee.pk), '')
        self.assertEqual(len(reports[0]['filas']), 1)
        row = reports[0]['filas'][0]
        self.assertIn('Permiso empresarial', row['permiso_texto'])
        self.assertEqual(row['incidencias'], [])
        self.assertEqual(count, 0)
        self.assertEqual(reports[0]['resumen']['permisos'], 1)
        exported = _build_export_rows(reports)
        self.assertEqual(exported[0][4], 'Permiso empresarial')
        self.assertIn(self.permit.clave, exported[0][8])

    def test_pantallas_y_anexo_no_inventan_tratamiento_sin_goce(self):
        data = build_reporte_departamento(self.day, self.day, empleado_id=str(self.employee.pk))
        report = data['reportes'][0]
        html = render_to_string('rrhh/includes/reporte_asistencia_detalle.html', {'reporte': report})
        self.assertIn('Conservar sin ajuste automático', html)
        self.assertIn(self.permit.clave, html)
        self.client.force_login(self.actor)
        response = self.client.get(reverse('rrhh:rrhh_reporte_asistencia'), {
            'fecha_inicio': self.day.isoformat(), 'fecha_fin': self.day.isoformat(),
            'empleado': self.employee.pk, 'vista': 'incidencias',
        })
        self.assertContains(response, self.permit.clave)

    def test_legacy_incluye_inactivo_con_cobertura_historica(self):
        self.employee.activo = False
        self.employee.save(update_fields=['activo'])
        reports, count = _build_reporte_asistencia(self.day, self.day, '', '')
        self.assertEqual(len(reports), 1)
        self.assertIn(self.permit.clave, reports[0]['filas'][0]['permiso_texto'])
        self.assertEqual(count, 0)

    def test_exportacion_con_incidencia_conserva_folio_empresarial(self):
        IncidenciaAsistencia.objects.create(
            empleado=self.employee, fecha=self.day, tipo='comida_excedida', detalle='Evidencia de comida',
        )
        reports, _ = _build_reporte_asistencia(self.day, self.day, str(self.employee.pk), '')
        rows = _build_export_rows(reports)
        self.assertTrue(any(self.permit.clave in str(cell) for row in rows for cell in row))

    def test_legacy_excluye_sancion_sin_ocultar_evidencia_original(self):
        incidence = IncidenciaAsistencia.objects.create(
            empleado=self.employee, fecha=self.day, tipo='falta', detalle='Evidencia original',
        )
        reports, count = _build_reporte_asistencia(self.day, self.day, str(self.employee.pk), '')
        self.assertEqual(reports[0]['resumen']['faltas'], 0)
        self.assertEqual(count, 1)
        self.assertEqual(reports[0]['filas'][0]['incidencias'][0]['id'], incidence.pk)
        self.assertEqual(reports[0]['filas'][0]['incidencias'][0]['detalle'], 'Evidencia original')

    def test_trabajo_real_no_oculta_falta_derivada_de_entrada_tardia(self):
        AsistenciaEmpleado.objects.create(
            empleado=self.employee, fecha=self.day,
            entrada=timezone.make_aware(datetime.combine(self.day, time(11))),
        )
        IncidenciaAsistencia.objects.create(empleado=self.employee, fecha=self.day, tipo='falta')
        data = build_reporte_departamento(self.day,self.day,empleado_id=str(self.employee.pk))
        self.assertEqual(data['reportes'][0]['resumen']['faltas'], 1)
        self.assertEqual(data['reportes'][0]['faltas_30d'], 1)
        reports, _ = _build_reporte_asistencia(self.day,self.day,str(self.employee.pk),'')
        self.assertEqual(reports[0]['resumen']['faltas'], 1)

    def test_revocado_no_muestra_permiso_y_fuera_de_cobertura_no_aplica(self):
        self.permit.estado = 'revocado'
        self.permit.save(update_fields=['estado'])
        data = build_reporte_departamento(self.day, self.day, empleado_id=str(self.employee.pk))
        self.assertEqual(data['reportes'][0]['filas'][0]['permiso_texto'], '')
        self.assertEqual(data['reportes'][0]['resumen']['permisos'], 0)
