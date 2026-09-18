from datetime import date, datetime, time
from io import BytesIO
from unittest.mock import patch

import pdfplumber
from django.contrib.auth.models import User
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from core.models import UserModuleAccess
from rrhh.models import Empleado, AsistenciaEmpleado, HoraExtra, IncidenciaAsistencia, PermisoSalida


def local(day, hour, minute=0):
    return timezone.make_aware(datetime.combine(day, time(hour, minute)))


class ReportesDepartamentoFlowTests(TestCase):
    def setUp(self):
        self.viewer = User.objects.create_user('qa-view', password='test')
        UserModuleAccess.objects.create(user=self.viewer, module='rrhh', access='view')
        self.denied = User.objects.create_user('qa-denied', password='test')
        self.people = [Empleado.objects.create(codigo=f'QA-{n:02d}',nombre=f'Persona Producción {n:02d}',
            departamento=Empleado.DEP_PRODUCCION,area='Hornos',sucursal='Matriz',fecha_ingreso=date(2026,1,1)) for n in range(12)]
        self.other = Empleado.objects.create(codigo='QA-VENTAS',nombre='Persona Ventas',departamento=Empleado.DEP_VENTAS,fecha_ingreso=date(2026,1,1))
        self.day = date(2026,9,15)
        self.attendance = AsistenciaEmpleado.objects.create(empleado=self.people[0],fecha=self.day,
            entrada=local(self.day,8),salida=local(self.day,18),salida_comida=local(self.day,12),
            regreso_comida=local(self.day,12,35),minutos_comida=35)
        HoraExtra.objects.create(empleado=self.people[0],fecha=self.day,horas=2,estado='autorizado')
        HoraExtra.objects.create(empleado=self.people[1],fecha=self.day,horas=2,estado='pendiente')
        self.params={'fecha_inicio':'2026-09-01','fecha_fin':'2026-09-15','departamento':Empleado.DEP_PRODUCCION}
        self.url=reverse('rrhh:rrhh_reporte_asistencia')
        self.client.force_login(self.viewer)

    def test_consulta_page2_complete_print_and_pdf_same_scope(self):
        first=self.client.get(self.url,self.params)
        second=self.client.get(self.url,{**self.params,'page':2})
        self.assertEqual(first.status_code,200)
        self.assertEqual(second.status_code,200)
        self.assertEqual(first.context['resumen_global']['empleados'],12)
        self.assertEqual(second.context['resumen_global']['extra']['autorizado_minutos'],120)
        ids=[r['datos']['id'] for response in [first,second] for r in response.context['reportes']]
        self.assertEqual(set(ids),{p.pk for p in self.people})
        self.assertEqual(len(ids),12)
        printed=self.client.get(self.url,{**self.params,'page':2,'export':'imprimir'})
        self.assertEqual(len(printed.context['reportes']),12)
        self.assertEqual(printed.content.decode().count('data-empleado-id='),12)
        self.assertEqual(printed.content.decode().count('data-fecha='),12*15)
        pdf=self.client.get(self.url,{**self.params,'page':2,'export':'pdf'})
        self.assertEqual(pdf.status_code,200)
        self.assertEqual(pdf['Content-Type'],'application/pdf')
        self.assertTrue(pdf.content.startswith(b'%PDF'))
        self.assertIn('private',pdf['Cache-Control'])
        self.assertIn('no-store',pdf['Cache-Control'])
        with pdfplumber.open(BytesIO(pdf.content)) as doc:
            content='\n'.join(page.extract_text() or '' for page in doc.pages)
        for person in self.people:self.assertIn(person.nombre,content)
        self.assertNotIn(self.other.nombre,content)
        self.assertEqual(content.count('08/09/2026'),12) # an interior date appears once per employee
        self.assertEqual(first.context['resumen_global'],printed.context['resumen_global'])

    def test_requested_unknown_difference_is_visible_per_person_day(self):
        response=self.client.get(self.url,{**self.params,'empleado':self.people[1].pk})
        self.assertContains(response,'Solicitado pendiente')
        self.assertContains(response,'120 min')
        self.assertEqual(response.context['reportes'][0]['extra_resumen']['solicitado_minutos'],120)
        self.assertIsNone(response.context['reportes'][0]['extra_resumen']['pendiente_minutos'])

    def test_roll30_alert_before_quincena_present(self):
        IncidenciaAsistencia.objects.create(empleado=self.people[0],fecha=date(2026,8,25),tipo='aviso_baja_faltas',estado='pendiente',detalle='Aviso de agosto')
        IncidenciaAsistencia.objects.create(empleado=self.people[0],fecha=date(2026,8,24),tipo='falta',estado='pendiente')
        response=self.client.get(self.url,{**self.params,'empleado':self.people[0].pk})
        self.assertContains(response,'Aviso de agosto')
        self.assertEqual(response.context['reportes'][0]['faltas_30d'],1)
        self.assertEqual(response.context['resumen_global']['avisos_baja_30d'],1)

    def test_quincena_controls_preserve_period(self):
        first=self.client.get(self.url,{**self.params,'fecha_inicio':'2024-02-01','fecha_fin':'2024-02-15'})
        self.assertEqual(first.context['mes_periodo'],'2024-02')
        self.assertEqual(first.context['quincena_seleccionada'],'1')
        second=self.client.get(self.url,{**self.params,'fecha_inicio':'2024-02-16','fecha_fin':'2024-02-29'})
        self.assertEqual(second.context['quincena_seleccionada'],'2')

    def test_invalid_and_incompatible_filters_never_broaden(self):
        for filters in [{'departamento':'NO-EXISTE'},{'area':'NO-EXISTE'},{'empleado':'abc'},
                        {'fecha_inicio':'2026-02-31'},{'fecha_fin':'2026-08-31'},
                        {'fecha_fin':'2026-10-02'},{'export':'inventado'},
                        {'fecha_inicio':'9999-12-31','fecha_fin':'9999-12-31'}]:
            with self.subTest(filters=filters):
                response=self.client.get(self.url,{**self.params,**filters})
                self.assertEqual(response.status_code,400)
                self.assertEqual(response.context['reportes'],[])
        empty=self.client.get(self.url,{**self.params,'empleado':self.other.pk})
        self.assertEqual(empty.status_code,200)
        self.assertEqual(empty.context['resumen_global']['empleados'],0)

    def test_legacy_department_export_cannot_silently_ignore_scope(self):
        for format in ['csv','xlsx']:
            response=self.client.get(self.url,{**self.params,'export':format})
            self.assertEqual(response.status_code,400)
        legacy=self.client.get(self.url,{'fecha_inicio':self.params['fecha_inicio'],'fecha_fin':self.params['fecha_fin'],'export':'csv'})
        self.assertEqual(legacy.status_code,200)
        self.assertTrue(legacy['Content-Type'].startswith('text/csv'))

    def test_view_only_all_formats_denied_edit_and_no_access_direct_pdf(self):
        for format in ['', 'pdf','imprimir']:
            allowed=self.client.get(self.url,{**self.params,'export':format})
            self.assertEqual(allowed.status_code,200)
            self.assertIn('no-store',allowed['Cache-Control'])
        incidence=IncidenciaAsistencia.objects.create(empleado=self.people[0],fecha=self.day,tipo='falta')
        edit=self.client.post(reverse('rrhh:rrhh_incidencia_editar',args=[incidence.pk]),
            {'estado':'resuelto','minutos':0,'comentario':'no permitido'})
        self.assertEqual(edit.status_code,403)
        self.client.force_login(self.denied)
        for format in ['', 'pdf','imprimir','csv','xlsx']:
            self.assertEqual(self.client.get(self.url,{**self.params,'export':format}).status_code,403)
        self.client.logout()
        self.assertEqual(self.client.get(self.url,{**self.params,'export':'pdf'}).status_code,302)

    def test_repeat_read_all_formats_never_write_operational_rows(self):
        PermisoSalida.objects.create(empleado=self.people[0],tipo='permiso_hora',fecha_inicio=local(self.day,14),fecha_fin=local(self.day,16),estado='aprobado',motivo='Permiso QA')
        models=[AsistenciaEmpleado,HoraExtra,IncidenciaAsistencia,PermisoSalida]
        before=[list(model.objects.order_by('pk').values()) for model in models]
        with CaptureQueriesContext(connection) as captured:
            for format in ['', 'pdf','imprimir']:
                self.assertEqual(self.client.get(self.url,{**self.params,'export':format}).status_code,200)
        self.assertEqual(before,[list(model.objects.order_by('pk').values()) for model in models])
        mutations=[q['sql'] for q in captured if q['sql'].lstrip().upper().startswith(('INSERT','UPDATE','DELETE'))]
        self.assertEqual(mutations,[])

    def test_filter_error_preserves_person_and_area_in_form(self):
        response=self.client.get(self.url,{**self.params,'empleado':self.people[0].pk,'area':'Hornos','fecha_fin':'2026-02-31'})
        self.assertEqual(response.status_code,400)
        self.assertContains(response,'Hornos (filtro conservado)',status_code=400)
        self.assertContains(response,f'Empleado {self.people[0].pk} (filtro conservado)',status_code=400)

    def test_pdf_failure_returns_html_error_keeps_filters(self):
        with patch('rrhh.exports_reporte_asistencia.exportar_pdf_reporte',side_effect=RuntimeError('QA PDF failure')):
            response=self.client.get(self.url,{**self.params,'export':'pdf'})
        self.assertEqual(response.status_code,503)
        self.assertTrue(response['Content-Type'].startswith('text/html'))
        self.assertContains(response,'Conservamos los filtros',status_code=503)
        self.assertEqual(response.context['departamento'],Empleado.DEP_PRODUCCION)
        self.assertNotContains(response,'No hay empleados que coincidan',status_code=503)

    def test_legacy_filter_form_preserves_management_mode(self):
        response=self.client.get(self.url,{'vista':'incidencias','fecha_inicio':'2026-08-01','fecha_fin':'2026-09-15'})
        self.assertEqual(response.status_code,200)
        self.assertContains(response,'name="vista" value="incidencias"')

    def test_empty_archived_employee_or_area_preserves_filter(self):
        archived=Empleado.objects.create(codigo='QA-ARCHIVE',nombre='Persona archivada',activo=False,area='Área archivada',fecha_ingreso=date(2026,1,1))
        for filters, expected in [({'empleado':archived.pk},f'Empleado {archived.pk} (filtro conservado)'),({'area':'Área archivada'},'Área archivada (filtro conservado)')]:
            response=self.client.get(self.url,{**self.params,**filters})
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.context['resumen_global']['empleados'],0)
            self.assertContains(response,expected)
