"""PDF layout and completeness checks over a materialized contract."""
from datetime import date, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pdfplumber
from django.test import SimpleTestCase

from .exports_reporte_asistencia import exportar_pdf_reporte


def pdf_fixture(employees=2, days=15):
    start = date(2026, 9, 1)
    extra = dict(detectado_minutos=None, autorizado_minutos=60, pendiente_minutos=None,
                 rechazado_minutos=0, solicitado_minutos=30, dias_calculables=0,
                 dias_no_calculables=0, dias_sin_registro=days, dias_no_aplica=0, parcial=False)
    reports = []
    for number in range(employees):
        reports.append(dict(datos=dict(id=number + 1, nombre=f'José Muñoz {number + 1}', codigo=f'COD{number + 1}',
                            departamento='Producción', sucursal='Centro', area='Hornos'),
                            resumen=dict(retardos=2, faltas=0, falta_retardos=0), extra_resumen=extra,
                            filas=[dict(fecha=start + timedelta(days=day), asistencia=None,
                                        incidencias=[], extra=extra, tarde_minutos=None,
                                        retardos_registrados=0, permiso_texto='',
                                        estado_laboral_label='Activo', observaciones=[]) for day in range(days)],
                            permisos=[], faltas_30d=0, avisos_30d=[]))
    return dict(reportes=reports, resumen=dict(empleados=employees, retardos=2 * employees,
                faltas=0, falta_retardos=0, faltas_30d=0, avisos_baja=0, extra=extra,
                permisos=dict(minutos_cg=0, minutos_sg=0, dias_cg=0, dias_sg=0, conflictos=0)),
                fecha_inicio=start, fecha_fin=start + timedelta(days=days - 1),
                ventana_inicio=start + timedelta(days=days - 30),
                consultado_en=datetime(2026, 9, 18, 10, tzinfo=ZoneInfo('America/Mazatlan')))


class ReporteAsistenciaPDFTests(SimpleTestCase):
    def text_and_pages(self, data):
        response = exportar_pdf_reporte(data)
        with pdfplumber.open(BytesIO(response.content)) as pdf:
            return response, [page.extract_text() or '' for page in pdf.pages]

    def test_pdf_headers_unicode_and_daily_rows(self):
        response, pages = self.text_and_pages(pdf_fixture())
        self.assertEqual(response['Content-Type'], 'application/pdf')
        self.assertTrue(response.content.startswith(b'%PDF-'))
        self.assertIn('asistencia-Produccion-20260901-20260915.pdf', response['Content-Disposition'])
        self.assertEqual(response['Cache-Control'], 'private, no-store')
        self.assertEqual(len(pages), 2)
        text = '\n'.join(pages)
        self.assertIn('José Muñoz 1', text)
        self.assertIn('José Muñoz 2', text)
        self.assertIn('Detectado: N/D', text)
        self.assertIn('Solicitado pendiente: 0 h 30 min', text)
        # Exactly one daily row per person; period metadata is a different format.
        self.assertEqual(text.count('08/09/2026'), 2)
        self.assertIn('Cobertura:', text)

    def test_full_29_employee_scope_and_31_days_are_preserved(self):
        data = pdf_fixture(employees=29, days=31)
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        for number in range(1, 30):
            self.assertIn(f'COD{number}', text)
        self.assertEqual(text.count('20/09/2026'), 29)
        self.assertEqual(text.count('30/09/2026'), 29)
        self.assertGreaterEqual(text.count('01/10/2026'), 29)
        # Every continuation repeats the employee context and column headings.
        for page in pages[1:]:
            self.assertIn('Código:', page)
            self.assertIn('Entrada', page)

    def test_long_names_literal_markup_incidents_and_observations_are_complete(self):
        data = pdf_fixture(employees=1)
        report = data['reportes'][0]
        report['datos']['nombre'] = 'Ángeles <Hornos> & Muñoz ' * 10
        row = report['filas'][2]
        row['incidencias'] = [dict(tipo='Retardo', estado='Pendiente', detalle='FOLIO-A'),
                              dict(tipo='Observación', estado='Resuelta', detalle='FOLIO-B')]
        row['observaciones'] = ['NOTA-INICIO ' + ('Contenido completo & <literal> ' * 180) + ' NOTA-FINAL']
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        self.assertIn('Ángeles <Hornos> & Muñoz', text)
        self.assertIn('FOLIO-A', text)
        self.assertIn('FOLIO-B', text)
        self.assertIn('NOTA-INICIO', text)
        self.assertIn('NOTA-FINAL', text)
        self.assertEqual(text.count('03/09/2026'), 1)
        self.assertEqual(text.count('04/09/2026'), 1)

    def test_partial_coverage_and_permission_folio_in_annex(self):
        data = pdf_fixture(employees=1)
        data['reportes'][0]['extra_resumen'] = dict(data['reportes'][0]['extra_resumen'], parcial=True,
                                                  detectado_minutos=11, dias_calculables=1)
        data['reportes'][0]['permisos'] = [dict(folio='PERMISO-2026-17', tipo='Por día', estado='Aprobado',
                                               goce_sueldo=True, dias=1, minutos=None)]
        data['reportes'][0]['avisos_30d'] = ['Aviso 19']
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        self.assertIn('Subtotal parcial', text)
        self.assertIn('PERMISO-2026-17', text)
        self.assertIn('no implica baja efectiva', text)

    def test_meal_minutes_and_actual_employee_status_are_present(self):
        data = pdf_fixture(employees=1)
        report = data['reportes'][0]
        report['empleado'] = SimpleNamespace(activo=False, fecha_baja_reporte=date(2026, 9, 10))
        report['filas'][0]['asistencia'] = SimpleNamespace(
            entrada=None, salida=None, salida_comida=None, regreso_comida=None, minutos_comida=35)
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        self.assertIn('35 min', text)
        self.assertIn('Estado actual: Baja', text)
        self.assertIn('Baja registrada: 10/09/2026', text)

    def test_materialized_contract_reads_no_database_and_preserves_summary(self):
        data = pdf_fixture(employees=1)
        data['resumen'].update(faltas_conciliadas=3, avisos_baja=1, avisos_baja_30d=7)
        data['resumen']['permisos']['duracion_desconocida'] = 2
        data['reportes'][0]['resumen']['faltas_conciliadas'] = 3
        data['reportes'][0]['permisos_resumen'] = {'duracion_desconocida': 2}
        # SimpleTestCase prohibits all SQL by default, including renderer reads.
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        self.assertEqual(text.count('Faltas conciliadas: 3'), 2)
        self.assertIn('baja: 7', text)
        self.assertEqual(text.count('Permisos con duración N/D: 2'), 2)

    def test_daily_reconciliation_status_without_incidence_is_visible(self):
        data = pdf_fixture(employees=2)
        for index, report in enumerate(data['reportes']):
            report['filas'][0]['extra'] = dict(report['filas'][0]['extra'], detectado_minutos=60,
                autorizado_minutos=120, pendiente_minutos=0,
                estado='Autorizado superior a lo detectado; revisar' if index == 0 else 'Autorización parcial')
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        self.assertIn('Autorizado superior a lo detectado; revisar', text)
        self.assertIn('Autorización parcial', text)

    def test_long_incidence_detail_in_annex_keeps_daily_type_and_state(self):
        data = pdf_fixture(employees=1)
        row = data['reportes'][0]['filas'][0]
        row['extra'] = dict(row['extra'], estado='Autorización parcial')
        row['incidencias'] = [dict(tipo='Jornada incompleta', estado='Pendiente',
                                  detalle='DETALLE-INICIO ' + 'evidencia extensa ' * 30 + ' DETALLE-FINAL')]
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        self.assertIn('Jornada incompleta - Pendiente | Detalle en anexo', text)
        self.assertIn('Autorización parcial', text)
        self.assertIn('DETALLE-INICIO', text)
        self.assertIn('DETALLE-FINAL', text)

    def test_person_header_preserves_excluded_activity_summary_warning(self):
        data = pdf_fixture(employees=1)
        warning = 'Evidencia anterior al ingreso y posterior a la baja: excluida de indicadores; revisar <folio> & registro.'
        data['reportes'][0]['observaciones_resumen'] = [warning]
        _, pages = self.text_and_pages(data)
        self.assertIn(warning, '\n'.join(pages))

    def test_explicit_missing_record_warning_avoids_duplicate_unknown_reason(self):
        data = pdf_fixture(employees=1)
        for row in data['reportes'][0]['filas']:
            row['extra'] = dict(row['extra'], estado='No calculable: faltan checadas o intervalo válido')
            row['observaciones'] = ['Sin registro; no se infiere falta ni descanso']
        _, pages = self.text_and_pages(data)
        text = '\n'.join(pages)
        self.assertIn('Sin registro; no se infiere falta ni descanso', text)
        self.assertNotIn('No calculable: faltan checadas', text)

    def test_empty_scope_is_valid_pdf(self):
        _, pages = self.text_and_pages(pdf_fixture(employees=0))
        self.assertEqual(len(pages), 1)
        self.assertIn('No hay empleados', pages[0])
