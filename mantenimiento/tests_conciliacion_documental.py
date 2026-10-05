from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from activos.models import Activo, OrdenMantenimiento
from core.models import Sucursal, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
from mantenimiento.services_conciliacion_documental import leer_conciliacion_documental


class ConciliacionDocumentalTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin = get_user_model().objects.create_superuser('qa-concil-admin', '', 'test')
        cls.reader = get_user_model().objects.create_user('qa-concil-reader')
        cls.own = Sucursal.objects.create(codigo='CD-OWN', nombre='Propia')
        cls.other = Sucursal.objects.create(codigo='CD-OTHER', nombre='Ajena')
        UserProfile.objects.create(user=cls.reader, sucursal=cls.own)
        UserModuleAccess.objects.create(user=cls.reader, module='mantenimiento', access='view')
        cls.asset = Activo.objects.create(nombre='Equipo documental', sucursal=cls.own, ubicacion='Producción')
        cls.order = OrdenMantenimiento.objects.create(activo_ref=cls.asset, estatus='CERRADA',
            fecha_programada=date(2026, 9, 1), fecha_cierre=date(2026, 10, 2), costo_otros=Decimal('100'))
        cls.category = CategoriaFalla.objects.create(nombre='Instalación documental', tipo=CategoriaFalla.TIPO_INSTALACION)
        cls.report = ReporteFalla.objects.create(sucursal=cls.own, categoria=cls.category,
            tipo_objetivo='INSTALACION', area_instalacion='Pared', titulo='Humedad', descripcion='Revisión',
            justificacion_sin_foto='Histórico', reportado_por=cls.admin, estatus='cerrado',
            fecha_cierre=datetime(2026, 10, 3, tzinfo=ZoneInfo('America/Mazatlan')),
            costo_estimado=Decimal('900'), costo_real=Decimal('0'))

    def read(self, user=None, **filters):
        return leer_conciliacion_documental(user or self.admin, anio='2026', mes='10', **filters)

    def test_real_cero_no_usa_estimado_y_paridad_vigente(self):
        result = self.read()
        report = next(r for r in result['filas'] if r['tipo'] == 'falla')
        self.assertEqual(report['importe_fuente'], '0.00')
        self.assertEqual(report['clasificacion'], 'Capturado real')
        self.assertEqual(result['total_vigente'], '100.00')
        self.assertEqual(result['diferencia_reproduccion'], '0.00')
        self.assertEqual(result['total_conciliado'], None)

    def test_null_estimado_y_duplicado_no_descuentan(self):
        self.report.costo_real = None
        self.report.duplicado_de = ReporteFalla.objects.create(sucursal=self.own, categoria=self.category,
            titulo='Principal', descripcion='Otro', reportado_por=self.admin,
            tipo_objetivo='INSTALACION', area_instalacion='Pared', justificacion_sin_foto='Histórico')
        self.report.save()
        result = self.read()
        self.assertEqual(result['total_vigente'], '1000.00')
        self.assertEqual(next(r for r in result['filas'] if r['tipo'] == 'falla' and r['id'] == self.report.pk)['clasificacion'], 'Estimado histórico')
        self.report.costo_estimado = None
        self.report.save()
        result = self.read()
        row = next(r for r in result['filas'] if r['id'] == self.report.pk and r['tipo'] == 'falla')
        self.assertIsNone(row['importe_fuente'])
        self.assertEqual(row['clasificacion'], 'No capturado')
        self.assertEqual(result['total_vigente'], '100.00')

    def test_cero_orden_no_acredita_gratuito_y_sin_fecha_no_suma(self):
        self.order.costo_otros = Decimal('0')
        self.order.save()
        result = self.read()
        self.assertEqual(next(r for r in result['filas'] if r['tipo'] == 'orden')['clasificacion'], 'Cero sin confirmación')
        self.order.costo_otros = Decimal('100')
        self.order.fecha_cierre = None
        self.order.fecha_programada = date(2026, 10, 2)
        self.order.save()
        self.assertEqual(self.read()['total_vigente'], '0.00')

    def test_ambito_y_costos_no_llegan_al_payload(self):
        other = Activo.objects.create(nombre='Ajeno secreto', sucursal=self.other)
        OrdenMantenimiento.objects.create(activo_ref=other, fecha_cierre=date(2026,10,3), estatus='CERRADA', costo_otros=999)
        result = self.read(self.reader)
        self.assertEqual({r['sucursal'] for r in result['filas']}, {'Propia'})
        self.assertFalse(result['puede_ver_costos'])
        self.assertIsNone(result['total_vigente'])
        self.assertTrue(all('importe_fuente' not in r and 'componentes' not in r for r in result['filas']))
        with self.assertRaises(PermissionDenied):
            self.read(self.reader, sucursal=str(self.other.pk))

    def test_revocacion_actual_invalida_actor_con_cache(self):
        self.read(self.reader)
        UserModuleAccess.objects.filter(user=self.reader, module='mantenimiento').update(access='none')
        with self.assertRaises(PermissionDenied):
            self.read(self.reader)

    def test_periodo_anual_diciembre_y_errores_acotados(self):
        self.order.fecha_cierre = date(2026, 12, 31)
        self.order.save()
        annual = leer_conciliacion_documental(self.admin, anio='2026', mes='')
        self.assertEqual(annual['total_vigente'], '100.00')
        december = leer_conciliacion_documental(self.admin, anio='2026', mes='12')
        self.assertEqual(december['total_vigente'], '100.00')
        for filters in ({'anio':'2026','mes':'13'}, {'anio':'2026','mes':'x'}, {'anio':'0','mes':''}, {'anio':'2026','estado':'inventado'}):
            with self.assertRaises(ValidationError):
                leer_conciliacion_documental(self.admin, **filters)

    def test_lectura_no_escribe_fuentes_ni_auditoria(self):
        with CaptureQueriesContext(connection) as queries:
            self.read()
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')) for q in queries))

    def test_html_csv_y_reintento_filtro_invalido(self):
        self.client.force_login(self.admin)
        query = {'anio': '2026', 'mes': '10'}
        html = self.client.get(reverse('mantenimiento:conciliacion_documental'), query)
        self.assertEqual(html.status_code, 200)
        self.assertContains(html, 'Humedad')
        self.assertContains(html, 'Total conciliado: pendiente')
        self.report.titulo = '=HYPERLINK("https://evil.example")'
        self.report.save()
        response = self.client.get(reverse('mantenimiento:conciliacion_documental_csv'), query)
        self.assertEqual(response.status_code, 200)
        self.assertIn("'=HYPERLINK", response.content.decode('utf-8'))
        self.assertIn('no-store', response['Cache-Control'])
        invalid = self.client.get(reverse('mantenimiento:conciliacion_documental'), {'anio': '2026', 'mes':'13'})
        self.assertEqual(invalid.status_code, 400)
        self.assertContains(invalid, 'Consultar', status_code=400)
        self.client.force_login(self.reader)
        response = self.client.get(reverse('mantenimiento:conciliacion_documental_csv'), query)
        self.assertNotIn('Importe fuente', response.content.decode('utf-8'))
        self.assertNotIn('900.00', response.content.decode('utf-8'))

    def test_paginacion_html_no_recorta_exportacion_ni_total(self):
        OrdenMantenimiento.objects.bulk_create([OrdenMantenimiento(folio=f'CD-PAGE-{i}', activo_ref=self.asset,
            estatus='CERRADA', fecha_cierre=date(2026,10,2), costo_otros=Decimal('1')) for i in range(51)])
        self.client.force_login(self.admin)
        query = {'anio': '2026', 'mes': '10'}
        response = self.client.get(reverse('mantenimiento:conciliacion_documental'), query)
        self.assertEqual(response.context['filas_total'], 53)
        self.assertEqual(len(response.context['filas']), 50)
        self.assertEqual(response.context['total_vigente'], '151.00')
        second = self.client.get(reverse('mantenimiento:conciliacion_documental'), {**query, 'page': '2'})
        self.assertEqual(len(second.context['filas']), 3)
        export = self.client.get(reverse('mantenimiento:conciliacion_documental_csv'), query)
        self.assertIn('CD-PAGE-50', export.content.decode('utf-8'))
