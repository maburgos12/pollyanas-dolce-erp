"""Consulta de trabajos sin cambiar el motor de captura ni certificación financiera."""
from decimal import Decimal
from html import unescape
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlencode, urlsplit

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.template.loader import render_to_string
from django.test import RequestFactory, TestCase, override_settings
from django.urls import reverse

from core.models import UserModuleAccess
from .models import Activo, EvidenciaOrden, OrdenMantenimiento
from .services_pasaporte import construir_pasaporte
from .views import _orden_enterprise_profile


class ConsultaTrabajoTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser('consulta_admin', password='test')
        self.lector = get_user_model().objects.create_user('consulta_lector')
        UserModuleAccess.objects.create(user=self.lector, module='inventario', access='view')
        self.sin_acceso = get_user_model().objects.create_user('consulta_sin_acceso')
        self.activo = Activo.objects.create(nombre='Equipo consulta')
        self.client.force_login(self.admin)

    def orden(self, **kwargs):
        return OrdenMantenimiento.objects.create(activo_ref=self.activo, tipo='CORRECTIVO', **kwargs)

    def test_todas_las_ordenes_ofrecen_consulta_sin_reabrir(self):
        self.client.force_login(self.lector)
        for estado in ('PENDIENTE', 'EN_PROCESO', 'CERRADA', 'CANCELADA'):
            orden = self.orden(estatus=estado)
            filtros = {'estatus': estado, 'enterprise_gap': 'SIN_RESPONSABLE'}
            page = self.client.get(reverse('activos:ordenes'), filtros)
            enlace = reverse('activos:orden_evidencias', args=[orden.pk]) + '?' + urlencode({
                'origen': 'ordenes', 'return_query': urlencode(filtros),
            })
            self.assertIn(enlace, unescape(page.content.decode()))
            detalle = self.client.get(enlace)
            self.assertEqual(detalle.status_code, 200)
            self.assertNotContains(detalle, 'name="action" value="update_factura"')
            self.assertIn(reverse('activos:ordenes') + '?' + urlencode(filtros) + f'#orden-{orden.pk}', unescape(detalle.content.decode()))
            self.assertEqual(orden.evidencias.count(), 0)
            self.assertFalse(orden.bitacora.exists())
            orden.refresh_from_db()
            self.assertEqual(orden.estatus, estado)
        self.client.force_login(self.sin_acceso)
        self.assertEqual(self.client.get(enlace).status_code, 403)

    def test_reportes_conservan_busqueda_y_contexto_sin_tabs_locales(self):
        orden = self.orden()
        filtros = {'estatus': 'ABIERTAS', 'q': orden.folio, 'semaforo': 'VERDE'}
        page = self.client.get(reverse('activos:reportes'), filtros)
        self.assertContains(page, f'id="orden-{orden.pk}"')
        detalle = self.client.get(reverse('activos:orden_evidencias', args=[orden.pk]), {
            'origen': 'reportes', 'return_query': urlencode(filtros),
        })
        retorno = urlsplit(detalle.context['consulta_volver_url'])
        self.assertEqual(retorno.path, reverse('activos:reportes'))
        self.assertEqual(parse_qs(retorno.query), {key: [value] for key, value in filtros.items()})
        self.assertEqual(retorno.fragment, f'orden-{orden.pk}')
        self.assertLessEqual(detalle.content.decode().count('class="module-tabs'), 1)
        self.assertContains(detalle, 'Reportes de servicio')
        self.assertContains(detalle, 'name="origen" value="reportes"')

    def test_origen_y_query_no_permiten_destinos_arbitrarios_ni_html(self):
        orden = self.orden()
        query = urlencode({'q': '"><script>alert(1)</script>&x=1', 'next': 'https://evil.test', 'export': 'csv'})
        for origen in ('https://evil.test', '//evil.test', 'reportes'):
            response = self.client.get(reverse('activos:orden_evidencias', args=[orden.pk]), {'origen': origen, 'return_query': query})
            destino = urlsplit(response.context['consulta_volver_url'])
            self.assertFalse(destino.netloc)
            self.assertEqual(set(parse_qs(destino.query)), {'q'} if origen == 'reportes' else set())
            self.assertNotContains(response, '<script>alert(1)</script>')
            self.assertNotContains(response, 'evil.test')

    def test_posts_existentes_conservan_contexto_y_datos(self):
        orden = self.orden(estatus='CERRADA', costo_otros=Decimal('17.25'))
        detalle_url = reverse('activos:orden_evidencias', args=[orden.pk])
        filtros = urlencode({'estatus': 'CERRADA', 'semaforo': 'VERDE', 'q': 'texto & "'})
        datos = {'origen': 'reportes', 'return_query': filtros + '&next=https://evil.test&export=csv'}
        esperado = detalle_url + '?' + urlencode({'origen': 'reportes', 'return_query': filtros})
        # Error de archivo faltante y actualización de nota usan el mismo contexto.
        self.assertEqual(self.client.post(detalle_url, datos).url, esperado)
        response = self.client.post(detalle_url, {**datos, 'action': 'update_factura', 'numero_factura': 'F-123', 'nota_trabajo': 'Atendido'})
        self.assertEqual(response.url, esperado)
        with TemporaryDirectory() as media, override_settings(MEDIA_ROOT=media):
            response = self.client.post(detalle_url, {**datos, 'archivo': SimpleUploadedFile('soporte.pdf', b'soporte', content_type='application/pdf'), 'tipo': 'DOCUMENTO'})
            self.assertEqual(response.url, esperado)
            evidencia = orden.evidencias.get()
            response = self.client.post(reverse('activos:eliminar_evidencia', args=[evidencia.pk]), datos)
            self.assertEqual(response.url, esperado)
            self.assertFalse(EvidenciaOrden.objects.filter(pk=evidencia.pk).exists())
        orden.refresh_from_db()
        self.assertEqual((orden.estatus, orden.costo_total, orden.numero_factura, orden.nota_trabajo), ('CERRADA', Decimal('17.25'), 'F-123', 'Atendido'))
        self.assertEqual(list(orden.bitacora.values_list('accion', flat=True)).count('FACTURA'), 1)
        self.assertEqual(list(orden.bitacora.values_list('accion', flat=True)).count('EVIDENCIA'), 2)

    def test_perfil_conserva_gaps_y_tonos_sin_certificacion_financiera(self):
        completa = self.orden(estatus='CERRADA', responsable='Técnico', costo_mano_obra=Decimal('25'))
        perfil = _orden_enterprise_profile(completa)
        self.assertEqual(perfil['gaps'], [])
        self.assertEqual(perfil['status_tone'], 'success')
        self.assertEqual(perfil['status_label'], 'Datos técnicos completos')
        self.assertEqual(perfil['next_action'], 'Sin pendientes técnicos detectados')
        cero = self.orden(estatus='CERRADA', responsable='Técnico')
        perfil = _orden_enterprise_profile(cero)
        self.assertEqual([g['key'] for g in perfil['gaps']], ['SIN_COSTO_CIERRE'])
        self.assertEqual(perfil['status_tone'], 'danger')
        self.assertIn('Confirmar', perfil['next_action'])
        page = self.client.get(reverse('activos:ordenes'), {'estatus': 'CERRADA'})
        card = next(c for c in page.context['enterprise_cards'] if c['key'] == 'SIN_COSTO_CIERRE')
        self.assertEqual(card['count'], 1)
        csv = self.client.get(reverse('activos:ordenes'), {'estatus': 'CERRADA', 'export': 'csv'})
        self.assertIn(completa.folio, csv.content.decode())
        self.assertNotIn('Datos técnicos completos', csv.content.decode())


class CostosPasaporteClaridadTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser('costos_admin', password='test')
        self.sin_costos = get_user_model().objects.create_user('costos_operacion')
        self.activo = Activo.objects.create(nombre='Equipo costos')

    def html(self, user=None):
        return render_to_string('operacion/activo_pasaporte.html', construir_pasaporte(self.activo, user or self.admin), request=RequestFactory().get('/'))

    def test_adquisicion_distingue_nulo_cero_y_positivo(self):
        for costo, esperado in ((None, 'Sin capturar'), (Decimal('0'), '0.00 MXN'), (Decimal('125.50'), '125.50 MXN')):
            self.activo.costo_adquisicion = costo
            self.activo.save(update_fields=['costo_adquisicion'])
            self.assertIn('<span>Adquisición</span><b>' + esperado + '</b>', self.html())

    def test_ausencia_y_cero_con_orden_son_distintos(self):
        self.assertIn('No hay costos de órdenes registrados.', self.html())
        OrdenMantenimiento.objects.create(activo_ref=self.activo)
        html = self.html()
        self.assertNotIn('No hay costos de órdenes registrados.', html)
        self.assertIn('Confirma importes y soporte.', html)

    def test_total_incluye_componentes_pendientes_y_cancelados_conserva_permisos(self):
        OrdenMantenimiento.objects.create(activo_ref=self.activo, estatus='PENDIENTE', costo_repuestos=Decimal('11.10'), costo_mano_obra=Decimal('22.20'))
        OrdenMantenimiento.objects.create(activo_ref=self.activo, estatus='CANCELADA', costo_otros=Decimal('33.30'))
        self.assertEqual(construir_pasaporte(self.activo, self.admin)['costos']['mantenimiento_total'], Decimal('66.60'))
        html = self.html()
        self.assertIn('<span>Costos registrados en órdenes</span><b>66.60 MXN</b>', html)
        self.assertIn('incluidas pendientes y canceladas', html)
        self.assertIn('No acredita gasto pagado ni conciliado', html)
        restringido = self.html(self.sin_costos)
        self.assertNotIn('66.60', restringido)
        self.assertNotIn('Costos registrados en órdenes', restringido)
