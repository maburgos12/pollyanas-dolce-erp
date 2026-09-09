from datetime import date
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from core.models import AuditLog
from maestros.models import Proveedor
from reportes.models import AreaPresupuesto
from compras.models import CotizacionCompraDepartamental, ItemCompraDepartamental, SolicitudCompraDepartamental


class ProveedorCotizacionTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser('compra-test', 'test@example.com', 'test')
        self.client.force_login(self.user)
        self.area = AreaPresupuesto.objects.create(nombre='Prueba', codigo='prueba')
        self.solicitud = SolicitudCompraDepartamental.objects.create(area=self.area, solicitante=self.user, periodo=date(2026,9,1), estado='ENVIADA')
        self.item = ItemCompraDepartamental.objects.create(solicitud=self.solicitud, descripcion='Cargador', cantidad=1)
        self.proveedor = Proveedor.objects.create(nombre='Tienda existente')
        self.headers = {'HTTP_ACCEPT': 'application/json'}

    def alta(self, nombre='Tienda nueva', **kw):
        return self.client.post(reverse('compras:departamental_proveedor_crear', args=[self.item.pk]), {'nombre':nombre, 'lead_time_dias':'3', **kw}, **self.headers)

    def cotizar(self, **kw):
        data = {'proveedor':self.proveedor.pk,'cantidad_ofertada':'1','costo_unitario':'250', 'plataforma':'AMAZON','enlace_producto':'https://www.amazon.com.mx/dp/ejemplo', **kw}
        return self.client.post(reverse('compras:departamental_cotizar', args=[self.item.pk]), data, **self.headers)

    def test_alta_selecciona_sin_redirigir_y_audita(self):
        response = self.alta('  Tienda   nueva  ')
        self.assertEqual(response.status_code, 201)
        proveedor = Proveedor.objects.get(nombre='Tienda nueva')
        self.assertNotIn('redirect', response.json())
        self.assertIn(f'value="{proveedor.pk}" selected', response.json()['html'])
        self.assertEqual(response.json()['target'], f'#proveedor-item-{self.item.pk}')
        self.assertTrue(AuditLog.objects.filter(user=self.user, model='maestros.Proveedor', object_id=str(proveedor.pk)).exists())

    def test_duplicado_normalizado_e_inactivo_no_se_crean(self):
        Proveedor.objects.create(nombre='Café López', activo=False)
        response=self.alta(' CAFE   lopez ')
        self.assertEqual(response.status_code,409)
        self.assertIn('inactivo', response.json()['toast']['message'])
        self.assertEqual(Proveedor.objects.count(),2)

    def test_alta_rechaza_plazo_negativo(self):
        self.assertEqual(self.alta(lead_time_dias='-1').status_code,400)
        self.assertEqual(Proveedor.objects.count(),1)

    def test_sin_permiso_no_puede_dar_alta(self):
        self.client.force_login(get_user_model().objects.create_user('sin-permiso'))
        self.assertEqual(self.alta().status_code,403)
        self.assertEqual(Proveedor.objects.count(),1)

    def test_cotizacion_online_conserva_evidencia_sin_seleccionar(self):
        response=self.cotizar()
        self.assertEqual(response.status_code,200)
        quote=CotizacionCompraDepartamental.objects.get()
        self.assertEqual(quote.plataforma,'AMAZON')
        self.assertEqual(quote.enlace_producto,'https://www.amazon.com.mx/dp/ejemplo')
        self.assertFalse(quote.seleccionada)
        self.assertFalse(hasattr(self.item,'compromiso'))
        self.assertIn(f'#item-{self.item.pk}',response.json()['redirect'])

    def test_url_insegura_sin_enlace_y_cantidad_invalida_no_guardan(self):
        for values in ({'enlace_producto':'javascript:alert(1)'},{'enlace_producto':''},{'cantidad_ofertada':'0'},{'costo_unitario':'-1'},{'plataforma':'NO_EXISTE'}):
            with self.subTest(values=values):
                self.assertEqual(self.cotizar(**values).status_code,400)
        self.assertFalse(CotizacionCompraDepartamental.objects.exists())

    def test_proveedor_inactivo_no_admitido(self):
        self.proveedor.activo=False
        self.proveedor.save()
        self.assertEqual(self.cotizar().status_code,400)

    def test_compra_directa_no_requiere_enlace(self):
        self.assertEqual(self.cotizar(plataforma='',enlace_producto='').status_code,200)
        self.assertEqual(CotizacionCompraDepartamental.objects.get().plataforma,'')

    def test_error_tradicional_conserva_captura(self):
        response=self.client.post(reverse('compras:departamental_cotizar',args=[self.item.pk]),{'proveedor':self.proveedor.pk,'costo_unitario':'321','cantidad_ofertada':'0','garantia_observaciones':'Conservar esto'})
        self.assertEqual(response.status_code,400)
        self.assertContains(response,'Conservar esto',status_code=400)
        self.assertContains(response,'value="321"',status_code=400)

    def test_comparar_no_cambia_cotizacion_seleccionada(self):
        self.cotizar(seleccionar='1')
        original = CotizacionCompraDepartamental.objects.get()
        self.item.refresh_from_db()
        estado = self.item.estado
        self.cotizar(costo_unitario='275')
        self.item.refresh_from_db()
        original.refresh_from_db()
        self.assertEqual(self.item.estado,estado)
        self.assertTrue(original.seleccionada)

    def test_puede_seleccionar_una_cotizacion_guardada(self):
        self.cotizar()
        quote = CotizacionCompraDepartamental.objects.get()
        response = self.client.post(reverse('compras:departamental_cotizacion_seleccionar',args=[quote.pk]),{},**self.headers)
        self.assertEqual(response.status_code,200)
        quote.refresh_from_db()
        self.assertTrue(quote.seleccionada)

    def test_seleccion_repetida_no_duplica_eventos(self):
        self.cotizar()
        quote=CotizacionCompraDepartamental.objects.get()
        url=reverse('compras:departamental_cotizacion_seleccionar',args=[quote.pk])
        self.client.post(url,{},**self.headers)
        before=self.item.eventos.count()
        self.client.post(url,{},**self.headers)
        self.assertEqual(self.item.eventos.count(),before)

    def test_no_cotizar_ni_reseleccionar_item_ordenado(self):
        self.cotizar()
        quote=CotizacionCompraDepartamental.objects.get()
        self.item.estado='ORDENADO'
        self.item.save()
        self.assertEqual(self.cotizar().status_code,409)
        response=self.client.post(reverse('compras:departamental_cotizacion_seleccionar',args=[quote.pk]),{},**self.headers)
        self.assertEqual(response.status_code,409)
        self.assertEqual(CotizacionCompraDepartamental.objects.count(),1)

    def test_alta_tradicional_regresa_al_articulo(self):
        response=self.client.post(reverse('compras:departamental_proveedor_crear',args=[self.item.pk]),{'nombre':'Alta tradicional','lead_time_dias':'2'})
        self.assertEqual(response.status_code,302)
        self.assertIn(f'#cotizar-{self.item.pk}',response.url)
        rendered=self.client.get(response.url)
        self.assertContains(rendered,'Alta tradicional')

    def test_duplicado_activo_no_se_crea(self):
        response=self.alta('TIENDA EXISTENTE')
        self.assertEqual(response.status_code,409)
        self.assertEqual(Proveedor.objects.count(),1)
