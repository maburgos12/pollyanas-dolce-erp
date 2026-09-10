from datetime import date, timedelta
from decimal import Decimal
from tempfile import TemporaryDirectory

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from compras.models import (CotizacionCompraDepartamental, ItemCompraDepartamental,
                            SolicitudCompraDepartamental, RecepcionItemDepartamental,
                            CompraRealizadaDepartamental, HistorialCotizacionDepartamental)
from compras.services_departamentales import seleccionar_cotizacion, generar_ordenes_departamentales
from maestros.models import Proveedor
from reportes.models import AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual


class EdicionCompraTests(TestCase):
    def setUp(self):
        self.media = TemporaryDirectory()
        self.addCleanup(self.media.cleanup)
        self.override = override_settings(MEDIA_ROOT=self.media.name)
        self.override.enable()
        self.addCleanup(self.override.disable)
        self.user = get_user_model().objects.create_superuser('compra-revision', password='test')
        self.client.force_login(self.user)
        self.area = AreaPresupuesto.objects.create(nombre='Pruebas', codigo='prueba')
        self.rubro = RubroPresupuesto.objects.create(area=self.area, concepto='Equipo')
        LineaPresupuestoMensual.objects.create(rubro=self.rubro, periodo=date(2026,9,1),
                                               monto_presupuesto=10000, monto_real=0)
        self.solicitud = SolicitudCompraDepartamental.objects.create(area=self.area, solicitante=self.user,
                                                                    periodo=date(2026,9,1), estado='ENVIADA')
        self.item = ItemCompraDepartamental.objects.create(solicitud=self.solicitud, descripcion='Bancos', cantidad=2)
        self.proveedor = Proveedor.objects.create(nombre='Proveedor')
        self.quote = CotizacionCompraDepartamental.objects.create(item=self.item, proveedor=self.proveedor,
                                                                  cantidad_ofertada=2, costo_unitario=100)
        seleccionar_cotizacion(self.quote, actor=self.user)
        self.headers = {'HTTP_ACCEPT':'application/json'}

    def editar(self, **changes):
        data = {'proveedor':self.proveedor.pk, 'cantidad_ofertada':'2', 'costo_unitario':'100',
                'descuento':'0', 'impuestos':'0', 'envio':'0', 'instalacion':'0', 'otros_cargos':'0',
                'plataforma':'', 'enlace_producto':'', 'garantia_observaciones':'Corregida',
                'motivo':'Corrección solicitada', 'version':'1', **changes}
        return self.client.post(reverse('compras:departamental_cotizacion_editar',args=[self.quote.pk]),data,**self.headers)

    def comprar(self, **changes):
        self.quote.refresh_from_db()
        data = {'cotizacion_id':self.quote.pk, 'version':self.quote.version,
                'fecha_compra':timezone.localdate().isoformat(), 'importe_final':'200.00',
                'numero_pedido':'PEDIDO-PRUEBA',
                'comprobante':SimpleUploadedFile('prueba.pdf',b'%PDF-1.4\n%%EOF',content_type='application/pdf'), **changes}
        return self.client.post(reverse('compras:departamental_compra_registrar',args=[self.item.pk]),data,**self.headers)

    def ordenar(self):
        generar_ordenes_departamentales([self.item],actor=self.user)
        self.item.refresh_from_db()

    def test_edicion_de_texto_audita_sin_perder_autorizacion(self):
        response=self.editar()
        self.assertEqual(response.status_code,200,response.content)
        self.quote.refresh_from_db(); self.item.refresh_from_db()
        self.assertEqual(self.quote.version,2)
        self.assertEqual(self.quote.garantia_observaciones,'Corregida')
        self.assertEqual(self.item.estado,'AUTORIZADO')
        history=HistorialCotizacionDepartamental.objects.get(cotizacion=self.quote)
        self.assertEqual(history.actor,self.user)
        self.assertEqual(history.motivo,'Corrección solicitada')
        self.assertNotEqual(history.antes,history.despues)

    def test_incremento_requiere_dg_aunque_haya_presupuesto_global(self):
        self.assertEqual(self.editar(costo_unitario='150').status_code,200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ESPERANDO_DG')
        self.assertFalse(self.item.compromiso.activo)
        self.assertGreaterEqual(self.comprar(importe_final='300').status_code,400)
        self.assertFalse(CompraRealizadaDepartamental.objects.exists())

    def test_edicion_concurrente_no_pisa_version_reciente(self):
        self.assertEqual(self.editar().status_code,200)
        response=self.editar(costo_unitario='999')
        self.assertEqual(response.status_code,409,response.content)
        self.quote.refresh_from_db()
        self.assertEqual(self.quote.costo_unitario,Decimal('100'))
        self.assertEqual(HistorialCotizacionDepartamental.objects.count(),1)

    def test_edicion_requiere_motivo_y_no_admite_importes_negativos(self):
        for data in ({'motivo':''},{'costo_unitario':'-1'},{'cantidad_ofertada':'0'}):
            with self.subTest(data=data):
                self.assertGreaterEqual(self.editar(**data).status_code,400)
        self.assertFalse(HistorialCotizacionDepartamental.objects.exists())

    def test_orden_existente_se_corrige_sin_duplicar(self):
        self.ordenar()
        line_pk=self.item.linea_orden.pk
        self.assertEqual(self.editar(costo_unitario='90').status_code,200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ORDENADO')
        self.assertEqual(self.item.linea_orden.pk,line_pk)
        self.assertEqual(self.item.linea_orden.total,Decimal('180'))
        self.assertEqual(self.comprar(importe_final='180').status_code,200)

    def test_reautorizacion_actualiza_orden_existente(self):
        self.ordenar()
        line_pk=self.item.linea_orden.pk
        self.assertEqual(self.editar(costo_unitario='150').status_code,200)
        response=self.client.post(reverse('compras:departamental_decidir',args=[self.item.pk]),
                                  {'decision':'AUTORIZAR','cotizacion_id':self.quote.pk,
                                   'version':CotizacionCompraDepartamental.objects.get(pk=self.quote.pk).version},**self.headers)
        self.assertEqual(response.status_code,200,response.content)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ORDENADO')
        self.assertEqual(self.item.linea_orden.pk,line_pk)
        self.assertEqual(self.item.linea_orden.total,Decimal('300'))
        self.assertEqual(self.comprar(importe_final='300').status_code,200)

    def test_orden_no_permite_cambiar_proveedor_o_cantidad(self):
        self.ordenar()
        otro=Proveedor.objects.create(nombre='Otro')
        self.assertGreaterEqual(self.editar(proveedor=otro.pk).status_code,400)
        self.assertGreaterEqual(self.editar(cantidad_ofertada='3').status_code,400)

    def test_compra_crea_orden_sin_entrega_ni_gasto_contable(self):
        response=self.comprar()
        self.assertEqual(response.status_code,200,response.content)
        self.item.refresh_from_db()
        compra=CompraRealizadaDepartamental.objects.get(item=self.item)
        self.assertEqual(compra.importe_final,Decimal('200'))
        self.assertEqual(compra.registrado_por,self.user)
        self.assertEqual(self.item.estado,'COMPRADO')
        self.assertEqual(self.item.monto_gastado,0)
        self.assertTrue(hasattr(self.item,'linea_orden'))
        self.assertFalse(RecepcionItemDepartamental.objects.exists())
        self.assertEqual(self.item.compromiso.monto,Decimal('200'))

    def test_compra_duplicada_no_crea_segundo_registro(self):
        self.assertEqual(self.comprar().status_code,200)
        self.assertGreaterEqual(self.comprar().status_code,400)
        self.assertEqual(CompraRealizadaDepartamental.objects.count(),1)

    def test_compra_rechaza_monto_mayor_fecha_futura_y_falta_comprobante(self):
        for data in ({'importe_final':'201'},{'importe_final':'0'},
                     {'fecha_compra':(timezone.localdate()+timedelta(days=1)).isoformat()},
                     {'comprobante':''}):
            with self.subTest(data=data):
                self.assertGreaterEqual(self.comprar(**data).status_code,400)
        self.assertFalse(CompraRealizadaDepartamental.objects.exists())
        self.assertFalse(hasattr(ItemCompraDepartamental.objects.get(pk=self.item.pk),'linea_orden'))

    def test_despues_de_comprar_no_admite_editar_seleccionar_o_cotizar(self):
        self.comprar()
        self.assertGreaterEqual(self.editar().status_code,400)
        response=self.client.post(reverse('compras:departamental_cotizacion_seleccionar',args=[self.quote.pk]),{},**self.headers)
        self.assertEqual(response.status_code,409)
        response=self.client.post(reverse('compras:departamental_cotizar',args=[self.item.pk]),
                                  {'proveedor':self.proveedor.pk,'cantidad_ofertada':2,'costo_unitario':20},**self.headers)
        self.assertEqual(response.status_code,409)

    def test_recepcion_historica_bloquea_editar(self):
        self.ordenar()
        RecepcionItemDepartamental.objects.create(linea_orden=self.item.linea_orden,cantidad_recibida=1,registrado_por=self.user)
        self.assertGreaterEqual(self.editar().status_code,400)

    def test_entrega_posterior_a_compra_se_registra_por_separado(self):
        self.comprar()
        response=self.client.post(reverse('compras:departamental_recibir',args=[self.item.pk]),
                                  {'cantidad_recibida':'2'},**self.headers)
        self.assertEqual(response.status_code,200,response.content)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'PENDIENTE_CONFIRMACION')
        self.assertEqual(CompraRealizadaDepartamental.objects.count(),1)

    def test_sin_permiso_no_edita_ni_compra(self):
        self.client.force_login(get_user_model().objects.create_user('ajeno'))
        self.assertEqual(self.editar().status_code,403)
        self.assertEqual(self.comprar().status_code,403)
        self.assertFalse(HistorialCotizacionDepartamental.objects.exists())
        self.assertFalse(CompraRealizadaDepartamental.objects.exists())

    def test_comprobante_descarga_restringida(self):
        self.comprar()
        compra=CompraRealizadaDepartamental.objects.get()
        url=reverse('compras:departamental_compra_comprobante',args=[compra.pk])
        response=self.client.get(url)
        self.assertEqual(response.status_code,200)
        self.assertTrue(b"".join(response.streaming_content).startswith(b"%PDF-"))
        self.client.force_login(get_user_model().objects.create_user('ajeno'))
        self.assertEqual(self.client.get(url).status_code,403)

    def test_formulario_precargado_y_error_tradicional_conservan_valores(self):
        url=reverse('compras:departamental_cotizacion_editar',args=[self.quote.pk])
        response=self.client.get(url)
        self.assertContains(response,'Editar cotización')
        response=self.client.post(url,{'proveedor':self.proveedor.pk,'cantidad_ofertada':'2','costo_unitario':'-5',
                                      'version':'1','motivo':'Conservar motivo'})
        self.assertEqual(response.status_code,400)
        self.assertContains(response,'Conservar motivo',status_code=400)

    def test_detalle_muestra_compra_y_entrega_separadas(self):
        self.comprar()
        response=self.client.get(reverse('compras:departamental_detalle',args=[self.solicitud.pk]))
        self.assertContains(response,'Registrar entrega')
        self.assertContains(response,'PEDIDO-PRUEBA')
        self.assertNotContains(response,'Registrar compra realizada')

    def test_editar_alternativa_no_altera_seleccion_o_compromiso(self):
        seleccionada=self.quote
        self.quote=CotizacionCompraDepartamental.objects.create(item=self.item,proveedor=self.proveedor,
                                                                cantidad_ofertada=2,costo_unitario=80)
        self.assertEqual(self.editar(costo_unitario='900').status_code,200)
        self.item.refresh_from_db(); seleccionada.refresh_from_db()
        self.assertTrue(seleccionada.seleccionada)
        self.assertEqual(self.item.estado,'AUTORIZADO')
        self.assertEqual(self.item.compromiso.monto,Decimal('200'))

    def test_cotizacion_aumentada_no_se_aprueba_con_cambio_de_texto(self):
        self.assertEqual(self.editar(costo_unitario='150').status_code,200)
        self.assertEqual(self.editar(costo_unitario='150',version='2',motivo='Solo observaciones').status_code,200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ESPERANDO_DG')

    def test_no_reautorizar_una_compra_ya_realizada(self):
        self.comprar()
        response=self.client.post(reverse('compras:departamental_decidir',args=[self.item.pk]),
                                  {'decision':'AUTORIZAR','cotizacion_id':self.quote.pk,
                                   'version':CotizacionCompraDepartamental.objects.get(pk=self.quote.pk).version},**self.headers)
        self.assertGreaterEqual(response.status_code,400)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'COMPRADO')

    def test_compra_sigue_pendiente_en_resumen_y_exportacion(self):
        from compras.resumen_departamentales import construir_resumen_departamental, exportar_resumen_departamental
        from openpyxl import load_workbook
        from io import BytesIO
        self.comprar(importe_final='180')
        context=construir_resumen_departamental({'estado':'COMPRADO'})
        self.assertFalse(context['filtros'].errors)
        self.assertEqual(context['resumen']['articulos'],1)
        self.assertEqual(context['resumen']['comprometido'],Decimal('180'))
        book=load_workbook(BytesIO(exportar_resumen_departamental(context).content))
        rows=list(book['Artículos'].values)
        self.assertTrue(any('Comprado, pendiente de entrega' in row for row in rows))

    def test_compra_con_version_vieja_no_registra(self):
        self.assertEqual(self.editar().status_code,200)
        self.assertEqual(self.comprar(version='1').status_code,409)
        self.assertFalse(CompraRealizadaDepartamental.objects.exists())

    def test_rebaja_despues_de_incremento_sigue_esperando_direccion(self):
        self.editar(costo_unitario='150')
        self.editar(costo_unitario='140',version='2')
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ESPERANDO_DG')

    def test_orden_en_revision_no_admite_cambiar_a_otra_cotizacion(self):
        alternativa=CotizacionCompraDepartamental.objects.create(item=self.item,proveedor=self.proveedor,
                                                                 cantidad_ofertada=2,costo_unitario=70)
        self.ordenar(); self.editar(costo_unitario='150')
        response=self.client.post(reverse('compras:departamental_cotizacion_seleccionar',args=[alternativa.pk]),{},**self.headers)
        self.assertEqual(response.status_code,409)
        self.quote.refresh_from_db()
        self.assertTrue(self.quote.seleccionada)

    def test_comprobante_no_acepta_html_disfrazado_de_pdf(self):
        fake=SimpleUploadedFile('falso.pdf',b'<script>alert(1)</script>',content_type='application/pdf')
        self.assertEqual(self.comprar(comprobante=fake).status_code,400)
        self.assertFalse(CompraRealizadaDepartamental.objects.exists())

    def test_direccion_no_autoriza_una_version_que_no_vio(self):
        self.editar(costo_unitario='150')
        version_vista=2
        self.editar(costo_unitario='250',version='2')
        response=self.client.post(reverse('compras:departamental_decidir',args=[self.item.pk]),
                                  {'decision':'AUTORIZAR','cotizacion_id':self.quote.pk,'version':version_vista},**self.headers)
        self.assertEqual(response.status_code,409,response.content)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ESPERANDO_DG')
        self.assertFalse(self.item.compromiso.activo)

    def test_compra_fraccionaria_acepta_precio_redondeado_a_centavos(self):
        self.quote.refresh_from_db()
        self.quote.cantidad_ofertada=Decimal('0.666')
        self.quote.costo_unitario=Decimal('1')
        self.quote.save()
        response=self.comprar(importe_final='0.67')
        self.assertEqual(response.status_code,200,response.content)
        self.assertEqual(CompraRealizadaDepartamental.objects.get().importe_final,Decimal('0.67'))
