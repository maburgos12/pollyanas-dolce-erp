from datetime import date, timedelta
from decimal import Decimal
from tempfile import TemporaryDirectory

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from compras.models import (CotizacionCompraDepartamental, ItemCompraDepartamental,
                            SolicitudCompraDepartamental, RecepcionItemDepartamental,
                            CompraRealizadaDepartamental, CompromisoCompraDepartamental,
                            HistorialCotizacionDepartamental, IntentoCompraDepartamental,
                            AvisoCompraDepartamental)
from compras.services_departamentales import evaluar_presupuesto_item, seleccionar_cotizacion, generar_ordenes_departamentales
from compras.services_edicion_compra import (corregir_compra_realizada, registrar_compra_realizada,
                                             sincronizar_linea_orden, tiene_compra_o_recepcion, validar_edicion,
                                             tiene_recepcion_historica)
from maestros.models import Proveedor
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable, RubroPresupuesto, LineaPresupuestoMensual


class _CompraDepartamentalBase:
    """setUp y helpers compartidos; sin pruebas propias para no duplicarlas."""

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
        self.item = ItemCompraDepartamental.objects.create(solicitud=self.solicitud, descripcion='Bancos', cantidad=2, rubro=self.rubro)
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

    def compromiso_actual(self):
        intento = self.item.intento_vigente
        return (intento.compromiso if intento else self.item.compromisos.filter(intento__isnull=True).order_by('-pk').first())

    def linea_actual(self):
        intento = self.item.intento_vigente
        return intento.linea_orden if intento else None

class EdicionCompraTests(_CompraDepartamentalBase, TestCase):
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

    def test_autorizacion_existente_sin_rubro_se_conserva_al_consultar_y_reducir(self):
        self.item.rubro = None
        self.item.save(update_fields=['rubro'])
        for estado in ('AUTORIZADO', 'ORDENADO'):
            with self.subTest(estado=estado):
                if estado == 'ORDENADO':
                    self.ordenar()
                response = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
                self.assertContains(response, 'Sin presupuesto asignado')
                self.assertContains(response, 'No calculable')
                self.assertNotContains(response, '10,000.00')
                self.item.refresh_from_db()
                self.assertEqual(self.item.estado, estado)
                self.assertTrue(self.compromiso_actual().activo)
        self.assertEqual(self.editar(costo_unitario='90').status_code, 200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'ORDENADO')
        self.assertEqual(self.linea_actual().total, Decimal('180'))
        self.assertTrue(self.compromiso_actual().activo)

    def test_compra_realizada_sin_rubro_no_se_altera_al_consultar(self):
        self.assertEqual(self.comprar().status_code, 200)
        self.item.rubro = None
        self.item.save(update_fields=['rubro'])
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        response = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
        self.assertContains(response, 'Sin presupuesto asignado')
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'COMPRADO')
        self.assertEqual(CompraRealizadaDepartamental.objects.get(item=self.item).pk, compra.pk)
        self.assertTrue(self.compromiso_actual().activo)

    def test_sin_rubro_texto_preserva_autorizacion_pero_incremento_requiere_dg(self):
        self.item.rubro = None
        self.item.save(update_fields=['rubro'])
        self.assertEqual(self.editar().status_code, 200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'AUTORIZADO')
        self.assertEqual(self.editar(costo_unitario='150', version='2').status_code, 200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'ESPERANDO_DG')
        self.assertFalse(self.compromiso_actual().activo)

    def test_real_desconocido_visible_sin_inventar_disponibilidad(self):
        LineaPresupuestoMensual.objects.filter(rubro=self.rubro).update(monto_real=None)
        response = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
        self.assertContains(response, 'Gasto real no disponible')
        self.assertContains(response, 'No calculable')
        self.assertNotContains(response, 'Sin presupuesto asignado')
        self.assertContains(response, '10,000.00')
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'AUTORIZADO')

    def test_sin_rubro_seleccion_nueva_aparece_con_motivo_en_direccion(self):
        self.item.rubro = None
        self.item.save(update_fields=['rubro'])
        nueva = CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=self.proveedor, cantidad_ofertada=2, costo_unitario=90)
        seleccionar_cotizacion(nueva, actor=self.user)
        response = self.client.get(reverse('compras:departamental_direccion'))
        self.assertContains(response, 'Sin presupuesto asignado')
        self.assertContains(response, 'No calculable')

    def test_incremento_requiere_dg_aunque_haya_presupuesto_global(self):
        self.assertEqual(self.editar(costo_unitario='150').status_code,200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ESPERANDO_DG')
        self.assertFalse(self.compromiso_actual().activo)
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
        line_pk=self.linea_actual().pk
        self.assertEqual(self.editar(costo_unitario='90').status_code,200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ORDENADO')
        self.assertEqual(self.linea_actual().pk,line_pk)
        self.assertEqual(self.linea_actual().total,Decimal('180'))
        self.assertEqual(self.comprar(importe_final='180').status_code,200)

    def test_reautorizacion_actualiza_orden_existente(self):
        self.ordenar()
        line_pk=self.linea_actual().pk
        self.assertEqual(self.editar(costo_unitario='150').status_code,200)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'ESPERANDO_DG')
        self.assertEqual(self.comprar(importe_final='300').status_code,409)
        response=self.client.post(reverse('compras:departamental_decidir',args=[self.item.pk]),
                                  {'decision':'AUTORIZAR','cotizacion_id':self.quote.pk,
                                   'version':CotizacionCompraDepartamental.objects.get(pk=self.quote.pk).version},**self.headers)
        self.assertEqual(response.status_code,200,response.content)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'ORDENADO')
        self.assertEqual(self.linea_actual().pk,line_pk)
        self.assertEqual(self.linea_actual().total,Decimal('300'))
        self.assertEqual(self.comprar(importe_final='300').status_code,200)

    def test_orden_no_permite_cambiar_proveedor_o_cantidad(self):
        self.ordenar()
        otro=Proveedor.objects.create(nombre='Otro')
        self.assertGreaterEqual(self.editar(proveedor=otro.pk).status_code,400)
        self.assertGreaterEqual(self.editar(cantidad_ofertada='3').status_code,400)

    def test_formulario_advierte_restricciones_de_orden_vigente(self):
        url = reverse('compras:departamental_cotizacion_editar', args=[self.quote.pk])
        self.assertNotContains(self.client.get(url), 'La orden ya existe: este dato debe conservarse.')
        self.ordenar()
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'La orden ya existe: este dato debe conservarse.', count=2)
        detalle = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
        self.assertContains(detalle, url)

    def test_compra_crea_orden_sin_entrega_ni_gasto_contable(self):
        response=self.comprar()
        self.assertEqual(response.status_code,200,response.content)
        self.item.refresh_from_db()
        compra=CompraRealizadaDepartamental.objects.get(item=self.item)
        self.assertEqual(compra.importe_final,Decimal('200'))
        self.assertEqual(compra.registrado_por,self.user)
        self.assertEqual(self.item.estado,'COMPRADO')
        self.assertEqual(self.item.monto_gastado,0)
        self.assertIsNotNone(self.linea_actual())
        self.assertFalse(RecepcionItemDepartamental.objects.exists())
        self.assertEqual(self.compromiso_actual().monto,Decimal('200'))

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
        RecepcionItemDepartamental.objects.create(linea_orden=self.linea_actual(),cantidad_recibida=1,registrado_por=self.user)
        self.assertGreaterEqual(self.editar().status_code,400)

    def test_entrega_posterior_a_compra_se_registra_por_separado(self):
        self.comprar()
        intento = self.item.intento_vigente
        response=self.client.post(reverse('compras:departamental_recibir',args=[self.item.pk]),
                                  {'cantidad_recibida':'2', 'intento_id': intento.pk,
                                   'intento_version': intento.version},**self.headers)
        self.assertEqual(response.status_code,200,response.content)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado,'PENDIENTE_CONFIRMACION')
        self.assertEqual(CompraRealizadaDepartamental.objects.count(),1)

    def test_compra_y_compromiso_siguen_visibles_tras_entrega_total(self):
        from compras.resumen_departamentales import construir_resumen_departamental

        self.assertEqual(self.comprar().status_code, 200)
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        self.assertEqual(compra.avisos.filter(canal=AvisoCompraDepartamental.CANAL_CORREO).update(
            destino='area@example.com',
        ), 1)
        self.assertEqual(self.client.post(
            reverse('compras:departamental_recibir', args=[self.item.pk]),
            {'cantidad_recibida': '2', 'intento_id': compra.intento_id,
             'intento_version': compra.intento.version}, **self.headers,
        ).status_code, 200)
        compra.intento.refresh_from_db()
        self.assertEqual(compra.intento.estado, IntentoCompraDepartamental.ESTADO_ENTREGADO)

        detalle = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
        self.assertContains(detalle, 'PEDIDO-PRUEBA')
        self.assertContains(detalle, reverse('compras:departamental_compra_comprobante', args=[compra.pk]))
        self.assertContains(detalle, 'Aviso al solicitante')
        self.assertContains(detalle, 'area@example.com')
        item_visible = next(item for item in detalle.context['solicitud'].items.all() if item.pk == self.item.pk)
        self.assertEqual(item_visible.compra_realizada, compra)
        self.assertEqual(item_visible.intentos_compra_prefetched[0], compra.intento)
        self.assertEqual(len(item_visible.linea_orden.recepciones_prefetched), 1)
        self.assertEqual(detalle.context['total_comprometido'], Decimal('0'))
        self.assertContains(detalle, 'Comprometido vigente')
        self.assertContains(detalle, 'Exposición activa de la partida')
        self.assertContains(detalle, 'Incluye órdenes entregadas aún no liberadas y reembolsos pendientes')
        resumen = construir_resumen_departamental({'estado': 'PENDIENTE_CONFIRMACION'})
        # ENTREGADO conserva la compra histórica, pero ya no es Comprometido vigente.
        self.assertEqual(resumen['resumen']['comprometido'], Decimal('0'))
        nuevo = self.solicitud.items.create(descripcion='Reemplazo posterior', cantidad=1, rubro=self.rubro)
        evaluacion = evaluar_presupuesto_item(nuevo, Decimal('100'))
        self.assertEqual(evaluacion.compromisos_previos, Decimal('200'))
        self.assertEqual(evaluacion.disponible_despues, Decimal('9700'))
        compromiso_entregado = CompromisoCompraDepartamental.objects.get(intento=compra.intento)
        compromiso_entregado.activo = False
        compromiso_entregado.save(update_fields=['activo'])
        evaluacion_sin_exposicion = evaluar_presupuesto_item(nuevo, Decimal('100'))
        self.assertEqual(evaluacion_sin_exposicion.compromisos_previos, Decimal('0'))
        self.assertEqual(evaluacion_sin_exposicion.disponible_despues, Decimal('9900'))

        responsable = get_user_model().objects.create_user('responsable-area')
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=responsable, puede_capturar=True)
        self.client.force_login(responsable)
        detalle_area = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
        self.assertContains(detalle_area, 'Confirma lo que recibiste')
        self.assertContains(detalle_area, 'PEDIDO-PRUEBA')
        self.assertEqual(self.client.post(
            reverse('compras:departamental_confirmar', args=[self.item.pk]),
            {'conforme': '1'}, **self.headers,
        ).status_code, 200)
        detalle = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
        self.assertContains(detalle, 'PEDIDO-PRUEBA')
        self.assertContains(detalle, reverse('compras:departamental_compra_comprobante', args=[compra.pk]))
        self.assertContains(detalle, 'area@example.com')

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
        self.assertEqual(self.compromiso_actual().monto,Decimal('200'))

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
        self.assertFalse(self.compromiso_actual().activo)

    def test_compra_fraccionaria_acepta_precio_redondeado_a_centavos(self):
        self.quote.refresh_from_db()
        self.quote.cantidad_ofertada=Decimal('0.666')
        self.quote.costo_unitario=Decimal('1')
        self.quote.save()
        response=self.comprar(importe_final='0.67')
        self.assertEqual(response.status_code,200,response.content)
        self.assertEqual(CompraRealizadaDepartamental.objects.get().importe_final,Decimal('0.67'))


class CorreccionCompraRegistradaTests(_CompraDepartamentalBase, TestCase):
    """La compra ya pagada se corrige con motivo y sin reabrir la autorización."""

    def test_correccion_bloquea_item_intento_compra_en_ese_orden_sin_joins(self):
        self.assertEqual(self.comprar().status_code, 200)
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        bloqueos = []

        def registrar_bloqueo(execute, sql, params, many, context):
            if 'FOR UPDATE' in sql.upper():
                for modelo in ('itemcompradepartamental', 'intentocompradepartamental',
                               'comprarealizadadepartamental'):
                    if f'FROM "compras_{modelo}"' in sql:
                        bloqueos.append((modelo, sql))
                        break
            return execute(sql, params, many, context)

        with connection.execute_wrapper(registrar_bloqueo):
            corregida = corregir_compra_realizada(
                compra, datos={'importe_final': Decimal('190')},
                version=compra.version, motivo='Importe comprobado', actor=self.user,
            )
        self.assertEqual(corregida.importe_final, Decimal('190'))
        self.assertEqual([modelo for modelo, _ in bloqueos[:3]], [
            'itemcompradepartamental', 'intentocompradepartamental',
            'comprarealizadadepartamental',
        ])
        self.assertNotIn(' JOIN ', bloqueos[2][1].upper())

    def corregir(self, **changes):
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        data = {'version': compra.version, 'fecha_compra': compra.fecha_compra.isoformat(),
                'importe_final': '296.62', 'numero_pedido': compra.numero_pedido,
                'motivo': 'Se capturó el precio por pieza; el paquete cuesta más.', **changes}
        return self.client.post(
            reverse('compras:departamental_compra_corregir', args=[compra.pk]), data, **self.headers)

    def test_corrige_importe_con_historial_y_sin_reabrir_autorizacion(self):
        self.ordenar()
        self.comprar()
        respuesta = self.corregir()
        self.assertEqual(respuesta.status_code, 200)
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        self.item.refresh_from_db()
        self.assertEqual(compra.importe_final, Decimal('296.62'))
        self.assertEqual(compra.version, 2)
        # Ya se pagó: el artículo no vuelve con Dirección General.
        self.assertEqual(self.item.estado, 'COMPRADO')
        self.assertNotEqual(self.item.siguiente_responsable, ItemCompraDepartamental.RESPONSABLE_DG)
        historial = compra.historial.get()
        self.assertEqual(historial.antes['importe_final'], '200.00')
        self.assertEqual(historial.despues['importe_final'], '296.62')
        self.assertIn('precio por pieza', historial.motivo)
        self.assertTrue(self.item.eventos.filter(tipo='COMPRA_CORREGIDA').exists())
        # El importe comprometido sigue a lo realmente pagado.
        self.assertEqual(self.compromiso_actual().monto, Decimal('296.62'))

    def test_corregir_por_arriba_de_la_cotizacion_es_valido(self):
        """Registrar exige no superar la cotización; corregir existe justo porque
        la cotización pudo capturarse mal."""
        self.ordenar()
        self.comprar()
        self.corregir(importe_final='999.00')
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        self.assertEqual(compra.importe_final, Decimal('999.00'))

    def test_sin_motivo_no_corrige(self):
        self.ordenar()
        self.comprar()
        respuesta = self.corregir(motivo='')
        self.assertEqual(respuesta.status_code, 400)
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        self.assertEqual(compra.importe_final, Decimal('200.00'))
        self.assertFalse(compra.historial.exists())

    def test_version_vieja_no_pisa_una_correccion_ajena(self):
        self.ordenar()
        self.comprar()
        self.corregir(importe_final='296.62')
        respuesta = self.corregir(version='1', importe_final='50.00')
        self.assertEqual(respuesta.status_code, 409)
        compra = CompraRealizadaDepartamental.objects.get(item=self.item)
        self.assertEqual(compra.importe_final, Decimal('296.62'))

    def test_area_solicitante_no_puede_corregir(self):
        self.ordenar()
        self.comprar()
        otro = get_user_model().objects.create_user('area-sin-permiso', password='test')
        self.client.force_login(otro)
        respuesta = self.corregir()
        self.assertEqual(respuesta.status_code, 403)

    def test_detalle_ofrece_corregir_y_muestra_el_historial(self):
        self.ordenar()
        self.comprar()
        self.corregir()
        respuesta = self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))
        self.assertContains(respuesta, 'Corregir esta compra')
        self.assertContains(respuesta, 'precio por pieza')


class FlujoPorIntentoTests(_CompraDepartamentalBase, TestCase):
    def registrar(self):
        return registrar_compra_realizada(
            self.item, fecha_compra=timezone.localdate(), importe_final=Decimal('200'),
            numero_pedido='PEDIDO-PRUEBA',
            comprobante=SimpleUploadedFile('prueba.pdf', b'%PDF-1.4\n%%EOF'),
            actor=self.user, cotizacion_id=self.quote.pk, version=self.quote.version,
        )

    def test_segundo_intento_usa_linea_y_compra_vigentes_sin_alterar_historial(self):
        self.ordenar()
        primera = self.item.intento_vigente
        compra_vieja = self.registrar()
        linea_vieja = primera.linea_orden
        primera.estado = IntentoCompraDepartamental.ESTADO_REEMBOLSADO
        primera.save(update_fields=['estado'])
        primera.compromiso.activo = False
        primera.compromiso.save(update_fields=['activo'])
        self.item.estado = ItemCompraDepartamental.ESTADO_AUTORIZADO
        self.item.save(update_fields=['estado'])
        CompromisoCompraDepartamental.objects.create(
            item=self.item, cotizacion=self.quote, monto=Decimal('200'), activo=True,
        )
        self.assertFalse(tiene_compra_o_recepcion(self.item))
        self.assertIsNone(sincronizar_linea_orden(self.item, self.quote, actor=self.user))

        self.ordenar()
        segunda = self.item.intento_vigente
        self.assertNotEqual(segunda, primera)
        self.assertEqual(sincronizar_linea_orden(self.item, self.quote, actor=self.user), segunda.linea_orden)
        compra_nueva = self.registrar()
        self.assertEqual(compra_nueva.intento, segunda)
        self.assertEqual(compra_nueva.intento.compromiso.monto, Decimal('200'))
        compra_vieja.refresh_from_db()
        linea_vieja.refresh_from_db()
        self.assertEqual(compra_vieja.intento, primera)
        self.assertEqual(compra_vieja.importe_final, Decimal('200'))
        self.assertEqual(linea_vieja.intento, primera)
        self.assertTrue(compra_vieja.avisos.exists())
        self.assertTrue(compra_nueva.avisos.exists())
        self.assertEqual(set(compra_vieja.avisos.values_list('compra_id', flat=True)), {compra_vieja.pk})
        self.assertEqual(set(compra_nueva.avisos.values_list('compra_id', flat=True)), {compra_nueva.pk})

        corregir_compra_realizada(
            compra_vieja, datos={'importe_final': Decimal('190')}, version=compra_vieja.version,
            motivo='Corrección histórica', actor=self.user,
        )
        primera.compromiso.refresh_from_db()
        segunda.compromiso.refresh_from_db()
        self.assertEqual(primera.compromiso.monto, Decimal('190'))
        self.assertEqual(segunda.compromiso.monto, Decimal('200'))

    def test_cotizacion_nueva_no_reutiliza_compromiso_de_intento_historico(self):
        self.ordenar()
        historico = self.item.intento_vigente
        compromiso_historico = historico.compromiso
        historico.estado = IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO
        historico.save(update_fields=['estado'])
        compromiso_historico.activo = False
        compromiso_historico.save(update_fields=['activo'])
        self.item.estado = ItemCompraDepartamental.ESTADO_POR_COTIZAR
        self.item.save(update_fields=['estado'])
        nueva = CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=self.proveedor, cantidad_ofertada=2, costo_unitario=Decimal('90'),
        )

        seleccionar_cotizacion(nueva, actor=self.user)

        self.item.refresh_from_db()
        compromiso_historico.refresh_from_db()
        self.assertEqual(self.item.estado, ItemCompraDepartamental.ESTADO_AUTORIZADO)
        self.assertFalse(compromiso_historico.activo)
        self.assertEqual(compromiso_historico.cotizacion, self.quote)
        self.assertEqual(self.compromiso_actual().cotizacion, nueva)
        self.assertIsNone(self.compromiso_actual().intento)

    def test_recepcion_historica_bloquea_edicion_aunque_no_haya_intento_vigente(self):
        self.ordenar()
        intento = self.item.intento_vigente
        RecepcionItemDepartamental.objects.create(
            linea_orden=intento.linea_orden, cantidad_recibida=1, registrado_por=self.user,
        )
        intento.estado = IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO
        intento.save(update_fields=['estado'])
        self.assertFalse(tiene_compra_o_recepcion(self.item))
        self.assertTrue(tiene_recepcion_historica(self.item))
        with self.assertRaises(ValidationError):
            validar_edicion(self.item)

    def test_correccion_no_baja_de_reembolso_solicitado(self):
        compra = self.registrar()
        intento = compra.intento
        intento.reembolso_solicitado = Decimal('150')
        for estado in (IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO,
                       IntentoCompraDepartamental.ESTADO_REEMBOLSADO):
            with self.subTest(estado=estado):
                intento.estado = estado
                intento.save(update_fields=['estado', 'reembolso_solicitado'])
                with self.assertRaises(ValidationError):
                    corregir_compra_realizada(
                        compra, datos={'importe_final': Decimal('140')}, version=compra.version,
                        motivo='Corrección', actor=self.user,
                    )
        compra.refresh_from_db()
        self.assertEqual(compra.importe_final, Decimal('200'))

    def test_resumen_compromete_solo_el_intento_vigente(self):
        from compras.resumen_departamentales import construir_resumen_departamental

        historico = IntentoCompraDepartamental.objects.create(
            item=self.item, cotizacion=self.quote,
            estado=IntentoCompraDepartamental.ESTADO_REEMBOLSADO,
        )
        reserva = CompromisoCompraDepartamental.objects.get(item=self.item)
        reserva.intento = historico
        reserva.monto = Decimal('999')
        reserva.formalizado_en = timezone.now()
        reserva.save(update_fields=['intento', 'monto', 'formalizado_en'])
        actual = IntentoCompraDepartamental.objects.create(item=self.item, cotizacion=self.quote)
        CompromisoCompraDepartamental.objects.create(
            item=self.item, intento=actual, cotizacion=self.quote, monto=Decimal('200'),
            formalizado_en=timezone.now(), activo=True,
        )
        self.assertEqual(construir_resumen_departamental({})['resumen']['comprometido'], Decimal('200'))
