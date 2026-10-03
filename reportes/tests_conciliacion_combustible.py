from datetime import date, datetime
from decimal import Decimal
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Sucursal
from logistica.models import BitacoraSalidaLlegada, CargaCombustibleUnidad, Repartidor, Unidad
from sat_client.models import CfdiDescargado
from reportes.services_conciliacion_combustible import conciliar_combustible, render_conciliacion_texto


class ConciliacionDocumentalCombustibleTests(TestCase):
    periodo = date(2026, 9, 1)

    def factura(self, clave='15101505', descripcion='Diesel 34006', importe='1200', folio='2979907', **kwargs):
        total = Decimal(importe)
        xml = (f'<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Total="{total}" Moneda="MXN">'
               f'<cfdi:Conceptos><cfdi:Concepto ClaveProdServ="{clave}" Descripcion="{escape(descripcion)}" '
               f'Importe="{total}" Cantidad="44.610" ClaveUnidad="LTR" NoIdentificacion="PL/7296/EXP/ES/2015-{folio}"/>'
               '</cfdi:Conceptos></cfdi:Comprobante>')
        datos = dict(uuid=f'factura-{CfdiDescargado.objects.count()}', rfc_emisor='FAOR391222TTA',
                     nombre_emisor='ROSA HILDELIZA FAMANIA ORTEGA', rfc_receptor='GEF211230KR2',
                     nombre_receptor='EMPRESA', subtotal=total, total=total, moneda='MXN',
                     tipo_comprobante='I', tipo_cfdi='recibido', estatus='vigente', xml_raw=xml,
                     fecha_emision=datetime(2026, 9, 14, 12, tzinfo=ZoneInfo('America/Mazatlan')))
        datos.update(kwargs)
        return CfdiDescargado.objects.create(**datos)

    def carga(self, folio='2979907', importe='1200', estado='ok', fecha='2026-09-12', estacion='SERVICIO CARRANZA'):
        sucursal, _ = Sucursal.objects.get_or_create(codigo='TESTCOMB', defaults={'nombre': 'Prueba'})
        unidad, _ = Unidad.objects.get_or_create(codigo='GS-DC1', defaults={'descripcion':'Ducato','sucursal':sucursal})
        user, _ = get_user_model().objects.get_or_create(username='testcomb')
        repartidor, _ = Repartidor.objects.get_or_create(user=user, defaults={'sucursal':sucursal})
        bitacora = BitacoraSalidaLlegada.objects.create(unidad=unidad,repartidor=repartidor,km_salida=1,
                                                       nivel_gas_salida='1/2',foto_tablero_salida='test/tablero.jpg')
        obj = CargaCombustibleUnidad.objects.create(unidad=unidad,repartidor=repartidor,bitacora=bitacora,
              importe_total=importe,litros='44.61',foto_ticket='test/ticket.jpg',auditoria_estado=estado,
              auditoria_detalle={'ticket_leido':{'folio':folio,'importe_total':importe,'litros':'44.610',
                                               'fecha':fecha,'estacion':estacion,'legible':True,'es_ticket':estado=='ok'}})
        CargaCombustibleUnidad.objects.filter(pk=obj.pk).update(fecha_registro=datetime(2026,9,12,13,tzinfo=ZoneInfo('America/Mazatlan')))
        return obj

    def test_ribbon_premium_no_es_gasolina(self):
        self.factura(clave='44103124',descripcion='Ribbon Resina Premium para impresora',importe='2157.99',rfc_emisor='ETI200904JL7')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_facturado']['GASOLINA'],Decimal('0'))
        self.assertEqual(datos['facturas'],[])

    def test_anticipos_separados_del_consumo(self):
        self.factura(clave='84111506',descripcion='Anticipo del bien o servicio',importe='4000')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_anticipos'],Decimal('4000'))
        self.assertEqual(datos['total_facturado']['DIESEL'],Decimal('0'))
        self.assertIsNone(datos['saldo_vales'])

    def test_no_usa_emitidos_cancelados_ni_otro_receptor(self):
        self.factura(tipo_cfdi='emitido')
        self.factura(estatus='cancelado')
        self.factura(rfc_receptor='OTRO123456ABC')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['facturas'],[])

    def test_cruce_por_folio_con_factura_dos_dias_despues(self):
        factura=self.factura()
        carga=self.carga()
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['cargas'][0]['estado'],'CRUCE_DOCUMENTAL')
        self.assertEqual(datos['cargas'][0]['uuid_cfdi'],factura.uuid)
        self.assertEqual(datos['total_cruzado'],Decimal('1200'))
        carga.refresh_from_db()
        factura.refresh_from_db()
        self.assertFalse(factura.conciliado)
        self.assertEqual(carga.importe_total,Decimal('1200'))

    def test_foto_no_ticket_conserva_importe_pendiente(self):
        self.carga(estado='alto_riesgo')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['cargas'][0]['estado'],'RESPALDO_PENDIENTE')
        self.assertEqual(datos['total_bitacora'],Decimal('1200'))
        self.assertEqual(datos['total_cruzado'],Decimal('0'))

    def test_ticket_duplicado_no_cruza_dos_cargas(self):
        self.factura();self.carga();self.carga()
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_cruzado'],Decimal('0'))
        self.assertTrue(all(c['estado']=='DUPLICADO_REVISAR' for c in datos['cargas']))

    def test_otro_emisor_mismo_folio_no_cruza(self):
        self.factura(rfc_emisor='OTRO123456ABC');self.carga()
        self.assertEqual(conciliar_combustible(self.periodo)['total_cruzado'],Decimal('0'))

    def test_importe_distinto_no_cruza(self):
        self.factura();self.carga(importe='1000')
        self.assertEqual(conciliar_combustible(self.periodo)['total_cruzado'],Decimal('0'))

    def test_gasolina_sin_ticket_no_se_atribuye_por_centavos(self):
        self.factura(clave='15101514',descripcion='Gasolina Magna',importe='1535.02')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_facturado']['GASOLINA'],Decimal('1535.02'))
        texto=render_conciliacion_texto(datos)
        self.assertIn('SIN CARGA IDENTIFICADA',texto)
        self.assertNotIn('posible consumo directo',texto)
        self.assertNotIn('DIFERENCIA facturado',texto)

    def test_fuel_de_otro_mes_puede_respaldar_ticket_del_periodo(self):
        self.factura(fecha_emision=datetime(2026,10,1,12,tzinfo=ZoneInfo('America/Mazatlan')));self.carga()
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_cruzado'],Decimal('1200'))
        self.assertEqual(datos['total_facturado']['DIESEL'],Decimal('0'))

    def test_factura_mixta_distribuye_importe_e_impuestos_por_concepto(self):
        xml='''<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Total="232" Moneda="MXN"><cfdi:Conceptos>
        <cfdi:Concepto ClaveProdServ="15101514" Descripcion="Gasolina" Importe="100" Descuento="10"><cfdi:Impuestos><cfdi:Traslados><cfdi:Traslado Importe="14.40"/></cfdi:Traslados></cfdi:Impuestos></cfdi:Concepto>
        <cfdi:Concepto ClaveProdServ="55121600" Descripcion="Etiquetas" Importe="110"><cfdi:Impuestos><cfdi:Traslados><cfdi:Traslado Importe="17.60"/></cfdi:Traslados></cfdi:Impuestos></cfdi:Concepto>
        </cfdi:Conceptos></cfdi:Comprobante>'''
        self.factura(importe='232',xml_raw=xml)
        self.assertEqual(conciliar_combustible(self.periodo)['total_facturado']['GASOLINA'],Decimal('104.40'))

    def test_egreso_relacionado_se_muestra_sin_contarlo_como_consumo(self):
        self.factura(tipo_comprobante='E')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_facturado']['DIESEL'],Decimal('0'))
        self.assertEqual(len(datos['egresos']),1)

    def test_xml_ausente_se_reporta_pendiente(self):
        self.factura(xml_raw='')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['facturas'],[])
        self.assertIn('XML ausente o inválido',datos['pendientes'][0])

    def test_no_asigna_consumo_en_moneda_distinta_a_mxn(self):
        self.factura(moneda='USD')
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['facturas'],[])
        self.assertIn('pendiente de conversión',datos['pendientes'][0])

    def test_dos_cfdi_del_mismo_ticket_no_cruzan_automaticamente(self):
        self.factura();self.factura();self.carga()
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_cruzado'],Decimal('0'))
        self.assertEqual(datos['cargas'][0]['estado'],'CFDI_MULTIPLE_REVISAR')

    def test_anticipo_no_consistente_no_se_suma(self):
        self.factura(clave='84111506',descripcion='Anticipo del bien o servicio',importe='4000',total=Decimal('5000'))
        datos=conciliar_combustible(self.periodo)
        self.assertEqual(datos['total_anticipos'],Decimal('0'))
        self.assertIn('Anticipo',datos['pendientes'][0])

    def test_tarea_envia_reporte_corregido_con_asunto_parcial(self):
        from unittest.mock import patch
        from reportes.tasks import task_conciliar_combustible_mensual
        self.factura(clave='84111506',descripcion='Anticipo del bien o servicio',importe='4000')
        with patch('django.core.mail.send_mail') as mail, patch('django.utils.timezone.localdate',return_value=date(2026,10,3)):
            resultado=task_conciliar_combustible_mensual.run()
        self.assertEqual(resultado['estado'],'PARCIAL')
        self.assertIsNone(resultado['diferencia'])
        self.assertIn('— parcial',mail.call_args.kwargs['subject'])
        self.assertIn('ANTICIPOS FACTURADOS: $4,000.00',mail.call_args.kwargs['message'])
        self.assertNotIn('posible consumo directo',mail.call_args.kwargs['message'])
