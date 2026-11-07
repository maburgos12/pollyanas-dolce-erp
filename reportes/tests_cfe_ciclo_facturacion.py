"""CFE bimestral: facturación del mes distinta de referencia de consumo."""
from datetime import date, datetime
from decimal import Decimal
from django.test import TestCase
from django.contrib.auth import get_user_model
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from core.models import Sucursal
from sat_client.models import CfdiDescargado
from reportes.models import AreaPresupuesto, CategoriaGasto, CentroCosto, GastoOperativoMensual, ReglaFuenteRubro, RubroPresupuesto
from reportes.services_rentabilidad_gastos import leer_gastos_mensuales


class CicloFacturacionCFETests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.sucursal = Sucursal.objects.create(codigo='CFEBIM', nombre='Sucursal CFE')
        cls.centro = CentroCosto.objects.create(codigo='CFEBIM', nombre='Sucursal CFE', tipo='SUCURSAL_VENTA', sucursal=cls.sucursal)
        cls.area = AreaPresupuesto.objects.create(codigo='gastos-venta', nombre='Gastos de venta')
        cls.categoria = CategoriaGasto.objects.create(codigo='LUZ_SUC', nombre='Energía eléctrica', capa_objetivo='SUCURSAL')
        cls.rubro = RubroPresupuesto.objects.create(area=cls.area, concepto='Energía eléctrica', tipo='EGRESO', sucursal=cls.sucursal)
        cls.regla = ReglaFuenteRubro.objects.create(rubro=cls.rubro, tipo_fuente='GASTO_OPERATIVO', categoria_gasto=cls.categoria)
        cls.cfdi = cls.factura('11111111-1111-4111-8111-111111111111', '11690.32', 9)
        cls.gasto = GastoOperativoMensual.objects.create(periodo=date(2026, 9, 1), cobertura_mes_inicio=date(2026, 9, 1), cobertura_mes_fin=date(2026, 9, 1), centro_costo=cls.centro, categoria_gasto=cls.categoria, monto=Decimal('11690.32'), cfdi_soporte=cls.cfdi, servicio_desde=date(2026, 7, 14), servicio_hasta=date(2026, 9, 11))

    @classmethod
    def factura(cls, uuid, total, mes):
        xml = f'<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Total="{total}" Moneda="MXN" TipoDeComprobante="I"><cfdi:Emisor Rfc="CFE370814QI0"/><cfdi:Receptor Rfc="GEF211230KR2"/><cfdi:Complemento><t:TimbreFiscalDigital xmlns:t="http://www.sat.gob.mx/TimbreFiscalDigital" UUID="{uuid}"/></cfdi:Complemento></cfdi:Comprobante>'
        return CfdiDescargado.objects.create(uuid=uuid, tipo_cfdi='recibido', tipo_comprobante='I', estatus='vigente', moneda='MXN', rfc_emisor='CFE370814QI0', rfc_receptor='GEF211230KR2', total=Decimal(total), subtotal=Decimal(total), fecha_emision=timezone.make_aware(datetime(2026, mes, 13, 12)), xml_raw=xml)

    def test_mes_intermedio_se_explica_sin_duplicar_factura_ni_crear_costo_cero(self):
        resultado = leer_gastos_mensuales(date(2026, 10, 1))
        self.assertEqual(resultado['pendientes'], [])
        self.assertEqual(resultado['filas'], [])
        ciclos = resultado.get('ciclos_facturacion', [])
        self.assertEqual(len(ciclos), 1)
        self.assertEqual(ciclos[0]['gasto_id'], self.gasto.pk)
        self.assertEqual(ciclos[0]['uuid'], self.cfdi.uuid)
        self.assertEqual(ciclos[0]['mes_factura'], date(2026, 9, 1))
        self.assertEqual(ciclos[0]['importe_facturado'], Decimal('11690.32'))
        self.assertIsNone(ciclos[0]['monto_mensual'])
        self.gasto.refresh_from_db()
        self.assertEqual(self.gasto.monto, Decimal('11690.32'))
        self.assertEqual(self.gasto.periodo, date(2026, 9, 1))

    def test_mes_facturado_conserva_importe_entero(self):
        resultado = leer_gastos_mensuales(date(2026, 9, 1))
        self.assertEqual(sum(f['monto_mensual'] for f in resultado['filas']), Decimal('11690.32'))
        self.assertEqual(resultado.get('ciclos_facturacion', []), [])

    def test_siguiente_ciclo_exige_su_fuente(self):
        self.assertTrue(leer_gastos_mensuales(date(2026, 11, 1))['pendientes'])

    def test_sin_periodo_documentado_no_asume_ciclo(self):
        GastoOperativoMensual.objects.filter(pk=self.gasto.pk).update(servicio_desde=None, servicio_hasta=None)
        self.assertTrue(leer_gastos_mensuales(date(2026, 10, 1))['pendientes'])

    def test_recibo_mensual_no_elimina_falta_del_mes_siguiente(self):
        GastoOperativoMensual.objects.filter(pk=self.gasto.pk).update(servicio_desde=date(2026, 8, 11))
        self.assertTrue(leer_gastos_mensuales(date(2026, 10, 1))['pendientes'])

    def test_cfdi_cancelado_no_acredita_ciclo(self):
        CfdiDescargado.objects.filter(pk=self.cfdi.pk).update(estatus='cancelado')
        self.assertTrue(leer_gastos_mensuales(date(2026, 10, 1))['pendientes'])

    def test_importe_incompatible_no_acredita_ciclo(self):
        GastoOperativoMensual.objects.filter(pk=self.gasto.pk).update(monto=Decimal('12000'))
        self.assertTrue(leer_gastos_mensuales(date(2026, 10, 1))['pendientes'])

    def test_factura_extraordinaria_del_mes_se_suma_una_vez(self):
        cfdi = self.factura('22222222-2222-4222-8222-222222222222', '100.00', 10)
        GastoOperativoMensual.objects.create(periodo=date(2026, 10, 1), centro_costo=self.centro, categoria_gasto=self.categoria, monto=Decimal('100'), cfdi_soporte=cfdi)
        resultado = leer_gastos_mensuales(date(2026, 10, 1))
        self.assertEqual(sum(f['monto_mensual'] for f in resultado['filas']), Decimal('100'))
        self.assertEqual(resultado.get('ciclos_facturacion', []), [])

    def test_periodo_referencial_no_cubre_factura_de_otro_proveedor(self):
        CfdiDescargado.objects.filter(pk=self.cfdi.pk).update(rfc_emisor='OTRO010101XXX')
        self.assertTrue(leer_gastos_mensuales(date(2026, 10, 1))['pendientes'])

    def test_factura_de_otro_receptor_no_acredita_ciclo(self):
        CfdiDescargado.objects.filter(pk=self.cfdi.pk).update(rfc_receptor='OTRO010101XXX')
        self.assertTrue(leer_gastos_mensuales(date(2026, 10, 1))['pendientes'])

    def test_dos_facturas_previas_no_permiten_inferir_un_ciclo_unico(self):
        self.gasto.pk = None
        self.gasto.save()
        self.assertTrue(leer_gastos_mensuales(date(2026, 10, 1))['pendientes'])

    def test_ciclo_no_muta_fuentes_y_no_se_aplica_a_otra_sucursal(self):
        from reportes.services_rentabilidad_mensual import costos_de_sucursal
        with CaptureQueriesContext(connection) as queries:
            resultado = leer_gastos_mensuales(date(2026, 10, 1))
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')) for q in queries))
        propio = costos_de_sucursal(resultado, self.sucursal.pk)
        otro = costos_de_sucursal(resultado, self.sucursal.pk + 100)
        self.assertIn('electricidad', propio['familias_cubiertas'])
        self.assertNotIn('electricidad', otro['familias_cubiertas'])
        self.assertEqual(propio['total'], 0)
        self.assertEqual(len(propio['ciclos_facturacion']), 1)
        self.assertNotIn('electricidad', propio['totales'])

    def test_html_json_csv_explican_ciclo_sin_incluirlo_en_gastos(self):
        self.client.force_login(get_user_model().objects.create_superuser('ciclo-cfe', password='test-only'))
        url = '/reportes/planeacion-personal/?mes=2026-10'
        self.assertContains(self.client.get(url), 'CFE: mes intermedio de facturación bimestral')
        self.assertContains(self.client.get(url + '&formato=csv'), 'Ciclo documentado')
        global_row = self.client.get(url + '&formato=json').json()['selected']['global']
        self.assertEqual(global_row['fuentes_gastos'], [])
        self.assertEqual(global_row['pendientes_gastos'], [])
        self.assertIsNone(global_row['gastos_total'])
        self.assertEqual(global_row['ciclos_facturacion'][0]['uuid'], self.cfdi.uuid)
