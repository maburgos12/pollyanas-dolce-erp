from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock

from django.test import SimpleTestCase

from reportes.services_costeo_etapas import asignar_parcial, leer_logistica


class RepartoParcialTests(SimpleTestCase):
    def test_no_absorbe_actividad_sin_atribuir(self):
        importes, pendiente = asignar_parcial(Decimal('100.01'), {1: Decimal('10'), 2: Decimal('20')}, Decimal('100'))
        self.assertEqual(importes, {1: Decimal('10.00'), 2: Decimal('20.00')})
        self.assertEqual(pendiente, Decimal('70.01'))
        self.assertEqual(sum(importes.values()) + pendiente, Decimal('100.01'))

    def test_base_excesiva_o_invalida_no_sobrecarga_fondo(self):
        for bases, total in [({1: Decimal('11')}, Decimal('10')), ({1: Decimal('-1')}, Decimal('10')), ({}, Decimal('0'))]:
            self.assertEqual(asignar_parcial(Decimal('100'), bases, total), ({}, Decimal('100')))


class LogisticaTests(SimpleTestCase):
    def calcular(self, rutas, turnos, nomina):
        t = 'reportes.services_costeo_etapas.'
        models = ['RutaEntrega', 'BitacoraSalidaLlegada', 'CargaCombustibleUnidad', 'Repartidor', 'NominaLinea', 'LineaPresupuestoMensual', 'RutaCargaChecklistLinea']
        mocks = {name: MagicMock() for name in models}
        mocks['RutaEntrega'].objects.filter.return_value.exclude.return_value.values.return_value = rutas
        mocks['BitacoraSalidaLlegada'].objects.filter.return_value.values.return_value = turnos
        mocks['CargaCombustibleUnidad'].objects.filter.return_value.values.return_value.annotate.return_value = [{'unidad_id': 1, 'monto': Decimal('100')}]
        mocks['Repartidor'].objects.filter.return_value.values_list.return_value = [(1, 11)]
        mocks['NominaLinea'].objects.filter.return_value.values.return_value = nomina
        mocks['LineaPresupuestoMensual'].objects.filter.return_value.values.return_value = [{'id': 1, 'rubro__concepto': 'Diesel', 'monto_real': Decimal('100'), 'fuente_real': 'AUTO:COMBUSTIBLE_UNIDAD', 'metadata': {}}]
        mocks['RutaCargaChecklistLinea'].objects.filter.return_value.values.return_value.annotate.return_value = []
        with patch.multiple('reportes.services_costeo_etapas', **mocks):
            return leer_logistica(date(2026, 9, 1), date(2026, 9, 30), {'personas': []})

    def datos(self):
        a = datetime(2026, 9, 1, 10, tzinfo=timezone.utc)
        ruta = {'id': 1, 'folio': 'R1', 'fecha_ruta': date(2026, 9, 1), 'estatus': 'COMPLETADA',
            'unidad_operativa_id': 1, 'repartidor_id': 1, 'acompanante_id': None, 'bitacora_salida_id': 1,
            'hora_inicio_real': a, 'hora_cierre_real': a + timedelta(hours=2)}
        turno = {'id': 1, 'folio': 'B1', 'unidad_id': 1, 'unidad__codigo': 'U1', 'repartidor_id': 1,
            'cerrada': True, 'km_salida': 100, 'km_llegada': 110}
        n = {'id': 1, 'empleado_id': 1, 'empleado__nombre': 'Prueba', 'empleado__puesto_operativo': 'REPARTIDOR',
            'empleado__usuario_erp_id': 11, 'empleado__departamento': 'VENTAS', 'empleado__sucursal_ref_id': 2,
            'departamento_snapshot': 'VENTAS', 'total_percepciones': Decimal('100'), 'horas_trabajadas': Decimal('10')}
        return ruta, turno, n

    def test_fondo_no_duplica_combustible_y_personal(self):
        r, t, n = self.datos()
        d = self.calcular([r], [t], [n])
        self.assertEqual(d['fondo'], Decimal('200'))
        self.assertEqual(d['asignado'], Decimal('120'))
        self.assertEqual(d['pendiente'], Decimal('80'))
        self.assertEqual(d['exclusiones'], {2: Decimal('100')})

    def test_odometro_faltante_no_redistribuye_a_otras_rutas(self):
        r, t, n = self.datos()
        incompleta = {**t, 'id': 2, 'folio': 'B2', 'km_llegada': None}
        d = self.calcular([r], [t, incompleta], [n])
        self.assertIsNone(d['rutas'][0]['combustible'])
        self.assertEqual(d['unidades'][0]['pendiente'], Decimal('100'))
        self.assertEqual(d['unidades'][0]['pendientes'], ['B2'])

    def test_duracion_anomala_permanece_pendiente(self):
        r, t, n = self.datos()
        r['hora_cierre_real'] = r['hora_inicio_real'] + timedelta(hours=26)
        d = self.calcular([r], [t], [n])
        self.assertEqual(d['asignado'], 0)
        self.assertEqual(d['pendiente'], 200)
        self.assertIsNone(d['rutas'][0]['nomina'])

    def test_solapamiento_no_cobra_doble_y_candidato_no_reclasifica(self):
        r, t, n = self.datos()
        d = self.calcular([r, {**r, 'id': 2}], [t], [n])
        self.assertEqual(d['asignado'], 0)
        n['empleado__puesto_operativo'] = ''
        d = self.calcular([r], [t], [n])
        self.assertEqual(d['personal'], [])
        self.assertEqual(d['exclusiones'], {})
        self.assertEqual(len(d['candidatos']), 1)


class VentaEtapasTests(SimpleTestCase):
    def test_fabricacion_sin_despacho_y_regla_venta_con_remanente(self):
        from types import SimpleNamespace
        from reportes.services_costeo_etapas import calcular_etapas
        branch = {'source': 'v2_fact', 'source_detail': 'fact', 'coverage_accepted': True,
                  'rows': [{'branch_id': 1, 'total_sales': Decimal('300')}]}
        product = {**branch, 'rows': [
            {'recipe_id': 7, 'product_name': 'Pastel', 'total_sales': Decimal('200'), 'total_quantity': Decimal('2')},
            {'recipe_id': None, 'product_name': 'Sin vínculo', 'total_sales': Decimal('100'), 'total_quantity': Decimal('1')}]}
        logistica = {'exclusiones': {1: Decimal('20')}}
        with patch('reportes.services_costeo_etapas.leer_logistica', return_value=logistica), \
             patch('reportes.services_costeo_etapas.leer_costos_mensuales', return_value={}), \
             patch('reportes.services_costeo_etapas.costos_de_sucursal', return_value={'total': Decimal('120.01')}), \
             patch('reportes.services_costeo_etapas.get_sales_range_grouped', side_effect=[branch, product]), \
             patch('reportes.services_costeo_etapas.Sucursal') as suc, \
             patch('reportes.services_costeo_etapas.ReglaAsignacionGasto') as regla, \
             patch('reportes.services_costeo_etapas.PointSalesDailyProductFact') as fact, \
             patch('reportes.services_costeo_etapas.SolicitudDomicilio') as solicitud:
            suc.objects.all.return_value.order_by.return_value = [SimpleNamespace(id=1, nombre='Prueba')]
            regla.objects.filter.return_value.values.return_value = [{'id': 1, 'base_reparto': 'VENTAS'}]
            fact.objects.filter.return_value.values.return_value.annotate.return_value = []
            solicitud.objects.filter.return_value.exclude.return_value.values.return_value = []
            out = calcular_etapas({'periodo': '2026-09', 'productos': [{'id': 7, 'fabricacion_servicios': Decimal('50'), 'con_despacho_servicios': Decimal('60')}]})
        s = out['sucursales'][0]
        self.assertEqual(s['fabricacion'], Decimal('100'))
        self.assertEqual(s['gasto_venta'], Decimal('100.01'))
        self.assertEqual(s['pendientes_producto'], 1)
        self.assertEqual(s['productos'][0]['gasto_venta'], Decimal('66.67'))
        self.assertEqual(s['productos'][0]['contribucion_parcial'], Decimal('33.33'))
        self.assertIsNone(s['productos'][1]['contribucion_parcial'])
        self.assertEqual(sum(p['gasto_venta'] for p in s['productos']) + s['venta_sin_asignar'], s['gasto_venta'])
