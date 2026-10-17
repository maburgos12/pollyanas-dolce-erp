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

    def test_usuario_erp_ausente_no_coincide_con_acompanante_vacio(self):
        r, t, n = self.datos()
        n['empleado__usuario_erp_id'] = None
        d = self.calcular([r], [t], [n])
        self.assertIsNone(d['rutas'][0]['nomina'])
        self.assertEqual(d['personal'][0]['pendiente'], Decimal('100'))

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
        self.assertEqual(d['candidatos'], [])


class VentaEtapasTests(SimpleTestCase):
    def test_fabricacion_sin_despacho_y_regla_venta_con_remanente(self):
        from types import SimpleNamespace
        from reportes.services_costeo_etapas import calcular_etapas
        branch = {'source': 'v2_fact', 'source_detail': 'fact', 'coverage_accepted': True,
                  'rows': [{'branch_id': 1, 'total_sales': Decimal('300')}]}
        product = {**branch, 'rows': [
            {'recipe_id': 7, 'product_name': 'Pastel', 'total_sales': Decimal('200'), 'total_quantity': Decimal('2')},
            {'recipe_id': None, 'product_name': 'Sin vínculo', 'total_sales': Decimal('100'), 'total_quantity': Decimal('1')}]}
        logistica = {'exclusiones': {1: Decimal('20')}, 'fondo': Decimal('20'), 'pendiente': Decimal('20')}
        with patch('reportes.services_costeo_etapas.leer_logistica', return_value=logistica), \
             patch('reportes.services_costeo_etapas.leer_reparto_logistico', return_value={'destinos': []}), \
             patch('reportes.services_costeo_etapas.leer_gastos_mensuales', return_value={'filas': [], 'pendientes': []}), \
             patch('reportes.services_costeo_etapas.leer_personal_mensual', return_value={'filas': [], 'pendientes': []}), \
             patch('reportes.services_costeo_etapas.leer_cargas_pagadas', return_value={'confirmado': False, 'filas': []}), \
             patch('reportes.services_costeo_etapas.RecetaCostoHistoricoMensual'), \
             patch('reportes.services_costeo_etapas.ProductoReventaCostoHistoricoMensual'), \
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
            out = calcular_etapas({'periodo': '2026-09', 'personas': [], 'base': Decimal('100'), 'fondos': [], 'servicios': {'fondo': Decimal('0')}, 'pendiente_reparto': Decimal('0'), 'productos': [{'id': 7, 'mp': Decimal('50'), 'fabricacion_servicios': Decimal('50'), 'con_despacho_servicios': Decimal('60')}]})
        s = out['sucursales'][0]
        self.assertEqual(s['fabricacion'], Decimal('100'))
        self.assertEqual(s['gasto_venta'], Decimal('100.01'))
        self.assertEqual(s['pendientes_producto'], 1)
        self.assertEqual(s['productos'][0]['gasto_venta'], Decimal('66.67'))
        self.assertEqual(s['productos'][0]['contribucion_parcial'], Decimal('33.33'))
        self.assertIsNone(s['productos'][1]['contribucion_parcial'])
        self.assertEqual(sum(p['gasto_venta'] for p in s['productos']) + s['venta_sin_asignar'], s['gasto_venta'])


class CostosExistentesTests(SimpleTestCase):
    def test_receta_fuera_de_produccion_y_reventa_no_pagan_dos_veces_elaboracion(self):
        from types import SimpleNamespace as S
        from reportes.services_costeo_etapas import calcular_etapas
        nombres = ['leer_reparto_logistico', 'leer_logistica', 'leer_gastos_mensuales', 'leer_personal_mensual', 'leer_cargas_pagadas',
            'get_sales_range_grouped', 'Sucursal', 'ReglaAsignacionGasto', 'PointSalesDailyProductFact',
            'SolicitudDomicilio', 'RecetaCostoHistoricoMensual', 'ProductoReventaCostoHistoricoMensual']
        m = {n: MagicMock() for n in nombres}
        m['leer_reparto_logistico'].return_value = {'destinos': []}
        m['leer_logistica'].return_value = {'exclusiones': {}, 'fondo': Decimal(0), 'pendiente': Decimal(0)}
        for n in ['leer_gastos_mensuales', 'leer_personal_mensual']:
            m[n].return_value = {'filas': [], 'pendientes': []}
        m['leer_cargas_pagadas'].return_value = {'confirmado': False, 'filas': []}
        m['Sucursal'].objects.all.return_value.order_by.return_value = [S(id=1, nombre='Sucursal')]
        m['ReglaAsignacionGasto'].objects.filter.return_value.values.return_value = [{'id': 1, 'base_reparto': 'VENTAS'}]
        m['RecetaCostoHistoricoMensual'].objects.filter.return_value.select_related.return_value = [
            S(receta_id=7, receta=S(rendimiento_unidad=None, rendimiento_cantidad=None),
              costo_por_unidad_rendimiento=Decimal(10), costo_total=Decimal(10))]
        m['ProductoReventaCostoHistoricoMensual'].objects.filter.return_value.select_related.return_value = [
            S(producto_point_id=9, producto_point=S(sku='1005'), costo_promedio=Decimal(3))]
        base = {'coverage_accepted': True, 'source': 'authoritative', 'source_detail': 'Point'}
        m['get_sales_range_grouped'].side_effect = [
            {**base, 'rows': [{'branch_id': 1, 'total_sales': Decimal(100)}]},
            {**base, 'rows': [
                {'recipe_id': 7, 'total_quantity': Decimal(2), 'total_sales': Decimal(80)},
                {'recipe_id': None, 'product_code': '1005', 'total_quantity': Decimal(2), 'total_sales': Decimal(20)}]}]
        mo = {'periodo': '2026-09', 'personas': [], 'productos': [], 'base': Decimal(100),
              'fondos': [{'nombre': 'Elaboración y apoyo', 'monto': Decimal(50)},
                         {'nombre': 'Coordinación de despacho', 'monto': Decimal(90)}],
              'servicios': {'fondo': Decimal(10)}, 'pendiente_reparto': 0}
        with patch.multiple('reportes.services_costeo_etapas', **m):
            d = calcular_etapas(mo)
        filas = d['sucursales'][0]['productos']
        self.assertEqual(filas[0]['fabricacion_unitaria'], Decimal(16))
        self.assertEqual(filas[1]['fabricacion_unitaria'], Decimal(3))
        self.assertEqual(d['sucursales'][0]['fabricacion'], Decimal(38))
