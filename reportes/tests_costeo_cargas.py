from datetime import date
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from .services_costeo_cargas import clasificar_detalle, leer_cargas_pagadas


class CargasCosteoTests(SimpleTestCase):
    def detalle(self, **extra):
        datos = dict(empleado=SimpleNamespace(pk=35, puesto_operativo=''),
                     area_codigo='gastos-venta', sucursal_id=1, sucursal=SimpleNamespace(nombre='Matriz'), cuota_patronal=D('100'))
        datos.update(extra)
        return SimpleNamespace(**datos)

    def test_supervisor_ventas_no_es_logistica_por_conducir(self):
        self.assertEqual(clasificar_detalle(self.detalle(), {}), ('gastos-venta', 1))
        self.assertEqual(clasificar_detalle(self.detalle(), {35: 'Coordinación de despacho'}), ('logistica', None))
        self.assertEqual(clasificar_detalle(self.detalle(empleado=None), {}), ('sin_asignar', None))

    def test_no_infiere_fecha_de_pago_en_otros_meses(self):
        self.assertFalse(leer_cargas_pagadas(date(2026, 10, 1), [])['confirmado'])

    def test_exp_aplicado_no_duplica_componentes_y_rechaza_desfase(self):
        detalle = self.detalle(retiro=D('20'), cesantia_patronal=D('30'), aportacion_vivienda=D('50'))
        doc = SimpleNamespace(clase='SUA_XLS', archivo=SimpleNamespace(name='sua.xls'), detalles=MagicMock())
        doc.detalles.all.return_value = [detalle]
        exp = SimpleNamespace(pk=1, tipo='BIMESTRAL', total_patronal=D('100'), registro_patronal='E52-40157-10-0', documentos=MagicMock())
        exp.documentos.all.return_value = [doc]
        with patch('reportes.services_costeo_cargas.ExpedienteCedulaIMSS') as imss, patch('reportes.services_costeo_cargas.ExpedienteISN') as isn:
            imss.objects.filter.return_value.prefetch_related.return_value = [exp]
            isn.objects.filter.return_value.select_related.return_value.prefetch_related.return_value = []
            d = leer_cargas_pagadas(date(2026, 9, 1), [])
            self.assertEqual(d['total'], D('100'))
            self.assertEqual(sum(f['monto_mensual'] for f in d['filas']), D('100'))
            self.assertEqual(d['filas'][0]['cobertura_inicio'], date(2026, 8, 1))
            self.assertEqual(d['filas'][0]['periodo_pago'], date(2026, 9, 1))
            exp.total_patronal = D('101')
            d = leer_cargas_pagadas(date(2026, 9, 1), [])
            self.assertEqual(d['filas'], [])
            self.assertTrue(any('no concilia' in p for p in d['pendientes']))


from django.test import TestCase
from .models import FactVentaDiaria
from ventas.services.sales_read_service import _grouped_rows_from_analytic_facts


class IdentidadCosteoVentasTests(TestCase):
    def test_no_inventa_codigo_para_dos_articulos_con_mismo_nombre(self):
        for codigo in ('1005', '1006'):
            FactVentaDiaria.objects.create(fecha=date(2026, 9, 1), producto_clave=codigo,
                producto_nombre='Mismo nombre', source_kind='AUTHORITATIVE', venta_total=100, cantidad=2)
        filas = _grouped_rows_from_analytic_facts(start_date=date(2026, 9, 1), end_date=date(2026, 9, 30),
            dimension='product', branch_ids=None, source='authoritative')
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]['total_sales'], D('200'))
        self.assertIsNone(filas[0]['product_code'])
        FactVentaDiaria.objects.filter(producto_clave='1006').delete()
        filas = _grouped_rows_from_analytic_facts(start_date=date(2026, 9, 1), end_date=date(2026, 9, 30),
            dimension='product', branch_ids=None, source='authoritative')
        self.assertEqual(filas[0]['product_code'], '1005')
