from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from .services_cedula_pago import leer_pago_conciliado
from .services_costeo_cargas import _leer_cargas_conciliadas


class PagoConciliadoTests(SimpleTestCase):
    def setUp(self):
        self.data = dict(version=1, importe_pagado='82151.56', gasto_empresa='81001.59',
            retenciones='1149.97', cuentas_por_cobrar='0.00', fecha_pago='2025-09-17',
            soporte_pago_sha256='a' * 64, soporte_conciliacion_sha256='b' * 64,
            sua_sha256='c' * 64)
        docs = Mock()
        docs.all.return_value = [SimpleNamespace(clase='SUA_PDF', sha256='c' * 64)]
        self.exp = SimpleNamespace(pk=1, estado='APLICADO', periodo=date(2025, 8, 1),
            total_patronal=Decimal('74988.34'), metadata={'pago_conciliado': self.data}, documentos=docs)

    def test_retenciones_no_se_suman_al_gasto(self):
        result = leer_pago_conciliado(self.exp)
        self.assertEqual(result['gasto_empresa'], Decimal('81001.59'))
        self.assertEqual(result['fecha_pago'], date(2025, 9, 17))

    def test_control_ausente_no_inventa_pago(self):
        self.exp.metadata = {}
        self.assertIsNone(leer_pago_conciliado(self.exp))

    def test_control_desbalanceado_no_regresa_total_patronal_como_fallback(self):
        self.data['gasto_empresa'] = '81004.36'
        with self.assertRaisesMessage(ValueError, 'no concilia'):
            leer_pago_conciliado(self.exp)

    def test_no_admite_otro_sua_ni_dos_fuentes(self):
        self.data['sua_sha256'] = 'd' * 64
        with self.assertRaisesMessage(ValueError, 'SUA único'):
            leer_pago_conciliado(self.exp)
        self.data['sua_sha256'] = 'c' * 64
        self.exp.documentos.all.return_value *= 2
        with self.assertRaisesMessage(ValueError, 'SUA único'):
            leer_pago_conciliado(self.exp)

    def test_no_admite_importes_invalidos_o_exp_reemplazado(self):
        for value in ('NaN', 'Infinity', '-1', '82151.561'):
            with self.subTest(value=value):
                self.data['importe_pagado'] = value
                with self.assertRaises(ValueError):
                    leer_pago_conciliado(self.exp)
        self.data['importe_pagado'] = '82151.56'
        self.exp.estado = 'REEMPLAZADO'
        with self.assertRaises(ValueError):
            leer_pago_conciliado(self.exp)

    def test_costeo_conserva_control_y_no_infiere_sucursal_del_empleado(self):
        self.exp.registro_patronal = 'E5240157100'
        self.exp.tipo = 'MENSUAL'
        doc = self.exp.documentos.all.return_value[0]
        doc.detalles = Mock()
        doc.detalles.all.return_value = [SimpleNamespace(cuota_patronal=Decimal('74988.34'),
            area_codigo='produccion', sucursal_id=None)]
        result = _leer_cargas_conciliadas(date(2025, 9, 1), [self.exp])
        self.assertEqual(result['total'], Decimal('81001.59'))
        self.assertEqual(sum(r['monto_mensual'] for r in result['filas']), result['total'])
        self.assertEqual({r['area']: r['monto_mensual'] for r in result['filas']},
            {'produccion': Decimal('74988.34'), 'sin_asignar': Decimal('6013.25')})
        duplicated = _leer_cargas_conciliadas(date(2025, 9, 1), [self.exp, self.exp])
        self.assertEqual(duplicated['total'], 0)
        self.assertFalse(duplicated['confirmado'])
