from decimal import Decimal
from io import BytesIO
from uuid import uuid4

from django.test import TestCase
from openpyxl import load_workbook

from conciliacion.services.importador import resumen_periodo_conciliacion
from conciliacion.views import _export_paquete_xlsx, _paquete_conciliacion
from sat_client.models import CfdiDescargado


class ComparativoFiscalTests(TestCase):
    def setUp(self):
        for direccion, tipo, total in [('emitido', 'I', 1000), ('emitido', 'N', 300),
                                      ('emitido', 'E', 100), ('emitido', 'P', 0),
                                      ('recibido', 'I', 700), ('recibido', 'E', 50)]:
            for estado in ['vigente', 'cancelado']:
                CfdiDescargado.objects.create(
                    uuid=str(uuid4()).upper(), rfc_emisor='GEF211230KR2',
                    rfc_receptor='XAXX010101000', subtotal=total, total=total,
                    tipo_cfdi=direccion, tipo_comprobante=tipo, estatus=estado,
                    forma_pago='03', fecha_emision='2026-05-16T10:00:00-07:00')

    def test_separa_ingresos_nomina_y_notas_sin_perder_inventario(self):
        r = resumen_periodo_conciliacion(year=2026, month=5)
        e = r['resumen_ejecutivo']
        for key, total in [('sat_ventas', 1000), ('sat_nomina', 300),
                           ('sat_notas_emitidas', 100), ('sat_ventas_netas', 900),
                           ('sat_proveedores', 700), ('sat_notas_recibidas', 50),
                           ('sat_compras_netas', 650)]:
            self.assertEqual(e[key], Decimal(total), key)
        self.assertEqual(e['diferencia_abonos_ventas'], Decimal('-900'))
        self.assertEqual(e['diferencia_cargos_proveedores'], Decimal('-650'))
        self.assertEqual(r['cfdi_emitidos']['conteo'], 4)
        self.assertEqual(sum(row['sat_total'] for row in r['canales_comparativo']), Decimal('1000'))
        proveedor = next(row for row in r['trabajo_conciliacion'] if row['concepto'].startswith('Pagos a proveedores'))
        self.assertEqual(proveedor['sat_total'], Decimal('650'))
        nomina = next(row for row in r['trabajo_conciliacion'] if row['concepto'].startswith('Nomina pagada'))
        self.assertEqual(nomina['sat_total'], Decimal('300'))

    def test_exportacion_conserva_la_misma_separacion(self):
        p = _paquete_conciliacion(year=2026, month=5)
        w = load_workbook(BytesIO(_export_paquete_xlsx(p).content), data_only=True)
        rows = dict(w['Comparativo_SAT'].iter_rows(min_row=2, values_only=True))
        self.assertEqual(rows['Ventas brutas (I)'], 1000)
        self.assertEqual(rows['Nomina timbrada (N)'], 300)
        self.assertEqual(rows['Notas de credito emitidas (E)'], 100)
        self.assertEqual(rows['Notas de credito recibidas (E)'], 50)
