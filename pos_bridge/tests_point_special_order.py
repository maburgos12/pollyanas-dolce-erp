from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from pos_bridge.services.point_note_detail_service import PointNoteContractError
from pos_bridge.services.point_special_order_service import PointSpecialOrderService


class PointSpecialOrderContractTests(SimpleTestCase):
    def setUp(self):
        self.service = PointSpecialOrderService(bridge_settings=SimpleNamespace(base_url='https://point.example', timeout_ms=1000), http_session_service=Mock())
        self.header = {'PK_Pedido': 17114, 'LPK_Pedido': 426, 'Sucursal': 'Matriz'}
        self.row = {'PK_Pedido': 17114, 'LPK_Pedido': 426, 'Sucursal': 'Matriz',
            'Fecha_Hora': '2026-10-05T16:40:14', 'Fecha_Entrega': '2026-10-08T14:00:21',
            'Monto_Total': 254, 'Monto_Pagado': 254, 'Monto_Restante': 0,
            'Cancelado': False, 'Status': 0, 'StatusDescription': 'En espera',
            'Detalle': [{'Codigo': '0160', 'Producto': 'Bollo Lotus', 'Cantidad': 6,
                         'PrecioUnitario': 39, 'Importe': 234},
                        {'Codigo': '0306', 'Producto': 'Servicio a domicilio', 'Cantidad': 1,
                         'PrecioUnitario': 20, 'Importe': 20}]}

    def test_special_identity_folio_and_programming_are_distinct_from_final_note(self):
        with patch.object(self.service, '_get', return_value=self.row):
            result = self.service._fetch(Mock(), '17114', date(2026, 10, 8), self.header)
        self.assertEqual(result['folio'], '00426')
        self.assertEqual(result['pk_pedido'], '17114')
        self.assertEqual(result['fecha_entrega'], '2026-10-08T14:00:21-07:00')
        self.assertIsNone(result['nota_final'])
        self.assertEqual(result['total'], '254.00')

    def test_malformed_money_wrong_identity_or_date_fail_closed(self):
        for delta in ({'PK_Pedido': 17118}, {'Monto_Total': 255}, {'Monto_Pagado': 253},
                      {'Fecha_Entrega': '2026-10-09T14:00:21'}, {'Cancelado': 'unknown'}):
            with self.subTest(delta=delta), patch.object(self.service, '_get', return_value={**self.row, **delta}), self.assertRaises(PointNoteContractError):
                self.service._fetch(Mock(), '17114', date(2026, 10, 8), self.header)

    def test_no_final_ticket_is_inferred_without_explicit_lpk_nota(self):
        with patch.object(self.service, '_get', return_value={**self.row, 'Status': 5}), \
             patch.object(self.service, '_final_note') as final:
            self.service._fetch(Mock(), '17114', date(2026, 10, 8), self.header)
        final.assert_not_called()

    def test_uppercase_report_prefix_resolves_only_explicit_final_note(self):
        from crm.tests_point_order_link import _note
        from dataclasses import replace
        from decimal import Decimal
        note = replace(_note(), total=Decimal('254'), pk_nota='911200', folio='104422')
        with patch.object(self.service, '_get', return_value=[{'FOLIO': 'NOTA 104422', 'PK_NOTA': '911200', 'SUCURSAL': 'Matriz'}]), \
             patch.object(self.service, 'fetch_with_session', return_value=note) as fetch:
            result = self.service._final_note(Mock(), 104422, self.row)
        self.assertEqual(result['pk_nota'], '911200')
        self.assertEqual(fetch.call_args.kwargs, {'pk_nota': '911200'})
