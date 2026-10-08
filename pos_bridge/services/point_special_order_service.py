from decimal import Decimal
from urllib.parse import urljoin

import requests
from django.utils import timezone

from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.point_note_detail_service import (
    PointNoteContractError, PointNoteDetailService, PointNoteUnavailableError,
)
from pos_bridge.services.point_ticket_threshold_service import PointTicketThresholdService
from pos_bridge.utils.helpers import normalize_text


class PointSpecialOrderService(PointNoteDetailService):
    MONTHS = ('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
              'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre')

    @classmethod
    def _point_date(cls, day):
        return f'{day.day:02d}/{cls.MONTHS[day.month - 1]}/{day.year}'

    def _get(self, session, path, params):
        try:
            response = session.get(urljoin(self.settings.base_url, path), params=params,
                                   timeout=self.settings.timeout_ms / 1000)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            raise PointNoteUnavailableError('Point no respondió la consulta del pedido especial.') from exc
        except ValueError as exc:
            raise PointNoteContractError('Point devolvió JSON inválido.') from exc

    def _headers(self, session, delivery_date):
        payload = self._get(session, '/Pedidos/getPedidos', {
            'fechaInicio': self._point_date(delivery_date),
            'fechaFin': self._point_date(delivery_date),
            'sucursal': 'null', 'plaza': 'null', 'status': 'null',
        })
        if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
            raise PointNoteContractError('Point devolvió una lista inválida de pedidos especiales.')
        return payload

    def search(self, *, folio, branch, delivery_date):
        if not str(folio).isdigit():
            raise PointNoteContractError('El folio especial debe ser numérico.')
        with point_account_session_lock(wait=False) as acquired:
            if not acquired:
                raise PointNoteUnavailableError('La cuenta Point está ocupada; vuelve a consultar.')
            auth = self.http_session_service.create()
            try:
                return [self._fetch(auth.session, str(row['PK_Pedido']), delivery_date, row)
                        for row in self._headers(auth.session, delivery_date)
                        if str(row.get('LPK_Pedido', '')).isdigit()
                        and int(row['LPK_Pedido']) == int(folio)
                        and normalize_text(row.get('Sucursal', '')) == normalize_text(branch)]
            finally:
                auth.session.close()

    def fetch(self, *, pk_pedido, delivery_date):
        with point_account_session_lock(wait=False) as acquired:
            if not acquired:
                raise PointNoteUnavailableError('La cuenta Point está ocupada; vuelve a consultar.')
            auth = self.http_session_service.create()
            try:
                rows = [row for row in self._headers(auth.session, delivery_date)
                        if str(row.get('PK_Pedido')) == str(pk_pedido)]
                if len(rows) != 1:
                    raise PointNoteContractError('No se encontró el pedido especial en su fecha de entrega.')
                return self._fetch(auth.session, str(pk_pedido), delivery_date, rows[0])
            finally:
                auth.session.close()

    def _fetch(self, session, pk_pedido, delivery_date, header):
        row = self._get(session, '/Pedidos/GetById', {'pkPedido': pk_pedido})
        if not isinstance(row, dict):
            raise PointNoteContractError('Point devolvió un detalle especial inválido.')
        self._require_fields(row, ('PK_Pedido', 'LPK_Pedido', 'Fecha_Entrega', 'Fecha_Hora',
                                  'Sucursal', 'Monto_Total', 'Monto_Pagado', 'Monto_Restante',
                                  'Cancelado', 'Status', 'Detalle'), label='pedido especial')
        if str(row['PK_Pedido']) != pk_pedido or str(row['LPK_Pedido']) != str(header['LPK_Pedido']):
            raise PointNoteContractError('Las identidades del pedido especial no coinciden.')
        scheduled = self._datetime(row['Fecha_Entrega'], field='Fecha_Entrega')
        if timezone.localtime(scheduled).date() != delivery_date:
            raise PointNoteContractError('La fecha de entrega Point no coincide con la consultada.')
        if normalize_text(row['Sucursal']) != normalize_text(header['Sucursal']):
            raise PointNoteContractError('La sucursal del pedido especial no coincide.')
        total = self._money(row['Monto_Total'], field='Monto_Total')
        paid = self._money(row['Monto_Pagado'], field='Monto_Pagado')
        remaining = self._money(row['Monto_Restante'], field='Monto_Restante')
        if not isinstance(row['Detalle'], list) or not row['Detalle']:
            raise PointNoteContractError('El pedido especial no tiene productos válidos.')
        lines = []
        for line in row['Detalle']:
            self._require_fields(line, ('Codigo', 'Producto', 'Cantidad', 'PrecioUnitario', 'Importe'), label='producto especial')
            lines.append({'point_code': str(line['Codigo']), 'description': str(line['Producto']),
                          'quantity': str(self._decimal(line['Cantidad'], field='Cantidad', minimum=Decimal('0'))),
                          'unit_price': str(self._money(line['PrecioUnitario'], field='PrecioUnitario')),
                          'discount': str(self._money(line.get('DescuentoTotal', 0), field='DescuentoTotal')),
                          'line_total': str(self._money(line['Importe'], field='Importe'))})
        if sum(Decimal(line['line_total']) for line in lines) != total or paid + remaining != total:
            raise PointNoteContractError('Los importes del pedido especial no concilian.')
        result = {'pk_pedido': pk_pedido, 'folio': str(row['LPK_Pedido']).zfill(5),
                  'sucursal': row['Sucursal'], 'fecha_entrega': scheduled.isoformat(),
                  'cliente': str(row.get('Cliente', '')), 'total': str(total), 'pagado': str(paid),
                  'restante': str(remaining), 'cancelado': self._boolean(row['Cancelado'], field='Cancelado'),
                  'estado': str(row.get('StatusDescription', '')), 'lines': lines,
                  'source_endpoint': '/Pedidos/GetById', 'nota_final': None}
        final_folio = header.get('LPK_Nota')
        if final_folio and int(row['Status']) in (5, 6) and not result['cancelado']:
            result['nota_final'] = self._final_note(session, final_folio, row)
        return result

    def _final_note(self, session, folio, special):
        report = PointTicketThresholdService(self.settings, self.http_session_service)
        created = self._datetime(special['Fecha_Hora'], field='Fecha_Hora')
        payload = self._get(session, report.NOTES_BY_PLAZA_PATH, report._build_params(
            start_date=timezone.localtime(created).date(), end_date=timezone.localdate()))
        if not isinstance(payload, list):
            raise PointNoteContractError('Point devolvió un reporte de notas inválido.')
        from crm.services.point_order_link import canonical_point_ticket_folio
        matches = [row for row in payload if isinstance(row, dict)
                   and canonical_point_ticket_folio(str(row.get('FOLIO', ''))).isdigit()
                   and int(canonical_point_ticket_folio(str(row.get('FOLIO', '')))) == int(folio)
                   and normalize_text(row.get('SUCURSAL', '')) == normalize_text(special['Sucursal'])]
        if len(matches) != 1:
            raise PointNoteContractError('La nota final del pedido especial no tiene una identidad única.')
        pk_nota = str(matches[0].get('PK_NOTA') or '')
        note = self.fetch_with_session(session, pk_nota=pk_nota)
        if note.total != self._money(special['Monto_Total'], field='Monto_Total'):
            raise PointNoteContractError('El total de la nota final difiere del pedido especial.')
        from crm.services.point_order_link import _note_snapshot
        return _note_snapshot(note)
