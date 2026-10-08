from copy import deepcopy
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import TestCase

from crm.models import Cliente, DireccionCliente, PedidoCliente, PointOrderLink
from crm.services.point_document_link import has_verified_point, link_web_point, refresh_special_links
from logistica.models import SolicitudDomicilio


class PointDocumentLinkTests(TestCase):
    def setUp(self):
        customer = Cliente.objects.create(nombre='Compra WEB', telefono='6671260617')
        address = DireccionCliente.objects.create(cliente=customer, direccion='Calle WEB', latitud=25, longitud=-108)
        self.order = PedidoCliente.objects.create(cliente=customer, direccion_entrega=address,
            external_source='POLLYANAS_ECOMMERCE', external_id='ECOMMERCE:WEB-1', canal='WEB',
            fecha_compromiso=date(2026, 10, 9), monto_estimado=Decimal('540'), payload_snapshot={'original': True})
        self.delivery = SolicitudDomicilio.objects.create(pedido_cliente=self.order, cliente=customer,
            direccion_cliente=address, cliente_nombre=customer.nombre, cliente_telefono=customer.telefono,
            direccion=address.direccion, canal_origen='WEB')
        self.snapshot = {'pk_pedido': '17118', 'folio': '00428', 'sucursal': 'Matriz',
            'fecha_entrega': '2026-10-09T12:30:19-07:00', 'total': '540', 'restante': '0',
            'cancelado': False, 'lines': [], 'nota_final': None}

    def link(self, **overrides):
        data = dict(order=self.order, kind='SPECIAL', point_id='17118',
                    delivery_date=date(2026, 10, 9), actor=None, snapshot=self.snapshot)
        data.update(overrides)
        return link_web_point(**data)

    def test_special_preserves_web_identity_customer_snapshot_and_single_delivery(self):
        self.link()
        self.link()
        self.assertEqual(PointOrderLink.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)
        self.order.refresh_from_db()
        self.delivery.refresh_from_db()
        self.assertEqual(self.order.external_id, 'ECOMMERCE:WEB-1')
        self.assertEqual(self.order.payload_snapshot, {'original': True})
        self.assertEqual(self.order.cliente.telefono, '6671260617')
        self.assertEqual(self.order.point_note_id, '')
        self.assertEqual(self.delivery.ventana_inicio.isoformat(), '2026-10-09T19:30:19+00:00')
        self.assertTrue(has_verified_point(self.order))
        self.assertEqual(self.delivery.estatus, SolicitudDomicilio.ESTATUS_PENDIENTE_POINT)

    def test_wrong_amount_date_unpaid_cancelled_and_identity_are_rejected(self):
        for delta in ({'total': '541'}, {'restante': '1'}, {'cancelado': True},
                      {'pk_pedido': '17114'}, {'fecha_entrega': '2026-10-08T12:30:19-07:00'}):
            with self.subTest(delta=delta), self.assertRaises(ValidationError):
                self.link(snapshot={**self.snapshot, **delta})
        self.assertEqual(PointOrderLink.objects.count(), 0)

    def test_document_cannot_be_linked_to_another_web_order(self):
        self.link()
        other = PedidoCliente.objects.create(cliente=self.order.cliente, external_source='POLLYANAS_ECOMMERCE',
            external_id='ECOMMERCE:WEB-2', canal='WEB', fecha_compromiso=date(2026, 10, 9), monto_estimado=540)
        with self.assertRaises(ValidationError):
            self.link(order=other)

    def test_final_ticket_attaches_to_same_web_order_and_intake_excludes_it(self):
        link = self.link()
        fresh = deepcopy(self.snapshot)
        fresh['nota_final'] = {'pk_nota': '99001', 'folio': '104422', 'total': '540', 'lines': []}
        with patch('crm.services.point_document_link.PointSpecialOrderService') as service:
            service.return_value.fetch.return_value = fresh
            self.assertEqual(refresh_special_links(), 0)
        self.order.refresh_from_db()
        self.assertEqual(self.order.point_note_id, '99001')
        self.assertEqual(self.order.point_note_folio, '104422')
        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.estatus, SolicitudDomicilio.ESTATUS_CONFIRMADO)
        self.assertEqual(self.delivery.revision, 1)
        self.assertEqual(self.order.point_order_link.snapshot, self.snapshot)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertTrue(PedidoCliente.objects.exclude(point_note_id='').filter(pk=self.order.pk).exists())

    def test_final_note_confirms_once_on_repeated_link(self):
        snapshot = {'pk_nota': '99002', 'folio': '104423', 'total': '540',
                    'sold_at': '2026-10-09T12:30:19-07:00', 'lines': []}
        self.link(kind='NOTE', point_id='99002', snapshot=snapshot)
        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.estatus, SolicitudDomicilio.ESTATUS_CONFIRMADO)
        self.assertEqual(self.delivery.revision, 1)
        self.link(kind='NOTE', point_id='99002', snapshot=snapshot)
        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.revision, 1)

    def test_repeated_link_repairs_existing_pending_delivery(self):
        snapshot = {'pk_nota': '99002', 'folio': '104423', 'total': '540',
                    'sold_at': '2026-10-09T12:30:19-07:00', 'lines': []}
        # Reproduce a document linked before status confirmation was implemented.
        with patch('crm.services.point_document_link._confirm_final_note_delivery'):
            self.link(kind='NOTE', point_id='99002', snapshot=snapshot)
        self.link(kind='NOTE', point_id='99002', snapshot=snapshot)
        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.estatus, SolicitudDomicilio.ESTATUS_CONFIRMADO)
        self.assertEqual(self.delivery.revision, 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)

    def test_final_note_does_not_reopen_cancelled_delivery(self):
        self.delivery.estatus = SolicitudDomicilio.ESTATUS_CANCELADO
        self.delivery.cancelacion_motivo = 'Cancelado por cliente'
        self.delivery.save(update_fields=['estatus', 'cancelacion_motivo'])
        snapshot = {'pk_nota': '99002', 'folio': '104423', 'total': '540',
                    'sold_at': '2026-10-09T12:30:19-07:00', 'lines': []}
        self.link(kind='NOTE', point_id='99002', snapshot=snapshot)
        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.estatus, SolicitudDomicilio.ESTATUS_CANCELADO)

    def test_inactive_link_does_not_confirm_existing_pending_delivery(self):
        snapshot = {'pk_nota': '99002', 'folio': '104423', 'total': '540',
                    'sold_at': '2026-10-09T12:30:19-07:00', 'lines': []}
        with patch('crm.services.point_document_link._confirm_final_note_delivery'):
            link = self.link(kind='NOTE', point_id='99002', snapshot=snapshot)
        link.is_active = False
        link.save(update_fields=['is_active'])
        self.link(kind='NOTE', point_id='99002', snapshot=snapshot)
        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.estatus, SolicitudDomicilio.ESTATUS_PENDIENTE_POINT)

    def test_failed_verification_blocks_dispatch_without_mutating_source(self):
        self.link()
        with patch('crm.services.point_document_link.PointSpecialOrderService') as service:
            service.return_value.fetch.side_effect = RuntimeError('Point unavailable')
            self.assertEqual(refresh_special_links(), 1)
        self.order.refresh_from_db()
        self.assertFalse(has_verified_point(self.order))
        self.assertEqual(self.order.point_order_link.snapshot, self.snapshot)


class WebPointLinkApiTests(PointDocumentLinkTests):
    def setUp(self):
        super().setUp()
        from integraciones.models import PublicApiClient
        from rest_framework.test import APIClient
        self.owner, self.key = PublicApiClient.create_with_generated_key(nombre='WEB propietario')
        self.owner.capabilities = ['OMNICHANNEL']
        self.owner.save(update_fields=['capabilities'])
        self.order.public_api_client = self.owner
        self.order.save(update_fields=['public_api_client'])
        self.client = APIClient()
        self.url = f'/api/public/v1/omnichannel/deliveries/{self.delivery.pk}/point-link/'
        self.data = {'external_id': self.order.external_id, 'kind': 'SPECIAL', 'point_id': '17118', 'fecha': '2026-10-09'}

    def test_link_requires_auth_capability_and_original_owned_web_identity(self):
        from integraciones.models import PublicApiClient
        self.assertEqual(self.client.post(self.url, self.data, format='json').status_code, 401)
        foreign, key = PublicApiClient.create_with_generated_key(nombre='Otro propietario')
        self.assertEqual(self.client.post(self.url, self.data, format='json', HTTP_X_API_KEY=key).status_code, 403)
        foreign.capabilities = ['OMNICHANNEL']
        foreign.save(update_fields=['capabilities'])
        with patch('pos_bridge.services.point_special_order_service.PointSpecialOrderService.fetch') as fetch:
            self.assertEqual(self.client.post(self.url, self.data, format='json', HTTP_X_API_KEY=key).status_code, 404)
            self.assertEqual(self.client.post(self.url, {**self.data, 'external_id': 'ECOMMERCE:OTHER'}, format='json', HTTP_X_API_KEY=self.key).status_code, 409)
        fetch.assert_not_called()

    def test_link_returns_special_products_and_preserves_source_contract(self):
        with patch('pos_bridge.services.point_special_order_service.PointSpecialOrderService.fetch', return_value=self.snapshot):
            response = self.client.post(self.url, self.data, format='json', HTTP_X_API_KEY=self.key)
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data['fuente']['tipo'], 'POLLYANAS_ECOMMERCE')
        self.assertEqual(response.data['point_link']['folio'], '00428')
        self.assertIsNone(response.data['point_link']['folio_final'])
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)
