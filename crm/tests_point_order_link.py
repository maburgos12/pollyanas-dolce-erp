from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from threading import Barrier
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import close_old_connections
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from crm.models import Cliente, DireccionCliente, PedidoCliente
from crm.services.point_order_link import LinkPointOrderCommand, link_point_note
from logistica.models import SolicitudDomicilio
from pos_bridge.services.point_note_detail_service import PointNote, PointNoteLine


def _note() -> PointNote:
    return PointNote(
        pk_nota="900001",
        folio="18452",
        branch_name="Matriz",
        sold_at=timezone.now(),
        total=Decimal("565.00"),
        invoiced=False,
        payment_type="CONTADO",
        point_channel="MOSTRADOR",
        lines=(
            PointNoteLine(
                point_code="P001",
                description="Pastel tres leches",
                quantity=Decimal("1"),
                unit_price=Decimal("550.00"),
                discount=Decimal("10.00"),
                line_total=Decimal("540.00"),
            ),
            PointNoteLine(
                point_code="V001",
                description="Velas",
                quantity=Decimal("1"),
                unit_price=Decimal("25.00"),
                discount=Decimal("0.00"),
                line_total=Decimal("25.00"),
            ),
        ),
        source_endpoint="/Clientes/get_detalle_nota/",
        customer_external_id="C-123",
    )


def _command(**overrides) -> LinkPointOrderCommand:
    data = {
        "pk_nota": "900001",
        "channel": PedidoCliente.CANAL_FACEBOOK,
        "customer_name": "María López",
        "customer_phone": "667 123 4567",
        "customer_email": "MARIA@EXAMPLE.COM",
        "address": "Av. Central 123",
        "references": "Portón blanco",
        "latitude": Decimal("25.790466"),
        "longitude": Decimal("-108.985886"),
        "place_id": "place-123",
        "social_reference": "facebook-thread-1",
        "delivery_window_start": timezone.now() + timedelta(hours=1),
        "delivery_window_end": timezone.now() + timedelta(hours=2),
        "instructions": "Tocar el timbre",
    }
    data.update(overrides)
    return LinkPointOrderCommand(**data)


class PointOrderLinkTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_user(
            username="call-center",
            password="unused",
        )

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_same_point_note_returns_same_order_and_single_delivery(self, _fetch):
        first = link_point_note(command=_command(), actor=self.actor)
        second = link_point_note(command=_command(), actor=self.actor)

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.order.pk, second.order.pk)
        self.assertEqual(first.delivery.pk, second.delivery.pk)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_customer_and_address_reused_by_existing_normalized_values(self, _fetch):
        customer = Cliente.objects.create(
            nombre="María",
            telefono="667 123 4567",
        )
        address = DireccionCliente.objects.create(
            cliente=customer,
            direccion="AV. CENTRAL 123",
        )

        result = link_point_note(
            command=_command(customer_phone="6671234567"),
            actor=self.actor,
        )

        self.assertEqual(result.order.cliente_id, customer.id)
        self.assertEqual(result.order.direccion_entrega_id, address.id)
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(DireccionCliente.objects.count(), 1)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_server_note_is_the_only_source_of_commercial_snapshot(self, _fetch):
        result = link_point_note(command=_command(), actor=self.actor)
        snapshot = result.order.point_note_snapshot

        self.assertEqual(result.order.point_note_id, "900001")
        self.assertEqual(result.order.point_note_folio, "18452")
        self.assertEqual(result.order.monto_estimado, Decimal("565.00"))
        self.assertEqual(snapshot["pk_nota"], "900001")
        self.assertEqual(snapshot["lines"][0]["point_code"], "P001")
        self.assertNotIn("customer_name", snapshot)
        self.assertNotIn("customer_phone", snapshot)
        self.assertEqual(result.order.created_by, self.actor)
        self.assertEqual(result.delivery.created_by, self.actor)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_instagram_channel_gps_window_and_instructions_are_persisted(self, _fetch):
        command = _command(
            channel=PedidoCliente.CANAL_INSTAGRAM,
            social_reference="instagram-dm-9",
        )

        result = link_point_note(command=command, actor=self.actor)

        self.assertEqual(result.order.canal, PedidoCliente.CANAL_INSTAGRAM)
        self.assertEqual(result.order.social_reference, "instagram-dm-9")
        self.assertEqual(result.delivery.canal_origen, PedidoCliente.CANAL_INSTAGRAM)
        self.assertEqual(result.delivery.ventana_inicio, command.delivery_window_start)
        self.assertEqual(result.delivery.ventana_fin, command.delivery_window_end)
        self.assertEqual(result.delivery.instrucciones_entrega, "Tocar el timbre")
        self.assertEqual(result.order.direccion_entrega.latitud, Decimal("25.790466"))

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_invalid_channel_gps_window_or_actor_is_rejected(self, _fetch):
        invalid_commands = (
            _command(channel="TIKTOK"),
            _command(channel=PedidoCliente.CANAL_OTRO, social_reference=""),
            _command(longitude=None),
            _command(latitude=Decimal("91"), longitude=Decimal("-108")),
            _command(
                delivery_window_start=timezone.now() + timedelta(hours=2),
                delivery_window_end=timezone.now() + timedelta(hours=1),
            ),
        )
        for command in invalid_commands:
            with self.subTest(command=command):
                with self.assertRaises(ValidationError):
                    link_point_note(command=command, actor=self.actor)

        with self.assertRaises(ValidationError):
            link_point_note(command=_command(), actor=None)
        self.assertEqual(PedidoCliente.objects.count(), 0)


class PointOrderLinkConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self.actor = get_user_model().objects.create_user(
            username="call-center-race",
            password="unused",
        )

    def test_two_postgresql_connections_return_same_winner_without_duplicates(self):
        ready = Barrier(2)

        def attempt():
            close_old_connections()
            try:
                ready.wait(timeout=10)
                with patch(
                    "crm.services.point_order_link.PointNoteDetailService.fetch",
                    return_value=_note(),
                ):
                    result = link_point_note(command=_command(), actor=self.actor)
                    return result.order.pk, result.delivery.pk
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = [future.result(timeout=20) for future in (executor.submit(attempt), executor.submit(attempt))]

        self.assertEqual(len({result[0] for result in results}), 1)
        self.assertEqual(len({result[1] for result in results}), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(DireccionCliente.objects.count(), 1)
