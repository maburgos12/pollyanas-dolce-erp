from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone as dt_timezone
from decimal import Decimal
from importlib import import_module
from threading import Barrier
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db import close_old_connections
from django.db import IntegrityError
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone

from crm.models import Cliente, DireccionCliente, PedidoCliente
from crm.services.point_order_link import (
    LinkPointOrderCommand,
    link_point_note,
    point_link_fingerprint,
)
from logistica.models import SolicitudDomicilio
from pos_bridge.services.point_note_detail_service import PointNote, PointNoteLine


_DEFAULT_WINDOW_START = datetime(2030, 1, 15, 18, 0, tzinfo=dt_timezone.utc)


def _note(*, pk_nota="900001", folio="18452") -> PointNote:
    return PointNote(
        pk_nota=pk_nota,
        folio=folio,
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
        "delivery_window_start": _DEFAULT_WINDOW_START,
        "delivery_window_end": _DEFAULT_WINDOW_START + timedelta(hours=1),
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
    def test_fingerprint_rotation_accepts_old_primary_only_as_configured_fallback(
        self,
        _fetch,
    ):
        command = _command()
        with override_settings(POINT_LINK_FINGERPRINT_KEY="key-A"):
            first = link_point_note(command=command, actor=self.actor)
            fingerprint_a = first.order.point_link_fingerprint

        with override_settings(
            POINT_LINK_FINGERPRINT_KEY="key-B",
            POINT_LINK_FINGERPRINT_KEY_FALLBACKS=["key-A"],
        ):
            fingerprint_b = point_link_fingerprint(command)
            second = link_point_note(command=command, actor=self.actor)

        self.assertNotEqual(fingerprint_a, fingerprint_b)
        self.assertEqual(second.order.pk, first.order.pk)
        self.assertFalse(second.created)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_fingerprint_rotation_without_old_fallback_rejects_replay(self, _fetch):
        command = _command()
        with override_settings(POINT_LINK_FINGERPRINT_KEY="key-A"):
            link_point_note(command=command, actor=self.actor)

        with override_settings(
            POINT_LINK_FINGERPRINT_KEY="key-B",
            POINT_LINK_FINGERPRINT_KEY_FALLBACKS=[],
            SECRET_KEY_FALLBACKS=[],
        ):
            with self.assertRaises(ValidationError):
                link_point_note(command=command, actor=self.actor)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_secret_key_rotation_accepts_django_secret_key_fallbacks(self, _fetch):
        command = _command()
        with override_settings(SECRET_KEY="secret-A", SECRET_KEY_FALLBACKS=[]):
            first = link_point_note(command=command, actor=self.actor)

        with override_settings(
            SECRET_KEY="secret-B",
            SECRET_KEY_FALLBACKS=["secret-A"],
        ):
            second = link_point_note(command=command, actor=self.actor)

        self.assertEqual(second.order.pk, first.order.pk)
        self.assertFalse(second.created)

    def test_fingerprint_key_configuration_accepts_bytes_and_rejects_bad_shapes(self):
        command = _command()
        with override_settings(POINT_LINK_FINGERPRINT_KEY=b"binary-key"):
            binary_fingerprint = point_link_fingerprint(command)
        with override_settings(POINT_LINK_FINGERPRINT_KEY="binary-key"):
            text_fingerprint = point_link_fingerprint(command)
        self.assertEqual(binary_fingerprint, text_fingerprint)

        with override_settings(POINT_LINK_FINGERPRINT_KEY=123):
            with self.assertRaises(ImproperlyConfigured):
                point_link_fingerprint(command)
        with override_settings(
            POINT_LINK_FINGERPRINT_KEY="primary",
            POINT_LINK_FINGERPRINT_KEY_FALLBACKS="not-a-list",
        ):
            with self.assertRaises(ImproperlyConfigured):
                point_link_fingerprint(command)

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
    def test_point_link_stores_non_reversible_fingerprint_and_indexed_sale_facts(self, fetch):
        result = link_point_note(command=_command(), actor=self.actor)
        order = result.order

        self.assertEqual(len(order.point_link_fingerprint), 64)
        self.assertNotIn("6671234567", order.point_link_fingerprint)
        self.assertNotIn("example.com", order.point_link_fingerprint)
        self.assertEqual(order.point_note_sold_at, fetch.return_value.sold_at)
        self.assertEqual(order.sucursal, "Matriz")

        order.point_link_fingerprint = "0" * 64
        with self.assertRaises(ValidationError):
            order.save()

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
    def test_existing_address_is_enriched_only_when_fields_are_empty(self, _fetch):
        customer = Cliente.objects.create(nombre="María", telefono="6671234567")
        address = DireccionCliente.objects.create(
            cliente=customer,
            direccion="Av. Central 123",
        )

        result = link_point_note(command=_command(), actor=self.actor)
        address.refresh_from_db()

        self.assertEqual(address.latitud, Decimal("25.790466"))
        self.assertEqual(address.longitud, Decimal("-108.985886"))
        self.assertEqual(address.place_id, "place-123")
        self.assertEqual(address.referencias, "Portón blanco")
        result.delivery.estatus = SolicitudDomicilio.ESTATUS_LISTO
        result.delivery.save(update_fields=["estatus"])

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_existing_address_rejects_conflicting_non_empty_location(self, _fetch):
        customer = Cliente.objects.create(nombre="María", telefono="6671234567")
        DireccionCliente.objects.create(
            cliente=customer,
            direccion="Av. Central 123",
            referencias="Casa azul",
            latitud=Decimal("25.700000"),
            longitud=Decimal("-108.900000"),
            place_id="existing-place",
        )

        with self.assertRaisesMessage(ValidationError, "dirección guardada"):
            link_point_note(command=_command(), actor=self.actor)
        self.assertEqual(PedidoCliente.objects.count(), 0)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_social_reference_180_is_canonical_without_overflowing_delivery_detail(self, _fetch):
        reference = "m" * 180

        result = link_point_note(
            command=_command(social_reference=reference),
            actor=self.actor,
        )

        self.assertEqual(result.order.social_reference, reference)
        self.assertLessEqual(len(result.delivery.canal_detalle), 60)
        self.assertEqual(result.delivery.canal_detalle, PedidoCliente.CANAL_FACEBOOK)

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

    @patch("crm.services.point_order_link.PointNoteDetailService.fetch")
    def test_direct_contract_rejects_lengths_email_naive_dates_and_non_finite_gps_before_io(
        self,
        fetch,
    ):
        invalid_commands = (
            _command(customer_name="x" * 181),
            _command(customer_phone="1" * 41),
            _command(customer_email="no-es-email"),
            _command(address="x" * 301),
            _command(references="x" * 301),
            _command(place_id="x" * 256),
            _command(social_reference="x" * 181),
            _command(instructions="x" * 501),
            _command(latitude=Decimal("NaN"), longitude=Decimal("-108")),
            _command(
                delivery_window_start=timezone.now().replace(tzinfo=None),
                delivery_window_end=timezone.now().replace(tzinfo=None) + timedelta(hours=1),
            ),
        )

        for command in invalid_commands:
            with self.subTest(command=command):
                with self.assertRaises(ValidationError):
                    link_point_note(command=command, actor=self.actor)

        fetch.assert_not_called()

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_existing_point_order_without_fingerprint_requires_reconciliation(
        self,
        _fetch,
    ):
        customer = Cliente.objects.create(nombre="Cliente previo", telefono="6671234567")
        existing = PedidoCliente.objects.create(
            cliente=customer,
            descripcion="Nota Point previa",
            point_note_id="900001",
            point_note_folio="18452",
            point_note_snapshot={"pk_nota": "900001"},
            point_note_fetched_at=timezone.now(),
        )

        with self.assertRaises(ValidationError) as captured:
            link_point_note(command=_command(), actor=self.actor)
        existing.refresh_from_db()

        self.assertIn("conciliación", str(captured.exception))
        self.assertIsNone(existing.direccion_entrega_id)
        self.assertEqual(SolicitudDomicilio.objects.filter(pedido_cliente=existing).count(), 0)

    def test_migration_backfills_only_valid_indexed_point_facts(self):
        migration = import_module("crm.migrations.0008_point_link_fingerprint_indexes")

        class FakeOrder:
            def __init__(self, snapshot):
                self.point_note_snapshot = snapshot
                self.point_note_sold_at = None
                self.sucursal = ""
                self.saved_fields = []

            def save(self, *, update_fields):
                self.saved_fields.append(tuple(update_fields))

        valid = FakeOrder(
            {
                "sold_at": "2026-07-27T16:30:00-07:00",
                "branch_name": "Matriz",
            },
        )
        invalid = FakeOrder(
            {
                "sold_at": "fecha-inválida",
                "branch_name": "",
            },
        )

        class FakeQuerySet:
            def exclude(self, **_kwargs):
                return self

            def iterator(self, **_kwargs):
                return iter((valid, invalid))

        class FakePedidoCliente:
            objects = FakeQuerySet()

        class FakeApps:
            @staticmethod
            def get_model(app_label, model_name):
                self.assertEqual((app_label, model_name), ("crm", "PedidoCliente"))
                return FakePedidoCliente

        migration.backfill_point_indexed_facts(FakeApps(), None)

        self.assertEqual(valid.sucursal, "Matriz")
        self.assertEqual(
            valid.point_note_sold_at,
            datetime(2026, 7, 27, 16, 30, tzinfo=dt_timezone(timedelta(hours=-7))),
        )
        self.assertEqual(valid.saved_fields, [("point_note_sold_at", "sucursal")])
        self.assertIsNone(invalid.point_note_sold_at)
        self.assertEqual(invalid.saved_fields, [])

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_shared_email_with_different_non_empty_phone_is_rejected_without_pii(self, _fetch):
        Cliente.objects.create(
            nombre="Familiar uno",
            telefono="6671111111",
            email="familia@example.com",
        )

        with self.assertRaises(ValidationError) as captured:
            link_point_note(
                command=_command(
                    customer_phone="6672222222",
                    customer_email="FAMILIA@EXAMPLE.COM",
                ),
                actor=self.actor,
            )

        self.assertNotIn("667", str(captured.exception))
        self.assertNotIn("familia@example.com", str(captured.exception).lower())
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 0)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_email_candidate_without_phone_is_safely_enriched(self, _fetch):
        customer = Cliente.objects.create(
            nombre="Cliente correo",
            telefono="",
            email="correo@example.com",
        )

        result = link_point_note(
            command=_command(
                customer_phone="6673333333",
                customer_email="CORREO@EXAMPLE.COM",
            ),
            actor=self.actor,
        )
        customer.refresh_from_db()

        self.assertEqual(result.order.cliente_id, customer.id)
        self.assertEqual(customer.telefono, "6673333333")

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_customer_code_collision_retries_only_unique_code_constraint(self, _fetch):
        Cliente.objects.create(
            codigo="CLI-00001",
            nombre="Cliente externo",
            telefono="6670000000",
        )

        with patch.object(
            Cliente,
            "_generate_codigo",
            side_effect=("CLI-00001", "CLI-00002"),
        ):
            result = link_point_note(
                command=_command(
                    customer_phone="6679999999",
                    customer_email="nuevo@example.com",
                ),
                actor=self.actor,
            )

        self.assertEqual(result.order.cliente.codigo, "CLI-00002")
        self.assertEqual(Cliente.objects.count(), 2)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_note(),
    )
    def test_non_code_integrity_error_is_not_retried_or_hidden(self, _fetch):
        with patch(
            "crm.services.point_order_link.Cliente.objects.create",
            side_effect=IntegrityError("otra restricción"),
        ) as create:
            with self.assertRaises(IntegrityError):
                link_point_note(
                    command=_command(
                        customer_phone="6678888888",
                        customer_email="otro@example.com",
                    ),
                    actor=self.actor,
                )

        create.assert_called_once()


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

    def test_two_notes_with_same_phone_create_one_customer(self):
        ready = Barrier(2)

        def attempt(pk_nota, folio):
            close_old_connections()
            try:
                ready.wait(timeout=10)
                note = _note(pk_nota=pk_nota, folio=folio)
                point_service = type(
                    "FakePointService",
                    (),
                    {"fetch": lambda self, **kwargs: note},
                )()
                command = _command(pk_nota=pk_nota)
                return link_point_note(
                    command=command,
                    actor=self.actor,
                    point_service=point_service,
                ).order.pk
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = (
                executor.submit(attempt, "900001", "18452"),
                executor.submit(attempt, "900002", "18453"),
            )
            order_ids = [future.result(timeout=20) for future in futures]

        self.assertEqual(len(set(order_ids)), 2)
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(DireccionCliente.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 2)
        self.assertEqual(SolicitudDomicilio.objects.count(), 2)

    def _link_distinct_notes(self, commands):
        ready = Barrier(2)

        def attempt(command, folio):
            close_old_connections()
            try:
                ready.wait(timeout=10)
                note = _note(pk_nota=command.pk_nota, folio=folio)
                point_service = type(
                    "FakePointService",
                    (),
                    {"fetch": lambda self, **kwargs: note},
                )()
                result = link_point_note(
                    command=command,
                    actor=self.actor,
                    point_service=point_service,
                )
                return result.order.pk
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(attempt, command, f"F-{index}")
                for index, command in enumerate(commands, start=1)
            ]
            return [future.result(timeout=20) for future in futures]

    def test_two_notes_with_different_phones_get_distinct_customer_codes(self):
        order_ids = self._link_distinct_notes(
            (
                _command(
                    pk_nota="900011",
                    customer_phone="6671111111",
                    customer_email="one@example.com",
                ),
                _command(
                    pk_nota="900012",
                    customer_phone="6672222222",
                    customer_email="two@example.com",
                ),
            ),
        )

        self.assertEqual(len(set(order_ids)), 2)
        self.assertEqual(Cliente.objects.count(), 2)
        self.assertEqual(
            len(set(Cliente.objects.values_list("codigo", flat=True))),
            2,
        )

    def test_two_notes_with_shared_email_and_different_phones_do_not_merge(self):
        commands = (
            _command(
                pk_nota="900031",
                customer_phone="6671111111",
                customer_email="shared@example.com",
            ),
            _command(
                pk_nota="900032",
                customer_phone="6672222222",
                customer_email="SHARED@EXAMPLE.COM",
            ),
        )
        ready = Barrier(2)

        def attempt(command, folio):
            close_old_connections()
            try:
                ready.wait(timeout=10)
                note = _note(pk_nota=command.pk_nota, folio=folio)
                point_service = type(
                    "FakePointService",
                    (),
                    {"fetch": lambda self, **kwargs: note},
                )()
                try:
                    result = link_point_note(
                        command=command,
                        actor=self.actor,
                        point_service=point_service,
                    )
                    return ("created", result.order.pk)
                except ValidationError:
                    return ("conflict", None)
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(attempt, command, f"F-{index}")
                for index, command in enumerate(commands, start=1)
            ]
            outcomes = [future.result(timeout=20) for future in futures]

        self.assertEqual(sorted(outcome[0] for outcome in outcomes), ["conflict", "created"])
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)

    def test_two_notes_without_phone_or_email_create_distinct_customers_safely(self):
        order_ids = self._link_distinct_notes(
            (
                _command(
                    pk_nota="900021",
                    customer_phone="",
                    customer_email="",
                    customer_name="Cliente uno",
                ),
                _command(
                    pk_nota="900022",
                    customer_phone="",
                    customer_email="",
                    customer_name="Cliente dos",
                ),
            ),
        )

        self.assertEqual(len(set(order_ids)), 2)
        self.assertEqual(Cliente.objects.count(), 2)
        self.assertEqual(PedidoCliente.objects.count(), 2)
