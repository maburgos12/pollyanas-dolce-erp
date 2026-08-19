from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from threading import Event, Thread, current_thread
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import close_old_connections
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone

from core.models import Sucursal
from crm.models import Cliente, DireccionCliente, PedidoCliente
from crm.services.point_delivery_auto_sync import PointDeliveryAutoSyncService
from crm.services.point_order_link import (
    LinkPointOrderCommand,
    point_link_fingerprint,
    point_pending_external_id,
)
from integraciones.models import PublicApiClient
from logistica.models import SolicitudDomicilio
from pos_bridge.models import PointBranch, PointSyncJob
from pos_bridge.services.point_delivery_note_service import (
    PointDeliveryBatch,
    PointDeliveryFailure,
    PointDeliveryNote,
    PointDeliveryUnavailableError,
)
from pos_bridge.services.point_note_detail_service import PointNote, PointNoteLine


def _delivery(*, pk_nota="887410", folio="15616", sold_at=None):
    note = PointNote(
        pk_nota=pk_nota,
        folio=folio,
        branch_name="Matriz",
        sold_at=sold_at or timezone.make_aware(datetime(2026, 8, 2, 12, 0)),
        total=Decimal("514.00"),
        invoiced=False,
        payment_type="CONTADO",
        point_channel="Mostrador",
        lines=(
            PointNoteLine(
                point_code="P001",
                description="Pastel",
                quantity=Decimal("1"),
                unit_price=Decimal("514.00"),
                discount=Decimal("0.00"),
                line_total=Decimal("514.00"),
            ),
        ),
        source_endpoint="/Clientes/get_detalle_nota/",
        customer_external_id="321",
    )
    return PointDeliveryNote(
        note=note,
        customer_external_id="321",
        customer_name="Cliente de prueba",
        customer_phone="6871234567",
        customer_email="",
        address="Calle Principal 101, Colonia Centro",
        references="Portón gris",
        point_address_id="88",
    )


class _FakeDeliveryService:
    def __init__(
        self,
        deliveries_by_branch=None,
        failures_by_branch=None,
        note_failures_by_branch=None,
    ):
        self.deliveries_by_branch = deliveries_by_branch or {}
        self.failures_by_branch = failures_by_branch or {}
        self.note_failures_by_branch = note_failures_by_branch or {}
        self.calls = []

    def fetch_range(self, **kwargs):
        self.calls.append(kwargs)
        branch_id = kwargs["branch_external_id"]
        if branch_id in self.failures_by_branch:
            raise self.failures_by_branch[branch_id]
        all_notes = tuple(self.deliveries_by_branch.get(branch_id, ()))
        excluded = kwargs.get("exclude_note_ids") or set()
        return PointDeliveryBatch(
            seen_count=(
                len(all_notes) + len(self.note_failures_by_branch.get(branch_id, ()))
            ),
            notes=tuple(item for item in all_notes if item.note.pk_nota not in excluded),
            failures=tuple(self.note_failures_by_branch.get(branch_id, ())),
        )


class _SingleNoteService:
    def __init__(self, note):
        self.note = note

    def fetch(self, *, pk_nota):
        if str(pk_nota) != self.note.pk_nota:
            raise ValueError("Nota distinta")
        return self.note


class PointDeliveryAutoSyncTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_user(
            username="point-delivery-sync",
            password="unused",
        )
        self.api_client, _key = PublicApiClient.create_with_generated_key(
            nombre="Centro Operativo",
            created_by=self.actor,
        )
        self.api_client.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        self.api_client.save(update_fields=["capabilities"])
        self.matriz_erp = Sucursal.objects.create(
            codigo="POINT-DOM-10",
            nombre="Matriz Point Domicilios",
        )
        self.centro_erp = Sucursal.objects.create(
            codigo="POINT-DOM-20",
            nombre="Centro Point Domicilios",
        )
        self.matriz = PointBranch.objects.create(
            external_id="10",
            name="Matriz",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.matriz_erp,
        )
        self.centro = PointBranch.objects.create(
            external_id="20",
            name="Centro",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.centro_erp,
        )

    def test_run_scans_every_active_branch_with_configurable_seven_day_lookback(self):
        delivery = _delivery()
        point = _FakeDeliveryService({"10": [delivery], "20": []})

        result = PointDeliveryAutoSyncService(delivery_service=point).run(
            today=date(2026, 8, 7),
            lookback_days=7,
        )

        self.assertEqual(result["status"], PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(result["counts"], {"seen": 1, "created": 1, "existing": 0, "failed": 0})
        self.assertEqual(
            [(call["branch_external_id"], call["branch_display_name"]) for call in point.calls],
            [("10", "Matriz"), ("20", "Centro")],
        )
        self.assertTrue(all(call["start_date"] == date(2026, 8, 1) for call in point.calls))
        self.assertTrue(all(call["end_date"] == date(2026, 8, 7) for call in point.calls))
        order = PedidoCliente.objects.get(point_note_id="887410")
        self.assertEqual(order.public_api_client, self.api_client)
        self.assertEqual(order.canal, PedidoCliente.CANAL_POR_CONFIRMAR)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)
        job = PointSyncJob.objects.get(job_type=PointSyncJob.JOB_TYPE_DELIVERIES)
        self.assertEqual(job.result_summary, result["counts"])
        self.assertNotIn("Cliente de prueba", str(job.parameters))

    def test_run_scans_only_deduplicated_numeric_branches_linked_to_erp(self):
        PointBranch.objects.create(
            external_id="Matriz",
            name="Matriz duplicada nominal",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.matriz_erp,
        )
        PointBranch.objects.create(
            external_id="010",
            name="Matriz duplicada con ceros",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.matriz_erp,
        )
        PointBranch.objects.create(
            external_id="30",
            name="Numérica sin enlace ERP",
            status=PointBranch.STATUS_ACTIVE,
        )
        PointBranch.objects.create(
            external_id="40",
            name="Numérica inactiva",
            status=PointBranch.STATUS_INACTIVE,
            erp_branch=self.matriz_erp,
        )
        point = _FakeDeliveryService({"10": [], "20": []})

        result = PointDeliveryAutoSyncService(delivery_service=point).run(
            today=date(2026, 8, 7),
        )

        self.assertEqual(result["status"], PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(
            [call["branch_external_id"] for call in point.calls],
            ["10", "20"],
        )
        self.assertEqual(PointSyncJob.objects.get().parameters["branch_count"], 2)

    def test_run_repeatedly_keeps_one_order_and_skips_detail_for_existing_note(self):
        delivery = _delivery()
        point = _FakeDeliveryService({"10": [delivery], "20": []})
        service = PointDeliveryAutoSyncService(delivery_service=point)

        first = service.run(today=date(2026, 8, 7), lookback_days=7)
        second = service.run(today=date(2026, 8, 7), lookback_days=7)

        self.assertEqual(first["counts"]["created"], 1)
        self.assertEqual(second["counts"]["created"], 0)
        self.assertEqual(second["counts"]["existing"], 1)
        self.assertEqual(PedidoCliente.objects.filter(point_note_id="887410").count(), 1)
        self.assertIn("887410", point.calls[-1]["exclude_note_ids"])

    def test_run_fails_closed_without_exactly_one_active_omnichannel_owner(self):
        second_actor = get_user_model().objects.create_user(
            username="point-delivery-sync-2",
            password="unused",
        )
        second, _key = PublicApiClient.create_with_generated_key(
            nombre="Centro Operativo 2",
            created_by=second_actor,
        )
        second.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        second.save(update_fields=["capabilities"])
        point = _FakeDeliveryService({"10": [_delivery()]})

        result = PointDeliveryAutoSyncService(delivery_service=point).run(
            today=date(2026, 8, 7),
        )

        self.assertEqual(result["status"], PointSyncJob.STATUS_FAILED)
        self.assertEqual(result["error_code"], "OWNER_CONFIGURATION")
        self.assertEqual(PedidoCliente.objects.count(), 0)
        self.assertEqual(point.calls, [])

    @override_settings(POINT_DELIVERY_API_CLIENT_PREFIX="selected")
    def test_run_uses_explicit_owner_prefix_when_multiple_clients_are_active(self):
        self.api_client.clave_prefijo = "selected"
        self.api_client.save(update_fields=["clave_prefijo"])
        other_actor = get_user_model().objects.create_user(
            username="point-delivery-other",
            password="unused",
        )
        other, _key = PublicApiClient.create_with_generated_key(
            nombre="Otro omnicanal",
            created_by=other_actor,
        )
        other.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        other.save(update_fields=["capabilities"])
        point = _FakeDeliveryService({"10": [_delivery()]})

        result = PointDeliveryAutoSyncService(delivery_service=point).run(
            today=date(2026, 8, 7),
        )

        self.assertEqual(result["status"], PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(PedidoCliente.objects.get().public_api_client, self.api_client)

    def test_run_returns_locked_without_starting_duplicate_job(self):
        service = PointDeliveryAutoSyncService(delivery_service=_FakeDeliveryService())

        with patch.object(service, "_try_lock", return_value=False):
            result = service.run(today=date(2026, 8, 7))

        self.assertEqual(result["status"], PointSyncJob.STATUS_RUNNING)
        self.assertEqual(result["error_code"], "SYNC_IN_PROGRESS")
        self.assertEqual(PointSyncJob.objects.count(), 0)

    def test_run_marks_partial_and_never_persists_point_error_text_or_pii(self):
        point = _FakeDeliveryService(
            deliveries_by_branch={"20": []},
            failures_by_branch={
                "10": PointDeliveryUnavailableError(
                    "cliente@example.test 6871234567 Calle Privada 123",
                ),
            },
        )

        result = PointDeliveryAutoSyncService(delivery_service=point).run(
            today=date(2026, 8, 7),
        )

        self.assertEqual(result["status"], PointSyncJob.STATUS_PARTIAL)
        self.assertEqual(result["counts"]["failed"], 0)
        job = PointSyncJob.objects.get()
        serialized = f"{job.error_message} {job.result_summary} {job.parameters}"
        self.assertNotIn("cliente@example.test", serialized)
        self.assertNotIn("6871234567", serialized)
        self.assertNotIn("Calle Privada", serialized)

    def test_run_imports_valid_notes_and_marks_partial_for_isolated_note_failure(self):
        point = _FakeDeliveryService(
            deliveries_by_branch={"10": [_delivery()], "20": []},
            note_failures_by_branch={
                "10": [
                    PointDeliveryFailure(
                        point_note_id="999999",
                        error_code="POINT_CONTRACT",
                    ),
                ],
            },
        )

        result = PointDeliveryAutoSyncService(delivery_service=point).run(
            today=date(2026, 8, 7),
        )

        self.assertEqual(result["status"], PointSyncJob.STATUS_PARTIAL)
        self.assertEqual(
            result["counts"],
            {"seen": 2, "created": 1, "existing": 0, "failed": 1},
        )
        self.assertEqual(PedidoCliente.objects.filter(point_note_id="887410").count(), 1)

    def test_run_reconciles_matching_manual_pending_capture_without_duplicate_or_partial(self):
        delivery = _delivery()
        customer = Cliente.objects.create(
            nombre="Captura manual",
            telefono="6879998888",
        )
        address = DireccionCliente.objects.create(
            cliente=customer,
            direccion="Dirección manual 55",
            referencias="Referencia capturada",
        )
        command = LinkPointOrderCommand(
            pk_nota=delivery.note.pk_nota,
            channel=PedidoCliente.CANAL_TELEFONO,
            customer_name=customer.nombre,
            customer_phone=customer.telefono,
            customer_email="",
            address=address.direccion,
            references=address.referencias,
            latitude=None,
            longitude=None,
            place_id="",
            social_reference="",
            delivery_window_start=None,
            delivery_window_end=None,
            instructions="Llamar al llegar",
        )
        pending_id = point_pending_external_id(
            folio=delivery.note.folio,
            branch=delivery.note.branch_name,
            sale_date=timezone.localtime(delivery.note.sold_at).date(),
        )
        order = PedidoCliente.objects.create(
            cliente=customer,
            direccion_entrega=address,
            external_source="POINT_PENDING",
            external_id=pending_id,
            payload_snapshot={
                "point_pending": {
                    "external_id": pending_id,
                    "capture_fingerprint": point_link_fingerprint(command),
                    "capture": {
                        "channel": command.channel,
                        "social_reference": command.social_reference,
                        "customer_name": command.customer_name,
                        "customer_phone": command.customer_phone,
                        "customer_email": command.customer_email,
                        "address": command.address,
                        "references": command.references,
                        "latitude": None,
                        "longitude": None,
                        "place_id": "",
                        "delivery_window_start": None,
                        "delivery_window_end": None,
                        "instructions": command.instructions,
                    },
                },
            },
            public_api_client=self.api_client,
            canal=command.channel,
            descripcion="Captura manual en espera",
            created_by=self.actor,
        )
        SolicitudDomicilio.objects.create(
            pedido_cliente=order,
            cliente=customer,
            direccion_cliente=address,
            cliente_nombre=customer.nombre,
            cliente_telefono=customer.telefono,
            direccion=address.direccion,
            canal_origen=command.channel,
            notas=address.referencias,
            instrucciones_entrega=command.instructions,
            estatus=SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
            created_by=self.actor,
        )
        point = _FakeDeliveryService({"10": [delivery], "20": []})

        result = PointDeliveryAutoSyncService(delivery_service=point).run(
            today=date(2026, 8, 7),
        )

        self.assertEqual(result["status"], PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(result["counts"]["existing"], 1)
        self.assertEqual(result["counts"]["failed"], 0)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        order.refresh_from_db()
        self.assertEqual(order.point_note_id, delivery.note.pk_nota)
        self.assertEqual(order.monto_estimado, delivery.note.total)
        self.assertEqual(order.canal, PedidoCliente.CANAL_TELEFONO)
        self.assertEqual(order.direccion_entrega, address)
        self.assertEqual(order.cliente, customer)
        customer.refresh_from_db()
        self.assertEqual(customer.point_customer_id, delivery.customer_external_id)
        self.assertEqual(
            order.solicitudes_domicilio.get().estatus,
            SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )


class PointDeliveryAutoSyncConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self.actor = get_user_model().objects.create_user(
            username="point-delivery-sync-race",
            password="unused",
        )
        self.api_client, _key = PublicApiClient.create_with_generated_key(
            nombre="Centro Operativo Race",
            created_by=self.actor,
        )
        self.api_client.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        self.api_client.save(update_fields=["capabilities"])
        erp_branch = Sucursal.objects.create(
            id=(
                Sucursal.objects.order_by("-id")
                .values_list("id", flat=True)
                .first()
                or 0
            ) + 1,
            codigo="POINT-DOM-RACE",
            nombre="Matriz Point Race",
        )
        PointBranch.objects.create(
            external_id="10",
            name="Matriz",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=erp_branch,
        )
        self.delivery = _delivery()
        self.customer = Cliente.objects.create(
            nombre="Captura concurrente",
            telefono="6879998888",
        )
        self.address = DireccionCliente.objects.create(
            cliente=self.customer,
            direccion="Dirección concurrente 55",
            referencias="Referencia concurrente",
        )
        self.command = LinkPointOrderCommand(
            pk_nota=self.delivery.note.pk_nota,
            channel=PedidoCliente.CANAL_TELEFONO,
            customer_name=self.customer.nombre,
            customer_phone=self.customer.telefono,
            customer_email="",
            address=self.address.direccion,
            references=self.address.referencias,
            latitude=None,
            longitude=None,
            place_id="",
            social_reference="",
            delivery_window_start=None,
            delivery_window_end=None,
            instructions="Llamar al llegar",
        )
        pending_id = point_pending_external_id(
            folio=self.delivery.note.folio,
            branch=self.delivery.note.branch_name,
            sale_date=timezone.localtime(self.delivery.note.sold_at).date(),
        )
        order = PedidoCliente.objects.create(
            cliente=self.customer,
            direccion_entrega=self.address,
            external_source="POINT_PENDING",
            external_id=pending_id,
            payload_snapshot={
                "point_pending": {
                    "external_id": pending_id,
                    "capture_fingerprint": point_link_fingerprint(self.command),
                    "capture": {
                        "channel": self.command.channel,
                        "social_reference": "",
                        "customer_name": self.command.customer_name,
                        "customer_phone": self.command.customer_phone,
                        "customer_email": "",
                        "address": self.command.address,
                        "references": self.command.references,
                        "latitude": None,
                        "longitude": None,
                        "place_id": "",
                        "delivery_window_start": None,
                        "delivery_window_end": None,
                        "instructions": self.command.instructions,
                    },
                },
            },
            public_api_client=self.api_client,
            canal=self.command.channel,
            descripcion="Captura concurrente en espera",
            created_by=self.actor,
        )
        SolicitudDomicilio.objects.create(
            pedido_cliente=order,
            cliente=self.customer,
            direccion_cliente=self.address,
            cliente_nombre=self.customer.nombre,
            cliente_telefono=self.customer.telefono,
            direccion=self.address.direccion,
            canal_origen=self.command.channel,
            estatus=SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
            created_by=self.actor,
        )

    def test_auto_sync_and_manual_reconcile_share_canonical_lock_order(self):
        from crm.services import point_order_link as point_link_module
        from crm.services.point_order_link import link_point_note

        original_lock = point_link_module._lock_key
        auto_reached_note_lock = Event()
        manual_has_note_lock = Event()
        auto_results = []
        manual_results = []
        errors = []

        def coordinated_lock(namespace, value):
            if namespace == "crm-point-note":
                if current_thread().name == "manual-reconcile":
                    original_lock(namespace, value)
                    manual_has_note_lock.set()
                    self.assertTrue(auto_reached_note_lock.wait(5))
                    return
                if current_thread().name == "auto-sync":
                    auto_reached_note_lock.set()
                    self.assertTrue(manual_has_note_lock.wait(5))
            return original_lock(namespace, value)

        def auto_worker():
            close_old_connections()
            try:
                auto_results.append(
                    PointDeliveryAutoSyncService(
                        delivery_service=_FakeDeliveryService(
                            {"10": [self.delivery]},
                        ),
                    ).run(today=date(2026, 8, 7)),
                )
            except Exception as exc:  # noqa: BLE001 - evidencia del thread
                errors.append(exc)
            finally:
                close_old_connections()

        def manual_worker():
            close_old_connections()
            try:
                manual_results.append(
                    link_point_note(
                        command=self.command,
                        actor=get_user_model().objects.get(pk=self.actor.pk),
                        point_service=_SingleNoteService(self.delivery.note),
                    ),
                )
            except Exception as exc:  # noqa: BLE001 - evidencia del thread
                errors.append(exc)
            finally:
                close_old_connections()

        with patch.object(point_link_module, "_lock_key", side_effect=coordinated_lock):
            threads = [
                Thread(target=auto_worker, name="auto-sync"),
                Thread(target=manual_worker, name="manual-reconcile"),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(auto_results[0]["status"], PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(len(manual_results), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        order = PedidoCliente.objects.get()
        self.assertEqual(order.point_note_id, self.delivery.note.pk_nota)
