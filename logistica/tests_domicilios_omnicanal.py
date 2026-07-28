from decimal import Decimal
from datetime import timedelta
from importlib import import_module

from django.apps import apps
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from core.models import AuditLog, Notificacion
from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import (
    EntregaEcommerce,
    Repartidor,
    SolicitudDomicilio,
    Unidad,
)
from logistica.services_domicilio_assignment import assign_domicilio
from logistica.services_domicilio_status import (
    DomicilioStatusError,
    apply_domicilio_status_transition,
)
from rrhh.models import Empleado


class SolicitudDomicilioOmnicanalTests(APITestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="DOM-TEST", nombre="Domicilios Test")
        self.user = User.objects.create_user(username="repartidor_domicilios", password="pass123")
        self.unidad = Unidad.objects.create(
            codigo="DOM-TEST-U",
            descripcion="Unidad pruebas",
            sucursal=self.sucursal,
        )
        self.repartidor = Repartidor.objects.create(
            user=self.user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )

    def _pedido_y_direccion(self, *, point_note_id="900001", gps=True):
        cliente = Cliente.objects.create(nombre="Cliente estados", telefono="6671112233")
        direccion = DireccionCliente.objects.create(
            cliente=cliente,
            alias="Casa",
            direccion="Calle Estado 123",
            latitud=Decimal("25.790001") if gps else None,
            longitud=Decimal("-108.990001") if gps else None,
        )
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            direccion_entrega=direccion,
            descripcion="Pedido para validar domicilio",
            point_note_id=point_note_id,
            point_note_snapshot={"pk_nota": point_note_id} if point_note_id else {},
        )
        return cliente, direccion, pedido

    def _solicitud(self, *, estatus, point_note_id="900001", gps=True, **kwargs):
        cliente, direccion, pedido = self._pedido_y_direccion(
            point_note_id=point_note_id,
            gps=gps,
        )
        return SolicitudDomicilio(
            cliente=cliente,
            direccion_cliente=direccion,
            pedido_cliente=pedido,
            cliente_nombre=cliente.nombre,
            cliente_telefono=cliente.telefono,
            direccion=direccion.direccion,
            estatus=estatus,
            **kwargs,
        )

    def test_domicilio_listo_requires_point_note_and_gps(self):
        without_point = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_LISTO,
            point_note_id="",
        )
        with self.assertRaises(ValidationError) as point_error:
            without_point.full_clean()
        self.assertIn("pedido_cliente", point_error.exception.message_dict)

        without_gps = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_LISTO,
            gps=False,
        )
        with self.assertRaises(ValidationError) as gps_error:
            without_gps.full_clean()
        self.assertIn("direccion_cliente", gps_error.exception.message_dict)

    def test_confirmado_requires_canonical_point_note(self):
        without_point = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
            point_note_id="",
        )
        with self.assertRaises(ValidationError) as error:
            without_point.save()
        self.assertIn("pedido_cliente", error.exception.message_dict)

    def test_en_ruta_requires_assignment(self):
        solicitud = self._solicitud(estatus=SolicitudDomicilio.ESTATUS_EN_RUTA)

        with self.assertRaises(ValidationError) as error:
            solicitud.full_clean()

        self.assertIn("repartidor", error.exception.message_dict)

    def test_save_cannot_bypass_ready_invariants(self):
        solicitud = SolicitudDomicilio(
            cliente_nombre="Sin contrato Point",
            direccion="Sin GPS",
            estatus=SolicitudDomicilio.ESTATUS_LISTO,
        )

        with self.assertRaises(ValidationError):
            solicitud.save()

    def test_partial_save_validates_effective_persisted_state(self):
        solicitud = SolicitudDomicilio.objects.create(
            cliente_nombre="Persistido sin Point",
            direccion="Sin GPS",
        )
        cliente, direccion, pedido = self._pedido_y_direccion()
        solicitud.cliente = cliente
        solicitud.direccion_cliente = direccion
        solicitud.pedido_cliente = pedido
        solicitud.estatus = SolicitudDomicilio.ESTATUS_LISTO

        with self.assertRaises(ValidationError):
            solicitud.save(update_fields=["estatus"])

        solicitud.refresh_from_db()
        self.assertEqual(
            solicitud.estatus,
            SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
        )

    def test_queryset_writes_cannot_bypass_operational_invariants(self):
        solicitud = SolicitudDomicilio.objects.create(
            cliente_nombre="Actualización masiva",
            direccion="Sin GPS",
        )

        with self.assertRaises(ValidationError):
            SolicitudDomicilio.objects.filter(pk=solicitud.pk).update(
                estatus=SolicitudDomicilio.ESTATUS_LISTO,
            )

        solicitud.estatus = SolicitudDomicilio.ESTATUS_LISTO
        with self.assertRaises(ValidationError):
            SolicitudDomicilio.objects.bulk_update([solicitud], ["estatus"])

    def test_bulk_create_rejects_ready_without_point_or_gps_in_all_modes(self):
        invalid = SolicitudDomicilio(
            cliente_nombre="Bulk inválido",
            direccion="Sin GPS",
            estatus=SolicitudDomicilio.ESTATUS_LISTO,
        )

        with self.assertRaises(ValidationError):
            SolicitudDomicilio.objects.bulk_create([invalid])
        with self.assertRaises(ValidationError):
            SolicitudDomicilio.objects.bulk_create(
                [invalid],
                ignore_conflicts=True,
            )
        with self.assertRaises(ValidationError):
            SolicitudDomicilio.objects.bulk_create(
                [invalid],
                update_conflicts=True,
                update_fields=["estatus"],
                unique_fields=["id"],
            )

    def test_legacy_flag_is_migration_only_but_existing_history_remains_editable(self):
        runtime_created = SolicitudDomicilio(
            cliente_nombre="Falso histórico",
            direccion="Calle",
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            legacy_without_point=True,
        )
        with self.assertRaises(ValidationError):
            runtime_created.save()

        normal = SolicitudDomicilio.objects.create(
            cliente_nombre="Normal",
            direccion="Calle normal",
        )
        normal.legacy_without_point = True
        with self.assertRaises(ValidationError):
            normal.save(update_fields=["legacy_without_point"])

        historical = SolicitudDomicilio.objects.create(
            cliente_nombre="Histórico migrado",
            direccion="Calle histórica",
        )
        SolicitudDomicilio._base_manager.filter(pk=historical.pk).update(
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            legacy_without_point=True,
        )
        historical.refresh_from_db()
        historical.notas = "Cambio ajeno permitido"
        historical.save(update_fields=["notas"])
        historical.refresh_from_db()
        self.assertTrue(historical.legacy_without_point)
        self.assertEqual(historical.notas, "Cambio ajeno permitido")

    def test_legacy_history_cannot_become_active_without_clearing_flag(self):
        historical = SolicitudDomicilio.objects.create(
            cliente_nombre="Histórico terminal",
            direccion="Calle histórica",
        )
        SolicitudDomicilio._base_manager.filter(pk=historical.pk).update(
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            legacy_without_point=True,
        )
        historical.refresh_from_db()
        historical.estatus = SolicitudDomicilio.ESTATUS_PENDIENTE_POINT

        with self.assertRaises(ValidationError) as error:
            historical.save(update_fields=["estatus"])

        self.assertIn("legacy_without_point", error.exception.message_dict)
        historical.refresh_from_db()
        self.assertEqual(historical.estatus, SolicitudDomicilio.ESTATUS_ENTREGADO)
        self.assertTrue(historical.legacy_without_point)

    def test_legacy_reconciliation_validates_effective_fields_in_same_save(self):
        historical = SolicitudDomicilio.objects.create(
            cliente_nombre="Histórico por reconciliar",
            direccion="Calle histórica",
        )
        SolicitudDomicilio._base_manager.filter(pk=historical.pk).update(
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            legacy_without_point=True,
        )
        historical.refresh_from_db()
        cliente, direccion, pedido = self._pedido_y_direccion(
            point_note_id="POINT-RECONCILIADO"
        )
        historical.legacy_without_point = False
        historical.estatus = SolicitudDomicilio.ESTATUS_CONFIRMADO
        historical.cliente = cliente
        historical.pedido_cliente = pedido
        historical.direccion_cliente = direccion

        with self.assertRaises(ValidationError):
            historical.save(
                update_fields=["legacy_without_point", "estatus"],
            )

        historical.save(
            update_fields=[
                "legacy_without_point",
                "estatus",
                "cliente",
                "pedido_cliente",
                "direccion_cliente",
            ],
        )
        historical.refresh_from_db()
        self.assertFalse(historical.legacy_without_point)
        self.assertEqual(historical.estatus, SolicitudDomicilio.ESTATUS_CONFIRMADO)
        self.assertEqual(historical.pedido_cliente_id, pedido.id)
        self.assertEqual(historical.direccion_cliente_id, direccion.id)

    def test_save_rejects_inverted_window_in_pending_state(self):
        solicitud = SolicitudDomicilio(
            cliente_nombre="Ventana inválida",
            direccion="Calle",
            ventana_inicio=timezone.now(),
            ventana_fin=timezone.now() - timedelta(minutes=30),
        )

        with self.assertRaises(ValidationError) as error:
            solicitud.save()

        self.assertIn("ventana_fin", error.exception.message_dict)

    def test_entregado_requires_existing_delivery_evidence(self):
        solicitud = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            repartidor=self.repartidor,
            unidad=self.unidad,
        )

        with self.assertRaises(ValidationError) as error:
            solicitud.full_clean()
        self.assertIn("entregado_en", error.exception.message_dict)

        solicitud.entregado_en = timezone.now()
        solicitud.full_clean()

    def test_cancelado_requires_reason(self):
        solicitud = self._solicitud(estatus=SolicitudDomicilio.ESTATUS_CANCELADO)

        with self.assertRaises(ValidationError) as error:
            solicitud.full_clean()
        self.assertIn("cancelacion_motivo", error.exception.message_dict)

        solicitud.cancelacion_motivo = "Cliente solicitó cancelar"
        solicitud.full_clean()

    def test_legacy_terminal_without_point_preserves_history(self):
        for estatus in (
            SolicitudDomicilio.ESTATUS_ENTREGADO,
            SolicitudDomicilio.ESTATUS_CANCELADO,
        ):
            with self.subTest(estatus=estatus):
                solicitud = SolicitudDomicilio.objects.create(
                    cliente_nombre="Histórico",
                    direccion="Dirección histórica",
                )
                SolicitudDomicilio._base_manager.filter(pk=solicitud.pk).update(
                    estatus=estatus,
                    legacy_without_point=True,
                )
                solicitud.refresh_from_db()
                solicitud.full_clean()

    def test_delivery_window_end_cannot_precede_start(self):
        solicitud = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
            ventana_inicio=timezone.now(),
            ventana_fin=timezone.now() - timedelta(hours=1),
        )

        with self.assertRaises(ValidationError) as error:
            solicitud.full_clean()

        self.assertIn("ventana_fin", error.exception.message_dict)

    def test_canonical_state_machine_is_sequential_idempotent_and_cancel_requires_reason(self):
        solicitud = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
            repartidor=self.repartidor,
            unidad=self.unidad,
        )
        solicitud.save()
        self.assertFalse(
            apply_domicilio_status_transition(
                solicitud=solicitud,
                requested_status=SolicitudDomicilio.ESTATUS_CONFIRMADO,
            )
        )
        with self.assertRaises(DomicilioStatusError):
            apply_domicilio_status_transition(
                solicitud=solicitud,
                requested_status=SolicitudDomicilio.ESTATUS_LISTO,
            )
        for next_status in (
            SolicitudDomicilio.ESTATUS_PREPARANDO,
            SolicitudDomicilio.ESTATUS_LISTO,
            SolicitudDomicilio.ESTATUS_EN_RUTA,
            SolicitudDomicilio.ESTATUS_ENTREGADO,
        ):
            self.assertTrue(
                apply_domicilio_status_transition(
                    solicitud=solicitud,
                    requested_status=next_status,
                )
            )
        self.assertEqual(solicitud.estatus, SolicitudDomicilio.ESTATUS_ENTREGADO)

        cancellable = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
            point_note_id="POINT-CANCEL-STATE",
        )
        cancellable.save()
        with self.assertRaises(DomicilioStatusError):
            apply_domicilio_status_transition(
                solicitud=cancellable,
                requested_status=SolicitudDomicilio.ESTATUS_CANCELADO,
            )
        apply_domicilio_status_transition(
            solicitud=cancellable,
            requested_status=SolicitudDomicilio.ESTATUS_CANCELADO,
            cancelacion_motivo="Cliente canceló",
        )
        self.assertEqual(cancellable.cancelacion_motivo, "Cliente canceló")

    def test_direct_save_rejects_state_jump_and_advanced_initial_state(self):
        invalid = self._solicitud(estatus=SolicitudDomicilio.ESTATUS_LISTO)
        with self.assertRaises(ValidationError):
            invalid.save()
        persisted = self._solicitud(
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
            point_note_id="POINT-PERSISTED-JUMP",
        )
        persisted.save()
        persisted.estatus = SolicitudDomicilio.ESTATUS_LISTO
        with self.assertRaises(ValidationError):
            persisted.save(update_fields=["estatus"])

    def test_data_migration_maps_point_and_legacy_records_without_losing_history(self):
        _, direccion, pedido = self._pedido_y_direccion(
            point_note_id="POINT-MIGRATION"
        )
        linked_pending = SolicitudDomicilio.objects.create(
            pedido_cliente=pedido,
            cliente_nombre="Ligado pendiente",
            direccion="Calle ligada",
        )
        SolicitudDomicilio._base_manager.filter(pk=linked_pending.pk).update(
            estatus="PENDIENTE",
        )
        _, direccion, pedido_asignado = self._pedido_y_direccion(
            point_note_id="POINT-MIGRATION-ASIGNADO"
        )
        linked_assigned = SolicitudDomicilio.objects.create(
            pedido_cliente=pedido_asignado,
            direccion_cliente=direccion,
            cliente_nombre="Ligado asignado",
            direccion="Calle ligada",
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        SolicitudDomicilio._base_manager.filter(pk=linked_assigned.pk).update(
            estatus="ASIGNADO",
        )
        _, _, pedido_sin_gps = self._pedido_y_direccion(
            point_note_id="POINT-MIGRATION-SIN-GPS"
        )
        linked_assigned_without_gps = SolicitudDomicilio.objects.create(
            pedido_cliente=pedido_sin_gps,
            cliente_nombre="Ligado asignado sin GPS",
            direccion="Calle sin GPS",
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        SolicitudDomicilio._base_manager.filter(
            pk=linked_assigned_without_gps.pk,
        ).update(estatus="ASIGNADO")
        active_without_point = SolicitudDomicilio.objects.create(
            cliente_nombre="Activo legado",
            direccion="Calle activa",
        )
        delivered_without_point = SolicitudDomicilio.objects.create(
            cliente_nombre="Entregado legado",
            direccion="Calle histórica",
        )
        SolicitudDomicilio._base_manager.filter(pk=active_without_point.pk).update(
            estatus="EN_RUTA",
        )
        SolicitudDomicilio._base_manager.filter(pk=delivered_without_point.pk).update(
            estatus="ENTREGADO",
        )

        migration = import_module(
            "logistica.migrations.0046_solicituddomicilio_operacion_canonica"
        )
        migration.migrate_delivery_states(apps, None)

        linked_pending.refresh_from_db()
        linked_assigned.refresh_from_db()
        linked_assigned_without_gps.refresh_from_db()
        active_without_point.refresh_from_db()
        delivered_without_point.refresh_from_db()
        self.assertEqual(linked_pending.estatus, SolicitudDomicilio.ESTATUS_CONFIRMADO)
        self.assertEqual(linked_assigned.estatus, SolicitudDomicilio.ESTATUS_LISTO)
        self.assertEqual(
            linked_assigned_without_gps.estatus,
            SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        self.assertEqual(
            active_without_point.estatus,
            SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
        )
        self.assertEqual(
            delivered_without_point.estatus,
            SolicitudDomicilio.ESTATUS_ENTREGADO,
        )
        self.assertTrue(delivered_without_point.legacy_without_point)

    def test_solicitud_accepts_social_channels_shared_with_crm(self):
        self.assertEqual(SolicitudDomicilio.CANAL_FACEBOOK, PedidoCliente.CANAL_FACEBOOK)
        self.assertEqual(SolicitudDomicilio.CANAL_INSTAGRAM, PedidoCliente.CANAL_INSTAGRAM)
        self.assertIn(("FACEBOOK", "Facebook"), SolicitudDomicilio.CANAL_CHOICES)
        self.assertIn(("INSTAGRAM", "Instagram"), SolicitudDomicilio.CANAL_CHOICES)
        self.assertEqual(SolicitudDomicilio.CANAL_CHOICES, PedidoCliente.CANAL_CHOICES)

    def test_solicitud_ligada_conserva_cliente_direccion_y_texto_historico(self):
        cliente = Cliente.objects.create(nombre="Ana Pérez", telefono="6671234567")
        direccion = DireccionCliente.objects.create(
            cliente=cliente,
            alias="Casa",
            direccion="Av. Obregón 123",
            referencias="Portón blanco",
            latitud="24.809064",
            longitud="-107.394011",
        )
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            direccion_entrega=direccion,
            descripcion="Pedido con domicilio",
            point_note_id="POINT-LINKED-HISTORY",
            point_note_snapshot={"pk_nota": "POINT-LINKED-HISTORY"},
        )

        solicitud = SolicitudDomicilio.objects.create(
            cliente=cliente,
            direccion_cliente=direccion,
            pedido_cliente=pedido,
            cliente_nombre=cliente.nombre,
            cliente_telefono=cliente.telefono,
            direccion=direccion.direccion,
            repartidor=self.repartidor,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )

        self.assertEqual(solicitud.cliente_id, cliente.id)
        self.assertEqual(solicitud.direccion_cliente_id, direccion.id)
        self.assertEqual(solicitud.cliente_nombre, "Ana Pérez")
        self.assertEqual(solicitud.direccion, "Av. Obregón 123")

    def test_api_repartidor_mantiene_fallback_de_solicitud_historica(self):
        historical = SolicitudDomicilio.objects.create(
            cliente_nombre="Cliente legado",
            cliente_telefono="6670000000",
            direccion="Dirección histórica 45",
            notas="Tocar timbre",
            repartidor=self.repartidor,
        )
        SolicitudDomicilio._base_manager.filter(pk=historical.pk).update(
            estatus=SolicitudDomicilio.ESTATUS_ASIGNADO_LEGACY,
        )
        self.client.force_authenticate(self.user)

        response = self.client.get(reverse("api_logistica_domicilios_generales_asignados"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])


class AsignacionDomicilioApiTests(APITestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="DOM-ASIG", nombre="Domicilios Asignación")
        self.manager = User.objects.create_superuser(
            username="manager_domicilios", email="manager@example.com", password="pass123"
        )
        self.client.force_authenticate(self.manager)
        self.solicitud = SolicitudDomicilio.objects.create(
            cliente_nombre="Cliente asignable",
            direccion="Calle 10",
        )

    def _repartidor(self, username, **kwargs):
        user = User.objects.create_user(username=username, password="pass123")
        unidad = kwargs.pop("unidad_asignada", None) or Unidad.objects.create(
            codigo=f"U-{username}",
            descripcion=f"Unidad {username}",
            sucursal=self.sucursal,
        )
        return Repartidor.objects.create(
            user=user,
            sucursal=self.sucursal,
            unidad_asignada=unidad,
            **kwargs,
        )

    def test_catalogo_solo_devuelve_repartidores_activos_y_autorizados(self):
        disponible = self._repartidor("rep_disponible")
        inactivo = self._repartidor("rep_inactivo")
        inactivo.user.is_active = False
        inactivo.user.save(update_fields=["is_active"])
        baja_rrhh = self._repartidor("rep_baja")
        Empleado.objects.create(
            codigo="EMP-BAJA",
            nombre="Repartidor baja",
            usuario_erp=baja_rrhh.user,
            activo=False,
        )
        self._repartidor(
            "rep_tecnico",
            tipo_identidad=Repartidor.TIPO_CUENTA_TECNICA,
        )

        response = self.client.get(reverse("api_logistica_repartidores_disponibles"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in response.data], [disponible.id])
        rejected = self.client.post(
            reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id]),
            {"repartidor_id": inactivo.id},
            format="json",
        )
        self.assertEqual(rejected.status_code, status.HTTP_400_BAD_REQUEST)
        self.solicitud.refresh_from_db()
        self.assertIsNone(self.solicitud.repartidor_id)

    def test_asignacion_idempotente_y_reasignacion_auditada(self):
        primero = self._repartidor("rep_primero")
        segundo = self._repartidor("rep_segundo")
        url = reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id])

        first = self.client.post(url, {"repartidor_id": primero.id, "unidad_id": primero.unidad_asignada_id}, format="json")
        repeated = self.client.post(url, {"repartidor_id": primero.id, "unidad_id": primero.unidad_asignada_id}, format="json")
        reassigned = self.client.post(url, {"repartidor_id": segundo.id, "unidad_id": segundo.unidad_asignada_id}, format="json")

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(repeated.status_code, status.HTTP_200_OK)
        self.assertTrue(repeated.data["idempotent"])
        self.assertEqual(reassigned.status_code, status.HTTP_200_OK)
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.repartidor_id, segundo.id)
        logs = AuditLog.objects.filter(
            model="logistica.SolicitudDomicilio",
            object_id=str(self.solicitud.id),
            action="ASSIGN",
        ).order_by("timestamp")
        self.assertEqual(logs.count(), 2)
        self.assertEqual(logs.last().payload["repartidor_anterior_id"], primero.id)
        self.assertEqual(logs.last().payload["repartidor_nuevo_id"], segundo.id)
        self.assertEqual(logs.last().user_id, self.manager.id)
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)

    def test_asignar_repartidor_no_promueve_pendiente_point_a_listo(self):
        repartidor = self._repartidor("rep_sin_point")

        result = assign_domicilio(
            solicitud_id=self.solicitud.id,
            repartidor_id=repartidor.id,
            audit_user=self.manager,
        )

        self.solicitud.refresh_from_db()
        self.assertEqual(
            self.solicitud.estatus,
            SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
        )
        self.assertEqual(
            result["estatus"],
            SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
        )

    def test_repeticion_idempotente_conserva_estado_terminal_sin_efectos(self):
        repartidor = self._repartidor("rep_entregado")
        reemplazo = self._repartidor("rep_reemplazo_terminal")
        SolicitudDomicilio._base_manager.filter(pk=self.solicitud.pk).update(
            repartidor=repartidor,
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            legacy_without_point=True,
        )
        self.solicitud.refresh_from_db()
        url = reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id])

        response = self.client.post(
            url,
            {"repartidor_id": repartidor.id, "unidad_id": repartidor.unidad_asignada_id},
            format="json",
        )
        cambio_real = self.client.post(
            url,
            {"repartidor_id": reemplazo.id, "unidad_id": reemplazo.unidad_asignada_id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["idempotent"])
        self.assertEqual(cambio_real.status_code, status.HTTP_409_CONFLICT)
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_ENTREGADO)
        self.assertEqual(self.solicitud.repartidor_id, repartidor.id)
        self.assertEqual(AuditLog.objects.count(), 0)
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)

    def test_repeticion_idempotente_acepta_repartidor_que_quedo_inactivo(self):
        repartidor = self._repartidor("rep_asignado_inactivo")
        SolicitudDomicilio._base_manager.filter(pk=self.solicitud.pk).update(
            repartidor=repartidor,
            estatus=SolicitudDomicilio.ESTATUS_CANCELADO,
            legacy_without_point=True,
        )
        self.solicitud.refresh_from_db()
        repartidor.user.is_active = False
        repartidor.user.save(update_fields=["is_active"])
        url = reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id])

        response = self.client.post(
            url,
            {"repartidor_id": repartidor.id, "unidad_id": repartidor.unidad_asignada_id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["idempotent"])
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_CANCELADO)
        self.assertEqual(self.solicitud.repartidor_id, repartidor.id)
        self.assertEqual(AuditLog.objects.count(), 0)
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)

    def test_rechaza_sin_permisos_y_repartidor_no_disponible(self):
        repartidor = self._repartidor("rep_no_disponible")
        repartidor.user.is_active = False
        repartidor.user.save(update_fields=["is_active"])
        user = User.objects.create_user(username="sin_permiso", password="pass123")
        self.client.force_authenticate(user)

        catalog = self.client.get(reverse("api_logistica_repartidores_disponibles"))
        assign = self.client.post(
            reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id]),
            {"repartidor_id": repartidor.id},
            format="json",
        )

        self.assertEqual(catalog.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(assign.status_code, status.HTTP_403_FORBIDDEN)

    def test_ruta_html_legacy_no_asigna_y_redirige_a_bandeja_canonica(self):
        self.client.force_login(self.manager)
        repartidor = self._repartidor("rep_html")
        unidad = Unidad.objects.create(
            codigo="DOM-HTML",
            descripcion="Unidad HTML",
            sucursal=self.sucursal,
        )

        response = self.client.post(
            reverse("logistica:domicilios_generales"),
            {
                "accion": "asignar",
                "solicitud_id": self.solicitud.id,
                "repartidor": repartidor.id,
                "unidad_operativa": unidad.id,
            },
        )

        self.assertRedirects(
            response,
            reverse("crm:pedidos_domicilios"),
            fetch_redirect_response=False,
        )
        self.solicitud.refresh_from_db()
        self.assertIsNone(self.solicitud.repartidor_id)
        self.assertIsNone(self.solicitud.unidad_id)
        self.assertEqual(self.solicitud.revision, 0)
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_asignacion_html_no_resetea_terminal(self):
        self.client.force_login(self.manager)
        actual = self._repartidor("rep_html_terminal")
        reemplazo = self._repartidor("rep_html_reemplazo")
        unidad = Unidad.objects.create(
            codigo="DOM-TERM",
            descripcion="Unidad terminal",
            sucursal=self.sucursal,
        )
        SolicitudDomicilio._base_manager.filter(pk=self.solicitud.pk).update(
            repartidor=actual,
            unidad=unidad,
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            legacy_without_point=True,
            revision=7,
        )
        self.solicitud.refresh_from_db()

        response = self.client.post(
            reverse("logistica:domicilios_generales"),
            {
                "accion": "asignar",
                "solicitud_id": self.solicitud.id,
                "repartidor": reemplazo.id,
                "unidad_operativa": unidad.id,
            },
        )

        self.assertRedirects(
            response,
            reverse("crm:pedidos_domicilios"),
            fetch_redirect_response=False,
        )
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.repartidor_id, actual.id)
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_ENTREGADO)
        self.assertEqual(self.solicitud.revision, 7)
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_ruta_html_legacy_no_ofrece_catalogos_ni_acepta_asignaciones(self):
        self.client.force_login(self.manager)
        disponible = self._repartidor("rep_html_disponible")
        tecnico = self._repartidor(
            "rep_html_tecnico",
            tipo_identidad=Repartidor.TIPO_CUENTA_TECNICA,
        )
        baja = self._repartidor("rep_html_baja")
        Empleado.objects.create(
            codigo="EMP-HTML-BAJA",
            nombre="Baja HTML",
            usuario_erp=baja.user,
            activo=False,
        )
        unidad = Unidad.objects.create(
            codigo="DOM-FILTER",
            descripcion="Unidad filtro",
            sucursal=self.sucursal,
        )

        page = self.client.get(reverse("logistica:domicilios_generales"))
        self.assertRedirects(
            page,
            reverse("crm:pedidos_domicilios"),
            fetch_redirect_response=False,
        )
        self.assertIsNone(page.context)
        rejected = self.client.post(
            reverse("logistica:domicilios_generales"),
            {
                "accion": "asignar",
                "solicitud_id": self.solicitud.id,
                "repartidor": tecnico.id,
                "unidad_operativa": unidad.id,
            },
        )
        self.assertEqual(rejected.status_code, status.HTTP_302_FOUND)
        self.assertEqual(rejected.url, reverse("crm:pedidos_domicilios"))
        self.solicitud.refresh_from_db()
        self.assertIsNone(self.solicitud.repartidor_id)
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_cambio_solo_unidad_audita_snapshot_canonico_y_retry_no_audita(self):
        repartidor = self._repartidor("rep_unit_only")
        anterior = Unidad.objects.create(
            codigo="DOM-U1",
            descripcion="Unidad anterior",
            sucursal=self.sucursal,
        )
        nueva = Unidad.objects.create(
            codigo="DOM-U2",
            descripcion="Unidad nueva",
            sucursal=self.sucursal,
        )
        self.solicitud.repartidor = repartidor
        self.solicitud.unidad = anterior
        self.solicitud.save(update_fields=["repartidor", "unidad"])

        changed = assign_domicilio(
            solicitud_id=self.solicitud.id,
            repartidor_id=repartidor.id,
            unidad=nueva,
            audit_user=self.manager,
            audit_metadata={
                "unidad_anterior_id": 999999,
                "unidad_nueva_codigo": "FALSO",
            },
        )
        retried = assign_domicilio(
            solicitud_id=self.solicitud.id,
            repartidor_id=repartidor.id,
            unidad=nueva,
            audit_user=self.manager,
        )

        self.assertFalse(changed["idempotent"])
        self.assertTrue(retried["idempotent"])
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.unidad_id, nueva.id)
        self.assertEqual(self.solicitud.revision, 1)
        audit = AuditLog.objects.get(action="ASSIGN")
        self.assertEqual(audit.payload["unidad_anterior_id"], anterior.id)
        self.assertEqual(audit.payload["unidad_anterior_codigo"], anterior.codigo)
        self.assertEqual(audit.payload["unidad_nueva_id"], nueva.id)
        self.assertEqual(audit.payload["unidad_nueva_codigo"], nueva.codigo)
        self.assertEqual(AuditLog.objects.count(), 1)


class SolicitudDomicilioCanonicalMigrationTests(TransactionTestCase):
    migrate_from = [
        ("crm", "0007_pedidocliente_point_note_fetched_at_and_more"),
        ("logistica", "0045_alter_solicituddomicilio_canal_origen"),
    ]
    migrate_to = [("logistica", "0046_solicituddomicilio_operacion_canonica")]

    def setUp(self):
        super().setUp()
        executor = MigrationExecutor(connection)
        executor.migrate(self.migrate_from)
        old_apps = executor.loader.project_state(self.migrate_from).apps

        ClienteOld = old_apps.get_model("crm", "Cliente")
        DireccionOld = old_apps.get_model("crm", "DireccionCliente")
        PedidoOld = old_apps.get_model("crm", "PedidoCliente")
        SolicitudOld = old_apps.get_model("logistica", "SolicitudDomicilio")

        cliente = ClienteOld.objects.create(nombre="Migración real")
        direccion = DireccionOld.objects.create(
            cliente=cliente,
            direccion="Calle con GPS",
            latitud="25.790001",
            longitud="-108.990001",
        )
        pedido = PedidoOld.objects.create(
            cliente=cliente,
            descripcion="Nota Point histórica",
            point_note_id="POINT-MIGRATION-EXECUTOR",
            point_note_snapshot={"pk_nota": "POINT-MIGRATION-EXECUTOR"},
        )
        self.ids = {
            "ready": SolicitudOld.objects.create(
                pedido_cliente=pedido,
                direccion_cliente=direccion,
                cliente_nombre="Con GPS",
                direccion=direccion.direccion,
                estatus="ASIGNADO",
            ).pk,
            "not_ready": SolicitudOld.objects.create(
                pedido_cliente=pedido,
                cliente_nombre="Sin GPS",
                direccion="Sin GPS",
                estatus="ASIGNADO",
            ).pk,
            "active_legacy": SolicitudOld.objects.create(
                cliente_nombre="Activo legado",
                direccion="Sin Point",
                estatus="EN_RUTA",
            ).pk,
            "cancelled_legacy": SolicitudOld.objects.create(
                cliente_nombre="Cancelado legado",
                direccion="Sin Point",
                estatus="CANCELADO",
            ).pk,
            "point_delivered_incomplete": SolicitudOld.objects.create(
                pedido_cliente=pedido,
                cliente_nombre="Point entregado histórico",
                direccion="Sin evidencia nueva",
                estatus="ENTREGADO",
            ).pk,
            "point_cancelled_incomplete": SolicitudOld.objects.create(
                pedido_cliente=pedido,
                cliente_nombre="Point cancelado histórico",
                direccion="Sin motivo nuevo",
                estatus="CANCELADO",
            ).pk,
        }

        executor = MigrationExecutor(connection)
        executor.migrate(self.migrate_to)
        self.new_apps = executor.loader.project_state(self.migrate_to).apps

    def tearDown(self):
        executor = MigrationExecutor(connection)
        apps = executor.loader.project_state(self.migrate_to).apps
        Solicitud = apps.get_model("logistica", "SolicitudDomicilio")
        linked_ids = list(
            Solicitud.objects.exclude(pedido_cliente_id=None)
            .order_by("id")
            .values_list("id", flat=True)
        )
        if len(linked_ids) > 1:
            Solicitud.objects.filter(id__in=linked_ids[1:]).update(
                pedido_cliente_id=None,
            )
        executor.migrate(executor.loader.graph.leaf_nodes())
        super().tearDown()

    def test_forward_mapping_preserves_terminals_and_never_creates_invalid_ready(self):
        SolicitudNew = self.new_apps.get_model("logistica", "SolicitudDomicilio")

        self.assertEqual(
            SolicitudNew.objects.get(pk=self.ids["ready"]).estatus,
            "LISTO",
        )
        self.assertEqual(
            SolicitudNew.objects.get(pk=self.ids["not_ready"]).estatus,
            "CONFIRMADO",
        )
        self.assertEqual(
            SolicitudNew.objects.get(pk=self.ids["active_legacy"]).estatus,
            "PENDIENTE_POINT",
        )
        cancelled = SolicitudNew.objects.get(pk=self.ids["cancelled_legacy"])
        self.assertEqual(cancelled.estatus, "CANCELADO")
        self.assertTrue(cancelled.legacy_without_point)
        point_delivered = SolicitudNew.objects.get(
            pk=self.ids["point_delivered_incomplete"]
        )
        point_cancelled = SolicitudNew.objects.get(
            pk=self.ids["point_cancelled_incomplete"]
        )
        self.assertEqual(point_delivered.estatus, "ENTREGADO")
        self.assertTrue(point_delivered.legacy_without_point)
        self.assertEqual(point_cancelled.estatus, "CANCELADO")
        self.assertTrue(point_cancelled.legacy_without_point)

    def test_reverse_mapping_is_explicitly_lossy_but_preserves_terminals(self):
        migration = import_module(
            "logistica.migrations.0046_solicituddomicilio_operacion_canonica"
        )
        migration.reverse_delivery_states(self.new_apps, None)
        SolicitudNew = self.new_apps.get_model("logistica", "SolicitudDomicilio")

        self.assertEqual(
            SolicitudNew.objects.get(pk=self.ids["ready"]).estatus,
            "ASIGNADO",
        )
        self.assertEqual(
            SolicitudNew.objects.get(pk=self.ids["not_ready"]).estatus,
            "PENDIENTE",
        )
        self.assertEqual(
            SolicitudNew.objects.get(pk=self.ids["active_legacy"]).estatus,
            "PENDIENTE",
        )
        self.assertEqual(
            SolicitudNew.objects.get(
                pk=self.ids["point_delivered_incomplete"]
            ).estatus,
            "ENTREGADO",
        )
        self.assertEqual(
            SolicitudNew.objects.get(
                pk=self.ids["point_cancelled_incomplete"]
            ).estatus,
            "CANCELADO",
        )
