from concurrent.futures import ThreadPoolExecutor

from django.contrib.auth.models import User
from django.core.cache import cache
from django.db import close_old_connections
from django.test import TransactionTestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import AuditLog, Notificacion, Sucursal
from crm.models import Cliente, DireccionCliente, PedidoCliente
from integraciones.models import PublicApiAccessLog, PublicApiClient
from logistica.models import (
    EntregaEcommerce,
    Repartidor,
    SolicitudDomicilio,
    SolicitudDomicilioStatusOperation,
    Unidad,
)
from logistica.services_domicilio_assignment import (
    DomicilioAssignmentError,
    assign_domicilio,
)
from logistica.services_domicilio_status import (
    DomicilioStatusError,
    update_domicilio_status,
)


class PublicLogisticaAssignmentApiTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.api_client, self.raw_key = PublicApiClient.create_with_generated_key(
            nombre="Maya logística",
        )
        self.api_client.capabilities = [
            PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
        ]
        self.api_client.save(update_fields=["capabilities"])
        self.auth = {"HTTP_X_API_KEY": self.raw_key}
        self.other_client, self.other_key = PublicApiClient.create_with_generated_key(
            nombre="Otro integrador logístico",
        )
        self.other_client.capabilities = [
            PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
        ]
        self.other_client.save(update_fields=["capabilities"])
        self.sucursal = Sucursal.objects.create(
            codigo="M2M-LOG",
            nombre="M2M Logística",
        )
        cliente = Cliente.objects.create(
            nombre="Cliente M2M",
            telefono="6670000000",
        )
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido M2M",
            public_api_client=self.api_client,
        )
        self.solicitud = SolicitudDomicilio.objects.create(
            pedido_cliente=pedido,
            cliente_nombre="Cliente M2M",
            direccion="Calle M2M 10",
        )
        self.catalog_url = reverse(
            "api_public_logistica_repartidores_disponibles"
        )
        self.assign_url = reverse(
            "api_public_logistica_domicilio_asignar",
            args=[self.solicitud.id],
        )

    def _repartidor(self, username):
        user = User.objects.create_user(username=username, password="pass123")
        unidad = Unidad.objects.create(
            codigo=f"U-{username}",
            descripcion=f"Unidad {username}",
            sucursal=self.sucursal,
        )
        return Repartidor.objects.create(
            user=user,
            sucursal=self.sucursal,
            unidad_asignada=unidad,
        )

    def _payload(self, repartidor):
        return {
            "repartidor_id": repartidor.id,
            "unidad_id": repartidor.unidad_asignada_id,
            "actor": {"id": "ops-17", "nombre": "Operaciones"},
        }

    def _allow(self, *repartidores):
        self.api_client.repartidores_logistica_autorizados.add(*repartidores)

    def test_requiere_api_key_valida_y_capability_opt_in(self):
        missing = self.client.get(self.catalog_url)
        invalid = self.client.get(
            self.catalog_url,
            {"solicitud_id": self.solicitud.id},
            HTTP_X_API_KEY="pk_invalid",
        )
        capture_client, capture_key = PublicApiClient.create_with_generated_key(
            nombre="Captura omnicanal",
        )
        capture_client.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        capture_client.save(update_fields=["capabilities"])
        forbidden = self.client.get(
            self.catalog_url,
            {"solicitud_id": self.solicitud.id},
            HTTP_X_API_KEY=capture_key,
        )
        forbidden_assignment = self.client.post(
            self.assign_url,
            {
                "repartidor_id": 1,
                "actor": {"id": "ops-1", "nombre": "Operaciones"},
            },
            format="json",
            HTTP_X_API_KEY=capture_key,
        )

        self.assertEqual(missing.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(invalid.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(forbidden.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            forbidden_assignment.status_code,
            status.HTTP_403_FORBIDDEN,
        )
        self.assertEqual(
            forbidden.data["code"],
            "LOGISTICA_ASSIGNMENT_CAPABILITY_REQUIRED",
        )
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=capture_client,
                status_code=status.HTTP_403_FORBIDDEN,
            ).count(),
            2,
        )
        self.solicitud.refresh_from_db()
        self.assertIsNone(self.solicitud.repartidor_id)

    def test_catalogo_scoped_reutiliza_reglas_y_envuelve_results(self):
        disponible = self._repartidor("m2m_disponible")
        fuera_scope = self._repartidor("m2m_fuera_scope")
        inactivo = self._repartidor("m2m_inactivo")
        inactivo.user.is_active = False
        inactivo.user.save(update_fields=["is_active"])
        self._allow(disponible, inactivo)

        response = self.client.get(
            self.catalog_url,
            {"solicitud_id": self.solicitud.id},
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            [item["id"] for item in response.data["results"]],
            [disponible.id],
        )
        self.assertNotIn(
            fuera_scope.id,
            [item["id"] for item in response.data["results"]],
        )
        self.assertEqual(set(response.data), {"results"})
        self.assertEqual(
            set(response.data["results"][0]),
            {"id", "nombre"},
        )
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=self.api_client,
                endpoint=self.catalog_url,
                method="GET",
                status_code=status.HTTP_200_OK,
            ).count(),
            1,
        )

    def test_asignacion_retry_y_reasignacion_auditada_sin_pii_excesiva(self):
        primero = self._repartidor("m2m_primero")
        segundo = self._repartidor("m2m_segundo")
        self._allow(primero, segundo)
        payload = self._payload(primero)
        payload["actor"]["email"] = "no-guardar@example.com"
        payload["actor"]["telefono"] = "6671234567"

        first = self.client.post(
            self.assign_url,
            payload,
            format="json",
            **self.auth,
        )
        repeated = self.client.post(
            self.assign_url,
            payload,
            format="json",
            **self.auth,
        )
        reassigned = self.client.post(
            self.assign_url,
            self._payload(segundo),
            format="json",
            **self.auth,
        )

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertFalse(first.data["idempotent"])
        self.assertEqual(repeated.status_code, status.HTTP_200_OK)
        self.assertTrue(repeated.data["idempotent"])
        self.assertEqual(reassigned.status_code, status.HTTP_200_OK)
        logs = AuditLog.objects.filter(
            model="logistica.SolicitudDomicilio",
            object_id=str(self.solicitud.id),
            action="ASSIGN",
        ).order_by("timestamp")
        self.assertEqual(logs.count(), 2)
        self.assertIsNone(logs.last().user_id)
        self.assertEqual(
            logs.last().payload["repartidor_anterior_id"],
            primero.id,
        )
        self.assertEqual(
            logs.last().payload["repartidor_nuevo_id"],
            segundo.id,
        )
        self.assertEqual(
            logs.last().payload["api_client"],
            {"id": self.api_client.id, "nombre": "Maya logística"},
        )
        self.assertEqual(
            logs.last().payload["actor_externo"],
            {"id": "ops-17", "nombre": "Operaciones"},
        )
        self.assertNotIn("email", str(logs.first().payload).lower())
        self.assertNotIn("6671234567", str(logs.first().payload))
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=self.api_client,
                endpoint=self.assign_url,
                method="POST",
                status_code=status.HTTP_200_OK,
            ).count(),
            3,
        )

    def test_retry_exacto_terminal_e_inactivo_permanece_idempotente(self):
        repartidor = self._repartidor("m2m_terminal")
        self._allow(repartidor)
        SolicitudDomicilio._base_manager.filter(pk=self.solicitud.pk).update(
            repartidor=repartidor,
            estatus=SolicitudDomicilio.ESTATUS_CANCELADO,
            legacy_without_point=True,
        )
        self.solicitud.refresh_from_db()
        repartidor.user.is_active = False
        repartidor.user.save(update_fields=["is_active"])

        response = self.client.post(
            self.assign_url,
            self._payload(repartidor),
            format="json",
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["idempotent"])
        self.solicitud.refresh_from_db()
        self.assertEqual(
            self.solicitud.estatus,
            SolicitudDomicilio.ESTATUS_CANCELADO,
        )
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_cross_client_y_ownerless_son_404_incluso_en_retry_exacto(self):
        repartidor = self._repartidor("m2m_owned")
        self._allow(repartidor)
        self.solicitud.repartidor = repartidor
        self.solicitud.save(update_fields=["repartidor"])
        ownerless = SolicitudDomicilio.objects.create(
            cliente_nombre="Interno",
            direccion="Sin owner",
            repartidor=repartidor,
        )

        cross_catalog = self.client.get(
            self.catalog_url,
            {"solicitud_id": self.solicitud.id},
            HTTP_X_API_KEY=self.other_key,
        )
        cross_retry = self.client.post(
            self.assign_url,
            self._payload(repartidor),
            format="json",
            HTTP_X_API_KEY=self.other_key,
        )
        ownerless_catalog = self.client.get(
            self.catalog_url,
            {"solicitud_id": ownerless.id},
            **self.auth,
        )
        ownerless_retry = self.client.post(
            reverse(
                "api_public_logistica_domicilio_asignar",
                args=[ownerless.id],
            ),
            self._payload(repartidor),
            format="json",
            **self.auth,
        )

        self.assertEqual(cross_catalog.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(cross_retry.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(ownerless_catalog.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(ownerless_retry.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_catalogo_requiere_solicitud_owned_no_es_global(self):
        response = self.client.get(self.catalog_url, **self.auth)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["detail"], "solicitud_id es obligatorio.")

    def test_allowlist_vacia_falla_cerrado_y_fuera_scope_no_asigna(self):
        permitido = self._repartidor("m2m_permitido")
        fuera_scope = self._repartidor("m2m_no_permitido")

        empty_catalog = self.client.get(
            self.catalog_url,
            {"solicitud_id": self.solicitud.id},
            **self.auth,
        )
        self.solicitud.repartidor = fuera_scope
        self.solicitud.save(update_fields=["repartidor"])
        rejected = self.client.post(
            self.assign_url,
            self._payload(fuera_scope),
            format="json",
            **self.auth,
        )
        self._allow(permitido)
        scoped_catalog = self.client.get(
            self.catalog_url,
            {"solicitud_id": self.solicitud.id},
            **self.auth,
        )

        self.assertEqual(empty_catalog.status_code, status.HTTP_200_OK)
        self.assertEqual(empty_catalog.data, {"results": []})
        self.assertEqual(rejected.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(rejected.data, {"detail": "Repartidor no disponible."})
        self.assertEqual(
            [item["id"] for item in scoped_catalog.data["results"]],
            [permitido.id],
        )
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.repartidor_id, fuera_scope.id)
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_solicitud_id_sobredimensionado_retorna_400_y_access_log(self):
        response = self.client.get(
            self.catalog_url,
            {"solicitud_id": "9" * 5000},
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=self.api_client,
                endpoint=self.catalog_url,
                method="GET",
                status_code=status.HTTP_400_BAD_REQUEST,
            ).count(),
            1,
        )

    def test_actor_minimo_es_obligatorio_y_error_queda_auditado(self):
        repartidor = self._repartidor("m2m_actor_required")

        response = self.client.post(
            self.assign_url,
            {"repartidor_id": repartidor.id},
            format="json",
            **self.auth,
        )
        boolean_id = self.client.post(
            self.assign_url,
            {
                "repartidor_id": True,
                "actor": {"id": "ops-17", "nombre": "Operaciones"},
            },
            format="json",
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(boolean_id.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(AuditLog.objects.count(), 0)
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=self.api_client,
                status_code=status.HTTP_400_BAD_REQUEST,
            ).count(),
            2,
        )


class PublicLogisticaDriverExecutionApiTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.api_client, key = PublicApiClient.create_with_generated_key(
            nombre="Maya ejecución",
        )
        self.api_client.capabilities = [
            PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
        ]
        self.api_client.save(update_fields=["capabilities"])
        self.auth = {"HTTP_X_API_KEY": key}
        sucursal = Sucursal.objects.create(codigo="M2M-EXEC", nombre="M2M Exec")
        user = User.objects.create_user(username="driver_exec", password="x")
        self.unidad = Unidad.objects.create(
            codigo="M2M-EXEC-U",
            descripcion="Unidad ejecución",
            sucursal=sucursal,
        )
        self.driver = Repartidor.objects.create(
            user=user,
            sucursal=sucursal,
            unidad_asignada=self.unidad,
        )
        self.api_client.repartidores_logistica_autorizados.add(self.driver)
        cliente = Cliente.objects.create(
            nombre="Cliente visible",
            telefono="6671230000",
            email="oculto@example.com",
            notas="nota interna secreta",
        )
        direccion = DireccionCliente.objects.create(
            cliente=cliente,
            direccion="Av. Visible 123",
            referencias="Portón azul",
            latitud="24.809100",
            longitud="-107.394000",
            place_id="place-public",
        )
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            direccion_entrega=direccion,
            descripcion="Pastel de entrega",
            canal=PedidoCliente.CANAL_WHATSAPP,
            public_api_client=self.api_client,
            payload_snapshot={"token": "no-exponer"},
            point_note_id="POINT-EXEC-1",
            point_note_snapshot={"pk_nota": "POINT-EXEC-1"},
        )
        self.solicitud = SolicitudDomicilio.objects.create(
            pedido_cliente=pedido,
            cliente=cliente,
            direccion_cliente=direccion,
            cliente_nombre=cliente.nombre,
            cliente_telefono=cliente.telefono,
            direccion=direccion.direccion,
            notas="otra nota interna",
            repartidor=self.driver,
            unidad=self.unidad,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_PREPARANDO
        self.solicitud.save(update_fields=["estatus"])
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_LISTO
        self.solicitud.save(update_fields=["estatus"])
        self.list_url = reverse(
            "api_public_logistica_repartidor_domicilios",
            args=[self.driver.id],
        )
        self.status_url = reverse(
            "api_public_logistica_domicilio_estatus",
            args=[self.solicitud.id],
        )

    def payload(self, operation_id="48bf68a2-8642-4fd8-bc25-eaa23a0e5a84"):
        return {
            "repartidor_id": self.driver.id,
            "estatus": "EN_RUTA",
            "operation_id": operation_id,
            "actor": {"id": "app-driver", "nombre": "App repartidor"},
        }

    def test_lista_minima_incluye_gps_y_excluye_pii_notas_y_terminales(self):
        response = self.client.get(self.list_url, **self.auth)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        item = response.data["results"][0]
        self.assertEqual(
            set(item),
            {
                "id", "estatus", "revision", "cliente_nombre",
                "cliente_telefono", "direccion", "referencias", "latitud",
                "longitud", "place_id", "descripcion", "canal",
                "fecha_compromiso",
            },
        )
        rendered = str(response.data)
        self.assertNotIn("oculto@example.com", rendered)
        self.assertNotIn("secreta", rendered)
        self.assertNotIn("no-exponer", rendered)
        SolicitudDomicilio._base_manager.filter(pk=self.solicitud.pk).update(
            estatus=SolicitudDomicilio.ESTATUS_ENTREGADO,
            legacy_without_point=True,
        )
        self.solicitud.refresh_from_db()
        terminal = self.client.get(self.list_url, **self.auth)
        self.assertEqual(terminal.data, {"results": []})

    def test_operacion_durable_replay_exacto_y_mismatch(self):
        first = self.client.post(
            self.status_url, self.payload(), format="json", **self.auth
        )
        replay = self.client.post(
            self.status_url, self.payload(), format="json", **self.auth
        )
        mismatch_payload = self.payload()
        mismatch_payload["actor"]["nombre"] = "Otro actor"
        mismatch = self.client.post(
            self.status_url, mismatch_payload, format="json", **self.auth
        )

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertFalse(first.data["idempotent"])
        self.assertEqual(replay.status_code, status.HTTP_200_OK)
        self.assertTrue(replay.data["idempotent"])
        self.assertEqual(mismatch.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(SolicitudDomicilioStatusOperation.objects.count(), 1)
        self.assertEqual(
            AuditLog.objects.filter(action="STATUS_CHANGE").count(), 1
        )
        self.assertNotIn("latitud", str(AuditLog.objects.get().payload))

    def test_reasignada_transicion_invalida_y_foreign_fallan_cerrado(self):
        other_user = User.objects.create_user(username="other_driver", password="x")
        other = Repartidor.objects.create(
            user=other_user,
            sucursal=self.driver.sucursal,
        )
        self.solicitud.repartidor = other
        self.solicitud.save(update_fields=["repartidor"])
        reassigned = self.client.post(
            self.status_url, self.payload(), format="json", **self.auth
        )
        self.assertEqual(reassigned.status_code, status.HTTP_409_CONFLICT)

        foreign_client, foreign_key = PublicApiClient.create_with_generated_key(
            nombre="Foreign",
        )
        foreign_client.capabilities = [
            PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
        ]
        foreign_client.save(update_fields=["capabilities"])
        foreign = self.client.post(
            self.status_url,
            self.payload(),
            format="json",
            HTTP_X_API_KEY=foreign_key,
        )
        self.assertEqual(foreign.status_code, status.HTTP_404_NOT_FOUND)

        SolicitudDomicilio._base_manager.filter(pk=self.solicitud.pk).update(
            repartidor=self.driver,
            estatus=SolicitudDomicilio.ESTATUS_PENDIENTE,
        )
        self.solicitud.refresh_from_db()
        invalid = self.client.post(
            self.status_url,
            self.payload("f3a1d1ef-8996-4670-8fe2-094e4a6a1d56"),
            format="json",
            **self.auth,
        )
        self.assertEqual(invalid.status_code, status.HTTP_409_CONFLICT)

        SolicitudDomicilio._base_manager.filter(pk=self.solicitud.pk).update(
            estatus=SolicitudDomicilio.ESTATUS_ASIGNADO_LEGACY,
        )
        self.solicitud.refresh_from_db()
        legacy_assigned = self.client.post(
            self.status_url,
            self.payload("4a0e139e-b87d-48a5-a3ec-218a8bb63d53"),
            format="json",
            **self.auth,
        )
        self.assertEqual(legacy_assigned.status_code, status.HTTP_409_CONFLICT)


class PublicLogisticaExecutionConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.api_client, _ = PublicApiClient.create_with_generated_key(
            nombre="Maya concurrencia",
        )
        self.api_client.capabilities = [
            PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
        ]
        self.api_client.save(update_fields=["capabilities"])
        sucursal = Sucursal.objects.create(codigo="M2M-RACE", nombre="Race")
        self.drivers = []
        for index in range(2):
            user = User.objects.create_user(username=f"race_{index}", password="x")
            unidad = Unidad.objects.create(
                codigo=f"RACE-U-{index}",
                descripcion=f"Race {index}",
                sucursal=sucursal,
            )
            self.drivers.append(
                Repartidor.objects.create(
                    user=user,
                    sucursal=sucursal,
                    unidad_asignada=unidad,
                )
            )
        self.api_client.repartidores_logistica_autorizados.add(*self.drivers)
        cliente = Cliente.objects.create(nombre="Race")
        direccion = DireccionCliente.objects.create(
            cliente=cliente,
            direccion="Race",
            latitud="24.809100",
            longitud="-107.394000",
        )
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            direccion_entrega=direccion,
            descripcion="Race",
            public_api_client=self.api_client,
            point_note_id="POINT-RACE-1",
            point_note_snapshot={"pk_nota": "POINT-RACE-1"},
        )
        self.solicitud = SolicitudDomicilio.objects.create(
            pedido_cliente=pedido,
            direccion_cliente=direccion,
            cliente_nombre="Race",
            direccion="Race",
            repartidor=self.drivers[0],
            unidad=self.drivers[0].unidad_asignada,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_PREPARANDO
        self.solicitud.save(update_fields=["estatus"])
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_LISTO
        self.solicitud.save(update_fields=["estatus"])

    def _status(self, operation_id):
        close_old_connections()
        try:
            return update_domicilio_status(
                solicitud_id=self.solicitud.id,
                api_client=self.api_client,
                repartidor_id=self.drivers[0].id,
                requested_status=SolicitudDomicilio.ESTATUS_EN_RUTA,
                operation_id=operation_id,
                actor={"id": "race", "nombre": "Race"},
            )
        finally:
            close_old_connections()

    def test_mismo_operation_id_concurrente_muta_y_audita_una_vez(self):
        operation_id = "8841e703-fadd-4a76-86bb-06388a6f61af"
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(self._status, [operation_id, operation_id]))

        self.assertEqual(sorted(item["idempotent"] for item in results), [False, True])
        self.assertEqual(SolicitudDomicilioStatusOperation.objects.count(), 1)
        self.assertEqual(
            AuditLog.objects.filter(action="STATUS_CHANGE").count(), 1
        )

    def test_operation_id_distintos_concurrentes_segundo_es_idempotente(self):
        operation_ids = [
            "355aa71e-872b-46df-b476-19105fd3a050",
            "79258634-b608-4b1a-81b1-4d6388a79f0b",
        ]

        def attempt(operation_id):
            try:
                return self._status(operation_id)
            except DomicilioStatusError as exc:
                return exc.status_code

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(attempt, operation_ids))

        self.assertEqual(
            len([item for item in results if isinstance(item, dict)]),
            2,
        )
        self.assertEqual(
            sorted(item["idempotent"] for item in results),
            [False, True],
        )
        self.assertEqual(SolicitudDomicilioStatusOperation.objects.count(), 1)
        self.assertEqual(
            AuditLog.objects.filter(action="STATUS_CHANGE").count(), 1
        )

    def test_reasignacion_y_estatus_comparten_lock_y_no_se_cruzan(self):
        def reassign():
            close_old_connections()
            try:
                return assign_domicilio(
                    solicitud_id=self.solicitud.id,
                    repartidor_id=self.drivers[1].id,
                    owner_api_client=self.api_client,
                )
            except DomicilioAssignmentError:
                return "conflict"
            finally:
                close_old_connections()

        def status_change():
            try:
                return self._status("f90c4eb5-e018-4280-ae7a-f4a30b39ea46")
            except DomicilioStatusError:
                return "conflict"

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = [
                executor.submit(reassign),
                executor.submit(status_change),
            ]
            results = [future.result() for future in outcomes]

        self.solicitud.refresh_from_db()
        successful = [item for item in results if item != "conflict"]
        self.assertEqual(len(successful), 1)
        self.assertIn(
            (self.solicitud.repartidor_id, self.solicitud.estatus),
            {
                (self.drivers[1].id, SolicitudDomicilio.ESTATUS_LISTO),
                (self.drivers[0].id, SolicitudDomicilio.ESTATUS_EN_RUTA),
            },
        )
