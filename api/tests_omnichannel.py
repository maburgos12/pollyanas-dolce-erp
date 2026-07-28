from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from threading import Barrier, Thread
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.db import close_old_connections, connection
from django.test import TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient, APITestCase

from crm.models import Cliente, DireccionCliente, PedidoCliente
from core.models import Sucursal
from integraciones.models import PublicApiAccessLog, PublicApiClient
from logistica.models import Repartidor, SolicitudDomicilio, Unidad
from pos_bridge.services.point_note_detail_service import (
    PointNote,
    PointNoteLine,
    PointNoteUnavailableError,
)
from pos_bridge.utils.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ExtractionError,
)


class OmnichannelPublicApiTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.public_client, self.raw_api_key = PublicApiClient.create_with_generated_key(
            nombre="Maya omnicanal",
        )
        self.public_client.capabilities = ["OMNICHANNEL"]
        self.public_client.save(update_fields=["capabilities"])
        self.auth = {"HTTP_X_API_KEY": self.raw_api_key}
        self.url = reverse("api_public_omnichannel_orders")
        self.search_url = reverse("api_public_omnichannel_customers")
        self.payload = {
            "external_source": "ECOMMERCE",
            "external_id": "order_123",
            "canal": "WEB",
            "cliente": {
                "nombre": "Ana Pérez",
                "telefono": "667 123-4567",
                "email": "ANA@example.com",
            },
            "direccion": {
                "direccion": "Av. Obregón 123",
                "referencias": "Portón blanco",
                "latitud": "24.809064",
                "longitud": "-107.394011",
                "place_id": "ChIJ...",
            },
            "pedido": {
                "descripcion": "Pastel chocolate",
                "fecha_compromiso": "2026-07-26",
                "monto_estimado": "850.00",
            },
        }

    def test_api_key_ausente_retorna_401(self):
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_api_key_invalida_retorna_401(self):
        response = self.client.post(
            self.url,
            self.payload,
            format="json",
            HTTP_X_API_KEY="pk_invalid",
        )
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_key_activa_sin_capability_no_puede_usar_vistas_omnichannel(self):
        unscoped_client, unscoped_key = PublicApiClient.create_with_generated_key(
            nombre="Integrador sin scope",
        )
        unscoped_auth = {"HTTP_X_API_KEY": unscoped_key}
        created = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(created.status_code, status.HTTP_201_CREATED)
        before_counts = (
            Cliente.objects.count(),
            DireccionCliente.objects.count(),
            PedidoCliente.objects.count(),
            SolicitudDomicilio.objects.count(),
        )

        responses = [
            self.client.post(self.url, self.payload, format="json", **unscoped_auth),
            self.client.get(self.search_url, {"q": "ana"}, **unscoped_auth),
            self.client.get(
                created.data["links"]["pedido_seguimiento"],
                **unscoped_auth,
            ),
        ]

        for response in responses:
            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
            self.assertEqual(response.data["code"], "OMNICHANNEL_CAPABILITY_REQUIRED")
            self.assertEqual(set(response.data), {"detail", "code"})
        self.assertEqual(
            before_counts,
            (
                Cliente.objects.count(),
                DireccionCliente.objects.count(),
                PedidoCliente.objects.count(),
                SolicitudDomicilio.objects.count(),
            ),
        )
        self.assertFalse(unscoped_client.capabilities)
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=unscoped_client,
                status_code=status.HTTP_403_FORBIDDEN,
            ).count(),
            3,
        )

    @override_settings(PUBLIC_API_RATE_LIMIT_PER_MINUTE=1)
    def test_rate_limit_existente_aplica_al_endpoint(self):
        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        second = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    def test_payload_incompleto_retorna_400(self):
        response = self.client.post(
            self.url,
            {"external_source": "ECOMMERCE"},
            format="json",
            **self.auth,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_web_order_persists_references_and_instructions_without_merging_them(self):
        payload = deepcopy(self.payload)
        payload["instrucciones_entrega"] = "Llamar al llegar"
        created = self.client.post(self.url, payload, format="json", **self.auth)
        delivery = SolicitudDomicilio.objects.get(pk=created.data["solicitud_domicilio_id"])
        detail = self.client.get(
            reverse("api_public_omnichannel_delivery_detail", kwargs={"solicitud_id": delivery.id}),
            **self.auth,
        )
        self.assertEqual(created.status_code, status.HTTP_201_CREATED)
        self.assertEqual(delivery.instrucciones_entrega, "Llamar al llegar")
        self.assertEqual(delivery.notas, "Referencias: Portón blanco\nInstrucciones: Llamar al llegar")
        self.assertEqual(detail.data["direccion"]["referencias"], "Portón blanco")
        self.assertEqual(detail.data["instrucciones_entrega"], "Llamar al llegar")

    def test_coordenadas_invalidas_retornan_400(self):
        self.payload["direccion"]["latitud"] = "91"
        response = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("latitud", response.data["direccion"])

    def test_canal_invalido_retorna_400(self):
        self.payload["canal"] = "TIKTOK"
        response = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("canal", response.data)

    def test_reintento_no_duplica_cliente_direccion_pedido_o_domicilio(self):
        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        second = self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertTrue(first.data["created"])
        self.assertFalse(second.data["created"])
        for field in (
            "cliente_id",
            "direccion_id",
            "pedido_id",
            "solicitud_domicilio_id",
        ):
            self.assertEqual(first.data[field], second.data[field])
        self.assertIn("pedido_seguimiento", first.data["links"])
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(DireccionCliente.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)

        cliente = Cliente.objects.get()
        self.assertEqual(cliente.telefono, "6671234567")
        self.assertEqual(cliente.email, "ana@example.com")
        solicitud = SolicitudDomicilio.objects.get()
        self.assertEqual(solicitud.cliente_nombre, "Ana Pérez")
        self.assertEqual(solicitud.cliente_telefono, "6671234567")
        self.assertEqual(solicitud.direccion, "Av. Obregón 123")
        self.assertEqual(solicitud.notas, "Referencias: Portón blanco")

    def test_misma_clave_con_payload_distinto_retorna_409(self):
        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.payload["pedido"]["descripcion"] = "Pedido materialmente diferente"

        response = self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(response.data["code"], "OMNICHANNEL_IDEMPOTENCY_CONFLICT")
        self.assertEqual(PedidoCliente.objects.count(), 1)

    def test_misma_clave_con_instrucciones_distintas_retorna_409_y_conserva_repartidor(self):
        self.payload["instrucciones_entrega"] = "Llamar al llegar"
        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.payload["instrucciones_entrega"] = "No tocar timbre"
        second = self.client.post(self.url, self.payload, format="json", **self.auth)
        delivery = SolicitudDomicilio.objects.get(pk=first.data["solicitud_domicilio_id"])
        self.assertEqual(second.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(delivery.instrucciones_entrega, "Llamar al llegar")
        self.assertEqual(delivery.notas, "Referencias: Portón blanco\nInstrucciones: Llamar al llegar")

    def test_misma_clave_con_instrucciones_normalizadas_sigue_idempotente(self):
        self.payload["instrucciones_entrega"] = "  Llamar al llegar  "
        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.payload["instrucciones_entrega"] = "Llamar al llegar"
        second = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)

    def test_snapshot_no_puede_modificarse_despues_de_crear_pedido(self):
        created = self.client.post(self.url, self.payload, format="json", **self.auth)
        order = PedidoCliente.objects.get(id=created.data["pedido_id"])
        original_snapshot = deepcopy(order.payload_snapshot)
        order.payload_snapshot = {"alterado": True}

        with self.assertRaises(ValidationError):
            order.save()

        order.refresh_from_db()
        self.assertEqual(order.payload_snapshot, original_snapshot)

    def test_payload_no_puede_inyectar_snapshot(self):
        self.payload["payload_snapshot"] = {"inyectado": "secreto"}
        response = self.client.post(self.url, self.payload, format="json", **self.auth)
        order = PedidoCliente.objects.get(id=response.data["pedido_id"])
        self.assertNotEqual(order.payload_snapshot, self.payload["payload_snapshot"])
        self.assertNotIn("inyectado", order.payload_snapshot)

    def test_pedido_legacy_sin_snapshot_ni_domicilio_retorna_409_estable(self):
        customer = Cliente.objects.create(nombre="Cliente legacy")
        PedidoCliente.objects.create(
            cliente=customer,
            external_source=self.payload["external_source"],
            external_id=self.payload["external_id"],
            payload_snapshot={},
            public_api_client=self.public_client,
            canal=self.payload["canal"],
            descripcion=self.payload["pedido"]["descripcion"],
        )

        response = self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(response.data["code"], "LEGACY_EXTERNAL_ORDER_CONFLICT")
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 0)

    def test_pedido_sin_owner_no_permite_inferir_si_snapshot_coincide(self):
        created = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(created.status_code, status.HTTP_201_CREATED)
        PedidoCliente.objects.filter(id=created.data["pedido_id"]).update(
            public_api_client=None,
        )
        before_counts = (
            Cliente.objects.count(),
            DireccionCliente.objects.count(),
            PedidoCliente.objects.count(),
            SolicitudDomicilio.objects.count(),
        )

        same = self.client.post(self.url, self.payload, format="json", **self.auth)
        different_payload = deepcopy(self.payload)
        different_payload["pedido"]["descripcion"] = "Contenido diferente"
        different = self.client.post(
            self.url,
            different_payload,
            format="json",
            **self.auth,
        )

        self.assertEqual(same.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(different.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(same.data["code"], "OMNICHANNEL_ORDER_OWNERSHIP_CONFLICT")
        self.assertEqual(different.data["code"], same.data["code"])
        self.assertEqual(set(same.data), {"detail", "code"})
        self.assertEqual(set(different.data), {"detail", "code"})
        order = PedidoCliente.objects.get(id=created.data["pedido_id"])
        self.assertIsNone(order.public_api_client_id)
        self.assertEqual(
            before_counts,
            (
                Cliente.objects.count(),
                DireccionCliente.objects.count(),
                PedidoCliente.objects.count(),
                SolicitudDomicilio.objects.count(),
            ),
        )

    def test_pedido_con_snapshot_coincidente_sin_domicilio_retorna_409_estable(self):
        created = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(created.status_code, status.HTTP_201_CREATED)
        SolicitudDomicilio.objects.all().delete()

        response = self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(response.data["code"], "OMNICHANNEL_ORDER_INCOMPLETE")
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(DireccionCliente.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 0)

    def test_retry_identico_reutilizando_cliente_con_datos_previos_retorna_200(self):
        existing = Cliente.objects.create(
            nombre="Ana Registro Anterior",
            telefono="(667) 123-4567",
            email="anterior@example.com",
        )

        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        second = self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data["cliente_id"], existing.id)
        self.assertEqual(first.data["pedido_id"], second.data["pedido_id"])
        existing.refresh_from_db()
        self.assertEqual(existing.nombre, "Ana Registro Anterior")
        self.assertEqual(existing.email, "anterior@example.com")

    def test_retry_identico_reutilizando_direccion_con_metadatos_previos_retorna_200(self):
        existing_customer = Cliente.objects.create(
            nombre="Ana Pérez",
            telefono="6671234567",
            email="ana@example.com",
        )
        existing_address = DireccionCliente.objects.create(
            cliente=existing_customer,
            direccion="  AV. OBREGON 123  ",
            referencias="Referencia histórica",
            latitud="24.800000",
            longitud="-107.300000",
            place_id="place-historico",
        )

        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        second = self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data["direccion_id"], existing_address.id)
        self.assertEqual(first.data["pedido_id"], second.data["pedido_id"])
        existing_address.refresh_from_db()
        self.assertEqual(existing_address.referencias, "Referencia histórica")
        self.assertEqual(existing_address.place_id, "place-historico")

    def test_link_seguimiento_es_publico_y_consumible_con_api_key(self):
        created = self.client.post(self.url, self.payload, format="json", **self.auth)
        tracking_url = created.data["links"]["pedido_seguimiento"]
        order = PedidoCliente.objects.get(id=created.data["pedido_id"])

        unauthorized = self.client.get(tracking_url)
        authorized = self.client.get(tracking_url, **self.auth)

        self.assertEqual(unauthorized.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(authorized.status_code, status.HTTP_200_OK)
        self.assertIsNotNone(order.tracking_token)
        self.assertIn(str(order.tracking_token), tracking_url)
        self.assertNotEqual(tracking_url.rstrip("/").split("/")[-1], str(order.id))
        self.assertEqual(authorized.data["pedido_id"], created.data["pedido_id"])
        self.assertEqual(authorized.data["estatus"], PedidoCliente.ESTATUS_NUEVO)
        self.assertNotIn("cliente", authorized.data)
        self.assertNotIn("direccion", authorized.data)
        self.assertNotIn("external_source", authorized.data)
        self.assertNotIn("external_id", authorized.data)

    def test_seguimiento_deniega_otra_api_key_sin_filtrar_existencia(self):
        created = self.client.post(self.url, self.payload, format="json", **self.auth)
        other_client, other_key = PublicApiClient.create_with_generated_key(
            nombre="Integrador ajeno",
        )
        other_client.capabilities = ["OMNICHANNEL"]
        other_client.save(update_fields=["capabilities"])

        response = self.client.get(
            created.data["links"]["pedido_seguimiento"],
            HTTP_X_API_KEY=other_key,
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertNotEqual(other_client.id, self.public_client.id)

    def test_otra_key_no_puede_inferir_si_payload_coincide(self):
        created = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(created.status_code, status.HTTP_201_CREATED)
        other_client, other_key = PublicApiClient.create_with_generated_key(
            nombre="Integrador ajeno con scope",
        )
        other_client.capabilities = ["OMNICHANNEL"]
        other_client.save(update_fields=["capabilities"])
        other_auth = {"HTTP_X_API_KEY": other_key}

        same = self.client.post(self.url, self.payload, format="json", **other_auth)
        changed_payload = deepcopy(self.payload)
        changed_payload["pedido"]["descripcion"] = "Carga diferente"
        different = self.client.post(
            self.url,
            changed_payload,
            format="json",
            **other_auth,
        )

        self.assertEqual(same.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(different.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(same.data["code"], "OMNICHANNEL_ORDER_OWNERSHIP_CONFLICT")
        self.assertEqual(different.data["code"], same.data["code"])

    def test_seguimiento_token_invalido_retorna_404(self):
        response = self.client.get(
            reverse(
                "api_public_omnichannel_order_status",
                kwargs={"tracking_token": uuid4()},
            ),
            **self.auth,
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_pedido_interno_no_es_accesible_por_seguimiento_publico(self):
        customer = Cliente.objects.create(nombre="Pedido interno")
        order = PedidoCliente.objects.create(
            cliente=customer,
            descripcion="Pedido interno",
            tracking_token=uuid4(),
            public_api_client=self.public_client,
        )
        response = self.client.get(
            reverse(
                "api_public_omnichannel_order_status",
                kwargs={"tracking_token": order.tracking_token},
            ),
            **self.auth,
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_rollback_atomico_si_falla_despues_de_crear_cliente(self):
        with patch(
            "api.omnichannel_views._get_or_create_address",
            side_effect=RuntimeError("fallo simulado"),
        ):
            with self.assertRaises(RuntimeError):
                self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(Cliente.objects.count(), 0)
        self.assertEqual(DireccionCliente.objects.count(), 0)
        self.assertEqual(PedidoCliente.objects.count(), 0)
        self.assertEqual(SolicitudDomicilio.objects.count(), 0)

    def test_busqueda_rapida_es_segura_y_solo_devuelve_activos(self):
        active = Cliente.objects.create(
            nombre="Ana Pérez",
            telefono="6671234567",
            email="ana@example.com",
            notas="No exponer esta nota",
        )
        DireccionCliente.objects.create(
            cliente=active,
            direccion="Av. Obregón 123",
            referencias="Portón blanco",
        )
        DireccionCliente.objects.create(
            cliente=active,
            direccion="Dirección inactiva",
            activa=False,
        )
        inactive = Cliente.objects.create(
            nombre="Ana Inactiva",
            telefono="6679999999",
            activo=False,
        )
        DireccionCliente.objects.create(cliente=inactive, direccion="No visible")

        response = self.client.get(
            self.search_url,
            {"q": "ana"},
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        row = response.data["results"][0]
        self.assertEqual(row["id"], active.id)
        self.assertEqual(row["codigo"], active.codigo)
        self.assertNotIn("notas", row)
        self.assertEqual(len(row["direcciones"]), 1)
        self.assertEqual(row["direcciones"][0]["referencias"], "Portón blanco")
        self.assertIn("latitud", row["direcciones"][0])
        self.assertIn("longitud", row["direcciones"][0])
        self.assertNotIn("Dirección inactiva", str(response.data))

    def test_busqueda_requiere_api_key(self):
        response = self.client.get(self.search_url, {"q": "ana"})
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_busqueda_rechaza_q_ausente_o_vacio(self):
        absent = self.client.get(self.search_url, **self.auth)
        empty = self.client.get(self.search_url, {"q": "  "}, **self.auth)
        self.assertEqual(absent.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(empty.status_code, status.HTTP_400_BAD_REQUEST)

    def test_busqueda_rechaza_termino_demasiado_corto(self):
        response = self.client.get(self.search_url, {"q": "an"}, **self.auth)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    @override_settings(PUBLIC_API_RATE_LIMIT_PER_MINUTE=1)
    def test_rate_limit_existente_aplica_a_busqueda(self):
        first = self.client.get(self.search_url, {"q": "ana"}, **self.auth)
        second = self.client.get(self.search_url, {"q": "ana"}, **self.auth)
        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    @override_settings(
        PUBLIC_API_RATE_LIMIT_PER_MINUTE=100,
        OMNICHANNEL_POINT_SEARCH_RATE_LIMIT_PER_MINUTE=1,
    )
    @patch(
        "api.omnichannel_views._fetch_point_note_candidates",
        return_value=[],
    )
    def test_point_search_has_stricter_endpoint_specific_throttle(self, _fetch):
        url = reverse("api_public_omnichannel_point_notes")
        query = {"folio": "18452", "sucursal": "Matriz"}
        first = self.client.get(url, query, **self.auth)
        second = self.client.get(url, query, **self.auth)
        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_429_TOO_MANY_REQUESTS)
        self.assertEqual(second.data["code"], "POINT_SEARCH_RATE_LIMITED")

    def test_direccion_igual_no_se_cruza_entre_clientes(self):
        first_payload = self.payload
        second_payload = {
            **self.payload,
            "external_id": "order_456",
            "cliente": {
                "nombre": "Beto López",
                "telefono": "6677654321",
                "email": "beto@example.com",
            },
        }

        first = self.client.post(self.url, first_payload, format="json", **self.auth)
        second = self.client.post(self.url, second_payload, format="json", **self.auth)

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_201_CREATED)
        self.assertNotEqual(first.data["cliente_id"], second.data["cliente_id"])
        self.assertNotEqual(first.data["direccion_id"], second.data["direccion_id"])
        self.assertEqual(DireccionCliente.objects.count(), 2)

    def test_nombre_igual_no_fusiona_clientes_sin_telefono_o_email_coincidente(self):
        first_payload = {
            **self.payload,
            "cliente": {"nombre": "Cliente Repetido", "telefono": "", "email": ""},
        }
        second_payload = {
            **first_payload,
            "external_id": "order_456",
        }

        self.client.post(self.url, first_payload, format="json", **self.auth)
        self.client.post(self.url, second_payload, format="json", **self.auth)

        self.assertEqual(Cliente.objects.count(), 2)


class OmnichannelConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        cache.clear()
        _, self.raw_api_key = PublicApiClient.create_with_generated_key(
            nombre="Maya concurrencia",
        )
        public_client = PublicApiClient.objects.get(nombre="Maya concurrencia")
        public_client.capabilities = ["OMNICHANNEL"]
        public_client.save(update_fields=["capabilities"])
        self.url = reverse("api_public_omnichannel_orders")
        self.payload = {
            "external_source": "WHATSAPP",
            "external_id": "wa_race_123",
            "canal": "WHATSAPP",
            "cliente": {
                "nombre": "Cliente Concurrente",
                "telefono": "6671112233",
                "email": "race@example.com",
            },
            "direccion": {
                "direccion": "Calle Carrera 100",
                "referencias": "Casa azul",
                "latitud": "24.809064",
                "longitud": "-107.394011",
                "place_id": "race-place",
            },
            "pedido": {
                "descripcion": "Pedido concurrente",
                "fecha_compromiso": "2026-07-26",
                "monto_estimado": "500.00",
            },
        }

    def test_carrera_concurrente_no_duplica_entidades_ni_responde_500(self):
        barrier = Barrier(2)
        responses = []
        errors = []

        def submit():
            close_old_connections()
            client = APIClient()
            try:
                barrier.wait(timeout=10)
                response = client.post(
                    self.url,
                    deepcopy(self.payload),
                    format="json",
                    HTTP_X_API_KEY=self.raw_api_key,
                )
                responses.append((response.status_code, response.data))
            except Exception as exc:  # pragma: no cover - evidencia útil si falla el hilo
                errors.append(exc)
            finally:
                close_old_connections()

        threads = [Thread(target=submit), Thread(target=submit)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        self.assertFalse(errors)
        self.assertEqual(sorted(code for code, _ in responses), [200, 201])
        self.assertEqual(len({data["pedido_id"] for _, data in responses}), 1)
        self.assertEqual(Cliente.objects.count(), 1)
        self.assertEqual(DireccionCliente.objects.count(), 1)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)

    def test_external_ids_distintos_crean_folios_distintos_sin_colision(self):
        barrier = Barrier(2)
        responses = []
        errors = []
        for index in (1, 2):
            customer = Cliente.objects.create(
                nombre="Cliente Concurrente",
                telefono=f"66711122{index:02d}",
                email=f"race{index}@example.com",
            )
            DireccionCliente.objects.create(
                cliente=customer,
                direccion=f"Calle Carrera {index}",
                referencias="Casa azul",
                latitud="24.809064",
                longitud="-107.394011",
                place_id="race-place",
            )

        def submit(index):
            close_old_connections()
            client = APIClient()
            payload = deepcopy(self.payload)
            payload["external_id"] = f"wa_distinct_{index}"
            payload["cliente"]["telefono"] = f"66711122{index:02d}"
            payload["cliente"]["email"] = f"race{index}@example.com"
            payload["direccion"]["direccion"] = f"Calle Carrera {index}"
            try:
                barrier.wait(timeout=10)
                response = client.post(
                    self.url,
                    payload,
                    format="json",
                    HTTP_X_API_KEY=self.raw_api_key,
                )
                responses.append(response.status_code)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)
            finally:
                close_old_connections()

        threads = [Thread(target=submit, args=(index,)) for index in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        self.assertFalse(errors)
        self.assertEqual(sorted(responses), [201, 201])
        self.assertEqual(PedidoCliente.objects.count(), 2)
        self.assertEqual(
            PedidoCliente.objects.values("folio").distinct().count(),
            2,
        )


def _point_note_for_api(pk_nota="900001"):
    return PointNote(
        pk_nota=pk_nota,
        folio="18452",
        branch_name="Matriz",
        sold_at=timezone.now(),
        total=Decimal("565.00"),
        invoiced=False,
        payment_type="CONTADO",
        point_channel="Mostrador",
        lines=(
            PointNoteLine(
                point_code="P001",
                description="Pastel chocolate",
                quantity=Decimal("1"),
                unit_price=Decimal("565.00"),
                discount=Decimal("0"),
                line_total=Decimal("565.00"),
            ),
        ),
        source_endpoint="/Clientes/get_detalle_nota/",
        customer_external_id="POINT-CUSTOMER-1",
    )


class CanonicalOmnichannelApiTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.actor = get_user_model().objects.create_user(
            username="api-call-center",
            password="unused",
        )
        self.api_client, raw_key = PublicApiClient.create_with_generated_key(
            nombre="Centro operativo",
            created_by=self.actor,
        )
        self.api_client.capabilities = ["OMNICHANNEL"]
        self.api_client.save(update_fields=["capabilities"])
        self.auth = {"HTTP_X_API_KEY": raw_key}
        self.point_payload = {
            "pk_nota": "900001",
            "canal": "FACEBOOK",
            "social_reference": "thread-123",
            "cliente": {
                "nombre": "Ana Pérez",
                "telefono": "6671234567",
                "email": "ana@example.com",
            },
            "direccion": {
                "direccion": "Av. Obregón 123",
                "referencias": "Portón blanco",
                "latitud": "24.809064",
                "longitud": "-107.394011",
                "place_id": "place-123",
            },
            "ventana_inicio": (timezone.now() + timedelta(hours=1)).isoformat(),
            "ventana_fin": (timezone.now() + timedelta(hours=2)).isoformat(),
            "instrucciones_entrega": "Tocar el timbre",
        }

    @patch(
        "api.omnichannel_views._fetch_point_note_candidates",
        return_value=[
            {
                "pk_nota": "900001",
                "folio": "18452",
                "sucursal": "Matriz",
                "fecha": "2026-07-27",
                "hora": "13:15",
                "total": "565.00",
                "facturado": False,
                "canal_point": "Mostrador",
                "tipo_pago": "Contado",
            },
        ],
    )
    def test_note_search_returns_sanitized_candidates(self, fetch):
        response = self.client.get(
            reverse("api_public_omnichannel_point_notes"),
            {"folio": "18452", "sucursal": "Matriz"},
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["results"][0]["pk_nota"], "900001")
        self.assertEqual(
            set(response.data["results"][0]),
            {
                "pk_nota", "folio", "sucursal", "fecha", "hora", "total",
                "facturado", "canal_point", "tipo_pago",
            },
        )
        fetch.assert_called_once()

    def test_note_search_rejects_unbounded_or_oversized_queries(self):
        missing = self.client.get(
            reverse("api_public_omnichannel_point_notes"),
            **self.auth,
        )
        oversized = self.client.get(
            reverse("api_public_omnichannel_point_notes"),
            {"folio": "1" * 81, "sucursal": "Matriz"},
            **self.auth,
        )
        self.assertEqual(missing.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(oversized.status_code, status.HTTP_400_BAD_REQUEST)

    @patch("api.omnichannel_views.PointTicketThresholdService")
    def test_note_search_filters_real_report_contract_and_closes_point_session(self, service_type):
        session = MagicMock()
        session.get.return_value.json.return_value = [
            {
                "PK_NOTA": "900001",
                "FOLIO": "18452",
                "SUCURSAL": "Matriz",
                "DIA": "27/07/2026",
                "HORA": "13:15",
                "MONTO": "565.00",
                "FACTURADO": "False",
                "CANAL_VENTA": "Mostrador",
                "TIPO": "Contado",
                "Cliente": "No exponer",
            },
            {
                "PK_NOTA": "900002",
                "FOLIO": "18452",
                "SUCURSAL": "Otra",
                "MONTO": "999.00",
            },
        ]
        service = service_type.return_value
        service.NOTES_BY_PLAZA_PATH = "/Report/NotasByPlaza"
        service.settings = SimpleNamespace(
            base_url="https://point.test",
            timeout_ms=30000,
        )
        service._build_params.return_value = {"fi": "1", "ff": "2"}
        service.http_session_service.create.return_value = SimpleNamespace(
            session=session,
        )

        response = self.client.get(
            reverse("api_public_omnichannel_point_notes"),
            {
                "folio": "18452",
                "sucursal": "matriz",
                "fecha": "2026-07-27",
            },
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertFalse(response.data["results"][0]["facturado"])
        self.assertNotIn("Cliente", str(response.data))
        service._build_params.assert_called_once()
        session.get.assert_called_once_with(
            "https://point.test/Report/NotasByPlaza",
            params={"fi": "1", "ff": "2"},
            timeout=30,
        )
        session.close.assert_called_once()

    @patch("api.omnichannel_views.PointTicketThresholdService")
    def test_note_search_without_date_uses_small_default_window(self, service_type):
        session = MagicMock()
        session.get.return_value.json.return_value = []
        service = service_type.return_value
        service.NOTES_BY_PLAZA_PATH = "/Report/NotasByPlaza"
        service.settings = SimpleNamespace(
            base_url="https://point.test",
            timeout_ms=7000,
        )
        service._build_params.return_value = {"fi": "1", "ff": "2"}
        service.http_session_service.create.return_value = SimpleNamespace(
            session=session,
        )

        response = self.client.get(
            reverse("api_public_omnichannel_point_notes"),
            {"folio": "18452", "sucursal": "Matriz"},
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        dates = service._build_params.call_args.kwargs
        self.assertEqual((dates["end_date"] - dates["start_date"]).days, 2)
        session.get.assert_called_once_with(
            "https://point.test/Report/NotasByPlaza",
            params={"fi": "1", "ff": "2"},
            timeout=7,
        )

    @patch("api.omnichannel_views.PointTicketThresholdService")
    def test_note_search_filters_exact_real_date_and_skips_invalid_rows(self, service_type):
        session = MagicMock()
        session.get.return_value.json.return_value = [
            {
                "PK_NOTA": "same-date",
                "FOLIO": "18452",
                "SUCURSAL": "Matriz",
                "DIA": "27/07/2026",
            },
            {
                "PK_NOTA": "other-date",
                "FOLIO": "18452",
                "SUCURSAL": "Matriz",
                "DIA": "26/07/2026",
            },
            {
                "PK_NOTA": "invalid-date",
                "FOLIO": "18452",
                "SUCURSAL": "Matriz",
                "DIA": "fecha privada no válida",
            },
        ]
        service = service_type.return_value
        service.NOTES_BY_PLAZA_PATH = "/Report/NotasByPlaza"
        service.settings = SimpleNamespace(
            base_url="https://point.test",
            timeout_ms=30000,
        )
        service._build_params.return_value = {"fi": "1", "ff": "2"}
        service.http_session_service.create.return_value = SimpleNamespace(
            session=session,
        )

        response = self.client.get(
            reverse("api_public_omnichannel_point_notes"),
            {
                "folio": "18452",
                "sucursal": "Matriz",
                "fecha": "2026-07-27",
            },
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["pk_nota"], "same-date")
        self.assertEqual(response.data["results"][0]["fecha"], "2026-07-27")
        self.assertNotIn("other-date", str(response.data))
        self.assertNotIn("invalid-date", str(response.data))

    @patch("api.omnichannel_views.PointTicketThresholdService")
    def test_note_search_closes_point_session_when_http_fails(self, service_type):
        session = MagicMock()
        session.get.side_effect = RuntimeError("fallo de red")
        service = service_type.return_value
        service.NOTES_BY_PLAZA_PATH = "/Report/NotasByPlaza"
        service.settings = SimpleNamespace(
            base_url="https://point.test",
            timeout_ms=30000,
        )
        service._build_params.return_value = {"fi": "1", "ff": "2"}
        service.http_session_service.create.return_value = SimpleNamespace(
            session=session,
        )

        response = self.client.get(
            reverse("api_public_omnichannel_point_notes"),
            {"folio": "18452", "sucursal": "Matriz"},
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE)
        session.close.assert_called_once()

    @patch(
        "api.omnichannel_views.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_point_note_detail_uses_server_source_and_excludes_customer_snapshot(self, _fetch):
        response = self.client.get(
            reverse(
                "api_public_omnichannel_point_note_detail",
                kwargs={"pk_nota": "900001"},
            ),
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["pk_nota"], "900001")
        self.assertEqual(response.data["productos"][0]["codigo"], "P001")
        self.assertNotIn("customer_external_id", str(response.data))
        self.assertNotIn("cliente", response.data)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_create_from_point_is_idempotent_and_owns_single_canonical_delivery(self, _fetch):
        url = reverse("api_public_omnichannel_point_orders")
        first = self.client.post(url, self.point_payload, format="json", **self.auth)
        second = self.client.post(url, self.point_payload, format="json", **self.auth)

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data["pedido_id"], second.data["pedido_id"])
        self.assertEqual(
            first.data["solicitud_domicilio_id"],
            second.data["solicitud_domicilio_id"],
        )
        order = PedidoCliente.objects.get(pk=first.data["pedido_id"])
        self.assertEqual(order.public_api_client_id, self.api_client.id)
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_point_order_replay_with_materially_different_payload_returns_generic_409(self, _fetch):
        url = reverse("api_public_omnichannel_point_orders")
        first = self.client.post(url, self.point_payload, format="json", **self.auth)
        changed = deepcopy(self.point_payload)
        changed["canal"] = "INSTAGRAM"
        changed["social_reference"] = "otro-thread"

        replay = self.client.post(url, changed, format="json", **self.auth)

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(replay.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(replay.data["code"], "POINT_ORDER_CONFLICT")
        self.assertNotIn("otro-thread", str(replay.data))
        self.assertEqual(PedidoCliente.objects.count(), 1)
        self.assertEqual(SolicitudDomicilio.objects.count(), 1)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_point_order_fingerprint_rejects_symmetric_omissions_and_identity_changes(self, _fetch):
        url = reverse("api_public_omnichannel_point_orders")
        first = self.client.post(url, self.point_payload, format="json", **self.auth)
        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        mutations = (
            ("nombre", lambda payload: payload["cliente"].update(nombre="Otro nombre")),
            ("email", lambda payload: payload["cliente"].update(email="otro@example.com")),
            ("references", lambda payload: payload["direccion"].update(referencias="")),
            ("place", lambda payload: payload["direccion"].update(place_id="")),
            ("gps", lambda payload: (
                payload["direccion"].pop("latitud"),
                payload["direccion"].pop("longitud"),
            )),
            ("window", lambda payload: (
                payload.pop("ventana_inicio"),
                payload.pop("ventana_fin"),
            )),
            ("instructions", lambda payload: payload.update(instrucciones_entrega="")),
        )

        for label, mutate in mutations:
            payload = deepcopy(self.point_payload)
            mutate(payload)
            with self.subTest(label=label):
                response = self.client.post(url, payload, format="json", **self.auth)
                self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
                self.assertEqual(response.data["code"], "POINT_ORDER_CONFLICT")

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_delivery_list_detail_filters_legacy_and_do_not_duplicate_point_snapshot(self, _fetch):
        created = self.client.post(
            reverse("api_public_omnichannel_point_orders"),
            self.point_payload,
            format="json",
            **self.auth,
        )
        delivery_id = created.data["solicitud_domicilio_id"]
        SolicitudDomicilio.objects.create(
            cliente_nombre="Legacy",
            direccion="No visible",
        )

        listing = self.client.get(
            reverse("api_public_omnichannel_deliveries"),
            {"canal": "FACEBOOK", "page_size": 10},
            **self.auth,
        )
        detail = self.client.get(
            reverse(
                "api_public_omnichannel_delivery_detail",
                kwargs={"solicitud_id": delivery_id},
            ),
            **self.auth,
        )

        self.assertEqual(listing.status_code, status.HTTP_200_OK)
        self.assertEqual(listing.data["count"], 1)
        self.assertEqual(listing.data["results"][0]["id"], delivery_id)
        self.assertEqual(detail.status_code, status.HTTP_200_OK)
        self.assertEqual(detail.data["fuente"]["tipo"], "POINT")
        self.assertEqual(detail.data["productos"][0]["codigo"], "P001")
        self.assertNotIn("point_note_snapshot", detail.data)
        self.assertNotIn("payload_snapshot", str(detail.data))
        self.assertIn("auditoria", detail.data)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_delivery_list_combines_branch_sale_date_status_channel_driver_and_pagination(self, _fetch):
        created = self.client.post(
            reverse("api_public_omnichannel_point_orders"),
            self.point_payload,
            format="json",
            **self.auth,
        )
        delivery = SolicitudDomicilio.objects.get(
            pk=created.data["solicitud_domicilio_id"],
        )
        sold_at = PedidoCliente.objects.get(
            pk=created.data["pedido_id"],
        ).point_note_sold_at
        self.assertIsNotNone(sold_at)
        sold_date = timezone.localtime(sold_at).date().isoformat()

        response = self.client.get(
            reverse("api_public_omnichannel_deliveries"),
            {
                "sucursal": "Matriz",
                "fecha": sold_date,
                "estatus": delivery.estatus,
                "canal": "FACEBOOK",
                "page": 1,
                "page_size": 1,
            },
            **self.auth,
        )
        wrong_branch = self.client.get(
            reverse("api_public_omnichannel_deliveries"),
            {"sucursal": "Otra", "fecha": sold_date},
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["page_size"], 1)
        self.assertEqual(response.data["results"][0]["id"], delivery.id)
        self.assertEqual(wrong_branch.data["count"], 0)

    def test_delivery_list_exposes_persisted_external_identity_without_point_note(self):
        created = self.client.post(
            reverse("api_public_omnichannel_orders"),
            {
                "external_source": "POLLYANAS_ECOMMERCE",
                "external_id": "ECOMMERCE:PD-TIMEOUT-42",
                "canal": "WEB",
                "cliente": {"nombre": "Ana", "telefono": "6671234567", "email": ""},
                "direccion": {"direccion": "Av. Obregón 123"},
                "pedido": {"descripcion": "Pastel", "monto_estimado": "565.00"},
            },
            format="json",
            **self.auth,
        )

        listing = self.client.get(
            reverse("api_public_omnichannel_deliveries"),
            {"canal": "WEB"},
            **self.auth,
        )

        self.assertEqual(created.status_code, status.HTTP_201_CREATED)
        self.assertEqual(listing.status_code, status.HTTP_200_OK)
        self.assertEqual(listing.data["count"], 1)
        self.assertEqual(
            listing.data["results"][0]["external_source"],
            "POLLYANAS_ECOMMERCE",
        )
        self.assertEqual(
            listing.data["results"][0]["external_id"],
            "ECOMMERCE:PD-TIMEOUT-42",
        )

    def test_delivery_identity_lookup_confirms_only_owned_external_keys(self):
        self.client.post(
            reverse("api_public_omnichannel_orders"),
            {
                "external_source": "POLLYANAS_ECOMMERCE",
                "external_id": "ECOMMERCE:PD-TIMEOUT-42",
                "canal": "WEB",
                "cliente": {"nombre": "Ana", "telefono": "6671234567", "email": ""},
                "direccion": {"direccion": "Av. Obregón 123"},
                "pedido": {"descripcion": "Pastel", "monto_estimado": "565.00"},
            },
            format="json",
            **self.auth,
        )

        response = self.client.get(
            reverse("api_public_omnichannel_delivery_identities"),
            [
                ("external_source", "POLLYANAS_ECOMMERCE"),
                ("external_id", "ECOMMERCE:PD-TIMEOUT-42"),
                ("external_id", "ECOMMERCE:PD-MISSING"),
            ],
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {
            "identities": [{
                "external_source": "POLLYANAS_ECOMMERCE",
                "external_id": "ECOMMERCE:PD-TIMEOUT-42",
            }],
        })

    def test_delivery_identity_lookup_rejects_oversized_external_keys(self):
        url = reverse("api_public_omnichannel_delivery_identities")
        oversized_source = self.client.get(
            url,
            [("external_source", "S" * 41), ("external_id", "ECOMMERCE:PD-42")],
            **self.auth,
        )
        oversized_id = self.client.get(
            url,
            [("external_source", "POLLYANAS_ECOMMERCE"), ("external_id", "I" * 121)],
            **self.auth,
        )
        self.assertEqual(oversized_source.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(oversized_id.status_code, status.HTTP_400_BAD_REQUEST)

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_delivery_detail_is_scoped_by_api_client_without_existence_leak(self, _fetch):
        created = self.client.post(
            reverse("api_public_omnichannel_point_orders"),
            self.point_payload,
            format="json",
            **self.auth,
        )
        other, other_key = PublicApiClient.create_with_generated_key(
            nombre="Otro centro",
            created_by=self.actor,
        )
        other.capabilities = ["OMNICHANNEL"]
        other.save(update_fields=["capabilities"])

        response = self.client.get(
            reverse(
                "api_public_omnichannel_delivery_detail",
                kwargs={"solicitud_id": created.data["solicitud_domicilio_id"]},
            ),
            HTTP_X_API_KEY=other_key,
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_every_new_route_requires_key_and_capability(self):
        unscoped, unscoped_key = PublicApiClient.create_with_generated_key(
            nombre="Sin capability",
            created_by=self.actor,
        )
        urls = (
            reverse("api_public_omnichannel_point_notes"),
            reverse(
                "api_public_omnichannel_point_note_detail",
                kwargs={"pk_nota": "900001"},
            ),
            reverse("api_public_omnichannel_deliveries"),
            reverse(
                "api_public_omnichannel_delivery_detail",
                kwargs={"solicitud_id": 999},
            ),
        )
        for url in urls:
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, status.HTTP_401_UNAUTHORIZED)
                denied = self.client.get(url, HTTP_X_API_KEY=unscoped_key)
                self.assertEqual(denied.status_code, status.HTTP_403_FORBIDDEN)
        self.assertFalse(unscoped.capabilities)

        point_orders = reverse("api_public_omnichannel_point_orders")
        self.assertEqual(
            self.client.post(point_orders, self.point_payload, format="json").status_code,
            status.HTTP_401_UNAUTHORIZED,
        )
        denied = self.client.post(
            point_orders,
            self.point_payload,
            format="json",
            HTTP_X_API_KEY=unscoped_key,
        )
        self.assertEqual(denied.status_code, status.HTTP_403_FORBIDDEN)

    @override_settings(PUBLIC_API_RATE_LIMIT_PER_MINUTE=1)
    @patch(
        "api.omnichannel_views._fetch_point_note_candidates",
        return_value=[],
    )
    def test_rate_limit_applies_to_new_point_search(self, _fetch):
        url = reverse("api_public_omnichannel_point_notes")
        query = {"folio": "18452", "sucursal": "Matriz"}
        first = self.client.get(url, query, **self.auth)
        second = self.client.get(url, query, **self.auth)
        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    @patch(
        "api.omnichannel_views.PointNoteDetailService.fetch",
        side_effect=PointNoteUnavailableError("token=secreto cliente=Ana"),
    )
    def test_point_error_is_typed_retryable_and_does_not_leak_internal_message(self, _fetch):
        response = self.client.get(
            reverse(
                "api_public_omnichannel_point_note_detail",
                kwargs={"pk_nota": "900001"},
            ),
            **self.auth,
        )
        rendered = str(response.data)
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE)
        self.assertEqual(response.data["code"], "POINT_UNAVAILABLE")
        self.assertTrue(response.data["retryable"])
        self.assertNotIn("secreto", rendered)
        self.assertNotIn("Ana", rendered)
        access = PublicApiAccessLog.objects.filter(client=self.api_client).latest("id")
        self.assertNotIn("secreto", str(access))

    def test_point_session_failures_are_typed_audited_and_sanitized_on_all_three_routes(self):
        cases = (
            (
                "api.omnichannel_views.PointTicketThresholdService",
                "search",
                AuthenticationError("login Point cliente=Ana token=secreto"),
            ),
            (
                "api.omnichannel_views.PointNoteDetailService.fetch",
                "detail",
                ConfigurationError("POINT_PASSWORD=secreto"),
            ),
            (
                "api.omnichannel_views.link_point_note",
                "create",
                ExtractionError("body_preview cliente=Ana"),
            ),
        )
        for target, route, error in cases:
            cache.clear()
            before = PublicApiAccessLog.objects.filter(client=self.api_client).count()
            with self.subTest(route=route), patch(target) as dependency:
                if route == "search":
                    dependency.side_effect = error
                    response = self.client.get(
                        reverse("api_public_omnichannel_point_notes"),
                        {"folio": "18452", "sucursal": "Matriz"},
                        **self.auth,
                    )
                elif route == "detail":
                    dependency.side_effect = error
                    response = self.client.get(
                        reverse(
                            "api_public_omnichannel_point_note_detail",
                            kwargs={"pk_nota": "900001"},
                        ),
                        **self.auth,
                    )
                else:
                    dependency.side_effect = error
                    response = self.client.post(
                        reverse("api_public_omnichannel_point_orders"),
                        self.point_payload,
                        format="json",
                        **self.auth,
                    )

            self.assertIn(
                response.status_code,
                {status.HTTP_502_BAD_GATEWAY, status.HTTP_503_SERVICE_UNAVAILABLE},
            )
            self.assertIn(response.data["code"], {"POINT_UNAVAILABLE", "POINT_CONTRACT_ERROR"})
            self.assertNotIn("secreto", str(response.data))
            self.assertNotIn("Ana", str(response.data))
            self.assertEqual(
                PublicApiAccessLog.objects.filter(client=self.api_client).count(),
                before + 1,
            )

    @patch(
        "crm.services.point_order_link.PointNoteDetailService.fetch",
        return_value=_point_note_for_api(),
    )
    def test_patch_status_calls_canonical_idempotent_transition(self, _fetch):
        created = self.client.post(
            reverse("api_public_omnichannel_point_orders"),
            self.point_payload,
            format="json",
            **self.auth,
        )
        delivery = SolicitudDomicilio.objects.get(
            pk=created.data["solicitud_domicilio_id"],
        )
        branch = Sucursal.objects.create(codigo="API-DOM", nombre="API Domicilios")
        driver_user = get_user_model().objects.create_user(
            username="api-driver",
            password="unused",
        )
        unit = Unidad.objects.create(
            codigo="API-DOM-U",
            descripcion="Unidad API Domicilios",
            sucursal=branch,
        )
        driver = Repartidor.objects.create(
            user=driver_user,
            sucursal=branch,
            unidad_asignada=unit,
        )
        self.api_client.capabilities = ["OMNICHANNEL", "LOGISTICA_ASSIGNMENT"]
        self.api_client.save(update_fields=["capabilities"])
        self.api_client.repartidores_logistica_autorizados.add(driver)
        delivery.repartidor = driver
        delivery.unidad = unit
        delivery.estatus = SolicitudDomicilio.ESTATUS_PREPARANDO
        delivery.save(update_fields=["repartidor", "unidad", "estatus"])
        delivery.estatus = SolicitudDomicilio.ESTATUS_LISTO
        delivery.save(update_fields=["estatus"])
        url = reverse(
            "api_public_omnichannel_delivery_status",
            kwargs={"solicitud_id": delivery.id},
        )
        payload = {
            "repartidor_id": driver.id,
            "estatus": "EN_RUTA",
            "operation_id": str(uuid4()),
            "actor": {"id": "call-center", "nombre": "Centro operativo"},
        }

        first = self.client.patch(url, payload, format="json", **self.auth)
        replay = self.client.patch(url, payload, format="json", **self.auth)

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertFalse(first.data["idempotent"])
        self.assertEqual(replay.status_code, status.HTTP_200_OK)
        self.assertEqual(replay.data, first.data)
        self.assertFalse(replay.data["idempotent"])

    def test_patch_status_requires_logistica_capability_in_addition_to_omnichannel(self):
        response = self.client.patch(
            reverse(
                "api_public_omnichannel_delivery_status",
                kwargs={"solicitud_id": 999},
            ),
            {
                "repartidor_id": 1,
                "estatus": "EN_RUTA",
                "operation_id": str(uuid4()),
                "actor": {"id": "scope", "nombre": "Scope"},
            },
            format="json",
            **self.auth,
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            response.data["code"],
            "LOGISTICA_ASSIGNMENT_CAPABILITY_REQUIRED",
        )

    def test_patch_status_hides_non_point_and_legacy_deliveries(self):
        self.api_client.capabilities = ["OMNICHANNEL", "LOGISTICA_ASSIGNMENT"]
        self.api_client.save(update_fields=["capabilities"])
        branch = Sucursal.objects.create(codigo="API-SCOPE", nombre="API Scope")
        driver_user = get_user_model().objects.create_user(
            username="api-scope-driver",
            password="unused",
        )
        unit = Unidad.objects.create(
            codigo="API-SCOPE-U",
            descripcion="Unidad API Scope",
            sucursal=branch,
        )
        driver = Repartidor.objects.create(
            user=driver_user,
            sucursal=branch,
            unidad_asignada=unit,
        )
        self.api_client.repartidores_logistica_autorizados.add(driver)
        customer = Cliente.objects.create(nombre="Scope")
        address = DireccionCliente.objects.create(
            cliente=customer,
            direccion="Scope",
            latitud="24.809064",
            longitud="-107.394011",
        )
        non_point_order = PedidoCliente.objects.create(
            cliente=customer,
            direccion_entrega=address,
            descripcion="Sin Point",
            public_api_client=self.api_client,
        )
        non_point = SolicitudDomicilio.objects.create(
            pedido_cliente=non_point_order,
            direccion_cliente=address,
            cliente_nombre="Scope",
            direccion="Scope",
            repartidor=driver,
            estatus=SolicitudDomicilio.ESTATUS_PENDIENTE_POINT,
        )
        point_order = PedidoCliente.objects.create(
            cliente=customer,
            direccion_entrega=address,
            descripcion="Point legacy",
            public_api_client=self.api_client,
            point_note_id="POINT-LEGACY-SCOPE",
            point_note_snapshot={"pk_nota": "POINT-LEGACY-SCOPE"},
        )
        legacy = SolicitudDomicilio.objects.create(
            pedido_cliente=point_order,
            direccion_cliente=address,
            cliente_nombre="Scope legacy",
            direccion="Scope",
            repartidor=driver,
            unidad=unit,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        legacy.estatus = SolicitudDomicilio.ESTATUS_PREPARANDO
        legacy.save(update_fields=["estatus"])
        legacy.estatus = SolicitudDomicilio.ESTATUS_LISTO
        legacy.save(update_fields=["estatus"])
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE logistica_solicituddomicilio "
                "SET legacy_without_point = TRUE WHERE id = %s",
                [legacy.id],
            )
        payload = {
            "repartidor_id": driver.id,
            "estatus": "EN_RUTA",
            "operation_id": str(uuid4()),
            "actor": {"id": "scope", "nombre": "Scope"},
        }

        for delivery in (non_point, legacy):
            with self.subTest(delivery=delivery.id):
                response = self.client.patch(
                    reverse(
                        "api_public_omnichannel_delivery_status",
                        kwargs={"solicitud_id": delivery.id},
                    ),
                    payload,
                    format="json",
                    **self.auth,
                )
                self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
