from copy import deepcopy
from threading import Barrier, Thread
from unittest.mock import patch

from django.core.cache import cache
from django.db import close_old_connections
from django.test import TransactionTestCase, override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient, APITestCase

from crm.models import Cliente, DireccionCliente, PedidoCliente
from integraciones.models import PublicApiClient
from logistica.models import SolicitudDomicilio


class OmnichannelPublicApiTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.public_client, self.raw_api_key = PublicApiClient.create_with_generated_key(
            nombre="Maya omnicanal",
        )
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
        self.assertEqual(solicitud.notas, "Portón blanco")

    def test_misma_clave_con_payload_distinto_retorna_409(self):
        first = self.client.post(self.url, self.payload, format="json", **self.auth)
        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.payload["pedido"]["descripcion"] = "Pedido materialmente diferente"

        response = self.client.post(self.url, self.payload, format="json", **self.auth)

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(response.data["code"], "OMNICHANNEL_IDEMPOTENCY_CONFLICT")
        self.assertEqual(PedidoCliente.objects.count(), 1)

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

        unauthorized = self.client.get(tracking_url)
        authorized = self.client.get(tracking_url, **self.auth)

        self.assertEqual(unauthorized.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(authorized.status_code, status.HTTP_200_OK)
        self.assertEqual(authorized.data["pedido_id"], created.data["pedido_id"])
        self.assertEqual(authorized.data["estatus"], PedidoCliente.ESTATUS_NUEVO)
        self.assertNotIn("cliente", authorized.data)
        self.assertNotIn("direccion", authorized.data)

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
