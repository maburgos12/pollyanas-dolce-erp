from django.contrib.auth.models import Group, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.access import ROLE_LECTURA, ROLE_VENTAS
from crm.models import Cliente, PedidoCliente


class CRMApiTests(APITestCase):
    def setUp(self):
        self.user_ventas = User.objects.create_user(username="ventas_api", password="pass123")
        ventas_group, _ = Group.objects.get_or_create(name=ROLE_VENTAS)
        self.user_ventas.groups.add(ventas_group)

        self.user_lectura = User.objects.create_user(username="lectura_api", password="pass123")
        lectura_group, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.user_lectura.groups.add(lectura_group)

    def test_clientes_list_and_create(self):
        self.client.force_authenticate(self.user_ventas)
        list_url = reverse("api_crm_clientes")

        resp_create = self.client.post(
            list_url,
            {
                "nombre": "Cliente API",
                "telefono": "6671002000",
                "email": "cliente@api.com",
                "tipo_cliente": "Mostrador",
            },
            format="json",
        )
        self.assertEqual(resp_create.status_code, status.HTTP_201_CREATED)
        self.assertTrue(Cliente.objects.filter(nombre="Cliente API").exists())

        resp_list = self.client.get(list_url)
        self.assertEqual(resp_list.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(resp_list.data["count"], 1)

    def test_pedidos_create_followup_and_dashboard(self):
        self.client.force_authenticate(self.user_ventas)
        cliente = Cliente.objects.create(nombre="Cliente Pedido API")

        pedidos_url = reverse("api_crm_pedidos")
        resp_create = self.client.post(
            pedidos_url,
            {
                "cliente": cliente.id,
                "descripcion": "Pedido API pastel chocolate",
                "estatus": PedidoCliente.ESTATUS_NUEVO,
                "prioridad": PedidoCliente.PRIORIDAD_MEDIA,
                "canal": PedidoCliente.CANAL_WHATSAPP,
                "monto_estimado": "950.50",
            },
            format="json",
        )
        self.assertEqual(resp_create.status_code, status.HTTP_201_CREATED)
        pedido_id = resp_create.data["id"]

        seguimiento_url = reverse("api_crm_pedido_seguimiento", kwargs={"pedido_id": pedido_id})
        resp_followup = self.client.post(
            seguimiento_url,
            {
                "estatus_nuevo": PedidoCliente.ESTATUS_CONFIRMADO,
                "comentario": "Cliente confirma pago",
            },
            format="json",
        )
        self.assertEqual(resp_followup.status_code, status.HTTP_200_OK)

        dashboard_url = reverse("api_crm_dashboard")
        resp_dashboard = self.client.get(dashboard_url)
        self.assertEqual(resp_dashboard.status_code, status.HTTP_200_OK)
        self.assertIn("clientes", resp_dashboard.data)
        self.assertIn("pedidos", resp_dashboard.data)

    def test_lectura_can_view_but_cannot_create(self):
        self.client.force_authenticate(self.user_lectura)
        list_url = reverse("api_crm_clientes")

        resp_list = self.client.get(list_url)
        self.assertEqual(resp_list.status_code, status.HTTP_200_OK)

        resp_create = self.client.post(list_url, {"nombre": "No permitido"}, format="json")
        self.assertEqual(resp_create.status_code, status.HTTP_403_FORBIDDEN)
