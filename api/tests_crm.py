from django.contrib.auth.models import Group, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.access import ROLE_LECTURA, ROLE_VENTAS
from core.models import Sucursal
from crm.models import Cliente, PedidoCliente
from pos_bridge.models import PointBranch


class CRMApiTests(APITestCase):
    def setUp(self):
        self.user_ventas = User.objects.create_user(username="ventas_api", password="pass123")
        ventas_group, _ = Group.objects.get_or_create(name=ROLE_VENTAS)
        self.user_ventas.groups.add(ventas_group)

        self.user_lectura = User.objects.create_user(username="lectura_api", password="pass123")
        lectura_group, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.user_lectura.groups.add(lectura_group)
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.point_branch = PointBranch.objects.create(
            external_id="1",
            name="Matriz Point",
            erp_branch=self.sucursal,
            status=PointBranch.STATUS_ACTIVE,
        )

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

    def test_cliente_detail_get_and_patch(self):
        self.client.force_authenticate(self.user_ventas)
        cliente = Cliente.objects.create(nombre="Cliente Detalle API", telefono="6670000000")
        PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido visible API",
            sucursal_ref=self.sucursal,
            monto_estimado="350.00",
        )

        detail_url = reverse("api_crm_cliente_detail", kwargs={"pk": cliente.id})
        resp_detail = self.client.get(detail_url)
        self.assertEqual(resp_detail.status_code, status.HTTP_200_OK)
        self.assertEqual(resp_detail.data["cliente"]["nombre"], "Cliente Detalle API")
        self.assertEqual(len(resp_detail.data["pedidos"]), 1)

        resp_patch = self.client.patch(
            detail_url,
            {"nombre": "Cliente API Editado", "activo": False, "notas": "Actualizado por API"},
            format="json",
        )
        self.assertEqual(resp_patch.status_code, status.HTTP_200_OK)
        cliente.refresh_from_db()
        self.assertEqual(cliente.nombre, "Cliente API Editado")
        self.assertFalse(cliente.activo)
        self.assertEqual(cliente.notas, "Actualizado por API")

    def test_pedidos_create_followup_and_dashboard(self):
        self.client.force_authenticate(self.user_ventas)
        cliente = Cliente.objects.create(nombre="Cliente Pedido API")

        pedidos_url = reverse("api_crm_pedidos")
        resp_create = self.client.post(
            pedidos_url,
            {
                "cliente": cliente.id,
                "descripcion": "Pedido API pastel chocolate",
                "sucursal": self.point_branch.external_id,
                "estatus": PedidoCliente.ESTATUS_NUEVO,
                "prioridad": PedidoCliente.PRIORIDAD_MEDIA,
                "canal": PedidoCliente.CANAL_WHATSAPP,
                "monto_estimado": "950.50",
            },
            format="json",
        )
        self.assertEqual(resp_create.status_code, status.HTTP_201_CREATED)
        pedido_id = resp_create.data["id"]
        pedido = PedidoCliente.objects.get(id=pedido_id)
        self.assertEqual(pedido.sucursal_ref_id, self.sucursal.id)
        self.assertEqual(pedido.sucursal, self.sucursal.nombre)

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

        cliente = Cliente.objects.create(nombre="Solo Lectura")
        detail_url = reverse("api_crm_cliente_detail", kwargs={"pk": cliente.id})
        resp_patch = self.client.patch(detail_url, {"nombre": "No permitido"}, format="json")
        self.assertEqual(resp_patch.status_code, status.HTTP_403_FORBIDDEN)
