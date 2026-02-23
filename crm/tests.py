from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse

from core.access import ROLE_VENTAS

from .models import Cliente, PedidoCliente, SeguimientoPedido


class CRMViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="ventas", password="pass123")
        ventas_group, _ = Group.objects.get_or_create(name=ROLE_VENTAS)
        self.user.groups.add(ventas_group)
        self.client.login(username="ventas", password="pass123")

    def test_clientes_view_and_create(self):
        resp = self.client.get(reverse("crm:clientes"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "CRM · Clientes")

        resp_post = self.client.post(
            reverse("crm:clientes"),
            {
                "nombre": "Cliente Demo",
                "telefono": "6671230000",
                "email": "demo@example.com",
                "tipo_cliente": "Mostrador",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertTrue(Cliente.objects.filter(nombre="Cliente Demo").exists())

    def test_pedidos_create_and_tracking(self):
        cliente = Cliente.objects.create(nombre="Cliente Pedido")
        resp_post = self.client.post(
            reverse("crm:pedidos"),
            {
                "cliente_id": cliente.id,
                "descripcion": "Pastel cumpleaños 30 personas",
                "canal": PedidoCliente.CANAL_WHATSAPP,
                "prioridad": PedidoCliente.PRIORIDAD_ALTA,
                "estatus": PedidoCliente.ESTATUS_NUEVO,
                "monto_estimado": "1450.00",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)

        pedido = PedidoCliente.objects.get(cliente=cliente)
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_NUEVO)
        self.assertEqual(pedido.monto_estimado, 1450)
        self.assertTrue(SeguimientoPedido.objects.filter(pedido=pedido).exists())

        detail_url = reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id})
        resp_detail = self.client.post(
            detail_url,
            {
                "estatus_nuevo": PedidoCliente.ESTATUS_CONFIRMADO,
                "comentario": "Cliente confirma anticipo",
            },
            follow=True,
        )
        self.assertEqual(resp_detail.status_code, 200)
        pedido.refresh_from_db()
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_CONFIRMADO)
        self.assertTrue(
            SeguimientoPedido.objects.filter(
                pedido=pedido,
                estatus_nuevo=PedidoCliente.ESTATUS_CONFIRMADO,
                comentario__icontains="anticipo",
            ).exists()
        )

    def test_redirects_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("crm:pedidos"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)
