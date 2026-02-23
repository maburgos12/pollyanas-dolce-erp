from django.contrib.auth.models import Group, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from crm.models import Cliente, PedidoCliente


class LogisticaApiTests(APITestCase):
    def setUp(self):
        self.user_logistica = User.objects.create_user(username="logistica_api", password="pass123")
        log_group, _ = Group.objects.get_or_create(name="LOGISTICA")
        self.user_logistica.groups.add(log_group)

        self.user_lectura = User.objects.create_user(username="lectura_log", password="pass123")
        lectura_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user_lectura.groups.add(lectura_group)

    def test_rutas_create_and_list(self):
        self.client.force_authenticate(self.user_logistica)
        rutas_url = reverse("api_logistica_rutas")
        resp_create = self.client.post(
            rutas_url,
            {
                "nombre": "Ruta API Centro",
                "fecha_ruta": "2026-02-24",
                "chofer": "Mario",
                "unidad": "Van 2",
                "estatus": "PLANEADA",
            },
            format="json",
        )
        self.assertEqual(resp_create.status_code, status.HTTP_201_CREATED)

        resp_list = self.client.get(rutas_url)
        self.assertEqual(resp_list.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(resp_list.data["count"], 1)

    def test_entregas_create_and_dashboard(self):
        self.client.force_authenticate(self.user_logistica)
        cliente = Cliente.objects.create(nombre="Cliente API Log")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido API Log")

        rutas_url = reverse("api_logistica_rutas")
        ruta_resp = self.client.post(
            rutas_url,
            {"nombre": "Ruta API Norte", "fecha_ruta": "2026-02-24", "estatus": "PLANEADA"},
            format="json",
        )
        self.assertEqual(ruta_resp.status_code, status.HTTP_201_CREATED)
        ruta_id = ruta_resp.data["id"]

        entregas_url = reverse("api_logistica_ruta_entregas", kwargs={"ruta_id": ruta_id})
        entrega_resp = self.client.post(
            entregas_url,
            {
                "secuencia": 1,
                "pedido_id": pedido.id,
                "direccion": "Sucursal Centro",
                "estatus": "PENDIENTE",
                "monto_estimado": "1200.00",
            },
            format="json",
        )
        self.assertEqual(entrega_resp.status_code, status.HTTP_201_CREATED)

        dashboard_url = reverse("api_logistica_dashboard")
        dash_resp = self.client.get(dashboard_url)
        self.assertEqual(dash_resp.status_code, status.HTTP_200_OK)
        self.assertIn("rutas", dash_resp.data)
        self.assertIn("entregas", dash_resp.data)

    def test_lectura_can_view_but_not_create(self):
        self.client.force_authenticate(self.user_lectura)
        rutas_url = reverse("api_logistica_rutas")

        resp_list = self.client.get(rutas_url)
        self.assertEqual(resp_list.status_code, status.HTTP_200_OK)

        resp_create = self.client.post(rutas_url, {"nombre": "No permitido"}, format="json")
        self.assertEqual(resp_create.status_code, status.HTTP_403_FORBIDDEN)
