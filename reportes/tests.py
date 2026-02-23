from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse

from compras.models import OrdenCompra
from crm.models import Cliente, PedidoCliente
from maestros.models import Proveedor


class ReportesBITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="lectura_reportes", password="pass123")
        group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user.groups.add(group)
        self.client.login(username="lectura_reportes", password="pass123")

        cliente = Cliente.objects.create(nombre="Cliente BI")
        PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido BI", monto_estimado=1200)
        prov = Proveedor.objects.create(nombre="Proveedor BI")
        solicitud_insumo = None
        # Orden sin solicitud para no depender de más catálogos en este test.
        OrdenCompra.objects.create(proveedor=prov, monto_estimado=950, solicitud=solicitud_insumo)

    def test_bi_view_renders(self):
        resp = self.client.get(reverse("reportes:bi"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "BI Ejecutivo")

    def test_bi_exports(self):
        resp_csv = self.client.get(reverse("reportes:bi"), {"export": "csv"})
        self.assertEqual(resp_csv.status_code, 200)
        self.assertIn("text/csv", resp_csv["Content-Type"])

        resp_xlsx = self.client.get(reverse("reportes:bi"), {"export": "xlsx"})
        self.assertEqual(resp_xlsx.status_code, 200)
        self.assertIn("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resp_xlsx["Content-Type"])

    def test_requires_login(self):
        self.client.logout()
        resp = self.client.get(reverse("reportes:bi"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)
