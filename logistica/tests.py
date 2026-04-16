from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse

from crm.models import Cliente, PedidoCliente
from logistica.models import EntregaRuta, RutaEntrega


class LogisticaViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="logistica", password="pass123")
        group, _ = Group.objects.get_or_create(name="LOGISTICA")
        self.user.groups.add(group)
        self.client.login(username="logistica", password="pass123")

    def test_dashboard_view_renders_executive_surface(self):
        cliente = Cliente.objects.create(nombre="Cliente Logística")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido de reparto",
            estatus=PedidoCliente.ESTATUS_CONFIRMADO,
            monto_estimado=950,
        )
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Centro",
            fecha_ruta="2026-03-25",
            chofer="Mario",
            unidad="Van 1",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_EN_CAMINO,
            monto_estimado=950,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Logística en control")
        self.assertContains(resp, "Distribución de estatus de ruta")
        self.assertContains(resp, "Distribución de estatus de entrega")
        self.assertContains(resp, "Últimos 7 días")

    def test_dashboard_view_supports_real_focus_filter(self):
        cliente = Cliente.objects.create(nombre="Cliente Incidencia")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido con incidencia", monto_estimado=500)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Incidencia",
            fecha_ruta="2026-03-25",
            chofer="Mario",
            unidad="Van 1",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_INCIDENCIA,
            monto_estimado=500,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"), {"enterprise_focus": "INCIDENCIAS"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["enterprise_focus"], "INCIDENCIAS")
        self.assertIsNotNone(resp.context["focus_summary"])
        self.assertContains(resp, "Foco")

    def test_dashboard_view_supports_real_search_filter(self):
        cliente = Cliente.objects.create(nombre="Cliente Busqueda Logística")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido filtro", monto_estimado=500)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Busqueda",
            fecha_ruta="2026-03-25",
            chofer="Mario",
            unidad="Van 99",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_PENDIENTE,
            monto_estimado=500,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"), {"q": "Van 99"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["selected_q"], "Van 99")
        self.assertContains(resp, "Ruta o pedido")

    def test_rutas_view_and_create(self):
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Logística · Rutas")
        self.assertContains(resp, "Nueva ruta de entrega")
        self.assertContains(resp, "Filtro operativo")
        self.assertContains(resp, "Rutas de entrega")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Ruta crítica ERP")
        self.assertNotContains(resp, "Mesa de gobierno ERP")
        self.assertTrue(resp.context["focus_cards"])
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta Centro",
                "fecha_ruta": "2026-02-24",
                "chofer": "Mario",
                "unidad": "Van 1",
                "km_estimado": "18.5",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Detalle de ruta")
        self.assertContains(resp_post, "Agregar entrega")
        self.assertContains(resp_post, "Entregas de ruta")
        self.assertNotContains(resp_post, "Centro de mando ERP")
        self.assertNotContains(resp_post, "Cadena documental ERP")
        self.assertNotContains(resp_post, "Mesa de gobierno ERP")

    def test_ruta_detail_add_entrega(self):
        cliente = Cliente.objects.create(nombre="Cliente Logística")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pastel para entrega")
        self.client.post(
            reverse("logistica:rutas"),
            {"nombre": "Ruta Norte", "fecha_ruta": "2026-02-24"},
            follow=True,
        )

        from logistica.models import RutaEntrega

        ruta = RutaEntrega.objects.first()
        resp = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "add_entrega",
                "pedido_id": str(pedido.id),
                "secuencia": "1",
                "cliente_nombre": "",
                "direccion": "Sucursal Centro",
                "estatus": "PENDIENTE",
                "monto_estimado": "950",
            },
            follow=True,
        )
        self.assertEqual(resp.status_code, 200)
        ruta.refresh_from_db()
        self.assertEqual(ruta.total_entregas, 1)
        self.assertContains(resp, "Agregar entrega")
        self.assertContains(resp, "Entregas de ruta")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Madurez ERP de logística")
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertTrue(resp.context["operational_health_cards"])

    def test_rutas_view_can_focus_enterprise_subset(self):
        cliente = Cliente.objects.create(nombre="Cliente Focus")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Entrega foco")
        self.client.post(
            reverse("logistica:rutas"),
            {"nombre": "Ruta Focus", "fecha_ruta": "2026-02-24"},
            follow=True,
        )
        from logistica.models import RutaEntrega

        ruta = RutaEntrega.objects.first()
        self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "add_entrega",
                "pedido_id": str(pedido.id),
                "secuencia": "1",
                "direccion": "Centro",
                "estatus": "INCIDENCIA",
                "monto_estimado": "100",
            },
            follow=True,
        )

        resp = self.client.get(reverse("logistica:rutas"), {"enterprise_focus": "INCIDENCIAS"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Focos operativos")
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "INCIDENCIAS")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_redirect_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)
