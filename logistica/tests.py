from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess
from crm.models import Cliente, PedidoCliente
from logistica.models import EntregaRuta, Repartidor, RutaEntrega, Unidad
from logistica.tasks import _emails_de_grupo


class LogisticaGroupAliasCompatibilityTests(TestCase):
    def test_emails_de_grupo_legacy_dg_uses_canonical_group(self):
        user = User.objects.create_user(
            username="dg.logistica.alias",
            email="dg.logistica@example.com",
        )
        user.groups.add(Group.objects.get_or_create(name="DG")[0])

        self.assertEqual(_emails_de_grupo("dg"), ["dg.logistica@example.com"])


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
            fecha_ruta=timezone.localdate(),
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


class LogisticaPwaApiTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="repartidor.api", password="pass123")
        group, _ = Group.objects.get_or_create(name="repartidor")
        self.user.groups.add(group)
        self.sucursal = Sucursal.objects.create(codigo="QA-LOG", nombre="QA Logística", activa=True)
        self.unidad = Unidad.objects.create(codigo="QA-LOG-1", descripcion="Unidad QA", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.client.force_login(self.user)

    def test_bitacora_activa_without_open_shift_returns_null_not_404(self):
        response = self.client.get(reverse("api_logistica_bitacora_salida_activa"))

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")

    def test_session_token_bridges_unified_app_login_to_logistica_pwa(self):
        response = self.client.get(reverse("api_logistica_auth_session_token"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("access", payload)
        self.assertIn("refresh", payload)

        self.client.logout()
        perfil = self.client.get(reverse("api_logistica_mi_perfil"), HTTP_AUTHORIZATION=f"Bearer {payload['access']}")
        self.assertEqual(perfil.status_code, 200)
        self.assertEqual(perfil.json()["user"]["username"], self.user.username)

    def test_session_token_rejects_users_without_logistica_access(self):
        self.client.logout()
        other = User.objects.create_user(username="sin.logistica", password="pass123")
        self.client.force_login(other)

        response = self.client.get(reverse("api_logistica_auth_session_token"))

        self.assertEqual(response.status_code, 403)

    def test_mantenimiento_user_can_open_logistica_pwa_without_logistica_module(self):
        self.client.logout()
        user = User.objects.create_user(username="mant.pwa", password="pass123")
        UserModuleAccess.objects.create(user=user, module="mantenimiento", access=ACCESS_MANAGE)
        self.client.force_login(user)

        response = self.client.get(reverse("logistica:pwa_app"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Logística")
