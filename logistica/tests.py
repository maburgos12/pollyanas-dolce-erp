from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse

from crm.models import Cliente, PedidoCliente


class LogisticaViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="logistica", password="pass123")
        group, _ = Group.objects.get_or_create(name="LOGISTICA")
        self.user.groups.add(group)
        self.client.login(username="logistica", password="pass123")

    def test_rutas_view_and_create(self):
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Logística · Rutas")
        self.assertContains(resp, "Centro de mando ERP")
        self.assertContains(resp, "Cockpit operativo de logística")
        self.assertContains(resp, "Cadena documental ERP")
        self.assertContains(resp, "Cadena troncal de logística")
        self.assertContains(resp, "Ruta crítica ERP")
        self.assertContains(resp, "Radar ejecutivo ERP")
        self.assertContains(resp, "Depende de")
        self.assertContains(resp, "Dependencia")
        self.assertContains(resp, "Madurez ERP de logística")
        self.assertContains(resp, "Criterios de cierre ERP")
        self.assertContains(resp, "Cierre global")
        self.assertContains(resp, "Cadena de control logístico")
        self.assertContains(resp, "Entrega logística a downstream")
        self.assertContains(resp, "Cierre por etapa documental")
        self.assertContains(resp, "Mesa de gobierno ERP")
        self.assertContains(resp, "Responsable")
        self.assertContains(resp, "Cierre")
        self.assertContains(resp, "Salud operativa ERP")
        self.assertTrue(resp.context["focus_cards"])
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("erp_command_center", resp.context)
        self.assertIn("critical_path_rows", resp.context)
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp.context)
        self.assertIn("release_gate_rows", resp.context)
        self.assertIn("release_gate_completion", resp.context)
        self.assertIn("handoff_map", resp.context)
        self.assertIn("owner", resp.context["handoff_map"][0])
        self.assertIn("depends_on", resp.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp.context["handoff_map"][0])
        self.assertIn("next_step", resp.context["handoff_map"][0])
        self.assertIn("completion", resp.context["handoff_map"][0])
        self.assertTrue(resp.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("executive_radar_rows", resp.context)
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
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
        self.assertContains(resp_post, "Centro de mando ERP")
        self.assertContains(resp_post, "Cadena documental ERP")
        self.assertContains(resp_post, "Cadena troncal de logística")
        self.assertContains(resp_post, "Ruta crítica ERP")
        self.assertContains(resp_post, "Radar ejecutivo ERP")
        self.assertContains(resp_post, "Depende de")
        self.assertContains(resp_post, "Dependencia")
        self.assertContains(resp_post, "Madurez ERP de logística")
        self.assertContains(resp_post, "Criterios de cierre ERP")
        self.assertContains(resp_post, "Cierre global")
        self.assertContains(resp_post, "Cadena de control logístico")
        self.assertContains(resp_post, "Entrega logística a downstream")
        self.assertContains(resp_post, "Cierre por etapa documental")
        self.assertContains(resp_post, "Mesa de gobierno ERP")
        self.assertContains(resp_post, "Responsable")
        self.assertContains(resp_post, "Cierre")
        self.assertContains(resp_post, "Salud operativa ERP")

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
        self.assertContains(resp, "Cadena documental ERP")
        self.assertContains(resp, "Centro de mando ERP")
        self.assertContains(resp, "Cadena troncal de logística")
        self.assertContains(resp, "Depende de")
        self.assertContains(resp, "Dependencia")
        self.assertContains(resp, "Madurez ERP de logística")
        self.assertContains(resp, "Criterios de cierre ERP")
        self.assertContains(resp, "Cierre global")
        self.assertContains(resp, "Cadena de control logístico")
        self.assertContains(resp, "Cierre por etapa documental")
        self.assertContains(resp, "Salud operativa ERP")
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("erp_command_center", resp.context)
        self.assertIn("critical_path_rows", resp.context)
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp.context)
        self.assertIn("release_gate_rows", resp.context)
        self.assertIn("release_gate_completion", resp.context)
        self.assertIn("handoff_map", resp.context)
        self.assertIn("owner", resp.context["handoff_map"][0])
        self.assertIn("depends_on", resp.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp.context["handoff_map"][0])
        self.assertIn("next_step", resp.context["handoff_map"][0])
        self.assertIn("completion", resp.context["handoff_map"][0])
        self.assertTrue(resp.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
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
        self.assertContains(resp, "Entrega logística a downstream")
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "INCIDENCIAS")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_redirect_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)
