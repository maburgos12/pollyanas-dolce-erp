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
        self.assertContains(resp, "Centro de mando ERP")
        self.assertContains(resp, "Cadena documental ERP")
        self.assertContains(resp, "Cadena troncal comercial")
        self.assertContains(resp, "Ruta crítica ERP")
        self.assertContains(resp, "Radar ejecutivo ERP")
        self.assertContains(resp, "Depende de")
        self.assertContains(resp, "Dependencia")
        self.assertContains(resp, "Madurez ERP comercial")
        self.assertContains(resp, "Criterios de cierre ERP")
        self.assertContains(resp, "Cierre global")
        self.assertContains(resp, "Cadena de control comercial")
        self.assertContains(resp, "Entrega comercial a downstream")
        self.assertContains(resp, "Cierre por etapa documental")
        self.assertContains(resp, "Mesa de gobierno ERP")
        self.assertContains(resp, "Responsable")
        self.assertContains(resp, "Cierre")
        self.assertContains(resp, "Salud operativa ERP")
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("crm_maturity_summary", resp.context)
        self.assertIn("crm_handoff_map", resp.context)
        self.assertIn("owner", resp.context["crm_handoff_map"][0])
        self.assertIn("depends_on", resp.context["crm_handoff_map"][0])
        self.assertIn("exit_criteria", resp.context["crm_handoff_map"][0])
        self.assertIn("next_step", resp.context["crm_handoff_map"][0])
        self.assertIn("completion", resp.context["crm_handoff_map"][0])
        self.assertIn("critical_path_rows", resp.context)
        self.assertTrue(resp.context["document_stage_rows"])
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
        self.assertTrue(resp.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp.context)
        self.assertTrue(resp.context["operational_health_cards"])
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("executive_radar_rows", resp.context)
        self.assertIn("erp_command_center", resp.context)

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
        resp_index = self.client.get(reverse("crm:pedidos"))
        self.assertEqual(resp_index.status_code, 200)
        self.assertContains(resp_index, "Centro de mando ERP")
        self.assertContains(resp_index, "Cockpit comercial operativo")
        self.assertContains(resp_index, "Cadena documental ERP")
        self.assertContains(resp_index, "Cadena troncal comercial")
        self.assertContains(resp_index, "Ruta crítica ERP")
        self.assertContains(resp_index, "Radar ejecutivo ERP")
        self.assertContains(resp_index, "Depende de")
        self.assertContains(resp_index, "Dependencia")
        self.assertContains(resp_index, "Madurez ERP comercial")
        self.assertContains(resp_index, "Criterios de cierre ERP")
        self.assertContains(resp_index, "Cierre global")
        self.assertContains(resp_index, "Cadena de control comercial")
        self.assertContains(resp_index, "Entrega comercial a downstream")
        self.assertContains(resp_index, "Cierre por etapa documental")
        self.assertContains(resp_index, "Mesa de gobierno ERP")
        self.assertContains(resp_index, "Responsable")
        self.assertContains(resp_index, "Cierre")
        self.assertContains(resp_index, "Salud operativa ERP")
        self.assertTrue(resp_index.context["focus_cards"])
        self.assertTrue(resp_index.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_index.context["enterprise_chain"][0])
        self.assertIn("crm_maturity_summary", resp_index.context)
        self.assertIn("crm_handoff_map", resp_index.context)
        self.assertIn("owner", resp_index.context["crm_handoff_map"][0])
        self.assertIn("depends_on", resp_index.context["crm_handoff_map"][0])
        self.assertIn("exit_criteria", resp_index.context["crm_handoff_map"][0])
        self.assertIn("next_step", resp_index.context["crm_handoff_map"][0])
        self.assertIn("completion", resp_index.context["crm_handoff_map"][0])
        self.assertIn("critical_path_rows", resp_index.context)
        self.assertTrue(resp_index.context["document_stage_rows"])
        self.assertIn("owner", resp_index.context["document_stage_rows"][0])
        self.assertIn("completion", resp_index.context["document_stage_rows"][0])
        self.assertTrue(resp_index.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_index.context)
        self.assertTrue(resp_index.context["operational_health_cards"])
        self.assertIn("erp_governance_rows", resp_index.context)
        self.assertIn("executive_radar_rows", resp_index.context)
        self.assertIn("erp_command_center", resp_index.context)
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
        self.assertContains(resp_detail, "Centro de mando ERP")
        self.assertContains(resp_detail, "Cadena documental ERP")
        self.assertContains(resp_detail, "Cadena troncal comercial")
        self.assertContains(resp_detail, "Ruta crítica ERP")
        self.assertContains(resp_detail, "Radar ejecutivo ERP")
        self.assertContains(resp_detail, "Depende de")
        self.assertContains(resp_detail, "Dependencia")
        self.assertContains(resp_detail, "Madurez ERP comercial")
        self.assertContains(resp_detail, "Criterios de cierre ERP")
        self.assertContains(resp_detail, "Cierre global")
        self.assertContains(resp_detail, "Cadena de control comercial")
        self.assertContains(resp_detail, "Entrega comercial a downstream")
        self.assertContains(resp_detail, "Cierre por etapa documental")
        self.assertContains(resp_detail, "Mesa de gobierno ERP")
        self.assertContains(resp_detail, "Responsable")
        self.assertContains(resp_detail, "Cierre")
        self.assertContains(resp_detail, "Salud operativa ERP")
        self.assertTrue(resp_detail.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_detail.context["enterprise_chain"][0])
        self.assertIn("crm_maturity_summary", resp_detail.context)
        self.assertIn("crm_handoff_map", resp_detail.context)
        self.assertIn("owner", resp_detail.context["crm_handoff_map"][0])
        self.assertIn("depends_on", resp_detail.context["crm_handoff_map"][0])
        self.assertIn("exit_criteria", resp_detail.context["crm_handoff_map"][0])
        self.assertIn("next_step", resp_detail.context["crm_handoff_map"][0])
        self.assertIn("completion", resp_detail.context["crm_handoff_map"][0])
        self.assertIn("critical_path_rows", resp_detail.context)
        self.assertTrue(resp_detail.context["document_stage_rows"])
        self.assertIn("owner", resp_detail.context["document_stage_rows"][0])
        self.assertIn("completion", resp_detail.context["document_stage_rows"][0])
        self.assertTrue(resp_detail.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_detail.context)
        self.assertTrue(resp_detail.context["operational_health_cards"])
        self.assertIn("erp_governance_rows", resp_detail.context)
        self.assertIn("executive_radar_rows", resp_detail.context)
        self.assertIn("erp_command_center", resp_detail.context)
        pedido.refresh_from_db()
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_CONFIRMADO)
        self.assertTrue(
            SeguimientoPedido.objects.filter(
                pedido=pedido,
                estatus_nuevo=PedidoCliente.ESTATUS_CONFIRMADO,
                comentario__icontains="anticipo",
            ).exists()
        )

    def test_pedidos_can_focus_operational_subset(self):
        cliente = Cliente.objects.create(nombre="Cliente Foco")
        PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido en producción",
            estatus=PedidoCliente.ESTATUS_EN_PRODUCCION,
        )

        resp = self.client.get(reverse("crm:pedidos"), {"enterprise_focus": "PRODUCCION"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "PRODUCCION")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_redirects_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("crm:pedidos"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)
