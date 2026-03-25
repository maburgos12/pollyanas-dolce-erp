from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse


class RRHHViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="rrhh", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        self.user.groups.add(rrhh_group)
        self.client.login(username="rrhh", password="pass123")

    def test_empleados_view_and_create(self):
        resp = self.client.get(reverse("rrhh:empleados"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "RRHH · Empleados")
        self.assertContains(resp, "Cockpit operativo de RRHH")
        self.assertContains(resp, "Cadena documental ERP")
        self.assertContains(resp, "Cadena troncal de RRHH")
        self.assertContains(resp, "Ruta crítica ERP")
        self.assertContains(resp, "Radar ejecutivo ERP")
        self.assertContains(resp, "Depende de")
        self.assertContains(resp, "Dependencia")
        self.assertContains(resp, "Madurez ERP de RRHH")
        self.assertContains(resp, "Criterios de cierre ERP")
        self.assertContains(resp, "Cierre global")
        self.assertContains(resp, "Cadena de control de RRHH")
        self.assertContains(resp, "Entrega de RRHH a downstream")
        self.assertContains(resp, "Cierre por etapa documental")
        self.assertContains(resp, "Mesa de gobierno ERP")
        self.assertContains(resp, "Centro de mando ERP")
        self.assertContains(resp, "Responsable")
        self.assertContains(resp, "Cierre")
        self.assertContains(resp, "Salud operativa ERP")
        self.assertTrue(resp.context["focus_cards"])
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp.context)
        self.assertIn("critical_path_rows", resp.context)
        self.assertIn("handoff_map", resp.context)
        self.assertIn("owner", resp.context["handoff_map"][0])
        self.assertIn("depends_on", resp.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp.context["handoff_map"][0])
        self.assertIn("next_step", resp.context["handoff_map"][0])
        self.assertIn("completion", resp.context["handoff_map"][0])
        self.assertTrue(resp.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("executive_radar_rows", resp.context)
        self.assertIn("erp_command_center", resp.context)
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
        self.assertTrue(resp.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp.context)
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Demo",
                "area": "Producción",
                "puesto": "Pastelero",
                "salario_diario": "450.00",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Empleado Demo")

    def test_empleados_can_focus_operational_subset(self):
        self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Sin Area",
                "puesto": "Auxiliar",
                "salario_diario": "250.00",
            },
            follow=True,
        )
        resp = self.client.get(reverse("rrhh:empleados"), {"enterprise_focus": "SIN_AREA"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "SIN_AREA")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_nomina_create_and_line(self):
        self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Nómina",
                "area": "Ventas",
                "puesto": "Vendedor",
                "salario_diario": "350.00",
            },
            follow=True,
        )

        resp_nomina_index = self.client.get(reverse("rrhh:nomina"))
        self.assertEqual(resp_nomina_index.status_code, 200)
        self.assertContains(resp_nomina_index, "Cadena documental ERP")
        self.assertContains(resp_nomina_index, "Cadena troncal de RRHH")
        self.assertContains(resp_nomina_index, "Ruta crítica ERP")
        self.assertContains(resp_nomina_index, "Radar ejecutivo ERP")
        self.assertContains(resp_nomina_index, "Depende de")
        self.assertContains(resp_nomina_index, "Dependencia")
        self.assertContains(resp_nomina_index, "Madurez ERP de RRHH")
        self.assertContains(resp_nomina_index, "Criterios de cierre ERP")
        self.assertContains(resp_nomina_index, "Cierre global")
        self.assertContains(resp_nomina_index, "Cadena de control de RRHH")
        self.assertContains(resp_nomina_index, "Entrega de RRHH a downstream")
        self.assertContains(resp_nomina_index, "Cierre por etapa documental")
        self.assertContains(resp_nomina_index, "Mesa de gobierno ERP")
        self.assertContains(resp_nomina_index, "Centro de mando ERP")
        self.assertContains(resp_nomina_index, "Responsable")
        self.assertContains(resp_nomina_index, "Cierre")
        self.assertContains(resp_nomina_index, "Salud operativa ERP")
        self.assertTrue(resp_nomina_index.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_nomina_index.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp_nomina_index.context)
        self.assertIn("critical_path_rows", resp_nomina_index.context)
        self.assertIn("handoff_map", resp_nomina_index.context)
        self.assertIn("owner", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("depends_on", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("next_step", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("completion", resp_nomina_index.context["handoff_map"][0])
        self.assertTrue(resp_nomina_index.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp_nomina_index.context)
        self.assertIn("executive_radar_rows", resp_nomina_index.context)
        self.assertIn("erp_command_center", resp_nomina_index.context)
        self.assertIn("owner", resp_nomina_index.context["document_stage_rows"][0])
        self.assertIn("completion", resp_nomina_index.context["document_stage_rows"][0])
        self.assertTrue(resp_nomina_index.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_nomina_index.context)
        self.assertTrue(resp_nomina_index.context["operational_health_cards"])

        resp_nomina = self.client.post(
            reverse("rrhh:nomina"),
            {
                "tipo_periodo": "QUINCENAL",
                "fecha_inicio": "2026-02-01",
                "fecha_fin": "2026-02-15",
                "estatus": "BORRADOR",
            },
            follow=True,
        )
        self.assertEqual(resp_nomina.status_code, 200)
        self.assertContains(resp_nomina, "Capturar línea de nómina")
        self.assertContains(resp_nomina, "Cadena documental ERP")
        self.assertContains(resp_nomina, "Cadena troncal de RRHH")
        self.assertContains(resp_nomina, "Ruta crítica ERP")
        self.assertContains(resp_nomina, "Radar ejecutivo ERP")
        self.assertContains(resp_nomina, "Depende de")
        self.assertContains(resp_nomina, "Dependencia")
        self.assertContains(resp_nomina, "Madurez ERP de RRHH")
        self.assertContains(resp_nomina, "Criterios de cierre ERP")
        self.assertContains(resp_nomina, "Cierre global")
        self.assertContains(resp_nomina, "Cadena de control de RRHH")
        self.assertContains(resp_nomina, "Entrega de RRHH a downstream")
        self.assertContains(resp_nomina, "Cierre por etapa documental")
        self.assertContains(resp_nomina, "Mesa de gobierno ERP")
        self.assertContains(resp_nomina, "Centro de mando ERP")
        self.assertContains(resp_nomina, "Responsable")
        self.assertContains(resp_nomina, "Cierre")
        self.assertContains(resp_nomina, "Salud operativa ERP")
        self.assertTrue(resp_nomina.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_nomina.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp_nomina.context)
        self.assertIn("critical_path_rows", resp_nomina.context)
        self.assertIn("handoff_map", resp_nomina.context)
        self.assertIn("owner", resp_nomina.context["handoff_map"][0])
        self.assertIn("depends_on", resp_nomina.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp_nomina.context["handoff_map"][0])
        self.assertIn("next_step", resp_nomina.context["handoff_map"][0])
        self.assertIn("completion", resp_nomina.context["handoff_map"][0])
        self.assertTrue(resp_nomina.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp_nomina.context)
        self.assertIn("executive_radar_rows", resp_nomina.context)
        self.assertIn("erp_command_center", resp_nomina.context)
        self.assertIn("owner", resp_nomina.context["document_stage_rows"][0])
        self.assertIn("completion", resp_nomina.context["document_stage_rows"][0])
        self.assertTrue(resp_nomina.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_nomina.context)
        self.assertTrue(resp_nomina.context["operational_health_cards"])

        from rrhh.models import Empleado, NominaPeriodo

        empleado = Empleado.objects.get(nombre="Empleado Nómina")
        periodo = NominaPeriodo.objects.first()

        resp_line = self.client.post(
            reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            {
                "action": "add_line",
                "empleado_id": empleado.id,
                "dias_trabajados": "15",
                "bonos": "500",
                "descuentos": "120",
            },
            follow=True,
        )
        self.assertEqual(resp_line.status_code, 200)
        self.assertContains(resp_line, "Cadena documental ERP")
        self.assertContains(resp_line, "Cadena troncal de RRHH")
        self.assertContains(resp_line, "Ruta crítica ERP")
        self.assertContains(resp_line, "Radar ejecutivo ERP")
        self.assertContains(resp_line, "Depende de")
        self.assertContains(resp_line, "Dependencia")
        self.assertContains(resp_line, "Madurez ERP de RRHH")
        self.assertContains(resp_line, "Criterios de cierre ERP")
        self.assertContains(resp_line, "Cierre global")
        self.assertContains(resp_line, "Cadena de control de RRHH")
        self.assertContains(resp_line, "Entrega de RRHH a downstream")
        self.assertContains(resp_line, "Cierre por etapa documental")
        self.assertContains(resp_line, "Mesa de gobierno ERP")
        self.assertContains(resp_line, "Centro de mando ERP")
        self.assertContains(resp_line, "Responsable")
        self.assertContains(resp_line, "Cierre")
        self.assertContains(resp_line, "Salud operativa ERP")
        self.assertIn("dependency_status", resp_line.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp_line.context)
        self.assertIn("critical_path_rows", resp_line.context)
        self.assertIn("handoff_map", resp_line.context)
        self.assertIn("owner", resp_line.context["handoff_map"][0])
        self.assertIn("depends_on", resp_line.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp_line.context["handoff_map"][0])
        self.assertIn("next_step", resp_line.context["handoff_map"][0])
        self.assertIn("completion", resp_line.context["handoff_map"][0])
        self.assertIn("erp_governance_rows", resp_line.context)
        self.assertIn("executive_radar_rows", resp_line.context)
        self.assertIn("erp_command_center", resp_line.context)
        self.assertIn("owner", resp_line.context["document_stage_rows"][0])
        self.assertIn("completion", resp_line.context["document_stage_rows"][0])
        self.assertTrue(resp_line.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_line.context)
        periodo.refresh_from_db()
        self.assertGreater(periodo.total_neto, 0)

    def test_redirect_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("rrhh:empleados"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)
