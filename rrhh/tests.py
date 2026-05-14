from django.contrib.auth.models import Group, User
from django.core.management import call_command
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from decimal import Decimal
from pathlib import Path
from rest_framework.test import APIClient
from unittest import SkipTest

from rrhh.models import Empleado, NominaConceptoLinea, NominaImportacion, NominaPeriodo
from rrhh.services.lista_raya import parse_lista_raya_xls


LISTA_RAYA_SAMPLE = Path("/Users/mauricioburgos/Downloads/Lista de raya del 16 al 31 de abril 2026.xls")


class CapitalHumanoServiceTests(TestCase):
    def test_generar_horas_extra_desde_asistencia(self):
        from datetime import date, datetime, time
        from zoneinfo import ZoneInfo

        from rrhh.models import AsistenciaEmpleado, HoraExtra, Turno
        from rrhh.services import calcular_horas_extra, generar_horas_extra_automatico

        empleado = Empleado.objects.create(nombre="Empleado HE", salario_diario="400.00")
        turno = Turno.objects.create(
            nombre="Matutino",
            hora_entrada=time(8, 0),
            hora_salida=time(16, 0),
            tolerancia_minutos=10,
        )
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=empleado,
            fecha=date(2026, 5, 13),
            entrada=datetime(2026, 5, 13, 8, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            salida=datetime(2026, 5, 13, 17, 15, tzinfo=ZoneInfo("America/Mazatlan")),
            minutos_trabajados=555,
            turno=turno,
            fuente="manual",
        )

        self.assertEqual(calcular_horas_extra(asistencia), Decimal("1.25"))
        he = generar_horas_extra_automatico(asistencia)
        self.assertIsNotNone(he)
        self.assertEqual(HoraExtra.objects.count(), 1)
        self.assertEqual(he.horas, Decimal("1.25"))

        he.estado = "autorizado"
        he.horas = Decimal("1.00")
        he.save(update_fields=["estado", "horas"])
        asistencia.minutos_trabajados = 600
        asistencia.save(update_fields=["minutos_trabajados"])

        actualizado = generar_horas_extra_automatico(asistencia)
        actualizado.refresh_from_db()
        self.assertEqual(actualizado.estado, "autorizado")
        self.assertEqual(actualizado.horas, Decimal("1.00"))


class CapitalHumanoAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username="empleado.app",
            email="empleado.app@example.com",
            password="pass123",
        )
        self.empleado = Empleado.objects.create(
            nombre="Empleado App",
            email="empleado.app@example.com",
            salario_diario="350.00",
        )
        self.client = APIClient()
        self.client.force_authenticate(user=self.user)

    def test_permiso_api_crea_folio_para_empleado_actual(self):
        resp = self.client.post(
            reverse("rrhh:permiso-list"),
            {
                "tipo": "cita_medica",
                "fecha_inicio": timezone.datetime(2026, 5, 14, 10, 0, tzinfo=timezone.get_current_timezone()).isoformat(),
                "fecha_fin": timezone.datetime(2026, 5, 14, 12, 0, tzinfo=timezone.get_current_timezone()).isoformat(),
                "motivo": "Consulta programada",
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["empleado"], self.empleado.id)
        self.assertTrue(resp.data["folio"].startswith("PS-"))
        self.assertEqual(resp.data["estado"], "solicitado")

    def test_hora_extra_api_crea_para_empleado_actual_por_nombre_reordenado(self):
        user = User.objects.create_user(
            username="paula.lugo",
            first_name="Paula Elizabeth",
            last_name="Lugo Espinoza",
            email="capitalhumano@pollyanasdolce.com",
            password="pass123",
        )
        empleado = Empleado.objects.create(
            nombre="LUGO ESPINOZA PAULA ELIZABETH",
            salario_diario="500.00",
        )
        self.client.force_authenticate(user=user)

        me_resp = self.client.get(reverse("rrhh:capital_humano_me"))
        self.assertEqual(me_resp.status_code, 200)
        self.assertEqual(me_resp.data["empleado"], empleado.id)

        resp = self.client.post(
            reverse("rrhh:hora-extra-list"),
            {
                "fecha": "2026-05-14",
                "horas": "2.50",
                "notas": "Cierre de sucursal",
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["empleado"], empleado.id)
        self.assertEqual(resp.data["estado"], "pendiente")
        self.assertEqual(resp.data["notas"], "Cierre de sucursal")

    def test_usuario_sin_empleado_no_se_vincula_a_otro_empleado(self):
        Empleado.objects.create(nombre="XANTECO MENA MARISOL", salario_diario="500.00")
        user = User.objects.create_user(
            username="yesenia.soto",
            first_name="Yesenia",
            last_name="Soto",
            email="admon.yesenia@pollyanasdolce.com",
            password="pass123",
        )
        self.client.force_authenticate(user=user)

        me_resp = self.client.get(reverse("rrhh:capital_humano_me"))
        self.assertEqual(me_resp.status_code, 200)
        self.assertIsNone(me_resp.data["empleado"])

        resp = self.client.post(
            reverse("rrhh:hora-extra-list"),
            {
                "fecha": "2026-05-14",
                "horas": "1.00",
                "notas": "No debe crear",
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 400)

    def test_rutas_capital_humano_cargan(self):
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        self.user.groups.add(rrhh_group)
        self.client.force_login(self.user)

        for url_name in ["rrhh_dashboard", "rrhh_pwa"]:
            resp = self.client.get(reverse(f"rrhh:{url_name}"))
            self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Registrar horas extra")


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
        self.assertContains(resp, "Alta de empleado")
        self.assertContains(resp, "Vista rápida")
        self.assertContains(resp, "Catálogo de empleados")
        self.assertContains(resp, "Modificar")
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

        empleado = Empleado.objects.get(nombre="Empleado Demo")
        resp_update = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "update",
                "empleado_id": str(empleado.id),
                "nombre": "Empleado Demo Editado",
                "rfc": "DEMO-010101-AA1",
                "curp": "DEMO010101HSLAAA01",
                "nss": "11-22-33-4444-5",
                "area": "Administración",
                "puesto": "Coordinador",
                "tipo_contrato": "FIJO",
                "fecha_ingreso": "2026-04-30",
                "salario_diario": "500.00",
                "telefono": "6870000000",
                "email": "demo@example.com",
                "sucursal": "Matriz",
                "activo": "on",
            },
            follow=True,
        )
        self.assertEqual(resp_update.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.nombre, "Empleado Demo Editado")
        self.assertEqual(empleado.rfc, "DEMO-010101-AA1")
        self.assertEqual(empleado.area, "Administración")

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


class ListaRayaImportTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if not LISTA_RAYA_SAMPLE.exists():
            raise SkipTest("No está disponible el archivo real de lista de raya.")

    def test_parser_cuadra_con_totales_generales(self):
        result = parse_lista_raya_xls(LISTA_RAYA_SAMPLE)

        self.assertEqual(result.fecha_inicio.isoformat(), "2026-04-16")
        self.assertEqual(result.fecha_fin.isoformat(), "2026-04-30")
        self.assertEqual(len(result.empleados), 67)
        self.assertEqual(result.total_percepciones_calculado, result.total_percepciones_reportado)
        self.assertEqual(result.total_deducciones_calculado, result.total_deducciones_reportado)
        self.assertEqual(result.total_neto_calculado, result.total_neto_reportado)
        self.assertEqual(result.empleados[0].codigo, "2")
        self.assertEqual(result.empleados[0].rfc, "LOMM-750126-DB4")
        self.assertEqual(result.empleados[0].curp, "LOMM-750126-MSLPDR06")

    def test_command_dry_run_no_toca_base(self):
        call_command("importar_lista_raya", str(LISTA_RAYA_SAMPLE))

        self.assertEqual(Empleado.objects.count(), 0)
        self.assertEqual(NominaPeriodo.objects.count(), 0)
        self.assertEqual(NominaImportacion.objects.count(), 0)

    def test_command_commit_importa_periodo_y_conceptos(self):
        call_command("importar_lista_raya", str(LISTA_RAYA_SAMPLE), "--commit")

        periodo = NominaPeriodo.objects.get(fecha_inicio="2026-04-16", fecha_fin="2026-04-30")
        self.assertEqual(Empleado.objects.count(), 67)
        self.assertEqual(periodo.lineas.count(), 67)
        self.assertEqual(NominaConceptoLinea.objects.count(), 336)
        self.assertEqual(periodo.total_bruto, periodo.importaciones.first().total_percepciones)
        self.assertEqual(periodo.total_descuentos, periodo.importaciones.first().total_deducciones)
        self.assertEqual(periodo.total_neto, periodo.importaciones.first().total_neto)
