from decimal import Decimal
from io import BytesIO
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TestCase, TransactionTestCase
from django.urls import reverse
from openpyxl import Workbook, load_workbook

from core.access import ROLE_ADMIN, ROLE_ALMACEN, ROLE_VENTAS
from core.models import AuditLog

from .models import Activo, OrdenMantenimiento, PlanMantenimiento
from django.utils import timezone


class ActivosFlowsTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_user("admin_activos", "admin_activos@example.com", "test12345")
        self.almacen = user_model.objects.create_user("almacen_activos", "almacen_activos@example.com", "test12345")
        self.ventas = user_model.objects.create_user("ventas_activos", "ventas_activos@example.com", "test12345")

        Group.objects.get_or_create(name=ROLE_ADMIN)[0].user_set.add(self.admin)
        Group.objects.get_or_create(name=ROLE_ALMACEN)[0].user_set.add(self.almacen)
        Group.objects.get_or_create(name=ROLE_VENTAS)[0].user_set.add(self.ventas)

    def test_task_pages_use_shared_navigation_and_keep_contextual_routes(self):
        self.client.force_login(self.admin)
        from core.navigation import NAV_GROUPS

        items = next(group["items"] for group in NAV_GROUPS if group["key"] == "administracion")
        self.assertEqual(
            [item[2] for item in items if item[0] == "activos"],
            ["Resumen de mantenimiento", "Equipos", "Mantenimiento preventivo",
             "Órdenes de mantenimiento", "Reportes de servicio"],
        )
        for route in ("activos", "planes", "ordenes", "reportes", "dashboard", "calendario",
                      "registro_rapido", "solicitudes_falla"):
            with self.subTest(route=route):
                response = self.client.get(reverse(f"activos:{route}"))
                self.assertEqual(response.status_code, 200)
                self.assertLessEqual(response.content.decode().count('class="module-tabs'), 1)
                self.assertContains(response, "Equipos")
        self.assertContains(self.client.get(reverse("activos:planes")), reverse("activos:calendario"))
        self.assertContains(self.client.get(reverse("activos:ordenes")), reverse("activos:registro_rapido"))
        self.assertContains(self.client.get(reverse("activos:reportes")), reverse("activos:solicitudes_falla"))
        self.assertContains(self.client.get(reverse("activos:reportes")), "Solicitudes históricas de falla")

    def test_equipment_list_precedes_secondary_capture_and_keeps_actions(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="Equipo visible")
        response = self.client.get(reverse("activos:activos"))
        html = response.content.decode()
        self.assertLess(html.index('id="catalogo-equipos"'), html.index('id="nuevo-equipo"'))
        self.assertContains(response, '<summary class="card-header">Nuevo equipo</summary>', html=True)
        self.assertContains(response, "Editar ficha técnica")
        self.assertContains(response, 'value="update_identity"')
        self.assertContains(response, 'value="set_estado"')
        self.assertContains(response, 'value="toggle_activo"')
        self.assertContains(response, 'value="import_bitacora"')
        self.assertContains(response, "data-async-action")
        self.assertContains(response, "export=depuracion_csv")
        self.assertContains(response, "export=template_bitacora_xlsx")
        self.assertContains(response, reverse("activos:etiquetas"))
        self.assertContains(response, f'id="activo-{activo.id}"')
        self.assertContains(response, "Datos pendientes por campo")

    @patch("activos.views.can_manage_inventario", return_value=False)
    def test_equipment_management_guard_hides_write_actions(self, manage_permission):
        self.client.force_login(self.almacen)
        Activo.objects.create(nombre="Equipo consulta")
        response = self.client.get(reverse("activos:activos"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'id="catalogo-equipos"')
        for action in ("create_activo", "update_identity", "set_estado", "toggle_activo", "import_bitacora"):
            self.assertNotContains(response, f'value="{action}"')
        self.assertNotContains(response, 'href="#nuevo-equipo"')
        self.assertNotContains(response, reverse("activos:etiquetas"))

    def test_lists_precede_order_and_report_capture(self):
        self.client.force_login(self.admin)
        ordenes = self.client.get(reverse("activos:ordenes"))
        html = ordenes.content.decode()
        self.assertLess(html.index("Órdenes registradas"), html.index('value="create_orden"'))
        for action in ("create_orden", "update_costos", "update_factura"):
            self.assertContains(ordenes, f'value="{action}"')
        self.assertContains(ordenes, 'name="enterprise_gap"')
        self.assertContains(ordenes, "export=xlsx")
        reportes = self.client.get(reverse("activos:reportes"))
        html = reportes.content.decode()
        self.assertLess(html.index("Reportes correctivos"), html.index('id="nuevo-reporte"'))
        self.assertContains(reportes, 'name="semaforo"')
        self.assertContains(reportes, "export=csv")
        self.assertContains(reportes, "Levantar reporte")

    def test_zero_plans_reports_missing_preventive_coverage(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:planes"))
        self.assertContains(response, "Sin cobertura preventiva registrada")
        self.assertNotContains(response, 'class="planes-kpi-num ok"')

    def test_admin_can_create_activo_from_ui(self):
        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "create_activo",
                "nombre": "Refrigerador Cámara 01",
                "categoria": "Refrigeración",
                "estado": "OPERATIVO",
                "criticidad": "ALTA",
                "activo": "1",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        activo = Activo.objects.get(nombre="Refrigerador Cámara 01")
        self.assertEqual(activo.creado_por, self.admin)
        self.assertNotContains(response, "Cockpit operativo de activos")

    def test_almacen_can_raise_service_report(self):
        activo = Activo.objects.create(nombre="AA Oficina", categoria="Aire")
        self.client.force_login(self.almacen)
        response = self.client.post(
            reverse("activos:reportes"),
            {
                "activo_id": str(activo.id),
                "prioridad": "MEDIA",
                "descripcion": "No enfría correctamente",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            OrdenMantenimiento.objects.filter(activo_ref=activo, tipo=OrdenMantenimiento.TIPO_CORRECTIVO).exists()
        )

    def test_ventas_cannot_access_activos_module(self):
        self.client.force_login(self.ventas)
        response = self.client.get(reverse("activos:activos"))
        self.assertEqual(response.status_code, 403)

    def test_admin_can_create_plan(self):
        activo = Activo.objects.create(nombre="Horno 02", categoria="Hornos")
        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("activos:planes"),
            {
                "action": "create_plan",
                "activo_id": str(activo.id),
                "nombre": "Mantenimiento mensual horno",
                "tipo": PlanMantenimiento.TIPO_PREVENTIVO,
                "frecuencia_dias": "30",
                "estatus": PlanMantenimiento.ESTATUS_ACTIVO,
                "activo": "1",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(PlanMantenimiento.objects.filter(activo_ref=activo).exists())

    def test_activos_catalog_shows_enterprise_cockpit_and_focus(self):
        self.client.force_login(self.admin)
        Activo.objects.create(nombre="Activo sin categoría", categoria="", activo=True)
        response = self.client.get(reverse("activos:activos"), {"master_gap": "SIN_CATEGORIA"})
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Centro de mando ERP")
        self.assertNotContains(response, "Cockpit operativo de activos")
        self.assertNotContains(response, "Entrega de activos a downstream")
        self.assertNotContains(response, "Ruta crítica ERP")
        self.assertNotContains(response, "Radar ejecutivo ERP")
        self.assertContains(response, "Quitar foco")
        self.assertIn("erp_command_center", response.context)
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        self.assertTrue(response.context["enterprise_focus_cards"])
        self.assertIsNotNone(response.context["focus_summary"])
        self.assertEqual(response.context["focus_summary"]["label"], "Sin categoría")

    def test_dashboard_shows_release_gate_enterprise_block(self):
        # El dashboard activos es ahora una shell JS que carga datos vía API.
        self.client.force_login(self.admin)
        Activo.objects.create(nombre="Activo QA", categoria="Frío", activo=True)
        response = self.client.get(reverse("activos:dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Resumen de mantenimiento")
        self.assertContains(response, "Director General")

    def test_planes_view_shows_enterprise_cards_and_filter(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="Horno sin responsable", categoria="Hornos")
        PlanMantenimiento.objects.create(
            activo_ref=activo,
            nombre="Plan sin responsable",
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
            frecuencia_dias=30,
            responsable="",
        )
        response = self.client.get(reverse("activos:planes"), {"enterprise_gap": "SIN_RESPONSABLE"})
        self.assertEqual(response.status_code, 200)
        # El rediseño de planes (792b2503 + estandarización 71e46ad1) retiró el
        # cockpit de gobernanza del template; las estructuras siguen siendo
        # contrato de contexto de la vista.
        self.assertContains(response, "Planes de mantenimiento")
        self.assertTrue(response.context["enterprise_cards"])
        self.assertTrue(response.context["enterprise_chain"])
        self.assertTrue(response.context["operational_health_cards"])
        self.assertTrue(response.context["document_stage_rows"])
        self.assertIn("erp_command_center", response.context)
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        rows = response.context["planes_rows"]
        self.assertTrue(rows)
        self.assertEqual(rows[0]["enterprise"]["status_label"], "Pendiente")
        self.assertTrue(response.context["enterprise_chain"])
        self.assertIn("dependency_status", response.context["enterprise_chain"][0])

    def test_export_planes_csv(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="Horno QA", categoria="Hornos")
        PlanMantenimiento.objects.create(
            activo_ref=activo,
            nombre="Plan QA",
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
            frecuencia_dias=30,
            proxima_ejecucion=timezone.localdate(),
        )
        response = self.client.get(reverse("activos:planes"), {"export": "csv"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.get("Content-Type", ""))
        self.assertIn("activos_planes_", response.get("Content-Disposition", ""))
        body = response.content.decode("utf-8")
        self.assertIn("activo_codigo,activo,plan,tipo,estatus", body)
        self.assertIn("Plan QA", body)

    def test_export_ordenes_xlsx(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="Batidora QA", categoria="Batidoras")
        OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            fecha_programada=timezone.localdate(),
            descripcion="Orden QA",
        )
        response = self.client.get(reverse("activos:ordenes"), {"export": "xlsx", "estatus": "ABIERTAS"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.get("Content-Type", ""),
        )
        self.assertIn("activos_ordenes_", response.get("Content-Disposition", ""))
        wb = load_workbook(filename=BytesIO(response.content))
        ws = wb.active
        headers = [cell.value for cell in ws[1]]
        self.assertEqual(
            headers[:7],
            ["folio", "activo_codigo", "activo", "plan", "tipo", "prioridad", "estatus"],
        )

    def test_admin_can_update_orden_costos_and_close(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="Conservador QA", categoria="Refrigeración")
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            estatus=OrdenMantenimiento.ESTATUS_EN_PROCESO,
            fecha_programada=timezone.localdate(),
            descripcion="Servicio costo",
        )
        response = self.client.post(
            reverse("activos:ordenes"),
            {
                "action": "update_costos",
                "orden_id": str(orden.id),
                "costo_repuestos": "1500.25",
                "costo_mano_obra": "800",
                "costo_otros": "120",
                "cerrar_orden": "1",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        orden.refresh_from_db()
        self.assertEqual(str(orden.costo_repuestos), "1500.25")
        self.assertEqual(str(orden.costo_mano_obra), "800.00")
        self.assertEqual(str(orden.costo_otros), "120.00")
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_CERRADA)

    def test_ordenes_view_shows_enterprise_cards_and_filter(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="AA sin responsable", categoria="Aire")
        OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_MEDIA,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            fecha_programada=timezone.localdate(),
            responsable="",
            descripcion="Orden sin responsable",
        )
        response = self.client.get(reverse("activos:ordenes"), {"enterprise_gap": "SIN_RESPONSABLE"})
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Centro de mando ERP")
        self.assertTrue(response.context["enterprise_cards"])
        self.assertTrue(response.context["enterprise_focus_cards"])
        self.assertTrue(response.context["enterprise_chain"])
        self.assertTrue(response.context["operational_health_cards"])
        self.assertTrue(response.context["document_stage_rows"])
        self.assertIn("erp_command_center", response.context)
        self.assertNotContains(response, "Cadena documental ERP")
        self.assertNotContains(response, "Cadena troncal del mantenimiento")
        self.assertNotContains(response, "Ruta crítica ERP")
        self.assertNotContains(response, "Radar ejecutivo ERP")
        self.assertNotContains(response, "Cockpit documental de órdenes")
        self.assertNotContains(response, "Salud operativa ERP")
        self.assertNotContains(response, "Cierre por etapa documental")
        self.assertNotContains(response, "Mesa de gobierno ERP")
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        rows = response.context["ordenes_rows"]
        self.assertTrue(rows)
        self.assertEqual(rows[0]["enterprise"]["status_label"], "Pendiente")
        self.assertIsNotNone(response.context["focus_summary"])

    def test_calendario_days_window(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:calendario"), {"days": "15"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Centro de mando ERP")
        self.assertEqual(response.context["days"], 15)
        self.assertTrue(response.context["enterprise_chain"])
        self.assertTrue(response.context["operational_health_cards"])
        self.assertTrue(response.context["document_stage_rows"])
        self.assertIn("erp_command_center", response.context)
        self.assertContains(response, "Cadena documental ERP")
        self.assertContains(response, "Cadena troncal del mantenimiento")
        self.assertContains(response, "Ruta crítica ERP")
        self.assertContains(response, "Radar ejecutivo ERP")
        self.assertContains(response, "Salud operativa ERP")
        self.assertContains(response, "Cierre por etapa documental")
        self.assertContains(response, "Mesa de gobierno ERP")
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)

    def test_export_reportes_servicio_csv(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="Refrigerador QA", categoria="Refrigeración")
        OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_MEDIA,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            fecha_programada=timezone.localdate(),
            descripcion="Falla de prueba",
        )
        response = self.client.get(
            reverse("activos:reportes"),
            {"export": "csv", "estatus": "ABIERTAS"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.get("Content-Type", ""))
        self.assertIn("activos_reportes_servicio_", response.get("Content-Disposition", ""))
        body = response.content.decode("utf-8")
        self.assertIn("folio,fecha,activo_codigo,activo,prioridad,estatus,semaforo,dias,descripcion,responsable", body)
        self.assertIn("Falla de prueba", body)

    def test_filter_reportes_servicio_by_semaforo(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="Horno QA2", categoria="Hornos")
        OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_MEDIA,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            fecha_programada=timezone.localdate() - timedelta(days=8),
            descripcion="Falla roja",
        )
        response = self.client.get(
            reverse("activos:reportes"),
            {"estatus": "ABIERTAS", "semaforo": "ROJO"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Centro de mando ERP")
        reportes = response.context["reportes"]
        self.assertTrue(reportes)
        self.assertTrue(all(item.get("semaforo_key") == "ROJO" for item in reportes))
        self.assertTrue(response.context["enterprise_chain"])
        self.assertTrue(response.context["enterprise_focus_cards"])
        self.assertTrue(response.context["operational_health_cards"])
        self.assertTrue(response.context["document_stage_rows"])
        self.assertIn("erp_command_center", response.context)
        self.assertNotContains(response, "Cadena documental ERP")
        self.assertNotContains(response, "Ruta crítica ERP")
        self.assertNotContains(response, "Cockpit de incidentes ERP")
        self.assertNotContains(response, "Salud operativa ERP")
        self.assertNotContains(response, "Cierre por etapa documental")
        self.assertNotContains(response, "Mesa de gobierno ERP")
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        self.assertIsNotNone(response.context["focus_summary"])

    def test_dashboard_alertas_criticas_context(self):
        # El dashboard activos es ahora una shell JS; los datos se cargan vía API.
        # Verificamos que la vista carga y que los activos/planes/órdenes se crean.
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="AA Critico", categoria="Aire", criticidad=Activo.CRITICIDAD_ALTA)
        PlanMantenimiento.objects.create(
            activo_ref=activo,
            nombre="Plan vencido QA",
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
            proxima_ejecucion=timezone.localdate() - timedelta(days=3),
            frecuencia_dias=30,
        )
        OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_CRITICA,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            fecha_programada=timezone.localdate(),
            descripcion="Orden critica QA",
        )
        response = self.client.get(reverse("activos:dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Resumen de mantenimiento")
        self.assertTrue(Activo.objects.filter(nombre="AA Critico").exists())
        self.assertTrue(PlanMantenimiento.objects.filter(activo_ref=activo).exists())
        self.assertTrue(OrdenMantenimiento.objects.filter(activo_ref=activo, prioridad=OrdenMantenimiento.PRIORIDAD_CRITICA).exists())

    def test_activos_catalog_filters_by_enterprise_gap(self):
        self.client.force_login(self.admin)
        sin_categoria = Activo.objects.create(nombre="Activo sin categoria", categoria="", estado=Activo.ESTADO_OPERATIVO)
        con_categoria = Activo.objects.create(nombre="Activo con categoria", categoria="Frío", estado=Activo.ESTADO_OPERATIVO)
        response = self.client.get(reverse("activos:activos"), {"master_gap": "SIN_CATEGORIA"})
        self.assertEqual(response.status_code, 200)
        rows = response.context["activos_rows"]
        self.assertTrue(any(row["activo"].id == sin_categoria.id for row in rows))
        self.assertFalse(any(row["activo"].id == con_categoria.id for row in rows))
        self.assertTrue(response.context["enterprise_cards"])
        self.assertTrue(response.context["enterprise_chain"])
        self.assertTrue(response.context["document_stage_rows"])
        self.assertNotContains(response, "Cadena documental ERP")
        self.assertNotContains(response, "Ruta crítica ERP")
        self.assertNotContains(response, "Cierre por etapa documental")
        self.assertNotContains(response, "Mesa de gobierno ERP")
        self.assertIn("owner", response.context["document_stage_rows"][0])
        self.assertIn("completion", response.context["document_stage_rows"][0])
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)

    def test_generar_ordenes_programadas_creates_preventive_order(self):
        self.client.force_login(self.admin)
        activo = Activo.objects.create(nombre="AA Planta", categoria="Aire", criticidad=Activo.CRITICIDAD_ALTA)
        plan = PlanMantenimiento.objects.create(
            activo_ref=activo,
            nombre="Plan semanal",
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
            proxima_ejecucion=timezone.localdate(),
            frecuencia_dias=7,
        )
        response = self.client.post(
            reverse("activos:planes"),
            {
                "action": "generar_ordenes_programadas",
                "scope": "overdue",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            OrdenMantenimiento.objects.filter(
                plan_ref=plan,
                tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
                fecha_programada=timezone.localdate(),
            ).exists()
        )

    def test_generar_ordenes_programadas_avoids_duplicates(self):
        self.client.force_login(self.admin)
        today = timezone.localdate()
        activo = Activo.objects.create(nombre="Horno Línea 2", categoria="Hornos")
        plan = PlanMantenimiento.objects.create(
            activo_ref=activo,
            nombre="Plan mensual",
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
            proxima_ejecucion=today,
            frecuencia_dias=30,
        )
        OrdenMantenimiento.objects.create(
            activo_ref=activo,
            plan_ref=plan,
            tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            fecha_programada=today,
            descripcion="Existente",
        )
        response = self.client.post(
            reverse("activos:planes"),
            {
                "action": "generar_ordenes_programadas",
                "scope": "overdue",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            OrdenMantenimiento.objects.filter(plan_ref=plan, fecha_programada=today).count(),
            1,
        )

    def test_export_activos_depuracion_csv(self):
        self.client.force_login(self.admin)
        Activo.objects.create(nombre="MATRIZ", categoria="Equipos", notas="")
        response = self.client.get(reverse("activos:activos"), {"export": "depuracion_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.get("Content-Type", ""))
        self.assertIn("activos_pendientes_depuracion_", response.get("Content-Disposition", ""))
        body = response.content.decode("utf-8")
        self.assertIn("codigo,nombre,ubicacion,categoria,estado,notas,motivos,acciones_sugeridas", body)
        self.assertIn("MATRIZ", body)

    def test_export_activos_depuracion_xlsx(self):
        self.client.force_login(self.admin)
        Activo.objects.create(nombre="NIO", categoria="Equipos", notas="")
        response = self.client.get(reverse("activos:activos"), {"export": "depuracion_xlsx"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.get("Content-Type", ""),
        )
        self.assertIn("activos_pendientes_depuracion_", response.get("Content-Disposition", ""))
        wb = load_workbook(filename=BytesIO(response.content))
        ws = wb.active
        headers = [cell.value for cell in ws[1]]
        self.assertEqual(
            headers,
            ["codigo", "nombre", "ubicacion", "categoria", "estado", "notas", "motivos", "acciones_sugeridas"],
        )

    def test_export_template_bitacora_csv(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:activos"), {"export": "template_bitacora_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.get("Content-Type", ""))
        self.assertIn("plantilla_bitacora_activos.csv", response.get("Content-Disposition", ""))
        body = response.content.decode("utf-8")
        self.assertIn("nombre,marca,modelo,serie,fecha_1,costo_1,fecha_2,costo_2", body)

    def test_export_template_bitacora_xlsx(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:activos"), {"export": "template_bitacora_xlsx"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.get("Content-Type", ""),
        )
        self.assertIn("plantilla_bitacora_activos.xlsx", response.get("Content-Disposition", ""))
        wb = load_workbook(filename=BytesIO(response.content))
        ws = wb.active
        headers = [cell.value for cell in ws[1]]
        self.assertEqual(headers, ["nombre", "marca", "modelo", "serie", "fecha_1", "costo_1", "fecha_2", "costo_2"])

    def test_admin_can_import_bitacora_from_ui_dry_run(self):
        self.client.force_login(self.admin)
        upload = self._build_bitacora_upload("bitacora_dryrun.xlsx")
        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "import_bitacora",
                "dry_run": "1",
                "archivo_bitacora": upload,
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(Activo.objects.filter(nombre="HORNO TEST UI").exists())

    def test_admin_can_import_bitacora_from_ui_apply(self):
        self.client.force_login(self.admin)
        upload = self._build_bitacora_upload("bitacora_apply.xlsx")
        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "import_bitacora",
                "archivo_bitacora": upload,
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(Activo.objects.filter(nombre="HORNO TEST UI").exists())
        self.assertTrue(OrdenMantenimiento.objects.filter(descripcion__icontains="bitácora histórica").exists())
        self.assertTrue(
            AuditLog.objects.filter(action="IMPORT", model="activos.BitacoraImport", user=self.admin).exists()
        )

    def test_activos_view_shows_import_runs_block(self):
        AuditLog.objects.create(
            user=self.admin,
            action="IMPORT",
            model="activos.BitacoraImport",
            object_id="demo",
            payload={"filename": "bitacora_test.csv", "filas_validas": 5, "source_format": "CSV"},
        )
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:activos"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Consultar importaciones de bitácora")
        self.assertContains(response, "bitacora_test.csv")

    def test_export_import_runs_csv(self):
        AuditLog.objects.create(
            user=self.admin,
            action="IMPORT",
            model="activos.BitacoraImport",
            object_id="run1",
            payload={"filename": "historial_test.csv", "filas_validas": 3, "source_format": "CSV"},
        )
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:activos"), {"export": "import_runs_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.get("Content-Type", ""))
        self.assertIn("activos_import_bitacora_historial_", response.get("Content-Disposition", ""))
        body = response.content.decode("utf-8")
        self.assertIn("fecha,usuario,archivo,modo,formato,hoja", body)
        self.assertIn("historial_test.csv", body)

    def test_export_import_runs_xlsx(self):
        AuditLog.objects.create(
            user=self.admin,
            action="IMPORT",
            model="activos.BitacoraImport",
            object_id="run2",
            payload={"filename": "historial_test.xlsx", "filas_validas": 4, "source_format": "XLSX"},
        )
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:activos"), {"export": "import_runs_xlsx"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.get("Content-Type", ""),
        )
        self.assertIn("activos_import_bitacora_historial_", response.get("Content-Disposition", ""))
        wb = load_workbook(filename=BytesIO(response.content))
        ws = wb.active
        headers = [cell.value for cell in ws[1]]
        self.assertEqual(
            headers,
            [
                "fecha",
                "usuario",
                "archivo",
                "modo",
                "formato",
                "hoja",
                "filas_leidas",
                "filas_validas",
                "activos_creados",
                "activos_actualizados",
                "servicios_creados",
                "servicios_omitidos",
            ],
        )

    def test_import_runs_filter_by_mode(self):
        AuditLog.objects.create(
            user=self.admin,
            action="IMPORT",
            model="activos.BitacoraImport",
            object_id="run_aplicado",
            payload={"filename": "aplicado.xlsx", "dry_run": False, "source_format": "XLSX"},
        )
        AuditLog.objects.create(
            user=self.admin,
            action="IMPORT",
            model="activos.BitacoraImport",
            object_id="run_simulacion",
            payload={"filename": "simulacion.csv", "dry_run": True, "source_format": "CSV"},
        )
        self.client.force_login(self.admin)
        response = self.client.get(reverse("activos:activos"), {"import_mode": "SIMULACION"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "simulacion.csv")
        self.assertNotContains(response, "aplicado.xlsx")

    def test_export_import_runs_csv_respects_format_filter(self):
        AuditLog.objects.create(
            user=self.admin,
            action="IMPORT",
            model="activos.BitacoraImport",
            object_id="run_csv",
            payload={"filename": "only_csv.csv", "dry_run": False, "source_format": "CSV"},
        )
        AuditLog.objects.create(
            user=self.admin,
            action="IMPORT",
            model="activos.BitacoraImport",
            object_id="run_xlsx",
            payload={"filename": "other.xlsx", "dry_run": False, "source_format": "XLSX"},
        )
        self.client.force_login(self.admin)
        response = self.client.get(
            reverse("activos:activos"),
            {"export": "import_runs_csv", "import_format": "CSV"},
        )
        self.assertEqual(response.status_code, 200)
        body = response.content.decode("utf-8")
        self.assertIn("only_csv.csv", body)
        self.assertNotIn("other.xlsx", body)

    def test_admin_can_import_bitacora_csv_from_ui_apply(self):
        self.client.force_login(self.admin)
        upload = self._build_bitacora_csv_upload("bitacora_apply.csv")
        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "import_bitacora",
                "archivo_bitacora": upload,
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(Activo.objects.filter(nombre="HORNO TEST CSV").exists())
        self.assertTrue(OrdenMantenimiento.objects.filter(descripcion__icontains="bitácora histórica").exists())

    def test_admin_can_import_bitacora_csv_semicolon_decimal_comma(self):
        self.client.force_login(self.admin)
        upload = self._build_bitacora_csv_upload("bitacora_decimal.csv", semicolon_decimal=True)
        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "import_bitacora",
                "archivo_bitacora": upload,
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        activo = Activo.objects.get(nombre="HORNO TEST CSV DECIMAL")
        orden = OrdenMantenimiento.objects.filter(activo_ref=activo).order_by("-id").first()
        self.assertIsNotNone(orden)
        self.assertEqual(str(orden.costo_otros), "1250.75")

    def test_orden_folio_retries_on_collision(self):
        # Simula creación concurrente: el primer folio calculado ya existe;
        # save() debe reintentar con uno único en vez de reventar con IntegrityError.
        activo = Activo.objects.create(nombre="Equipo folio", categoria="Test")
        existente = OrdenMantenimiento.objects.create(
            activo_ref=activo,
            fecha_programada=timezone.localdate(),
            descripcion="ocupa folio",
        )
        with patch.object(
            OrdenMantenimiento,
            "_next_folio",
            side_effect=[existente.folio, "OM-RETRY-TEST-1"],
        ):
            nueva = OrdenMantenimiento(
                activo_ref=activo,
                fecha_programada=timezone.localdate(),
                descripcion="reintenta folio",
            )
            nueva.save()
        nueva.refresh_from_db()
        self.assertEqual(nueva.folio, "OM-RETRY-TEST-1")
        self.assertEqual(OrdenMantenimiento.objects.filter(activo_ref=activo).count(), 2)

    def test_edit_plan_recomputes_proxima_when_frecuencia_changes(self):
        # Bajar la frecuencia debe adelantar la próxima ejecución aunque el form
        # reenvíe la proxima anterior prellenada (no es override manual real).
        self.client.force_login(self.admin)
        ultima = timezone.localdate() - timedelta(days=5)
        activo = Activo.objects.create(nombre="Horno recalculo", categoria="Hornos")
        plan = PlanMantenimiento.objects.create(
            activo_ref=activo,
            nombre="Plan recalculo",
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
            frecuencia_dias=30,
            ultima_ejecucion=ultima,
            proxima_ejecucion=ultima + timedelta(days=30),
        )
        response = self.client.post(
            reverse("activos:planes"),
            {
                "action": "edit_plan",
                "plan_id": str(plan.id),
                "nombre": plan.nombre,
                "tipo": plan.tipo,
                "estatus": plan.estatus,
                "frecuencia_dias": "7",
                "ultima_ejecucion": str(ultima),
                "proxima_ejecucion": str(ultima + timedelta(days=30)),
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        plan.refresh_from_db()
        self.assertEqual(plan.frecuencia_dias, 7)
        self.assertEqual(plan.proxima_ejecucion, ultima + timedelta(days=7))

    def test_edit_plan_honors_manual_proxima_override(self):
        # Si el usuario captura una proxima distinta, se respeta (no se recalcula).
        self.client.force_login(self.admin)
        ultima = timezone.localdate() - timedelta(days=5)
        activo = Activo.objects.create(nombre="Horno override", categoria="Hornos")
        plan = PlanMantenimiento.objects.create(
            activo_ref=activo,
            nombre="Plan override",
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
            frecuencia_dias=30,
            ultima_ejecucion=ultima,
            proxima_ejecucion=ultima + timedelta(days=30),
        )
        manual = timezone.localdate() + timedelta(days=100)
        response = self.client.post(
            reverse("activos:planes"),
            {
                "action": "edit_plan",
                "plan_id": str(plan.id),
                "nombre": plan.nombre,
                "tipo": plan.tipo,
                "estatus": plan.estatus,
                "frecuencia_dias": "30",
                "ultima_ejecucion": str(ultima),
                "proxima_ejecucion": str(manual),
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        plan.refresh_from_db()
        self.assertEqual(plan.proxima_ejecucion, manual)

    @staticmethod
    def _build_bitacora_upload(filename: str) -> SimpleUploadedFile:
        wb = Workbook()
        ws = wb.active
        ws.title = "Hoja1"
        ws.cell(2, 2, "HORNOS")
        ws.cell(2, 3, "MARCA")
        ws.cell(2, 4, "MODELO")
        ws.cell(2, 5, "SERIE:")
        ws.cell(2, 6, "FECHA MANTENIMIENTO")
        ws.cell(2, 7, "COSTO")
        ws.cell(2, 8, "FECHA MANTENIMIENTO")
        ws.cell(2, 9, "COSTO")
        ws.cell(3, 2, "PRODUCCION MATRIZ")
        ws.cell(4, 2, "HORNO TEST UI")
        ws.cell(4, 3, "ALPHA")
        ws.cell(4, 4, "HX-10")
        ws.cell(4, 5, "SER-001")
        ws.cell(4, 6, "2026-02-20")
        ws.cell(4, 7, 1200)
        stream = BytesIO()
        wb.save(stream)
        stream.seek(0)
        return SimpleUploadedFile(
            filename,
            stream.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @staticmethod
    def _build_bitacora_csv_upload(filename: str, *, semicolon_decimal: bool = False) -> SimpleUploadedFile:
        if semicolon_decimal:
            csv_content = "\n".join(
                [
                    "nombre;marca;modelo;serie;fecha_1;costo_1;fecha_2;costo_2",
                    "HORNO TEST CSV DECIMAL;ALPHA;HX-12;SER-003;2026-02-20;1.250,75;;",
                ]
            )
        else:
            csv_content = "\n".join(
                [
                    "nombre,marca,modelo,serie,fecha_1,costo_1,fecha_2,costo_2",
                    "HORNO TEST CSV,ALPHA,HX-11,SER-002,2026-02-20,950.5,,",
                ]
            )
        return SimpleUploadedFile(
            filename,
            csv_content.encode("utf-8"),
            content_type="text/csv",
        )


class ActivoIdentidadTecnicaTests(TestCase):
    """Identidad técnica opcional y token QR permanente del activo."""

    def test_activo_sin_datos_tecnicos_queda_vacio_y_recibe_token(self):
        activo = Activo.objects.create(nombre="Horno histórico sin placa")
        activo.refresh_from_db()
        self.assertTrue(activo.qr_token)
        self.assertEqual(activo.marca, "")
        self.assertEqual(activo.modelo, "")
        self.assertEqual(activo.numero_serie, "")
        self.assertIsNone(activo.proveedor_compra)
        self.assertIsNone(activo.fecha_compra)
        self.assertIsNone(activo.costo_adquisicion)
        self.assertIsNone(activo.garantia_hasta)

    def test_activo_completo_conserva_cada_campo(self):
        from decimal import Decimal as _Decimal

        from maestros.models import Proveedor

        proveedor = Proveedor.objects.create(nombre="Refrigeración del Valle")
        activo = Activo.objects.create(
            nombre="Cámara de refrigeración 01",
            marca="ACME",
            modelo="HX-20",
            numero_serie="SER-0001",
            proveedor_compra=proveedor,
            fecha_compra=timezone.localdate(),
            costo_adquisicion=_Decimal("125000.50"),
            garantia_hasta=timezone.localdate() + timedelta(days=365),
        )
        activo.refresh_from_db()
        self.assertEqual(activo.marca, "ACME")
        self.assertEqual(activo.modelo, "HX-20")
        self.assertEqual(activo.numero_serie, "SER-0001")
        self.assertEqual(activo.proveedor_compra, proveedor)
        self.assertEqual(activo.costo_adquisicion, _Decimal("125000.50"))
        self.assertEqual(proveedor.activos_vendidos.get(), activo)

    def test_token_qr_es_unico_por_activo(self):
        uno = Activo.objects.create(nombre="Batidora 20L")
        dos = Activo.objects.create(nombre="Batidora 40L")
        self.assertNotEqual(uno.qr_token, dos.qr_token)

    def test_token_qr_no_cambia_al_editar_el_activo(self):
        activo = Activo.objects.create(nombre="Abatidor")
        token = activo.qr_token
        activo.nombre = "Abatidor de temperatura"
        activo.ubicacion = "Producción"
        activo.save()
        activo.refresh_from_db()
        self.assertEqual(activo.qr_token, token)

    def test_numeros_de_serie_repetidos_no_destruyen_historico(self):
        uno = Activo.objects.create(nombre="Vitrina Payán", numero_serie="SIN-PLACA")
        dos = Activo.objects.create(nombre="Vitrina Leyva", numero_serie="SIN-PLACA")
        self.assertEqual(Activo.objects.filter(numero_serie="SIN-PLACA").count(), 2)
        self.assertNotEqual(uno.pk, dos.pk)
        self.assertNotEqual(uno.qr_token, dos.qr_token)


class ActivoPasaporteMigracionTests(TransactionTestCase):
    """La migración 0006 debe darle un UUID distinto a cada activo ya existente."""

    available_apps = None

    def setUp(self):
        super().setUp()
        self.addCleanup(self._restaurar_migraciones_actuales)

    def _restaurar_migraciones_actuales(self):
        # Revertir activos también revierte sus dependientes; restaurar todos
        # antes del flush, incluso cuando falle una aserción del backfill.
        executor = MigrationExecutor(connection)
        executor.migrate(executor.loader.graph.leaf_nodes())

    def test_backfill_genera_un_uuid_por_fila_existente(self):
        executor = MigrationExecutor(connection)
        executor.migrate([("activos", "0005_trazabilidad_mantenimiento")])
        executor.loader.build_graph()

        historico = executor.loader.project_state(
            [("activos", "0005_trazabilidad_mantenimiento")]
        ).apps
        ActivoHistorico = historico.get_model("activos", "Activo")
        for indice in range(3):
            ActivoHistorico.objects.create(codigo=f"ACT-MIG-{indice}", nombre=f"Equipo {indice}")

        executor = MigrationExecutor(connection)
        executor.migrate([("activos", "0006_activo_pasaporte_qr")])

        tokens = set(
            Activo.objects.filter(codigo__startswith="ACT-MIG-").values_list("qr_token", flat=True)
        )
        self.assertEqual(len(tokens), 3)
        self.assertNotIn(None, tokens)


class ActivoFichaTecnicaUITests(TestCase):
    """Captura y corrección de la ficha técnica sin inventar información."""

    def setUp(self):
        from maestros.models import Proveedor

        user_model = get_user_model()
        self.admin = user_model.objects.create_user("admin_ficha", "admin_ficha@example.com", "test12345")
        Group.objects.get_or_create(name=ROLE_ADMIN)[0].user_set.add(self.admin)
        self.ventas = user_model.objects.create_user("ventas_ficha", "ventas_ficha@example.com", "test12345")
        Group.objects.get_or_create(name=ROLE_VENTAS)[0].user_set.add(self.ventas)
        self.proveedor = Proveedor.objects.create(nombre="Refrigeración del Valle")
        self.otro_proveedor = Proveedor.objects.create(nombre="Servicios Industriales")
        self.client.force_login(self.admin)

    def test_alta_guarda_la_ficha_tecnica_completa(self):
        self.client.post(
            reverse("activos:activos"),
            {
                "action": "create_activo",
                "nombre": "Horno rotatorio",
                "marca": "ACME",
                "modelo": "HX-20",
                "numero_serie": "SER-777",
                "proveedor_compra_id": str(self.proveedor.id),
                "fecha_compra": "2026-02-01",
                "costo_adquisicion": "125000.50",
                "garantia_hasta": "2027-02-01",
                "activo": "1",
            },
            follow=True,
        )
        activo = Activo.objects.get(nombre="Horno rotatorio")

        self.assertEqual(activo.marca, "ACME")
        self.assertEqual(activo.numero_serie, "SER-777")
        self.assertEqual(activo.proveedor_compra, self.proveedor)
        self.assertEqual(str(activo.fecha_compra), "2026-02-01")
        self.assertEqual(activo.costo_adquisicion, Decimal("125000.50"))

    def test_los_campos_vacios_se_guardan_vacios_y_no_se_inventan(self):
        self.client.post(
            reverse("activos:activos"),
            {"action": "create_activo", "nombre": "Equipo histórico", "activo": "1"},
            follow=True,
        )
        activo = Activo.objects.get(nombre="Equipo histórico")

        self.assertEqual(activo.marca, "")
        self.assertIsNone(activo.proveedor_compra)
        self.assertIsNone(activo.fecha_compra)
        self.assertIsNone(activo.costo_adquisicion)
        self.assertIsNone(activo.garantia_hasta)

    def test_el_proveedor_de_mantenimiento_nunca_se_copia_como_proveedor_de_compra(self):
        self.client.post(
            reverse("activos:activos"),
            {
                "action": "create_activo",
                "nombre": "Cámara fría",
                "proveedor_mantenimiento_id": str(self.otro_proveedor.id),
                "activo": "1",
            },
            follow=True,
        )
        activo = Activo.objects.get(nombre="Cámara fría")

        self.assertEqual(activo.proveedor_mantenimiento, self.otro_proveedor)
        self.assertIsNone(activo.proveedor_compra)

    def test_update_identity_corrige_la_ficha_y_responde_json(self):
        activo = Activo.objects.create(nombre="Batidora")

        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "update_identity",
                "activo_id": str(activo.id),
                "marca": "Hobart",
                "modelo": "H-600",
                "numero_serie": "SER-42",
                "fecha_compra": "2025-05-10",
                "costo_adquisicion": "80000",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        activo.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["marca"], "Hobart")
        self.assertEqual(activo.modelo, "H-600")
        self.assertEqual(activo.costo_adquisicion, Decimal("80000"))

    def test_update_identity_rechaza_costo_negativo_sin_tocar_el_activo(self):
        activo = Activo.objects.create(nombre="Vitrina", marca="ACME")

        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "update_identity",
                "activo_id": str(activo.id),
                "marca": "Corregida",
                "costo_adquisicion": "-5",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        activo.refresh_from_db()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(activo.marca, "ACME")

    def test_update_identity_rechaza_fecha_invalida(self):
        activo = Activo.objects.create(nombre="Congelador")

        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "update_identity",
                "activo_id": str(activo.id),
                "fecha_compra": "01/02/2026",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        activo.refresh_from_db()

        self.assertEqual(response.status_code, 400)
        self.assertIsNone(activo.fecha_compra)

    def test_update_identity_rechaza_proveedor_inexistente(self):
        activo = Activo.objects.create(nombre="Licuadora")

        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "update_identity",
                "activo_id": str(activo.id),
                "proveedor_compra_id": "999999",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        activo.refresh_from_db()

        self.assertEqual(response.status_code, 400)
        self.assertIsNone(activo.proveedor_compra)

    def test_una_serie_repetida_avisa_pero_no_fusiona(self):
        Activo.objects.create(nombre="Vitrina Payán", numero_serie="SIN-PLACA")
        activo = Activo.objects.create(nombre="Vitrina Leyva")

        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "update_identity",
                "activo_id": str(activo.id),
                "numero_serie": "SIN-PLACA",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        activo.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(activo.numero_serie, "SIN-PLACA")
        self.assertIn("SIN-PLACA", response.json()["advertencia"])
        self.assertEqual(Activo.objects.filter(numero_serie="SIN-PLACA").count(), 2)

    def test_sin_permiso_de_gestion_no_se_edita_la_ficha(self):
        activo = Activo.objects.create(nombre="Horno ajeno", marca="ACME")
        self.client.force_login(self.ventas)

        response = self.client.post(
            reverse("activos:activos"),
            {"action": "update_identity", "activo_id": str(activo.id), "marca": "Cambiada"},
        )
        activo.refresh_from_db()

        self.assertEqual(response.status_code, 403)
        self.assertEqual(activo.marca, "ACME")

    def test_la_completitud_se_muestra_sin_bloquear_nada(self):
        Activo.objects.create(nombre="Equipo incompleto")

        response = self.client.get(reverse("activos:activos"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Datos pendientes")

    def test_la_respuesta_async_usa_el_envelope_global_del_erp(self):
        """`static/js/erp_actions.js` exige `ok` y `toast`; sin eso el toast no sale."""
        activo = Activo.objects.create(nombre="Amasadora")

        exito = self.client.post(
            reverse("activos:activos"),
            {"action": "update_identity", "activo_id": str(activo.id), "marca": "Sinmag"},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        ).json()
        fallo = self.client.post(
            reverse("activos:activos"),
            {"action": "update_identity", "activo_id": str(activo.id), "costo_adquisicion": "-1"},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        ).json()

        self.assertTrue(exito["ok"])
        self.assertEqual(exito["toast"]["type"], "success")
        self.assertFalse(fallo["ok"])
        self.assertEqual(fallo["toast"]["type"], "error")


class ActivoDatosEquipoTests(TestCase):
    def setUp(self):
        from core.models import Sucursal
        self.sucursal = Sucursal.objects.create(codigo="P2", nombre="Sucursal P2")
        self.admin = get_user_model().objects.create_user("admin_datos_equipo")
        self.admin.groups.add(Group.objects.get_or_create(name=ROLE_ADMIN)[0])
        self.client.force_login(self.admin)
        self.activo = Activo.objects.create(
            nombre="Batidora P2", categoria="Producción", sucursal=self.sucursal,
            ubicacion="Área de mezclado", marca="Hobart", numero_serie="P2-777",
            costo_adquisicion=Decimal("12345.67"), notas="Original",
        )
        self.url = reverse("activos:activos") + "?q=Batidora&solo_activos=0"
        self.data = {
            "action": "update_description", "activo_id": self.activo.pk,
            "actualizado_en": self.activo.actualizado_en.isoformat(),
            "nombre": "Batidora P2", "categoria": "Producción",
            "criticidad": "MEDIA", "notas": "Original",
        }

    def post(self, **changes):
        return self.client.post(self.url, {**self.data, **changes}, HTTP_X_REQUESTED_WITH="XMLHttpRequest")

    def test_edicion_preserva_todos_los_datos_ajenos_y_audita_solo_cambios(self):
        before = Activo.objects.values().get(pk=self.activo.pk)
        response = self.post(nombre="Batidora corregida", notas="Nota nueva", costo_adquisicion="1", sucursal_id="", codigo="CAMBIO", marca="OTRA")
        self.assertEqual(response.status_code, 200)
        after = Activo.objects.values().get(pk=self.activo.pk)
        for key in before.keys() - {"nombre", "notas", "actualizado_en"}:
            self.assertEqual(after[key], before[key], key)
        audit = AuditLog.objects.get(model="activos.Activo", object_id=str(self.activo.pk))
        self.assertEqual(audit.payload, {"datos_equipo": {
            "nombre": {"antes": "Batidora P2", "despues": "Batidora corregida"},
            "notas": {"antes": "Original", "despues": "Nota nueva"},
        }})
        self.assertTrue(response.json()["ok"])
        self.assertIn('name="actualizado_en"', response.json()["html"])
        self.assertNotIn("12345.67", response.json()["html"])
        self.assertNotIn("costo_adquisicion", response.json())

    def test_campos_invalidos_no_truncan_ni_guardan(self):
        for changes in ({"nombre": ""}, {"nombre": "x" * 181}, {"categoria": "x" * 121}, {"criticidad": "OTRA"}):
            with self.subTest(changes=changes):
                response = self.post(**changes)
                self.assertEqual(response.status_code, 400)
                self.assertFalse(response.json()["ok"])
                self.assertTrue(response.json()["field_errors"])
                self.activo.refresh_from_db()
                self.assertEqual(self.activo.nombre, "Batidora P2")
                self.assertFalse(AuditLog.objects.filter(model="activos.Activo").exists())

    def test_formulario_viejo_no_pisa_edicion_nueva(self):
        self.assertEqual(self.post(nombre="Corrección nueva").status_code, 200)
        response = self.post(nombre="Corrección antigua")
        self.assertEqual(response.status_code, 409)
        self.activo.refresh_from_db()
        self.assertEqual(self.activo.nombre, "Corrección nueva")
        self.assertEqual(AuditLog.objects.filter(model="activos.Activo").count(), 1)

    def test_reenvio_igual_no_actualiza_fecha_ni_audita(self):
        self.assertEqual(self.post(nombre="Corregido").status_code, 200)
        self.activo.refresh_from_db()
        stamp = self.activo.actualizado_en
        self.assertEqual(self.post(nombre="Corregido").status_code, 200)
        self.activo.refresh_from_db()
        self.assertEqual(self.activo.actualizado_en, stamp)
        self.assertEqual(AuditLog.objects.filter(model="activos.Activo").count(), 1)

    def test_auditoria_y_cambio_son_atomicos(self):
        with patch("activos.views.log_event", side_effect=RuntimeError("audit fail")):
            with self.assertRaises(RuntimeError):
                self.post(nombre="No debe persistir")
        self.activo.refresh_from_db()
        self.assertEqual(self.activo.nombre, "Batidora P2")

    @patch("activos.views.can_manage_inventario", return_value=False)
    def test_solo_lectura_no_puede_editar(self, permission):
        self.assertEqual(self.post(nombre="No autorizado").status_code, 403)
        self.assertNotContains(self.client.get(self.url), 'value="update_description"')
        self.activo.refresh_from_db()
        self.assertEqual(self.activo.nombre, "Batidora P2")

    def test_alta_sucursal_canonica_o_null_sin_inferir_ubicacion(self):
        for branch_id in (str(self.sucursal.pk), ""):
            response = self.client.post(self.url, {
                "action": "create_activo", "nombre": "Alta P2", "sucursal_id": branch_id,
                "ubicacion": self.sucursal.nombre, "activo": "1",
            })
            self.assertEqual(response.status_code, 302)
            activo = Activo.objects.latest("pk")
            self.assertEqual(activo.sucursal_id, self.sucursal.pk if branch_id else None)
            self.assertTrue(activo.codigo)
            self.assertTrue(activo.qr_token)
        self.assertEqual(Activo.objects.filter(nombre="Alta P2").count(), 2)

    def test_alta_fk_invalida_no_crea_y_conserva_inputs_en_html(self):
        for branch_id in ("99999999", "texto", "-1", "1.0", "9999999999999999999", "9" * 100):
            with self.subTest(branch_id=branch_id):
                response = self.client.post(self.url, {
                    "action": "create_activo", "nombre": "Mi borrador", "sucursal_id": branch_id,
                    "ubicacion": "Mi área", "notas": "Mis notas", "marca": "Mi marca",
                })
                self.assertEqual(response.status_code, 400)
                self.assertContains(response, 'value="Mi borrador"', status_code=400)
                self.assertContains(response, 'value="Mi marca"', status_code=400)
                self.assertEqual(Activo.objects.count(), 1)

    def test_error_tradicional_conserva_datos_y_filtro(self):
        response = self.client.post(self.url, {**self.data, "nombre": "", "notas": "Borrador retenido"})
        self.assertEqual(response.status_code, 400)
        self.assertContains(response, "Borrador retenido", status_code=400)
        self.assertContains(response, 'aria-invalid="true"', status_code=400)
        self.assertEqual(response.context["filters"]["q"], "Batidora")

    def test_exito_tradicional_vuelve_a_filtro_y_ancla(self):
        response = self.client.post(self.url, {**self.data, "nombre": "Guardado"})
        self.assertRedirects(response, f"{self.url}#activo-{self.activo.pk}", fetch_redirect_response=False)

    def test_enlace_pasaporte_solo_autorizado_y_ubicaciones_separadas(self):
        response = self.client.get(self.url)
        self.assertContains(response, reverse("operacion:activo_pasaporte", args=[self.activo.qr_token]))
        self.assertContains(response, "Sucursal P2")
        self.assertContains(response, "Área/ubicación interna")
        from core.models import UserModuleAccess
        reader = get_user_model().objects.create_user("inventario_sin_pasaporte")
        UserModuleAccess.objects.create(user=reader, module="inventario", access="view")
        self.client.force_login(reader)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, reverse("operacion:activo_pasaporte", args=[self.activo.qr_token]))

    def test_nombres_repetidos_conservan_identidades_independientes(self):
        otro = Activo.objects.create(nombre="Mismo nombre")
        self.assertEqual(self.post(nombre="Mismo nombre").status_code, 200)
        self.activo.refresh_from_db()
        self.assertNotEqual(otro.qr_token, self.activo.qr_token)
        self.assertEqual(Activo.objects.filter(nombre="Mismo nombre").count(), 2)

    def test_id_equipo_fuera_de_rango_no_produce_error_de_base(self):
        for activo_id in ("9999999999999999999", "9" * 100, "texto", "-1"):
            with self.subTest(activo_id=activo_id):
                self.assertEqual(self.post(activo_id=activo_id).status_code, 404)

    def test_nuevo_guardado_renderiza_token_y_estados_vigentes_sin_recargar(self):
        self.activo.estado = Activo.ESTADO_FUERA_SERVICIO
        self.activo.save(update_fields=["estado", "actualizado_en"])
        self.data["actualizado_en"] = self.activo.actualizado_en.isoformat()
        response = self.post(criticidad="ALTA", categoria="")
        self.assertEqual(response.status_code, 200)
        self.activo.refresh_from_db()
        html = response.json()["html"]
        self.assertIn(self.activo.actualizado_en.isoformat(), html)
        self.assertIn("Crítico", html)
        self.assertIn("Sin categoría", html)
        self.assertNotIn("reload", response.json())
        self.data["actualizado_en"] = self.activo.actualizado_en.isoformat()
        self.assertEqual(self.post(criticidad="MEDIA", categoria="Refrigeración").status_code, 200)

    def test_alta_async_reutiliza_envelope_con_contexto_y_error_de_campo(self):
        response = self.client.post(self.url, {
            "action": "create_activo", "nombre": "Borrador", "sucursal_id": "inexistente",
        }, HTTP_X_REQUESTED_WITH="XMLHttpRequest")
        self.assertEqual(response.status_code, 400)
        self.assertIn("Sucursal:", response.json()["toast"]["message"])
        self.assertIn("sucursal_id", response.json()["field_errors"])
        response = self.client.post(self.url, {
            "action": "create_activo", "nombre": "Alta async", "sucursal_id": "", "activo": "1",
        }, HTTP_X_REQUESTED_WITH="XMLHttpRequest")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["redirect"], self.url + "#nuevo-equipo")

    def test_error_html_conserva_version_vieja_para_no_bypasear_conflicto(self):
        version_vieja = self.data["actualizado_en"]
        self.post(nombre="Cambio nuevo")
        response = self.client.post(self.url, {**self.data, "nombre": "Mi borrador"})
        self.assertEqual(response.status_code, 409)
        self.assertContains(response, version_vieja, status_code=409)
        self.assertContains(response, 'value="Mi borrador"', status_code=409)

    def test_token_html_real_puede_guardarse_sin_falso_conflicto_de_zona_horaria(self):
        import re
        response = self.client.get(self.url)
        token = re.search(r'name="actualizado_en" value="([^"]+)"', response.content.decode()).group(1)
        self.assertEqual(self.post(actualizado_en=token, nombre="Guardado desde HTML").status_code, 200)

    def test_token_ausente_rechazado_y_fallback_no_inventa_token_nuevo(self):
        response = self.client.post(self.url, {**self.data, "actualizado_en": "", "nombre": "Borrador"})
        self.assertEqual(response.status_code, 409)
        self.assertContains(response, 'name="actualizado_en" value=""', status_code=409)
        self.activo.refresh_from_db()
        self.assertEqual(self.activo.nombre, "Batidora P2")

    def test_alta_criticidad_invalida_conserva_opcion_e_inputs(self):
        response = self.client.post(self.url, {
            "action": "create_activo", "nombre": "Borrador", "criticidad": "INVALIDA", "notas": "Captura pendiente",
        })
        self.assertEqual(response.status_code, 400)
        self.assertContains(response, '<option value="INVALIDA" selected>INVALIDA</option>', status_code=400, html=True)
        self.assertContains(response, 'value="Captura pendiente"', status_code=400)
        self.assertEqual(Activo.objects.count(), 1)

    def test_foco_local_retorna_tras_exito_sin_mover_scroll_y_no_en_error(self):
        import re
        import shutil
        import subprocess
        from pathlib import Path

        if not shutil.which("node"):
            self.skipTest("Node.js no está disponible en este runtime")
        template = Path(__file__).parent / "templates" / "activos" / "activos.html"
        script = re.search(r"<script>(.*?)</script>", template.read_text(), re.S).group(1)
        harness = r'''
const assert = require('node:assert/strict');
let onSubmit, onMutation, focusCalls = [];
const replacement = {focus: options => focusCalls.push(options)};
const tbody = {addEventListener: (name, callback) => { onSubmit = callback; }};
global.document = {
  querySelector: () => tbody,
  getElementById: id => { assert.equal(id, 'nombre-equipo-7'); return replacement; }
};
global.MutationObserver = class {
  constructor(callback) { onMutation = callback; }
  observe() {}
};
'''
        checks = r'''
function form(action) {
  return {isConnected: true, matches: () => true,
    elements: {action: {value: action}},
    querySelector: () => ({id: 'nombre-equipo-7'})};
}
// Errors leave the original form connected: preserve its focused input.
let original = form('update_description');
onSubmit({target: original}); onMutation(); assert.equal(focusCalls.length, 0);
// Successful replacements restore focus without changing scroll, repeatedly.
for (let i = 0; i < 3; i++) {
  original = form('update_description'); onSubmit({target: original});
  original.isConnected = false; onMutation();
}
assert.equal(focusCalls.length, 3);
assert.deepEqual(focusCalls, Array(3).fill({preventScroll: true}));
// Unrelated mutations and technical actions never steal focus.
onMutation(); original = form('update_identity'); onSubmit({target: original});
original.isConnected = false; onMutation(); assert.equal(focusCalls.length, 3);
'''
        result = subprocess.run(["node", "-e", harness + script + checks], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
