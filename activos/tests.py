from io import BytesIO

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from openpyxl import Workbook, load_workbook

from core.access import ROLE_ADMIN, ROLE_ALMACEN, ROLE_VENTAS

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
        self.assertTrue(Activo.objects.filter(nombre="Refrigerador Cámara 01").exists())

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
