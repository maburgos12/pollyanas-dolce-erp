import json
import os
from datetime import timedelta
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone
from django_celery_beat.models import PeriodicTask

from core.access import can_view_submodule
from core.navigation import build_nav_groups
from rrhh.models import Empleado

from .models import SeguimientoChecklistItem, SeguimientoComentario, SeguimientoEvidencia, SeguimientoItem
from .services import empleado_de_usuario
from .management.commands.importar_agente_dg_seguimiento import Command as ImportarAgenteDGCommand
from .management.commands.importar_agente_dg_seguimiento import _status_agente_a_erp


@override_settings(SECURE_SSL_REDIRECT=False)
class SeguimientoColaboradorTests(TestCase):
    def setUp(self):
        self.group = Group.objects.create(name="PRODUCCION")
        self.user = get_user_model().objects.create_user(
            username="carolina.cayetano",
            email="carolina.cayetano@pollyanasdolce.com",
            first_name="Carolina",
            last_name="Cayetano",
            password="test12345",
        )
        self.user.groups.add(self.group)
        self.empleado = Empleado.objects.create(
            nombre="Carolina Cayetano",
            email="carolina.cayetano@pollyanasdolce.com",
            area="PRODUCCION",
            puesto="Supervisora",
            sucursal="CEDIS",
        )
        self.item = SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_COMPROMISO,
            titulo="Validar inventarios en cuartos fríos",
            descripcion="Revisión de inventario final antes de cierre.",
            entregable_esperado="Diferencias documentadas y enviadas a revisión.",
            responsable_empleado=self.empleado,
            area="PRODUCCION",
            fecha_limite=timezone.now() + timedelta(days=1),
        )
        self.check = SeguimientoChecklistItem.objects.create(
            seguimiento=self.item,
            titulo="Confirmar avance real",
        )
        self.client.force_login(self.user)

    def test_empleado_se_resuelve_por_email_real(self):
        self.assertEqual(empleado_de_usuario(self.user), self.empleado)

    def test_portal_muestra_trabajo_en_dashboard_por_tipo(self):
        response = self.client.get("/seguimiento/")

        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        self.assertIn("Mis minutas, proyectos y compromisos", content)
        self.assertIn("Acuerdos acumulados", content)
        self.assertIn("Por vencer (24h)", content)
        self.assertIn("Compromisos", content)
        self.assertIn("Minutas", content)
        self.assertIn("Proyectos", content)
        self.assertIn("Validar inventarios en cuartos fríos", content)
        self.assertIn("Retroalimentación", content)
        self.assertIn("No visible:", content)
        self.assertIn("información económica, compensación y nómina sensible", content)

    def test_checklist_se_puede_palomear(self):
        response = self.client.post(f"/seguimiento/{self.item.pk}/checklist/{self.check.pk}/")

        self.assertEqual(response.status_code, 302)
        self.check.refresh_from_db()
        self.assertTrue(self.check.completado)
        self.assertEqual(self.check.completado_por, self.user)

    def test_retroalimentacion_coloca_en_revision(self):
        response = self.client.post(
            f"/seguimiento/{self.item.pk}/retroalimentacion/",
            {"comentario": "Inventario revisado, faltan dos diferencias por validar."},
        )

        self.assertEqual(response.status_code, 302)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus, SeguimientoItem.ESTATUS_EN_REVISION)
        self.assertTrue(SeguimientoComentario.objects.filter(seguimiento=self.item, usuario=self.user).exists())

    def test_evidencia_se_adjunta_y_coloca_en_revision(self):
        archivo = SimpleUploadedFile("evidencia.txt", b"foto o documento", content_type="text/plain")

        response = self.client.post(f"/seguimiento/{self.item.pk}/evidencias/", {"archivo": archivo})

        self.assertEqual(response.status_code, 302)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus, SeguimientoItem.ESTATUS_EN_REVISION)
        self.assertTrue(SeguimientoEvidencia.objects.filter(seguimiento=self.item, usuario=self.user).exists())

    def test_colaborador_ve_mis_acuerdos_y_conserva_bonos_operativos_de_su_rol(self):
        groups = build_nav_groups(self.user, "/seguimiento/")
        labels = [item["label"] for group in groups for item in group["items"]]

        self.assertIn("Mis acuerdos", labels)
        self.assertIn("Bonos producción", labels)
        self.assertTrue(can_view_submodule(self.user, "produccion", "bonos"))

    def test_accesos_directos_respetan_bonos_operativos_del_rol(self):
        response_prod = self.client.get("/bp/")
        response_ventas = self.client.get("/bv/")
        api_prod = self.client.get("/api/bonos-produccion/periodos/")
        api_ventas = self.client.get("/api/bonos-ventas/periodos/")

        self.assertEqual(response_prod.status_code, 302)
        self.assertEqual(response_prod["Location"], "/bonos-produccion/app/?captura=1")
        self.assertEqual(response_ventas.status_code, 302)
        self.assertEqual(response_ventas["Location"], "/seguimiento/")
        self.assertEqual(api_prod.status_code, 200)
        self.assertEqual(api_ventas.status_code, 403)

    def test_proyecto_compartido_importado_aparece_a_participante_por_empleado(self):
        ventas = Group.objects.create(name="VENTAS")
        johana = get_user_model().objects.create_user(
            username="johana.lopez",
            email="ventas.johanna@pollyanasdolce.com",
            first_name="Johana",
            last_name="López",
        )
        johana.groups.add(ventas)
        johana_empleado = Empleado.objects.create(
            nombre="LOPEZ PALOS JOHANA ADELIN",
            email="ventas.johanna@pollyanasdolce.com",
            area="ADMINISTRACION",
            activo=True,
        )
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}

        command._upsert_item(
            {
                "id": 6,
                "titulo": "Producto Mes Junio",
                "descripcion": "Lanzamiento de producto del mes",
                "expected_deliverable": "",
                "status": "AT_RISK",
                "target_date": timezone.now() + timedelta(days=5),
                "user_email": johana.email,
                "user_name": johana.get_full_name(),
                "user_id": 3,
                "area_name": "Proyecto",
            },
            "minute_projects",
            SeguimientoItem.TIPO_PROYECTO,
            counters,
            checklist=[{"titulo": "Prueba", "descripcion": "", "completado": False}],
            participants=[
                {
                    "user_id": 5,
                    "user_email": self.user.email,
                    "user_name": self.user.get_full_name(),
                    "role": "STEP_OWNER",
                }
            ],
        )

        item = SeguimientoItem.objects.get(titulo="Producto Mes Junio")
        self.assertEqual(item.responsable_user, johana)
        self.assertEqual(item.responsable_empleado, johana_empleado)
        self.assertIn(self.user, item.participantes_user.all())
        self.assertIn(self.empleado, item.participantes_empleado.all())

        response = self.client.get("/seguimiento/")
        content = response.content.decode()
        self.assertContains(response, "Producto Mes Junio")
        self.assertIn("Compartido", content)

    def test_superusuario_conserva_bonos_en_menu(self):
        admin = get_user_model().objects.create_superuser(username="admin-seguimiento", password="x")

        labels = [item["label"] for group in build_nav_groups(admin, "/dashboard/") for item in group["items"]]

        self.assertIn("Bonos producción", labels)
        self.assertIn("Bonos ventas", labels)

    def test_empleado_se_resuelve_por_tokens_y_area_cuando_rrhh_no_tiene_email(self):
        ventas = Group.objects.create(name="VENTAS")
        user = get_user_model().objects.create_user(
            username="johana.lopez",
            email="ventas.johanna@pollyanasdolce.com",
            first_name="Johana",
            last_name="López",
        )
        user.groups.add(ventas)
        administracion = Empleado.objects.create(
            nombre="LOPEZ PALOS JOHANA ADELIN",
            email="ventas.johanna@pollyanasdolce.com",
            area="ADMINISTRACION",
            activo=True,
        )
        empleado_ventas = Empleado.objects.create(nombre="LOPEZ CASTRO ALEJANDRA JOHANA", area="VENTAS", activo=True)

        self.assertEqual(empleado_de_usuario(user), administracion)
        self.assertNotEqual(empleado_de_usuario(user), empleado_ventas)

    def test_status_de_agente_dg_se_mapea_a_estatus_erp(self):
        self.assertEqual(_status_agente_a_erp("SUBMITTED"), SeguimientoItem.ESTATUS_EN_REVISION)
        self.assertEqual(_status_agente_a_erp("COMPLETED"), SeguimientoItem.ESTATUS_COMPLETADO)
        self.assertEqual(_status_agente_a_erp("AT_RISK"), SeguimientoItem.ESTATUS_BLOQUEADO)
        self.assertEqual(_status_agente_a_erp("OVERDUE"), SeguimientoItem.ESTATUS_EN_PROCESO)

    def test_schedule_de_sync_queda_pausado_si_falta_database_url(self):
        env = {
            "AGENTE_DG_SYNC_DATABASE_URL": "",
            "AGENTE_DG_DATABASE_URL": "",
            "AGENTE_DG_SYNC_ENABLED": "",
        }
        with patch.dict(os.environ, env):
            call_command("setup_seguimiento_schedules", stdout=StringIO())

        task = PeriodicTask.objects.get(name="seguimiento: importar Agente DG")
        self.assertEqual(task.task, "seguimiento.importar_agente_dg")
        self.assertFalse(task.enabled)
        self.assertEqual(json.loads(task.kwargs), {"limit": 0})

    def test_schedule_de_sync_se_activa_si_existe_database_url(self):
        env = {
            "AGENTE_DG_SYNC_DATABASE_URL": "postgres://user:pass@example.com:5432/agente",
            "AGENTE_DG_DATABASE_URL": "",
        }
        with patch.dict(os.environ, env):
            call_command("setup_seguimiento_schedules", "--interval-minutes", "15", "--limit", "25", stdout=StringIO())

        task = PeriodicTask.objects.get(name="seguimiento: importar Agente DG")
        self.assertTrue(task.enabled)
        self.assertEqual(task.interval.every, 15)
        self.assertEqual(task.interval.period, "minutes")
        self.assertEqual(json.loads(task.kwargs), {"limit": 25})
