import hashlib
import hmac
import json
import os
from datetime import datetime, time, timedelta
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone
from django_celery_beat.models import PeriodicTask

from core.access import ROLE_DG, can_view_submodule
from core.models import Notificacion, UserProfile
from core.navigation import build_nav_groups
from rrhh.models import Empleado

from .models import (
    ActividadCalendario,
    SeguimientoChecklistItem,
    SeguimientoComentario,
    SeguimientoEvidencia,
    SeguimientoItem,
    SeguimientoProrrogaSolicitud,
)
from .services import empleado_de_usuario
from .management.commands.importar_agente_dg_seguimiento import Command as ImportarAgenteDGCommand
from .management.commands.importar_agente_dg_seguimiento import MINUTE_QUERY
from .management.commands.importar_agente_dg_seguimiento import _status_agente_a_erp
from .services import upsert_agente_dg_payload


def _agente_dg_signature(payload: dict, secret: str) -> tuple[str, str]:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return body.decode(), f"sha256={signature}"


@override_settings(SECURE_SSL_REDIRECT=False)
class SeguimientoColaboradorTests(TestCase):
    def setUp(self):
        self.group, _ = Group.objects.get_or_create(name="PRODUCCION")
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
        self.assertIn("Resumen general", content)
        self.assertIn("Trabajos acumulados", content)
        self.assertIn("Por vencer (24h)", content)
        self.assertIn("Compromisos", content)
        self.assertIn("Minutas", content)
        self.assertIn("Proyectos", content)
        self.assertIn('data-dashboard-url="/seguimiento/"', content)
        self.assertIn("Validar inventarios en cuartos fríos", content)
        self.assertIn("Retroalimentación", content)
        self.assertIn("Solicitar más tiempo", content)
        self.assertNotIn("Alcance", content)
        self.assertNotIn("Control visible y auditable", content)
        self.assertNotIn("Visible:", content)
        self.assertNotIn("No visible:", content)
        self.assertNotIn("Auditable:", content)
        self.assertNotIn("información económica, compensación y nómina sensible", content)

    def test_checklist_se_puede_palomear(self):
        response = self.client.post(f"/seguimiento/{self.item.pk}/checklist/{self.check.pk}/")

        self.assertEqual(response.status_code, 302)
        self.check.refresh_from_db()
        self.assertTrue(self.check.completado)
        self.assertEqual(self.check.completado_por, self.user)

    def test_paso_agente_dg_no_acepta_toggle_local(self):
        self.item.tipo = SeguimientoItem.TIPO_PROYECTO
        self.item.origen = "Agente DG"
        self.item.metadata = {"source": "agente_dg", "source_table": "minute_projects", "source_id": 9}
        self.item.save(update_fields=["tipo", "origen", "metadata"])
        self.check.origen_step_id = 14
        self.check.estatus_origen = "READY"
        self.check.save(update_fields=["origen_step_id", "estatus_origen"])

        response = self.client.post(f"/seguimiento/{self.item.pk}/checklist/{self.check.pk}/")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"/seguimiento/{self.item.pk}/")
        self.check.refresh_from_db()
        self.assertFalse(self.check.completado)
        self.assertFalse(Notificacion.objects.filter(objeto_id=str(self.item.pk), tipo=Notificacion.TIPO_SEGUIMIENTO).exists())

    def test_mi_trabajo_no_expone_toggle_local_para_paso_agente_dg(self):
        self.item.tipo = SeguimientoItem.TIPO_PROYECTO
        self.item.origen = "Agente DG"
        self.item.metadata = {"source": "agente_dg", "source_table": "minute_projects", "source_id": 9}
        self.item.save(update_fields=["tipo", "origen", "metadata"])
        self.check.origen_step_id = 14
        self.check.estatus_origen = "READY"
        self.check.save(update_fields=["origen_step_id", "estatus_origen"])

        response = self.client.get("/seguimiento/")
        content = response.content.decode()

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(f'action="/seguimiento/{self.item.pk}/checklist/{self.check.pk}/"', content)
        self.assertIn(f'href="/seguimiento/{self.item.pk}/"', content)

    def test_usuario_ajeno_no_ve_ni_actualiza_acuerdo(self):
        otro = get_user_model().objects.create_user(username="usuario.ajeno", password="test12345")
        self.client.force_login(otro)

        response = self.client.get("/seguimiento/")
        self.assertNotContains(response, "Validar inventarios en cuartos fríos")

        toggle_response = self.client.post(f"/seguimiento/{self.item.pk}/checklist/{self.check.pk}/")
        feedback_response = self.client.post(
            f"/seguimiento/{self.item.pk}/retroalimentacion/",
            {"comentario": "Intento ajeno"},
        )
        archivo = SimpleUploadedFile("evidencia.txt", b"foto o documento", content_type="text/plain")
        evidencia_response = self.client.post(f"/seguimiento/{self.item.pk}/evidencias/", {"archivo": archivo})

        self.assertEqual(toggle_response.status_code, 404)
        self.assertEqual(feedback_response.status_code, 404)
        self.assertEqual(evidencia_response.status_code, 404)
        self.check.refresh_from_db()
        self.assertFalse(self.check.completado)
        self.assertFalse(SeguimientoComentario.objects.filter(seguimiento=self.item, comentario="Intento ajeno").exists())
        self.assertFalse(SeguimientoEvidencia.objects.filter(seguimiento=self.item, usuario=otro).exists())

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

    def test_evidencia_rechaza_archivo_activo(self):
        archivo = SimpleUploadedFile("evidencia.html", b"<script>alert(1)</script>", content_type="text/html")

        response = self.client.post(f"/seguimiento/{self.item.pk}/evidencias/", {"archivo": archivo})

        self.assertEqual(response.status_code, 302)
        self.assertFalse(SeguimientoEvidencia.objects.filter(seguimiento=self.item, usuario=self.user).exists())

    @override_settings(SEGUIMIENTO_EVIDENCIA_MAX_UPLOAD_BYTES=4)
    def test_evidencia_rechaza_archivo_mayor_al_limite(self):
        archivo = SimpleUploadedFile("evidencia.txt", b"12345", content_type="text/plain")

        response = self.client.post(f"/seguimiento/{self.item.pk}/evidencias/", {"archivo": archivo})

        self.assertEqual(response.status_code, 302)
        self.assertFalse(SeguimientoEvidencia.objects.filter(seguimiento=self.item, usuario=self.user).exists())

    def test_evidencia_acepta_documentos_operativos_e_imagenes(self):
        casos = [
            ("soporte.docx", b"word", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            ("reporte.xlsm", b"excel", "application/vnd.ms-excel.sheet.macroEnabled.12"),
            ("foto.gif", b"GIF89a", "image/gif"),
        ]

        for nombre, contenido, content_type in casos:
            with self.subTest(nombre=nombre):
                archivo = SimpleUploadedFile(nombre, contenido, content_type=content_type)
                response = self.client.post(f"/seguimiento/{self.item.pk}/evidencias/", {"archivo": archivo})

                self.assertEqual(response.status_code, 302)
                self.assertTrue(SeguimientoEvidencia.objects.filter(seguimiento=self.item, nombre_original=nombre).exists())

    def test_usuario_solicita_prorroga_sin_evidencia(self):
        fecha_solicitada = (timezone.localdate() + timedelta(days=5)).isoformat()

        response = self.client.post(
            f"/seguimiento/{self.item.pk}/prorroga/",
            {"fecha_solicitada": fecha_solicitada, "motivo": "Necesito cierre de inventario adicional."},
        )

        self.assertEqual(response.status_code, 302)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus, SeguimientoItem.ESTATUS_EN_REVISION)
        solicitud = SeguimientoProrrogaSolicitud.objects.get(seguimiento=self.item)
        self.assertEqual(solicitud.usuario, self.user)
        self.assertEqual(solicitud.fecha_solicitada.isoformat(), fecha_solicitada)
        self.assertEqual(solicitud.motivo, "Necesito cierre de inventario adicional.")
        self.assertEqual(solicitud.estatus, SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE)

    def test_usuario_ajeno_no_solicita_prorroga(self):
        otro = get_user_model().objects.create_user(username="usuario.ajeno.prorroga", password="test12345")
        self.client.force_login(otro)

        response = self.client.post(
            f"/seguimiento/{self.item.pk}/prorroga/",
            {"fecha_solicitada": (timezone.localdate() + timedelta(days=5)).isoformat(), "motivo": "Intento ajeno"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertFalse(SeguimientoProrrogaSolicitud.objects.filter(seguimiento=self.item).exists())

    def test_colaborador_ve_mis_acuerdos_y_conserva_bonos_operativos_de_su_rol(self):
        groups = build_nav_groups(self.user, "/seguimiento/")
        labels = [item["label"] for group in groups for item in group["items"]]

        self.assertIn("Minutas", labels)
        self.assertIn("Proyectos", labels)
        self.assertIn("Compromisos", labels)
        self.assertNotIn("Mis acuerdos", labels)
        self.assertIn("Bonos producción", labels)
        self.assertTrue(can_view_submodule(self.user, "produccion", "bonos"))

    def test_colaborador_staff_no_hereda_panel_dg_de_seguimiento(self):
        self.user.is_staff = True
        self.user.save(update_fields=["is_staff"])
        self.item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
        self.item.save(update_fields=["estatus"])

        groups = build_nav_groups(self.user, "/seguimiento/")
        labels = [item["label"] for group in groups for item in group["items"]]
        response_mi_trabajo = self.client.get("/seguimiento/")
        response_panel = self.client.get("/seguimiento/panel/")
        response_revision = self.client.get("/seguimiento/revision/")
        response_detalle_dg = self.client.get(f"/seguimiento/panel/{self.item.pk}/")
        response_resolver = self.client.post(
            f"/seguimiento/{self.item.pk}/resolver/",
            {"accion": "aprobar"},
        )

        self.assertNotIn("Panel de acuerdos", labels)
        self.assertNotContains(response_mi_trabajo, "Bandeja DG")
        self.assertEqual(response_panel.status_code, 302)
        self.assertEqual(response_panel["Location"], "/seguimiento/")
        self.assertEqual(response_revision.status_code, 302)
        self.assertEqual(response_revision["Location"], "/seguimiento/")
        self.assertEqual(response_detalle_dg.status_code, 302)
        self.assertEqual(response_detalle_dg["Location"], "/seguimiento/")
        self.assertEqual(response_resolver.status_code, 302)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus, SeguimientoItem.ESTATUS_EN_REVISION)

    def test_colaborador_staff_no_aterriza_en_dashboard_dg(self):
        self.user.is_staff = True
        self.user.save(update_fields=["is_staff"])

        response_dashboard = self.client.get("/dashboard/")
        self.client.logout()
        response_login = self.client.post(
            "/login/",
            {"username": self.user.username, "password": "test12345"},
        )

        self.assertEqual(response_dashboard.status_code, 302)
        self.assertEqual(response_dashboard["Location"], "/seguimiento/")
        self.assertEqual(response_login.status_code, 302)
        self.assertEqual(response_login["Location"], "/seguimiento/")

    def test_dg_real_conserva_panel_y_resuelve_revision(self):
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.dg", password="test12345")
        dg_user.groups.add(dg_group)
        self.item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
        self.item.save(update_fields=["estatus"])
        self.client.force_login(dg_user)

        response_panel = self.client.get("/seguimiento/panel/")
        response_detalle_dg = self.client.get(f"/seguimiento/panel/{self.item.pk}/")
        response_resolver = self.client.post(
            f"/seguimiento/{self.item.pk}/resolver/",
            {"accion": "aprobar", "next": "panel"},
        )

        self.assertContains(response_panel, "Panel de acuerdos")
        self.assertContains(response_detalle_dg, self.item.titulo)
        self.assertEqual(response_resolver.status_code, 302)
        self.assertEqual(response_resolver["Location"], "/seguimiento/panel/")
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus, SeguimientoItem.ESTATUS_COMPLETADO)

    def test_dg_cierra_minuta_agente_dg_con_writeback(self):
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.writeback", password="test12345")
        dg_user.groups.add(dg_group)
        self.item.tipo = SeguimientoItem.TIPO_MINUTA
        self.item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
        self.item.requiere_aprobacion = True
        self.item.origen = "Agente DG"
        self.item.referencia_externa = "minute_agreements:77"
        self.item.metadata = {"source": "agente_dg", "source_table": "minute_agreements", "source_id": 77}
        self.item.save(update_fields=["tipo", "estatus", "requiere_aprobacion", "origen", "referencia_externa", "metadata"])
        self.client.force_login(dg_user)

        with patch.dict(os.environ, {"AGENTE_DG_WRITEBACK_ENABLED": "true"}), \
            patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
            patch("seguimiento.agente_dg_client.patch_minute_agreement", return_value={"id": 77}) as patch_minute:
            response = self.client.post(
                f"/seguimiento/{self.item.pk}/resolver/",
                {"accion": "aprobar", "comentario": "Cierre correcto", "next": "panel"},
            )

        self.assertEqual(response.status_code, 302)
        patch_minute.assert_called_once_with(77, status="COMPLETED", completion_note="Cierre correcto")
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus, SeguimientoItem.ESTATUS_COMPLETADO)

    def test_panel_dg_no_inventa_porcentaje_sin_checklist_y_marca_desfase(self):
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.panel", password="test12345")
        dg_user.groups.add(dg_group)
        self.item.tipo = SeguimientoItem.TIPO_MINUTA
        self.item.estatus = SeguimientoItem.ESTATUS_PENDIENTE
        self.item.origen = "Agente DG"
        self.item.aprobado_at = timezone.now()
        self.item.metadata = {
            "source": "agente_dg",
            "source_table": "minute_agreements",
            "source_id": 77,
            "source_status": "OPEN",
        }
        self.item.save(update_fields=["tipo", "estatus", "origen", "aprobado_at", "metadata"])
        self.item.checklist.all().delete()
        self.client.force_login(dg_user)

        response = self.client.get("/seguimiento/panel/?bucket=desfases&tipo=MINUTA&vista=tabla&tab=MINUTA")

        self.assertContains(response, "Sin checklist")
        self.assertContains(response, "Desfase app")
        self.assertNotContains(response, ">10%</span>")

    def test_detalle_no_muestra_cerrado_si_fuente_sigue_abierta(self):
        self.item.tipo = SeguimientoItem.TIPO_MINUTA
        self.item.estatus = SeguimientoItem.ESTATUS_PENDIENTE
        self.item.origen = "Agente DG"
        self.item.aprobado_at = timezone.now()
        self.item.metadata = {
            "source": "agente_dg",
            "source_table": "minute_agreements",
            "source_id": 77,
            "source_status": "OPEN",
        }
        self.item.save(update_fields=["tipo", "estatus", "origen", "aprobado_at", "metadata"])

        response = self.client.get(f"/seguimiento/{self.item.pk}/")

        self.assertContains(response, "Aprobación local anterior pendiente de sincronizar")
        self.assertNotContains(response, "Cerrado el")

    def test_dg_crea_minuta_en_app_y_se_refleja_en_erp(self):
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.crea", password="test12345")
        dg_user.groups.add(dg_group)
        self.client.force_login(dg_user)
        created_payload = {
            "id": 918,
            "collaborator_user_id": 45,
            "title": "Nuevo acuerdo desde ERP",
            "agreement_text": "Detalle operativo",
            "checklist_items": [
                {"id": "a", "text": "Primer paso", "completed": False, "completed_at": None},
                {"id": "b", "text": "Segundo paso", "completed": False, "completed_at": None},
            ],
            "due_at": (timezone.now() + timedelta(days=3)).isoformat(),
            "status": "OPEN",
            "meeting_label": "ERP Seguimiento",
        }

        with patch.dict(os.environ, {"AGENTE_DG_WRITEBACK_ENABLED": "true"}), \
            patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
            patch("seguimiento.agente_dg_client.get_users", return_value=[{"id": 45, "email": self.user.email}]), \
            patch("seguimiento.agente_dg_client.create_minute_agreement", return_value=created_payload) as create_minute:
            response = self.client.post(
                "/seguimiento/panel/crear-agente-dg/",
                {
                    "responsable_user_id": self.user.pk,
                    "titulo": "Nuevo acuerdo desde ERP",
                    "descripcion": "Detalle operativo",
                    "fecha_limite": (timezone.now() + timedelta(days=3)).date().isoformat(),
                    "hora_limite": "18:00",
                    "checklist": "Primer paso\nSegundo paso",
                },
            )

        self.assertEqual(response.status_code, 302)
        create_minute.assert_called_once()
        payload = create_minute.call_args.kwargs
        self.assertEqual(payload["collaborator_user_id"], 45)
        self.assertEqual(payload["checklist_items"][0]["text"], "Primer paso")
        item = SeguimientoItem.objects.get(metadata__source="agente_dg", metadata__source_id=918)
        self.assertEqual(item.titulo, "Nuevo acuerdo desde ERP")
        self.assertEqual(item.checklist.count(), 2)

    def test_dg_no_cierra_local_si_writeback_agente_dg_falla(self):
        from .agente_dg_client import AgenteDGError

        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.writeback.fail", password="test12345")
        dg_user.groups.add(dg_group)
        self.item.tipo = SeguimientoItem.TIPO_MINUTA
        self.item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
        self.item.requiere_aprobacion = True
        self.item.origen = "Agente DG"
        self.item.metadata = {"source": "agente_dg", "source_table": "minute_agreements", "source_id": 77}
        self.item.save(update_fields=["tipo", "estatus", "requiere_aprobacion", "origen", "metadata"])
        self.client.force_login(dg_user)

        with patch.dict(os.environ, {"AGENTE_DG_WRITEBACK_ENABLED": "true"}), \
            patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
            patch("seguimiento.agente_dg_client.patch_minute_agreement", side_effect=AgenteDGError("sin API")):
            response = self.client.post(
                f"/seguimiento/{self.item.pk}/resolver/",
                {"accion": "aprobar", "comentario": "Cierre correcto", "next": "panel"},
            )

        self.assertEqual(response.status_code, 302)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus, SeguimientoItem.ESTATUS_EN_REVISION)
        self.assertFalse(
            SeguimientoComentario.objects.filter(
                seguimiento=self.item,
                tipo=SeguimientoComentario.TIPO_REVISION_DG,
                comentario__icontains="[APROBADO]",
            ).exists()
        )

    def test_colaborador_ve_conversacion_y_cierre_sin_enviar_feedback(self):
        self.item.estatus = SeguimientoItem.ESTATUS_COMPLETADO
        self.item.aprobado_at = timezone.now()
        self.item.save(update_fields=["estatus", "aprobado_at"])
        SeguimientoComentario.objects.create(
            seguimiento=self.item,
            usuario=self.user,
            tipo=SeguimientoComentario.TIPO_REVISION_DG,
            comentario="[APROBADO] Retroalimentación visible para cierre.",
        )

        response = self.client.get(f"/seguimiento/{self.item.pk}/")

        self.assertContains(response, "Conversación y cierre")
        self.assertContains(response, "Cierre establecido")
        self.assertContains(response, "Retroalimentación visible para cierre.")

    def test_dg_real_aterriza_en_dashboard_dg(self):
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.dashboard", password="test12345")
        dg_user.groups.add(dg_group)
        self.client.logout()

        response = self.client.post(
            "/login/",
            {"username": dg_user.username, "password": "test12345"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/dashboard/")

    def test_colaborador_staff_aprueba_paso_designado_desde_mi_trabajo(self):
        ventas, _ = Group.objects.get_or_create(name="VENTAS")
        johana = get_user_model().objects.create_user(
            username="johana.lopez",
            email="ventas.johanna@pollyanasdolce.com",
            first_name="Johana",
            last_name="López",
            password="test12345",
            is_staff=True,
        )
        johana.groups.add(ventas)
        proyecto = SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_PROYECTO,
            titulo="Producto Julio",
            responsable_user=self.user,
            area="VENTAS",
            estatus=SeguimientoItem.ESTATUS_EN_PROCESO,
        )
        paso = SeguimientoChecklistItem.objects.create(
            seguimiento=proyecto,
            titulo="Validar arte final",
            requiere_aprobacion=True,
            aprobador_user=johana,
            aprobador_nombre="Johana López",
            estatus_origen="SUBMITTED",
        )
        self.client.force_login(johana)

        with patch.dict(os.environ, {"AGENTE_DG_WRITEBACK_ENABLED": ""}):
            response = self.client.get("/seguimiento/")
            post_response = self.client.post(
                f"/seguimiento/{proyecto.pk}/paso/{paso.pk}/aprobar/",
                {"accion": "aprobar"},
            )

        self.assertContains(response, "Pasos que esperan tu aprobación")
        self.assertContains(response, "Validar arte final")
        self.assertNotContains(response, "Bandeja DG")
        self.assertEqual(post_response.status_code, 302)
        paso.refresh_from_db()
        self.assertTrue(paso.completado)
        self.assertEqual(paso.estatus_origen, "COMPLETED")
        self.assertEqual(paso.completado_por, johana)

    def test_colaborador_staff_devuelve_paso_con_motivo(self):
        ventas, _ = Group.objects.get_or_create(name="VENTAS")
        johana = get_user_model().objects.create_user(
            username="johana.devolver",
            first_name="Johana",
            last_name="López",
            password="test12345",
            is_staff=True,
        )
        johana.groups.add(ventas)
        proyecto = SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_PROYECTO,
            titulo="Producto Julio",
            responsable_user=self.user,
            area="VENTAS",
            estatus=SeguimientoItem.ESTATUS_EN_PROCESO,
        )
        paso = SeguimientoChecklistItem.objects.create(
            seguimiento=proyecto,
            titulo="Validar presupuesto",
            requiere_aprobacion=True,
            aprobador_user=johana,
            aprobador_nombre="Johana López",
            estatus_origen="SUBMITTED",
        )
        self.client.force_login(johana)

        with patch.dict(os.environ, {"AGENTE_DG_WRITEBACK_ENABLED": ""}):
            response = self.client.post(
                f"/seguimiento/{proyecto.pk}/paso/{paso.pk}/aprobar/",
                {"accion": "devolver", "motivo": "Falta evidencia de costos."},
            )

        self.assertEqual(response.status_code, 302)
        paso.refresh_from_db()
        self.assertFalse(paso.completado)
        self.assertEqual(paso.estatus_origen, "IN_PROGRESS")
        self.assertTrue(
            SeguimientoComentario.objects.filter(
                seguimiento=proyecto,
                usuario=johana,
                comentario__icontains="Falta evidencia de costos.",
            ).exists()
        )

    def test_mi_trabajo_filtra_por_pestana_de_tipo(self):
        SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_MINUTA,
            titulo="Acuerdo de junta semanal",
            responsable_empleado=self.empleado,
            area="PRODUCCION",
        )
        SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_PROYECTO,
            titulo="Proyecto producto mes",
            responsable_empleado=self.empleado,
            area="PRODUCCION",
        )

        response = self.client.get("/seguimiento/proyectos/")
        content = response.content.decode()

        self.assertContains(response, "Proyectos")
        self.assertContains(response, "Proyecto producto mes")
        self.assertNotContains(response, "Acuerdo de junta semanal")
        self.assertNotContains(response, "Validar inventarios en cuartos fríos")
        self.assertIn('href="/seguimiento/minutas/"', content)
        self.assertIn('href="/seguimiento/proyectos/" class="module-tab active"', content)
        self.assertIn('href="/seguimiento/compromisos/"', content)

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
        ventas, _ = Group.objects.get_or_create(name="VENTAS")
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

    def test_importador_elimina_checks_que_desaparecen_de_agente_dg(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        row = {
            "id": 9,
            "titulo": "Proyecto con pasos variables",
            "descripcion": "Validar limpieza de pasos",
            "expected_deliverable": "",
            "status": "IN_PROGRESS",
            "target_date": timezone.now() + timedelta(days=5),
            "user_email": self.user.email,
            "user_name": self.user.get_full_name(),
            "user_id": 4,
            "area_name": "Proyecto",
        }

        command._upsert_item(
            row,
            "minute_projects",
            SeguimientoItem.TIPO_PROYECTO,
            counters,
            checklist=[
                {"titulo": "Paso vigente", "descripcion": "", "completado": True},
                {"titulo": "Paso removido", "descripcion": "", "completado": False},
            ],
        )
        item = SeguimientoItem.objects.get(titulo="Proyecto con pasos variables")
        check = item.checklist.get(orden=1)
        check.completado_por = self.user
        check.completado_at = timezone.now()
        check.save(update_fields=["completado_por", "completado_at"])

        command._upsert_item(
            row,
            "minute_projects",
            SeguimientoItem.TIPO_PROYECTO,
            counters,
            checklist=[
                {"titulo": "Paso vigente actualizado", "descripcion": "", "completado": False},
            ],
        )

        checks = list(item.checklist.order_by("orden"))
        self.assertEqual(len(checks), 1)
        self.assertEqual(checks[0].titulo, "Paso vigente actualizado")
        self.assertFalse(checks[0].completado)
        self.assertIsNone(checks[0].completado_por)
        self.assertIsNone(checks[0].completado_at)

    def test_sync_no_desmarca_mismo_check_local_si_agente_dg_llega_atrasado(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        row = {
            "id": 11,
            "titulo": "Proyecto con avance local",
            "descripcion": "Validar avance local",
            "expected_deliverable": "",
            "status": "IN_PROGRESS",
            "target_date": timezone.now() + timedelta(days=5),
            "user_email": self.user.email,
            "user_name": self.user.get_full_name(),
            "user_id": 4,
            "area_name": "Proyecto",
        }
        payload = [{"titulo": "Paso vigente", "descripcion": "", "completado": False}]
        command._upsert_item(row, "minute_projects", SeguimientoItem.TIPO_PROYECTO, counters, checklist=payload)
        item = SeguimientoItem.objects.get(titulo="Proyecto con avance local")
        check = item.checklist.get()
        check.completado = True
        check.completado_por = self.user
        check.completado_at = timezone.now()
        check.save(update_fields=["completado", "completado_por", "completado_at"])

        command._upsert_item(row, "minute_projects", SeguimientoItem.TIPO_PROYECTO, counters, checklist=payload)

        check.refresh_from_db()
        self.assertTrue(check.completado)
        self.assertEqual(check.completado_por, self.user)
        self.assertIsNotNone(check.completado_at)

    def test_sync_preserva_subpuntos_locales_si_agente_dg_llega_atrasado(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        row = {
            "id": 12,
            "titulo": "Proyecto con subpuntos",
            "descripcion": "Validar avance local",
            "expected_deliverable": "",
            "status": "IN_PROGRESS",
            "target_date": timezone.now() + timedelta(days=5),
            "user_email": self.user.email,
            "user_name": self.user.get_full_name(),
            "user_id": 4,
            "area_name": "Proyecto",
        }
        checklist_json = json.dumps([
            {"text": "Primer subpunto", "completed": False},
            {"text": "Segundo subpunto", "completed": False},
        ])
        payload = [
            {
                "titulo": "Paso con checklist",
                "origen_step_id": 201,
                "descripcion": "",
                "completado": False,
                "checklist_items_json": checklist_json,
            }
        ]
        command._upsert_item(row, "minute_projects", SeguimientoItem.TIPO_PROYECTO, counters, checklist=payload)
        item = SeguimientoItem.objects.get(titulo="Proyecto con subpuntos")
        check = item.checklist.get(origen_step_id=201)
        check.sub_checklist[0]["completado"] = True
        check.save(update_fields=["sub_checklist"])

        command._upsert_item(row, "minute_projects", SeguimientoItem.TIPO_PROYECTO, counters, checklist=payload)

        check.refresh_from_db()
        self.assertTrue(check.sub_checklist[0]["completado"])
        self.assertFalse(check.sub_checklist[1]["completado"])

    def test_webhook_parcial_no_borra_participantes_existentes(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        row = {
            "id": 13,
            "titulo": "Proyecto compartido parcial",
            "descripcion": "Validar participantes",
            "expected_deliverable": "",
            "status": "IN_PROGRESS",
            "target_date": timezone.now() + timedelta(days=5),
            "user_email": "otra@pollyanasdolce.com",
            "user_name": "Otra Persona",
            "user_id": 4,
            "area_name": "Proyecto",
        }
        command._upsert_item(
            row,
            "minute_projects",
            SeguimientoItem.TIPO_PROYECTO,
            counters,
            checklist=[{"titulo": "Paso", "descripcion": "", "completado": False}],
            participants=[
                {
                    "user_id": 5,
                    "user_email": self.user.email,
                    "user_name": self.user.get_full_name(),
                    "role": "STEP_OWNER",
                }
            ],
        )
        item = SeguimientoItem.objects.get(titulo="Proyecto compartido parcial")

        upsert_agente_dg_payload({
            "source_table": "minute_projects",
            "source_id": 13,
            "record": {**row, "descripcion": "Actualizacion parcial"},
        })

        item.refresh_from_db()
        self.assertIn(self.user, item.participantes_user.all())
        self.assertIn(self.empleado, item.participantes_empleado.all())

    def test_importador_preserva_identidad_de_pasos_por_origen_step_id_al_reordenar(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        row = {
            "id": 10,
            "titulo": "Proyecto reordenado",
            "descripcion": "Validar reorden de pasos",
            "expected_deliverable": "",
            "status": "IN_PROGRESS",
            "target_date": timezone.now() + timedelta(days=5),
            "user_email": self.user.email,
            "user_name": self.user.get_full_name(),
            "user_id": 4,
            "area_name": "Proyecto",
        }
        command._upsert_item(
            row,
            "minute_projects",
            SeguimientoItem.TIPO_PROYECTO,
            counters,
            checklist=[
                {"titulo": "Paso uno", "origen_step_id": 101, "descripcion": "", "completado": False},
                {"titulo": "Paso dos", "origen_step_id": 102, "descripcion": "", "completado": False},
            ],
        )
        item = SeguimientoItem.objects.get(titulo="Proyecto reordenado")
        pk_by_step = {check.origen_step_id: check.pk for check in item.checklist.all()}

        command._upsert_item(
            row,
            "minute_projects",
            SeguimientoItem.TIPO_PROYECTO,
            counters,
            checklist=[
                {"titulo": "Paso dos actualizado", "origen_step_id": 102, "descripcion": "", "completado": False},
                {"titulo": "Paso uno actualizado", "origen_step_id": 101, "descripcion": "", "completado": True},
            ],
        )

        checks = list(item.checklist.order_by("orden"))
        self.assertEqual(len(checks), 2)
        self.assertEqual(checks[0].origen_step_id, 102)
        self.assertEqual(checks[0].pk, pk_by_step[102])
        self.assertEqual(checks[0].titulo, "Paso dos actualizado")
        self.assertEqual(checks[1].origen_step_id, 101)
        self.assertEqual(checks[1].pk, pk_by_step[101])
        self.assertEqual(checks[1].titulo, "Paso uno actualizado")
        self.assertTrue(checks[1].completado)

    def test_importador_trunca_titulo_de_checklist_largo_y_preserva_texto(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        texto_largo = (
            "TURBOLINO CRUCERO: Sensor de flama ya esta muy tostado, es por eso que el horno "
            "batalla para encender, se esta intentando conseguir para ponerle justo el que lleva, "
            "de no encontrarse se le puede hacer una adaptacion con otro sensor encontrado."
        )

        command._upsert_item(
            {
                "id": 78,
                "titulo": "Refacciones de hornos",
                "descripcion": "Validar refacciones",
                "expected_deliverable": "",
                "status": "OPEN",
                "due_at": timezone.now() + timedelta(days=5),
                "user_email": self.user.email,
                "user_name": self.user.get_full_name(),
                "user_id": 4,
                "area_name": "Junta",
            },
            "minute_agreements",
            SeguimientoItem.TIPO_MINUTA,
            counters,
            checklist=[{"titulo": texto_largo, "descripcion": "", "completado": False}],
        )

        item = SeguimientoItem.objects.get(titulo="Refacciones de hornos")
        check = item.checklist.get()
        self.assertLessEqual(len(check.titulo), 220)
        self.assertTrue(check.titulo.endswith("..."))
        self.assertEqual(check.descripcion, texto_largo)

    def test_minute_query_incluye_archivadas_cerradas(self):
        self.assertIn("m.archived_at IS NULL", MINUTE_QUERY)
        self.assertIn("m.archived_at", MINUTE_QUERY)
        self.assertIn("COMPLETED", MINUTE_QUERY)
        self.assertIn("CLOSED", MINUTE_QUERY)
        self.assertIn("CANCELLED", MINUTE_QUERY)

    def test_importador_minuta_archivada_completed_entra_como_historial(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        archived_at = timezone.now() - timedelta(days=12)

        command._upsert_item(
            {
                "id": 125,
                "titulo": "Cotización cerrada histórica",
                "descripcion": "Minuta finalizada en la app.",
                "expected_deliverable": "",
                "status": "COMPLETED",
                "archived_at": archived_at,
                "due_at": timezone.now() - timedelta(days=30),
                "user_email": self.user.email,
                "user_name": self.user.get_full_name(),
                "user_id": 4,
                "area_name": "Junta",
            },
            "minute_agreements",
            SeguimientoItem.TIPO_MINUTA,
            counters,
            checklist=[],
        )

        item = SeguimientoItem.objects.get(metadata__source_table="minute_agreements", metadata__source_id=125)
        self.assertEqual(item.estatus, SeguimientoItem.ESTATUS_COMPLETADO)
        self.assertFalse(item.esta_vencido)
        self.assertEqual(item.aprobado_at, archived_at)
        self.assertEqual(item.metadata["source_status"], "COMPLETED")
        self.assertIn("source_archived_at", item.metadata)

    def test_panel_dg_manda_archivada_completed_a_historico_sin_vencido(self):
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.historico", password="test12345")
        dg_user.groups.add(dg_group)
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        command._upsert_item(
            {
                "id": 126,
                "titulo": "Minuta archivada sin retraso operativo",
                "descripcion": "Histórico",
                "status": "COMPLETED",
                "archived_at": timezone.now() - timedelta(days=3),
                "due_at": timezone.now() - timedelta(days=10),
                "user_email": self.user.email,
                "user_name": self.user.get_full_name(),
                "user_id": 4,
                "area_name": "Junta",
            },
            "minute_agreements",
            SeguimientoItem.TIPO_MINUTA,
            counters,
            checklist=[],
        )
        self.client.force_login(dg_user)

        activos = self.client.get("/seguimiento/panel/?bucket=activos&tab=MINUTA&vista=tabla")
        historico = self.client.get("/seguimiento/panel/?bucket=historico&tab=MINUTA&vista=tabla")

        self.assertNotContains(activos, "Minuta archivada sin retraso operativo")
        self.assertContains(historico, "Minuta archivada sin retraso operativo")
        self.assertEqual(historico.context["vencidos"], 0)
        self.assertEqual(historico.context["completados"], 1)
        self.assertNotContains(historico, "Desfase app")

    def test_importador_open_limpia_aprobado_at_y_panel_lo_mantiene_activo(self):
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.desfase", password="test12345")
        dg_user.groups.add(dg_group)
        aprobado_at = timezone.now() - timedelta(days=2)
        item = SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_MINUTA,
            titulo="Búsqueda de camisas",
            responsable_user=self.user,
            area="Junta",
            fecha_limite=timezone.now() - timedelta(days=1),
            estatus=SeguimientoItem.ESTATUS_COMPLETADO,
            aprobado_at=aprobado_at,
            origen="Agente DG",
            referencia_externa="minute_agreements:73",
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 73, "source_status": "COMPLETED"},
        )
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}

        command._upsert_item(
            {
                "id": 73,
                "titulo": "Búsqueda de camisas",
                "descripcion": "Pendiente en app",
                "status": "OPEN",
                "due_at": timezone.now() - timedelta(days=1),
                "user_email": self.user.email,
                "user_name": self.user.get_full_name(),
                "user_id": 4,
                "area_name": "Junta",
            },
            "minute_agreements",
            SeguimientoItem.TIPO_MINUTA,
            counters,
            checklist=[],
        )
        item.refresh_from_db()
        self.client.force_login(dg_user)

        activos = self.client.get("/seguimiento/panel/?bucket=activos&tab=MINUTA&vista=tabla")
        desfases = self.client.get("/seguimiento/panel/?bucket=desfases&tab=MINUTA&vista=tabla")

        self.assertEqual(item.estatus, SeguimientoItem.ESTATUS_PENDIENTE)
        self.assertIsNone(item.aprobado_at)
        self.assertEqual(item.metadata["source_status"], "OPEN")
        self.assertContains(activos, "Búsqueda de camisas")
        self.assertNotContains(activos, "Desfase app")
        self.assertNotContains(desfases, "Búsqueda de camisas")
        self.assertEqual(desfases.context["vencidos"], 0)

    def test_importador_minuta_preserva_avance_checklist_de_app(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        checklist_items = [
            {"text": "Búsqueda de camisas para área administrativa", "completed": True, "completed_at": "2026-06-11T04:39:31Z"},
            {"text": "Cotización de camisas", "completed": True, "completed_at": "2026-06-11T04:39:31Z"},
            {"text": "Aprobación por parte de DG", "completed": True, "completed_at": "2026-06-11T04:39:31Z"},
            {"text": "Solicitud de compra", "completed": True, "completed_at": "2026-06-11T04:39:31Z"},
            {"text": "Compra entregada", "completed": False, "completed_at": None},
        ]

        command._upsert_item(
            {
                "id": 73,
                "titulo": "Búsqueda de camisas",
                "descripcion": "Pendiente entrega",
                "checklist_items_json": json.dumps(checklist_items),
                "status": "OPEN",
                "due_at": timezone.now() + timedelta(days=5),
                "user_email": self.user.email,
                "user_name": self.user.get_full_name(),
                "user_id": 4,
                "area_name": "Junta",
            },
            "minute_agreements",
            SeguimientoItem.TIPO_MINUTA,
            counters,
        )

        item = SeguimientoItem.objects.prefetch_related("checklist").get(
            metadata__source="agente_dg",
            metadata__source_table="minute_agreements",
            metadata__source_id=73,
        )
        checks = list(item.checklist.all())
        self.assertEqual(len(checks), 5)
        self.assertEqual(sum(check.completado for check in checks), 4)
        self.assertTrue(checks[3].completado)
        self.assertEqual(checks[3].titulo, "Solicitud de compra")
        self.assertFalse(checks[4].completado)
        self.assertEqual(checks[4].titulo, "Compra entregada")
        self.client.force_login(self.user)
        response = self.client.get(f"/seguimiento/{item.pk}/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "app.pollyanasdolce.com")

    def test_webhook_parcial_preserva_checklist_existente(self):
        command = ImportarAgenteDGCommand()
        counters = {"created": 0, "updated": 0, "skipped": 0}
        command._upsert_item(
            {
                "id": 601,
                "titulo": "Proyecto con pasos preservados",
                "descripcion": "Proyecto",
                "status": "IN_PROGRESS",
                "target_date": timezone.now() + timedelta(days=5),
                "user_email": self.user.email,
                "user_name": self.user.get_full_name(),
                "user_id": 4,
                "area_name": "Proyecto",
            },
            "minute_projects",
            SeguimientoItem.TIPO_PROYECTO,
            counters,
            checklist=[{"titulo": "Paso existente", "descripcion": "", "completado": False}],
        )

        upsert_agente_dg_payload(
            {
                "source_table": "minute_projects",
                "source_id": 601,
                "record": {
                    "id": 601,
                    "titulo": "Proyecto con pasos preservados",
                    "descripcion": "Proyecto actualizado",
                    "status": "IN_PROGRESS",
                    "target_date": timezone.now() + timedelta(days=6),
                    "user_email": self.user.email,
                    "user_name": self.user.get_full_name(),
                    "user_id": 4,
                    "area_name": "Proyecto",
                },
            }
        )

        item = SeguimientoItem.objects.get(metadata__source_table="minute_projects", metadata__source_id=601)
        self.assertEqual(item.checklist.count(), 1)
        self.assertEqual(item.checklist.get().titulo, "Paso existente")

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

    @override_settings(AGENTE_DG_WEBHOOK_SECRET="test-webhook-secret")
    def test_webhook_agente_dg_firmado_crea_compromiso(self):
        payload = {
            "event_id": "evt-compromiso-1",
            "source_table": "commitments",
            "source_id": 42,
            "action": "upsert",
            "record": {
                "id": 42,
                "titulo": "Validar bitácora de producción",
                "descripcion": "Revisión diaria generada desde Agente DG.",
                "expected_deliverable": "Bitácora validada",
                "status": "IN_PROGRESS",
                "due_date": (timezone.localdate() + timedelta(days=2)).isoformat(),
                "user_id": 7,
                "user_email": self.user.email,
                "user_name": self.user.get_full_name(),
                "area_name": "PRODUCCION",
            },
        }
        body, signature = _agente_dg_signature(payload, "test-webhook-secret")

        response = self.client.post(
            "/seguimiento/webhooks/agente-dg/",
            data=body,
            content_type="application/json",
            HTTP_X_AGENTE_DG_SIGNATURE=signature,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["updated"], 0)
        item = SeguimientoItem.objects.get(metadata__source_table="commitments", metadata__source_id=42)
        self.assertEqual(item.tipo, SeguimientoItem.TIPO_COMPROMISO)
        self.assertEqual(item.titulo, "Validar bitácora de producción")
        self.assertEqual(item.responsable_user, self.user)
        self.assertEqual(item.estatus, SeguimientoItem.ESTATUS_EN_PROCESO)
        self.assertEqual(item.origen, "Agente DG")

    @override_settings(AGENTE_DG_WEBHOOK_SECRET="test-webhook-secret")
    def test_webhook_agente_dg_rechaza_firma_invalida(self):
        payload = {
            "source_table": "commitments",
            "source_id": 43,
            "action": "upsert",
            "record": {
                "id": 43,
                "titulo": "No debe guardarse",
                "status": "IN_PROGRESS",
                "user_email": self.user.email,
            },
        }

        response = self.client.post(
            "/seguimiento/webhooks/agente-dg/",
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_X_AGENTE_DG_SIGNATURE="sha256=firma-invalida",
        )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(SeguimientoItem.objects.filter(metadata__source_table="commitments", metadata__source_id=43).exists())


@override_settings(SECURE_SSL_REDIRECT=False)
class CalendarioTests(TestCase):
    def setUp(self):
        self.group, _ = Group.objects.get_or_create(name="PRODUCCION")
        self.dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user_a = get_user_model().objects.create_user(
            username="usuario.a",
            email="a@pollyanasdolce.com",
            first_name="Usuario",
            last_name="A",
            password="test12345",
        )
        self.user_a.groups.add(self.group)
        self.user_b = get_user_model().objects.create_user(
            username="usuario.b",
            email="b@pollyanasdolce.com",
            first_name="Usuario",
            last_name="B",
            password="test12345",
        )
        self.user_b.groups.add(self.group)
        self.dg = get_user_model().objects.create_user(username="dg.calendario", password="test12345")
        self.dg.groups.add(self.dg_group)
        self.hoy = timezone.localdate()
        self.manana = self.hoy + timedelta(days=1)

    def _dt(self, fecha, hora=time(10, 0)):
        return timezone.make_aware(datetime.combine(fecha, hora), timezone.get_current_timezone())

    def _eventos(self, user, usuario_param=None):
        self.client.force_login(user)
        params = {
            "start": self.hoy.isoformat(),
            "end": (self.hoy + timedelta(days=7)).isoformat(),
        }
        if usuario_param is not None:
            params["usuario"] = str(usuario_param)
        return self.client.get("/seguimiento/calendario/eventos/", params)

    def test_scope_colaborador_ve_solo_su_item_y_actividad(self):
        SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_COMPROMISO,
            titulo="Compromiso A",
            responsable_user=self.user_a,
            fecha_limite=self._dt(self.hoy),
        )
        ActividadCalendario.objects.create(usuario=self.user_a, titulo="Actividad A", fecha=self.hoy)

        response_a = self._eventos(self.user_a)
        response_b = self._eventos(self.user_b)

        self.assertEqual(response_a.status_code, 200)
        self.assertEqual(len(response_a.json()["eventos"]), 2)
        self.assertEqual(response_b.status_code, 200)
        self.assertEqual(response_b.json()["eventos"], [])

    def test_colaborador_ignora_usuario_param_y_ve_solo_lo_suyo(self):
        ActividadCalendario.objects.create(usuario=self.user_a, titulo="Actividad A", fecha=self.hoy)
        ActividadCalendario.objects.create(usuario=self.user_b, titulo="Actividad B", fecha=self.hoy)

        response = self._eventos(self.user_b, usuario_param=self.user_a.pk)

        self.assertEqual(response.status_code, 200)
        eventos = response.json()["eventos"]
        self.assertEqual(len(eventos), 1)
        self.assertEqual(eventos[0]["titulo"], "Actividad B")

    def test_dg_ve_todo_y_filtra_por_colaborador(self):
        item = SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_MINUTA,
            titulo="Minuta A",
            responsable_user=self.user_a,
            fecha_limite=self._dt(self.hoy, time(9, 0)),
        )
        ActividadCalendario.objects.create(usuario=self.user_a, titulo="Actividad A", fecha=self.hoy)
        ActividadCalendario.objects.create(usuario=self.user_b, titulo="Actividad B", fecha=self.hoy)

        response_all = self._eventos(self.dg)
        response_a = self._eventos(self.dg, usuario_param=self.user_a.pk)

        self.assertEqual(response_all.status_code, 200)
        self.assertEqual(len(response_all.json()["eventos"]), 3)
        self.assertEqual(response_a.status_code, 200)
        self.assertEqual({evento["titulo"] for evento in response_a.json()["eventos"]}, {"Minuta A", "Actividad A"})
        evento_minuta = next(evento for evento in response_a.json()["eventos"] if evento["titulo"] == "Minuta A")
        self.assertEqual(evento_minuta["url"], f"/seguimiento/panel/{item.pk}/")

    def test_colaborador_mantiene_url_personal_de_seguimiento(self):
        item = SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_COMPROMISO,
            titulo="Compromiso propio",
            responsable_user=self.user_a,
            fecha_limite=self._dt(self.hoy),
        )

        response = self._eventos(self.user_a)

        self.assertEqual(response.status_code, 200)
        evento = next(evento for evento in response.json()["eventos"] if evento["titulo"] == "Compromiso propio")
        self.assertEqual(evento["url"], f"/seguimiento/{item.pk}/")
        self.assertEqual(evento["source_label"], "Compromiso")
        self.assertEqual(evento["accion_label"], "Ver seguimiento")
        self.assertFalse(evento["finalizado"])

    def test_completados_siguen_visibles_como_finalizados(self):
        item = SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_PROYECTO,
            titulo="Proyecto cerrado",
            responsable_user=self.user_a,
            fecha_limite=self._dt(self.hoy),
            estatus=SeguimientoItem.ESTATUS_COMPLETADO,
        )
        check = SeguimientoChecklistItem.objects.create(
            seguimiento=item,
            titulo="Paso cerrado",
            vence=self._dt(self.hoy, time(11, 0)),
            completado=True,
            completado_por=self.user_a,
            completado_at=timezone.now(),
        )

        response = self._eventos(self.user_a)

        self.assertEqual(response.status_code, 200)
        eventos = {evento["id"]: evento for evento in response.json()["eventos"]}
        self.assertTrue(eventos[f"item-{item.pk}"]["finalizado"])
        self.assertEqual(eventos[f"item-{item.pk}"]["source_label"], "Proyecto")
        self.assertFalse(eventos[f"item-{item.pk}"]["vencido"])
        self.assertTrue(eventos[f"paso-{check.pk}"]["finalizado"])
        self.assertEqual(eventos[f"paso-{check.pk}"]["source_label"], "Paso")

    def test_item_por_responsable_empleado_usuario_erp_aparece(self):
        empleado = Empleado.objects.create(
            nombre="Empleado Calendario",
            email="empleado.calendario@pollyanasdolce.com",
            area="PRODUCCION",
            puesto="Supervisor",
            usuario_erp=self.user_a,
        )
        SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_PROYECTO,
            titulo="Proyecto por empleado",
            responsable_empleado=empleado,
            fecha_limite=self._dt(self.hoy),
        )

        response = self._eventos(self.user_a)

        self.assertEqual(response.status_code, 200)
        eventos = response.json()["eventos"]
        self.assertEqual(len(eventos), 1)
        self.assertEqual(eventos[0]["titulo"], "Proyecto por empleado")

    def test_crud_actividad_respeta_ownership_y_soft_delete(self):
        ajena = ActividadCalendario.objects.create(usuario=self.user_a, titulo="Ajena", fecha=self.hoy)
        propia = ActividadCalendario.objects.create(usuario=self.user_b, titulo="Propia", fecha=self.hoy)
        self.client.force_login(self.user_b)

        editar_ajena = self.client.post(
            f"/seguimiento/calendario/actividades/{ajena.pk}/",
            {"titulo": "Intento", "fecha": self.hoy.isoformat()},
        )
        completar_ajena = self.client.post(f"/seguimiento/calendario/actividades/{ajena.pk}/completar/")
        eliminar_ajena = self.client.post(f"/seguimiento/calendario/actividades/{ajena.pk}/eliminar/")
        eliminar_propia = self.client.post(f"/seguimiento/calendario/actividades/{propia.pk}/eliminar/")

        self.assertEqual(editar_ajena.status_code, 404)
        self.assertEqual(completar_ajena.status_code, 404)
        self.assertEqual(eliminar_ajena.status_code, 404)
        self.assertEqual(eliminar_propia.status_code, 200)
        propia.refresh_from_db()
        self.assertFalse(propia.activo)

    def test_calendario_no_expone_boton_eliminar_actividad(self):
        self.client.force_login(self.user_a)

        response = self.client.get("/seguimiento/calendario/")

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Eliminar")

    def test_crud_crear_editar_completar_valida_campos(self):
        self.client.force_login(self.user_a)
        sin_titulo = self.client.post("/seguimiento/calendario/actividades/", {"fecha": self.hoy.isoformat()})
        sin_fecha = self.client.post("/seguimiento/calendario/actividades/", {"titulo": "Actividad"})
        hora_invalida = self.client.post(
            "/seguimiento/calendario/actividades/",
            {"titulo": "Actividad", "fecha": self.hoy.isoformat(), "hora_inicio": "16:00", "hora_fin": "15:00"},
        )
        crear = self.client.post(
            "/seguimiento/calendario/actividades/",
            {"titulo": "Actividad", "fecha": self.hoy.isoformat(), "hora_inicio": "09:00", "hora_fin": "10:00"},
        )

        self.assertEqual(sin_titulo.status_code, 400)
        self.assertEqual(sin_fecha.status_code, 400)
        self.assertEqual(hora_invalida.status_code, 400)
        self.assertEqual(crear.status_code, 200)
        actividad = ActividadCalendario.objects.get(usuario=self.user_a)
        editar = self.client.post(
            f"/seguimiento/calendario/actividades/{actividad.pk}/",
            {"titulo": "Actividad editada", "fecha": self.manana.isoformat()},
        )
        completar = self.client.post(f"/seguimiento/calendario/actividades/{actividad.pk}/completar/")

        self.assertEqual(editar.status_code, 200)
        self.assertEqual(completar.status_code, 200)
        actividad.refresh_from_db()
        self.assertEqual(actividad.titulo, "Actividad editada")
        self.assertEqual(actividad.estatus, ActividadCalendario.ESTATUS_COMPLETADA)

    def test_crear_reunion_recurrente_notifica_y_se_muestra_a_invitado(self):
        UserProfile.objects.create(user=self.user_b, telefono="6687654321")
        self.client.force_login(self.user_a)

        with patch("seguimiento.views.send_mail") as send_mail_mock, patch("seguimiento.views._enviar_whatsapp_maya") as whatsapp_mock:
            response = self.client.post(
                "/seguimiento/calendario/actividades/",
                {
                    "tipo": "REUNION",
                    "titulo": "Reunión semanal DG",
                    "fecha": self.hoy.isoformat(),
                    "hora_inicio": "09:00",
                    "hora_fin": "09:30",
                    "invitado_user": str(self.user_b.pk),
                    "direccion_general": "1",
                    "periodicidad": "SEMANAL",
                    "repeticiones": "3",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["creadas"], 3)
        self.assertEqual(ActividadCalendario.objects.filter(tipo=ActividadCalendario.TIPO_REUNION).count(), 3)
        self.assertTrue(Notificacion.objects.filter(usuario=self.user_b, titulo__startswith="Reunión:").exists())
        self.assertTrue(Notificacion.objects.filter(usuario=self.dg, titulo__startswith="Reunión:").exists())
        send_mail_mock.assert_called()
        whatsapp_mock.assert_called_once()

        response_invitado = self._eventos(self.user_b)
        response_creador = self._eventos(self.user_a)

        evento_invitado = next(evento for evento in response_invitado.json()["eventos"] if evento["titulo"] == "Reunión semanal DG")
        evento_creador = next(evento for evento in response_creador.json()["eventos"] if evento["titulo"] == "Reunión semanal DG")
        self.assertEqual(evento_invitado["source_label"], "Reunión DG")
        self.assertEqual(evento_invitado["invitado"], "Usuario B")
        self.assertTrue(evento_invitado["direccion_general"])
        self.assertEqual(evento_creador["source_label"], "Reunión DG")

    def test_validaciones_endpoint_eventos(self):
        self.client.force_login(self.user_a)

        sin_rango = self.client.get("/seguimiento/calendario/eventos/")
        rango_largo = self.client.get(
            "/seguimiento/calendario/eventos/",
            {"start": self.hoy.isoformat(), "end": (self.hoy + timedelta(days=63)).isoformat()},
        )
        rango_invertido = self.client.get(
            "/seguimiento/calendario/eventos/",
            {"start": self.manana.isoformat(), "end": self.hoy.isoformat()},
        )

        self.assertEqual(sin_rango.status_code, 400)
        self.assertEqual(rango_largo.status_code, 400)
        self.assertEqual(rango_invertido.status_code, 400)

    def test_recordatorios_crean_notificacion_y_canales(self):
        SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_COMPROMISO,
            titulo="Vence hoy",
            responsable_user=self.user_a,
            fecha_limite=self._dt(self.hoy),
        )
        ActividadCalendario.objects.create(usuario=self.user_a, titulo="Actividad mañana", fecha=self.manana)
        UserProfile.objects.create(user=self.user_a, telefono="6681234567")

        with patch("seguimiento.tasks.send_mail") as send_mail_mock, patch("seguimiento.tasks._enviar_whatsapp_maya") as whatsapp_mock:
            from seguimiento.tasks import recordatorios_calendario

            result = recordatorios_calendario()

        self.assertEqual(result["usuarios_notificados"], 1)
        self.assertEqual(result["correos"], 1)
        self.assertEqual(result["whatsapps"], 1)
        self.assertTrue(Notificacion.objects.filter(usuario=self.user_a, tipo=Notificacion.TIPO_SEGUIMIENTO).exists())
        self.assertFalse(Notificacion.objects.filter(usuario=self.user_b).exists())
        send_mail_mock.assert_called_once()
        whatsapp_mock.assert_called_once()

    def test_navegacion_incluye_calendario_para_usuario_con_rol(self):
        groups = build_nav_groups(self.user_a, "/seguimiento/calendario/")
        labels = [item["label"] for group in groups for item in group["items"]]

        self.assertIn("Mi calendario", labels)
        self.assertTrue(can_view_submodule(self.user_a, "seguimiento", "calendario"))
