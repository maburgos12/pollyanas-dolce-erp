import hashlib
import hmac
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

from core.access import ROLE_DG, can_view_submodule
from core.navigation import build_nav_groups
from rrhh.models import Empleado

from .models import (
    SeguimientoChecklistItem,
    SeguimientoComentario,
    SeguimientoEvidencia,
    SeguimientoItem,
    SeguimientoProrrogaSolicitud,
)
from .services import empleado_de_usuario
from .management.commands.importar_agente_dg_seguimiento import Command as ImportarAgenteDGCommand
from .management.commands.importar_agente_dg_seguimiento import _status_agente_a_erp


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

        response = self.client.get("/seguimiento/panel/?tipo=MINUTA&vista=tabla&tab=MINUTA")

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
