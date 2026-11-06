import os
from datetime import timedelta
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, SimpleTestCase, override_settings
from django.utils import timezone

from core.access import ROLE_DG
from seguimiento.models import SeguimientoItem
from seguimiento.services import upsert_agente_dg_payload


class CommunicationClientTests(SimpleTestCase):
    def test_partial_delivery_is_returned_per_channel(self):
        from seguimiento import agente_dg_client
        response = Mock(status_code=200)
        response.json.return_value = {"results": [{"minute_id": 104, "channels": {
            "email": {"status": "accepted"}, "whatsapp": {"status": "failed", "error": "No disponible"}}}]}
        with patch.object(agente_dg_client, "_request", return_value=response) as request:
            result = agente_dg_client.send_minute_agreement(104, send_email=True, send_whatsapp=True)
        self.assertEqual(result["channels"]["whatsapp"]["status"], "failed")
        self.assertFalse(request.call_args.kwargs["json"]["force_resend"])


@override_settings(SECURE_SSL_REDIRECT=False)
class AgreementCommunicationTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.dg = User.objects.create_user(username="dg.paridad", password="test")
        self.dg.groups.add(Group.objects.get_or_create(name=ROLE_DG)[0])
        self.recipient = User.objects.create_user(username="colaboradora.paridad", email="operacion@pollyanasdolce.com")
        self.client.force_login(self.dg)
        self.payload = {"id": 104, "collaborator_user_id": 45, "title": "Solicitar los tickets de las cargas de gasolina.",
                        "agreement_text": "", "status": "OPEN", "due_at": (timezone.now() + timedelta(days=3)).isoformat(),
                        "checklist_items": [{"id": "one", "text": "Solicitar los tickets de las cargas de gasolina.", "completed": False}]}
        self.form = {"responsable_user_id": self.recipient.pk, "titulo": self.payload["title"],
                     "fecha_limite": (timezone.localdate() + timedelta(days=3)).isoformat(), "hora_limite": "18:00",
                     "checklist": self.payload["title"], "accion": "notificar", "send_email": "on", "send_whatsapp": "on"}
        self.env = patch.dict(os.environ, {"AGENTE_DG_WRITEBACK_ENABLED": "true"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def _create(self, action):
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.get_users", return_value=[{"id": 45, "email": self.recipient.email}]), \
             patch("seguimiento.agente_dg_client.create_minute_agreement", return_value=self.payload), \
             patch("seguimiento.agente_dg_client.send_minute_agreement", return_value={"minute_id": 104, "channels": {
                 "email": {"status": "accepted"}, "whatsapp": {"status": "failed", "error": "No disponible"}}}) as send:
            response = self.client.post("/seguimiento/panel/crear-agente-dg/", {**self.form, "accion": action}, follow=True)
        return response, send

    def test_create_and_notify_reports_partial_failure_without_recreating(self):
        response, send = self._create("notificar")
        send.assert_called_once()
        self.assertEqual(SeguimientoItem.objects.filter(metadata__source_id=104).count(), 1)
        self.assertContains(response, "WhatsApp")
        self.assertContains(response, "fallido")

    def test_save_does_not_send(self):
        response, send = self._create("guardar")
        send.assert_not_called()
        self.assertContains(response, "sin solicitar notificaciones")

    def test_import_communication_does_not_erase_on_legacy_payload(self):
        record = {"id": 104, "titulo": self.payload["title"], "user_email": self.recipient.email,
                  "content_version": "v1", "communications": [{"event_key": "assignment:104", "event_type": "assignment",
                  "channels": {"whatsapp": {"status": "read"}}}]}
        upsert_agente_dg_payload({"source_table": "minute_agreements", "source_id": 104, "record": record})
        record.pop("communications")
        upsert_agente_dg_payload({"source_table": "minute_agreements", "source_id": 104, "record": record})
        item = SeguimientoItem.objects.get(metadata__source_id=104)
        self.assertEqual(item.metadata["communications"][0]["channels"]["whatsapp"]["status"], "read")

    def test_colleague_cannot_manage_another_agreement(self):
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Privado", responsable_user=self.recipient,
                                             metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104, "source_user_id": 45})
        self.client.force_login(self.recipient)
        response = self.client.post(f"/seguimiento/panel/{item.pk}/gestionar/", {"accion": "guardar"})
        self.assertIn(response.status_code, (302, 403))
        item.refresh_from_db()
        self.assertEqual(item.titulo, "Privado")

    def test_stale_webhook_does_not_restore_old_content(self):
        record = {"id": 104, "titulo": "Versión nueva", "updated_at": "2026-11-06T20:00:00Z"}
        upsert_agente_dg_payload({"source_table": "minute_agreements", "source_id": 104, "record": record})
        record.update(titulo="Versión antigua", updated_at="2026-11-06T19:00:00Z")
        upsert_agente_dg_payload({"source_table": "minute_agreements", "source_id": 104, "record": record})
        self.assertEqual(SeguimientoItem.objects.get(metadata__source_id=104).titulo, "Versión nueva")

    def test_creation_keeps_the_same_intent_after_api_timeout(self):
        from requests.exceptions import Timeout
        from seguimiento.agente_dg_client import AgenteDGError
        session = self.client.session
        session["agreement_creation_request_id"] = "699d69f3-9be1-4a43-89f0-6ff51939d83f"
        session.save()
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.get_users", return_value=[{"id": 45, "email": self.recipient.email}]), \
             patch("seguimiento.agente_dg_client.create_minute_agreement", side_effect=Timeout) as create:
            self.client.post("/seguimiento/panel/crear-agente-dg/", self.form)
        self.assertEqual(create.call_args.kwargs["request_id"], session["agreement_creation_request_id"])
        self.assertEqual(self.client.session["agreement_creation_request_id"], session["agreement_creation_request_id"])

    def test_source_minute_submission_uses_common_dispatch(self):
        from core.notificaciones import notificar_seguimiento_entrega
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Solicitud", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104,
                      "send_email_on_create": True, "send_whatsapp_on_create": True})
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.send_minute_agreement", return_value={"event_key": "event", "channels": {}}) as send, \
             patch("core.notificaciones.send_mail") as direct_email:
            notificar_seguimiento_entrega(item, actor=self.recipient)
        send.assert_called_once()
        self.assertEqual(send.call_args.kwargs["event_type"], "submitted")
        direct_email.assert_not_called()

    def test_source_comment_failure_does_not_claim_local_success(self):
        from seguimiento.agente_dg_client import AgenteDGError
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Aviso", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104, "source_user_id": 45})
        self.client.force_login(self.recipient)
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.get_minute_agreement", return_value=self.payload), \
             patch("seguimiento.agente_dg_client.post_minute_comment", side_effect=AgenteDGError("No confirmado")) as comment:
            self.client.post(f"/seguimiento/{item.pk}/retroalimentacion/", {"comentario": "Recibido"})
        comment.assert_called_once()
        self.assertEqual(item.comentarios.count(), 0)

    def test_reminder_counts_exclude_source_minutes_but_keep_local_work(self):
        from seguimiento.tasks import _conteos_recordatorio_usuario
        for source in (True, False):
            SeguimientoItem.objects.create(tipo="MINUTA", titulo="Aviso", responsable_user=self.recipient,
                fecha_limite=timezone.now() + timedelta(minutes=1),
                metadata={"source_table": "minute_agreements"} if source else {})
        result = _conteos_recordatorio_usuario(self.recipient, timezone.localdate(), timezone.localdate() + timedelta(days=1))
        self.assertEqual(result["seguimientos"], 1)

    def test_source_communication_creates_one_internal_notice_on_replay(self):
        from core.models import Notificacion
        record = {"id": 104, "titulo": "Aviso", "user_id": 45, "user_email": self.recipient.email,
            "communications": [{"event_key": "assignment", "content_version": "v1", "event_type": "assignment",
                                "channels": {"email": {"status": "accepted"}}}]}
        for _ in range(2):
            upsert_agente_dg_payload({"source_table": "minute_agreements", "source_id": 104, "record": record})
        self.assertEqual(Notificacion.objects.filter(usuario=self.recipient, tipo="seguimiento").count(), 1)

    def test_refresh_reassignment_revokes_previous_owner(self):
        from seguimiento.views import _refresh_minute
        new_user = get_user_model().objects.create_user(username="nuevo.responsable", email="nuevo@pollyanasdolce.com")
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Aviso", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104, "source_user_id": 45})
        with patch("seguimiento.agente_dg_client.get_minute_agreement", return_value={**self.payload, "collaborator_user_id": 3}), \
             patch("seguimiento.agente_dg_client.get_users", return_value=[{"id": 3, "name": "Nuevo", "email": new_user.email}]):
            _refresh_minute(item)
        self.assertEqual(item.responsable_user, new_user)
        self.client.force_login(self.recipient)
        self.assertEqual(self.client.get(f"/seguimiento/{item.pk}/").status_code, 404)

    def test_full_app_titles_and_checklist_are_not_truncated(self):
        import json
        title = "Acuerdo " + "x" * 270
        point = "Solicitar " + "detalle " * 60
        record = {"id": 104, "titulo": title, "area_name": "Junta " + "y" * 180,
                  "checklist_items_json": json.dumps([{"id": "full", "text": point, "completed": False}])}
        upsert_agente_dg_payload({"source_table": "minute_agreements", "source_id": 104, "record": record})
        item = SeguimientoItem.objects.get(metadata__source_id=104)
        self.assertEqual(item.titulo, title)
        self.assertEqual(item.checklist.get().titulo, point.strip())

    def test_mirror_failure_does_not_drop_requested_notification(self):
        with patch("seguimiento.views.upsert_agente_dg_payload", side_effect=RuntimeError("fallo de espejo")):
            response, send = self._create("notificar")
        send.assert_called_once()
        self.assertTrue(self.client.session["agreement_creation_pending"])
        self.assertIn("agreement_creation_request_id", self.client.session)

    def test_dg_feedback_has_explicit_collaborator_recipient_and_actor(self):
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Aviso", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104, "source_user_id": 45})
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.post_minute_comment", return_value={"id": 1}) as comment, \
             patch("seguimiento.views._refresh_minute"):
            response = self.client.post(f"/seguimiento/{item.pk}/retroalimentacion/", {"comentario": "Observación para el responsable"}, follow=True)
        self.assertEqual(comment.call_args.kwargs["notify_recipient"], "collaborator")
        self.assertEqual(comment.call_args.kwargs["origin_actor"]["user_id"], self.dg.pk)
        self.assertEqual(response.status_code, 200)
        self.assertRedirects(response, f"/seguimiento/panel/{item.pk}/")

    def test_internal_lifecycle_notice_survives_disabled_external_channels(self):
        from core.models import Notificacion
        from seguimiento.communications import dispatch_event
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Sin canales", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104,
                      "send_email_on_create": False, "send_whatsapp_on_create": False})
        result = {"minute_id": 104, "event_type": "submitted", "event_key": "erp-submission", "content_version": "v1",
                  "channels": {"email": {"status": "not_requested"}, "whatsapp": {"status": "not_requested"}}}
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.send_minute_agreement", return_value=result):
            dispatch_event(item, "submitted", actor=self.recipient)
            dispatch_event(item, "submitted", actor=self.recipient)
        notice = Notificacion.objects.get(usuario=self.dg, objeto_id=item.pk)
        self.assertEqual(notice.url, f"/seguimiento/panel/{item.pk}/")
        response = self.client.get(notice.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Sin canales")

    def test_previous_owner_cannot_publish_after_source_reassignment(self):
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Reasignado", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104, "source_user_id": 45})
        self.client.force_login(self.recipient)
        cases = [
            ("retroalimentacion", {"comentario": "Comentario anterior"}),
            ("evidencias", {"archivo": SimpleUploadedFile("avance.txt", b"avance", content_type="text/plain")}),
            ("prorroga", {"fecha_solicitada": (timezone.localdate() + timedelta(days=7)).isoformat(), "motivo": "Mas tiempo"}),
        ]
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.get_minute_agreement", return_value={**self.payload, "collaborator_user_id": 46}), \
             patch("seguimiento.agente_dg_client.post_minute_comment") as comment, \
             patch("seguimiento.agente_dg_client.post_minute_evidence") as evidence, \
             patch("seguimiento.agente_dg_client.post_minute_extension") as extension:
            for action, data in cases:
                with self.subTest(action=action):
                    response = self.client.post(f"/seguimiento/{item.pk}/{action}/", data)
                    self.assertEqual(response.status_code, 302)
            comment.assert_not_called()
            evidence.assert_not_called()
            extension.assert_not_called()
        self.assertEqual(item.comentarios.count(), 0)
        self.assertEqual(item.evidencias.count(), 0)
        self.assertEqual(item.prorrogas.count(), 0)

    def test_partial_retry_projection_retains_previous_accepted_channel(self):
        from seguimiento.communications import _store_result
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Parcial", responsable_user=self.recipient)
        event = {"content_version": "v1", "event_type": "assignment", "event_key": "assignment", "channels": {
            "email": {"status": "accepted", "provider_message_id": "synthetic"}, "whatsapp": {"status": "failed"}}}
        _store_result(item, event)
        _store_result(item, {**event, "channels": {"email": {"status": "not_requested"}, "whatsapp": {"status": "accepted"}}})
        self.assertEqual(item.metadata["communications"][0]["channels"]["email"]["provider_message_id"], "synthetic")

    def test_status_writeback_preserves_metadata_updated_by_synchronous_webhook(self):
        from seguimiento.views import _writeback_agente_dg_item
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Estado", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104})
        def webhook(*args, **kwargs):
            SeguimientoItem.objects.filter(pk=item.pk).update(metadata={**item.metadata, "content_version": "fresh"})
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.patch_minute_agreement", side_effect=webhook):
            self.assertTrue(_writeback_agente_dg_item(item, accion="aprobar"))
        self.assertEqual(item.metadata["content_version"], "fresh")

    def test_reassigned_former_owner_cannot_close_directly(self):
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Estado", responsable_user=self.recipient, requiere_aprobacion=False,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104, "source_user_id": 45})
        self.client.force_login(self.recipient)
        with patch("seguimiento.agente_dg_client.get_minute_agreement", return_value={**self.payload, "collaborator_user_id": 46}), \
             patch("seguimiento.agente_dg_client.patch_minute_agreement") as change:
            self.client.post(f"/seguimiento/{item.pk}/completar/")
        change.assert_not_called()
        item.refresh_from_db()
        self.assertFalse(item.esta_cerrado)

    def test_source_evidence_uses_the_same_type_and_size_limits_as_app(self):
        from django.core.files.uploadedfile import SimpleUploadedFile
        from seguimiento.views import _validar_archivo_evidencia
        self.assertIsNotNone(_validar_archivo_evidencia(SimpleUploadedFile("avance.txt", b"dato"), origen_minuta=True))
        self.assertIsNone(_validar_archivo_evidencia(SimpleUploadedFile("avance.png", b"png", content_type="image/png"), origen_minuta=True))
        self.assertIsNone(_validar_archivo_evidencia(SimpleUploadedFile("avance.pptx", b"presentacion", content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation"), origen_minuta=True))
        large = SimpleUploadedFile("avance.png", b"png", content_type="image/png")
        large.size = 10 * 1024 * 1024 + 1
        self.assertIsNotNone(_validar_archivo_evidencia(large, origen_minuta=True))

    def test_source_submission_accepts_app_pptx_format(self):
        from django.core.files.uploadedfile import SimpleUploadedFile
        item = SeguimientoItem.objects.create(tipo="MINUTA", titulo="Entrega", responsable_user=self.recipient,
            metadata={"source": "agente_dg", "source_table": "minute_agreements", "source_id": 104, "source_user_id": 45})
        self.client.force_login(self.recipient)
        with patch("seguimiento.agente_dg_client.is_configured", return_value=True), \
             patch("seguimiento.agente_dg_client.get_minute_agreement", return_value=self.payload), \
             patch("seguimiento.minuta_checklist.sincronizar_checklist_minuta"), \
             patch("seguimiento.agente_dg_client.post_minute_evidence", return_value={"id": "synthetic"}) as upload, \
             patch("seguimiento.views._refresh_minute"):
            response = self.client.post(f"/seguimiento/{item.pk}/entregar/", {"archivo": SimpleUploadedFile("avance.pptx", b"presentacion", content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation")})
        self.assertEqual(response.status_code, 302)
        upload.assert_called_once()
        item.refresh_from_db()
        self.assertEqual(item.estatus, SeguimientoItem.ESTATUS_EN_REVISION)
