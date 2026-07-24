from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import AuditLog, Notificacion
from rrhh.models import Empleado
from seguimiento.models import SeguimientoItem


class SuperuserPreviewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.actor = user_model.objects.create_superuser(
            username="mauricio.preview",
            email="mauricio@example.com",
            password="test-password",
            first_name="Mauricio",
            last_name="Burgos",
        )
        self.target = user_model.objects.create_user(
            username="colaboradora.preview",
            email="colaboradora@example.com",
            password="test-password",
            first_name="Ana",
            last_name="López",
        )
        self.employee = Empleado.objects.create(
            codigo="PREVIEW-001",
            nombre="Ana López",
            area="PRODUCCIÓN",
            puesto="Supervisora",
            sucursal="Matriz",
            usuario_erp=self.target,
        )
        self.client.force_login(self.actor)

    def _start_preview(self, *, next_url="/dashboard/"):
        return self.client.post(
            reverse("superuser_preview_start"),
            {"user_id": self.target.pk, "next": next_url},
        )

    def test_only_superuser_sees_view_as_control(self):
        response = self.client.get(reverse("notificaciones"))
        self.assertContains(response, "Ver como empleado")

        normal_user = get_user_model().objects.create_user(
            username="usuario.normal",
            password="test-password",
        )
        self.client.force_login(normal_user)
        response = self.client.get(reverse("notificaciones"))
        self.assertNotContains(response, "Ver como empleado")

    def test_options_only_include_active_linked_personnel(self):
        unlinked = get_user_model().objects.create_user(
            username="sin.empleado",
            first_name="Sin",
            last_name="Empleado",
        )
        inactive_user = get_user_model().objects.create_user(
            username="inactiva.preview",
            is_active=False,
        )
        Empleado.objects.create(
            codigo="PREVIEW-002",
            nombre="Persona Inactiva",
            activo=True,
            usuario_erp=inactive_user,
        )

        response = self.client.get(
            reverse("superuser_preview_options"),
            {"q": "Ana producción"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual([item["user_id"] for item in payload["results"]], [self.target.pk])
        self.assertEqual(payload["results"][0]["name"], "Ana López")
        self.assertNotContains(response, unlinked.username)
        self.assertNotContains(response, inactive_user.username)

    def test_non_superuser_cannot_start_preview(self):
        normal_user = get_user_model().objects.create_user(
            username="usuario.normal",
            password="test-password",
        )
        self.client.force_login(normal_user)

        response = self.client.post(
            reverse("superuser_preview_start"),
            {"user_id": self.target.pk},
        )

        self.assertEqual(response.status_code, 403)
        self.assertNotIn("superuser_preview_user_id", self.client.session)

    def test_preview_uses_target_identity_for_safe_requests(self):
        response = self._start_preview(next_url="/seguimiento/")
        self.assertRedirects(response, "/seguimiento/", fetch_redirect_response=False)

        response = self.client.get("/seguimiento/")

        self.assertEqual(response.wsgi_request.user.pk, self.target.pk)
        self.assertEqual(response.wsgi_request.preview_actor.pk, self.actor.pk)
        self.assertContains(response, "Estás viendo como")
        self.assertContains(response, "Ana López")
        self.assertContains(response, "Solo lectura")

    def test_preview_preserves_employee_assignment_isolation_in_my_work(self):
        other_user = get_user_model().objects.create_user(username="otra.persona")
        SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_MINUTA,
            titulo="Minuta exclusiva de Ana",
            responsable_user=self.target,
            creado_por=self.actor,
        )
        SeguimientoItem.objects.create(
            tipo=SeguimientoItem.TIPO_MINUTA,
            titulo="Minuta exclusiva de otra persona",
            responsable_user=other_user,
            creado_por=self.actor,
        )
        self._start_preview(next_url=reverse("seguimiento:minutas"))

        response = self.client.get(reverse("seguimiento:minutas"))

        self.assertContains(response, "Minuta exclusiva de Ana")
        self.assertNotContains(response, "Minuta exclusiva de otra persona")

    def test_preview_blocks_mutations_before_view_runs(self):
        notification = Notificacion.objects.create(
            usuario=self.target,
            titulo="Pendiente de lectura",
            mensaje="No debe modificarse durante la vista previa",
        )
        self._start_preview()

        response = self.client.post(reverse("notificaciones_marcar_todas"))

        self.assertEqual(response.status_code, 403)
        notification.refresh_from_db()
        self.assertFalse(notification.leida)

    def test_exit_restores_actor_and_records_audit_events(self):
        self._start_preview()
        response = self.client.post(
            reverse("superuser_preview_stop"),
            {"next": "/dashboard/"},
        )

        self.assertRedirects(response, "/dashboard/", fetch_redirect_response=False)
        self.assertNotIn("superuser_preview_user_id", self.client.session)
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.wsgi_request.user.pk, self.actor.pk)
        self.assertEqual(
            list(
                AuditLog.objects.filter(
                    user=self.actor,
                    model="auth.User",
                    object_id=str(self.target.pk),
                ).values_list("action", flat=True)
            ),
            ["SUPERUSER_PREVIEW_STOP", "SUPERUSER_PREVIEW_START"],
        )

    def test_preview_rejects_external_next_url(self):
        response = self._start_preview(next_url="https://example.com/phishing")
        self.assertRedirects(response, "/dashboard/", fetch_redirect_response=False)

    def test_deleted_or_inactive_target_clears_preview(self):
        self._start_preview()
        self.target.is_active = False
        self.target.save(update_fields=["is_active"])

        response = self.client.get(reverse("dashboard"))

        self.assertEqual(response.wsgi_request.user.pk, self.actor.pk)
        self.assertNotIn("superuser_preview_user_id", self.client.session)
        self.assertNotContains(response, "Estás viendo como")
