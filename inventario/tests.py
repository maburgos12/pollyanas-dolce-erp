from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from inventario.models import AlmacenSyncRun


class InventarioAliasesPendingTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_inv",
            email="admin_inv@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

    def test_pending_persisted_hide_and_restore_visibility(self):
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=10,
            unmatched=2,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 8,
                    "nombre_origen": "Harina pastelera 25kg",
                    "nombre_normalizado": "harina pastelera 25kg",
                    "sugerencia": "Harina pastelera",
                    "score": 92.0,
                }
            ],
        )

        response = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["pending_visible_count"], 1)
        self.assertEqual(response.context["pending_source"], "persisted")

        self.client.post(
            reverse("inventario:aliases_catalog"),
            {"action": "clear_pending", "hide_run_id": str(run.id)},
        )
        response_hidden = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response_hidden.status_code, 200)
        self.assertEqual(response_hidden.context["pending_visible_count"], 0)
        self.assertEqual(response_hidden.context["hidden_run_id"], run.id)
        self.assertIsNotNone(response_hidden.context["hidden_pending_run"])

        self.client.post(
            reverse("inventario:aliases_catalog"),
            {"action": "reset_hidden_pending"},
        )
        response_restored = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response_restored.status_code, 200)
        self.assertEqual(response_restored.context["pending_visible_count"], 1)
        self.assertEqual(response_restored.context["pending_source"], "persisted")

    def test_load_pending_run_moves_preview_to_session(self):
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_SCHEDULED,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=20,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 4,
                    "nombre_origen": "Mantequilla barra",
                    "nombre_normalizado": "mantequilla barra",
                    "sugerencia": "Mantequilla",
                    "score": 88.0,
                }
            ],
        )

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {"action": "load_pending_run", "run_id": str(run.id)},
        )
        self.assertEqual(response.status_code, 302)
        session_preview = self.client.session.get("inventario_pending_preview")
        self.assertIsInstance(session_preview, list)
        self.assertEqual(len(session_preview), 1)

        response_after = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response_after.status_code, 200)
        self.assertEqual(response_after.context["pending_source"], "session")
        self.assertEqual(response_after.context["pending_visible_count"], 1)
