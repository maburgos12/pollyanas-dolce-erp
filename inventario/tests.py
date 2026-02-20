from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from inventario.models import AlmacenSyncRun
from maestros.models import Insumo, InsumoAlias, PointPendingMatch, UnidadMedida
from recetas.models import LineaReceta, Receta


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

    def test_export_cross_pending_csv(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="P-100",
            point_nombre="Mantequilla Barra",
            fuzzy_score=88.5,
            fuzzy_sugerencia="Mantequilla",
        )
        receta = Receta.objects.create(nombre="Receta Test Export", hash_contenido="hash-export-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Mantequilla Barra",
            cantidad=1,
            unidad=None,
            unidad_texto="kg",
            costo_unitario_snapshot=0,
            match_status=LineaReceta.STATUS_REJECTED,
        )

        response = self.client.get(
            reverse("inventario:aliases_catalog"),
            {"export": "cross_pending_csv"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        body = response.content.decode("utf-8")
        self.assertIn("nombre_muestra", body)
        self.assertIn("Mantequilla Barra", body)

    def test_auto_apply_suggestions_creates_alias_and_cleans_pending(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo = Insumo.objects.create(nombre="Harina Pastelera", unidad_base=unidad)
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=11,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 9,
                    "nombre_origen": "Harina pastelera 25kg",
                    "nombre_normalizado": "harina pastelera 25kg",
                    "sugerencia": "Harina Pastelera",
                    "score": 95.0,
                }
            ],
        )

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "auto_apply_suggestions",
                "auto_min_score": "90",
                "auto_max_rows": "50",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            InsumoAlias.objects.filter(nombre_normalizado="harina pastelera 25kg", insumo=insumo).exists()
        )

        run.refresh_from_db()
        self.assertEqual(run.pending_preview, [])
