import os
from types import SimpleNamespace
from django.db import OperationalError
from django.test import TestCase
from django.urls import reverse
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from unittest.mock import MagicMock, patch

from core.access import ROLE_ADMIN, ROLE_COMPRAS, can_view_compras
from core.models import UserProfile
from core.views import _compute_plan_forecast_semaforo
from core.management.commands.ejecutar_rutina_diaria_erp import _prefer_public_database_url_if_needed
from inventario.models import AlmacenSyncRun
from maestros.models import PointPendingMatch
from recetas.models import LineaReceta, Receta


class DashboardForecastRobustnessTests(TestCase):
    def test_compute_plan_forecast_handles_missing_pronostico_table(self):
        with patch("core.views.PronosticoVenta.objects.filter", side_effect=OperationalError("missing table")):
            result = _compute_plan_forecast_semaforo("2026-02")

        self.assertEqual(result["periodo_mes"], "2026-02")
        self.assertEqual(result["recetas_total"], 0)
        self.assertEqual(result["recetas_con_desviacion"], 0)
        self.assertTrue(result["data_unavailable"])


class DashboardHomologacionContextTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_dashboard",
            email="admin_dashboard@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

    def test_dashboard_context_includes_homologacion_counts(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-100",
            point_nombre="Insumo pendiente",
            fuzzy_sugerencia="",
            fuzzy_score=0,
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_PRODUCTO,
            point_codigo="PT-PROD-100",
            point_nombre="Producto pendiente",
            fuzzy_sugerencia="",
            fuzzy_score=0,
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_PROVEEDOR,
            point_codigo="PT-PROV-100",
            point_nombre="Proveedor pendiente",
            fuzzy_sugerencia="",
            fuzzy_score=0,
        )

        receta = Receta.objects.create(nombre="Receta test", hash_contenido="hash-dashboard-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo_texto="Insumo por revisar",
            match_status=LineaReceta.STATUS_NEEDS_REVIEW,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=2,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo_texto="Insumo rechazado",
            match_status=LineaReceta.STATUS_REJECTED,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=3,
            tipo_linea=LineaReceta.TIPO_SUBSECCION,
            insumo_texto="Subsección no cuenta",
            match_status=LineaReceta.STATUS_NEEDS_REVIEW,
        )

        AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            unmatched=5,
            matched=20,
        )

        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)

        self.assertEqual(response.context["point_pending_total"], 3)
        self.assertEqual(response.context["point_pending_insumos"], 1)
        self.assertEqual(response.context["point_pending_productos"], 1)
        self.assertEqual(response.context["point_pending_proveedores"], 1)
        self.assertEqual(response.context["recetas_pending_matching_count"], 2)
        self.assertEqual(response.context["inventario_last_unmatched_count"], 5)
        self.assertEqual(response.context["homologacion_total_pending"], 10)


class UsersAccessTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_user(
            username="admin_users",
            email="admin_users@example.com",
            password="test12345",
        )
        admin_group, _ = Group.objects.get_or_create(name=ROLE_ADMIN)
        self.admin.groups.add(admin_group)

        self.compras = user_model.objects.create_user(
            username="compras_user",
            email="compras_user@example.com",
            password="test12345",
        )
        compras_group, _ = Group.objects.get_or_create(name=ROLE_COMPRAS)
        self.compras.groups.add(compras_group)

    def test_admin_can_open_users_access_page(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("users_access"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Crear Usuario")

    def test_non_admin_cannot_open_users_access_page(self):
        self.client.force_login(self.compras)
        response = self.client.get(reverse("users_access"))
        self.assertEqual(response.status_code, 403)

    def test_lock_compras_blocks_access_even_with_compras_role(self):
        self.assertTrue(can_view_compras(self.compras))
        profile, _ = UserProfile.objects.get_or_create(user=self.compras)
        profile.lock_compras = True
        profile.save(update_fields=["lock_compras"])
        self.assertFalse(can_view_compras(self.compras))


class RutinaDiariaDatabaseFallbackTests(TestCase):
    def test_fallback_returns_none_without_public_url(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://u:p@postgres.railway.internal:5432/railway",
            },
            clear=False,
        ):
            os.environ.pop("DATABASE_PUBLIC_URL", None)
            result = _prefer_public_database_url_if_needed()
        self.assertIsNone(result)

    @patch("core.management.commands.ejecutar_rutina_diaria_erp.dj_database_url.parse")
    @patch("core.management.commands.ejecutar_rutina_diaria_erp.socket.getaddrinfo", side_effect=OSError("dns fail"))
    def test_fallback_switches_to_public_url_when_internal_dns_fails(self, _dns_mock, parse_mock):
        parse_mock.return_value = {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "railway",
            "USER": "postgres",
            "PASSWORD": "secret",
            "HOST": "shinkansen.proxy.rlwy.net",
            "PORT": "29018",
        }
        mock_conn = MagicMock()
        fake_connections = {"default": mock_conn}
        fake_settings = SimpleNamespace(DATABASES={"default": {"ENGINE": "django.db.backends.postgresql"}})

        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://postgres:secret@postgres.railway.internal:5432/railway",
                "DATABASE_PUBLIC_URL": "postgresql://postgres:secret@shinkansen.proxy.rlwy.net:29018/railway?sslmode=require",
            },
            clear=False,
        ):
            with (
                patch("core.management.commands.ejecutar_rutina_diaria_erp.connections", fake_connections),
                patch("core.management.commands.ejecutar_rutina_diaria_erp.settings", fake_settings),
            ):
                result = _prefer_public_database_url_if_needed()
                effective_db_url = os.environ.get("DATABASE_URL")

        self.assertIn("DATABASE_URL fallback aplicado", result or "")
        self.assertIn("DATABASE_PUBLIC_URL", result or "")
        self.assertEqual(
            effective_db_url,
            "postgresql://postgres:secret@shinkansen.proxy.rlwy.net:29018/railway?sslmode=require",
        )
        parse_mock.assert_called_once()
        mock_conn.close.assert_called_once()
