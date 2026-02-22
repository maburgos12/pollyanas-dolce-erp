from django.db import OperationalError
from django.test import TestCase
from django.urls import reverse
from django.contrib.auth import get_user_model
from unittest.mock import patch

from core.views import _compute_plan_forecast_semaforo
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
