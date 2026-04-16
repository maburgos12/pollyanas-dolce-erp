from __future__ import annotations

import json
import tempfile
from datetime import date, datetime, timedelta
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import AuditLog
from core.models import Sucursal
from inventario.models import AlmacenSyncRun
from integraciones.management.commands.sync_pickup_catalog import Command as SyncPickupCatalogCommand
from maestros.models import Insumo, InsumoAlias, PointPendingMatch, UnidadMedida
from recetas.models import LineaReceta, Receta, RecetaCodigoPointAlias

from .models import PublicApiAccessLog, PublicApiClient
from .views import (
    EXECUTIVE_STATUS_CRITICAL,
    EXECUTIVE_STATUS_DELAYED,
    EXECUTIVE_STATUS_OK,
    EXECUTIVE_STATUS_PARTIAL,
    _build_executive_semaphore,
)


User = get_user_model()


class IntegracionesPanelTests(TestCase):
    def setUp(self):
        self.url = reverse("integraciones:panel")
        self.user = User.objects.create_user(username="operador", password="x")
        self.admin = User.objects.create_superuser(username="admin_integraciones", email="admin@test.local", password="x")

    def test_requires_authentication(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_requires_audit_permission(self):
        self.client.force_login(self.user)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 403)

    def test_panel_get_ok_for_admin(self):
        self.client.force_login(self.admin)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Integraciones")
        self.assertContains(response, "Centro de mando ERP")
        self.assertContains(response, "Monitor de integraciones Point")
        self.assertContains(response, "Semáforo ejecutivo del corte")
        self.assertContains(response, "Acciones manuales seguras")
        self.assertContains(response, "Schedules, ventanas y auditoría")
        self.assertContains(response, "Jobs recientes Point")
        self.assertContains(response, "Expediente ERP del módulo")
        self.assertContains(response, "Workflow ERP del módulo")
        self.assertContains(response, "Clientes API registrados")
        self.assertContains(response, "Tendencia API (7 días)")
        self.assertContains(response, "Cadena documental ERP")
        self.assertContains(response, "Cadena troncal de integraciones")
        self.assertContains(response, "Ruta crítica ERP")
        self.assertContains(response, "Radar ejecutivo ERP")
        self.assertContains(response, "Mesa de gobierno ERP")
        self.assertContains(response, "Depende de")
        self.assertContains(response, "Dependencia")
        self.assertContains(response, "Madurez ERP de integraciones")
        self.assertContains(response, "Criterios de cierre ERP")
        self.assertContains(response, "Cierre global")
        self.assertContains(response, "Cadena de control de integraciones")
        self.assertContains(response, "Entrega de integraciones a downstream")
        self.assertContains(response, "Salud operativa ERP")
        self.assertContains(response, "Cierre por etapa documental")
        self.assertContains(response, "Responsable")
        self.assertContains(response, "Cierre")
        self.assertIn("enterprise_chain", response.context)
        self.assertIn("integraciones_critical_path_rows", response.context)
        self.assertIn("dependency_status", response.context["enterprise_chain"][0])
        self.assertIn("owner", response.context["document_stage_rows"][0])
        self.assertIn("completion", response.context["document_stage_rows"][0])
        self.assertIn("integraciones_maturity_summary", response.context)
        self.assertIn("erp_command_center", response.context)
        self.assertIn("integraciones_handoff_map", response.context)
        self.assertIn("owner", response.context["integraciones_handoff_map"][0])
        self.assertIn("depends_on", response.context["integraciones_handoff_map"][0])
        self.assertIn("exit_criteria", response.context["integraciones_handoff_map"][0])
        self.assertIn("next_step", response.context["integraciones_handoff_map"][0])
        self.assertIn("completion", response.context["integraciones_handoff_map"][0])
        self.assertIn("operational_health_cards", response.context)
        self.assertIn("document_stage_rows", response.context)
        self.assertIn("erp_governance_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        self.assertIn("release_gate_rows", response.context)
        self.assertIn("release_gate_completion", response.context)
        self.assertIn("integration_monitor", response.context)
        self.assertIn("executive_semaphore", response.context["integration_monitor"])
        self.assertIn("sales_cards", response.context["integration_monitor"])
        self.assertIn("pipeline_rows", response.context["integration_monitor"])
        self.assertIn("schedule_rows", response.context["integration_monitor"])

    def test_create_client_shows_generated_key_once(self):
        self.client.force_login(self.admin)
        response = self.client.post(
            self.url,
            {"action": "create", "nombre": "ERP Point", "descripcion": "Integracion POS"},
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(PublicApiClient.objects.count(), 1)
        self.assertTrue(response.context["last_generated_api_key"])
        audit = AuditLog.objects.filter(action="CREATE", model="integraciones.PublicApiClient").first()
        self.assertIsNotNone(audit)
        self.assertEqual(audit.user, self.admin)
        self.assertEqual(audit.payload.get("nombre"), "ERP Point")

        second = self.client.get(self.url)
        self.assertEqual(second.context["last_generated_api_key"], "")

    def test_rotate_client_key_changes_hash_and_prefix(self):
        client, _raw = PublicApiClient.create_with_generated_key(nombre="ERP Mobile", descripcion="")
        old_hash = client.clave_hash
        old_prefix = client.clave_prefijo

        self.client.force_login(self.admin)
        response = self.client.post(
            self.url,
            {"action": "rotate", "client_id": str(client.id)},
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        client.refresh_from_db()
        self.assertNotEqual(client.clave_hash, old_hash)
        self.assertNotEqual(client.clave_prefijo, old_prefix)
        self.assertTrue(response.context["last_generated_api_key"])
        self.assertTrue(
            AuditLog.objects.filter(
                action="ROTATE_KEY",
                model="integraciones.PublicApiClient",
                object_id=str(client.id),
            ).exists()
        )

    def test_toggle_client_active_flag(self):
        client, _raw = PublicApiClient.create_with_generated_key(nombre="ERP Sucursal", descripcion="")
        self.assertTrue(client.activo)

        self.client.force_login(self.admin)
        self.client.post(self.url, {"action": "toggle", "client_id": str(client.id)})
        client.refresh_from_db()
        self.assertFalse(client.activo)

        self.client.post(self.url, {"action": "toggle", "client_id": str(client.id)})
        client.refresh_from_db()
        self.assertTrue(client.activo)
        self.assertEqual(
            AuditLog.objects.filter(
                action="TOGGLE_ACTIVE",
                model="integraciones.PublicApiClient",
                object_id=str(client.id),
            ).count(),
            2,
        )

    def test_deactivate_idle_clients_action(self):
        active_idle, _ = PublicApiClient.create_with_generated_key(nombre="ERP IDLE", descripcion="")
        active_recent, _ = PublicApiClient.create_with_generated_key(nombre="ERP ACTIVE", descripcion="")
        PublicApiAccessLog.objects.create(
            client=active_recent,
            endpoint="/api/public/v1/ping/",
            method="GET",
            status_code=200,
        )
        PublicApiAccessLog.objects.filter(client=active_recent).update(created_at=timezone.now() - timedelta(days=2))

        self.client.force_login(self.admin)
        response = self.client.post(
            self.url,
            {
                "action": "deactivate_idle_clients",
                "idle_days": "30",
                "idle_limit": "100",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        active_idle.refresh_from_db()
        active_recent.refresh_from_db()
        self.assertFalse(active_idle.activo)
        self.assertTrue(active_recent.activo)
        self.assertTrue(
            AuditLog.objects.filter(
                action="DEACTIVATE_IDLE_API_CLIENTS",
                model="integraciones.PublicApiClient",
            ).exists()
        )

    def test_purge_api_logs_action_respects_retention_and_limit(self):
        client, _ = PublicApiClient.create_with_generated_key(nombre="ERP PURGE", descripcion="")
        recent = PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/recent-purge/",
            method="GET",
            status_code=200,
        )
        old_1 = PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/old-purge-1/",
            method="GET",
            status_code=200,
        )
        old_2 = PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/old-purge-2/",
            method="GET",
            status_code=500,
        )
        PublicApiAccessLog.objects.filter(id=recent.id).update(created_at=timezone.now() - timedelta(days=5))
        PublicApiAccessLog.objects.filter(id__in=[old_1.id, old_2.id]).update(created_at=timezone.now() - timedelta(days=120))

        self.client.force_login(self.admin)
        response = self.client.post(
            self.url,
            {
                "action": "purge_api_logs",
                "retain_days": "90",
                "max_delete": "1",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            PublicApiAccessLog.objects.filter(created_at__lt=timezone.now() - timedelta(days=90)).count(),
            1,
        )
        self.assertTrue(PublicApiAccessLog.objects.filter(id=recent.id).exists())
        self.assertTrue(
            AuditLog.objects.filter(
                action="PURGE_API_LOGS",
                model="integraciones.PublicApiAccessLog",
            ).exists()
        )

    def test_recent_logs_are_rendered(self):
        client, _raw = PublicApiClient.create_with_generated_key(nombre="ERP Logs", descripcion="")
        PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/insumos/",
            method="GET",
            status_code=200,
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/api/public/v1/insumos/")

    @patch("pos_bridge.tasks.celery_tasks.task_analytics_refresh_cycle.delay")
    def test_manual_analytics_refresh_is_queued_and_audited(self, delay_mock):
        cache.delete("integraciones:analytics-refresh-lock")
        self.client.force_login(self.admin)
        response = self.client.post(
            self.url,
            {
                "action": "refresh_analytics_monitor",
                "reference_date": "2026-04-05",
                "lookback_days": "7",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        delay_mock.assert_called_once_with(
            reference_date_iso="2026-04-05",
            lookback_days=7,
            triggered_by_id=self.admin.id,
        )
        audit = AuditLog.objects.filter(action="INTEGRATIONS_ANALYTICS_REFRESH_REQUESTED").latest("timestamp")
        self.assertEqual(audit.payload["scope"], "analytics_only")
        self.assertEqual(audit.payload["reference_date"], "2026-04-05")
        self.assertEqual(cache.get("integraciones:analytics-refresh-lock"), "2026-04-05")
        cache.delete("integraciones:analytics-refresh-lock")

    @patch("pos_bridge.tasks.celery_tasks.task_operations_automation_cycle.delay")
    def test_manual_operations_refresh_is_queued_and_audited(self, delay_mock):
        cache.delete("integraciones:operational-refresh-lock")
        branch = Sucursal.objects.create(codigo="TEST_BRANCH", nombre="Sucursal test", activa=True)
        self.client.force_login(self.admin)
        response = self.client.post(
            self.url,
            {
                "action": "refresh_operations_monitor",
                "reference_date": "2026-04-05",
                "lookback_days": "5",
                "sucursal_id": str(branch.id),
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        delay_mock.assert_called_once_with(
            reference_date_iso="2026-04-05",
            lookback_days=5,
            sucursal_id=branch.id,
            triggered_by_id=self.admin.id,
        )
        audit = AuditLog.objects.filter(action="INTEGRATIONS_OPERATIONAL_REFRESH_REQUESTED").latest("timestamp")
        self.assertEqual(audit.payload["scope"], "operations_cycle")
        self.assertEqual(audit.payload["sucursal_id"], branch.id)
        self.assertEqual(cache.get("integraciones:operational-refresh-lock"), "2026-04-05")
        cache.delete("integraciones:operational-refresh-lock")

    def test_logs_filters_by_status_and_client(self):
        client_ok, _ = PublicApiClient.create_with_generated_key(nombre="ERP OK", descripcion="")
        client_err, _ = PublicApiClient.create_with_generated_key(nombre="ERP ERR", descripcion="")
        PublicApiAccessLog.objects.create(
            client=client_ok,
            endpoint="/api/public/v1/insumos/",
            method="GET",
            status_code=200,
        )
        PublicApiAccessLog.objects.create(
            client=client_err,
            endpoint="/api/public/v1/pedidos/",
            method="POST",
            status_code=500,
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url, {"client": str(client_err.id), "status": "error"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/api/public/v1/pedidos/")
        self.assertNotContains(response, "/api/public/v1/insumos/")


    def test_logs_filters_by_date_range(self):
        client, _ = PublicApiClient.create_with_generated_key(nombre="ERP FECHA", descripcion="")
        recent = PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/recent/",
            method="GET",
            status_code=200,
        )
        old = PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/old/",
            method="GET",
            status_code=200,
        )
        PublicApiAccessLog.objects.filter(id=recent.id).update(created_at=timezone.now() - timedelta(hours=1))
        PublicApiAccessLog.objects.filter(id=old.id).update(created_at=timezone.now() - timedelta(days=10))

        self.client.force_login(self.admin)
        from_date = (timezone.localdate() - timedelta(days=1)).isoformat()
        response = self.client.get(self.url, {"from": from_date})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/api/public/v1/recent/")
        self.assertNotContains(response, "/api/public/v1/old/")

    def test_logs_csv_export(self):
        client, _ = PublicApiClient.create_with_generated_key(nombre="ERP CSV", descripcion="")
        PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/resumen/",
            method="GET",
            status_code=200,
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url, {"export": "csv"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        body = response.content.decode("utf-8")
        self.assertIn("endpoint", body)
        self.assertIn("/api/public/v1/resumen/", body)

    def test_audit_csv_export(self):
        self.client.force_login(self.admin)
        self.client.post(
            self.url,
            {"action": "create", "nombre": "ERP Audit CSV", "descripcion": "Integracion"},
            follow=True,
        )
        response = self.client.get(self.url, {"export": "audit_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        body = response.content.decode("utf-8")
        self.assertIn("accion", body)
        self.assertIn("integraciones.PublicApiClient", body)

    def test_health_csv_export(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-CSV-01",
            point_nombre="Insumo CSV",
            fuzzy_score=88.0,
            fuzzy_sugerencia="Insumo CSV",
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url, {"export": "health_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        body = response.content.decode("utf-8")
        self.assertIn("point_pending_total", body)
        self.assertIn("alerta_nivel", body)
        self.assertIn("errors_prev_24h", body)

    def test_errors_csv_export(self):
        client_a, _ = PublicApiClient.create_with_generated_key(nombre="ERP A", descripcion="")
        client_b, _ = PublicApiClient.create_with_generated_key(nombre="ERP B", descripcion="")
        PublicApiAccessLog.objects.create(
            client=client_a,
            endpoint="/api/public/v1/insumos/",
            method="GET",
            status_code=500,
        )
        PublicApiAccessLog.objects.create(
            client=client_b,
            endpoint="/api/public/v1/insumos/",
            method="GET",
            status_code=429,
        )
        PublicApiAccessLog.objects.create(
            client=client_b,
            endpoint="/api/public/v1/pedidos/",
            method="POST",
            status_code=503,
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url, {"export": "errors_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        body = response.content.decode("utf-8")
        self.assertIn("total_errores_24h", body)
        self.assertIn("/api/public/v1/insumos/", body)
        self.assertIn("ERP B", body)

    def test_trend_csv_export(self):
        client, _ = PublicApiClient.create_with_generated_key(nombre="ERP TREND", descripcion="")
        PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/tendencia/",
            method="GET",
            status_code=200,
        )
        PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/tendencia/",
            method="GET",
            status_code=500,
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url, {"export": "trend_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        body = response.content.decode("utf-8")
        self.assertIn("error_rate_pct", body)
        self.assertIn(str(timezone.localdate()), body)

    def test_clients_csv_export(self):
        client, _ = PublicApiClient.create_with_generated_key(nombre="ERP CLIENT CSV", descripcion="")
        PublicApiAccessLog.objects.create(
            client=client,
            endpoint="/api/public/v1/clientes/",
            method="GET",
            status_code=200,
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url, {"export": "clients_csv"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        body = response.content.decode("utf-8")
        self.assertIn("requests_30d", body)
        self.assertIn("ERP CLIENT CSV", body)

    def test_homologacion_summary_blocks_render(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=1000,
        )
        insumo = Insumo.objects.create(nombre="Azucar test", unidad_base=unidad, activo=True, codigo_point="")
        receta = Receta.objects.create(nombre="Receta demo", hash_contenido="hash-integraciones-panel-001")
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-001",
            point_nombre="Azucar point",
            fuzzy_score=92,
            fuzzy_sugerencia=insumo.nombre,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Azucar point",
            cantidad=1,
            unidad=unidad,
            unidad_texto="kg",
            match_status=LineaReceta.STATUS_NEEDS_REVIEW,
            match_method=LineaReceta.MATCH_FUZZY,
            match_score=80,
        )
        AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_MANUAL,
            status=AlmacenSyncRun.STATUS_OK,
            pending_preview=[
                {
                    "nombre_origen": "Azucar point",
                    "nombre_normalizado": "azucar point",
                    "suggestion": "Azucar test",
                    "score": 95,
                }
            ],
        )

        self.client.force_login(self.admin)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Centro de conexiones operativas")
        self.assertContains(response, "Observaciones de catálogo comercial")
        self.assertContains(response, "Componentes de receta en revisión")
        self.assertContains(response, "01 · Integración comercial")
        self.assertContains(response, "Bitácora de acciones (Integraciones)")

    def test_mass_resolve_point_pending_insumos_from_panel(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=1000,
        )
        insumo = Insumo.objects.create(nombre="Harina Trigo", unidad_base=unidad, activo=True)
        pending_ok = PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-500",
            point_nombre="Harina Trigo Point",
            fuzzy_score=95.0,
            fuzzy_sugerencia=insumo.nombre,
        )
        pending_low_score = PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-501",
            point_nombre="Harina Trigo Secundaria",
            fuzzy_score=40.0,
            fuzzy_sugerencia=insumo.nombre,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            self.url,
            {
                "action": "resolve_point_sugerencias_insumos",
                "auto_score_min": "90",
                "auto_limit": "100",
                "create_aliases": "on",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Aplicar resoluciones sugeridas")

        insumo.refresh_from_db()
        self.assertEqual(insumo.codigo_point, "PT-INS-500")
        self.assertEqual(insumo.nombre_point, "Harina Trigo Point")
        self.assertFalse(PointPendingMatch.objects.filter(id=pending_ok.id).exists())
        self.assertTrue(PointPendingMatch.objects.filter(id=pending_low_score.id).exists())
        self.assertTrue(
            InsumoAlias.objects.filter(
                insumo=insumo,
                nombre_normalizado="harina trigo point",
            ).exists()
        )
        self.assertTrue(
            AuditLog.objects.filter(
                action="AUTO_RESOLVE_POINT_INSUMOS",
                model="maestros.PointPendingMatch",
            ).exists()
        )

    def test_operational_alerts_render_when_point_pending_exists(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-ALERTA-1",
            point_nombre="Insumo alerta",
            fuzzy_score=80.0,
            fuzzy_sugerencia="Insumo alerta",
        )
        self.client.force_login(self.admin)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Alertas operativas")
        self.assertContains(response, "Observaciones de catálogo comercial")


class ExecutiveSemaphoreTests(TestCase):
    def _build_cards(self, *, point_date, fact_date, dashboard_date, authoritative_date=None):
        return [
            {"key": "point_daily_sale", "latest_date": point_date},
            {"key": "point_sales_v2", "latest_date": point_date},
            {"key": "authoritative_sales", "latest_date": authoritative_date or point_date},
            {"key": "fact_venta_diaria", "latest_date": fact_date},
            {"key": "dashboard_materialized", "latest_date": dashboard_date},
        ]

    def _schedule_rows(self):
        return [
            {"key": "ventas", "schedule_label": "01:30", "enabled": True},
            {"key": "analytics", "schedule_label": "03:35", "enabled": True},
        ]

    def test_executive_semaphore_marks_ok_when_cut_is_loaded(self):
        result = _build_executive_semaphore(
            reference_date=date(2026, 4, 7),
            now=timezone.make_aware(datetime(2026, 4, 7, 6, 0)),
            sales_cards=self._build_cards(
                point_date=date(2026, 4, 6),
                fact_date=date(2026, 4, 6),
                dashboard_date=date(2026, 4, 6),
            ),
            pipeline_rows=[],
            pending_windows=[],
            quality_alerts=[],
            recent_point_jobs=[],
            analytic_audits=[],
            schedule_rows=self._schedule_rows(),
        )
        self.assertEqual(result["status_code"], EXECUTIVE_STATUS_OK)
        self.assertEqual(result["recommended_action"]["kind"], "none")

    def test_executive_semaphore_marks_partial_inside_normal_window(self):
        result = _build_executive_semaphore(
            reference_date=date(2026, 4, 7),
            now=timezone.make_aware(datetime(2026, 4, 7, 2, 15)),
            sales_cards=self._build_cards(
                point_date=date(2026, 4, 5),
                fact_date=date(2026, 4, 5),
                dashboard_date=date(2026, 4, 5),
            ),
            pipeline_rows=[],
            pending_windows=[],
            quality_alerts=[],
            recent_point_jobs=[],
            analytic_audits=[],
            schedule_rows=self._schedule_rows(),
        )
        self.assertEqual(result["status_code"], EXECUTIVE_STATUS_PARTIAL)
        self.assertEqual(result["recommended_action"]["kind"], "wait")

    def test_executive_semaphore_marks_technical_delay_when_dashboard_lags(self):
        result = _build_executive_semaphore(
            reference_date=date(2026, 4, 7),
            now=timezone.make_aware(datetime(2026, 4, 7, 6, 0)),
            sales_cards=self._build_cards(
                point_date=date(2026, 4, 6),
                fact_date=date(2026, 4, 6),
                dashboard_date=date(2026, 4, 5),
            ),
            pipeline_rows=[],
            pending_windows=[],
            quality_alerts=[],
            recent_point_jobs=[],
            analytic_audits=[],
            schedule_rows=self._schedule_rows(),
        )
        self.assertEqual(result["status_code"], EXECUTIVE_STATUS_DELAYED)
        self.assertEqual(result["blocking_layer"], "Dashboard materializado")
        self.assertEqual(result["recommended_action"]["kind"], "refresh_analytics")

    def test_executive_semaphore_marks_critical_when_recent_sales_failure_blocks_cut(self):
        failing_job = type(
            "Job",
            (),
            {
                "job_type": "sales",
                "status": "FAILED",
                "started_at": timezone.make_aware(datetime(2026, 4, 7, 5, 10)),
                "finished_at": timezone.make_aware(datetime(2026, 4, 7, 5, 20)),
                "error_message": "Point timeout",
            },
        )()
        result = _build_executive_semaphore(
            reference_date=date(2026, 4, 7),
            now=timezone.make_aware(datetime(2026, 4, 7, 6, 0)),
            sales_cards=self._build_cards(
                point_date=date(2026, 4, 5),
                fact_date=date(2026, 4, 5),
                dashboard_date=date(2026, 4, 5),
            ),
            pipeline_rows=[],
            pending_windows=[],
            quality_alerts=[],
            recent_point_jobs=[failing_job],
            analytic_audits=[],
            schedule_rows=self._schedule_rows(),
        )
        self.assertEqual(result["status_code"], EXECUTIVE_STATUS_CRITICAL)
        self.assertEqual(result["blocking_layer"], "Point ventas")
        self.assertEqual(result["recommended_action"]["kind"], "refresh_operations")


class SyncPickupCatalogCommandTests(TestCase):
    def setUp(self):
        self.direct_recipe = Receta.objects.create(
            nombre="Pay de Platano Grande",
            codigo_point="0005",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-direct",
        )
        self.alias_recipe = Receta.objects.create(
            nombre="Pay de Queso Grande",
            codigo_point="ERP-PAY-G",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-alias",
        )
        self.blank_recipe = Receta.objects.create(
            nombre="Cheesecake Lotus",
            codigo_point="",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-blank",
        )

    def test_sync_pickup_catalog_maps_direct_alias_and_missing_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            report_path = f"{tmp_dir}/pickup_catalog_report.json"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "1,Pay de Platano Grande,pay-de-platano-grande,0005,0005,10.0,True,PENDING_ERP_MATCH\n"
                    "2,Pay de Queso Grande,pay-de-queso-grande,0001,0001,10.0,True,PENDING_ERP_MATCH\n"
                    "3,Cheesecake Lotus,cheesecake-lotus,PD-CHL-001,PD-CHL-001,10.0,True,PENDING_ERP_MATCH\n"
                    "4,Producto Faltante,producto-faltante,MISS-001,MISS-001,10.0,True,PENDING_ERP_MATCH\n"
                )

            stdout = StringIO()
            call_command("sync_pickup_catalog", csv_path, "--report-path", report_path, stdout=stdout)
            payload = json.loads(stdout.getvalue())
            with open(report_path, encoding="utf-8") as report_file:
                report_payload = json.load(report_file)

        self.blank_recipe.refresh_from_db()
        self.assertEqual(self.blank_recipe.codigo_point, "PD-CHL-001")
        self.assertEqual(
            RecetaCodigoPointAlias.objects.filter(receta=self.alias_recipe, codigo_point="0001", activo=True).count(),
            1,
        )
        self.assertEqual(payload["counts"]["direct_mappings"], 2)
        self.assertEqual(payload["counts"]["alias_mappings"], 1)
        self.assertEqual(payload["counts"]["missing_in_erp"], 1)
        self.assertTrue(AuditLog.objects.filter(action="IMPORT", model="integraciones.PickupCatalogSync").exists())
        self.assertEqual(report_payload["counts"]["direct_mappings"], 2)
        self.assertEqual(report_payload["counts"]["alias_mappings"], 1)
        self.assertEqual(report_payload["counts"]["missing_in_erp"], 1)

    def test_sync_pickup_catalog_is_idempotent_for_alias_creation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "2,Pay de Queso Grande,pay-de-queso-grande,0001,0001,10.0,True,PENDING_ERP_MATCH\n"
                )

            call_command("sync_pickup_catalog", csv_path, stdout=StringIO())
            call_command("sync_pickup_catalog", csv_path, stdout=StringIO())

        self.assertEqual(
            RecetaCodigoPointAlias.objects.filter(receta=self.alias_recipe, codigo_point="0001").count(),
            1,
        )

    def test_sync_pickup_catalog_retargets_existing_aliases_by_safe_name_variants(self):
        wrong_recipe = Receta.objects.create(
            nombre="Cheesecakes Tortuga Individual",
            codigo_point="CHEESETORTIND",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-wrong-alias",
        )
        target_lotus = Receta.objects.create(
            nombre="Cheesecakes Lotus Individual",
            codigo_point="01CHLI01",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-lotus",
        )
        target_rebanada = Receta.objects.create(
            nombre="Pastel 3 Pecados - Rebanada",
            codigo_point="",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-rebanada",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=wrong_recipe,
            codigo_point="CHCAKEINDLOTUS",
            nombre_point="Cheesecake Lotus Individual",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=self.direct_recipe,
            codigo_point="0110",
            nombre_point="Pastel de 3 Pecados R",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "1,Cheesecake Lotus Individual,cheesecake-lotus-individual,CHCAKEINDLOTUS,CHCAKEINDLOTUS,10.0,True,PENDING_ERP_MATCH\n"
                    "2,Pastel de 3 Pecados R,pastel-3-pecados-r,0110,0110,10.0,True,PENDING_ERP_MATCH\n"
                )

            stdout = StringIO()
            call_command("sync_pickup_catalog", csv_path, stdout=stdout)
            payload = json.loads(stdout.getvalue())

        lotus_alias = RecetaCodigoPointAlias.objects.get(codigo_point="CHCAKEINDLOTUS")
        rebanada_alias = RecetaCodigoPointAlias.objects.get(codigo_point="0110")
        target_rebanada.refresh_from_db()

        self.assertEqual(lotus_alias.receta_id, target_lotus.id)
        self.assertEqual(rebanada_alias.receta_id, target_rebanada.id)
        self.assertEqual(target_rebanada.codigo_point, "0110")
        self.assertEqual(payload["counts"]["direct_mappings"], 1)
        self.assertEqual(payload["counts"]["alias_mappings"], 1)

    def test_sync_pickup_catalog_prefers_strict_name_match_for_lotus_variants(self):
        generic_lotus = Receta.objects.create(
            nombre="Pastel 3 Leches Lotus - Chico",
            codigo_point="5430",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-lotus-generic",
        )
        exact_lotus = Receta.objects.create(
            nombre="Pastel Lotus - Chico",
            codigo_point="",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-lotus-exact",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=generic_lotus,
            codigo_point="6423",
            nombre_point="Pastel Lotus Chico",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "1,Pastel Lotus Chico,pastel-lotus-chico,6423,6423,10.0,True,PENDING_ERP_MATCH\n"
                )

            stdout = StringIO()
            call_command("sync_pickup_catalog", csv_path, stdout=stdout)
            payload = json.loads(stdout.getvalue())

        exact_lotus.refresh_from_db()
        lotus_alias = RecetaCodigoPointAlias.objects.get(codigo_point="6423")
        self.assertEqual(exact_lotus.codigo_point, "6423")
        self.assertEqual(lotus_alias.receta_id, exact_lotus.id)
        self.assertEqual(payload["counts"]["direct_mappings"], 1)
        self.assertEqual(payload["counts"]["alias_mappings"], 0)

    def test_sync_pickup_catalog_retypes_strict_name_match_from_preparacion(self):
        preparacion = Receta.objects.create(
            nombre="Galleta Lotus",
            codigo_point="5638",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="hash-pickup-galleta-lotus",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=preparacion,
            codigo_point="5638",
            nombre_point="Galleta Lotus",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "1,Galleta Lotus,galleta-lotus,5638,5638,10.0,True,PENDING_ERP_MATCH\n"
                )

            stdout = StringIO()
            call_command("sync_pickup_catalog", csv_path, stdout=stdout)
            payload = json.loads(stdout.getvalue())

        preparacion.refresh_from_db()
        self.assertEqual(preparacion.tipo, Receta.TIPO_PRODUCTO_FINAL)
        self.assertEqual(payload["counts"]["direct_mappings"], 1)
        self.assertEqual(payload["direct_mappings"][0]["store_code"], "5638")

    def test_sync_pickup_catalog_retypes_loose_subset_match_from_preparacion(self):
        preparacion = Receta.objects.create(
            nombre="Galleta Red Velvet Mejorada 2025",
            codigo_point="0126",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="hash-pickup-galleta-red-velvet",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=preparacion,
            codigo_point="0126",
            nombre_point="Galleta Red Velvet",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "1,Galleta Red Velvet,galleta-red-velvet,0126,0126,10.0,True,PENDING_ERP_MATCH\n"
                )

            stdout = StringIO()
            call_command("sync_pickup_catalog", csv_path, stdout=stdout)
            payload = json.loads(stdout.getvalue())

        preparacion.refresh_from_db()
        self.assertEqual(preparacion.tipo, Receta.TIPO_PRODUCTO_FINAL)
        self.assertEqual(payload["counts"]["direct_mappings"], 1)
        self.assertEqual(payload["direct_mappings"][0]["store_code"], "0126")

    def test_sync_pickup_catalog_retypes_loose_subset_direct_preparacion(self):
        preparacion = Receta.objects.create(
            nombre="Galleta Red Velvet Mejorada 2025",
            codigo_point="0126",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="hash-pickup-galleta-red-velvet-direct",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "1,Galleta Red Velvet,galleta-red-velvet,0126,0126,10.0,True,PENDING_ERP_MATCH\n"
                )

            stdout = StringIO()
            call_command("sync_pickup_catalog", csv_path, stdout=stdout)
            payload = json.loads(stdout.getvalue())

        preparacion.refresh_from_db()
        self.assertEqual(preparacion.tipo, Receta.TIPO_PRODUCTO_FINAL)
        self.assertEqual(payload["counts"]["direct_mappings"], 1)
        self.assertEqual(payload["direct_mappings"][0]["notes"], "retyped_preparacion_direct_to_producto_final")

    def test_sync_pickup_catalog_deactivates_invalid_existing_alias(self):
        wrong_recipe = Receta.objects.create(
            nombre="Pastel Fresas Con Crema - Chico",
            codigo_point="0101",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pickup-invalid-alias",
        )
        alias = RecetaCodigoPointAlias.objects.create(
            receta=wrong_recipe,
            codigo_point="0145",
            nombre_point="Vaso Fresas con Crema Chico",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = f"{tmp_dir}/pickup_catalog.csv"
            with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
                csv_file.write(
                    "product_id,name,slug,internal_code,sku,price,is_active,erp_status\n"
                    "1,Vaso Fresas con Crema Chico,vaso-fresas-con-crema-chico,0145,0145,10.0,True,PENDING_ERP_MATCH\n"
                )

            stdout = StringIO()
            call_command("sync_pickup_catalog", csv_path, stdout=stdout)
            payload = json.loads(stdout.getvalue())

        alias.refresh_from_db()
        self.assertFalse(alias.activo)
        self.assertEqual(payload["counts"]["missing_in_erp"], 1)
        self.assertEqual(payload["missing_in_erp"][0]["notes"], "invalid_existing_alias_deactivated")


class IntegracionesMaintenanceCommandTests(TestCase):
    def _seed_idle_and_old(self):
        idle_client, _ = PublicApiClient.create_with_generated_key(nombre="Cmd Idle", descripcion="")
        recent_client, _ = PublicApiClient.create_with_generated_key(nombre="Cmd Recent", descripcion="")
        PublicApiAccessLog.objects.create(
            client=recent_client,
            endpoint="/api/public/v1/cmd-recent/",
            method="GET",
            status_code=200,
        )
        PublicApiAccessLog.objects.filter(client=recent_client).update(created_at=timezone.now() - timedelta(days=2))
        old_log = PublicApiAccessLog.objects.create(
            client=idle_client,
            endpoint="/api/public/v1/cmd-old/",
            method="GET",
            status_code=500,
        )
        PublicApiAccessLog.objects.filter(id=old_log.id).update(created_at=timezone.now() - timedelta(days=140))
        return idle_client, recent_client, old_log

    def test_run_integraciones_maintenance_dry_run(self):
        idle_client, recent_client, old_log = self._seed_idle_and_old()
        out = StringIO()
        call_command(
            "run_integraciones_maintenance",
            dry_run=True,
            idle_days=30,
            idle_limit=100,
            retain_days=90,
            max_delete=5000,
            stdout=out,
        )
        idle_client.refresh_from_db()
        recent_client.refresh_from_db()
        self.assertTrue(idle_client.activo)
        self.assertTrue(recent_client.activo)
        self.assertTrue(PublicApiAccessLog.objects.filter(id=old_log.id).exists())
        audit = AuditLog.objects.filter(action="PREVIEW_RUN_API_MAINTENANCE", model="integraciones.Operaciones").first()
        self.assertIsNotNone(audit)
        self.assertEqual((audit.payload or {}).get("source"), "CLI")
        self.assertIn('"dry_run": true', out.getvalue().lower())

    def test_run_integraciones_maintenance_live_requires_confirm(self):
        with self.assertRaises(CommandError):
            call_command("run_integraciones_maintenance", dry_run=False)

    def test_run_integraciones_maintenance_live(self):
        admin = User.objects.create_superuser(username="cmd_admin", email="cmd_admin@test.local", password="x")
        idle_client, recent_client, old_log = self._seed_idle_and_old()
        call_command(
            "run_integraciones_maintenance",
            idle_days=30,
            idle_limit=100,
            retain_days=90,
            max_delete=5000,
            confirm_live="YES",
            actor_username=admin.username,
            stdout=StringIO(),
        )
        idle_client.refresh_from_db()
        recent_client.refresh_from_db()
        self.assertFalse(idle_client.activo)
        self.assertTrue(recent_client.activo)
        self.assertFalse(PublicApiAccessLog.objects.filter(id=old_log.id).exists())
        audit = AuditLog.objects.filter(action="RUN_API_MAINTENANCE", model="integraciones.Operaciones").first()
        self.assertIsNotNone(audit)
        self.assertEqual(audit.user_id, admin.id)
        self.assertEqual((audit.payload or {}).get("source"), "CLI")


class SyncPickupCatalogBranchStatusTests(TestCase):
    def test_build_branch_status_report_uses_operational_branch_helper(self):
        Sucursal.objects.create(codigo="COL", nombre="Colosio", activa=True)
        Sucursal.objects.create(
            codigo="COLOSIO",
            nombre="Colosio",
            activa=True,
            fecha_apertura=timezone.localdate() + timedelta(days=2),
        )

        rows = SyncPickupCatalogCommand()._build_branch_status_report(freshness_seconds=60)
        colosio_row = next(row for row in rows if row["branch_code"] == "COLOSIO")

        self.assertFalse(colosio_row["erp_branch_found"])
