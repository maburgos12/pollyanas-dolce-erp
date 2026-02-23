from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import AuditLog
from inventario.models import AlmacenSyncRun
from maestros.models import Insumo, InsumoAlias, PointPendingMatch, UnidadMedida
from recetas.models import LineaReceta, Receta

from .models import PublicApiAccessLog, PublicApiClient


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
        self.assertContains(response, "Clientes API registrados")
        self.assertContains(response, "Tendencia API (7 días)")

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
        self.assertContains(response, "Homologacion Point y Match Operativo")
        self.assertContains(response, "Point pendientes")
        self.assertContains(response, "Recetas sin match")
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
        self.assertContains(response, "Auto-resolución de pendientes Point (insumos)")

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
        self.assertContains(response, "Pendientes Point abiertos")
