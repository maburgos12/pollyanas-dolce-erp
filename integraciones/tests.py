from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import AuditLog
from inventario.models import AlmacenSyncRun
from maestros.models import Insumo, PointPendingMatch, UnidadMedida
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
