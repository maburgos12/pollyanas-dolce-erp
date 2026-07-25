from django.contrib.auth.models import User
from django.core.cache import cache
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import AuditLog, Notificacion, Sucursal
from integraciones.models import PublicApiAccessLog, PublicApiClient
from logistica.models import EntregaEcommerce, Repartidor, SolicitudDomicilio


class PublicLogisticaAssignmentApiTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.api_client, self.raw_key = PublicApiClient.create_with_generated_key(
            nombre="Maya logística",
        )
        self.api_client.capabilities = [
            PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
        ]
        self.api_client.save(update_fields=["capabilities"])
        self.auth = {"HTTP_X_API_KEY": self.raw_key}
        self.sucursal = Sucursal.objects.create(
            codigo="M2M-LOG",
            nombre="M2M Logística",
        )
        self.solicitud = SolicitudDomicilio.objects.create(
            cliente_nombre="Cliente M2M",
            direccion="Calle M2M 10",
        )
        self.catalog_url = reverse(
            "api_public_logistica_repartidores_disponibles"
        )
        self.assign_url = reverse(
            "api_public_logistica_domicilio_asignar",
            args=[self.solicitud.id],
        )

    def _repartidor(self, username):
        user = User.objects.create_user(username=username, password="pass123")
        return Repartidor.objects.create(user=user, sucursal=self.sucursal)

    def _payload(self, repartidor):
        return {
            "repartidor_id": repartidor.id,
            "actor": {"id": "ops-17", "nombre": "Operaciones"},
        }

    def test_requiere_api_key_valida_y_capability_opt_in(self):
        missing = self.client.get(self.catalog_url)
        invalid = self.client.get(
            self.catalog_url,
            HTTP_X_API_KEY="pk_invalid",
        )
        capture_client, capture_key = PublicApiClient.create_with_generated_key(
            nombre="Captura omnicanal",
        )
        capture_client.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        capture_client.save(update_fields=["capabilities"])
        forbidden = self.client.get(
            self.catalog_url,
            HTTP_X_API_KEY=capture_key,
        )
        forbidden_assignment = self.client.post(
            self.assign_url,
            {
                "repartidor_id": 1,
                "actor": {"id": "ops-1", "nombre": "Operaciones"},
            },
            format="json",
            HTTP_X_API_KEY=capture_key,
        )

        self.assertEqual(missing.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(invalid.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(forbidden.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            forbidden_assignment.status_code,
            status.HTTP_403_FORBIDDEN,
        )
        self.assertEqual(
            forbidden.data["code"],
            "LOGISTICA_ASSIGNMENT_CAPABILITY_REQUIRED",
        )
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=capture_client,
                status_code=status.HTTP_403_FORBIDDEN,
            ).count(),
            2,
        )
        self.solicitud.refresh_from_db()
        self.assertIsNone(self.solicitud.repartidor_id)

    def test_catalogo_scoped_reutiliza_reglas_y_envuelve_results(self):
        disponible = self._repartidor("m2m_disponible")
        inactivo = self._repartidor("m2m_inactivo")
        inactivo.user.is_active = False
        inactivo.user.save(update_fields=["is_active"])

        response = self.client.get(self.catalog_url, **self.auth)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            [item["id"] for item in response.data["results"]],
            [disponible.id],
        )
        self.assertEqual(set(response.data), {"results"})
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=self.api_client,
                endpoint=self.catalog_url,
                method="GET",
                status_code=status.HTTP_200_OK,
            ).count(),
            1,
        )

    def test_asignacion_retry_y_reasignacion_auditada_sin_pii_excesiva(self):
        primero = self._repartidor("m2m_primero")
        segundo = self._repartidor("m2m_segundo")
        payload = self._payload(primero)
        payload["actor"]["email"] = "no-guardar@example.com"
        payload["actor"]["telefono"] = "6671234567"

        first = self.client.post(
            self.assign_url,
            payload,
            format="json",
            **self.auth,
        )
        repeated = self.client.post(
            self.assign_url,
            payload,
            format="json",
            **self.auth,
        )
        reassigned = self.client.post(
            self.assign_url,
            self._payload(segundo),
            format="json",
            **self.auth,
        )

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertFalse(first.data["idempotent"])
        self.assertEqual(repeated.status_code, status.HTTP_200_OK)
        self.assertTrue(repeated.data["idempotent"])
        self.assertEqual(reassigned.status_code, status.HTTP_200_OK)
        logs = AuditLog.objects.filter(
            model="logistica.SolicitudDomicilio",
            object_id=str(self.solicitud.id),
            action="ASSIGN",
        ).order_by("timestamp")
        self.assertEqual(logs.count(), 2)
        self.assertIsNone(logs.last().user_id)
        self.assertEqual(
            logs.last().payload["repartidor_anterior_id"],
            primero.id,
        )
        self.assertEqual(
            logs.last().payload["repartidor_nuevo_id"],
            segundo.id,
        )
        self.assertEqual(
            logs.last().payload["api_client"],
            {"id": self.api_client.id, "nombre": "Maya logística"},
        )
        self.assertEqual(
            logs.last().payload["actor_externo"],
            {"id": "ops-17", "nombre": "Operaciones"},
        )
        self.assertNotIn("email", str(logs.first().payload).lower())
        self.assertNotIn("6671234567", str(logs.first().payload))
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=self.api_client,
                endpoint=self.assign_url,
                method="POST",
                status_code=status.HTTP_200_OK,
            ).count(),
            3,
        )

    def test_retry_exacto_terminal_e_inactivo_permanece_idempotente(self):
        repartidor = self._repartidor("m2m_terminal")
        self.solicitud.repartidor = repartidor
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_CANCELADO
        self.solicitud.save(update_fields=["repartidor", "estatus"])
        repartidor.user.is_active = False
        repartidor.user.save(update_fields=["is_active"])

        response = self.client.post(
            self.assign_url,
            self._payload(repartidor),
            format="json",
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["idempotent"])
        self.solicitud.refresh_from_db()
        self.assertEqual(
            self.solicitud.estatus,
            SolicitudDomicilio.ESTATUS_CANCELADO,
        )
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_actor_minimo_es_obligatorio_y_error_queda_auditado(self):
        repartidor = self._repartidor("m2m_actor_required")

        response = self.client.post(
            self.assign_url,
            {"repartidor_id": repartidor.id},
            format="json",
            **self.auth,
        )
        boolean_id = self.client.post(
            self.assign_url,
            {
                "repartidor_id": True,
                "actor": {"id": "ops-17", "nombre": "Operaciones"},
            },
            format="json",
            **self.auth,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(boolean_id.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(AuditLog.objects.count(), 0)
        self.assertEqual(
            PublicApiAccessLog.objects.filter(
                client=self.api_client,
                status_code=status.HTTP_400_BAD_REQUEST,
            ).count(),
            2,
        )
