from django.contrib.auth.models import Group, User
from django.core.cache import cache
from django.urls import reverse
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase

from core.models import Sucursal
from integraciones.models import PublicApiClient
from logistica.models import Repartidor, Unidad


class PublicDriverIdentityApiTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.api_client, self.raw_key = PublicApiClient.create_with_generated_key(
            nombre="Centro operativo",
        )
        self.api_client.capabilities = [
            PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
        ]
        self.api_client.save(update_fields=["capabilities"])
        self.auth = {"HTTP_X_API_KEY": self.raw_key}
        self.sucursal = Sucursal.objects.create(
            codigo="AUTH-REP",
            nombre="Autenticación reparto",
        )
        self.unidad = Unidad.objects.create(
            codigo="AUTH-U1",
            descripcion="Unidad reparto",
            sucursal=self.sucursal,
        )
        self.user = User.objects.create_user(
            username="Maria.Reparto",
            password="erp-password",
            first_name="María",
            last_name="Reparto",
            is_active=True,
        )
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.driver = Repartidor.objects.create(
            user=self.user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.api_client.repartidores_logistica_autorizados.add(self.driver)
        self.login_url = reverse("api_public_driver_identity_login")
        self.status_url = reverse(
            "api_public_driver_identity_status",
            args=[self.user.id],
        )

    def _login(self, username="Maria.Reparto", password="erp-password"):
        return self.client.post(
            self.login_url,
            {"username": username, "password": password},
            format="json",
            **self.auth,
        )

    def test_valid_credentials_return_only_canonical_driver_identity(self):
        response = self._login(username="  Maria.Reparto  ")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.json(),
            {
                "user": {
                    "id": self.user.id,
                    "username": "Maria.Reparto",
                    "full_name": "María Reparto",
                },
                "driver": {
                    "id": self.driver.id,
                    "name": "María Reparto",
                },
            },
        )
        self.assertFalse(Token.objects.filter(user=self.user).exists())

    def test_invalid_inactive_non_driver_and_ineligible_are_indistinguishable(self):
        invalid = self._login(password="wrong-password")

        self.user.is_active = False
        self.user.save(update_fields=["is_active"])
        inactive = self._login()

        self.user.is_active = True
        self.user.save(update_fields=["is_active"])
        self.user.groups.clear()
        non_driver = self._login()

        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.unidad.activa = False
        self.unidad.save(update_fields=["activa"])
        ineligible = self._login()

        expected = {
            "detail": "No fue posible validar las credenciales de reparto."
        }
        for response in (invalid, inactive, non_driver, ineligible):
            self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
            self.assertEqual(response.json(), expected)

    def test_driver_must_be_explicitly_authorized_for_the_calling_client(self):
        self.api_client.repartidores_logistica_autorizados.clear()

        response = self._login()

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(
            response.json(),
            {"detail": "No fue posible validar las credenciales de reparto."},
        )

    def test_status_refresh_fails_closed_after_role_or_account_revocation(self):
        current = self.client.get(self.status_url, **self.auth)
        self.assertEqual(current.status_code, status.HTTP_200_OK)
        self.assertEqual(current.json()["driver"]["id"], self.driver.id)

        self.user.groups.clear()
        revoked = self.client.get(self.status_url, **self.auth)

        self.assertEqual(revoked.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(
            revoked.json(),
            {"detail": "Cuenta de reparto no disponible."},
        )

    def test_api_key_requires_logistics_capability(self):
        self.api_client.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        self.api_client.save(update_fields=["capabilities"])

        response = self._login()

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            response.json(),
            {"detail": "Integración no autorizada para identidad de reparto."},
        )
