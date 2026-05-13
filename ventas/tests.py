from django.test import TestCase
from django.urls import reverse


class VentasModuleTests(TestCase):
    def test_placeholder(self):
        self.assertTrue(True)

    def test_legacy_eventos_route_redirects_to_pronostico(self):
        response = self.client.get(reverse("ventas:eventos"))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], reverse("ventas:pronostico"))

    def test_legacy_tendencias_route_redirects_to_pronostico(self):
        response = self.client.get(reverse("ventas:tendencias"))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], reverse("ventas:pronostico"))
