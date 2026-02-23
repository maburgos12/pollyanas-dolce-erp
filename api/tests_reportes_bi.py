from django.contrib.auth.models import Group, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase


class ReportesBIApiTests(APITestCase):
    def setUp(self):
        self.user_lectura = User.objects.create_user(username="lectura_bi_api", password="pass123")
        lectura_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user_lectura.groups.add(lectura_group)

        self.user_plain = User.objects.create_user(username="plain_bi_api", password="pass123")

    def test_dashboard_requires_role(self):
        url = reverse("api_reportes_bi_dashboard")

        self.client.force_authenticate(self.user_plain)
        resp_forbidden = self.client.get(url)
        self.assertEqual(resp_forbidden.status_code, status.HTTP_403_FORBIDDEN)

        self.client.force_authenticate(self.user_lectura)
        resp_ok = self.client.get(url)
        self.assertEqual(resp_ok.status_code, status.HTTP_200_OK)
        self.assertIn("kpis", resp_ok.data)
        self.assertIn("series_mensual", resp_ok.data)
