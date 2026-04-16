from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from django.contrib.auth.models import Group, User
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from reportes.models import ProyectoInversion, ProyectoInversionEscenario


class InvestmentProjectApiTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="lectura_inv_api", password="pass123")
        lectura_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user.groups.add(lectura_group)
        self.sucursal = Sucursal.objects.create(codigo="API-INV", nombre="Sucursal API")
        self.project = ProyectoInversion.objects.create(
            nombre_proyecto="Sucursal Plaza X 2027",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            sucursal_relacionada=self.sucursal,
            fecha_inicio=timezone.localdate() - timedelta(days=30),
            fecha_apertura=timezone.localdate() - timedelta(days=20),
            monto_inversion_planeado=Decimal("500000"),
        )
        self.scenario = ProyectoInversionEscenario.objects.create(
            proyecto=self.project,
            nombre="Optimista API",
            tipo_escenario=ProyectoInversionEscenario.TIPO_OPTIMISTA,
            ventas_promedio_mensuales=Decimal("90000"),
            crecimiento_mensual_pct=Decimal("0.03"),
            margen_bruto_pct=Decimal("0.58"),
            gastos_operativos_mensuales=Decimal("32000"),
        )

    def test_dashboard_requires_role_and_returns_payload(self):
        url = reverse("api_reportes_investment_project_dashboard", args=[self.project.pk])

        plain = User.objects.create_user(username="plain_inv_api", password="pass123")
        self.client.force_authenticate(plain)
        forbidden = self.client.get(url)
        self.assertEqual(forbidden.status_code, status.HTTP_403_FORBIDDEN)

        self.client.force_authenticate(self.user)
        ok = self.client.get(url)
        self.assertEqual(ok.status_code, status.HTTP_200_OK)
        self.assertIn("kpis", ok.data)
        self.assertIn("chart_rows", ok.data)
        self.assertIn("cash_on_cash", ok.data["kpis"])
        self.assertIn("health_score", ok.data["kpis"])
        self.assertIn("active_alerts", ok.data)

    def test_scenario_simulation_returns_metrics(self):
        self.client.force_authenticate(self.user)
        url = reverse(
            "api_reportes_investment_project_scenario_simulate",
            args=[self.project.pk, self.scenario.pk],
        )
        response = self.client.post(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("payback_months", response.data)
        self.assertIn("projection_rows", response.data)
        self.assertIn("annual_cash_on_cash_pct", response.data)
