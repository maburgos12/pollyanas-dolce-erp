from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.contrib.auth.models import Group, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from control.models import MermaMensualSucursal
from reportes.models import EmpresaResultadoMensual, PresupuestoResumenMensual
from reportes.services_budget_vs_actual import BUDGET_VS_ACTUAL_SOURCE


class ReportesDashboardChartsApiTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="lectura_charts", password="pass123")
        group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user.groups.add(group)

    def test_dashboard_charts_uses_monthly_results_and_budget_snapshot(self):
        EmpresaResultadoMensual.objects.create(
            periodo=date(2026, 1, 1),
            venta_total=Decimal("3519375.91"),
            costo_materia_prima_total=Decimal("1067141.19"),
            costo_reventa_total=Decimal("30622.02"),
            mano_obra_prod_total=Decimal("316178.99"),
            gasto_comercial_total=Decimal("300000.00"),
            gasto_corporativo_total=Decimal("198665.49"),
            utilidad_operativa_total=Decimal("1922947.21"),
        )
        PresupuestoResumenMensual.objects.create(
            period=date(2026, 1, 1),
            tipo=PresupuestoResumenMensual.TIPO_FUENTE,
            fuente_nombre=BUDGET_VS_ACTUAL_SOURCE,
            total_budget=Decimal("1515255.00"),
            total_actual=Decimal("1922947.21"),
            metadata={
                "rows": [
                    {"concept": "ventas", "budget": "4525810.00", "actual": "3519375.91"},
                    {"concept": "utilidad_operativa", "budget": "1515255.00", "actual": "1922947.21"},
                    {"concept": "logistica", "budget": "112000.00", "actual": "0.00"},
                ]
            },
        )
        MermaMensualSucursal.objects.create(
            periodo=date(2026, 1, 1),
            nombre_producto="Prueba merma",
            unidades_merma=Decimal("3"),
            costo_merma=Decimal("120.50"),
        )

        self.client.force_authenticate(self.user)
        response = self.client.get(reverse("api_reportes_dashboard_charts"), {"año": "2026"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["year"], 2026)
        self.assertEqual(response.data["charts"]["ventas_vs_presupuesto"]["real"], ["3519375.91"])
        self.assertEqual(response.data["charts"]["ventas_vs_presupuesto"]["presupuesto"], ["4525810.00"])
        self.assertEqual(response.data["charts"]["utilidad_operativa"]["presupuesto"], ["1515255.00"])
        self.assertEqual(response.data["charts"]["desglose_costos"]["gasto_fijo"], ["498665.49"])
        self.assertEqual(response.data["charts"]["merma_mensual"]["costo_merma"], ["120.50"])
