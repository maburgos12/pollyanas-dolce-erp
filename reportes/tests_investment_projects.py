from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from reportes.models import (
    CategoriaGasto,
    CentroCosto,
    FactVentaDiaria,
    GastoOperativoMensual,
    ProyectoInversion,
    ProyectoInversionEscenario,
    ProyectoInversionGasto,
    ProyectoInversionPagoDeuda,
    ProyectoInversionSnapshotMensual,
)
from reportes.services_investment_projects import ProyectoInversionRefreshService
from reportes.services_operating_finance import OperatingFinanceBootstrapService


class ProyectoInversionRefreshServiceTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="GML-INV", nombre="Guamúchil Inversión")
        OperatingFinanceBootstrapService().bootstrap()
        self.project = ProyectoInversion.objects.create(
            nombre_proyecto="Sucursal Guamúchil 2026",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            sucursal_relacionada=self.sucursal,
            fecha_inicio=date(2026, 1, 5),
            fecha_apertura=date(2026, 2, 1),
            monto_inversion_planeado=Decimal("450000"),
            deuda_asociada=Decimal("120000"),
            tasa_interes_anual=Decimal("18"),
            plazo_deuda_meses=24,
            recovery_strategy=ProyectoInversion.RECOVERY_PERCENTAGE_OF_PROFIT,
            recovery_percentage=Decimal("0.70"),
        )
        ProyectoInversionGasto.objects.create(
            proyecto=self.project,
            fecha=date(2026, 1, 10),
            categoria=ProyectoInversionGasto.CATEGORIA_OBRA_CIVIL,
            descripcion="Adecuación local",
            monto=Decimal("100000"),
            iva=Decimal("16000"),
            monto_total=Decimal("116000"),
        )
        ProyectoInversionGasto.objects.create(
            proyecto=self.project,
            fecha=date(2026, 1, 20),
            categoria=ProyectoInversionGasto.CATEGORIA_EQUIPAMIENTO,
            descripcion="Hornos y vitrinas",
            monto=Decimal("84000"),
            iva=Decimal("13440"),
            monto_total=Decimal("97440"),
        )
        FactVentaDiaria.objects.create(
            fecha=date(2026, 2, 5),
            sucursal=self.sucursal,
            producto_clave="PASTEL-FRESA",
            producto_nombre="Pastel Fresa",
            cantidad=Decimal("120"),
            tickets=30,
            venta_bruta=Decimal("22000"),
            descuento=Decimal("500"),
            venta_total=Decimal("21500"),
            venta_neta=Decimal("18534"),
            costo_estimado=Decimal("9500"),
            margen=Decimal("12000"),
            source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
        )
        branch_center = CentroCosto.objects.get(codigo=f"SUC_{self.sucursal.codigo}")
        GastoOperativoMensual.objects.create(
            periodo=date(2026, 2, 1),
            centro_costo=branch_center,
            categoria_gasto=CategoriaGasto.objects.get(codigo="RENTA_SUC"),
            monto=Decimal("8000"),
        )
        GastoOperativoMensual.objects.create(
            periodo=date(2026, 2, 1),
            centro_costo=branch_center,
            categoria_gasto=CategoriaGasto.objects.get(codigo="NOMINA_SUC"),
            monto=Decimal("12000"),
        )
        ProyectoInversionPagoDeuda.objects.create(
            proyecto=self.project,
            fecha_pago=date(2026, 2, 15),
            monto_pago=Decimal("6400"),
            interes_pagado=Decimal("1800"),
            capital_amortizado=Decimal("4600"),
            saldo_insoluto=Decimal("115400"),
        )

    def test_refresh_materializes_snapshot(self):
        result = ProyectoInversionRefreshService().refresh_project(self.project, until=date(2026, 2, 28))

        self.assertEqual(result.project_status, ProyectoInversion.ESTATUS_EN_RECUPERACION)
        snapshot = ProyectoInversionSnapshotMensual.objects.get(
            proyecto=self.project,
            periodo=date(2026, 2, 1),
        )
        self.assertEqual(snapshot.ventas_mensuales, Decimal("21500.00"))
        self.assertEqual(snapshot.costo_venta_mensual, Decimal("9500.00"))
        self.assertEqual(snapshot.gastos_operativos, Decimal("20000.00"))
        self.assertEqual(snapshot.servicio_deuda, Decimal("6400.00"))
        self.assertEqual(snapshot.flujo_operativo, Decimal("-8000.00"))
        self.assertEqual(snapshot.flujo_libre, Decimal("-14400.00"))
        self.assertEqual(snapshot.flujo_para_recuperacion, Decimal("0.00"))
        self.assertEqual(snapshot.monto_recuperacion_mes, Decimal("0.00"))
        self.assertEqual(snapshot.data_source, ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT)
        self.assertEqual(snapshot.confidence_score, 100)
        self.assertEqual(snapshot.fuentes.get("expense_coverage_status"), "COMPLETE")
        self.assertIsNotNone(snapshot.health_score)
        self.project.refresh_from_db()
        self.assertEqual(self.project.monto_inversion_real, Decimal("213440.00"))


class ProyectoInversionViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="lectura_inv", password="pass123", first_name="Dirección")
        group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user.groups.add(group)
        self.client.login(username="lectura_inv", password="pass123")
        self.sucursal = Sucursal.objects.create(codigo="CEN-INV", nombre="Centro Inversión")
        OperatingFinanceBootstrapService().bootstrap()
        self.project = ProyectoInversion.objects.create(
            nombre_proyecto="Sucursal Centro 2026",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            sucursal_relacionada=self.sucursal,
            fecha_inicio=timezone.localdate() - timedelta(days=60),
            fecha_apertura=timezone.localdate() - timedelta(days=45),
            monto_inversion_planeado=Decimal("300000"),
            recovery_strategy=ProyectoInversion.RECOVERY_FULL_NET_CASHFLOW,
        )
        ProyectoInversionGasto.objects.create(
            proyecto=self.project,
            fecha=timezone.localdate() - timedelta(days=58),
            categoria=ProyectoInversionGasto.CATEGORIA_TECNOLOGIA,
            descripcion="Punto de venta",
            monto=Decimal("50000"),
            iva=Decimal("8000"),
            monto_total=Decimal("58000"),
        )

    def test_portfolio_and_detail_render(self):
        response = self.client.get(reverse("reportes:proyectos_inversion"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Portafolio de proyectos de inversión")
        self.assertContains(response, "Sucursal Centro 2026")

        detail = self.client.get(reverse("reportes:proyecto_inversion_detail", args=[self.project.pk]))
        self.assertEqual(detail.status_code, 200)
        self.assertContains(detail, "Dashboard ejecutivo del proyecto")
        self.assertContains(detail, "Detalle de inversión")
        self.assertContains(detail, "Registrar pago")

    def test_comparison_view_renders(self):
        response = self.client.get(reverse("reportes:proyectos_inversion_comparativo"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ranking ejecutivo de proyectos")
        self.assertContains(response, "Sucursal Centro 2026")

    def test_detail_view_can_save_scenario_and_export(self):
        scenario_response = self.client.post(
            reverse("reportes:proyecto_inversion_detail", args=[self.project.pk]),
            {
                "action": "save_scenario",
                "nombre": "Base 2026",
                "tipo_escenario": ProyectoInversionEscenario.TIPO_BASE,
                "ventas_promedio_mensuales": "85000",
                "crecimiento_mensual_pct": "0.02",
                "margen_bruto_pct": "0.55",
                "gastos_operativos_mensuales": "28000",
                "horizonte_meses": "18",
            },
            follow=True,
        )
        self.assertEqual(scenario_response.status_code, 200)
        self.assertTrue(ProyectoInversionEscenario.objects.filter(proyecto=self.project, nombre="Base 2026").exists())

        export_response = self.client.get(
            reverse("reportes:proyecto_inversion_detail", args=[self.project.pk]),
            {"report": "summary", "export": "xlsx"},
        )
        self.assertEqual(export_response.status_code, 200)
        self.assertEqual(
            export_response["Content-Type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
