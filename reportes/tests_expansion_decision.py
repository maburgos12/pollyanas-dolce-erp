from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from reportes.models import (
    EmpresaResultadoMensual,
    ExpansionPolicyConfig,
    ProyectoInversion,
    ProyectoInversionEscenario,
    ProyectoInversionSnapshotMensual,
)
from reportes.services_expansion_calibration import ExpansionCalibrationService
from reportes.services_expansion_decision import ExpansionDecisionService
from reportes.services_expansion_forecast import ExpansionForecastService, recomendar_apertura
from reportes.services_expansion_simulations import ExpansionSimulationRegistryService


class ExpansionDecisionServiceTests(TestCase):
    def setUp(self):
        self.sucursal_expand = Sucursal.objects.create(codigo="EXP-1", nombre="Sucursal Fuerte")
        self.sucursal_risk = Sucursal.objects.create(codigo="EXP-2", nombre="Sucursal Riesgo")
        self.expand_project = ProyectoInversion.objects.create(
            nombre_proyecto="Sucursal Fuerte 2026",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            sucursal_relacionada=self.sucursal_expand,
            fecha_inicio=date(2025, 10, 1),
            fecha_apertura=date(2025, 11, 1),
            monto_inversion_planeado=Decimal("250000"),
            monto_inversion_real=Decimal("240000"),
            roi_objetivo=Decimal("25"),
            payback_objetivo_meses=12,
        )
        self.risk_project = ProyectoInversion.objects.create(
            nombre_proyecto="Sucursal Riesgo 2026",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            sucursal_relacionada=self.sucursal_risk,
            fecha_inicio=date(2025, 9, 1),
            fecha_apertura=date(2025, 10, 1),
            monto_inversion_planeado=Decimal("280000"),
            monto_inversion_real=Decimal("300000"),
            roi_objetivo=Decimal("20"),
            payback_objetivo_meses=14,
        )
        self.watch_project = ProyectoInversion.objects.create(
            nombre_proyecto="Sucursal Vigilada 2026",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            sucursal_relacionada=Sucursal.objects.create(codigo="EXP-3", nombre="Sucursal Vigilada"),
            fecha_inicio=date(2025, 8, 1),
            fecha_apertura=date(2025, 9, 1),
            monto_inversion_planeado=Decimal("260000"),
            monto_inversion_real=Decimal("250000"),
            roi_objetivo=Decimal("22"),
            payback_objetivo_meses=13,
        )
        EmpresaResultadoMensual.objects.create(
            periodo=date(2026, 3, 1),
            venta_total=Decimal("1500000"),
            utilidad_operativa_total=Decimal("250000"),
        )
        ExpansionPolicyConfig.objects.create(
            nombre="Política test",
            activa=True,
            min_free_cashflow_total=Decimal("5000"),
            max_debt_to_income_ratio=Decimal("0.40"),
            max_average_payback_months=Decimal("18"),
            max_projects_in_risk=2,
            metadata={
                "calibration": {
                    "mode_enabled": True,
                    "minimum_sample_projects": 3,
                    "minimum_months": 6,
                }
            },
        )
        self._create_snapshot_series(
            self.expand_project,
            [
                (date(2025, 10, 1), Decimal("155000"), Decimal("48000"), Decimal("5000"), Decimal("43000"), Decimal("14"), Decimal("82"), Decimal("14"), Decimal("7"), Decimal("12"), Decimal("65000")),
                (date(2025, 11, 1), Decimal("160000"), Decimal("52000"), Decimal("5000"), Decimal("47000"), Decimal("17"), Decimal("85"), Decimal("15"), Decimal("8"), Decimal("11"), Decimal("60000")),
                (date(2025, 12, 1), Decimal("170000"), Decimal("56000"), Decimal("5000"), Decimal("51000"), Decimal("18"), Decimal("88"), Decimal("16"), Decimal("9"), Decimal("10"), Decimal("55000")),
                (date(2026, 1, 1), Decimal("180000"), Decimal("60000"), Decimal("5000"), Decimal("55000"), Decimal("20"), Decimal("90"), Decimal("18"), Decimal("10"), Decimal("10"), Decimal("40000")),
                (date(2026, 2, 1), Decimal("190000"), Decimal("65000"), Decimal("5000"), Decimal("60000"), Decimal("32"), Decimal("92"), Decimal("19"), Decimal("11"), Decimal("9"), Decimal("45000")),
                (date(2026, 3, 1), Decimal("210000"), Decimal("72000"), Decimal("5000"), Decimal("67000"), Decimal("38"), Decimal("94"), Decimal("21"), Decimal("12"), Decimal("8"), Decimal("50000")),
            ],
        )
        self._create_snapshot_series(
            self.risk_project,
            [
                (date(2025, 10, 1), Decimal("105000"), Decimal("10000"), Decimal("12000"), Decimal("-2000"), Decimal("10"), Decimal("48"), Decimal("7"), Decimal("4"), Decimal("18"), Decimal("150000")),
                (date(2025, 11, 1), Decimal("98000"), Decimal("7000"), Decimal("12000"), Decimal("-5000"), Decimal("9"), Decimal("46"), Decimal("7"), Decimal("4"), Decimal("19"), Decimal("155000")),
                (date(2025, 12, 1), Decimal("95000"), Decimal("6000"), Decimal("12000"), Decimal("-6000"), Decimal("8"), Decimal("45"), Decimal("6"), Decimal("3"), Decimal("20"), Decimal("158000")),
                (date(2026, 1, 1), Decimal("90000"), Decimal("5000"), Decimal("12000"), Decimal("-7000"), Decimal("8"), Decimal("45"), Decimal("6"), Decimal("3"), Decimal("20"), Decimal("160000")),
                (date(2026, 2, 1), Decimal("85000"), Decimal("-3000"), Decimal("12000"), Decimal("-15000"), Decimal("5"), Decimal("42"), Decimal("5"), Decimal("2"), Decimal("22"), Decimal("170000")),
                (date(2026, 3, 1), Decimal("80000"), Decimal("-6000"), Decimal("12000"), Decimal("-18000"), Decimal("4"), Decimal("40"), Decimal("4"), Decimal("2"), Decimal("24"), Decimal("180000")),
            ],
        )
        self._create_snapshot_series(
            self.watch_project,
            [
                (date(2025, 10, 1), Decimal("120000"), Decimal("26000"), Decimal("7000"), Decimal("19000"), Decimal("11"), Decimal("58"), Decimal("9"), Decimal("5"), Decimal("16"), Decimal("90000")),
                (date(2025, 11, 1), Decimal("125000"), Decimal("28000"), Decimal("7000"), Decimal("21000"), Decimal("12"), Decimal("60"), Decimal("10"), Decimal("6"), Decimal("15"), Decimal("85000")),
                (date(2025, 12, 1), Decimal("128000"), Decimal("30000"), Decimal("7000"), Decimal("23000"), Decimal("13"), Decimal("62"), Decimal("10"), Decimal("7"), Decimal("14"), Decimal("82000")),
                (date(2026, 1, 1), Decimal("130000"), Decimal("32000"), Decimal("7000"), Decimal("25000"), Decimal("14"), Decimal("64"), Decimal("11"), Decimal("8"), Decimal("13"), Decimal("78000")),
                (date(2026, 2, 1), Decimal("132000"), Decimal("31000"), Decimal("7000"), Decimal("24000"), Decimal("15"), Decimal("66"), Decimal("11"), Decimal("9"), Decimal("13"), Decimal("76000")),
                (date(2026, 3, 1), Decimal("135000"), Decimal("33000"), Decimal("7000"), Decimal("26000"), Decimal("16"), Decimal("68"), Decimal("12"), Decimal("10"), Decimal("12"), Decimal("74000")),
            ],
        )

    def _create_snapshot_series(self, project, rows):
        for period, sales, operating_profit, debt_service, free_cashflow, roi, health, cash_on_cash, recovery_pct, payback, debt_balance in rows:
            ProyectoInversionSnapshotMensual.objects.create(
                proyecto=project,
                periodo=period,
                periodo_fin=period + timedelta(days=27),
                ventas_mensuales=sales,
                utilidad_bruta=sales * Decimal("0.68"),
                gastos_operativos=(sales * Decimal("0.68")) - operating_profit,
                renta=Decimal("12000") if project == self.expand_project else Decimal("15000"),
                utilidad_operativa=operating_profit,
                flujo_operativo=operating_profit,
                servicio_deuda=debt_service,
                flujo_libre=free_cashflow,
                flujo_para_recuperacion=max(free_cashflow, Decimal("0")),
                flujo_neto=free_cashflow,
                recuperacion_acumulada=Decimal("90000") if project == self.expand_project else Decimal("20000"),
                saldo_pendiente=Decimal("150000") if project == self.expand_project else Decimal("280000"),
                porcentaje_recuperado=recovery_pct,
                cash_on_cash=cash_on_cash,
                roi_acumulado=roi,
                payback_real_meses=payback,
                payback_forecast_meses=payback,
                saldo_insoluto=debt_balance,
                health_score=int(health),
                health_status=ProyectoInversionSnapshotMensual.HEALTH_GREEN if int(health) >= 80 else ProyectoInversionSnapshotMensual.HEALTH_RED,
                data_source=ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT,
                confidence_score=100,
            )

    def test_classifies_projects_for_expansion_and_risk(self):
        service = ExpansionDecisionService()
        expand_result = service.classify_project(self.expand_project)
        risk_result = service.classify_project(self.risk_project)

        self.assertEqual(expand_result["classification"], ExpansionDecisionService.CLASSIFICATION_EXPAND)
        self.assertEqual(risk_result["classification"], ExpansionDecisionService.CLASSIFICATION_RISK)
        self.assertTrue(risk_result["recurrent_negative_free_cashflow"])

    def test_recommends_opening_when_capacity_and_history_are_healthy(self):
        payload = recomendar_apertura(
            base_project=self.expand_project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("10000"),
            sales_adjustment_pct=Decimal("8"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
        )

        self.assertEqual(payload["decision"], "ABRIR")
        self.assertGreaterEqual(payload["recommended_branch_count"], 1)
        self.assertEqual(payload["forecast"]["outputs"]["risk_level"], "BAJO")
        self.assertGreater(payload["forecast"]["outputs"]["projected_operating_expenses"], Decimal("0"))
        self.assertGreater(payload["forecast"]["outputs"]["projected_recovery_cashflow"], Decimal("0"))

    def test_forecast_inputs_change_projection(self):
        service = ExpansionForecastService()
        base_payload = service.forecast(
            base_project=self.expand_project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("0"),
            sales_adjustment_pct=Decimal("0"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
        )
        stressed_payload = service.forecast(
            base_project=self.expand_project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("30000"),
            sales_adjustment_pct=Decimal("-15"),
            scenario=ExpansionForecastService.SCENARIO_CONSERVADOR,
        )

        self.assertGreater(
            base_payload["outputs"]["projected_sales"],
            stressed_payload["outputs"]["projected_sales"],
        )
        self.assertGreater(
            base_payload["outputs"]["projected_free_cashflow"],
            stressed_payload["outputs"]["projected_free_cashflow"],
        )
        self.assertGreater(
            stressed_payload["outputs"]["projected_operating_expenses"],
            base_payload["outputs"]["projected_operating_expenses"],
        )
        self.assertIn("sales_adjustment_pct", stressed_payload["inputs"])
        self.assertIn("monthly_rent", stressed_payload["inputs"])
        self.assertIn("free_cashflow_base", base_payload["historical_reference"])
        self.assertIn("payback_base", base_payload["historical_reference"])
        self.assertIn("roi_base", base_payload["historical_reference"])

    def test_calibration_service_builds_accuracy_and_applies_recalibration(self):
        calibration_service = ExpansionCalibrationService()
        calibration_service.set_real_classification(self.expand_project, "EXPANDIR")
        calibration_service.set_real_classification(self.watch_project, "VIGILAR")
        calibration_service.set_real_classification(self.risk_project, "RIESGO")

        context = calibration_service.build_context(use_cache=False)
        self.assertEqual(context["accuracy"]["labeled_cases"], 3)
        self.assertIsNotNone(context["accuracy"]["accuracy_pct"])

        result = calibration_service.calibrate()
        self.assertTrue(result["applied"])
        self.assertIn("settings", result)

    def test_simulation_registry_deduplicates_same_inputs(self):
        forecast_service = ExpansionForecastService()
        registry = ExpansionSimulationRegistryService()
        forecast_payload = forecast_service.forecast(
            base_project=self.expand_project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("12000"),
            sales_adjustment_pct=Decimal("5"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
        )
        recommendation = recomendar_apertura(
            base_project=self.expand_project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("12000"),
            sales_adjustment_pct=Decimal("5"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
            forecast_payload=forecast_payload,
        )
        comparison_rows = [{"label": "Ventas", "base_display": "$100.00", "projected_display": "$110.00", "delta_display": "+10.00%", "delta_class": "is-positive"}]
        sensitivity_rows = [{"label": "Base", "adjustment_pct": Decimal("5"), "free_cashflow_display": "$50.00", "payback_display": "6.00 meses", "roi_display": "20.00%", "recommendation_display": "RECOMENDADO", "recommendation_class": "is-positive"}]

        first, created_first = registry.save_simulation(
            project=self.expand_project,
            forecast_payload=forecast_payload,
            opening_recommendation=recommendation,
            comparison_rows=comparison_rows,
            sensitivity_rows=sensitivity_rows,
            executive_note="Primera corrida",
            status=ProyectoInversionEscenario.ESTATUS_CANDIDATO,
            user=None,
        )
        second, created_second = registry.save_simulation(
            project=self.expand_project,
            forecast_payload=forecast_payload,
            opening_recommendation=recommendation,
            comparison_rows=comparison_rows,
            sensitivity_rows=sensitivity_rows,
            executive_note="Segunda corrida",
            status=ProyectoInversionEscenario.ESTATUS_APROBADO_PRELIMINAR,
            user=None,
        )

        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first.pk, second.pk)
        self.assertEqual(ProyectoInversionEscenario.objects.exclude(simulacion_hash="").count(), 1)
        second.refresh_from_db()
        self.assertEqual(second.estatus_simulacion, ProyectoInversionEscenario.ESTATUS_APROBADO_PRELIMINAR)
        self.assertEqual(second.notas, "Segunda corrida")


class ExpansionViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="director_exp", password="pass123", first_name="Dirección")
        lectura_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user.groups.add(lectura_group)
        self.client.login(username="director_exp", password="pass123")
        self.sucursal = Sucursal.objects.create(codigo="EXP-VIEW", nombre="Sucursal Vista")
        self.project = ProyectoInversion.objects.create(
            nombre_proyecto="Sucursal Vista 2026",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            sucursal_relacionada=self.sucursal,
            fecha_inicio=timezone.localdate() - timedelta(days=150),
            fecha_apertura=timezone.localdate() - timedelta(days=120),
            monto_inversion_planeado=Decimal("260000"),
            monto_inversion_real=Decimal("240000"),
            roi_objetivo=Decimal("20"),
            payback_objetivo_meses=12,
        )
        EmpresaResultadoMensual.objects.create(
            periodo=date(2026, 3, 1),
            venta_total=Decimal("1100000"),
            utilidad_operativa_total=Decimal("180000"),
        )
        ProyectoInversionSnapshotMensual.objects.create(
            proyecto=self.project,
            periodo=date(2026, 3, 1),
            periodo_fin=date(2026, 3, 31),
            ventas_mensuales=Decimal("175000"),
            utilidad_operativa=Decimal("62000"),
            flujo_operativo=Decimal("62000"),
            servicio_deuda=Decimal("4000"),
            flujo_libre=Decimal("58000"),
            flujo_para_recuperacion=Decimal("58000"),
            flujo_neto=Decimal("58000"),
            recuperacion_acumulada=Decimal("120000"),
            saldo_pendiente=Decimal("120000"),
            porcentaje_recuperado=Decimal("50"),
            cash_on_cash=Decimal("24"),
            roi_acumulado=Decimal("28"),
            payback_real_meses=Decimal("10"),
            payback_forecast_meses=Decimal("10"),
            saldo_insoluto=Decimal("30000"),
            health_score=88,
            health_status=ProyectoInversionSnapshotMensual.HEALTH_GREEN,
            data_source=ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT,
            confidence_score=100,
        )
        ExpansionPolicyConfig.objects.create(activa=True)

    def test_expansion_view_renders(self):
        response = self.client.get(reverse("reportes:proyectos_inversion_expansion"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Simulador para abrir nuevas sucursales")
        self.assertContains(response, "Sucursal Vista 2026")

    def test_expansion_view_runs_forecast(self):
        response = self.client.post(
            reverse("reportes:proyectos_inversion_expansion"),
            {
                "action": "run_expansion_forecast",
                "base_project_id": str(self.project.pk),
                "scenario": ExpansionForecastService.SCENARIO_BASE,
                "investment_estimate": "180000",
                "monthly_rent_reference": "12000",
                "sales_adjustment_reference": "5",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Resultado final")
        self.assertContains(response, "Comparación contra la sucursal base")
        self.assertContains(response, "Sensibilidad comercial")
        self.assertContains(response, "Recomendación basada en")
        self.assertContains(response, "Supuestos del usuario")
        self.assertEqual(len(response.context["comparison_rows"]), 6)
        self.assertEqual(len(response.context["sensitivity_rows"]), 3)

    def test_expansion_view_can_save_real_classification(self):
        response = self.client.post(
            reverse("reportes:proyectos_inversion_expansion"),
            {
                "action": "set_real_classification",
                "project_id": str(self.project.pk),
                "real_classification": "VIGILAR",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.project.refresh_from_db()
        self.assertEqual(self.project.metadata["calibration"]["real_classification"], "VIGILAR")

    def test_expansion_view_can_save_simulation_history(self):
        response = self.client.post(
            reverse("reportes:proyectos_inversion_expansion_simulador"),
            {
                "action": "save_current_simulation",
                "base_project_id": str(self.project.pk),
                "scenario": ExpansionForecastService.SCENARIO_BASE,
                "investment_estimate": "180000",
                "monthly_rent_reference": "12000",
                "sales_adjustment_reference": "5",
                "scenario_status": ProyectoInversionEscenario.ESTATUS_CANDIDATO,
                "executive_note": "Caso candidato para comité",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Simulación guardada")
        saved = ProyectoInversionEscenario.objects.exclude(simulacion_hash="").get()
        self.assertEqual(saved.proyecto, self.project)
        self.assertEqual(saved.estatus_simulacion, ProyectoInversionEscenario.ESTATUS_CANDIDATO)
        self.assertEqual(saved.notas, "Caso candidato para comité")
        self.assertContains(response, saved.nombre)

    def test_expansion_view_can_load_saved_simulation(self):
        registry = ExpansionSimulationRegistryService()
        forecast_service = ExpansionForecastService()
        forecast_payload = forecast_service.forecast(
            base_project=self.project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("12000"),
            sales_adjustment_pct=Decimal("5"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
        )
        recommendation = recomendar_apertura(
            base_project=self.project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("12000"),
            sales_adjustment_pct=Decimal("5"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
            forecast_payload=forecast_payload,
        )
        scenario, _ = registry.save_simulation(
            project=self.project,
            forecast_payload=forecast_payload,
            opening_recommendation=recommendation,
            comparison_rows=[],
            sensitivity_rows=[],
            executive_note="Guardar para reabrir",
            status=ProyectoInversionEscenario.ESTATUS_EN_REVISION,
        )

        response = self.client.get(
            reverse("reportes:proyectos_inversion_expansion_simulador"),
            {"load_simulation": scenario.pk},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["active_saved_simulation"]["id"], scenario.pk)
        self.assertEqual(response.context["forecast_form"]["base_project_id"], str(self.project.pk))

    def test_expansion_view_exports_saved_simulation_pdf(self):
        registry = ExpansionSimulationRegistryService()
        forecast_service = ExpansionForecastService()
        forecast_payload = forecast_service.forecast(
            base_project=self.project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("12000"),
            sales_adjustment_pct=Decimal("5"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
        )
        recommendation = recomendar_apertura(
            base_project=self.project,
            investment_estimate=Decimal("180000"),
            monthly_rent=Decimal("12000"),
            sales_adjustment_pct=Decimal("5"),
            scenario=ExpansionForecastService.SCENARIO_BASE,
            forecast_payload=forecast_payload,
        )
        scenario, _ = registry.save_simulation(
            project=self.project,
            forecast_payload=forecast_payload,
            opening_recommendation=recommendation,
            comparison_rows=[],
            sensitivity_rows=[],
            executive_note="Exportar",
            status=ProyectoInversionEscenario.ESTATUS_EN_REVISION,
        )

        response = self.client.get(
            reverse("reportes:proyectos_inversion_expansion_simulador"),
            {"export_saved_simulation": scenario.pk, "export_format": "pdf"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/pdf")
