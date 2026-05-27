from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from maestros.models import CostoInsumo, Insumo, UnidadMedida
from recetas.models import CosteoProductoDraft, CosteoProductoDraftLinea
from recetas.services.costeo_simulator import (
    calculate_line_cost,
    suggest_sale_price,
)


class CosteoSimulatorTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_costeo_sim",
            email="admin_costeo_sim@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.g = UnidadMedida.objects.create(
            codigo="g",
            nombre="Gramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1"),
        )
        self.kg = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(
            codigo="HAR001",
            codigo_point="PT-HAR-001",
            nombre="Harina pastelera",
            unidad_base=self.kg,
            activo=True,
        )
        CostoInsumo.objects.create(
            insumo=self.insumo,
            costo_unitario=Decimal("120.00"),
            source_hash="costeo-sim-harina-001",
        )

    def test_calculate_line_cost_converts_live_cost_to_selected_unit(self):
        result = calculate_line_cost(
            insumo=self.insumo,
            cantidad=Decimal("250"),
            unidad=self.g,
        )

        self.assertEqual(result.unit_cost, Decimal("0.120000"))
        self.assertEqual(result.line_cost, Decimal("30.000000"))
        self.assertEqual(result.unit.codigo, "g")
        self.assertEqual(result.source, "COSTO_CANONICO")

    def test_suggest_sale_price_rounds_up_to_target_margin(self):
        suggestion = suggest_sale_price(
            unit_cost=Decimal("73.10"),
            target_margin_pct=Decimal("55.00"),
        )

        self.assertEqual(suggestion.raw_price, Decimal("162.444444"))
        self.assertEqual(suggestion.suggested_price, Decimal("165.00"))
        self.assertEqual(suggestion.target_margin_pct, Decimal("55.00"))

    def test_costeo_draft_endpoint_persists_lines_and_suggested_price(self):
        response = self.client.post(
            reverse("recetas:costeo_simulador_draft_save"),
            data={
                "nombre": "Pastel nuevo prueba",
                "target_margin_pct": "55",
                "lines": [
                    {
                        "insumo_id": self.insumo.id,
                        "cantidad": "250",
                        "unidad_id": self.g.id,
                    }
                ],
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["draft"]["nombre"], "Pastel nuevo prueba")
        self.assertEqual(payload["summary"]["total_cost"]["raw"], "30.00")
        self.assertEqual(payload["summary"]["suggested_price"]["raw"], "70.00")

        draft = CosteoProductoDraft.objects.get(nombre="Pastel nuevo prueba")
        self.assertEqual(draft.creado_por, self.user)
        self.assertEqual(draft.costo_unitario_resultado_snapshot, Decimal("30.000000"))
        self.assertEqual(draft.precio_venta_sugerido, Decimal("70.00"))

        line = CosteoProductoDraftLinea.objects.get(draft=draft)
        self.assertEqual(line.insumo, self.insumo)
        self.assertEqual(line.costo_unitario_snapshot, Decimal("0.120000"))
        self.assertEqual(line.costo_total_snapshot, Decimal("30.000000"))

    def test_costeo_dashboard_renders_simulator_as_separate_tab_without_yield_fields(self):
        response = self.client.get(reverse("recetas:costeo_dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Historial semanal")
        self.assertContains(response, "Simulador producto nuevo")
        self.assertContains(response, 'data-costeo-tab-panel="simulador"')
        self.assertNotContains(response, "Rendimiento")
        self.assertNotContains(response, "Unidad rendimiento")
