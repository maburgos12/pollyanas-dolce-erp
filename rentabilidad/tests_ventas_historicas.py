from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from reportes.models import FactVentaDiaria
from rentabilidad.services_resultado import leer_resultado_mensual
from rentabilidad.views_rentabilidad import _build_productos_panel


class VentasHistoricasRentabilidadTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.periodo = date(2025, 9, 1)
        cls.sucursal = Sucursal.objects.create(codigo="HIST", nombre="Histórica", activa=False)
        cls.fact = FactVentaDiaria.objects.create(
            fecha=cls.periodo, sucursal=cls.sucursal, producto_clave="H1",
            producto_nombre="Producto histórico", cantidad=2,
            venta_bruta=1160, descuento=50, venta_total=1110, venta_neta=950,
            costo_estimado=600, source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
        )

    def test_venta_oficial_sin_stage_no_inventa_costo_ni_excluye_sucursal_cerrada(self):
        resultado = leer_resultado_mensual(self.periodo)
        fila = resultado["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["ventas_brutas"] - fila["descuentos"], self.fact.venta_neta)
        self.assertEqual(fila["costo_reventa"], 0)
        self.assertTrue(fila["pendientes"])
        self.assertEqual(resultado["fuente"], {"max_sale_date": self.periodo, "rows": 1})
        panel = _build_productos_panel(self.periodo, self.periodo, date(2025, 9, 30))
        self.assertEqual(panel["productos_revisados"], 1)
        self.assertEqual(panel["riesgo_margen"][0]["venta"], self.fact.venta_neta)
        self.assertIsNone(panel["riesgo_margen"][0]["utilidad"])

    def test_precedencia_oficial_excluye_otro_espejo_del_mismo_dia(self):
        FactVentaDiaria.objects.create(
            fecha=self.periodo, sucursal=self.sucursal, producto_clave="H1",
            cantidad=2, venta_neta=9999, source_kind=FactVentaDiaria.SOURCE_LEGACY,
        )
        resultado = leer_resultado_mensual(self.periodo)
        fila = resultado["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["ventas_brutas"] - fila["descuentos"], self.fact.venta_neta)
        self.assertEqual(resultado["fuente"]["rows"], 1)

    def test_stage_existente_se_conserva_sin_sumar_fact(self):
        branch = PointBranch.objects.create(external_id="H", name="H", erp_branch=self.sucursal)
        product = PointProduct.objects.create(external_id="H", sku="H1", name="Producto histórico")
        PointDailySale.objects.create(
            branch=branch, product=product, sale_date=self.periodo, quantity=2,
            gross_amount=1160, tax_amount=160, discount_amount=50, net_amount=950,
        )
        fila = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["ventas_brutas"] - fila["descuentos"], Decimal("950"))

    def test_selector_y_total_corresponden_al_mes_sin_snapshot(self):
        self.client.force_login(get_user_model().objects.create_superuser("historico", password="test"))
        response = self.client.get(reverse("rentabilidad_dashboard"), {"periodo": "2025-09"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(self.periodo, response.context["periodos_disponibles"])
        self.assertEqual(response.context["totales"]["ventas_netas"], self.fact.venta_neta)
        self.assertContains(response, 'value="2025-09" selected')
        self.assertFalse(response.context["totales"]["margen_disponible"])
