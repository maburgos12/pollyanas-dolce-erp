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
from reportes.models import ProductoReventaCosto
from reportes.sales_confidence import build_sales_confidence


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

    def producto_con_costo(self, external_id="9310", name="PRODUCTO HISTÓRICO", sku="H1", fecha=None):
        producto = PointProduct.objects.create(external_id=external_id, sku=sku, name=name)
        ProductoReventaCosto.objects.create(producto_point=producto,
            fecha_vigencia=fecha or self.periodo, costo_unitario=20,
            source_hash="hist-" + external_id, metadata={"point_name": name, "point_code": sku})
        return producto

    def test_sku_y_nombre_unicos_recuperan_compra_historica_en_todos_los_lectores(self):
        producto = self.producto_con_costo()
        PointProduct.objects.create(external_id="official:H1", sku="H1", name=producto.name)
        fila = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["costo_reventa"], 40)
        self.assertEqual(fila["pendientes"], [])
        panel = _build_productos_panel(self.periodo, self.periodo, date(2025, 9, 30))
        self.assertEqual(panel["riesgo_margen"][0]["utilidad"], Decimal("910"))
        evidencia = build_sales_confidence(start_date=self.periodo, end_date=date(2025, 9, 30))
        self.assertEqual(evidencia["margin"], Decimal("910"))
        self.fact.refresh_from_db()
        self.assertIsNone(self.fact.point_product_id)
        self.assertEqual(self.fact.costo_estimado, 600)
        self.assertEqual(self.fact.cantidad, 2)
        self.assertEqual(self.fact.venta_neta, 950)

    def test_dos_identidades_nativas_no_eligen_el_primer_costo(self):
        self.producto_con_costo()
        self.producto_con_costo(external_id="9311")
        fila = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["costo_reventa"], 0)
        self.assertTrue(fila["pendientes"])
        self.assertIsNone(build_sales_confidence(start_date=self.periodo, end_date=date(2025, 9, 30))["margin"])

    def test_nombre_distinto_codigo_distinto_y_espejo_solo_no_acreditan_costo(self):
        for kwargs in [dict(name="Otro producto"), dict(sku="0H1"), dict(external_id="official:H1")]:
            with self.subTest(kwargs=kwargs):
                producto = self.producto_con_costo(**kwargs)
                fila = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
                self.assertEqual(fila["costo_reventa"], 0)
                self.assertTrue(fila["pendientes"])
                producto.delete()

    def test_enlace_no_anticipa_costo_futuro_ni_modifica_2026(self):
        self.producto_con_costo(fecha=date(2025, 10, 1))
        fila = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["costo_reventa"], 0)
        FactVentaDiaria.objects.filter(pk=self.fact.pk).update(fecha=date(2026, 9, 1))
        fila = leer_resultado_mensual(date(2026, 9, 1))["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["costo_reventa"], 0)
        self.assertTrue(fila["pendientes"])
        self.assertIsNone(build_sales_confidence(start_date=date(2026, 9, 1), end_date=date(2026, 9, 30))["margin"])

    def test_relacion_explicita_existente_se_conserva(self):
        producto = self.producto_con_costo()
        self.producto_con_costo(external_id="9311")
        FactVentaDiaria.objects.filter(pk=self.fact.pk).update(point_product=producto)
        fila = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(fila["costo_reventa"], 40)
