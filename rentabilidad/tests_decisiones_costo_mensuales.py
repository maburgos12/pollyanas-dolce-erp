from datetime import date
from decimal import Decimal

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from reportes.models import ProductoReventaCosto
from rentabilidad.services_costos import _fuentes_reventa_para_periodo
from rentabilidad.views_rentabilidad import _build_productos_panel


class DecisionCostoMensualTests(TestCase):
    def setUp(self):
        self.producto = PointProduct.objects.create(
            external_id="1062", sku="0306", name="Servicio a domicilio 4")
        self.metadata = {
            "cost_treatment": "UNALLOCATED_SERVICE",
            "cost_treatment_period": "2026-10-01",
            "authorization": "DG: servicio sin costo directo asignado en octubre",
            "point_external_id": self.producto.external_id,
            "point_name": self.producto.name,
        }
        self.decision = ProductoReventaCosto.objects.create(
            producto_point=self.producto, fecha_vigencia=date(2026, 10, 5),
            fuente=ProductoReventaCosto.FUENTE_MANUAL, costo_unitario=0,
            source_hash="decision-mensual", metadata=self.metadata)

    def fuentes(self, mes):
        return _fuentes_reventa_para_periodo(
            {self.producto.pk}, date(2026, mes, 1), date(2026, mes, 28))

    def test_se_aplica_solo_en_el_mes_autorizado(self):
        fuente = self.fuentes(10)[self.producto.pk]
        self.assertEqual(fuente["treatment"], "UNALLOCATED_SERVICE")
        self.assertTrue(fuente["authorized"])
        self.assertEqual(fuente["cost"], 0)
        self.assertEqual(self.fuentes(9), {})
        self.assertEqual(self.fuentes(11), {})

    def test_no_anula_una_compra_posterior_al_mes(self):
        ProductoReventaCosto.objects.create(
            producto_point=self.producto, fecha_vigencia=date(2026, 11, 2),
            fuente=ProductoReventaCosto.FUENTE_POINT_HISTORIAL,
            costo_unitario=Decimal("12.50"), source_hash="compra-noviembre")
        self.assertEqual(self.fuentes(11)[self.producto.pk]["cost"], Decimal("12.50"))
        self.assertNotIn("treatment", self.fuentes(11)[self.producto.pk])

    def test_periodo_malformado_no_se_interpreta_como_indefinido(self):
        for periodo in (None, "", "2026-10", "2026-10-02", 0):
            with self.subTest(periodo=periodo):
                self.decision.metadata = {**self.metadata, "cost_treatment_period": periodo}
                self.decision.save()
                self.assertEqual(self.fuentes(10), {})

    def test_conserva_el_contrato_de_decisiones_sin_limite_mensual(self):
        self.decision.metadata = {k: v for k, v in self.metadata.items()
                                  if k != "cost_treatment_period"}
        self.decision.save()
        self.assertEqual(self.fuentes(11)[self.producto.pk]["treatment"], "UNALLOCATED_SERVICE")

    def test_periodo_correcto_no_sustituye_autorizacion_e_identidad(self):
        for key, value in (("authorization", ""), ("point_external_id", "otro"),
                           ("point_name", "Otro servicio")):
            with self.subTest(key=key):
                self.decision.metadata = {**self.metadata, key: value}
                self.decision.save()
                self.assertEqual(self.fuentes(10), {})

    def test_panel_no_presenta_servicio_como_margen_cien_por_ciento(self):
        sucursal = Sucursal.objects.create(codigo="DM", nombre="Decisión mensual")
        branch = PointBranch.objects.create(external_id="DM", name="Mensual", erp_branch=sucursal)
        PointDailySale.objects.create(branch=branch, product=self.producto,
            sale_date=date(2026, 10, 6), quantity=3, gross_amount=60)
        panel = _build_productos_panel(date(2026, 10, 1), date(2026, 10, 1), date(2026, 10, 31))
        self.assertEqual(panel["productos_revisados"], 1)
        self.assertEqual(panel["costo_faltante_reventa"], 0)
        self.assertEqual(panel["top_utilidad"], [])
        self.assertEqual(panel["riesgo_margen"], [])
