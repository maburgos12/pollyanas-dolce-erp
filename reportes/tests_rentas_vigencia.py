"""El reparto del inmueble compartido no puede perder ni inventar pesos."""

from decimal import Decimal

from django.test import SimpleTestCase

from reportes.management.commands.rentas_vigencia_real import (
    REPARTO,
    RENTA_INMUEBLE,
)


class RepartoInmuebleTests(SimpleTestCase):
    def test_los_porcentajes_suman_cien(self):
        self.assertEqual(sum(p for _c, p in REPARTO), 100)

    def test_el_reparto_da_la_factura_completa(self):
        partes = [
            (RENTA_INMUEBLE * p / 100).quantize(Decimal("0.01")) for _c, p in REPARTO
        ]
        self.assertEqual(sum(partes), RENTA_INMUEBLE)

    def test_el_importe_viejo_era_la_factura_entre_cinco_no_el_treinta_y_cinco(self):
        # Lo que el maestro tenía para Matriz: la señal de que estaba mal.
        self.assertEqual((RENTA_INMUEBLE / 5).quantize(Decimal("0.01")), Decimal("13242.02"))
        treinta_y_cinco = (RENTA_INMUEBLE * 35 / 100).quantize(Decimal("0.01"))
        self.assertEqual(treinta_y_cinco, Decimal("23173.54"))
        self.assertNotEqual(treinta_y_cinco, Decimal("13242.02"))

    def test_produccion_recibe_la_mayor_parte(self):
        # La planta ocupa más inmueble que la tienda: 65/35, igual que luz y agua.
        reparto = dict(REPARTO)
        self.assertGreater(reparto["produccion"], reparto["MATRIZ"])
