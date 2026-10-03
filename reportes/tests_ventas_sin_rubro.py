"""Ningún producto puede quedar en dos rubros: eso contaría la venta doble."""

from django.test import SimpleTestCase

from reportes.management.commands.conectar_ventas_sin_rubro import (
    CAMPO,
    MERCANCIA,
    PROPIOS,
)


class CoberturaVentasTests(SimpleTestCase):
    def test_ningun_producto_aparece_dos_veces(self):
        todos = PROPIOS + MERCANCIA
        self.assertEqual(len(todos), len(set(todos)))

    def test_la_mercancia_no_se_mezcla_con_el_postre(self):
        self.assertFalse(set(PROPIOS) & set(MERCANCIA))

    def test_lee_el_importe_con_iva_como_las_demas_reglas(self):
        # Las 91 reglas de venta usan total_venta; otro campo descuadraría el
        # área contra su propia fuente.
        self.assertEqual(CAMPO, "total_venta")

    def test_cubre_los_dieciseis_productos_que_faltaban(self):
        self.assertEqual(len(PROPIOS) + len(MERCANCIA), 16)
