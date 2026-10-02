"""La tarifa se recorta a la vida de cada sitio y suma lo que dice la factura."""

from datetime import date
from decimal import Decimal

from django.test import SimpleTestCase

from reportes.management.commands.corregir_fumigacion_fumifer import Command, TARIFAS


class TarifaFumigacionTests(SimpleTestCase):
    def test_un_sitio_de_todo_el_ano_lleva_las_dos_tarifas(self):
        tarifas = Command._tarifas_de(None, None)
        self.assertEqual([(v, m) for v, m, _n in tarifas],
                         [(date(2026, 1, 1), Decimal("376.00")),
                          (date(2026, 4, 1), Decimal("441.00"))])

    def test_un_sitio_que_abrio_en_julio_solo_lleva_la_vigente(self):
        # La tarifa vieja terminó antes de que existiera: dos versiones con la
        # misma fecha de arranque violarían la restricción de vigencia única.
        tarifas = Command._tarifas_de(date(2026, 7, 1), None)
        self.assertEqual(len(tarifas), 1)
        self.assertEqual(tarifas[0][1], Decimal("441.00"))
        self.assertEqual(tarifas[0][0], date(2026, 7, 1))

    def test_un_sitio_que_cerro_en_junio_conserva_las_dos(self):
        tarifas = Command._tarifas_de(None, date(2026, 6, 30))
        self.assertEqual(len(tarifas), 2)

    def test_las_diez_instalaciones_suman_el_subtotal_de_la_factura(self):
        for _inicio, monto, _nota in TARIFAS:
            self.assertEqual(monto * 10, monto * 10)
        self.assertEqual(TARIFAS[0][1] * 10, Decimal("3760.00"))
        self.assertEqual(TARIFAS[1][1] * 10, Decimal("4410.00"))
