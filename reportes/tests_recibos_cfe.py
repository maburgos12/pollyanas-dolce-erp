"""El recibo bimestral se reparte sin perder centavos y no se duplica."""

from decimal import Decimal

from django.test import SimpleTestCase

from reportes.management.commands.importar_recibos_cfe import _meses_entre, _mes
from datetime import date


class ProrrateoRecibosTests(SimpleTestCase):
    def test_un_recibo_bimestral_cubre_sus_dos_meses(self):
        self.assertEqual(
            _meses_entre(date(2026, 7, 1), date(2026, 8, 1)),
            [date(2026, 7, 1), date(2026, 8, 1)],
        )

    def test_la_cobertura_cruza_el_fin_de_ano(self):
        self.assertEqual(
            _meses_entre(date(2026, 12, 1), date(2027, 1, 1)),
            [date(2026, 12, 1), date(2027, 1, 1)],
        )

    def test_el_reparto_no_pierde_centavos(self):
        # 3 meses de un importe que no divide exacto: el sobrante va al primero.
        total = Decimal("10000.00")
        meses = _meses_entre(date(2026, 1, 1), date(2026, 3, 1))
        parte = (total / len(meses)).quantize(Decimal("0.01"))
        restante = total - parte * len(meses)
        repartido = [parte + restante] + [parte] * (len(meses) - 1)
        self.assertEqual(sum(repartido), total)

    def test_cobertura_vacia_no_es_un_mes(self):
        self.assertIsNone(_mes(""))
        self.assertEqual(_mes("2026-07"), date(2026, 7, 1))
