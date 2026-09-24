from decimal import Decimal as D

from django.test import SimpleTestCase

from reportes.services_isn import calcular_isn_sinaloa, prorratear_isn


class ISNMathTests(SimpleTestCase):
    def test_tarifa_progresiva_sinaloa(self):
        self.assertEqual(calcular_isn_sinaloa(D("500000.00")), D("12000.00"))
        self.assertEqual(calcular_isn_sinaloa(D("660307.70")), D("16168.00"))
        self.assertEqual(calcular_isn_sinaloa(D("707079.82")), D("17398.23"))
        self.assertEqual(calcular_isn_sinaloa(D("1000000.00")), D("25800.00"))

    def test_prorrateo_cierra_exactamente_a_centavos(self):
        result = prorratear_isn({3: D("1"), 1: D("1"), 2: D("1")}, D("10.00"))

        self.assertEqual(sum(result.values(), D("0")), D("10.00"))
        self.assertEqual(result, {1: D("3.34"), 2: D("3.33"), 3: D("3.33")})
