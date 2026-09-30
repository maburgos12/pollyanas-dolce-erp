"""El reparto no puede perder ni inventar centavos."""

from decimal import Decimal

from django.test import SimpleTestCase

from reportes.management.commands.realinear_agregados_cadena import _repartir


class RepartoTests(SimpleTestCase):
    def test_la_suma_del_detalle_iguala_al_agregado(self):
        # 8 sucursales a 551 y una a 580: ningún reparto es exacto en centavos.
        pesos = {i: Decimal("551") for i in range(8)}
        pesos[8] = Decimal("580")
        asignado = _repartir(Decimal("3990.00"), pesos)
        self.assertEqual(sum(asignado.values()), Decimal("3990.00"))

    def test_reparte_en_proporcion_al_contrato(self):
        asignado = _repartir(Decimal("10800.00"), {1: Decimal("1392"), 2: Decimal("1392")})
        self.assertEqual(asignado[1], Decimal("5400.00"))
        self.assertEqual(asignado[2], Decimal("5400.00"))

    def test_sin_pesos_no_reparte_nada(self):
        self.assertEqual(_repartir(Decimal("100.00"), {}), {})
