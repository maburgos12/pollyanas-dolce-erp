from decimal import Decimal

from django.test import TestCase

from recetas.models import Receta, RecetaEquivalencia
from reportes.views_produccion import ProducidoVsVendidoMermaView


class ProducidoVsVendidoConversionTests(TestCase):
    def test_detecta_rebanadas_vendidas_sin_equivalencia(self):
        rebanada_sin_equivalencia = Receta.objects.create(
            nombre="Pastel Test R",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Rebanada",
            hash_contenido="test-rebanada-sin-equivalencia",
        )
        padre = Receta.objects.create(
            nombre="Pastel Test Mediano",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pastel Mediano",
            hash_contenido="test-padre-mediano",
        )
        rebanada_con_equivalencia = Receta.objects.create(
            nombre="Pastel Test Rebanada",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Rebanada",
            hash_contenido="test-rebanada-con-equivalencia",
        )
        RecetaEquivalencia.objects.create(
            receta_porcion=rebanada_con_equivalencia,
            receta_padre=padre,
            factor_conversion=Decimal("10"),
            activo=True,
        )

        missing = ProducidoVsVendidoMermaView()._missing_slice_equivalences(
            {
                rebanada_sin_equivalencia.id: Decimal("3"),
                rebanada_con_equivalencia.id: Decimal("5"),
            }
        )

        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]["receta_id"], rebanada_sin_equivalencia.id)
        self.assertEqual(missing[0]["receta"], "Pastel Test R")
        self.assertEqual(missing[0]["vendido"], Decimal("3"))
