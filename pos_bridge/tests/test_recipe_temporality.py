from django.test import SimpleTestCase

from recetas.models import Receta
from recetas.utils.temporalidad import inferir_temporalidad_receta


class RecipeTemporalityTests(SimpleTestCase):
    def test_inferir_temporalidad_receta_detects_special_date(self):
        temporalidad, detalle = inferir_temporalidad_receta("Pastel Día del Padre Mediano")
        self.assertEqual(temporalidad, Receta.TEMPORALIDAD_FECHA_ESPECIAL)
        self.assertEqual(detalle, "Día del Padre")

    def test_inferir_temporalidad_receta_detects_temporal_campaign(self):
        temporalidad, detalle = inferir_temporalidad_receta("Pay edición limitada mango")
        self.assertEqual(temporalidad, Receta.TEMPORALIDAD_TEMPORAL)
        self.assertEqual(detalle, "Temporal / campaña")

    def test_inferir_temporalidad_receta_defaults_to_permanente(self):
        temporalidad, detalle = inferir_temporalidad_receta("Pastel de Ciruela R")
        self.assertEqual(temporalidad, Receta.TEMPORALIDAD_PERMANENTE)
        self.assertEqual(detalle, "")
