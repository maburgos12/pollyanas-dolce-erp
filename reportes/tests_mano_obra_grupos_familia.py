from django.test import TestCase

from reportes.mano_obra_grupos_familia import familias_del_grupo, grupo_de_familia


class GrupoDeFamiliaTests(TestCase):
    def test_familias_conocidas_de_point_se_agrupan(self):
        self.assertEqual(grupo_de_familia("Pastel Chico"), "Pastel")
        self.assertEqual(grupo_de_familia("Pastel Grande"), "Pastel")
        self.assertEqual(grupo_de_familia("Pastel Mediano"), "Pastel")
        self.assertEqual(grupo_de_familia("Pastel Mini"), "Pastel")
        self.assertEqual(grupo_de_familia("Pastel"), "Pastel")
        self.assertEqual(
            grupo_de_familia("Betún y Rellenos"), "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"
        )

    def test_familias_no_agrupadas_no_se_fusionan(self):
        # Point trae GALLETAS/Galletas y PAN/Pan con distinta capitalización
        # -- no se fusiona automáticamente (decisión explícita: Point es la
        # única fuente, no se inventa normalización de texto).
        self.assertEqual(grupo_de_familia("GALLETAS"), "GALLETAS")
        self.assertEqual(grupo_de_familia("Galletas"), "Galletas")
        self.assertEqual(grupo_de_familia("PAN"), "PAN")
        self.assertEqual(grupo_de_familia("Pan"), "Pan")

    def test_familia_desconocida_es_su_propio_grupo(self):
        self.assertEqual(grupo_de_familia("Bebidas"), "Bebidas")

    def test_familias_del_grupo_expande_correctamente(self):
        self.assertEqual(
            set(familias_del_grupo("Pastel")),
            {"Pastel", "Pastel Chico", "Pastel Grande", "Pastel Mediano", "Pastel Mini"},
        )
        self.assertEqual(familias_del_grupo("Bebidas"), ["Bebidas"])
