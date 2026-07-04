from django.test import TestCase

from reportes.mano_obra_grupos_familia import familias_del_grupo, grupo_de_familia
from reportes.models import FamiliaGrupoManoObra


class GrupoDeFamiliaTests(TestCase):
    def setUp(self):
        for familia_real, grupo in [
            ("Pastel", "Pastel"),
            ("Pastel Chico", "Pastel"),
            ("Pastel Grande", "Pastel"),
            ("Pastel Mediano", "Pastel"),
            ("Pastel Mini", "Pastel"),
            ("Betún y Rellenos", "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"),
            ("Betún, Cremas, Rellenos (INSUMO PRODUCIDO)", "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"),
            ("GALLETAS", "GALLETAS"),
            ("Galletas", "Galletas"),
            ("PAN", "PAN"),
            ("Pan", "Pan"),
        ]:
            FamiliaGrupoManoObra.objects.create(familia_real=familia_real, grupo=grupo)

    def test_familias_fusionadas_resuelven_al_grupo_canonico(self):
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

    def test_familia_sin_fila_es_su_propio_grupo(self):
        self.assertEqual(grupo_de_familia("Bebidas"), "Bebidas")

    def test_familias_del_grupo_expande_correctamente(self):
        self.assertEqual(
            set(familias_del_grupo("Pastel")),
            {"Pastel", "Pastel Chico", "Pastel Grande", "Pastel Mediano", "Pastel Mini"},
        )
        self.assertEqual(familias_del_grupo("Bebidas"), ["Bebidas"])

    def test_fusionar_grupo_editando_la_fila_cambia_la_resolucion(self):
        # Simula lo que hace la pantalla de clasificación al fusionar una
        # familia real nueva (ej. "RELLENOS Y CREMAS") a un grupo existente,
        # sin depender de un cambio de código.
        FamiliaGrupoManoObra.objects.create(familia_real="RELLENOS Y CREMAS", grupo="RELLENOS Y CREMAS")
        self.assertEqual(grupo_de_familia("RELLENOS Y CREMAS"), "RELLENOS Y CREMAS")

        FamiliaGrupoManoObra.objects.filter(familia_real="RELLENOS Y CREMAS").update(
            grupo="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"
        )

        self.assertEqual(
            grupo_de_familia("RELLENOS Y CREMAS"), "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"
        )
        self.assertIn("RELLENOS Y CREMAS", familias_del_grupo("Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"))
