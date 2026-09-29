from django.apps import apps
from django.test import SimpleTestCase

from core.management.commands.inventario_fuentes_datos import (
    _candidate,
    _expand_terms,
    _normalize,
)


class InventarioFuentesDatosTests(SimpleTestCase):
    def test_sinonimo_descubre_modelo_sin_declarar_identidad(self):
        terms = _expand_terms([_normalize("Colaborador")])

        self.assertEqual(terms, ["colaborador", "empleado"])
        score, matches = _candidate(apps.get_model("rrhh", "Empleado"), terms)
        self.assertGreater(score, 0)
        self.assertIn("modelo:empleado", matches)

    def test_normaliza_acentos_y_no_duplica_pistas(self):
        terms = _expand_terms([_normalize("Materia prima"), _normalize("Insumo")])

        self.assertEqual(terms, ["materia prima", "insumo"])

    def test_modelos_distintos_siguen_siendo_candidatos_separados(self):
        proveedor = apps.get_model("maestros", "Proveedor")
        proveedor_servicio = apps.get_model("mantenimiento", "ProveedorServicio")

        self.assertGreater(_candidate(proveedor, ["proveedor"])[0], 0)
        self.assertGreater(_candidate(proveedor_servicio, ["proveedor"])[0], 0)
        self.assertNotEqual(proveedor._meta.db_table, proveedor_servicio._meta.db_table)
