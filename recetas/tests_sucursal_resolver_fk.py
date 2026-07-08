from __future__ import annotations

from django.test import TestCase

from core.models import Sucursal
from recetas.views.reabasto import _resolve_sucursal_for_sales


class ResolveSucursalForSalesTests(TestCase):
    """FASE 2: el import de ventas resuelve sucursal por el resolver canónico (no nombre exacto)."""

    def test_resuelve_por_nombre_viejo_acentuado(self):
        suc = Sucursal.objects.create(codigo="PAYAN", nombre="Sucursal Payan", activa=True)
        # Nombre viejo/acentuado debe resolver al catálogo renombrado.
        self.assertEqual(_resolve_sucursal_for_sales("Payán", "", None), suc)

    def test_codigo_tiene_prioridad(self):
        suc = Sucursal.objects.create(codigo="LEYVA", nombre="Sucursal Leyva", activa=True)
        self.assertEqual(_resolve_sucursal_for_sales("texto que no macha", "LEYVA", None), suc)

    def test_cae_al_default_si_no_macha(self):
        default = Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)
        self.assertEqual(_resolve_sucursal_for_sales("inexistente", "", default), default)
