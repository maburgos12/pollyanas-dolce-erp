from __future__ import annotations

from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from core.models import Sucursal


class CanonizarCatalogoSucursalesCommandTests(TestCase):
    def test_dry_run_reporta_sin_escribir(self):
        Sucursal.objects.create(codigo="EL_TUNEL", nombre="El Túnel", activa=True)
        Sucursal.objects.update_or_create(codigo="CEDIS", defaults={"nombre": "CEDIS", "activa": True})
        stdout = StringIO()

        call_command("canonizar_catalogo_sucursales", stdout=stdout)

        self.assertIn("Sucursal El Túnel", stdout.getvalue())
        self.assertIn("Dry-run: sin cambios", stdout.getvalue())
        self.assertEqual(Sucursal.objects.get(codigo="EL_TUNEL").nombre, "El Túnel")
        self.assertEqual(Sucursal.objects.get(codigo="CEDIS").nombre, "CEDIS")

    def test_apply_canoniza_nombres_del_catalogo_point(self):
        Sucursal.objects.create(codigo="EL_TUNEL", nombre="El Túnel", activa=True)
        Sucursal.objects.create(codigo="LAS_GLORIAS", nombre="Las Glorias", activa=True)
        Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)
        Sucursal.objects.create(codigo="CRUCERO", nombre="Sucursal Crucero", activa=True)
        stdout = StringIO()

        call_command("canonizar_catalogo_sucursales", "--apply", stdout=stdout)

        self.assertIn("3 sucursales actualizadas", stdout.getvalue())
        self.assertEqual(Sucursal.objects.get(codigo="EL_TUNEL").nombre, "Sucursal El Túnel")
        self.assertEqual(Sucursal.objects.get(codigo="LAS_GLORIAS").nombre, "Sucursal Las Glorias")
        self.assertEqual(Sucursal.objects.get(codigo="MATRIZ").nombre, "Sucursal Matriz")
        self.assertEqual(Sucursal.objects.get(codigo="CRUCERO").nombre, "Sucursal Bamoa")
