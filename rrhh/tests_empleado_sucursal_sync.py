from __future__ import annotations

from io import StringIO

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from bonos_ventas.models import BonoVentasEmpleado, ConfigBonoVentasPeriodo
from core.models import Sucursal
from rrhh.importers import _sucursal_de_empleado
from rrhh.models import Empleado
from rrhh.services_bonos import sincronizar_bonos_operativos_periodo_actual


class EmpleadoSucursalSyncTests(TestCase):
    """FASE 2: los caminos de sync empleado->sucursal usan el FK canónico, no el nombre exacto."""

    def _periodo_ventas_actual(self):
        hoy = timezone.localdate()
        return ConfigBonoVentasPeriodo.objects.create(mes=hoy.month, anio=hoy.year)

    def test_sync_bono_ventas_usa_sucursal_ref_pese_a_texto_viejo(self):
        suc = Sucursal.objects.create(codigo="PAYAN", nombre="Sucursal Payan", activa=True)
        emp = Empleado.objects.create(
            nombre="Vendedora Payan", area="VENTAS", sucursal="Payán",  # texto viejo que NO macha exacto
            sucursal_ref=suc, activo=True, participa_bonos_ventas=True,
        )
        self._periodo_ventas_actual()

        sincronizar_bonos_operativos_periodo_actual(emp)

        self.assertEqual(BonoVentasEmpleado.objects.get(empleado=emp).sucursal_id, suc.id)

    def test_sync_bono_ventas_cae_al_resolver_si_no_hay_fk(self):
        suc = Sucursal.objects.create(codigo="GUAMUCHIL", nombre="Sucursal Guamuchil", activa=True)
        emp = Empleado.objects.create(
            nombre="Vendedora Guamuchil", area="VENTAS", sucursal="Guamúchil",  # sin FK, texto viejo/acentuado
            activo=True, participa_bonos_ventas=True,
        )
        self._periodo_ventas_actual()

        sincronizar_bonos_operativos_periodo_actual(emp)

        self.assertEqual(BonoVentasEmpleado.objects.get(empleado=emp).sucursal_id, suc.id)

    def test_sucursal_de_empleado_importer_usa_fk(self):
        suc = Sucursal.objects.create(codigo="LEYVA", nombre="Sucursal Leyva", activa=True)
        emp = Empleado.objects.create(nombre="X", area="VENTAS", sucursal="texto que no macha", sucursal_ref=suc)

        self.assertEqual(_sucursal_de_empleado(emp), suc)

    def test_sucursal_display_prefiere_fk_y_cae_al_texto(self):
        suc = Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)
        con_fk = Empleado.objects.create(nombre="Con FK", area="VENTAS", sucursal="Crucero", sucursal_ref=suc)
        sin_fk = Empleado.objects.create(nombre="Sin FK", area="VENTAS", sucursal="Guamúchil")
        vacio = Empleado.objects.create(nombre="Vacio", area="VENTAS", sucursal="")

        self.assertEqual(con_fk.sucursal_display, "Sucursal Matriz")  # FK gana sobre texto viejo
        self.assertEqual(sin_fk.sucursal_display, "Guamúchil")        # cae al texto legacy
        self.assertEqual(vacio.sucursal_display, "")

    def test_canonizar_empleado_sucursal_dry_run_no_escribe(self):
        Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)
        empleado = Empleado.objects.create(nombre="Legacy Matriz", area="VENTAS", sucursal="Matriz")
        out = StringIO()

        call_command("canonizar_empleado_sucursal", stdout=out)

        empleado.refresh_from_db()
        self.assertEqual(empleado.sucursal, "Matriz")
        self.assertIsNone(empleado.sucursal_ref_id)
        self.assertIn("Legacy Matriz", out.getvalue())
        self.assertIn("Sucursal Matriz", out.getvalue())

    def test_canonizar_empleado_sucursal_apply_canoniza_texto_y_fk(self):
        suc = Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)
        empleado = Empleado.objects.create(nombre="Legacy Matriz", area="VENTAS", sucursal="Matriz")

        call_command("canonizar_empleado_sucursal", "--apply")

        empleado.refresh_from_db()
        self.assertEqual(empleado.sucursal, "Sucursal Matriz")
        self.assertEqual(empleado.sucursal_ref_id, suc.id)

    def test_canonizar_empleado_sucursal_no_adivina_no_resueltos(self):
        Empleado.objects.create(nombre="Sucursal Rara", area="VENTAS", sucursal="Sucursal Inventada")
        out = StringIO()

        call_command("canonizar_empleado_sucursal", stdout=out)

        text = out.getvalue()
        self.assertIn("No resolvió contra core.Sucursal", text)
        self.assertIn("Sucursal Inventada", text)
