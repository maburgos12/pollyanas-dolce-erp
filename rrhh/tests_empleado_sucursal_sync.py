from __future__ import annotations

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
