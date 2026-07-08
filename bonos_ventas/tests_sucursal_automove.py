from __future__ import annotations

from decimal import Decimal

from django.test import TestCase
from django.utils import timezone

from bonos_ventas.models import BonoVentasEmpleado, ConfigBonoVentasPeriodo
from core.models import Sucursal
from rrhh.models import Empleado
from rrhh.services_bonos import sincronizar_bonos_operativos_periodo_actual


class BonoVentasSucursalAutomoveTests(TestCase):
    """Editar la sucursal del empleado re-asigna el bono de ventas aunque ya haya
    asistencia; solo se bloquea si hay captura manual real (ajustes/bono_extra)."""

    def _periodo_actual(self):
        hoy = timezone.localdate()
        return ConfigBonoVentasPeriodo.objects.create(mes=hoy.month, anio=hoy.year)

    def test_reasigna_sucursal_aunque_haya_asistencia(self):
        vieja = Sucursal.objects.create(codigo="COL", nombre="Sucursal Colosio", activa=True)
        nueva = Sucursal.objects.create(codigo="LEY", nombre="Sucursal Leyva", activa=True)
        emp = Empleado.objects.create(
            nombre="Vendedora Movida", area="VENTAS", sucursal_ref=nueva,
            activo=True, participa_bonos_ventas=True,
        )
        periodo = self._periodo_actual()
        bono = BonoVentasEmpleado.objects.create(periodo=periodo, empleado=emp, sucursal=vieja, dias_asistencia=5)

        sincronizar_bonos_operativos_periodo_actual(emp)

        bono.refresh_from_db()
        self.assertEqual(bono.sucursal_id, nueva.id)

    def test_no_reasigna_si_hay_captura_manual(self):
        vieja = Sucursal.objects.create(codigo="COL", nombre="Sucursal Colosio", activa=True)
        nueva = Sucursal.objects.create(codigo="LEY", nombre="Sucursal Leyva", activa=True)
        emp = Empleado.objects.create(
            nombre="Vendedora Con Ajuste", area="VENTAS", sucursal_ref=nueva,
            activo=True, participa_bonos_ventas=True,
        )
        periodo = self._periodo_actual()
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo, empleado=emp, sucursal=vieja, bono_extra=Decimal("100.00"),
        )

        sincronizar_bonos_operativos_periodo_actual(emp)

        bono.refresh_from_db()
        self.assertEqual(bono.sucursal_id, vieja.id)  # protegido
        self.assertEqual(bono.bono_extra, Decimal("100.00"))
