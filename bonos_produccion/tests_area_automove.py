from __future__ import annotations

from decimal import Decimal

from django.test import TestCase
from django.utils import timezone

from bonos_produccion.models import BonoProduccionEmpleado, ConfigBonoPeriodo
from rrhh.models import Empleado
from rrhh.services_bonos import sincronizar_bonos_operativos_periodo_actual


class BonoProduccionAreaAutomoveTests(TestCase):
    """Editar el grupo del empleado re-clasifica el área del bono aunque ya haya
    asistencia sincronizada; solo se bloquea si hay captura manual real."""

    def _periodo_actual(self):
        hoy = timezone.localdate()
        return ConfigBonoPeriodo.objects.create(mes=hoy.month, anio=hoy.year)

    def _empleado_logistica(self, nombre):
        return Empleado.objects.create(
            nombre=nombre, puesto_operativo="ENVIO_SUCURSAL", area="ENVIO A SUCURSAL",
            activo=True, participa_bonos_produccion=True,
        )

    def test_mueve_area_aunque_haya_asistencia(self):
        periodo = self._periodo_actual()
        emp = self._empleado_logistica("Repartidor Con Asistencia")
        # Bono con área vieja + asistencia sincronizada, SIN captura manual.
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo, empleado=emp, area="PRODUCCION", dias_asistencia=5, dias_trabajados=5,
        )

        sincronizar_bonos_operativos_periodo_actual(emp)

        bono.refresh_from_db()
        self.assertEqual(bono.area, "LOGISTICA")

    def test_no_mueve_si_hay_captura_manual(self):
        periodo = self._periodo_actual()
        emp = self._empleado_logistica("Repartidor Con Ajuste")
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo, empleado=emp, area="PRODUCCION", bono_extra=Decimal("100.00"),
        )

        sincronizar_bonos_operativos_periodo_actual(emp)

        bono.refresh_from_db()
        self.assertEqual(bono.area, "PRODUCCION")  # protegido: captura manual
        self.assertEqual(bono.bono_extra, Decimal("100.00"))
