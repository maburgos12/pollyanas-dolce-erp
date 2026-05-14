from datetime import date
from decimal import Decimal

from django.test import TestCase

from rrhh.models import Empleado, NominaLinea, NominaPeriodo

from .models import BonoProduccionEmpleado, ConfigBonoPeriodo


class BonosProduccionTests(TestCase):
    def test_recalcular_usa_dias_trabajados_como_base_de_asistencia(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", sucursal="Matriz", area="HORNOS")
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area="HORNOS",
            dias_trabajados=15,
            dias_uniforme=15,
            dias_asistencia=15,
            dias_puntualidad=15,
            dias_produccion=15,
        )

        bono.recalcular()

        self.assertTrue(bono.pasa_asistencia)
        self.assertEqual(bono.total_a_pagar, Decimal("1000.00"))

    def test_aplicar_a_nomina_escribe_total_en_linea_bonos(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", sucursal="Matriz", area="HORNOS")
        periodo_bono = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        nomina = NominaPeriodo.objects.create(fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 31))
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo_bono,
            empleado=empleado,
            area="HORNOS",
            dias_trabajados=15,
            dias_uniforme=15,
            dias_asistencia=15,
            dias_puntualidad=15,
            dias_produccion=15,
        )
        bono.recalcular()
        bono.save()

        updated = periodo_bono.aplicar_a_nomina(nomina)

        self.assertEqual(updated, 1)
        linea = NominaLinea.objects.get(periodo=nomina, empleado=empleado)
        self.assertEqual(linea.dias_trabajados, Decimal("15"))
        self.assertEqual(linea.bonos, Decimal("1000.00"))
