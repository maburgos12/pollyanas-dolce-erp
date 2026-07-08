from __future__ import annotations

from django.test import TestCase
from django.utils import timezone

from bonos_ventas.models import ConfigBonoVentasPeriodo
from rrhh.tasks import reconciliar_bonos_asistencia_periodo_actual


class ReconciliarBonosAsistenciaTaskTests(TestCase):
    """El task de reconciliación diaria corre el recompute de asistencia del periodo vigente."""

    def test_sin_periodos_no_crashea(self):
        res = reconciliar_bonos_asistencia_periodo_actual()
        self.assertTrue(res["ok"])
        self.assertEqual(res["ventas"], "sin periodo")
        self.assertEqual(res["produccion"], "sin periodo")

    def test_con_periodo_ventas_ejecuta_el_sync(self):
        hoy = timezone.localdate()
        ConfigBonoVentasPeriodo.objects.create(mes=hoy.month, anio=hoy.year)

        res = reconciliar_bonos_asistencia_periodo_actual()

        self.assertTrue(res["ok"])
        # El sync devolvió su resumen (dict con contadores), no "sin periodo".
        self.assertIsInstance(res["ventas"], dict)
        self.assertIn("bonos_sincronizados", res["ventas"])
