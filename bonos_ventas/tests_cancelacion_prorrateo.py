from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase

from core.models import Sucursal
from rrhh.models import Empleado

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo
from .views_html import _parse_decimal


JUL_INICIO = date(2026, 7, 1)
JUL_FIN = date(2026, 7, 27)


class CancelacionProrrateoVentasTests(TestCase):
    def crear_bono(self, *, cancela_faltas=True, limite_faltas=1, **dias):
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=7,
            anio=2026,
            dias_laborables=23,
            fecha_inicio=JUL_INICIO,
            fecha_fin=JUL_FIN,
            cancela_por_asistencia=cancela_faltas,
            limite_asistencia_cancelacion=limite_faltas,
        )
        sucursal = Sucursal.objects.create(codigo="S01", nombre="Sucursal Centro", activa=True)
        empleado = Empleado.objects.create(
            nombre="Vendedora Centro", area="VENTAS", sucursal=sucursal.nombre, fecha_ingreso=date(2026, 1, 1)
        )
        return BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal, **dias)

    def recalcular_en(self, bono, hoy: date):
        with patch("bonos_ventas.models.timezone.localdate", return_value=hoy):
            bono.recalcular()
        bono.save()
        bono.refresh_from_db()
        return bono

    def test_dias_exigibles_prorratea_y_converge(self):
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=7, anio=2026, dias_laborables=23, fecha_inicio=JUL_INICIO, fecha_fin=JUL_FIN
        )
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=date(2026, 7, 25)), 21)
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=JUL_FIN), 23)
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=date(2026, 6, 30)), 0)

    def test_asistencia_perfecta_mes_en_curso_no_cancela_y_paga(self):
        bono = self.crear_bono(
            dias_trabajados=21, dias_asistencia=21, dias_uniforme=21, dias_puntualidad=21
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertFalse(bono.cancela_bono)
        self.assertGreater(bono.total_a_pagar, Decimal("0.00"))

    def test_una_falta_mes_en_curso_cancela_con_motivo_persistido(self):
        bono = self.crear_bono(
            dias_trabajados=20, dias_asistencia=20, dias_uniforme=20, dias_puntualidad=20
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertTrue(bono.cancela_bono)
        self.assertEqual(bono.cancela_motivo, "1 falta (límite 1)")
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_limite_cero_deja_la_regla_inactiva(self):
        bono = self.crear_bono(
            limite_faltas=0, dias_trabajados=15, dias_asistencia=15, dias_uniforme=15, dias_puntualidad=15
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertFalse(bono.cancela_bono)

    def test_nueve_llegadas_tarde_se_convierten_en_falta_y_cancelan(self):
        bono = self.crear_bono(
            dias_trabajados=21, dias_asistencia=21, dias_uniforme=21, dias_puntualidad=12
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertTrue(bono.cancela_bono)
        self.assertEqual(bono.cancela_motivo, "1 falta (límite 1), incluye 1 por retardos")
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_ocho_llegadas_tarde_no_llegan_a_falta_y_paga(self):
        bono = self.crear_bono(
            dias_trabajados=21, dias_asistencia=21, dias_uniforme=21, dias_puntualidad=13
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertFalse(bono.cancela_bono)
        self.assertGreater(bono.total_a_pagar, Decimal("0.00"))

    def test_parse_decimal_conserva_valor_actual(self):
        actual = Decimal("300.00")
        self.assertEqual(_parse_decimal("", actual), actual)
        self.assertEqual(_parse_decimal(None, actual), actual)
        self.assertEqual(_parse_decimal("12.50", actual), Decimal("12.50"))
