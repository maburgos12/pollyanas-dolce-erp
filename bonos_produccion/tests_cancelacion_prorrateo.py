from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase

from rrhh.models import Empleado

from .models import AREA_HORNOS, BonoProduccionEmpleado, ConfigBonoPeriodo
from .views_html import _parse_decimal


JUL_INICIO = date(2026, 7, 1)
JUL_FIN = date(2026, 7, 27)


def crear_periodo(**kwargs):
    defaults = dict(mes=7, anio=2026, dias_laborables=23, fecha_inicio=JUL_INICIO, fecha_fin=JUL_FIN)
    defaults.update(kwargs)
    return ConfigBonoPeriodo.objects.create(**defaults)


class DiasLaborablesExigiblesTests(TestCase):
    def test_prorratea_mes_en_curso(self):
        periodo = crear_periodo()
        # 25 de 27 días transcurridos -> floor(23 * 25 / 27) = 21
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=date(2026, 7, 25)), 21)

    def test_al_cierre_regresa_total(self):
        periodo = crear_periodo()
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=JUL_FIN), 23)
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=date(2026, 8, 15)), 23)

    def test_antes_de_iniciar_es_cero(self):
        periodo = crear_periodo()
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=date(2026, 6, 30)), 0)

    def test_sin_fechas_usa_mes_calendario(self):
        periodo = crear_periodo(fecha_inicio=None, fecha_fin=None)
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=date(2026, 7, 31)), 23)
        # 15 de 31 días -> floor(23 * 15 / 31) = 11
        self.assertEqual(periodo.dias_laborables_exigibles(hoy=date(2026, 7, 15)), 11)


class RecalcularCancelacionTests(TestCase):
    def crear_bono(self, *, cancela_faltas=True, limite_faltas=1, cancela_retardos=False, limite_retardos=0, **dias):
        periodo = crear_periodo()
        regla = periodo.get_regla_area(AREA_HORNOS)
        regla.cancela_por_asistencia = cancela_faltas
        regla.limite_asistencia_cancelacion = limite_faltas
        regla.cancela_por_puntualidad = cancela_retardos
        regla.limite_retardos_cancelacion = limite_retardos
        regla.save()
        empleado = Empleado.objects.create(nombre="Empleada Hornos", area="HORNOS", fecha_ingreso=date(2026, 1, 1))
        return BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS, **dias)

    def recalcular_en(self, bono, hoy: date):
        with patch("bonos_produccion.models.timezone.localdate", return_value=hoy):
            bono.recalcular()
        bono.save()
        bono.refresh_from_db()
        return bono

    def test_asistencia_perfecta_mes_en_curso_no_cancela_y_paga(self):
        bono = self.crear_bono(
            dias_trabajados=21, dias_asistencia=21, dias_uniforme=21, dias_puntualidad=21, dias_produccion=21
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertFalse(bono.cancela_bono)
        self.assertEqual(bono.cancela_motivo, "")
        self.assertTrue(bono.pasa_asistencia)
        self.assertGreater(bono.total_a_pagar, Decimal("0.00"))

    def test_una_falta_mes_en_curso_cancela_con_motivo_persistido(self):
        bono = self.crear_bono(
            dias_trabajados=20, dias_asistencia=20, dias_uniforme=20, dias_puntualidad=20, dias_produccion=20
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertTrue(bono.cancela_bono)
        self.assertEqual(bono.cancela_motivo, "1 falta (límite 1)")
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_cierre_converge_a_formula_historica(self):
        con_falta = self.crear_bono(
            dias_trabajados=22, dias_asistencia=22, dias_uniforme=22, dias_puntualidad=22, dias_produccion=22
        )
        con_falta = self.recalcular_en(con_falta, JUL_FIN)
        self.assertTrue(con_falta.cancela_bono)

        perfecto = BonoProduccionEmpleado.objects.create(
            periodo=con_falta.periodo,
            empleado=Empleado.objects.create(nombre="Perfecta", area="HORNOS", fecha_ingreso=date(2026, 1, 1)),
            area=AREA_HORNOS,
            dias_trabajados=23,
            dias_asistencia=23,
            dias_uniforme=23,
            dias_puntualidad=23,
            dias_produccion=23,
        )
        perfecto = self.recalcular_en(perfecto, JUL_FIN)
        self.assertFalse(perfecto.cancela_bono)
        self.assertGreater(perfecto.total_a_pagar, Decimal("0.00"))

    def test_limite_cero_deja_la_regla_inactiva(self):
        bono = self.crear_bono(
            limite_faltas=0,
            dias_trabajados=15,
            dias_asistencia=15,
            dias_uniforme=15,
            dias_puntualidad=15,
            dias_produccion=15,
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertFalse(bono.cancela_bono)

    def test_nueve_llegadas_tarde_se_convierten_en_falta_y_cancelan(self):
        # política escalonada: 3 llegadas tarde = 1 retardo formal;
        # 3 retardos formales = 1 falta; 1 falta cancela (límite 1)
        bono = self.crear_bono(
            dias_trabajados=21,
            dias_asistencia=21,
            dias_uniforme=21,
            dias_puntualidad=12,
            dias_produccion=21,
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertTrue(bono.cancela_bono)
        self.assertEqual(bono.cancela_motivo, "1 falta (límite 1), incluye 1 por retardos")
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_ocho_llegadas_tarde_no_llegan_a_falta_y_paga(self):
        bono = self.crear_bono(
            dias_trabajados=21,
            dias_asistencia=21,
            dias_uniforme=21,
            dias_puntualidad=13,
            dias_produccion=21,
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertFalse(bono.cancela_bono)
        # pierde el concepto de puntualidad (8 retardos > límite 2) pero cobra el resto
        self.assertFalse(bono.pasa_puntualidad)
        self.assertGreater(bono.total_a_pagar, Decimal("0.00"))

    def test_falta_real_y_faltas_por_retardos_se_suman_en_motivo(self):
        bono = self.crear_bono(
            dias_trabajados=20,
            dias_asistencia=20,
            dias_uniforme=20,
            dias_puntualidad=11,
            dias_produccion=20,
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertTrue(bono.cancela_bono)
        self.assertEqual(bono.cancela_motivo, "2 faltas (límite 1), incluye 1 por retardos")

    def test_cancela_por_retardos_con_motivo(self):
        bono = self.crear_bono(
            cancela_faltas=False,
            cancela_retardos=True,
            limite_retardos=2,
            dias_trabajados=21,
            dias_asistencia=21,
            dias_uniforme=21,
            dias_puntualidad=19,
            dias_produccion=21,
        )
        bono = self.recalcular_en(bono, date(2026, 7, 25))
        self.assertTrue(bono.cancela_bono)
        self.assertEqual(bono.cancela_motivo, "2 retardos (límite 2)")
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))


class ParseDecimalTests(TestCase):
    def test_campo_vacio_o_invalido_conserva_valor_actual(self):
        actual = Decimal("300.00")
        self.assertEqual(_parse_decimal("", actual), actual)
        self.assertEqual(_parse_decimal("   ", actual), actual)
        self.assertEqual(_parse_decimal(None, actual), actual)
        self.assertEqual(_parse_decimal("abc", actual), actual)

    def test_valores_explicitos_se_respetan(self):
        self.assertEqual(_parse_decimal("12.50", Decimal("300.00")), Decimal("12.50"))
        self.assertEqual(_parse_decimal("0", Decimal("300.00")), Decimal("0"))
