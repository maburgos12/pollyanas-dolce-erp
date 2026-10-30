"""El recibo bimestral se reparte sin perder centavos y no se duplica."""

from decimal import Decimal

from django.test import SimpleTestCase

from reportes.management.commands.importar_recibos_cfe import _meses_entre, _mes
from datetime import date


class ProrrateoRecibosTests(SimpleTestCase):
    def test_un_recibo_bimestral_cubre_sus_dos_meses(self):
        self.assertEqual(
            _meses_entre(date(2026, 7, 1), date(2026, 8, 1)),
            [date(2026, 7, 1), date(2026, 8, 1)],
        )

    def test_la_cobertura_cruza_el_fin_de_ano(self):
        self.assertEqual(
            _meses_entre(date(2026, 12, 1), date(2027, 1, 1)),
            [date(2026, 12, 1), date(2027, 1, 1)],
        )

    def test_el_reparto_no_pierde_centavos(self):
        # 3 meses de un importe que no divide exacto: el sobrante va al primero.
        total = Decimal("10000.00")
        meses = _meses_entre(date(2026, 1, 1), date(2026, 3, 1))
        parte = (total / len(meses)).quantize(Decimal("0.01"))
        restante = total - parte * len(meses)
        repartido = [parte + restante] + [parte] * (len(meses) - 1)
        self.assertEqual(sum(repartido), total)

    def test_cobertura_vacia_no_es_un_mes(self):
        self.assertIsNone(_mes(""))
        self.assertEqual(_mes("2026-07"), date(2026, 7, 1))


from types import SimpleNamespace
from django.test import TestCase
from django.core.management.base import CommandError
from django.utils import timezone
from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from reportes.management.commands.importar_recibos_cfe import Command


class ImportacionCfeTests(TestCase):
    def setUp(self):
        self.cat = CategoriaGasto.objects.create(codigo="LUZ_SUC", nombre="Luz")
        self.centro = CentroCosto.objects.create(codigo="CFE-TEST", nombre="Centro", tipo="SUCURSAL_VENTA")
        self.factura = SimpleNamespace(total=Decimal("100.01"), uuid="A", fecha_emision=timezone.now())
        self.recibo = {"centro": self.centro.codigo, "no_servicio": "123", "inicio": date(2026, 7, 1), "fin": date(2026, 8, 1), "evidencia": "Recibo", "archivo_soporte": "recibo.pdf", "uuid": "A"}

    def test_reimportacion_conserva_total_y_cobertura_de_cada_parte(self):
        cmd = Command()
        for _ in range(2):
            cmd._asignar([self.factura], {self.factura.total: self.recibo}, self.cat)
        filas = list(GastoOperativoMensual.objects.all())
        self.assertEqual(len(filas), 2)
        self.assertEqual(sum(g.monto for g in filas), Decimal("100.01"))
        for g in filas:
            self.assertEqual(g.cobertura_mes_inicio, g.periodo)
            self.assertEqual(g.cobertura_mes_fin, g.periodo)
            self.assertTrue(g.es_estimado)
            self.assertEqual(g.archivo_soporte, "recibo.pdf")

    def test_importe_ambiguo_exige_uuid_y_uuid_distinto_no_asigna(self):
        otra = SimpleNamespace(total=self.factura.total, uuid="B", fecha_emision=timezone.now())
        self.recibo["uuid"] = ""
        with self.assertRaises(CommandError):
            Command()._asignar([self.factura, otra], {self.factura.total: self.recibo}, self.cat)
        self.recibo["uuid"] = "A"
        asignadas, sin_recibo, _ = Command()._asignar([self.factura, otra], {self.factura.total: self.recibo}, self.cat)
        self.assertEqual(len(asignadas), 1)
        self.assertEqual(sin_recibo, [otra])

    def test_cobertura_invertida_no_escribe(self):
        self.recibo["fin"] = date(2026, 6, 1)
        with self.assertRaises(CommandError):
            Command()._asignar([self.factura], {self.factura.total: self.recibo}, self.cat)
        self.assertFalse(GastoOperativoMensual.objects.exists())
