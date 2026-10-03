"""Abrir renglones no puede pisar presupuesto autorizado ni inventar rubros."""

from datetime import date
from decimal import Decimal
from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from reportes.models import (
    AreaPresupuesto,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)


class AbrirLineasTests(TestCase):
    def setUp(self):
        self.area = AreaPresupuesto.objects.create(codigo="pruebas", nombre="Pruebas")

    def _rubro(self, concepto, *, con_regla=True, activo=True):
        rubro = RubroPresupuesto.objects.create(
            concepto=concepto, area=self.area, activo=activo
        )
        if con_regla:
            ReglaFuenteRubro.objects.create(
                rubro=rubro,
                tipo_fuente=ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO,
                modo_asignacion=ReglaFuenteRubro.MODO_CANONICA,
                activa=True,
            )
        return rubro

    def _correr(self, *extra):
        salida = StringIO()
        call_command(
            "abrir_lineas_presupuesto",
            "--desde", "2026-01", "--hasta", "2026-12",
            *extra, stdout=salida,
        )
        return salida.getvalue()

    def _lineas(self, rubro):
        return LineaPresupuestoMensual.objects.filter(rubro=rubro, periodo__year=2026)

    def test_abre_los_doce_meses_del_rubro_con_fuente(self):
        rubro = self._rubro("Arrendamiento local")
        self._correr("--apply")
        self.assertEqual(self._lineas(rubro).count(), 12)
        self.assertEqual(
            {l.monto_presupuesto for l in self._lineas(rubro)}, {Decimal("0")}
        )
        self.assertTrue(all(l.monto_real is None for l in self._lineas(rubro)))

    def test_no_toca_el_rubro_sin_fuente(self):
        rubro = self._rubro("Sin fuente", con_regla=False)
        self._correr("--apply")
        self.assertEqual(self._lineas(rubro).count(), 0)

    def test_no_toca_el_rubro_desactivado(self):
        rubro = self._rubro("Desactivado", activo=False)
        self._correr("--apply")
        self.assertEqual(self._lineas(rubro).count(), 0)

    def test_no_pisa_el_presupuesto_autorizado(self):
        rubro = self._rubro("Con presupuesto")
        LineaPresupuestoMensual.objects.create(
            rubro=rubro,
            periodo=date(2026, 3, 1),
            version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            monto_presupuesto=Decimal("5000.00"),
            monto_real=Decimal("4800.00"),
            fuente_real="AUTO:OBLIGACION_GASTO",
        )
        self._correr("--apply")
        marzo = self._lineas(rubro).get(periodo=date(2026, 3, 1))
        self.assertEqual(marzo.monto_presupuesto, Decimal("5000.00"))
        self.assertEqual(marzo.monto_real, Decimal("4800.00"))
        self.assertEqual(self._lineas(rubro).count(), 12)

    def test_respeta_el_rango_de_meses(self):
        # Crucero cerró en junio y Bamoa la reemplazó en julio: cada una vive la
        # mitad del año y abrirle doce meses inventa vida que no tuvo.
        rubro = self._rubro("Media vida")
        salida = StringIO()
        call_command(
            "abrir_lineas_presupuesto",
            "--desde", "2026-01", "--hasta", "2026-06", "--apply",
            stdout=salida,
        )
        self.assertEqual(
            sorted(l.periodo.month for l in self._lineas(rubro)), [1, 2, 3, 4, 5, 6]
        )

    def test_el_simulacro_no_escribe(self):
        rubro = self._rubro("Simulacro")
        salida = self._correr()
        self.assertEqual(self._lineas(rubro).count(), 0)
        self.assertIn("SIMULACRO", salida)

    def test_es_idempotente(self):
        rubro = self._rubro("Idempotente")
        self._correr("--apply")
        self._correr("--apply")
        self.assertEqual(self._lineas(rubro).count(), 12)

    def test_filtra_por_tipo_de_fuente(self):
        rubro = self._rubro("Obligación")
        self._correr("--tipo-fuente", "VENTA_POS", "--apply")
        self.assertEqual(self._lineas(rubro).count(), 0)
