"""La limpieza no puede borrar historia ni lo que todavía tiene fuente."""

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


class LimpiezaSinFuenteTests(TestCase):
    def setUp(self):
        self.area = AreaPresupuesto.objects.create(codigo="pruebas", nombre="Pruebas")

    def _linea(self, concepto, fuente, *, activo=True, monto="100.00"):
        rubro = RubroPresupuesto.objects.create(
            concepto=concepto, area=self.area, activo=activo
        )
        LineaPresupuestoMensual.objects.create(
            rubro=rubro,
            periodo=date(2026, 5, 1),
            version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            monto_presupuesto=Decimal("0"),
            monto_real=Decimal(monto),
            fuente_real=fuente,
        )
        return rubro

    def _correr(self):
        salida = StringIO()
        call_command("limpiar_reales_sin_fuente", "--anio", "2026", "--apply", stdout=salida)
        return salida.getvalue()

    def test_borra_el_importe_de_un_rubro_que_perdio_su_regla(self):
        rubro = self._linea("Huérfano", "AUTO:GASTO_OPERATIVO")
        self._correr()
        linea = LineaPresupuestoMensual.objects.get(rubro=rubro)
        self.assertIsNone(linea.monto_real)
        self.assertEqual(linea.fuente_real, "")

    def test_no_toca_el_legado_del_excel(self):
        # AUTO:LEGADO nunca tuvo regla: borrarlo perdería la historia.
        rubro = self._linea("Legado", "AUTO:LEGADO")
        self._correr()
        self.assertEqual(
            LineaPresupuestoMensual.objects.get(rubro=rubro).monto_real, Decimal("100.00")
        )

    def test_no_toca_una_captura_manual(self):
        rubro = self._linea("Manual", "MANUAL:yesenia")
        self._correr()
        self.assertEqual(
            LineaPresupuestoMensual.objects.get(rubro=rubro).monto_real, Decimal("100.00")
        )

    def test_no_toca_un_rubro_desactivado(self):
        # Ya no se muestra en ningún reporte: su importe es rastro, no engaño.
        rubro = self._linea("Desactivado", "AUTO:GASTO_OPERATIVO", activo=False)
        self._correr()
        self.assertEqual(
            LineaPresupuestoMensual.objects.get(rubro=rubro).monto_real, Decimal("100.00")
        )

    def test_no_toca_un_rubro_que_si_tiene_la_regla_viva(self):
        rubro = self._linea("Con regla", "AUTO:GASTO_OPERATIVO")
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            activa=True,
        )
        self._correr()
        self.assertEqual(
            LineaPresupuestoMensual.objects.get(rubro=rubro).monto_real, Decimal("100.00")
        )
