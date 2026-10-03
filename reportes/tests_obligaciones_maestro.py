"""La obligación del maestro no puede duplicar una captura que el rubro ya lee."""

from datetime import date
from decimal import Decimal
from io import StringIO

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase

from reportes.models import (
    AreaPresupuesto,
    CategoriaGasto,
    CentroCosto,
    GastoOperativoMensual,
    ObligacionGasto,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from reportes.services_gastos_compromisos import crear_gasto_recurrente


class ObligacionesMaestroTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.usuario = get_user_model().objects.create_superuser("maestro", password="test")
        cls.area = AreaPresupuesto.objects.create(codigo="gastos-venta", nombre="Ventas")
        cls.rubro = RubroPresupuesto.objects.create(
            area=cls.area, concepto="Arrendamiento local", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        cls.centro = CentroCosto.objects.create(
            codigo="MAESTRO", nombre="Centro", tipo=CentroCosto.TIPO_SUCURSAL
        )
        cls.categoria = CategoriaGasto.objects.create(codigo="RENTA", nombre="Renta")
        cls.contrato = crear_gasto_recurrente(
            usuario=cls.usuario,
            area=cls.area,
            rubro=cls.rubro,
            centro_costo=cls.centro,
            categoria_gasto=cls.categoria,
            concepto="Arrendamiento local",
            vigencia_inicio=date(2026, 1, 1),
            monto=Decimal("6244.98"),
            dia_vencimiento=5,
            condicion_pago="CONTADO",
        )

    def _regla_de_capturas(self):
        return ReglaFuenteRubro.objects.create(
            rubro=self.rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            modo_asignacion=ReglaFuenteRubro.MODO_CANONICA,
            categoria_gasto=self.categoria,
            centro_costo=self.centro,
            activa=True,
        )

    def _captura_del_excel(self):
        return GastoOperativoMensual.objects.create(
            periodo=date(2026, 1, 1),
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            monto=Decimal("6960.00"),
            external_key="FIJO-2026-01-MAESTRO-RENTA",
        )

    def _generar(self):
        salida = StringIO()
        call_command(
            "generar_obligaciones_maestro",
            "--desde", "2026-01", "--hasta", "2026-01",
            "--categoria", "RENTA",
            "--usuario", "maestro",
            "--apply",
            stdout=salida,
        )
        return salida.getvalue()

    def test_no_genera_cuando_el_rubro_ya_lee_la_captura_de_ese_mes(self):
        self._regla_de_capturas()
        self._captura_del_excel()
        salida = self._generar()
        self.assertEqual(ObligacionGasto.objects.count(), 0)
        self.assertIn("ya lee una captura de ese mes", salida)

    def test_genera_cuando_el_rubro_ya_no_lee_capturas(self):
        # La regla desactivada es el caso de la renta: la captura se queda como
        # historia y el contrato verificado pasa a ser la única fuente.
        regla = self._regla_de_capturas()
        regla.activa = False
        regla.save()
        self._captura_del_excel()
        self._generar()
        obligacion = ObligacionGasto.objects.get()
        self.assertEqual(obligacion.monto_reconocido, Decimal("6244.98"))

    def test_genera_cuando_no_hay_captura_previa(self):
        self._regla_de_capturas()
        self._generar()
        self.assertEqual(ObligacionGasto.objects.get().monto_reconocido, Decimal("6244.98"))

    def test_es_idempotente(self):
        self._generar()
        self._generar()
        self.assertEqual(ObligacionGasto.objects.count(), 1)
