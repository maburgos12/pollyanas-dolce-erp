"""Cobertura del puente compras Point -> kardex de insumos."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from io import StringIO

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.services.inventory_baseline import POINT_ALMACEN_BASELINE_PREFIX


def _compra(insumo, *, source_hash, quantity, unit, fecha=date(2026, 5, 10), **extra):
    raw = {
        "source": "POINT_COMPRAS_HISTORICAS",
        "purchase_id": "999",
        "folio": "F-1",
        "supplier": "PROVEEDOR DEMO",
        "article_name": insumo.nombre,
        "quantity": quantity,
        "unit": unit,
        "unit_cost": "10",
        "match_method": "INDEX",
    }
    raw.update(extra)
    return CostoInsumo.objects.create(
        insumo=insumo, fecha=fecha, costo_unitario=Decimal("10"), source_hash=source_hash, raw=raw
    )


class ImportarComprasPointAKardexTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.gramo = UnidadMedida.objects.create(codigo="g", nombre="Gramo", tipo="MASS", factor_to_base=Decimal("1"))
        UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo="MASS", factor_to_base=Decimal("1000"))
        cls.pza = UnidadMedida.objects.create(codigo="pza", nombre="Pieza", tipo="UNIT", factor_to_base=Decimal("1"))
        cls.harina = Insumo.objects.create(nombre="HARINA NEVADA", unidad_base=cls.gramo)
        cls.caja = Insumo.objects.create(nombre="CAJA CHICA", unidad_base=cls.pza)

    def _run(self, *args):
        out = StringIO()
        call_command("importar_compras_point_a_kardex", *args, stdout=out)
        return out.getvalue()

    def test_convierte_a_unidad_base_y_no_toca_stock(self):
        _compra(self.harina, source_hash="h1", quantity="100.0", unit="KG")

        self.assertIn("Entradas por crear: 1", self._run())
        self.assertFalse(MovimientoInventario.objects.exists())

        self._run("--apply")
        movimiento = MovimientoInventario.objects.get()
        self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_ENTRADA)
        # 100 KG -> 100,000 g. Copiar la cantidad cruda sería un error de 1000x.
        self.assertEqual(movimiento.cantidad, Decimal("100000.000"))
        self.assertEqual(movimiento.almacen, "ALMACEN_1")
        self.assertEqual(movimiento.referencia, "POINT-COMPRA-F-1")
        self.assertEqual(movimiento.trazabilidad["purchase_id"], "999")
        # Es ledger histórico: la conciliación del stock vivo la hace Point.
        self.assertFalse(ExistenciaInsumo.objects.exists())

    def test_es_idempotente(self):
        _compra(self.harina, source_hash="h1", quantity="100.0", unit="KG")
        self._run("--apply")
        salida = self._run("--apply")
        self.assertEqual(MovimientoInventario.objects.count(), 1)
        self.assertIn("Ya existentes (idempotencia): 1", salida)

    def test_bloquea_lo_que_no_puede_convertir_con_certeza(self):
        _compra(self.caja, source_hash="b1", quantity="5.0", unit="KG")            # masa -> pieza
        _compra(self.harina, source_hash="b2", quantity="5.0", unit="TONELADA")    # alias inexistente
        _compra(self.harina, source_hash="b3", quantity="5.0", unit="KG", match_method="FUZZY")
        _compra(self.harina, source_hash="b4", quantity="0", unit="KG")
        _compra(self.harina, source_hash="b5", quantity=None, unit="KG")

        salida = self._run("--apply")

        self.assertEqual(MovimientoInventario.objects.count(), 0)
        self.assertIn("Bloqueadas: 5", salida)
        for razon in (
            "UNIDAD_INCOMPATIBLE",
            "UNIDAD_DESCONOCIDA",
            "MATCH_NO_CONFIABLE",
            "CANTIDAD_NO_POSITIVA",
            "CANTIDAD_INVALIDA",
        ):
            self.assertIn(razon, salida)

    def test_respeta_rango_de_fechas(self):
        _compra(self.harina, source_hash="f1", quantity="1.0", unit="KG", fecha=date(2026, 1, 5))
        _compra(self.harina, source_hash="f2", quantity="1.0", unit="KG", fecha=date(2026, 6, 5))

        self._run("--apply", "--desde", "2026-06-01")

        self.assertEqual(MovimientoInventario.objects.count(), 1)
        self.assertEqual(MovimientoInventario.objects.get().fecha.date(), date(2026, 6, 5))


class BaselineAntiDobleConteoTests(TestCase):
    """El corte de Point ALMACÉN ya absorbió las compras previas: reinyectarlas duplica."""

    @classmethod
    def setUpTestData(cls):
        gramo = UnidadMedida.objects.create(codigo="g", nombre="Gramo", tipo="MASS", factor_to_base=Decimal("1"))
        UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo="MASS", factor_to_base=Decimal("1000"))
        cls.harina = Insumo.objects.create(nombre="HARINA NEVADA", unidad_base=gramo)

    def setUp(self):
        AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_MANUAL,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.make_aware(datetime(2026, 8, 4, 19, 0)),
            finished_at=timezone.make_aware(datetime(2026, 8, 4, 19, 4)),
            message=f"{POINT_ALMACEN_BASELINE_PREFIX}branch=ALMACEN|cutover_at=2026-08-04",
        )
        _compra(self.harina, source_hash="antes", quantity="1.0", unit="KG", fecha=date(2026, 6, 5))
        _compra(self.harina, source_hash="despues", quantity="2.0", unit="KG", fecha=date(2026, 8, 20))

    def _run(self, *args):
        out = StringIO()
        call_command("importar_compras_point_a_kardex", *args, stdout=out)
        return out.getvalue()

    def test_omite_compras_anteriores_al_corte(self):
        salida = self._run("--apply")

        self.assertIn("ANTERIOR_A_BASELINE", salida)
        movimiento = MovimientoInventario.objects.get()
        self.assertEqual(movimiento.cantidad, Decimal("2000.000"))
        self.assertEqual(movimiento.fecha.date(), date(2026, 8, 20))

    def test_ignorar_baseline_permite_reconstruir_el_historico(self):
        self._run("--apply", "--ignorar-baseline")
        self.assertEqual(MovimientoInventario.objects.count(), 2)
