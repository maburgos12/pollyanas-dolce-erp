"""Cobertura de la extracción de compras Point con cantidad."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from io import StringIO
from unittest.mock import patch

from django.core.management import CommandError, call_command
from django.test import TestCase

from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.services.point_purchase_cost_import_service import PointPurchaseCostImportService
from pos_bridge.services.point_purchase_extraction_service import (
    PointPurchaseExtractionService,
    PurchaseExtractionResult,
    _parse_purchase_date,
)

COMPRA = {
    "purchase_id": "1659352",
    "folio": "13283",
    "branch": "Almacen",
    "supplier": "Hielo y Agua Mar de Cortez, S. A. de C. V.",
    "purchase_date": date(2026, 8, 29),
    "lines": [
        {
            "articulo": "AGUA",
            "cantidad": 114.0,
            "unidad": "Litro",
            "costo_unitario": 0.8947368421052632,
            "costo_total": 102.0,
            "raw": {"Articulo": "AGUA", "Cantidad": 114.0},
        }
    ],
}


class PersistPurchasesTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        litro = UnidadMedida.objects.create(
            codigo="lt", nombre="Litro", tipo="VOLUME", factor_to_base=Decimal("1000")
        )
        cls.agua = Insumo.objects.create(nombre="AGUA", unidad_base=litro)

    def test_persiste_en_el_formato_que_consume_el_puente_al_kardex(self):
        result = PointPurchaseCostImportService().persist_purchases([COMPRA])

        self.assertEqual(result.created, 1)
        costo = CostoInsumo.objects.get()
        self.assertEqual(costo.insumo, self.agua)
        self.assertEqual(costo.fecha, date(2026, 8, 29))
        # El puente al kardex lee exactamente estas llaves.
        self.assertEqual(costo.raw["source"], "POINT_COMPRAS_HISTORICAS")
        self.assertEqual(costo.raw["quantity"], 114.0)
        self.assertEqual(costo.raw["unit"], "Litro")
        self.assertEqual(costo.raw["folio"], "13283")
        self.assertEqual(costo.raw["purchase_id"], "1659352")

    def test_es_idempotente_entre_corridas(self):
        PointPurchaseCostImportService().persist_purchases([COMPRA])
        result = PointPurchaseCostImportService().persist_purchases([COMPRA])

        self.assertEqual(CostoInsumo.objects.count(), 1)
        self.assertEqual(result.created, 0)
        self.assertEqual(result.existing, 1)

    def test_reporta_articulos_sin_insumo_en_vez_de_inventarlos(self):
        compra = {**COMPRA, "lines": [{**COMPRA["lines"][0], "articulo": "ARTICULO FANTASMA"}]}

        result = PointPurchaseCostImportService().persist_purchases([compra])

        self.assertEqual(result.created, 0)
        self.assertEqual(result.unresolved, 1)
        self.assertEqual(result.unresolved_articles, ["ARTICULO FANTASMA"])
        self.assertFalse(CostoInsumo.objects.exists())


class ExtraerComprasPointCommandTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        litro = UnidadMedida.objects.create(
            codigo="lt", nombre="Litro", tipo="VOLUME", factor_to_base=Decimal("1000")
        )
        Insumo.objects.create(nombre="AGUA", unidad_base=litro)

    def _run(self, *args):
        out = StringIO()
        with patch.object(
            PointPurchaseExtractionService,
            "fetch_purchases",
            return_value=([COMPRA], PurchaseExtractionResult(purchases_seen=3, purchases_kept=1, lines_kept=1)),
        ):
            call_command("extraer_compras_point", *args, stdout=out)
        return out.getvalue()

    def test_dry_run_no_escribe(self):
        salida = self._run("--desde", "2026-08-01", "--hasta", "2026-08-31")

        self.assertIn("DRY-RUN", salida)
        self.assertFalse(CostoInsumo.objects.exists())

    def test_apply_persiste(self):
        salida = self._run("--desde", "2026-08-01", "--hasta", "2026-08-31", "--apply")

        self.assertIn("Costos creados      : 1", salida)
        self.assertEqual(CostoInsumo.objects.count(), 1)

    def test_rechaza_rango_invertido(self):
        with self.assertRaises(CommandError):
            call_command("extraer_compras_point", "--desde", "2026-09-01", "--hasta", "2026-08-01")


class ParsePurchaseDateTests(TestCase):
    def test_lee_el_formato_que_devuelve_point(self):
        self.assertEqual(_parse_purchase_date("2026-08-29T07:00:00"), date(2026, 8, 29))

    def test_tolera_valores_vacios_o_ilegibles(self):
        self.assertIsNone(_parse_purchase_date(""))
        self.assertIsNone(_parse_purchase_date(None))
        self.assertIsNone(_parse_purchase_date("29/08/2026"))
