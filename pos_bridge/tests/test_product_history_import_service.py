from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from django.test import SimpleTestCase, TestCase

from recetas.models import Receta, RecetaCostoSemanal
from pos_bridge.models import PointProductCostReconciliation, PointProductHistoryImport, PointProductHistoryRow
from pos_bridge.services.product_history_import_service import PointProductHistoryImportService


def _build_history_workbook(path: Path) -> None:
    rows = [
        [None, None, None, None, None, None, None, None, None],
        [None, None, "HISTORIAL DE MOVIMIENTOS DE PASTEL FRESAS CON CREMA MINI\nCEDIS\n27/Mar/2026", None, None, None, None, None, None],
        [None, None, None, None, None, None, None, None, None],
        ["FECHA", None, None, "MOVIMIENTO", "EXISTENCIA ANTERIOR", "CANTIDAD", "EXISTENCIA NUEVA", "COSTO", "CANCELADO"],
        [datetime(2026, 3, 26, 23, 35, 28), None, None, "ENTRADA POR PRODUCCION", -2, 10, 8, 417.636233, "NO"],
        [datetime(2026, 3, 26, 20, 39, 53), None, None, "SALIDA POR TRANSFERENCIA", -1, 1, -2, 41.962823, "NO"],
    ]
    dataframe = pd.DataFrame(rows)
    dataframe.to_excel(path, index=False, header=False)


class PointProductHistoryImportServiceParseTests(SimpleTestCase):
    def test_parse_report_extracts_title_and_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "historial.xlsx"
            _build_history_workbook(path)

            parsed = PointProductHistoryImportService().parse_report(report_path=str(path))

            self.assertEqual(parsed.product_name, "Pastel Fresas Con Crema Mini")
            self.assertEqual(parsed.branch_name, "CEDIS")
            self.assertEqual(str(parsed.report_date), "2026-03-27")
            self.assertEqual(len(parsed.rows), 2)
            self.assertEqual(parsed.rows[0].movement_type, "ENTRADA POR PRODUCCION")
            self.assertEqual(str(parsed.rows[0].unit_cost), "41.763623")


class PointProductHistoryImportServicePersistenceTests(TestCase):
    def test_import_report_creates_staging_and_reconciliation(self):
        receta = Receta.objects.create(
            nombre="Pastel Fresas Con Crema Mini",
            codigo_point="PFCMINI",
            hash_contenido="hash_historial_pfcmini",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        RecetaCostoSemanal.objects.create(
            scope_type=RecetaCostoSemanal.SCOPE_RECIPE,
            identity_key=f"RECIPE:{receta.id}",
            label=receta.nombre,
            week_start=datetime(2026, 3, 23).date(),
            week_end=datetime(2026, 3, 29).date(),
            receta=receta,
            temporalidad=receta.temporalidad,
            temporalidad_detalle=receta.temporalidad_detalle,
            familia=receta.familia,
            categoria=receta.categoria,
            costo_mp="58.806218",
            costo_mo="0",
            costo_indirecto="0",
            costo_total="58.806218",
            metadata={},
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "historial.xlsx"
            _build_history_workbook(path)

            import_record, created = PointProductHistoryImportService().import_report(report_path=str(path))

        self.assertTrue(created)
        self.assertTrue(PointProductHistoryImport.objects.filter(id=import_record.id).exists())
        self.assertEqual(PointProductHistoryRow.objects.filter(import_record=import_record).count(), 2)
        reconciliation = PointProductCostReconciliation.objects.get(import_record=import_record)
        self.assertEqual(reconciliation.status, PointProductCostReconciliation.STATUS_DELTA)
        self.assertEqual(str(reconciliation.erp_unit_cost), "58.806218")
        self.assertEqual(import_record.receta.nombre, "Pastel Fresas Con Crema Mini")
