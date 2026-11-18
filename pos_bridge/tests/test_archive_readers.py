import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase

from pos_bridge.services.archive_access import ArchiveStore, ArchivePending, entry_id
from pos_bridge.services.product_history_import_service import PointProductHistoryImportService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_report_reconciliation_service import SalesReportReconciliationService
from pos_bridge.tests.test_product_history_import_service import _build_history_workbook


class ArchiveReaderTests(SimpleTestCase):
    def test_archived_product_history_recovers_same_logical_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            sources = root / 'raw_exports'
            sources.mkdir()
            original = sources / 'producthistory_1.xls'
            staged = root / 'history.xlsx'
            _build_history_workbook(staged)
            store = ArchiveStore(root=root / 'archive', sources_root=sources, wait_seconds=0)
            snapshot = store.snapshot(staged)
            entry = dict(schema=1, relative_path=original.name, **snapshot,
                         id=entry_id(original.name, snapshot['sha256']),
                         nas_key=f"point-archive/objects/{snapshot['sha256']}.xls",
                         verified_at=1700000000, consumers=['product_history'])
            store.register(entry)
            with patch('pos_bridge.services.product_history_import_service.open_source', store.open_source):
                with self.assertRaises(ArchivePending):
                    PointProductHistoryImportService().parse_report(report_path=str(original))
                store.publish_restored(entry, staged)
                parsed = PointProductHistoryImportService().parse_report(report_path=str(original))
            self.assertEqual(parsed.report_path, str(original))
            self.assertEqual(parsed.source_filename, original.name)
            self.assertEqual(parsed.file_hash, snapshot['sha256'])
            self.assertEqual(len(parsed.rows), 2)

    def test_category_and_reconciliation_use_restored_xls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            sources = root / 'raw_exports'
            sources.mkdir()
            original = sources / 'point_sales_category_1.xls'
            staged = root / 'category.xlsx'
            with pd.ExcelWriter(staged) as writer:
                pd.DataFrame([['CATEGORÍA', None, None, 'CÓDIGO', 'PRODUCTO', 'CANTIDAD', 'BRUTO', None, 'DESCUENTOS', 'VENTA', 'IMPUESTOS', 'VENTA NETA'], ['Pasteles', None, None, '100', 'Pastel', 2, 50, None, 0, 50, 0, 50]]).to_excel(writer, sheet_name='Sheet1', header=False, index=False)
                pd.DataFrame([['Total Ventas', 50, 0, 50, 0, 50]]).to_excel(writer, sheet_name='Sheet2', header=False, index=False)
            store = ArchiveStore(root=root / 'archive', sources_root=sources, wait_seconds=0)
            snapshot = store.snapshot(staged)
            entry = dict(schema=1, relative_path=original.name, **snapshot,
                         id=entry_id(original.name, snapshot['sha256']),
                         nas_key=f"point-archive/objects/{snapshot['sha256']}.xls",
                         verified_at=1700000000, consumers=['category_report', 'sales_reconciliation'])
            store.register(entry)
            with patch('pos_bridge.services.sales_category_report_service.open_source', store.open_source):
                with self.assertRaises(ArchivePending):
                    PointSalesCategoryReportService().parse_report(report_path=str(original))
                store.publish_restored(entry, staged)
                parsed = PointSalesCategoryReportService().parse_report(report_path=str(original))
            self.assertEqual(parsed.report_path, str(original))
            self.assertEqual(len(parsed.rows), 1)
            with patch('pos_bridge.services.sales_report_reconciliation_service.open_source', store.open_source):
                rows, summary = SalesReportReconciliationService()._parse_excel_report(str(original))
            self.assertEqual(len(rows), 1)

    def test_broker_signed_approval_retirement_recovery_and_real_parser_equivalence(self):
        from io import BytesIO
        import os
        from pos_bridge.management.commands.point_archive_inventory import semantic_digest
        from scripts.tests.test_point_archive import PointLifecycleFixture

        workbook = BytesIO()
        with pd.ExcelWriter(workbook, engine='openpyxl') as writer:
            pd.DataFrame([
                ['CATEGORÍA', 'CÓDIGO', 'PRODUCTO', 'CANTIDAD', 'BRUTO', 'DESCUENTOS', 'VENTA', 'IMPUESTOS', 'VENTA NETA'],
                ['Pasteles', '100', 'Pastel de prueba', 2, 100, 0, 100, 0, 100],
            ]).to_excel(writer, sheet_name='Sheet1', header=False, index=False)
            pd.DataFrame([['Total Ventas', 100, 0, 100, 0, 100]]).to_excel(writer, sheet_name='Sheet2', header=False, index=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = PointLifecycleFixture(root, workbook.getvalue(), parse=semantic_digest)
            with patch.dict(os.environ, {'POINT_BRIDGE_STORAGE_ROOT': str(root)}), \
                 patch('time.time', return_value=1794938400):
                before = PointSalesCategoryReportService().parse_report(report_path=str(fixture.source))
                self.assertEqual(fixture.prepare(), 'requested')
                self.assertTrue(fixture.source.exists())
                fixture.decide()
                self.assertEqual(fixture.execute(), 'completed')
                self.assertFalse(fixture.source.exists())
                with patch('pos_bridge.services.sales_category_report_service.open_source', fixture.store.open_source):
                    with self.assertRaises(ArchivePending):
                        PointSalesCategoryReportService().parse_report(report_path=str(fixture.source))
                self.assertEqual(fixture.broker.recover_requests()[0]['state'], 'recovered')
                after = PointSalesCategoryReportService().parse_report(report_path=str(fixture.source))
                self.assertEqual(after, before)
                self.assertEqual(len(after.rows), 1)
                self.assertEqual(after.report_path, str(fixture.source))
                self.assertEqual(semantic_digest(fixture.source), {key: fixture.candidate[key] for key in ('semantic_sha256', 'rows')})
