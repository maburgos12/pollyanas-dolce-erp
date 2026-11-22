import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase, TestCase

from pos_bridge.services.archive_access import ArchiveStore, ArchivePending, entry_id
from pos_bridge.services.product_history_import_service import PointProductHistoryImportService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_report_reconciliation_service import SalesReportReconciliationService
from pos_bridge.tests.test_product_history_import_service import _build_history_workbook


class AuthoritativeArchiveReaderTests(TestCase):
    def test_restored_category_preserves_authoritative_rows_and_logical_source(self):
        from core.models import Sucursal
        from ventas.models import VentaAutoritativaPoint
        from ventas.services.sales_truth import import_authoritative_point_category_report
        Sucursal.objects.create(codigo='ARCHIVE', nombre='ARCHIVE')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            sources = root / 'raw_exports'; sources.mkdir()
            original = sources / 'category.xls'; staged = root / 'restored.xlsx'
            with pd.ExcelWriter(staged) as writer:
                pd.DataFrame([['VENTA POR CATEGORÍA\nARCHIVE\n01/Jan/2026']]).to_excel(writer, header=False, index=False)
                pd.DataFrame([['Pasteles', '100', 'Pastel', 2, 100, 0, 100, 0, 100]],
                    columns=['CATEGORÍA','CÓDIGO','PRODUCTO','CANTIDAD','BRUTO','DESCUENTOS','VENTA','IMPUESTOS','VENTA NETA']).to_excel(writer, startrow=5, index=False)
            original.write_bytes(staged.read_bytes())
            store = ArchiveStore(root=root / 'archive', sources_root=sources, wait_seconds=0)
            snap = store.snapshot(original)
            entry = dict(schema=1, relative_path=original.name, **snap,
                id=entry_id(original.name, snap['sha256']), nas_key=f"point-archive/objects/{snap['sha256']}.xls",
                verified_at=1700000000, consumers=['category_report'])
            store.register(entry)
            with patch('ventas.services.sales_truth.open_source', store.open_source, create=True):
                first = import_authoritative_point_category_report(str(original))
                fields = ['branch_id','product_id','product_code','point_name','category','quantity','gross_amount',
                    'discount_amount','total_amount','tax_amount','net_amount','source_file','source_sheet','raw_payload']
                before = list(VentaAutoritativaPoint.objects.values(*fields))
                self.assertEqual(first['created'], 1)
                original.unlink()
                with self.assertRaises(ArchivePending):
                    import_authoritative_point_category_report(str(original))
                self.assertEqual(list(VentaAutoritativaPoint.objects.values(*fields)), before)
                store.publish_restored(entry, staged)
                restored = import_authoritative_point_category_report(str(original))
                self.assertEqual(restored['updated'], 1)
                self.assertEqual(list(VentaAutoritativaPoint.objects.values(*fields)), before)
                self.assertEqual(before[0]['source_file'], str(original))


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
