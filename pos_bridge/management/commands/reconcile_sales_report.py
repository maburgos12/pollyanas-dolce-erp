from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand

from pos_bridge.services.sales_report_reconciliation_service import SalesReportReconciliationService


class Command(BaseCommand):
    help = "Concilia un reporte XLS de ventas Point contra VentaHistorica y PointDailySale."

    def add_arguments(self, parser):
        parser.add_argument("report_path", type=str, help="Ruta al XLS exportado desde Point.")
        parser.add_argument("--start-date", required=True, type=date.fromisoformat)
        parser.add_argument("--end-date", required=True, type=date.fromisoformat)

    def handle(self, *args, **options):
        service = SalesReportReconciliationService()
        result = service.reconcile(
            report_path=options["report_path"],
            start_date=options["start_date"],
            end_date=options["end_date"],
        )

        summary = result.summary
        self.stdout.write(self.style.WARNING("Conciliación de ventas Point"))
        self.stdout.write(f"Excel venta declarada: {summary['excel_declared_total_venta']}")
        self.stdout.write(f"Excel venta neta declarada: {summary['excel_declared_total_neta']}")
        self.stdout.write(f"Excel cantidad declarada: {summary['excel_declared_total_qty']}")
        self.stdout.write(f"Canónico ERP venta: {summary['canonical_total_venta']}")
        self.stdout.write(f"Canónico ERP cantidad: {summary['canonical_total_qty']}")
        self.stdout.write(f"Diferencia canónica vs Excel venta: {summary['canonical_vs_excel_venta_diff']}")
        self.stdout.write(f"Diferencia canónica vs Excel cantidad: {summary['canonical_vs_excel_qty_diff']}")
        self.stdout.write(f"PointDailySale venta: {summary['point_total_venta']}")
        self.stdout.write(f"PointDailySale cantidad: {summary['point_total_qty']}")
        self.stdout.write(f"CSV detallado: {result.report_path}")
