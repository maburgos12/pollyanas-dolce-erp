from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.models import PointMonthlySalesOfficial
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService


class Command(BaseCommand):
    help = "Carga ventas oficiales mensuales Point (todas las sucursales) para análisis YoY y auditoría."

    def add_arguments(self, parser):
        parser.add_argument("--start-month", required=True, help="Mes inicial YYYY-MM.")
        parser.add_argument("--end-month", required=True, help="Mes final YYYY-MM.")

    def _parse_month(self, value: str) -> date:
        try:
            parsed = datetime.strptime(value.strip(), "%Y-%m").date()
        except ValueError as exc:
            raise CommandError("Usa formato YYYY-MM.") from exc
        return date(parsed.year, parsed.month, 1)

    def handle(self, *args, **options):
        start_month = self._parse_month(options["start_month"])
        end_month = self._parse_month(options["end_month"])
        if end_month < start_month:
            raise CommandError("end-month no puede ser menor que start-month.")

        service = PointSalesCategoryReportService()
        current = start_month
        processed = 0

        while current <= end_month:
            month_end = date(current.year, current.month, monthrange(current.year, current.month)[1])
            report = service.fetch_report(
                start_date=current,
                end_date=month_end,
                branch_external_id=None,
                branch_display_name=None,
                credito=None,
            )
            parsed = service.parse_report(report_path=report.report_path)
            total_quantity = sum((Decimal(str(row.get("Cantidad") or 0)) for row in parsed.rows), Decimal("0"))
            PointMonthlySalesOfficial.objects.update_or_create(
                month_start=current,
                defaults={
                    "month_end": month_end,
                    "total_quantity": total_quantity,
                    "gross_amount": parsed.summary["bruto"],
                    "discount_amount": parsed.summary["descuentos"],
                    "total_amount": parsed.summary["venta"],
                    "tax_amount": parsed.summary["impuestos"],
                    "net_amount": parsed.summary["venta_neta"],
                    "report_path": report.report_path,
                    "raw_payload": {
                        "row_count": len(parsed.rows),
                        "start_date": current.isoformat(),
                        "end_date": month_end.isoformat(),
                    },
                },
            )
            processed += 1
            self.stdout.write(f"{current:%Y-%m} -> ${parsed.summary['venta']}")
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        self.stdout.write(self.style.SUCCESS(f"Meses oficiales cargados: {processed}"))
