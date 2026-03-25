from __future__ import annotations

from datetime import date
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.models import PointMonthlySalesOfficial
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.utils.helpers import decimal_from_value


class Command(BaseCommand):
    help = "Descarga y guarda un rango oficial parcial dentro del cache mensual oficial Point."
    FILE_CACHE_PATH = Path("storage/pos_bridge/reports/official_partial_sales_cache.json")

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=True, help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", required=True, help="Fecha final YYYY-MM-DD.")

    def handle(self, *args, **options):
        try:
            start_date = date.fromisoformat(options["start_date"])
            end_date = date.fromisoformat(options["end_date"])
        except ValueError as exc:
            raise CommandError("Fechas inválidas. Usa YYYY-MM-DD.") from exc

        if end_date < start_date:
            raise CommandError("end-date no puede ser menor que start-date.")

        service = PointSalesCategoryReportService()
        result = service.fetch_report(
            start_date=start_date,
            end_date=end_date,
            branch_external_id=None,
            branch_display_name=None,
            credito=None,
        )
        parsed = service.parse_report(report_path=result.report_path)
        total_quantity = sum((decimal_from_value(row.get("Cantidad")) for row in parsed.rows), decimal_from_value(0))
        month_start = start_date.replace(day=1)
        monthly_row = PointMonthlySalesOfficial.objects.filter(month_start=month_start).first()
        raw_payload = dict((monthly_row.raw_payload or {}) if monthly_row else {})
        partial_ranges = dict(raw_payload.get("partial_ranges") or {})
        partial_key = f"{start_date.isoformat()}_{end_date.isoformat()}"
        partial_ranges[partial_key] = {
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            "total_quantity": str(total_quantity),
            "gross_amount": str(parsed.summary["bruto"]),
            "discount_amount": str(parsed.summary["descuentos"]),
            "total_amount": str(parsed.summary["venta"]),
            "tax_amount": str(parsed.summary["impuestos"]),
            "net_amount": str(parsed.summary["venta_neta"]),
            "report_path": result.report_path,
            "request_url": result.request_url,
            "rows_count": len(parsed.rows),
        }
        raw_payload["partial_ranges"] = partial_ranges
        if monthly_row is not None:
            monthly_row.raw_payload = raw_payload
        file_cache = {}
        if self.FILE_CACHE_PATH.exists():
            file_cache = json.loads(self.FILE_CACHE_PATH.read_text(encoding="utf-8"))
        file_cache[partial_key] = partial_ranges[partial_key]
        self.FILE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.FILE_CACHE_PATH.write_text(json.dumps(file_cache, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            if monthly_row is not None:
                monthly_row.save(update_fields=["raw_payload", "updated_at"])
                self.stdout.write(self.style.SUCCESS("Rango oficial Point guardado dentro del cache mensual y archivo"))
            else:
                self.stdout.write(self.style.SUCCESS("Rango oficial Point guardado en archivo"))
        except Exception as exc:
            self.stdout.write(self.style.WARNING(f"SQLite no permitió guardar el cache mensual; quedó guardado en archivo. Detalle: {exc}"))
        self.stdout.write(f"Rango: {start_date.isoformat()} -> {end_date.isoformat()}")
        self.stdout.write(f"Venta: {parsed.summary['venta']}")
        self.stdout.write(f"Cantidad: {total_quantity}")
        self.stdout.write(f"Archivo: {result.report_path}")
