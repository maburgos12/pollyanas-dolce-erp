from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.models import PointMonthlySummary
from pos_bridge.services.sales_branch_indicator_service import PointSalesBranchIndicatorService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService


ZERO = Decimal("0")


class Command(BaseCommand):
    help = "Descarga totales mensuales oficiales de Point por sucursal y los persiste de forma idempotente."

    def add_arguments(self, parser):
        parser.add_argument("--start", default="2022-01", help="Mes inicial YYYY-MM.")
        parser.add_argument("--end", default="2024-12", help="Mes final YYYY-MM.")
        parser.add_argument("--branch", help="Sucursal Point por external_id o nombre.")

    def _parse_month(self, raw: str) -> date:
        try:
            parsed = datetime.strptime(str(raw).strip(), "%Y-%m").date()
        except ValueError as exc:
            raise CommandError("Usa formato YYYY-MM en --start/--end.") from exc
        return date(parsed.year, parsed.month, 1)

    def _iter_months(self, start_month: date, end_month: date):
        current = start_month
        while current <= end_month:
            yield current
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

    def _month_end(self, month_start: date) -> date:
        return date(month_start.year, month_start.month, monthrange(month_start.year, month_start.month)[1])

    def _download_month_total(
        self,
        *,
        branch,
        month_start: date,
        month_end: date,
        report_service: PointSalesCategoryReportService,
        indicator_service: PointSalesBranchIndicatorService,
    ) -> tuple[Decimal, int, str, dict]:
        try:
            report = report_service.fetch_report(
                start_date=month_start,
                end_date=month_end,
                branch_external_id=branch.external_id,
                branch_display_name=branch.name,
                credito=None,
            )
            parsed = report_service.parse_report(report_path=report.report_path)
            daily_payloads = indicator_service.fetch_range(
                start_date=month_start,
                end_date=month_end,
                branch_external_id=branch.external_id,
            )
            total_tickets = sum(int(item.total_tickets or 0) for item in daily_payloads)
            return (
                Decimal(str(parsed.summary.get("venta") or 0)),
                total_tickets,
                "POINT_MONTHLY_REPORT",
                {
                    "mode": "monthly_report",
                    "request_url": report.request_url,
                    "report_path": report.report_path,
                    "summary": {key: str(value) for key, value in parsed.summary.items()},
                    "rows_count": len(parsed.rows),
                    "indicator_days": len(daily_payloads),
                },
            )
        except Exception as monthly_exc:  # noqa: BLE001
            daily_payloads = indicator_service.fetch_range(
                start_date=month_start,
                end_date=month_end,
                branch_external_id=branch.external_id,
            )
            total_revenue = sum((Decimal(str(item.total_amount or 0)) for item in daily_payloads), ZERO)
            total_tickets = sum(int(item.total_tickets or 0) for item in daily_payloads)
            return (
                total_revenue,
                total_tickets,
                "POINT_DAILY_INDICATORS",
                {
                    "mode": "daily_indicator_fallback",
                    "warning": str(monthly_exc),
                    "days_count": len(daily_payloads),
                    "days": [
                        {
                            "date": item.indicator_date.isoformat(),
                            "amount": str(item.total_amount),
                            "tickets": int(item.total_tickets or 0),
                        }
                        for item in daily_payloads
                    ],
                },
            )

    def handle(self, *args, **options):
        start_month = self._parse_month(options["start"])
        end_month = self._parse_month(options["end"])
        if end_month < start_month:
            raise CommandError("--end no puede ser menor que --start.")

        branch_filter = (options.get("branch") or "").strip() or None
        branches = PointSalesBranchIndicatorService.canonical_branches(branch_filter=branch_filter)
        if not branches:
            raise CommandError("No se encontraron sucursales Point para procesar.")

        report_service = PointSalesCategoryReportService()
        indicator_service = PointSalesBranchIndicatorService()

        downloaded = 0
        failed = 0
        existing = 0

        for month_start in self._iter_months(start_month, end_month):
            month_end = self._month_end(month_start)
            for branch in branches:
                erp_branch = getattr(branch, "erp_branch", None)
                if erp_branch is not None and erp_branch.fecha_apertura and erp_branch.fecha_apertura > month_end:
                    self.stdout.write(
                        f"Descargando {month_start:%Y-%m} {branch.name}... SKIP (no operativa en el periodo)"
                    )
                    continue
                try:
                    total_revenue, total_transactions, source_report, raw_data = self._download_month_total(
                        branch=branch,
                        month_start=month_start,
                        month_end=month_end,
                        report_service=report_service,
                        indicator_service=indicator_service,
                    )
                    already_exists = PointMonthlySummary.objects.filter(
                        year=month_start.year,
                        month=month_start.month,
                        branch=branch.name,
                    ).exists()
                    PointMonthlySummary.objects.update_or_create(
                        year=month_start.year,
                        month=month_start.month,
                        branch=branch.name,
                        defaults={
                            "branch_code": str(branch.external_id or ""),
                            "total_revenue": total_revenue,
                            "total_transactions": total_transactions,
                            "source_report": source_report,
                            "raw_data": raw_data,
                        },
                    )
                    downloaded += 1
                    if already_exists:
                        existing += 1
                    self.stdout.write(
                        f"Descargando {month_start:%Y-%m} {branch.name}... OK (${total_revenue:,.2f})"
                    )
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    self.stderr.write(
                        f"Descargando {month_start:%Y-%m} {branch.name}... FAIL ({exc})"
                    )

        self.stdout.write("")
        self.stdout.write("RESUMEN DESCARGA POINT MONTHLY SUMMARY")
        self.stdout.write(f"Descargados: {downloaded}")
        self.stdout.write(f"Fallidos: {failed}")
        self.stdout.write(f"Ya existentes: {existing}")
