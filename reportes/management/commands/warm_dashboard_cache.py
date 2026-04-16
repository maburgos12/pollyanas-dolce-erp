from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.utils import timezone

from reportes.executive_panels import (
    build_central_flow_panel,
    build_executive_bi_panels,
    build_monthly_inventory_ledger_panel,
)


class Command(BaseCommand):
    help = "Precarga el cache versionado de dashboard/BI para el mes indicado."

    def add_arguments(self, parser):
        parser.add_argument("--year", type=int, help="Anio a precargar")
        parser.add_argument("--month", type=int, help="Mes a precargar")
        parser.add_argument(
            "--months",
            type=int,
            default=6,
            help="Ventana de meses para central flow e inventory ledger",
        )

    def handle(self, *args, **options):
        today = timezone.localdate()
        year = options.get("year") or today.year
        month = options.get("month") or today.month
        months_window = max(1, int(options.get("months") or 6))
        anchor = date(year, month, 1)

        self.stdout.write(
            self.style.NOTICE(
                f"Warming dashboard cache for {anchor.isoformat()} with window={months_window}"
            )
        )
        build_central_flow_panel(latest_date=anchor, months=months_window)
        build_monthly_inventory_ledger_panel(latest_date=anchor, months=months_window)
        build_executive_bi_panels(latest_date=anchor, months=months_window)
        self.stdout.write(self.style.SUCCESS("Dashboard/BI cache warmed successfully"))
