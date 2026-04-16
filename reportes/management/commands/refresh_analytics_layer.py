from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.utils import timezone

from reportes.analytics_service import full_rebuild, refresh_incremental
from reportes.executive_panels import build_central_flow_panel, build_monthly_inventory_ledger_panel


class Command(BaseCommand):
    help = "Refresca la capa analítica incremental o full rebuild y materializa snapshots ejecutivos."

    def add_arguments(self, parser):
        parser.add_argument("--full-rebuild", action="store_true", help="Reconstruye toda la capa analítica")
        parser.add_argument("--date", type=str, help="Fecha de referencia YYYY-MM-DD")
        parser.add_argument("--lookback-days", type=int, default=3, help="Ventana incremental hacia atrás")
        parser.add_argument("--start-date", type=str, help="Inicio YYYY-MM-DD para full rebuild")
        parser.add_argument("--end-date", type=str, help="Fin YYYY-MM-DD para full rebuild")
        parser.add_argument("--months", type=int, default=6, help="Ventana de snapshots ejecutivos")
        parser.add_argument("--skip-snapshots", action="store_true", help="No recalcular snapshots dashboard/BI")

    def handle(self, *args, **options):
        reference_date = date.fromisoformat(options["date"]) if options.get("date") else timezone.localdate()
        if options["full_rebuild"]:
            start_date = date.fromisoformat(options["start_date"]) if options.get("start_date") else None
            end_date = date.fromisoformat(options["end_date"]) if options.get("end_date") else reference_date
            summary = full_rebuild(start_date=start_date, end_date=end_date)
            self.stdout.write(
                self.style.NOTICE(
                    "Full rebuild analytics "
                    f"sales={summary.sales_rows} inventory={summary.inventory_rows} "
                    f"production={summary.production_rows} forecast={summary.forecast_rows}"
                )
            )
        else:
            summary = refresh_incremental(reference_date=reference_date, lookback_days=int(options["lookback_days"] or 3))
            self.stdout.write(
                self.style.NOTICE(
                    "Incremental analytics "
                    f"sales={summary.sales_rows} inventory={summary.inventory_rows} "
                    f"production={summary.production_rows} forecast={summary.forecast_rows}"
                )
            )

        if not options["skip_snapshots"]:
            months = max(1, int(options.get("months") or 6))
            build_monthly_inventory_ledger_panel(latest_date=reference_date, months=months)
            build_central_flow_panel(latest_date=reference_date, months=months)
            self.stdout.write(self.style.SUCCESS("Analytics layer refreshed with executive snapshots"))
        else:
            self.stdout.write(self.style.SUCCESS("Analytics layer refreshed"))
