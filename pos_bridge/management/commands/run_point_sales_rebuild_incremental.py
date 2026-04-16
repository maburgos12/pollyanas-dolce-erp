from __future__ import annotations

from datetime import date

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.utils.dates import resolve_incremental_window


class Command(BaseCommand):
    help = "Ejecuta la ventana incremental Point v2 usando el mismo pipeline auditable del backfill."

    def add_arguments(self, parser):
        parser.add_argument("--anchor-date")
        parser.add_argument("--lookback-days", type=int, default=3)
        parser.add_argument("--lag-days", type=int, default=1)
        parser.add_argument("--branch", default="")
        parser.add_argument("--batch-size", type=int, default=10)
        parser.add_argument("--max-tasks", type=int)
        parser.add_argument("--worker-name", default="")
        parser.add_argument("--build-report", action="store_true")

    def handle(self, *args, **options):
        anchor_date_raw = options.get("anchor_date")
        try:
            anchor_date = date.fromisoformat(anchor_date_raw) if anchor_date_raw else None
        except ValueError as exc:
            raise CommandError(f"anchor-date inválida: {exc}") from exc
        start_date, end_date = resolve_incremental_window(
            anchor_date=anchor_date,
            lookback_days=options["lookback_days"],
            lag_days=options["lag_days"],
        )
        call_command(
            "run_point_sales_rebuild_backfill",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            branch=options["branch"],
            batch_size=options["batch_size"],
            max_tasks=options.get("max_tasks"),
            worker_name=options["worker_name"],
            build_report=options["build_report"],
        )
