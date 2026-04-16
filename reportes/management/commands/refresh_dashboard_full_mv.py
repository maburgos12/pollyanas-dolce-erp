from __future__ import annotations

from django.core.management.base import BaseCommand

from reportes.analytics_service import refresh_dashboard_full_materialized_view
from reportes.dashboard_full_dataset import ALLOWED_MONTH_WINDOWS


class Command(BaseCommand):
    help = "Reconstruye el payload ejecutivo completo y refresca mv_dashboard_full."

    def add_arguments(self, parser):
        parser.add_argument(
            "--months",
            nargs="*",
            type=int,
            default=list(ALLOWED_MONTH_WINDOWS),
            help="Ventanas del dashboard ejecutivo (default: 6 9 12).",
        )

    def handle(self, *args, **options):
        months_windows = tuple(int(value) for value in (options.get("months") or ALLOWED_MONTH_WINDOWS))
        refreshed = refresh_dashboard_full_materialized_view(months_windows=months_windows)
        self.stdout.write(self.style.SUCCESS(f"Dashboard full materialized view refreshed rows={refreshed}"))
