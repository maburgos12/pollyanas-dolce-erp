from __future__ import annotations

from django.core.management.base import BaseCommand

from reportes.analytics_service import refresh_dashboard_daily_ops_materialized_view


class Command(BaseCommand):
    help = "Refresca la materialized view del dataset daily_ops para dashboard."

    def add_arguments(self, parser):
        parser.add_argument(
            "--non-concurrent",
            action="store_true",
            help="Usa REFRESH MATERIALIZED VIEW sin CONCURRENTLY.",
        )

    def handle(self, *args, **options):
        refresh_dashboard_daily_ops_materialized_view(concurrently=not options["non_concurrent"])
        self.stdout.write(self.style.SUCCESS("mv_dashboard_daily_ops refreshed"))
