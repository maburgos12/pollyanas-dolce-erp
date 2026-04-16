from __future__ import annotations

from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from reportes.analytics_service import audit_sales_fact_consistency


class Command(BaseCommand):
    help = "Audita consistencia entre facts analíticas y fuentes operativas."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", type=str, help="Inicio YYYY-MM-DD")
        parser.add_argument("--end-date", type=str, help="Fin YYYY-MM-DD")
        parser.add_argument("--days", type=int, default=7, help="Ventana si no se manda rango explícito")

    def handle(self, *args, **options):
        end_date = date.fromisoformat(options["end_date"]) if options.get("end_date") else timezone.localdate()
        start_date = (
            date.fromisoformat(options["start_date"])
            if options.get("start_date")
            else end_date - timedelta(days=max(1, int(options["days"] or 7)) - 1)
        )
        audit = audit_sales_fact_consistency(start_date=start_date, end_date=end_date)
        style = self.style.SUCCESS if audit.discrepancy_count == 0 else self.style.WARNING
        self.stdout.write(
            style(
                f"Analytics audit {audit.audit_type} status={audit.status} "
                f"discrepancies={audit.discrepancy_count} range={start_date}..{end_date}"
            )
        )
