from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_monitoring import BudgetMonitoringSnapshotService


class Command(BaseCommand):
    help = "Genera el resumen mensual de presupuesto consolidado a partir de líneas importadas."

    def add_arguments(self, parser):
        parser.add_argument(
            "--period",
            help="Periodo en formato YYYY-MM o YYYY-MM-DD. Si se omite, procesa todos los periodos importados.",
        )

    def handle(self, *args, **options):
        period_option = options.get("period")
        period_start = None
        if period_option:
            try:
                if len(period_option) == 7:
                    year, month = period_option.split("-")
                    period_start = date(int(year), int(month), 1)
                else:
                    parsed = date.fromisoformat(period_option)
                    period_start = date(parsed.year, parsed.month, 1)
            except Exception as exc:
                raise CommandError(f"Periodo inválido: {period_option}") from exc

        summary = BudgetMonitoringSnapshotService().build_snapshot(period_start=period_start)
        self.stdout.write(
            self.style.SUCCESS(
                f"Resumen presupuesto listo · created={summary.rows_created} updated={summary.rows_updated} periods={','.join(summary.periods)}"
            )
        )
