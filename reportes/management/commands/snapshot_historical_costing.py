from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from reportes.services_historical_costing import MonthlyHistoricalCostingService


class Command(BaseCommand):
    help = "Reconstruye costo histórico mensual por insumo y receta usando costo promedio mensual de insumos."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Periodo YYYY-MM")

    def handle(self, *args, **options):
        try:
            period = date.fromisoformat(f"{options['period']}-01")
        except Exception as exc:
            raise CommandError("El periodo debe venir en formato YYYY-MM.") from exc

        summary = MonthlyHistoricalCostingService().build_period(period_start=period)
        self.stdout.write(
            self.style.SUCCESS(
                f"snapshot_historical_costing period={summary.period_start:%Y-%m} "
                f"insumo_rows={summary.insumo_rows} receta_rows={summary.receta_rows} "
                f"missing_recipe_rows={summary.missing_recipe_rows} "
                f"producto_reventa_rows={summary.producto_reventa_rows}"
            )
        )
