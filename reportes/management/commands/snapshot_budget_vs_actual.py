from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_vs_actual import BudgetVsActualSnapshotService, parse_period


class Command(BaseCommand):
    help = "Calcula Presupuesto vs Real mensual usando EmpresaResultadoMensual como real financiero."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Periodo en formato YYYY-MM o YYYY-MM-DD.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula e imprime sin guardar snapshot.")

    def handle(self, *args, **options):
        period_raw = options["period"]
        try:
            period_start = parse_period(period_raw)
        except Exception as exc:
            raise CommandError(f"Periodo inválido: {period_raw}") from exc

        summary = BudgetVsActualSnapshotService().build_snapshot(
            period_start=period_start,
            dry_run=bool(options.get("dry_run")),
        )
        self.stdout.write(f"Presupuesto vs Real · {summary.period:%Y-%m} · persisted={summary.persisted}")
        self.stdout.write("Concepto | Presupuesto | Real | Varianza | Varianza %")
        for row in summary.rows:
            self.stdout.write(
                "{label} | ${budget:,.2f} | ${actual:,.2f} | ${variance:,.2f} | {variance_pct:.2f}%".format(
                    label=row["label"],
                    budget=row["budget"],
                    actual=row["actual"],
                    variance=row["variance"],
                    variance_pct=row["variance_pct"],
                )
            )
        if not summary.has_budget:
            self.stdout.write(self.style.WARNING("Sin presupuesto cargado para el periodo."))
        if not summary.has_actual:
            self.stdout.write(self.style.WARNING("Sin EmpresaResultadoMensual para el periodo."))
