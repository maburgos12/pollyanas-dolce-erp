from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_detail_import import BudgetAuditMaterializationService


class Command(BaseCommand):
    help = "Materializa el dictamen de auditoría de presupuesto en la base para que BI consuma solo líneas OK."

    def add_arguments(self, parser):
        parser.add_argument("--dir", required=True, help="Ruta de la carpeta con archivos XLSX.")

    def handle(self, *args, **options):
        folder_path = str(options["dir"]).strip()
        try:
            summary = BudgetAuditMaterializationService().materialize(folder_path)
        except FileNotFoundError as exc:
            raise CommandError(f"Carpeta o archivo no encontrado: {folder_path}") from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            self.style.SUCCESS(
                "Audit materialized "
                f"total={summary.total_lines} "
                f"ok={summary.ok_lines} "
                f"desviacion={summary.deviation_lines} "
                f"mala_formula={summary.bad_formula_lines} "
                f"sin_soporte={summary.missing_detail_lines} "
                f"excl_total={summary.excluded_total_lines} "
                f"excl_extra={summary.excluded_extra_lines} "
                f"excl_dup={summary.excluded_duplicate_lines}"
            )
        )
