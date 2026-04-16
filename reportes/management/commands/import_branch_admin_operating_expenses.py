from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from reportes.services_branch_admin_expense_import import BranchAdminExpenseImportService
from reportes.services_operating_finance import OperatingFinanceBootstrapService


class Command(BaseCommand):
    help = "Importa gasto operativo real de sucursales y corporativo desde ventas y administración."

    def add_arguments(self, parser):
        parser.add_argument("--dir", required=True, help="Carpeta con los archivos de presupuesto 2026.")

    def handle(self, *args, **options):
        folder = Path(options["dir"]).expanduser().resolve()
        if not folder.exists():
            raise CommandError(f"No existe la carpeta: {folder}")

        OperatingFinanceBootstrapService().bootstrap()
        summary = BranchAdminExpenseImportService().import_folder(folder)
        self.stdout.write(
            self.style.SUCCESS(
                "Importación sucursales/corporativo lista "
                f"· created={summary.created} updated={summary.updated} deleted={summary.deleted} periods={','.join(sorted(summary.periods))}"
            )
        )
        skipped = {key: sorted(value) for key, value in summary.skipped_concepts.items() if value}
        if skipped:
            self.stdout.write(f"Conceptos omitidos: {skipped}")
        if summary.flagged_outliers:
            self.stdout.write(f"Outliers detectados: {summary.flagged_outliers}")
