from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from reportes.services_operating_finance import OperatingFinanceBootstrapService
from reportes.services_production_expense_import import ProductionExpenseImportService


class Command(BaseCommand):
    help = "Importa gasto operativo real de producción desde los libros de presupuesto/nómina."

    def add_arguments(self, parser):
        parser.add_argument("--dir", help="Carpeta con los archivos de presupuesto 2026.")
        parser.add_argument(
            "--file",
            help="Archivo PRESUPUESTO PRODUCCIÓN para importar mano de obra e indirectos desde una sola hoja.",
        )
        parser.add_argument(
            "--through-month",
            type=int,
            default=None,
            help="Último mes a importar desde --file (1-12). Útil para meses cerrados.",
        )

    def handle(self, *args, **options):
        folder_option = options.get("dir")
        file_option = options.get("file")
        through_month = options.get("through_month")
        if bool(folder_option) == bool(file_option):
            raise CommandError("Usa exactamente una opción: --dir o --file.")
        if through_month is not None and not (1 <= int(through_month) <= 12):
            raise CommandError("--through-month debe estar entre 1 y 12.")

        OperatingFinanceBootstrapService().bootstrap()
        service = ProductionExpenseImportService()
        if file_option:
            file_path = Path(file_option).expanduser().resolve()
            if not file_path.exists():
                raise CommandError(f"No existe el archivo: {file_path}")
            summary = service.import_production_workbook(file_path, through_month=through_month)
        else:
            folder = Path(folder_option).expanduser().resolve()
            if not folder.exists():
                raise CommandError(f"No existe la carpeta: {folder}")
            summary = service.import_folder(folder)
        skipped = {key: sorted(value) for key, value in summary.skipped_concepts.items() if value}
        self.stdout.write(
            self.style.SUCCESS(
                "Importación producción lista "
                f"· created={summary.created} updated={summary.updated} deleted={summary.deleted} periods={','.join(sorted(summary.periods))}"
            )
        )
        if skipped:
            self.stdout.write(f"Conceptos omitidos: {skipped}")
        if summary.flagged_outliers:
            self.stdout.write(f"Outliers detectados: {summary.flagged_outliers}")
