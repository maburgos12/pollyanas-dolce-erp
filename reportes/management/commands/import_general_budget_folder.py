from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_import import GeneralBudgetImportService


class Command(BaseCommand):
    help = "Importa en lote las hojas GENERAL de una carpeta de presupuestos."

    def add_arguments(self, parser):
        parser.add_argument("--dir", required=True, help="Ruta de la carpeta con archivos XLSX.")

    def handle(self, *args, **options):
        folder_path = str(options["dir"]).strip()
        try:
            summary = GeneralBudgetImportService().import_folder(folder_path)
        except FileNotFoundError as exc:
            raise CommandError(f"Carpeta no encontrada: {folder_path}") from exc

        self.stdout.write(
            json.dumps(
                {
                    "imports_created": summary.imports_created,
                    "imports_updated": summary.imports_updated,
                    "lines_created": summary.lines_created,
                    "lines_updated": summary.lines_updated,
                    "periods": summary.periods,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
