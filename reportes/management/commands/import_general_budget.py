from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_import import GeneralBudgetImportService


class Command(BaseCommand):
    help = "Importa una hoja GENERAL de presupuesto mensual a staging consolidado."

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Ruta del archivo XLSX.")

    def handle(self, *args, **options):
        file_path = str(options["file"]).strip()
        try:
            summary = GeneralBudgetImportService().import_workbook(file_path)
        except FileNotFoundError as exc:
            raise CommandError(f"Archivo no encontrado: {file_path}") from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

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
