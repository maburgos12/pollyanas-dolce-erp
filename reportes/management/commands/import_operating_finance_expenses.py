from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from reportes.services_operating_finance_io import OperatingFinanceExpenseImportService


class Command(BaseCommand):
    help = "Importa gastos operativos mensuales desde una plantilla XLSX."

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Ruta del archivo XLSX.")

    def handle(self, *args, **options):
        file_path = str(options["file"]).strip()
        try:
            summary = OperatingFinanceExpenseImportService().import_workbook(file_path)
        except FileNotFoundError as exc:
            raise CommandError(f"Archivo no encontrado: {file_path}") from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            json.dumps(
                {
                    "created": summary.created,
                    "updated": summary.updated,
                    "periods": summary.periods,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
