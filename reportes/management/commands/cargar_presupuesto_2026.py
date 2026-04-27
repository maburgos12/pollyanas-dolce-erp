from __future__ import annotations

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_vs_actual import BudgetCsvImportService, write_example_budget_csv


class Command(BaseCommand):
    help = "Carga presupuesto anual 2026 desde CSV mensual simple e idempotente."

    def add_arguments(self, parser):
        parser.add_argument("--file", help="Ruta del CSV con columnas concepto, enero...diciembre.")
        parser.add_argument(
            "--write-example",
            help="Escribe un CSV ejemplo en la ruta indicada y termina sin importar datos.",
        )
        parser.add_argument("--year", type=int, default=2026, help="Año presupuestal. Default: 2026.")

    def handle(self, *args, **options):
        example_path = (options.get("write_example") or "").strip()
        if example_path:
            output_path = write_example_budget_csv(Path(example_path))
            self.stdout.write(str(output_path))
            return

        file_path = (options.get("file") or "").strip()
        if not file_path:
            raise CommandError("Debes enviar --file o --write-example.")

        try:
            summary = BudgetCsvImportService().import_csv(file_path, year=int(options["year"]))
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
                    "missing_required_concepts": summary.missing_required_concepts,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
