from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_detail_import import TrustedBudgetDetailImportService


class Command(BaseCommand):
    help = "Importa presupuesto detallado confiable desde hojas operativas por área y sucursal."

    def add_arguments(self, parser):
        parser.add_argument("--dir", required=True, help="Ruta de la carpeta con archivos XLSX.")

    def handle(self, *args, **options):
        folder_path = str(options["dir"]).strip()
        try:
            summary = TrustedBudgetDetailImportService().import_folder(folder_path)
        except FileNotFoundError as exc:
            raise CommandError(f"Carpeta no encontrada: {folder_path}") from exc
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
                    "sheets_imported": summary.sheets_imported,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
