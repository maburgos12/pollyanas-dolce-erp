from __future__ import annotations

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from reportes.services_branch_real_operating_expense_import import (
    BranchRealOperatingExpenseImportService,
    BranchRealOperatingExpenseImportValidationError,
    NON_REAL_POLICY_IGNORE,
    NON_REAL_POLICY_REJECT,
)


class Command(BaseCommand):
    help = "Importa gasto operativo mensual real 2026 por sucursal desde un archivo estructurado."

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Ruta del archivo XLSX.")
        parser.add_argument("--sheet", help="Hoja a procesar. Default: GastosSucursal o la primera hoja.")
        parser.add_argument("--year", type=int, default=2026, help="Año permitido para la carga. Default: 2026.")
        parser.add_argument(
            "--non-real-policy",
            choices=[NON_REAL_POLICY_IGNORE, NON_REAL_POLICY_REJECT],
            default=NON_REAL_POLICY_IGNORE,
            help="Qué hacer con filas PRESUPUESTO. Default: ignore.",
        )
        parser.add_argument(
            "--no-refresh-projects",
            action="store_true",
            help="No refrescar snapshots de proyectos tras la carga.",
        )

    def handle(self, *args, **options):
        file_path = Path(str(options["file"]).strip()).expanduser().resolve()
        if not file_path.exists():
            raise CommandError(f"Archivo no encontrado: {file_path}")

        try:
            summary = BranchRealOperatingExpenseImportService().import_workbook(
                file_path,
                target_year=int(options["year"]),
                non_real_policy=options["non_real_policy"],
                refresh_projects=not bool(options["no_refresh_projects"]),
                sheet_name=options.get("sheet") or None,
            )
        except FileNotFoundError as exc:
            raise CommandError(f"Archivo no encontrado: {file_path}") from exc
        except BranchRealOperatingExpenseImportValidationError as exc:
            raise CommandError(
                json.dumps(
                    {
                        "processed_rows": exc.summary.processed_rows,
                        "errors": [
                            {
                                "row_number": item.row_number,
                                "field": item.field,
                                "message": item.message,
                            }
                            for item in exc.summary.errors
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            ) from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            json.dumps(
                {
                    "processed_rows": summary.processed_rows,
                    "loaded_rows": summary.loaded_rows,
                    "created": summary.created,
                    "updated": summary.updated,
                    "skipped_non_real": summary.skipped_non_real,
                    "affected_branches": summary.affected_branches,
                    "periods": summary.periods,
                    "project_refresh_count": summary.project_refresh_count,
                    "project_ids": summary.project_ids,
                    "errors": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
