from __future__ import annotations

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from reportes.services_operating_expense_automation import OperatingExpenseImportAutomationService


class Command(BaseCommand):
    help = "Procesa automáticamente una carpeta inbox de archivos XLSX de gasto operativo mensual."

    def add_arguments(self, parser):
        parser.add_argument("--dir", required=True, help="Carpeta a monitorear/procesar.")
        parser.add_argument("--year", type=int, default=2026, help="Año operativo permitido. Default: 2026.")
        parser.add_argument(
            "--no-refresh-projects",
            action="store_true",
            help="No refrescar snapshots de proyectos después de cada archivo.",
        )

    def handle(self, *args, **options):
        directory = Path(str(options["dir"]).strip()).expanduser().resolve()
        if not directory.exists():
            raise CommandError(f"No existe la carpeta: {directory}")

        try:
            summary = OperatingExpenseImportAutomationService().process_directory(
                directory,
                target_year=int(options["year"]),
                refresh_projects=not bool(options["no_refresh_projects"]),
            )
        except FileNotFoundError as exc:
            raise CommandError(f"No existe la carpeta: {directory}") from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            json.dumps(
                {
                    "processed_files": summary.processed_files,
                    "success_files": summary.success_files,
                    "error_files": summary.error_files,
                    "duplicate_files": summary.duplicate_files,
                    "run_ids": summary.run_ids or [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
