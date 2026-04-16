from __future__ import annotations

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from reportes.services_historical_branch_expense_import import HistoricalBranchExpenseImportService
from reportes.services_operating_finance import OperatingFinanceBootstrapService


class Command(BaseCommand):
    help = "Importa histórico de gastos por sucursal desde un workbook de ventas con columnas PRESUPUESTADO/REAL."

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Archivo XLSX histórico por sucursal.")
        parser.add_argument("--year", type=int, default=2025, help="Año histórico a materializar. Default: 2025.")

    def handle(self, *args, **options):
        workbook = Path(options["file"]).expanduser().resolve()
        if not workbook.exists():
            raise CommandError(f"No existe el archivo: {workbook}")

        OperatingFinanceBootstrapService().bootstrap()
        result = HistoricalBranchExpenseImportService().import_sales_history_workbook(
            workbook,
            target_year=int(options["year"]),
        )
        upload = result.upload
        payload = {
            "upload_id": upload.pk,
            "status": upload.status,
            "classification": result.classification,
            "processed_rows": upload.processed_rows,
            "loaded_rows": upload.loaded_rows,
            "created_rows": upload.created_rows,
            "updated_rows": upload.updated_rows,
            "affected_branches": upload.affected_branches,
            "covered_periods": upload.covered_periods,
            "error_log": upload.error_log,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
