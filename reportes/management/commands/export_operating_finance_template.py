from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand

from reportes.services_operating_finance_io import OperatingFinanceTemplateService


class Command(BaseCommand):
    help = "Exporta una plantilla XLSX para cargar gastos operativos mensuales."

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            default="output/spreadsheet/operating_finance_expenses_template.xlsx",
            help="Ruta de salida del archivo XLSX.",
        )

    def handle(self, *args, **options):
        output = Path(options["output"]).expanduser()
        path = OperatingFinanceTemplateService().export_template(output)
        self.stdout.write(str(path))
