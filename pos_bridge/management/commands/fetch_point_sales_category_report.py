from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService


class Command(BaseCommand):
    help = "Descarga el XLS oficial de Point para Ventas por Categoría usando el mismo contrato de PrintVC."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=True, help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", required=True, help="Fecha final YYYY-MM-DD.")
        parser.add_argument("--branch", default="", help="ID externo de sucursal Point. Vacío = todas.")
        parser.add_argument("--branch-name", default="", help="Nombre visible de sucursal Point para nomSucursal.")
        parser.add_argument(
            "--credito",
            default="null",
            help="Filtro crédito: null=todos, false=contado, true=crédito.",
        )

    def handle(self, *args, **options):
        try:
            start_date = date.fromisoformat(options["start_date"])
            end_date = date.fromisoformat(options["end_date"])
        except ValueError as exc:
            raise CommandError("Fechas inválidas. Usa YYYY-MM-DD.") from exc

        if end_date < start_date:
            raise CommandError("end-date no puede ser menor que start-date.")

        branch = options["branch"].strip() or None
        branch_name = options["branch_name"].strip() or None
        credito = options["credito"].strip()
        if credito not in {"null", "false", "true"}:
            raise CommandError("--credito debe ser null, false o true.")

        result = PointSalesCategoryReportService().fetch_report(
            start_date=start_date,
            end_date=end_date,
            branch_external_id=branch,
            branch_display_name=branch_name,
            credito=None if credito == "null" else credito,
        )

        self.stdout.write("Reporte oficial Point descargado")
        self.stdout.write(f"Rango: {result.start_date.isoformat()} -> {result.end_date.isoformat()}")
        self.stdout.write(f"Sucursal: {result.branch_external_id or 'TODAS'}")
        self.stdout.write(f"Credito: {result.credito or 'null'}")
        self.stdout.write(f"URL: {result.request_url}")
        self.stdout.write(f"Archivo: {result.report_path}")
