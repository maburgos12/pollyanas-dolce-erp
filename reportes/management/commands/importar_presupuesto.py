from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from reportes.services_presupuesto_maestro import PresupuestoMaestroImportService


class Command(BaseCommand):
    help = "Importa presupuesto maestro por área desde CSV/XLSX con columnas concepto, tipo, sucursal y enero-diciembre."

    def add_arguments(self, parser):
        parser.add_argument("--archivo", required=True, help="Ruta del archivo CSV o XLSX.")
        parser.add_argument("--area", required=True, help="Código de área: ventas, produccion, gastos-venta, administracion, nomina, logistica, compras, capex.")
        parser.add_argument(
            "--version-presupuesto",
            "--budget-version",
            dest="budget_version",
            default="ORIGINAL",
            choices=["ORIGINAL", "REVISADO"],
            help="Versión presupuestal.",
        )
        parser.add_argument("--año", "--year", dest="year", type=int, default=2026, help="Año objetivo del presupuesto.")
        parser.add_argument("--fuente", default="", help="Etiqueta de fuente para metadata.")

    def handle(self, *args, **options):
        try:
            summary = PresupuestoMaestroImportService().import_file(
                archivo=options["archivo"],
                area_code=options["area"],
                version=options["budget_version"],
                year=options["year"],
                source_name=options["fuente"],
            )
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Presupuesto importado"))
        self.stdout.write(f"Área: {summary.area}")
        self.stdout.write(f"Versión: {summary.version}")
        self.stdout.write(f"Año: {summary.year}")
        self.stdout.write(f"Rubros creados: {summary.rubros_created}")
        self.stdout.write(f"Rubros actualizados: {summary.rubros_updated}")
        self.stdout.write(f"Líneas creadas: {summary.lines_created}")
        self.stdout.write(f"Líneas actualizadas: {summary.lines_updated}")
        self.stdout.write(f"Filas omitidas: {summary.skipped_rows}")
