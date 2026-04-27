from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_vs_actual import MONTH_COLUMNS
from reportes.services_presupuesto_maestro import PresupuestoMaestroImportService


class Command(BaseCommand):
    help = "Reimporta la proyección de ventas 2026 desde la hoja GENERAL y corrige columnas de venta en pesos."

    def add_arguments(self, parser):
        parser.add_argument("--archivo", required=True, help="Ruta del Excel de proyección de ventas.")
        parser.add_argument("--año", "--year", dest="year", type=int, default=2026, help="Año objetivo.")
        parser.add_argument(
            "--version-presupuesto",
            "--budget-version",
            dest="budget_version",
            default="ORIGINAL",
            choices=["ORIGINAL", "REVISADO"],
            help="Versión presupuestal.",
        )
        parser.add_argument("--limpiar-primero", action="store_true", help="Borra rubros de ventas antes de reimportar.")
        parser.add_argument("--dry-run", action="store_true", help="Simula la reimportación sin persistir cambios.")
        parser.add_argument("--fuente", default="", help="Etiqueta de fuente para metadata.")

    def handle(self, *args, **options):
        if options["limpiar_primero"] and not options["dry_run"]:
            self.stdout.write(self.style.WARNING("Se eliminarán rubros/líneas actuales del área ventas antes de importar."))
        try:
            summary = PresupuestoMaestroImportService().reimport_sales_projection(
                archivo=options["archivo"],
                version=options["budget_version"],
                year=options["year"],
                source_name=options["fuente"],
                clear_first=options["limpiar_primero"],
                dry_run=options["dry_run"],
            )
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        status = "DRY-RUN" if summary.dry_run else "PERSISTIDO"
        self.stdout.write(self.style.SUCCESS(f"Reimportación ventas presupuesto · {summary.year} · {status}"))
        self.stdout.write(f"Rubros a eliminar/eliminados: {summary.deleted_rubros}")
        self.stdout.write(f"Líneas a eliminar/eliminadas: {summary.deleted_lines}")
        self.stdout.write(f"Rubros creados: {summary.rubros_created}")
        self.stdout.write(f"Rubros actualizados: {summary.rubros_updated}")
        self.stdout.write(f"Líneas creadas: {summary.lines_created}")
        self.stdout.write(f"Líneas actualizadas: {summary.lines_updated}")
        self.stdout.write(f"Filas omitidas: {summary.skipped_rows}")
        self.stdout.write("")
        self.stdout.write("Totales por mes:")
        for month_name, _month_number in MONTH_COLUMNS:
            amount = summary.monthly_totals.get(month_name) or 0
            self.stdout.write(f"  {month_name.title()}: ${amount:,.2f}")
