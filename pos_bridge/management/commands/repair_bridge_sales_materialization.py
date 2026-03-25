from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.sales_materialization_repair_service import BridgeSalesMaterializationRepairService


class Command(BaseCommand):
    help = "Reevalúa PointDailySale con las reglas actuales de matching y reconstruye VentaHistorica POINT_BRIDGE_SALES."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=True, help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", required=True, help="Fecha final YYYY-MM-DD.")

    def handle(self, *args, **options):
        try:
            start_date = date.fromisoformat(options["start_date"])
            end_date = date.fromisoformat(options["end_date"])
        except ValueError as exc:
            raise CommandError("Fechas inválidas. Usa formato YYYY-MM-DD.") from exc

        if end_date < start_date:
            raise CommandError("end-date no puede ser menor que start-date.")

        service = BridgeSalesMaterializationRepairService()
        result = service.repair(start_date=start_date, end_date=end_date)

        self.stdout.write("Reparación de materialización Point bridge")
        self.stdout.write(f"Rango: {start_date.isoformat()} -> {end_date.isoformat()}")
        self.stdout.write(f"PointDailySale revisadas: {result.scanned_rows}")
        self.stdout.write(f"Recetas reasignadas: {result.recipe_rows_updated}")
        self.stdout.write(f"Recetas limpiadas: {result.recipe_rows_cleared}")
        self.stdout.write(f"Filas no-receta: {result.non_recipe_rows}")
        self.stdout.write(f"Filas sin match actual: {result.unresolved_rows}")
        self.stdout.write(f"Filas sin sucursal ERP: {result.branchless_rows}")
        self.stdout.write(f"Filas con match histórico inconsistente: {result.mismatched_recipe_rows}")
        self.stdout.write(f"VentaHistorica bridge eliminada: {result.bridge_history_deleted}")
        self.stdout.write(f"VentaHistorica bridge recreada: {result.bridge_history_created}")
