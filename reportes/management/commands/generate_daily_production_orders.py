from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.utils import timezone

from reportes.auto_production_service import generate_daily_production_orders, sync_production_execution_logs


class Command(BaseCommand):
    help = "Genera órdenes diarias de producción en estado PROPOSED y opcionalmente sincroniza bitácora de ejecución."

    def add_arguments(self, parser):
        parser.add_argument("--fecha", help="Fecha operativa en formato YYYY-MM-DD. Default: hoy.")
        parser.add_argument("--sucursal-id", type=int, help="Sucursal específica.")
        parser.add_argument(
            "--sync-execution",
            action="store_true",
            help="Sincroniza ProductionExecutionLog para la fecha indicada después de generar.",
        )

    def handle(self, *args, **options):
        target_date = date.fromisoformat(options["fecha"]) if options.get("fecha") else timezone.localdate()
        sucursal_id = options.get("sucursal_id")
        result = generate_daily_production_orders(target_date, sucursal_id=sucursal_id)
        self.stdout.write(
            self.style.SUCCESS(
                f"Production orders target={target_date} generated={result['generated_orders']} "
                f"updated={result['updated_orders']} skipped_locked={result['skipped_locked_orders']} lines={result['lines']}"
            )
        )
        if options.get("sync_execution"):
            sync_result = sync_production_execution_logs(target_date=target_date, sucursal_id=sucursal_id)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Execution logs synced target={target_date} orders={sync_result['orders']} logs={sync_result['logs']}"
                )
            )
