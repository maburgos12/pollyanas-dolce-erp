from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.utils import timezone

from reportes.alert_service import generate_operational_alerts
from reportes.analytics_service import refresh_incremental
from reportes.auto_production_service import generate_daily_production_orders
from reportes.auto_purchase_service import generate_purchase_requests_from_production
from reportes.operations_metrics_service import rebuild_operations_metrics


class Command(BaseCommand):
    help = (
        "Ejecuta el ciclo operativo diario: refresh analítico, órdenes de producción, "
        "compras derivadas, alertas y métricas DG."
    )

    def add_arguments(self, parser):
        parser.add_argument("--fecha", help="Fecha operativa en formato YYYY-MM-DD. Default: hoy.")
        parser.add_argument("--lookback-days", type=int, default=3, help="Ventana incremental hacia atrás para analytics.")
        parser.add_argument("--skip-refresh", action="store_true", help="No refresca la capa analítica antes del ciclo.")
        parser.add_argument("--sucursal-id", type=int, help="Sucursal específica.")

    def handle(self, *args, **options):
        target_date = date.fromisoformat(options["fecha"]) if options.get("fecha") else timezone.localdate()
        sucursal_id = options.get("sucursal_id")

        if not options.get("skip_refresh"):
            summary = refresh_incremental(reference_date=target_date, lookback_days=int(options["lookback_days"] or 3))
            self.stdout.write(
                self.style.NOTICE(
                    f"Analytics refreshed sales={summary.sales_rows} inventory={summary.inventory_rows} "
                    f"production={summary.production_rows} forecast={summary.forecast_rows}"
                )
            )

        production_result = generate_daily_production_orders(target_date, sucursal_id=sucursal_id)
        purchase_result = generate_purchase_requests_from_production(target_date, sucursal_id=sucursal_id)
        alert_result = generate_operational_alerts(target_date=target_date)
        metrics_result = rebuild_operations_metrics(target_date=target_date)

        self.stdout.write(
            self.style.SUCCESS(
                f"Production generated={production_result['generated_orders']} updated={production_result['updated_orders']} "
                f"lines={production_result['lines']}"
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Purchases generated={purchase_result['generated']} updated={purchase_result['updated']} "
                f"deleted={purchase_result['deleted']} lines={purchase_result['lines']}"
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Alerts total={alert_result['created_or_updated']} critical={alert_result['critical']} "
                f"adoption={metrics_result['adoption_pct']}%"
            )
        )
