from __future__ import annotations

import time

from django.core.management.base import BaseCommand

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.tasks.run_inventory_sync import run_inventory_sync
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync
from pos_bridge.tasks.run_production_sync import run_production_sync
from pos_bridge.tasks.run_transfer_sync import run_transfer_sync
from pos_bridge.tasks.run_waste_sync import run_waste_sync


class Command(BaseCommand):
    help = "Worker simple para correr sincronizaciones periódicas de pos_bridge."

    def add_arguments(self, parser):
        parser.add_argument("--interval-hours", type=int, default=None, help="Intervalo entre corridas.")
        parser.add_argument("--interval-minutes", type=int, default=None, help="Intervalo en minutos para workers de alta frecuencia.")
        parser.add_argument("--branch", default="", help="Filtra por sucursal.")
        parser.add_argument("--limit-branches", type=int, default=None, help="Límite de sucursales por corrida.")
        parser.add_argument("--run-inventory", action="store_true", help="Incluye sync de inventario en cada ciclo.")
        parser.add_argument("--run-sales", action="store_true", help="Incluye sync incremental de ventas en cada ciclo.")
        parser.add_argument("--run-waste", action="store_true", help="Incluye sync incremental de mermas en cada ciclo.")
        parser.add_argument("--run-production", action="store_true", help="Incluye sync incremental de producción en cada ciclo.")
        parser.add_argument("--run-transfers", action="store_true", help="Incluye sync incremental de transferencias en cada ciclo.")
        parser.add_argument("--sales-days", type=int, default=3, help="Días de ventas a reprocesar por ciclo.")
        parser.add_argument("--sales-lag-days", type=int, default=1, help="Desfase de días para ventas.")
        parser.add_argument("--movement-days", type=int, default=1, help="Días de mermas/producción/transferencias a reprocesar por ciclo.")
        parser.add_argument("--movement-lag-days", type=int, default=1, help="Desfase de días para mermas/producción/transferencias.")
        parser.add_argument("--once", action="store_true", help="Ejecuta una sola vez y termina.")

    def handle(self, *args, **options):
        settings = load_point_bridge_settings()
        interval_minutes = options.get("interval_minutes")
        if interval_minutes is not None:
            interval_seconds = max(int(interval_minutes) * 60, 300)
        else:
            interval_hours = int(options.get("interval_hours") or settings.sync_interval_hours)
            interval_seconds = max(interval_hours * 3600, 300)
        branch_filter = (options.get("branch") or "").strip() or None
        limit_branches = options.get("limit_branches")
        run_inventory = bool(options.get("run_inventory"))
        run_sales = bool(options.get("run_sales"))
        run_waste = bool(options.get("run_waste"))
        run_production = bool(options.get("run_production"))
        run_transfers = bool(options.get("run_transfers"))
        sales_days = max(int(options.get("sales_days") or 1), 1)
        sales_lag_days = max(int(options.get("sales_lag_days") or 0), 0)
        movement_days = max(int(options.get("movement_days") or 1), 1)
        movement_lag_days = max(int(options.get("movement_lag_days") or 0), 0)

        if not run_inventory and not run_sales and not run_waste and not run_production and not run_transfers:
            run_inventory = True

        while True:
            if run_inventory:
                run_inventory_sync(branch_filter=branch_filter, limit_branches=limit_branches)
            if run_sales:
                run_daily_sales_sync(
                    branch_filter=branch_filter,
                    lookback_days=sales_days,
                    lag_days=sales_lag_days,
                )
            if run_waste:
                run_waste_sync(
                    branch_filter=branch_filter,
                    lookback_days=movement_days,
                    lag_days=movement_lag_days,
                )
            if run_production:
                run_production_sync(
                    branch_filter=branch_filter,
                    lookback_days=movement_days,
                    lag_days=movement_lag_days,
                )
            if run_transfers:
                run_transfer_sync(
                    branch_filter=branch_filter,
                    lookback_days=movement_days,
                    lag_days=movement_lag_days,
                )
            if options.get("once"):
                break
            time.sleep(interval_seconds)
