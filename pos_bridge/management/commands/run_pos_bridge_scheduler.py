from __future__ import annotations

import time

from django.core.management.base import BaseCommand

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.tasks.run_inventory_sync import run_inventory_sync
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync


class Command(BaseCommand):
    help = "Worker simple para correr sincronizaciones periódicas de pos_bridge."

    def add_arguments(self, parser):
        parser.add_argument("--interval-hours", type=int, default=None, help="Intervalo entre corridas.")
        parser.add_argument("--branch", default="", help="Filtra por sucursal.")
        parser.add_argument("--limit-branches", type=int, default=None, help="Límite de sucursales por corrida.")
        parser.add_argument("--run-inventory", action="store_true", help="Incluye sync de inventario en cada ciclo.")
        parser.add_argument("--run-sales", action="store_true", help="Incluye sync incremental de ventas en cada ciclo.")
        parser.add_argument("--sales-days", type=int, default=3, help="Días de ventas a reprocesar por ciclo.")
        parser.add_argument("--sales-lag-days", type=int, default=1, help="Desfase de días para ventas.")
        parser.add_argument("--once", action="store_true", help="Ejecuta una sola vez y termina.")

    def handle(self, *args, **options):
        settings = load_point_bridge_settings()
        interval_hours = int(options.get("interval_hours") or settings.sync_interval_hours)
        interval_seconds = max(interval_hours * 3600, 300)
        branch_filter = (options.get("branch") or "").strip() or None
        limit_branches = options.get("limit_branches")
        run_inventory = bool(options.get("run_inventory"))
        run_sales = bool(options.get("run_sales"))
        sales_days = max(int(options.get("sales_days") or 1), 1)
        sales_lag_days = max(int(options.get("sales_lag_days") or 0), 0)

        if not run_inventory and not run_sales:
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
            if options.get("once"):
                break
            time.sleep(interval_seconds)
