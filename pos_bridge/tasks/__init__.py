"""Callable tasks for pos_bridge."""
from pos_bridge.tasks.celery_tasks import (
    task_daily_sales_sync,
    task_inventory_sync,
    task_production_sync,
    task_product_recipe_sync,
    task_realtime_inventory_sync,
    task_recipe_gap_audit,
    task_retry_failed_jobs,
    task_transfer_sync,
    task_weekly_cost_snapshot,
    task_waste_sync,
)

__all__ = (
    "task_daily_sales_sync",
    "task_inventory_sync",
    "task_production_sync",
    "task_product_recipe_sync",
    "task_realtime_inventory_sync",
    "task_recipe_gap_audit",
    "task_retry_failed_jobs",
    "task_transfer_sync",
    "task_weekly_cost_snapshot",
    "task_waste_sync",
)
