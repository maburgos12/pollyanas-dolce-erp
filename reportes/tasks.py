from __future__ import annotations

from datetime import date, timedelta

from celery import shared_task


@shared_task(name="reportes.snapshot_historical_costing_task")
def snapshot_historical_costing_task():
    """Congela costo historico del mes anterior al dia 1 de cada mes."""
    from reportes.services_historical_costing import MonthlyHistoricalCostingService

    mes_actual = date.today().replace(day=1)
    periodo = (mes_actual - timedelta(days=1)).replace(day=1)
    summary = MonthlyHistoricalCostingService().build_period(period_start=periodo)
    return {
        "period": f"{periodo:%Y-%m}",
        "insumo_rows": summary.insumo_rows,
        "receta_rows": summary.receta_rows,
        "missing_recipe_rows": summary.missing_recipe_rows,
    }
