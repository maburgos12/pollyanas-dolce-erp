from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, timedelta

from django.db.models import Max
from django.utils import timezone

from pos_bridge.models import PointSalesDailyProductFact
from pos_bridge.tasks.celery_tasks import task_daily_sales_sync


FORECAST_SALES_LAG_DAYS = 1
FORECAST_SALES_REFRESH_MAX_DAYS = 45


@dataclass(frozen=True)
class ForecastSalesFreshness:
    latest_sale_date: date | None
    target_sale_date: date
    is_fresh: bool
    missing_days: int
    refresh_days: int
    refresh_task_id: str = ""

    @property
    def latest_label(self) -> str:
        if not self.latest_sale_date:
            return "sin ventas cargadas"
        return self.latest_sale_date.isoformat()

    @property
    def target_label(self) -> str:
        return self.target_sale_date.isoformat()


def latest_sales_fact_date() -> date | None:
    return PointSalesDailyProductFact.objects.aggregate(latest=Max("sale_date"))["latest"]


def build_forecast_sales_freshness(
    *,
    latest_sale_date: date | None,
    reference_date: date | None = None,
    lag_days: int = FORECAST_SALES_LAG_DAYS,
) -> ForecastSalesFreshness:
    reference = reference_date or timezone.localdate()
    target_sale_date = reference - timedelta(days=max(int(lag_days), 0))

    if latest_sale_date and latest_sale_date >= target_sale_date:
        return ForecastSalesFreshness(
            latest_sale_date=latest_sale_date,
            target_sale_date=target_sale_date,
            is_fresh=True,
            missing_days=0,
            refresh_days=0,
        )

    missing_days = FORECAST_SALES_REFRESH_MAX_DAYS
    if latest_sale_date:
        missing_days = max((target_sale_date - latest_sale_date).days, 1)

    return ForecastSalesFreshness(
        latest_sale_date=latest_sale_date,
        target_sale_date=target_sale_date,
        is_fresh=False,
        missing_days=missing_days,
        refresh_days=min(missing_days, FORECAST_SALES_REFRESH_MAX_DAYS),
    )


def get_forecast_sales_freshness(
    *,
    reference_date: date | None = None,
    lag_days: int = FORECAST_SALES_LAG_DAYS,
) -> ForecastSalesFreshness:
    return build_forecast_sales_freshness(
        latest_sale_date=latest_sales_fact_date(),
        reference_date=reference_date,
        lag_days=lag_days,
    )


def queue_forecast_sales_refresh_if_needed(
    *,
    triggered_by_id: int | None,
    reference_date: date | None = None,
    lag_days: int = FORECAST_SALES_LAG_DAYS,
) -> ForecastSalesFreshness:
    freshness = get_forecast_sales_freshness(reference_date=reference_date, lag_days=lag_days)
    if freshness.is_fresh:
        return freshness

    task = task_daily_sales_sync.delay(
        days=freshness.refresh_days,
        lag_days=lag_days,
        triggered_by_id=triggered_by_id,
    )
    return replace(freshness, refresh_task_id=getattr(task, "id", ""))
