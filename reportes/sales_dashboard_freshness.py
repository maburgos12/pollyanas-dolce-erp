from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from core.audit import log_event
from pos_bridge.models import PointDailySale
from reportes.analytics_service import refresh_incremental
from reportes.dashboard_sales_dataset import get_dashboard_sales_dataset
from reportes.models import FactVentaDiaria


@dataclass(slots=True)
class SalesDashboardFreshnessResult:
    target_date: date | None
    point_latest_date: date | None
    fact_latest_date_before: date | None
    fact_latest_date_after: date | None
    visible_cut_date_before: date | None
    visible_cut_date_after: date | None
    catchup_attempted: bool
    catchup_succeeded: bool
    lag_days_before: int
    lag_days_after: int


def _lag_days(*, target_date: date | None, fact_date: date | None) -> int:
    if not target_date:
        return 0
    if not fact_date:
        return 0
    if fact_date >= target_date:
        return 0
    return (target_date - fact_date).days


def _visible_cut_for(today: date) -> date | None:
    snapshot = dict(get_dashboard_sales_dataset(today=today).get("daily_sales_snapshot") or {})
    latest_date = snapshot.get("date")
    return latest_date if isinstance(latest_date, date) else None


def ensure_sales_dashboard_freshness(
    *,
    reference_date: date,
    lookback_days: int,
    triggered_by=None,
    trigger: str,
) -> SalesDashboardFreshnessResult:
    point_latest_date = (
        PointDailySale.objects.filter(sale_date__lte=reference_date)
        .order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )
    target_date = min(point_latest_date, reference_date) if point_latest_date else None
    fact_latest_date_before = (
        FactVentaDiaria.objects.filter(fecha__lte=reference_date)
        .order_by("-fecha")
        .values_list("fecha", flat=True)
        .first()
    )
    visible_cut_date_before = _visible_cut_for(reference_date)
    lag_days_before = _lag_days(target_date=target_date, fact_date=fact_latest_date_before)

    result = SalesDashboardFreshnessResult(
        target_date=target_date,
        point_latest_date=point_latest_date,
        fact_latest_date_before=fact_latest_date_before,
        fact_latest_date_after=fact_latest_date_before,
        visible_cut_date_before=visible_cut_date_before,
        visible_cut_date_after=visible_cut_date_before,
        catchup_attempted=False,
        catchup_succeeded=lag_days_before == 0,
        lag_days_before=lag_days_before,
        lag_days_after=lag_days_before,
    )

    if not target_date or lag_days_before == 0:
        return result

    catchup_lookback_days = max(int(lookback_days or 1), lag_days_before + 1)
    result.catchup_attempted = True
    refresh_incremental(reference_date=target_date, lookback_days=catchup_lookback_days)

    fact_latest_date_after = (
        FactVentaDiaria.objects.filter(fecha__lte=reference_date)
        .order_by("-fecha")
        .values_list("fecha", flat=True)
        .first()
    )
    visible_cut_date_after = _visible_cut_for(reference_date)
    lag_days_after = _lag_days(target_date=target_date, fact_date=fact_latest_date_after)

    result.fact_latest_date_after = fact_latest_date_after
    result.visible_cut_date_after = visible_cut_date_after
    result.lag_days_after = lag_days_after
    result.catchup_succeeded = lag_days_after == 0

    payload = {
        "reference_date": reference_date.isoformat(),
        "target_date": target_date.isoformat(),
        "point_latest_date": point_latest_date.isoformat() if point_latest_date else "",
        "fact_latest_date_before": fact_latest_date_before.isoformat() if fact_latest_date_before else "",
        "fact_latest_date_after": fact_latest_date_after.isoformat() if fact_latest_date_after else "",
        "visible_cut_date_before": visible_cut_date_before.isoformat() if visible_cut_date_before else "",
        "visible_cut_date_after": visible_cut_date_after.isoformat() if visible_cut_date_after else "",
        "lag_days_before": lag_days_before,
        "lag_days_after": lag_days_after,
        "lookback_days": catchup_lookback_days,
        "trigger": trigger,
    }
    log_event(
        triggered_by,
        "INTEGRATIONS_SALES_DASHBOARD_CATCHUP_COMPLETED"
        if result.catchup_succeeded
        else "INTEGRATIONS_SALES_DASHBOARD_CATCHUP_FAILED",
        "reportes.FactVentaDiaria",
        target_date.isoformat(),
        payload=payload,
    )
    return result
