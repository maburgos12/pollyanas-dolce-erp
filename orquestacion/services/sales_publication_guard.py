from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from django.utils import timezone

from pos_bridge.models import PointDailySale, PointSyncJob
from reportes.dashboard_sales_dataset import get_dashboard_sales_dataset
from reportes.models import FactVentaDiaria


@dataclass(frozen=True)
class SalesPublicationGapScanResult:
    reference_date: date
    target_date: date | None
    point_latest_date: date | None
    fact_latest_date: date | None
    visible_cut_date: date | None
    fact_lag_days: int
    visible_lag_days: int
    severity: str
    is_blocking: bool
    reason: str
    suggestion: str
    sync_job_status: str
    sync_job_started_at: str
    sync_job_finished_at: str
    deferred_by_active_sync: bool

    @property
    def has_gap(self) -> bool:
        return (
            bool(self.target_date)
            and (self.fact_lag_days > 0 or self.visible_lag_days > 0)
            and not self.deferred_by_active_sync
        )


def scan_sales_publication_gap(*, reference_date: date | None = None) -> SalesPublicationGapScanResult:
    effective_reference = reference_date or (timezone.localdate() - timedelta(days=1))
    point_latest_date = (
        PointDailySale.objects.filter(sale_date__lte=effective_reference)
        .order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )
    target_date = min(point_latest_date, effective_reference) if point_latest_date else None
    fact_latest_date = (
        FactVentaDiaria.objects.filter(fecha__lte=effective_reference)
        .order_by("-fecha")
        .values_list("fecha", flat=True)
        .first()
    )
    visible_cut_date = _visible_cut_for(effective_reference)
    latest_sales_sync = (
        PointSyncJob.objects.filter(job_type=PointSyncJob.JOB_TYPE_SALES)
        .order_by("-started_at", "-id")
        .only("status", "started_at", "finished_at")
        .first()
    )
    fact_lag_days = _lag_days(target_date=target_date, actual_date=fact_latest_date)
    visible_lag_days = _lag_days(target_date=target_date, actual_date=visible_cut_date)
    max_lag = max(fact_lag_days, visible_lag_days)
    deferred_by_active_sync = _is_active_sales_sync(latest_sales_sync=latest_sales_sync, reference_date=effective_reference)
    severity = "high" if max_lag > 1 else "warning"

    if not target_date or max_lag == 0:
        reason = "Sin gap de publicación visible entre PointDailySale, facts y corte visible del dashboard."
    elif deferred_by_active_sync:
        reason = (
            "Existe rezago visible, pero el detector lo difiere temporalmente porque hay un sync de ventas "
            "activo o recién iniciado para la fecha de referencia."
        )
    else:
        reason = (
            "Existe rezago entre la extracción cerrada en PointDailySale y la publicación visible del ERP; "
            "la capa analítica o el corte visible no alcanzan todavía el último corte Point elegible."
        )

    if deferred_by_active_sync:
        suggestion = (
            "Esperar a que termine el sync de ventas activo y volver a correr "
            "`./.venv/bin/python manage.py run_quality_guards`; si el rezago persiste después del sync, "
            "ejecutar refresh de analytics."
        )
    else:
        suggestion = (
            "Revisar la publicación canónica y ejecutar `./.venv/bin/python manage.py refresh_analytics_layer "
            f"--date {target_date.isoformat() if target_date else effective_reference.isoformat()} --lookback-days {max(max_lag + 1, 3)}` "
            "si el sync diario ya terminó y el rezago persiste."
        )

    return SalesPublicationGapScanResult(
        reference_date=effective_reference,
        target_date=target_date,
        point_latest_date=point_latest_date,
        fact_latest_date=fact_latest_date,
        visible_cut_date=visible_cut_date,
        fact_lag_days=fact_lag_days,
        visible_lag_days=visible_lag_days,
        severity=severity,
        is_blocking=False,
        reason=reason,
        suggestion=suggestion,
        sync_job_status=latest_sales_sync.status if latest_sales_sync else "",
        sync_job_started_at=latest_sales_sync.started_at.isoformat() if latest_sales_sync and latest_sales_sync.started_at else "",
        sync_job_finished_at=latest_sales_sync.finished_at.isoformat() if latest_sales_sync and latest_sales_sync.finished_at else "",
        deferred_by_active_sync=deferred_by_active_sync,
    )


def _lag_days(*, target_date: date | None, actual_date: date | None) -> int:
    if not target_date or not actual_date:
        return 0 if not target_date else 999
    if actual_date >= target_date:
        return 0
    return (target_date - actual_date).days


def _visible_cut_for(today: date) -> date | None:
    snapshot = dict(get_dashboard_sales_dataset(today=today).get("daily_sales_snapshot") or {})
    latest_date = snapshot.get("date")
    return latest_date if isinstance(latest_date, date) else None


def _is_active_sales_sync(*, latest_sales_sync: PointSyncJob | None, reference_date: date) -> bool:
    if latest_sales_sync is None:
        return False
    if latest_sales_sync.status not in {PointSyncJob.STATUS_PENDING, PointSyncJob.STATUS_RUNNING}:
        return False
    started_at = latest_sales_sync.started_at
    if started_at is None:
        return False
    return started_at.date() >= reference_date
