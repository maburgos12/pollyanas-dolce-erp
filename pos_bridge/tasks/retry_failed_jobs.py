from __future__ import annotations

from datetime import timedelta

from django.utils import timezone

from pos_bridge.models import PointSyncJob
from pos_bridge.services.sync_service import PointSyncService

MAX_RUNNING_HOURS = 2


def cleanup_stale_running_jobs() -> int:
    """Marca como FAILED los jobs RUNNING que llevan más de MAX_RUNNING_HOURS sin actualizar."""
    cutoff = timezone.now() - timedelta(hours=MAX_RUNNING_HOURS)
    stale = PointSyncJob.objects.filter(
        status=PointSyncJob.STATUS_RUNNING,
        updated_at__lt=cutoff,
    )
    count = stale.count()
    if count:
        stale.update(
            status=PointSyncJob.STATUS_FAILED,
            error_message=f"Auto-marcado FAILED: job RUNNING por más de {MAX_RUNNING_HOURS}h sin actualizar.",
            finished_at=timezone.now(),
        )
    return count


def retry_failed_jobs(*, triggered_by=None, limit: int = 5, max_attempts: int | None = None):
    cleanup_stale_running_jobs()
    service = PointSyncService()
    return service.retry_failed_jobs(triggered_by=triggered_by, limit=limit, max_attempts=max_attempts)
