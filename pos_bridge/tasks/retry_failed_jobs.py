from __future__ import annotations

from pos_bridge.services.sync_service import PointSyncService


def retry_failed_jobs(*, triggered_by=None, limit: int = 5, max_attempts: int | None = None):
    service = PointSyncService()
    return service.retry_failed_jobs(triggered_by=triggered_by, limit=limit, max_attempts=max_attempts)
