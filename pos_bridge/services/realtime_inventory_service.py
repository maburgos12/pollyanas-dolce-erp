from __future__ import annotations

import json
import os
from datetime import time
from urllib import error, request

from django.utils import timezone

from pos_bridge.models import PointSyncJob
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.utils.logger import get_pos_bridge_logger

logger = get_pos_bridge_logger()

OPERATION_START = time(7, 0)
OPERATION_END = time(22, 0)


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    return [item.strip() for item in raw.split(",") if item.strip()] if raw else []


def is_within_operation_hours() -> bool:
    now_local = timezone.localtime(timezone.now()).time()
    return OPERATION_START <= now_local <= OPERATION_END


class RealtimeInventoryService:
    """
    Run higher-frequency inventory syncs for e-commerce critical branches.

    This stays on top of the current PointSyncService and can later be moved to
    Celery or any Linux scheduler without changing business logic.
    """

    def __init__(self, *, sync_service: PointSyncService | None = None):
        self.sync_service = sync_service or PointSyncService()
        self.webhook_url = os.getenv("POS_BRIDGE_ECOMMERCE_WEBHOOK_URL", "").strip()
        self.realtime_branches = _env_list("POS_BRIDGE_REALTIME_BRANCHES")

    def should_run(self) -> bool:
        if not is_within_operation_hours():
            logger.info("Fuera de horario operativo, se omite sync realtime de inventario.")
            return False
        return True

    def has_running_inventory_job(self) -> bool:
        return PointSyncJob.objects.filter(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_RUNNING,
        ).exists()

    def run_sync(self, *, force: bool = False, triggered_by=None) -> list[PointSyncJob]:
        if not force and not self.should_run():
            return []
        if self.has_running_inventory_job():
            logger.info("Se omite sync realtime de inventario porque ya existe un job inventory RUNNING.")
            return []

        branches = self.realtime_branches or [None]
        jobs: list[PointSyncJob] = []
        for branch in branches:
            job = self.sync_service.run_inventory_sync(
                triggered_by=triggered_by,
                branch_filter=branch,
            )
            jobs.append(job)

        if self.webhook_url:
            self._notify_ecommerce(jobs)
        return jobs

    def _notify_ecommerce(self, jobs: list[PointSyncJob]) -> None:
        successful = [job for job in jobs if job.status == PointSyncJob.STATUS_SUCCESS]
        if not successful:
            return

        payload = {
            "event": "inventory_updated",
            "timestamp": timezone.now().isoformat(),
            "sync_jobs": [
                {
                    "id": job.id,
                    "status": job.status,
                    "summary": job.result_summary,
                }
                for job in successful
            ],
        }
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            self.webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=10) as response:
                logger.info("Webhook e-commerce enviado. status=%s", response.status)
        except (error.URLError, TimeoutError, OSError) as exc:
            logger.warning("No se pudo notificar al e-commerce: %s", exc)


def run_realtime_inventory_sync(*, force: bool = False, triggered_by=None) -> list[PointSyncJob]:
    service = RealtimeInventoryService()
    return service.run_sync(force=force, triggered_by=triggered_by)
