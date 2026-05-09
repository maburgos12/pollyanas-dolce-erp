from __future__ import annotations

import json
import os
from datetime import timedelta, time
from urllib import error, request

from django.utils import timezone

from pos_bridge.models import PointSyncJob
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.utils.logger import get_pos_bridge_logger

logger = get_pos_bridge_logger()

OPERATION_START = time(7, 0)
OPERATION_END = time(22, 0)
CEDIS_AUTOMATION_GUARD_START = time(21, 55)
CEDIS_AUTOMATION_GUARD_END = time(22, 45)


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    return [item.strip() for item in raw.split(",") if item.strip()] if raw else []


def is_within_operation_hours() -> bool:
    now_local = timezone.localtime(timezone.now()).time()
    return OPERATION_START <= now_local <= OPERATION_END


def is_within_cedis_automation_guard() -> bool:
    now_local = timezone.localtime(timezone.now()).time()
    return CEDIS_AUTOMATION_GUARD_START <= now_local <= CEDIS_AUTOMATION_GUARD_END


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
        if is_within_cedis_automation_guard():
            logger.info("Ventana CEDIS nocturna activa, se omite sync realtime de inventario.")
            return False
        if not is_within_operation_hours():
            logger.info("Fuera de horario operativo, se omite sync realtime de inventario.")
            return False
        return True

    def has_running_inventory_job(self) -> bool:
        stale_before = timezone.now() - timedelta(minutes=90)
        stale_jobs = PointSyncJob.objects.filter(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_RUNNING,
            started_at__lt=stale_before,
            finished_at__isnull=True,
        )
        stale_count = stale_jobs.update(
            status=PointSyncJob.STATUS_FAILED,
            finished_at=timezone.now(),
            error_message="Marcado como fallido automaticamente: job RUNNING stale bloqueaba inventario realtime.",
        )
        if stale_count:
            logger.warning("Se cerraron %s jobs inventory RUNNING stale antes del sync realtime.", stale_count)
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
                capture_costs=False,
            )
            jobs.append(job)

        if self.webhook_url:
            self._queue_ecommerce_notification(jobs)
        return jobs

    def _build_webhook_payload(self, jobs: list[PointSyncJob]) -> dict | None:
        successful = [job for job in jobs if job.status == PointSyncJob.STATUS_SUCCESS]
        if not successful:
            return None

        return {
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

    def _queue_ecommerce_notification(self, jobs: list[PointSyncJob]) -> None:
        payload = self._build_webhook_payload(jobs)
        if not payload:
            return
        try:
            from pos_bridge.tasks.celery_tasks import task_ecommerce_webhook_delivery

            task_ecommerce_webhook_delivery.delay(webhook_url=self.webhook_url, payload=payload)
            logger.info("Webhook e-commerce encolado para entrega asincrona.")
        except Exception as exc:
            logger.warning("No se pudo encolar webhook e-commerce, se intenta entrega directa: %s", exc)
            deliver_ecommerce_webhook(webhook_url=self.webhook_url, payload=payload)


def deliver_ecommerce_webhook(*, webhook_url: str, payload: dict, timeout_seconds: int = 5) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout_seconds) as response:
        logger.info("Webhook e-commerce enviado. status=%s", response.status)


def run_realtime_inventory_sync(*, force: bool = False, triggered_by=None) -> list[PointSyncJob]:
    service = RealtimeInventoryService()
    return service.run_sync(force=force, triggered_by=triggered_by)
