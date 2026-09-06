"""Exclusive execution and recoverable queue leases for the recipe buttons."""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import timedelta
from functools import wraps
import time
import logging

from django.db import connection
from django.utils import timezone
from pos_bridge.models import PointSyncJob

ACTIONS = ("SYNC_ALL_RECIPES", "SYNC_ONLY_NEW_PRODUCTS")
DEADLINE = ContextVar("catalog_recipe_deadline", default=None)
MAX_AUTO_RECOVERIES = 2
logger = logging.getLogger(__name__)


def remaining_seconds():
    deadline = DEADLINE.get()
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(
            "Point excedió el tiempo de actualización. Vuelve a pulsar el botón para retomar los productos pendientes."
        )
    return remaining


@contextmanager
def execution_lock(job_id):
    key = 7532026091000000 + int(job_id)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", [key])
        acquired = cursor.fetchone()[0]
    try:
        yield acquired
    finally:
        if acquired:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", [key])


def recover_abandoned_catalog_jobs():
    from pos_bridge.tasks import task_catalog_recipe_sync

    cutoff = timezone.now() - timedelta(minutes=5)
    jobs = PointSyncJob.objects.filter(
        job_type=PointSyncJob.JOB_TYPE_RECIPES,
        parameters__action__in=ACTIONS,
        status__in=["PENDING", "RUNNING"],
        updated_at__lt=cutoff,
    )
    for job in jobs:
        with execution_lock(job.id) as acquired:
            if not acquired:
                continue  # A live executor owns it; never start a competing writer.
            # The executor may have finished between the candidate query and
            # acquiring its lock. Re-read under ownership before changing state.
            job.refresh_from_db()
            if job.status not in {"PENDING", "RUNNING"} or job.updated_at >= cutoff:
                continue
            recoveries = int(job.parameters.get("auto_recoveries") or 0)
            exhausted = recoveries >= MAX_AUTO_RECOVERIES
            job.status = "FAILED" if exhausted else "PENDING"
            job.finished_at = timezone.now() if exhausted else None
            detail = (
                "Se agotaron los dos reintentos automáticos. Revisa la conexión y pulsa nuevamente para retomar sin duplicar."
                if exhausted
                else f"Recuperación automática {recoveries + 1}/{MAX_AUTO_RECOVERIES}: esperando al procesador de recetas."
            )
            job.error_message = detail if exhausted else ""
            job.parameters = {
                **job.parameters,
                "auto_recoveries": recoveries if exhausted else recoveries + 1,
                "last_recovery_at": timezone.now().isoformat(),
                "progress": {
                    "stage": "FAILED" if exhausted else "QUEUED",
                    "detail": detail,
                },
            }
            job.save(
                update_fields=[
                    "status",
                    "finished_at",
                    "error_message",
                    "parameters",
                    "updated_at",
                ]
            )
        if not exhausted:
            # Release ownership BEFORE publishing: a fast consumer must be able
            # to acquire it. If publication is lost, the next scan retries it.
            try:
                task_catalog_recipe_sync.apply_async(
                    kwargs={"job_id": job.id}, retry=False, ignore_result=True
                )
            except Exception:
                logger.exception("Could not republish recipe job %s", job.id)
                PointSyncJob.objects.filter(
                    pk=job.pk,
                    status="PENDING",
                    parameters__auto_recoveries=recoveries + 1,
                ).update(
                    error_message="No se pudo publicar el reintento; el supervisor volverá a comprobarlo."
                )


def guarded_catalog_execution(fn):
    @wraps(fn)
    def wrapped(self, *, job_id):
        routing_key = (self.request.delivery_info or {}).get("routing_key")
        if routing_key and routing_key != "recipes":
            # Messages queued before deployment must not run on the solo worker.
            self.apply_async(
                kwargs={"job_id": job_id},
                queue="recipes",
                retry=False,
                ignore_result=True,
            )
            return {"job_id": job_id, "status": "PENDING"}
        with execution_lock(job_id) as acquired:
            if not acquired:
                return {"job_id": job_id, "status": "RUNNING"}
            job = PointSyncJob.objects.get(id=job_id)
            if job.status != "PENDING":
                return {"job_id": job_id, "status": job.status}
            token = DEADLINE.set(time.monotonic() + 900)
            try:
                return fn(self, job_id=job_id)
            finally:
                DEADLINE.reset(token)

    return wrapped
