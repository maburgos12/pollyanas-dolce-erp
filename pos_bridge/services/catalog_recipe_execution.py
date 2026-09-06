"""Exclusive execution and recoverable queue leases for the recipe buttons."""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import timedelta
from functools import wraps
import time

from django.db import connection
from django.utils import timezone
from pos_bridge.models import PointSyncJob

ACTIONS = ("SYNC_ALL_RECIPES", "SYNC_ONLY_NEW_PRODUCTS")
DEADLINE = ContextVar("catalog_recipe_deadline", default=None)


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
            job.status = "FAILED"
            job.finished_at = timezone.now()
            job.error_message = "El procesador no inició o se interrumpió. Pulsa nuevamente para retomar sin duplicar."
            job.parameters = {
                **job.parameters,
                "progress": {"stage": "FAILED", "detail": job.error_message},
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


def guarded_catalog_execution(fn):
    @wraps(fn)
    def wrapped(self, *, job_id):
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
