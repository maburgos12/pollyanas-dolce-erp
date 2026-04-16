from __future__ import annotations

from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from pos_bridge.models import PointSalesExtractionTask, PointSyncJob
from pos_bridge.services.sales_branch_indicator_service import PointSalesBranchIndicatorService
from pos_bridge.utils.dates import iter_business_dates


class PointSalesTaskQueueService:
    PIPELINE_CODE = "POINT_SALES_REBUILD_V2"

    def __init__(self):
        self.branch_service = PointSalesBranchIndicatorService()

    def resolve_branches(self, *, branch_filter: str | None = None):
        return self.branch_service.canonical_branches(branch_filter=branch_filter)

    def ensure_single_running_job(self) -> None:
        running_jobs = PointSyncJob.objects.filter(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
        ).order_by("-started_at")
        for job in running_jobs.only("id", "parameters"):
            parameters = job.parameters or {}
            if parameters.get("pipeline_code") == self.PIPELINE_CODE:
                raise RuntimeError(
                    f"Ya existe un job activo de reconstrucción Point v2 (job_id={job.id}). "
                    "Reanúdalo con --job-id o ciérralo antes de abrir otro."
                )

    def plan_tasks(
        self,
        *,
        sync_job: PointSyncJob,
        start_date,
        end_date,
        branch_filter: str | None = None,
        credito_scope: str = "null",
        source_mode: str = PointSalesExtractionTask.SOURCE_MODE_OFFICIAL,
    ) -> int:
        branches = self.resolve_branches(branch_filter=branch_filter)
        sale_dates = iter_business_dates(start_date, end_date)
        if not branches:
            raise RuntimeError("No se encontraron sucursales canónicas Point para planear el backfill.")

        existing_keys = set(
            PointSalesExtractionTask.objects.filter(sync_job=sync_job).values_list(
                "branch_id",
                "sale_date",
                "credito_scope",
                "source_mode",
            )
        )
        to_create: list[PointSalesExtractionTask] = []
        for sale_date in sale_dates:
            for branch in branches:
                key = (branch.id, sale_date, credito_scope, source_mode)
                if key in existing_keys:
                    continue
                to_create.append(
                    PointSalesExtractionTask(
                        sync_job=sync_job,
                        branch=branch,
                        sale_date=sale_date,
                        credito_scope=credito_scope,
                        source_mode=source_mode,
                        source_endpoint="/Report/PrintReportes?idreporte=3",
                    )
                )
        if to_create:
            PointSalesExtractionTask.objects.bulk_create(to_create, batch_size=1000)
        return len(to_create)

    def requeue_stale_tasks(self, *, sync_job: PointSyncJob, stale_after_minutes: int = 60) -> int:
        cutoff = timezone.now() - timedelta(minutes=max(int(stale_after_minutes or 60), 1))
        return PointSalesExtractionTask.objects.filter(
            sync_job=sync_job,
            status=PointSalesExtractionTask.STATUS_RUNNING,
            claimed_at__lt=cutoff,
        ).update(
            status=PointSalesExtractionTask.STATUS_PENDING,
            worker_name="",
            observations={"requeued": True, "reason": "stale_claim"},
        )

    @transaction.atomic
    def claim_tasks(
        self,
        *,
        sync_job: PointSyncJob,
        worker_name: str,
        limit: int = 10,
    ) -> list[PointSalesExtractionTask]:
        task_ids = list(
            PointSalesExtractionTask.objects.select_for_update(skip_locked=True)
            .filter(
                sync_job=sync_job,
                status__in=[PointSalesExtractionTask.STATUS_PENDING, PointSalesExtractionTask.STATUS_FAILED],
            )
            .order_by("sale_date", "branch_id", "id")
            .values_list("id", flat=True)[: max(int(limit or 1), 1)]
        )
        if not task_ids:
            return []
        tasks = list(
            PointSalesExtractionTask.objects.filter(id__in=task_ids)
            .select_related("branch", "branch__erp_branch")
            .order_by("sale_date", "branch_id", "id")
        )
        for task in tasks:
            task.mark_running(worker_name=worker_name)
            task.attempts += 1
            task.last_error = ""
            task.updated_at = timezone.now()
        PointSalesExtractionTask.objects.bulk_update(
            tasks,
            ["status", "worker_name", "claimed_at", "started_at", "attempts", "last_error", "updated_at"],
        )
        return tasks
