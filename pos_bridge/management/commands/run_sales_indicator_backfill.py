from __future__ import annotations

import json
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from pos_bridge.services.sales_branch_indicator_service import PointSalesBranchIndicatorService
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.models import PointExtractionLog, PointSyncJob


class Command(BaseCommand):
    help = "Backfill de tickets e importe diario por sucursal usando /Home/Get_indicadores."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=True, help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", required=True, help="Fecha final YYYY-MM-DD.")
        parser.add_argument("--branch", default="", help="Filtra por sucursal Point (id o nombre).")

    def handle(self, *args, **options):
        try:
            start_date = datetime.strptime(options["start_date"], "%Y-%m-%d").date()
            end_date = datetime.strptime(options["end_date"], "%Y-%m-%d").date()
        except ValueError as exc:
            raise CommandError("Fechas inválidas. Usa YYYY-MM-DD.") from exc
        if end_date < start_date:
            raise CommandError("end-date no puede ser menor que start-date.")

        branch_filter = (options.get("branch") or "").strip()
        service = PointSalesBranchIndicatorService()
        sync_service = PointSyncService()
        sync_job = sync_service.create_job(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            parameters={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "branch_filter": branch_filter,
                "source": "/Home/Get_indicadores",
            },
        )
        sync_service.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de backfill de indicadores diarios por sucursal.")

        branches = service.canonical_branches(branch_filter=branch_filter or None)

        created_count = 0
        updated_count = 0
        processed = 0
        try:
            payloads = service.fetch_range(
                start_date=start_date,
                end_date=end_date,
                branch_external_id=branches[0].external_id if len(branches) == 1 else None,
            )
            allowed_branch_ids = {branch.id for branch in branches}
            for payload in payloads:
                if payload.branch.id not in allowed_branch_ids:
                    continue
                _, created = service.persist_branch_day(indicator_payload=payload, sync_job=sync_job)
                processed += 1
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            sync_service.mark_success(
                sync_job,
                {
                    "branch_days_processed": processed,
                    "rows_created": created_count,
                    "rows_updated": updated_count,
                    "branches_processed": len(branches),
                },
            )
        except Exception as exc:
            sync_service.mark_failure(sync_job, exc)

        payload = {
            "job_id": sync_job.id,
            "status": sync_job.status,
            "summary": sync_job.result_summary,
            "error_message": sync_job.error_message,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
