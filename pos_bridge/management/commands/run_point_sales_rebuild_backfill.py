from __future__ import annotations

from datetime import date
import socket

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.sales_pipeline import PointSalesRebuildService, PointSalesValidationService


class Command(BaseCommand):
    help = "Reconstruye ventas Point v2 por sucursal+día con staging auditable, facts y conciliación."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=False, default="2022-01-01")
        parser.add_argument("--end-date", required=False, default="2026-04-04")
        parser.add_argument("--branch", default="")
        parser.add_argument("--job-id", type=int)
        parser.add_argument("--credito-scope", default="null")
        parser.add_argument("--batch-size", type=int, default=10)
        parser.add_argument("--max-tasks", type=int)
        parser.add_argument("--worker-name", default="")
        parser.add_argument("--plan-only", action="store_true")
        parser.add_argument("--no-promote-authoritative", action="store_true")
        parser.add_argument("--promote-legacy-history", action="store_true")
        parser.add_argument("--legacy-source-label", default="POINT_BRIDGE_SALES_V2")
        parser.add_argument("--build-report", action="store_true")

    def handle(self, *args, **options):
        service = PointSalesRebuildService()
        validation_service = PointSalesValidationService()
        worker_name = options["worker_name"] or f"{socket.gethostname()}:{self.__class__.__name__}"
        credito_scope = (options["credito_scope"] or "null").strip() or "null"
        if "," in credito_scope:
            raise CommandError("Este pipeline v2 solo permite un credito_scope por corrida autoritativa.")

        if options.get("job_id"):
            sync_job = service.get_job(job_id=options["job_id"])
        else:
            try:
                start_date = date.fromisoformat(options["start_date"])
                end_date = date.fromisoformat(options["end_date"])
            except ValueError as exc:
                raise CommandError(f"Fecha inválida: {exc}") from exc
            sync_job = service.create_backfill_job(
                start_date=start_date,
                end_date=end_date,
                branch_filter=options["branch"] or None,
                credito_scope=credito_scope,
            )

        self.stdout.write(f"job_id={sync_job.id}")
        if options["plan_only"]:
            summary = service.build_job_summary(sync_job=sync_job)
            self.stdout.write(self.style.WARNING(f"Planeado sin ejecutar: {summary}"))
            return

        sync_job = service.run_worker(
            sync_job=sync_job,
            worker_name=worker_name,
            batch_size=options["batch_size"],
            max_tasks=options.get("max_tasks"),
            promote_authoritative=not options["no_promote_authoritative"],
        )
        self.stdout.write(f"estado={sync_job.status}")
        self.stdout.write(f"summary={sync_job.result_summary}")

        if options["promote_legacy_history"]:
            promotion = service.promote_detail_to_legacy_history(
                sync_job=sync_job,
                source_label=options["legacy_source_label"],
            )
            self.stdout.write(f"legacy_history={promotion}")

        if options["build_report"] and sync_job.status in {"SUCCESS", "PARTIAL", "FAILED"}:
            report = validation_service.build_report(sync_job=sync_job)
            self.stdout.write(f"reconciliation_summary={report['reconciliation_summary']}")
            self.stdout.write(f"report_dir={validation_service.report_dir(sync_job=sync_job)}")
