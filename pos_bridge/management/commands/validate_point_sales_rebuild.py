from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.sales_pipeline import PointSalesRebuildService, PointSalesValidationService


class Command(BaseCommand):
    help = "Genera el reporte de conciliación del pipeline Point v2 para un job existente."

    def add_arguments(self, parser):
        parser.add_argument("--job-id", required=True, type=int)

    def handle(self, *args, **options):
        rebuild_service = PointSalesRebuildService()
        validation_service = PointSalesValidationService()
        try:
            sync_job = rebuild_service.get_job(job_id=options["job_id"])
        except Exception as exc:  # noqa: BLE001
            raise CommandError(f"No se pudo cargar el job {options['job_id']}: {exc}") from exc
        report = validation_service.build_report(sync_job=sync_job)
        self.stdout.write(f"job_id={sync_job.id}")
        self.stdout.write(f"summary={report['summary']}")
        self.stdout.write(f"reconciliation_summary={report['reconciliation_summary']}")
        self.stdout.write(f"report_dir={validation_service.report_dir(sync_job=sync_job)}")
