from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.movement_sync_service import PointMovementSyncService


class Command(BaseCommand):
    help = "Sincroniza transferencias recibidas de Point hacia staging y entradas CEDIS."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=True, help="Fecha inicio YYYY-MM-DD")
        parser.add_argument("--end-date", required=True, help="Fecha fin YYYY-MM-DD")
        parser.add_argument("--branch", dest="branch_filter", default="", help="Filtro opcional de sucursal")

    def handle(self, *args, **options):
        try:
            start_date = date.fromisoformat(options["start_date"])
            end_date = date.fromisoformat(options["end_date"])
        except ValueError as exc:
            raise CommandError("Las fechas deben usar formato YYYY-MM-DD.") from exc

        service = PointMovementSyncService()
        job = service.run_transfer_sync(
            start_date=start_date,
            end_date=end_date,
            branch_filter=options["branch_filter"] or None,
        )
        self.stdout.write(self.style.SUCCESS(f"Job {job.id} {job.status}"))
        self.stdout.write(str(job.result_summary or {}))
