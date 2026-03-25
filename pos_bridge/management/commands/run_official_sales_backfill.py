from __future__ import annotations

import json
from datetime import date, datetime

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.official_sales_backfill_service import OfficialSalesBackfillService


class Command(BaseCommand):
    help = "Ejecuta backfill oficial de ventas Point usando PrintReportes (Ventas por Categoría) por sucursal y día."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=True, help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", required=True, help="Fecha final YYYY-MM-DD.")
        parser.add_argument("--branch", default="", help="Filtra por sucursal Point (id o nombre).")
        parser.add_argument(
            "--credito-scopes",
            default="null",
            help="Scopes separados por coma: false,true,null",
        )

    def handle(self, *args, **options):
        try:
            start_date = datetime.strptime(options["start_date"], "%Y-%m-%d").date()
            end_date = datetime.strptime(options["end_date"], "%Y-%m-%d").date()
        except ValueError as exc:
            raise CommandError("Fechas inválidas. Usa YYYY-MM-DD.") from exc
        if end_date < start_date:
            raise CommandError("end-date no puede ser menor que start-date.")

        credito_scopes = [item.strip() for item in str(options["credito_scopes"] or "").split(",") if item.strip()]
        invalid = [item for item in credito_scopes if item not in {"false", "true", "null"}]
        if invalid:
            raise CommandError(f"Scopes crédito inválidos: {', '.join(invalid)}")

        sync_job = OfficialSalesBackfillService().run(
            start_date=start_date,
            end_date=end_date,
            branch_filter=(options["branch"] or "").strip() or None,
            credito_scopes=credito_scopes,
        )
        payload = {
            "job_id": sync_job.id,
            "status": sync_job.status,
            "summary": sync_job.result_summary,
            "error_message": sync_job.error_message,
            "artifacts": sync_job.artifacts,
            "parameters": sync_job.parameters,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
