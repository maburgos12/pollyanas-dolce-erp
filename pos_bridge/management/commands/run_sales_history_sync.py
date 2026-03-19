from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync

DEFAULT_BACKFILL_START = date(2022, 1, 1)
DEFAULT_ALREADY_LOADED_RANGE = (date(2026, 1, 1), date(2026, 3, 13))


def _parse_date(value: str, *, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise CommandError(f"{label} debe tener formato YYYY-MM-DD.") from exc


class Command(BaseCommand):
    help = "Ejecuta backfill de ventas históricas Point por producto y sucursal hacia pos_bridge/VentaHistorica."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", default=DEFAULT_BACKFILL_START.isoformat(), help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", default="", help="Fecha final YYYY-MM-DD. Si se omite, usa ayer local.")
        parser.add_argument("--branch", default="", help="Filtra por sucursal Point (id o nombre parcial).")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecución.")
        parser.add_argument("--max-days", type=int, default=None, help="Límite de días calendario a procesar.")
        parser.add_argument(
            "--skip-range",
            action="append",
            default=[],
            help="Rango excluido YYYY-MM-DD:YYYY-MM-DD. Se puede repetir.",
        )
        parser.add_argument(
            "--no-default-skip-range",
            action="store_true",
            help="No excluir el rango ya incorporado del 2026-01-01 al 2026-03-13.",
        )

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        start_date = _parse_date((options.get("start_date") or "").strip(), label="start-date")
        end_raw = (options.get("end_date") or "").strip()
        end_date = _parse_date(end_raw, label="end-date") if end_raw else (timezone.localdate() - timedelta(days=1))
        if end_date < start_date:
            raise CommandError("end-date no puede ser menor a start-date.")

        excluded_ranges: list[tuple[date, date]] = []
        if not options.get("no_default_skip_range"):
            excluded_ranges.append(DEFAULT_ALREADY_LOADED_RANGE)
        for item in options.get("skip_range") or []:
            raw = str(item or "").strip()
            if ":" not in raw:
                raise CommandError(f"skip-range inválido '{raw}'. Usa YYYY-MM-DD:YYYY-MM-DD.")
            raw_start, raw_end = raw.split(":", 1)
            excluded_ranges.append(
                (
                    _parse_date(raw_start.strip(), label="skip-range inicio"),
                    _parse_date(raw_end.strip(), label="skip-range fin"),
                )
            )

        sync_job = run_sales_history_sync(
            start_date=start_date,
            end_date=end_date,
            excluded_ranges=excluded_ranges,
            triggered_by=actor,
            branch_filter=(options.get("branch") or "").strip() or None,
            max_days=options.get("max_days"),
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
