from __future__ import annotations

import json
from datetime import date, datetime

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.tasks.run_attendance_sync import run_attendance_sync
from pos_bridge.utils.dates import resolve_incremental_window


def _parse_date(value: str, *, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise CommandError(f"{label} debe tener formato YYYY-MM-DD.") from exc


class Command(BaseCommand):
    help = "Ejecuta sincronizacion incremental de asistencias desde Point hacia RRHH."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=2, help="Dias a reprocesar en cada corrida.")
        parser.add_argument("--lag-days", type=int, default=0, help="Dias de desfase respecto a la fecha ancla.")
        parser.add_argument("--anchor-date", default="", help="Fecha ancla YYYY-MM-DD. Por default usa hoy local.")
        parser.add_argument("--branch", default="", help="Filtra por sucursal Point (id, plaza o nombre parcial).")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecucion.")

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        anchor_raw = (options.get("anchor_date") or "").strip()
        anchor_date = _parse_date(anchor_raw, label="anchor-date") if anchor_raw else None
        lookback_days = max(int(options.get("days") or 1), 1)
        lag_days = max(int(options.get("lag_days") or 0), 0)
        start_date, end_date = resolve_incremental_window(
            anchor_date=anchor_date,
            lookback_days=lookback_days,
            lag_days=lag_days,
        )
        sync_job = run_attendance_sync(
            triggered_by=actor,
            branch_filter=(options.get("branch") or "").strip() or None,
            lookback_days=lookback_days,
            lag_days=lag_days,
            anchor_date=anchor_date,
        )
        self.stdout.write(
            json.dumps(
                {
                    "window_start": start_date.isoformat(),
                    "window_end": end_date.isoformat(),
                    "job_id": sync_job.id,
                    "status": sync_job.status,
                    "summary": sync_job.result_summary,
                    "error_message": sync_job.error_message,
                    "artifacts": sync_job.artifacts,
                    "parameters": sync_job.parameters,
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
