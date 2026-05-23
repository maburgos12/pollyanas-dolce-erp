from __future__ import annotations

import json
import os

from django.core.management.base import BaseCommand, CommandError


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw if raw not in (None, "") else default)
    except (TypeError, ValueError):
        value = int(default)
    if minimum is not None:
        value = max(value, minimum)
    return value


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _has_agente_dg_database_url() -> bool:
    return bool(os.getenv("AGENTE_DG_SYNC_DATABASE_URL") or os.getenv("AGENTE_DG_DATABASE_URL"))


class Command(BaseCommand):
    help = "Registra el schedule periódico de sincronización Agente DG -> Seguimiento ERP."

    def add_arguments(self, parser):
        parser.add_argument("--interval-minutes", type=int, default=None)
        parser.add_argument("--limit", type=int, default=None)
        parser.add_argument("--enabled", action="store_true", help="Fuerza el schedule como activo.")
        parser.add_argument("--disabled", action="store_true", help="Fuerza el schedule como pausado.")

    def handle(self, *args, **options):
        from django_celery_beat.models import IntervalSchedule, PeriodicTask

        if options["enabled"] and options["disabled"]:
            raise CommandError("Usa solo --enabled o --disabled, no ambos.")

        interval_minutes = options["interval_minutes"]
        if interval_minutes is None:
            interval_minutes = _env_int("AGENTE_DG_SYNC_INTERVAL_MINUTES", 30, minimum=5)
        interval_minutes = max(int(interval_minutes or 30), 5)
        limit = options["limit"]
        if limit is None:
            limit = _env_int("AGENTE_DG_SYNC_LIMIT", 0, minimum=0)
        limit = max(int(limit or 0), 0)
        if options["enabled"]:
            enabled = True
        elif options["disabled"]:
            enabled = False
        else:
            enabled = _has_agente_dg_database_url() and _env_flag("AGENTE_DG_SYNC_ENABLED", True)

        interval, _ = IntervalSchedule.objects.get_or_create(
            every=interval_minutes,
            period=IntervalSchedule.MINUTES,
        )
        task, _ = PeriodicTask.objects.update_or_create(
            name="seguimiento: importar Agente DG",
            defaults={
                "task": "seguimiento.importar_agente_dg",
                "interval": interval,
                "crontab": None,
                "kwargs": json.dumps({"limit": limit}),
                "enabled": enabled,
            },
        )

        state = "activo" if task.enabled else "pausado"
        reason = "" if _has_agente_dg_database_url() else " Falta AGENTE_DG_SYNC_DATABASE_URL/AGENTE_DG_DATABASE_URL."
        self.stdout.write(
            self.style.SUCCESS(
                f"Schedule seguimiento: importar Agente DG {state}; intervalo={interval_minutes} min; limit={limit}.{reason}"
            )
        )
