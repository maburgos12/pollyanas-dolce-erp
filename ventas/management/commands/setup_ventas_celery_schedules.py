from __future__ import annotations

import json
import os

from django.conf import settings
from django.core.management.base import BaseCommand


def _env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw if raw not in (None, "") else default)
    except (TypeError, ValueError):
        value = int(default)
    if minimum is not None:
        value = max(value, minimum)
    if maximum is not None:
        value = min(value, maximum)
    return value


class Command(BaseCommand):
    help = "Registra schedules periódicos del módulo de ventas estacionales en django-celery-beat."

    def handle(self, *args, **options):
        from django_celery_beat.models import CrontabSchedule, IntervalSchedule, PeriodicTask

        timezone_name = getattr(settings, "TIME_ZONE", "America/Phoenix")

        monitor_hours = _env_int("VENTAS_EVENTOS_MONITOR_INTERVAL_HOURS", 4, minimum=1, maximum=24)
        monitor_interval, _ = IntervalSchedule.objects.get_or_create(
            every=monitor_hours,
            period=IntervalSchedule.HOURS,
        )
        PeriodicTask.objects.update_or_create(
            name="ventas: monitoreo eventos activos",
            defaults={
                "task": "ventas.monitor_active_events",
                "interval": monitor_interval,
                "crontab": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        close_hour = _env_int("VENTAS_EVENTOS_POSTMORTEM_HOUR", 22, minimum=0, maximum=23)
        close_minute = _env_int("VENTAS_EVENTOS_POSTMORTEM_MINUTE", 30, minimum=0, maximum=59)
        close_cron, _ = CrontabSchedule.objects.get_or_create(
            minute=str(close_minute),
            hour=str(close_hour),
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="ventas: monitoreo cierre postmortem",
            defaults={
                "task": "ventas.monitor_active_events",
                "crontab": close_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        self.stdout.write(self.style.SUCCESS("Schedules de ventas estacionales registrados correctamente."))
