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
    help = "Registra schedules periódicos del módulo de ventas en django-celery-beat."

    def handle(self, *args, **options):
        from django_celery_beat.models import CrontabSchedule, IntervalSchedule, PeriodicTask

        timezone_name = getattr(settings, "TIME_ZONE", "America/Phoenix")

        authoritative_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="3",
            day_of_week="*",
            day_of_month="2",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="ventas: sync ventas autoritativas mensual",
            defaults={
                "task": "ventas.sync_ventas_autoritativas",
                "crontab": authoritative_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        self.stdout.write(self.style.SUCCESS("Schedules de ventas registrados correctamente."))
