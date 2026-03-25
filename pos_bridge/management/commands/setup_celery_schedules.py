from __future__ import annotations

import json
import os

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Registra los schedules periódicos de pos_bridge en django-celery-beat."

    def handle(self, *args, **options):
        from django_celery_beat.models import CrontabSchedule, IntervalSchedule, PeriodicTask

        timezone_name = getattr(settings, "TIME_ZONE", "America/Mazatlan")

        sales_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="30",
            hour="1",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: ventas cerradas diario",
            defaults={
                "task": "pos_bridge.daily_sales_sync",
                "crontab": sales_cron,
                "interval": None,
                "kwargs": json.dumps({"days": 3, "lag_days": 1}),
                "enabled": True,
            },
        )

        inventory_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="15",
            hour="2",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: inventario completo diario",
            defaults={
                "task": "pos_bridge.inventory_sync",
                "crontab": inventory_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        waste_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="45",
            hour="2",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: mermas diario",
            defaults={
                "task": "pos_bridge.waste_sync",
                "crontab": waste_cron,
                "interval": None,
                "kwargs": json.dumps({"days": 1, "lag_days": 1}),
                "enabled": True,
            },
        )

        production_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="3",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: produccion diario",
            defaults={
                "task": "pos_bridge.production_sync",
                "crontab": production_cron,
                "interval": None,
                "kwargs": json.dumps({"days": 1, "lag_days": 1}),
                "enabled": True,
            },
        )

        transfer_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="15",
            hour="3",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: transferencias diario",
            defaults={
                "task": "pos_bridge.transfer_sync",
                "crontab": transfer_cron,
                "interval": None,
                "kwargs": json.dumps({"days": 1, "lag_days": 1}),
                "enabled": True,
            },
        )

        realtime_minutes = max(int(os.getenv("POS_BRIDGE_REALTIME_INTERVAL_MINUTES", "10") or "10"), 10)
        realtime_interval, _ = IntervalSchedule.objects.get_or_create(
            every=realtime_minutes,
            period=IntervalSchedule.MINUTES,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: inventario realtime",
            defaults={
                "task": "pos_bridge.realtime_inventory_sync",
                "interval": realtime_interval,
                "crontab": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        recipes_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="3",
            day_of_week="0",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: recetas semanal",
            defaults={
                "task": "pos_bridge.product_recipe_sync",
                "crontab": recipes_cron,
                "interval": None,
                "kwargs": json.dumps({"branch_hint": "MATRIZ"}),
                "enabled": True,
            },
        )

        retry_interval, _ = IntervalSchedule.objects.get_or_create(
            every=6,
            period=IntervalSchedule.HOURS,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: retry jobs fallidos",
            defaults={
                "task": "pos_bridge.retry_failed_jobs",
                "interval": retry_interval,
                "crontab": None,
                "kwargs": json.dumps({"limit": 3}),
                "enabled": True,
            },
        )

        audit_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="4",
            day_of_week="1",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: auditoria recetas semanal",
            defaults={
                "task": "pos_bridge.recipe_gap_audit",
                "crontab": audit_cron,
                "interval": None,
                "kwargs": json.dumps({"branch_hint": "MATRIZ"}),
                "enabled": True,
            },
        )

        weekly_cost_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="30",
            hour="4",
            day_of_week="1",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: snapshot semanal costeo",
            defaults={
                "task": "pos_bridge.weekly_cost_snapshot",
                "crontab": weekly_cost_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        self.stdout.write(self.style.SUCCESS("Schedules de pos_bridge registrados en django-celery-beat."))
