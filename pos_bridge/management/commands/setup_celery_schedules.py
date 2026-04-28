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
    help = "Registra los schedules periódicos operativos en django-celery-beat."

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

        monthly_closure_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="15",
            hour="5",
            day_of_week="*",
            day_of_month="2",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: cierre producto mensual",
            defaults={
                "task": "pos_bridge.monthly_product_closure",
                "crontab": monthly_closure_cron,
                "interval": None,
                "kwargs": json.dumps({"rebuild": False, "lock_after_build": False}),
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

        open_transfer_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="5",
            hour="22",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="pos_bridge: transferencias abiertas diario",
            defaults={
                "task": "pos_bridge.open_transfer_sync",
                "crontab": open_transfer_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        cedis_consolidado_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="30",
            hour="22",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="recetas: consolidado nocturno CEDIS",
            defaults={
                "task": "recetas.consolidado_nocturno_cedis",
                "crontab": cedis_consolidado_cron,
                "interval": None,
                "kwargs": json.dumps({"sincronizar_point": True}),
                "enabled": True,
            },
        )

        analytics_refresh_hour = _env_int("REPORTES_ANALYTICS_REFRESH_HOUR", 3, minimum=0, maximum=23)
        analytics_refresh_minute = _env_int("REPORTES_ANALYTICS_REFRESH_MINUTE", 35, minimum=0, maximum=59)
        analytics_lookback_days = _env_int("REPORTES_ANALYTICS_REFRESH_LOOKBACK_DAYS", 7, minimum=1, maximum=30)
        analytics_refresh_cron, _ = CrontabSchedule.objects.get_or_create(
            minute=str(analytics_refresh_minute),
            hour=str(analytics_refresh_hour),
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="reportes: refresh analytics operativo",
            defaults={
                "task": "reportes.operations_automation_cycle",
                "crontab": analytics_refresh_cron,
                "interval": None,
                "kwargs": json.dumps({"lookback_days": analytics_lookback_days}),
                "enabled": True,
            },
        )

        realtime_minutes = _env_int(
            "POS_BRIDGE_REALTIME_INTERVAL_MINUTES",
            10,
            minimum=1,
        )
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

        projection_daily_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="20",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="proyecciones: producción día siguiente",
            defaults={
                "task": "proyecciones.generar_proyeccion_dia_siguiente",
                "crontab": projection_daily_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        projection_weekly_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="21",
            day_of_week="0",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="proyecciones: producción semana siguiente",
            defaults={
                "task": "proyecciones.generar_proyeccion_semana_siguiente",
                "crontab": projection_weekly_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        forecast_quincenal_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="7",
            day_of_week="5",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="proyecciones: forecast quincenal semanal",
            defaults={
                "task": "proyecciones.generar_forecast_quincenal",
                "crontab": forecast_quincenal_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        bom_consumption_cron, _ = CrontabSchedule.objects.get_or_create(
            minute="30",
            hour="22",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="inventario: consumos BOM día anterior",
            defaults={
                "task": "inventario.generar_consumos_bom_dia_anterior",
                "crontab": bom_consumption_cron,
                "interval": None,
                "kwargs": json.dumps({}),
                "enabled": True,
            },
        )

        orchestration_daily_plan_hour = _env_int("ORQUESTACION_DAILY_PLAN_HOUR", 9, minimum=0, maximum=23)
        orchestration_daily_plan_minute = _env_int("ORQUESTACION_DAILY_PLAN_MINUTE", 5, minimum=0, maximum=59)
        orchestration_daily_plan_cron, _ = CrontabSchedule.objects.get_or_create(
            minute=str(orchestration_daily_plan_minute),
            hour=str(orchestration_daily_plan_hour),
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="orquestacion: plan diario faltante",
            defaults={
                "task": "orquestacion.run_rule",
                "crontab": orchestration_daily_plan_cron,
                "interval": None,
                "kwargs": json.dumps({"rule_code": "daily_production_plan_missing"}),
                "enabled": True,
            },
        )

        orchestration_chain_hour = _env_int("ORQUESTACION_PLAN_CHAIN_HOUR", 9, minimum=0, maximum=23)
        orchestration_chain_minute = _env_int("ORQUESTACION_PLAN_CHAIN_MINUTE", 20, minimum=0, maximum=59)
        orchestration_chain_cron, _ = CrontabSchedule.objects.get_or_create(
            minute=str(orchestration_chain_minute),
            hour=str(orchestration_chain_hour),
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
            timezone=timezone_name,
        )
        PeriodicTask.objects.update_or_create(
            name="orquestacion: cadena plan demanda-produccion-compras",
            defaults={
                "task": "orquestacion.run_rule",
                "crontab": orchestration_chain_cron,
                "interval": None,
                "kwargs": json.dumps({"rule_code": "plan_demand_production_purchase_chain"}),
                "enabled": True,
            },
        )

        purchase_exception_interval_hours = _env_int(
            "ORQUESTACION_PURCHASE_EXCEPTION_INTERVAL_HOURS",
            4,
            minimum=1,
        )
        purchase_exception_interval, _ = IntervalSchedule.objects.get_or_create(
            every=purchase_exception_interval_hours,
            period=IntervalSchedule.HOURS,
        )
        PeriodicTask.objects.update_or_create(
            name="orquestacion: excepciones compra DG",
            defaults={
                "task": "orquestacion.run_rule",
                "interval": purchase_exception_interval,
                "crontab": None,
                "kwargs": json.dumps({"rule_code": "purchase_exception_requires_dg_approval"}),
                "enabled": True,
            },
        )

        inventory_adjustment_interval_hours = _env_int(
            "ORQUESTACION_INVENTORY_GUARD_INTERVAL_HOURS",
            1,
            minimum=1,
        )
        inventory_adjustment_interval, _ = IntervalSchedule.objects.get_or_create(
            every=inventory_adjustment_interval_hours,
            period=IntervalSchedule.HOURS,
        )
        PeriodicTask.objects.update_or_create(
            name="orquestacion: guardia ajustes inventario",
            defaults={
                "task": "orquestacion.run_rule",
                "interval": inventory_adjustment_interval,
                "crontab": None,
                "kwargs": json.dumps({"rule_code": "inventory_adjustment_authorization_guard"}),
                "enabled": True,
            },
        )

        self.stdout.write(self.style.SUCCESS("Schedules operativos registrados en django-celery-beat."))
