from __future__ import annotations

import os

from django.core.management import call_command
from django.test import TestCase, override_settings


@override_settings(TIME_ZONE="America/Mazatlan")
class SetupCelerySchedulesCommandTests(TestCase):
    def test_registers_expected_periodic_tasks_idempotently(self):
        from django_celery_beat.models import PeriodicTask

        original = os.environ.get("POS_BRIDGE_REALTIME_INTERVAL_MINUTES")
        os.environ["POS_BRIDGE_REALTIME_INTERVAL_MINUTES"] = "5"
        try:
            call_command("setup_celery_schedules")
            call_command("setup_celery_schedules")
        finally:
            if original is None:
                os.environ.pop("POS_BRIDGE_REALTIME_INTERVAL_MINUTES", None)
            else:
                os.environ["POS_BRIDGE_REALTIME_INTERVAL_MINUTES"] = original

        task_names = set(PeriodicTask.objects.values_list("name", flat=True))
        self.assertEqual(
            task_names,
            {
                "pos_bridge: ventas cerradas diario",
                "sat: descarga cfdi nocturna",
                "pos_bridge: ventas intradia actual",
                "pos_bridge: asistencias Point intradia",
                "rentabilidad: recalculo intradia periodo actual",
                "pos_bridge: cierre producto mensual",
                "pos_bridge: inventario completo diario",
                "pos_bridge: costos reventa desde compras Point",
                "pos_bridge: inventario cierre diario",
                "pos_bridge: mermas diario",
                "pos_bridge: produccion diario",
                "pos_bridge: transferencias diario",
                "pos_bridge: transferencias abiertas diario",
                "pos_bridge: inventario realtime",
                "pos_bridge: recetas semanal",
                "pos_bridge: retry jobs fallidos",
                "pos_bridge: auditoria recetas semanal",
                "pos_bridge: snapshot semanal costeo",
                "pos_bridge: sync precios catalogo semanal",
                "proyecciones: producción día siguiente",
                "proyecciones: producción semana siguiente",
                "proyecciones: forecast quincenal semanal",
                "inventario: consumos BOM día anterior",
                "core: verificar datos mes anterior",
                "core: cierre automático mes anterior",
                "recetas: consolidado nocturno CEDIS",
                "recetas: inventario final cierre email",
                "reportes: refresh analytics operativo",
                "reportes: refresh snapshots inversion",
                "erp-doctor: reporte diario",
                "ventas: sync ventas autoritativas mensual",
                "reportes: snapshot operacion dg",
                "reportes: enviar reporte diario",
                "logistica: detectar GPS perdido rutas activas",
                "orquestacion: plan diario faltante",
                "orquestacion: cadena plan demanda-produccion-compras",
                "orquestacion: excepciones compra DG",
                "orquestacion: guardia ajustes inventario",
                "bonos: reconciliar asistencia periodo actual",
                "reportes: consolidar presupuesto real nocturno",
            },
        )
        self.assertEqual(PeriodicTask.objects.count(), 40)
        reporte_diario = PeriodicTask.objects.get(name="reportes: enviar reporte diario")
        self.assertEqual(reporte_diario.task, "reportes.enviar_reporte_diario")
        self.assertEqual(reporte_diario.crontab.hour, "4")
        self.assertEqual(reporte_diario.crontab.minute, "0")
        sat_download = PeriodicTask.objects.get(name="sat: descarga cfdi nocturna")
        self.assertEqual(sat_download.task, "sat_client.ejecutar_descarga_sat_nocturna")
        self.assertEqual(sat_download.crontab.hour, "1")
        self.assertEqual(sat_download.crontab.minute, "0")
        self.assertFalse(sat_download.enabled)
        self.assertFalse(
            PeriodicTask.objects.filter(name="syncfy: sincronizacion bancaria nocturna").exists()
        )
        intraday_sales = PeriodicTask.objects.get(name="pos_bridge: ventas intradia actual")
        self.assertEqual(intraday_sales.task, "pos_bridge.daily_sales_sync")
        self.assertEqual(intraday_sales.crontab.hour, "8-22")
        self.assertEqual(intraday_sales.crontab.minute, "0")
        self.assertEqual(intraday_sales.kwargs, '{"days": 1, "lag_days": 0}')
        intraday_profitability = PeriodicTask.objects.get(name="rentabilidad: recalculo intradia periodo actual")
        self.assertEqual(
            intraday_profitability.task,
            "rentabilidad.tasks_rentabilidad.recalcular_rentabilidad_periodo_actual",
        )
        self.assertEqual(intraday_profitability.crontab.hour, "8-22")
        self.assertEqual(intraday_profitability.crontab.minute, "10")
        realtime = PeriodicTask.objects.get(name="pos_bridge: inventario realtime")
        self.assertEqual(realtime.interval.every, 5)
        gps_perdido = PeriodicTask.objects.get(name="logistica: detectar GPS perdido rutas activas")
        self.assertEqual(gps_perdido.task, "logistica.tasks.detectar_gps_perdido_rutas")
        self.assertEqual(gps_perdido.interval.every, 5)
        self.assertEqual(gps_perdido.interval.period, "minutes")
        self.assertEqual(gps_perdido.kwargs, '{"umbral_minutos": 10}')
        inventory_close = PeriodicTask.objects.get(name="pos_bridge: inventario cierre diario")
        self.assertEqual(inventory_close.task, "pos_bridge.inventory_sync")
        self.assertEqual(inventory_close.crontab.hour, "23")
        self.assertEqual(inventory_close.crontab.minute, "0")
        self.assertEqual(inventory_close.kwargs, '{"capture_costs": false}')
        close_email = PeriodicTask.objects.get(name="recetas: inventario final cierre email")
        self.assertEqual(close_email.task, "recetas.inventario_final_cierre_email")
        self.assertEqual(close_email.crontab.hour, "1")
        self.assertEqual(close_email.crontab.minute, "0")
        monthly = PeriodicTask.objects.get(name="pos_bridge: cierre producto mensual")
        self.assertEqual(monthly.crontab.day_of_month, "1")
        self.assertEqual(monthly.crontab.hour, "5")
        product_prices = PeriodicTask.objects.get(name="pos_bridge: sync precios catalogo semanal")
        self.assertEqual(product_prices.task, "pos_bridge.sync_product_prices_task")
        self.assertEqual(product_prices.crontab.day_of_week, "1")
        self.assertEqual(product_prices.crontab.hour, "2")
        self.assertEqual(product_prices.crontab.minute, "0")
        analytics_refresh = PeriodicTask.objects.get(name="reportes: refresh analytics operativo")
        self.assertEqual(analytics_refresh.crontab.hour, "3")
        self.assertEqual(analytics_refresh.crontab.minute, "35")
        investment_refresh = PeriodicTask.objects.get(name="reportes: refresh snapshots inversion")
        self.assertEqual(investment_refresh.task, "reportes.refresh_investment_snapshots")
        self.assertEqual(investment_refresh.crontab.hour, "3")
        self.assertEqual(investment_refresh.crontab.minute, "50")
        erp_doctor = PeriodicTask.objects.get(name="erp-doctor: reporte diario")
        self.assertEqual(erp_doctor.task, "reportes.erp_doctor_daily_report")
        self.assertEqual(erp_doctor.crontab.hour, "6")
        self.assertEqual(erp_doctor.crontab.minute, "0")
        authoritative_sales = PeriodicTask.objects.get(name="ventas: sync ventas autoritativas mensual")
        self.assertEqual(authoritative_sales.task, "ventas.sync_ventas_autoritativas")
        self.assertEqual(authoritative_sales.crontab.day_of_month, "2")
        self.assertEqual(authoritative_sales.crontab.hour, "3")
        self.assertEqual(authoritative_sales.crontab.minute, "0")
        orchestration_daily_plan = PeriodicTask.objects.get(name="orquestacion: plan diario faltante")
        self.assertEqual(orchestration_daily_plan.crontab.hour, "9")
        self.assertEqual(orchestration_daily_plan.crontab.minute, "5")
        orchestration_chain = PeriodicTask.objects.get(name="orquestacion: cadena plan demanda-produccion-compras")
        self.assertEqual(orchestration_chain.crontab.hour, "9")
        self.assertEqual(orchestration_chain.crontab.minute, "20")
        purchase_exception = PeriodicTask.objects.get(name="orquestacion: excepciones compra DG")
        self.assertEqual(purchase_exception.interval.every, 4)
        self.assertEqual(purchase_exception.interval.period, "hours")
        inventory_guard = PeriodicTask.objects.get(name="orquestacion: guardia ajustes inventario")
        self.assertEqual(inventory_guard.interval.every, 1)
        self.assertEqual(inventory_guard.interval.period, "hours")
        projection_daily = PeriodicTask.objects.get(name="proyecciones: producción día siguiente")
        self.assertEqual(projection_daily.crontab.hour, "20")
        self.assertEqual(projection_daily.crontab.minute, "0")
        projection_weekly = PeriodicTask.objects.get(name="proyecciones: producción semana siguiente")
        self.assertEqual(projection_weekly.crontab.hour, "21")
        self.assertEqual(projection_weekly.crontab.minute, "0")
        self.assertEqual(projection_weekly.crontab.day_of_week, "0")
        bom_consumption = PeriodicTask.objects.get(name="inventario: consumos BOM día anterior")
        self.assertEqual(bom_consumption.crontab.hour, "22")
        self.assertEqual(bom_consumption.crontab.minute, "30")
        data_check = PeriodicTask.objects.get(name="core: verificar datos mes anterior")
        self.assertEqual(data_check.task, "core.tasks.verificar_datos_mes")
        self.assertEqual(data_check.crontab.day_of_month, "3")
        self.assertEqual(data_check.crontab.hour, "6")
        self.assertEqual(data_check.crontab.minute, "0")
        auto_close = PeriodicTask.objects.get(name="core: cierre automático mes anterior")
        self.assertEqual(auto_close.task, "core.tasks.cerrar_mes_anterior")
        self.assertEqual(auto_close.crontab.day_of_month, "5")
        self.assertEqual(auto_close.crontab.hour, "6")
        self.assertEqual(auto_close.crontab.minute, "0")

    def test_respects_orchestration_schedule_overrides(self):
        from django_celery_beat.models import PeriodicTask

        original_daily_hour = os.environ.get("ORQUESTACION_DAILY_PLAN_HOUR")
        original_daily_minute = os.environ.get("ORQUESTACION_DAILY_PLAN_MINUTE")
        original_chain_hour = os.environ.get("ORQUESTACION_PLAN_CHAIN_HOUR")
        original_chain_minute = os.environ.get("ORQUESTACION_PLAN_CHAIN_MINUTE")
        original_purchase_hours = os.environ.get("ORQUESTACION_PURCHASE_EXCEPTION_INTERVAL_HOURS")
        original_inventory_hours = os.environ.get("ORQUESTACION_INVENTORY_GUARD_INTERVAL_HOURS")
        original_analytics_hour = os.environ.get("REPORTES_ANALYTICS_REFRESH_HOUR")
        original_analytics_minute = os.environ.get("REPORTES_ANALYTICS_REFRESH_MINUTE")
        original_analytics_lookback = os.environ.get("REPORTES_ANALYTICS_REFRESH_LOOKBACK_DAYS")
        os.environ["ORQUESTACION_DAILY_PLAN_HOUR"] = "10"
        os.environ["ORQUESTACION_DAILY_PLAN_MINUTE"] = "20"
        os.environ["ORQUESTACION_PLAN_CHAIN_HOUR"] = "10"
        os.environ["ORQUESTACION_PLAN_CHAIN_MINUTE"] = "35"
        os.environ["ORQUESTACION_PURCHASE_EXCEPTION_INTERVAL_HOURS"] = "6"
        os.environ["ORQUESTACION_INVENTORY_GUARD_INTERVAL_HOURS"] = "2"
        os.environ["REPORTES_ANALYTICS_REFRESH_HOUR"] = "4"
        os.environ["REPORTES_ANALYTICS_REFRESH_MINUTE"] = "10"
        os.environ["REPORTES_ANALYTICS_REFRESH_LOOKBACK_DAYS"] = "9"
        try:
            call_command("setup_celery_schedules")
        finally:
            for name, original_value in (
                ("ORQUESTACION_DAILY_PLAN_HOUR", original_daily_hour),
                ("ORQUESTACION_DAILY_PLAN_MINUTE", original_daily_minute),
                ("ORQUESTACION_PLAN_CHAIN_HOUR", original_chain_hour),
                ("ORQUESTACION_PLAN_CHAIN_MINUTE", original_chain_minute),
                ("ORQUESTACION_PURCHASE_EXCEPTION_INTERVAL_HOURS", original_purchase_hours),
                ("ORQUESTACION_INVENTORY_GUARD_INTERVAL_HOURS", original_inventory_hours),
                ("REPORTES_ANALYTICS_REFRESH_HOUR", original_analytics_hour),
                ("REPORTES_ANALYTICS_REFRESH_MINUTE", original_analytics_minute),
                ("REPORTES_ANALYTICS_REFRESH_LOOKBACK_DAYS", original_analytics_lookback),
            ):
                if original_value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = original_value

        orchestration_daily_plan = PeriodicTask.objects.get(name="orquestacion: plan diario faltante")
        self.assertEqual(orchestration_daily_plan.crontab.hour, "10")
        self.assertEqual(orchestration_daily_plan.crontab.minute, "20")
        orchestration_chain = PeriodicTask.objects.get(name="orquestacion: cadena plan demanda-produccion-compras")
        self.assertEqual(orchestration_chain.crontab.hour, "10")
        self.assertEqual(orchestration_chain.crontab.minute, "35")
        purchase_exception = PeriodicTask.objects.get(name="orquestacion: excepciones compra DG")
        self.assertEqual(purchase_exception.interval.every, 6)
        inventory_guard = PeriodicTask.objects.get(name="orquestacion: guardia ajustes inventario")
        self.assertEqual(inventory_guard.interval.every, 2)
        analytics_refresh = PeriodicTask.objects.get(name="reportes: refresh analytics operativo")
        self.assertEqual(analytics_refresh.crontab.hour, "4")
        self.assertEqual(analytics_refresh.crontab.minute, "10")
        self.assertEqual(analytics_refresh.kwargs, '{"lookback_days": 9}')
        investment_refresh = PeriodicTask.objects.get(name="reportes: refresh snapshots inversion")
        self.assertEqual(investment_refresh.crontab.hour, "4")
        self.assertEqual(investment_refresh.crontab.minute, "25")
