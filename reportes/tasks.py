from __future__ import annotations

from datetime import date, timedelta

from celery import shared_task

from reportes.analytics_service import rebuild_production_facts


@shared_task(name="reportes.snapshot_historical_costing_task")
def snapshot_historical_costing_task():
    """Congela costo historico del mes anterior al dia 1 de cada mes."""
    from reportes.services_historical_costing import MonthlyHistoricalCostingService

    mes_actual = date.today().replace(day=1)
    periodo = (mes_actual - timedelta(days=1)).replace(day=1)
    summary = MonthlyHistoricalCostingService().build_period(period_start=periodo)
    return {
        "period": f"{periodo:%Y-%m}",
        "insumo_rows": summary.insumo_rows,
        "receta_rows": summary.receta_rows,
        "missing_recipe_rows": summary.missing_recipe_rows,
        "producto_reventa_rows": summary.producto_reventa_rows,
    }


@shared_task(name="reportes.cierre_produccion_nocturno", bind=True, max_retries=1, default_retry_delay=300)
def task_cierre_produccion_nocturno(self):
    """
    Reconstruye FactProduccionDiaria para los ultimos 3 dias.
    Corre despues del sync de ventas para usar datos frescos de Point.
    """
    today = date.today()
    results = []
    errors = []
    for delta in range(3):
        target = today - timedelta(days=delta)
        try:
            rows = rebuild_production_facts(start_date=target, end_date=target)
            results.append({"date": target.isoformat(), "rows": rows})
        except Exception as exc:  # noqa: BLE001
            errors.append({"date": target.isoformat(), "error": str(exc)})
    return {"rebuilt_dates": len(results), "results": results, "errors": errors}


@shared_task(name="reportes.alerta_produccion_sin_registros", bind=True, max_retries=1)
def task_alerta_produccion_sin_registros(self):
    """
    Revisa si Point registro produccion en el dia habil anterior.
    Si no hay registros, envia email de alerta a Direccion General.
    """
    from django.conf import settings
    from django.core.mail import send_mail
    from django.utils import timezone
    from pos_bridge.models import PointProductionLine

    today = timezone.localdate()
    target = today - timedelta(days=1)
    if target.weekday() == 6:  # domingo
        target = today - timedelta(days=2)

    count = PointProductionLine.objects.filter(
        production_date=target,
        is_insumo=False,
    ).count()

    if count > 0:
        return {
            "status": "ok",
            "date": target.isoformat(),
            "registros": count,
        }

    subject = f"Sin produccion registrada en Point - {target:%d/%b/%Y}"
    body = (
        "Alerta automatica del ERP Pollyana's Dolce.\n\n"
        f"Point no tiene registros de produccion para el {target:%d/%m/%Y}.\n\n"
        "Posibles causas:\n"
        "  - Produccion Crucero no capturo en Point\n"
        "  - El sync automatico del ERP no corrio\n"
        "  - No hubo produccion ese dia\n\n"
        "Verificar en: https://erp.pollyanasdolce.com/reportes/produccion/\n\n"
        "-- ERP Pollyana's Dolce"
    )

    try:
        send_mail(
            subject=subject,
            message=body,
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "erp@pollyanasdolce.com"),
            recipient_list=["mauricio@pollyanasdolce.com"],
            fail_silently=False,
        )
    except Exception as exc:
        raise self.retry(exc=exc)

    return {
        "status": "alerta_enviada",
        "date": target.isoformat(),
        "registros": 0,
    }
