from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from celery import shared_task
from django.db.models import Sum
from django.utils import timezone

from recetas.models import Receta
from ventas.models import VentaAutoritativaPoint

from .models import ForecastQuincenalRun, ProyeccionProduccion
from .services import ProyeccionProduccionService

ANOMALY_REDUCTION_FACTOR = Decimal("0.80")


@shared_task(name="proyecciones.generar_proyeccion_dia_siguiente")
def generar_proyeccion_dia_siguiente():
    target_date = timezone.localdate() + timedelta(days=1)
    summary = ProyeccionProduccionService().proyectar_dia(target_date, dry_run=False)
    return summary.as_dict()


@shared_task(name="proyecciones.generar_proyeccion_semana_siguiente")
def generar_proyeccion_semana_siguiente():
    today = timezone.localdate()
    next_monday = today + timedelta(days=(7 - today.weekday()))
    summary = ProyeccionProduccionService().proyectar_semana(next_monday, dry_run=False)
    return summary.as_dict()


@shared_task(name="proyecciones.generar_forecast_quincenal")
def generar_forecast_quincenal(fecha_inicio: str | None = None) -> dict:
    start_date = date.fromisoformat(fecha_inicio) if fecha_inicio else timezone.localdate()
    end_date = start_date + timedelta(days=14)
    anomalous_recipe_ids = _detectar_recetas_anomalas(start_date)
    service = ProyeccionProduccionService()
    total_created = 0
    total_updated = 0
    total_skipped = 0
    total_rows = 0
    target_dates = []

    for offset in range(15):
        target_date = start_date + timedelta(days=offset)
        summary = service.proyectar_dia(target_date, dry_run=False)
        total_created += summary.created
        total_updated += summary.updated
        total_skipped += summary.skipped
        total_rows += len(summary.rows)
        target_dates.extend(summary.target_dates)

    adjusted_rows = _aplicar_factor_anomalia(
        fecha_inicio=start_date,
        fecha_fin=end_date,
        receta_ids=anomalous_recipe_ids,
    )
    recetas_proyectadas = (
        ProyeccionProduccion.objects.filter(periodo__gte=start_date, periodo__lte=end_date)
        .values("receta_id")
        .distinct()
        .count()
    )
    run = ForecastQuincenalRun.objects.create(
        fecha_inicio=start_date,
        fecha_fin=end_date,
        estado=ForecastQuincenalRun.ESTADO_LISTO_REVISION,
        recetas_proyectadas=recetas_proyectadas,
        registros_generados=total_rows,
        recetas_anomalas=len(anomalous_recipe_ids),
        metadata={
            "created": total_created,
            "updated": total_updated,
            "skipped": total_skipped,
            "target_dates": [item.isoformat() for item in sorted(set(target_dates))],
            "anomalous_recipe_ids": sorted(anomalous_recipe_ids),
            "anomaly_reduction_factor": str(ANOMALY_REDUCTION_FACTOR),
            "adjusted_rows": adjusted_rows,
        },
    )
    return {
        "run_id": run.id,
        "fecha_inicio": run.fecha_inicio.isoformat(),
        "fecha_fin": run.fecha_fin.isoformat(),
        "estado": run.estado,
        "recetas_proyectadas": run.recetas_proyectadas,
        "registros_generados": run.registros_generados,
        "recetas_anomalas": run.recetas_anomalas,
        "created": total_created,
        "updated": total_updated,
        "skipped": total_skipped,
    }


def _detectar_recetas_anomalas(reference_date: date) -> set[int]:
    recipes = Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).exclude(
        modo_costeo=Receta.MODO_COSTEO_SERVICIO
    ).exclude(excluir_cierre=True)
    anomalous_ids = set()
    for receta in recipes.only("id"):
        last_week = (
            VentaAutoritativaPoint.objects.filter(
                product=receta,
                sale_date__gte=reference_date - timedelta(days=7),
                sale_date__lt=reference_date,
            ).aggregate(total=Sum("quantity"))["total"]
            or Decimal("0")
        )
        previous_weeks = (
            VentaAutoritativaPoint.objects.filter(
                product=receta,
                sale_date__gte=reference_date - timedelta(days=28),
                sale_date__lt=reference_date - timedelta(days=7),
            ).aggregate(total=Sum("quantity"))["total"]
            or Decimal("0")
        )
        if previous_weeks <= 0:
            continue
        previous_week_avg = Decimal(str(previous_weeks)) / Decimal("3")
        if Decimal(str(last_week)) > previous_week_avg * Decimal("2"):
            anomalous_ids.add(receta.id)
    return anomalous_ids


def _aplicar_factor_anomalia(*, fecha_inicio: date, fecha_fin: date, receta_ids: set[int]) -> int:
    if not receta_ids:
        return 0
    rows = ProyeccionProduccion.objects.filter(
        periodo__gte=fecha_inicio,
        periodo__lte=fecha_fin,
        receta_id__in=receta_ids,
    )
    updated = 0
    for projection in rows:
        projection.unidades_proyectadas_ajustadas = (
            Decimal(str(projection.unidades_proyectadas_ajustadas or 0)) * ANOMALY_REDUCTION_FACTOR
        ).quantize(Decimal("0.001"))
        projection.metodo = "FORECAST_QUINCENAL_AUTO"
        projection.metadata = {
            **(projection.metadata or {}),
            "forecast_quincenal": True,
            "anomaly_adjusted": True,
            "anomaly_reduction_factor": str(ANOMALY_REDUCTION_FACTOR),
        }
        projection.save(update_fields=["unidades_proyectadas_ajustadas", "metodo", "metadata", "actualizado_en"])
        updated += 1
    return updated


def register_forecast_quincenal_schedule() -> None:
    from django_celery_beat.models import CrontabSchedule, PeriodicTask

    schedule, _ = CrontabSchedule.objects.get_or_create(
        minute="40",
        hour="4",
        day_of_week="1",
        day_of_month="*",
        month_of_year="*",
        timezone="America/Mazatlan",
    )
    PeriodicTask.objects.update_or_create(
        name="proyecciones: forecast quincenal semanal",
        defaults={
            "task": "proyecciones.generar_forecast_quincenal",
            "crontab": schedule,
            "enabled": True,
        },
    )
