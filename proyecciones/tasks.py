from __future__ import annotations

from datetime import timedelta

from celery import shared_task
from django.utils import timezone

from .services import ProyeccionProduccionService


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
