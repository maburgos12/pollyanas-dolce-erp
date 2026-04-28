from __future__ import annotations

from datetime import date, timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render
from django.views.decorators.http import require_POST

from core.access import can_view_recetas
from proyecciones.models import ForecastQuincenalRun, ProyeccionProduccion
from proyecciones.tasks import generar_forecast_quincenal


def _parse_date(value: str | None):
    if not value:
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError:
        return date.today()


@login_required
def forecast_quincenal_revision(request):
    if not can_view_recetas(request.user):
        raise PermissionDenied
    fecha_inicio = _parse_date(request.GET.get("fecha_inicio"))
    fecha_fin = fecha_inicio + timedelta(days=14)
    latest_run = ForecastQuincenalRun.objects.filter(fecha_inicio=fecha_inicio, fecha_fin=fecha_fin).first()
    rows = (
        ProyeccionProduccion.objects.filter(periodo__gte=fecha_inicio, periodo__lte=fecha_fin)
        .select_related("receta", "sucursal")
        .order_by("periodo", "sucursal__codigo", "receta__nombre")[:600]
    )
    return render(
        request,
        "proyecciones/forecast_quincenal_revision.html",
        {
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "latest_run": latest_run,
            "rows": rows,
        },
    )


@login_required
@require_POST
def forecast_quincenal_generar(request):
    if not can_view_recetas(request.user):
        raise PermissionDenied
    fecha_inicio = _parse_date(request.POST.get("fecha_inicio"))
    result = generar_forecast_quincenal.run(fecha_inicio=fecha_inicio.isoformat())
    messages.success(request, f"Forecast quincenal generado. Run #{result.get('run_id')}.")
    return redirect(f"/proyecciones/forecast-quincenal/?fecha_inicio={fecha_inicio.isoformat()}")
