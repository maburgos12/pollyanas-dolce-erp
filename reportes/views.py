from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from core.access import can_view_reportes


@login_required
def costo_receta(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    return render(request, "reportes/costo_receta.html")


@login_required
def consumo(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    return render(request, "reportes/consumo.html")


@login_required
def faltantes(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    return render(request, "reportes/faltantes.html")
