from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


@login_required
def costo_receta(request: HttpRequest) -> HttpResponse:
    return render(request, "reportes/costo_receta.html")


@login_required
def consumo(request: HttpRequest) -> HttpResponse:
    return render(request, "reportes/consumo.html")


@login_required
def faltantes(request: HttpRequest) -> HttpResponse:
    return render(request, "reportes/faltantes.html")
