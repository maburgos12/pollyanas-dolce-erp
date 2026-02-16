from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


@login_required
def existencias(request: HttpRequest) -> HttpResponse:
    return render(request, "inventario/existencias.html")


@login_required
def movimientos(request: HttpRequest) -> HttpResponse:
    return render(request, "inventario/movimientos.html")


@login_required
def ajustes(request: HttpRequest) -> HttpResponse:
    return render(request, "inventario/ajustes.html")
