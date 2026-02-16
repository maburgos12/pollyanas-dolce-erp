from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


@login_required
def solicitudes(request: HttpRequest) -> HttpResponse:
    return render(request, "compras/solicitudes.html")


@login_required
def ordenes(request: HttpRequest) -> HttpResponse:
    return render(request, "compras/ordenes.html")


@login_required
def recepciones(request: HttpRequest) -> HttpResponse:
    return render(request, "compras/recepciones.html")
