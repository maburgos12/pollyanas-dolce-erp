from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render

from core.models import Sucursal


@login_required
def asignacion_sucursal_view(request):
    return render(request, "rrhh/asignacion_sucursal.html")


@login_required
def asignacion_sucursales_api(request):
    rows = list(
        Sucursal.objects.filter(activa=True)
        .exclude(nombre__in=["Matriz", "CEDIS", "Devoluciones", "Almacén"])
        .order_by("nombre")
        .values("id", "nombre", "activa")
    )
    return JsonResponse({"count": len(rows), "results": rows})
