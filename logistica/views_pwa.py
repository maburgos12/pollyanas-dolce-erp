from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render

from core.access import can_view_module, is_repartidor_only


@login_required
def pwa_app(request):
    if not (
        is_repartidor_only(request.user)
        or can_view_module(request.user, "logistica")
        or can_view_module(request.user, "mantenimiento")
    ):
        raise PermissionDenied("No tienes permisos para usar Logística")
    return render(request, "logistica/pwa.html")
