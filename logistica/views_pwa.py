from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.core.exceptions import PermissionDenied
from django.http import Http404, HttpResponse
from django.shortcuts import render
from django.views.decorators.cache import never_cache

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


@never_cache
def pwa_sw(request):
    path = finders.find("logistica/pwa/sw.js")
    if not path:
        raise Http404("Service worker de Logística no encontrado")
    with open(path, encoding="utf-8") as service_worker:
        return HttpResponse(service_worker.read(), content_type="application/javascript")
