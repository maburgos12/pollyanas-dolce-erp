from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie

import json


@login_required
@ensure_csrf_cookie
def bonos_ventas_pwa(request):
    return render(request, "bonos_ventas/index.html")


def _static_file_path(relative_path: str) -> str:
    path = finders.find(relative_path)
    if not path:
        raise Http404(f"Static file not found: {relative_path}")
    return path


@never_cache
def bonos_ventas_manifest(request):
    with open(_static_file_path("bonos_ventas/manifest.json"), encoding="utf-8") as manifest:
        return JsonResponse(json.load(manifest), content_type="application/manifest+json")


@never_cache
def bonos_ventas_sw(request):
    with open(_static_file_path("bonos_ventas/sw.js"), encoding="utf-8") as service_worker:
        return HttpResponse(service_worker.read(), content_type="application/javascript")
