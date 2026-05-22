from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from .services import build_operacion_context


@login_required
def app_home(request):
    return render(request, "operacion/app_home.html", build_operacion_context(request.user))
