from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse

from core.access import can_view_submodule
from sat_client.models import SolicitudDocumentoSat
from sat_client.services.documentos import solicitar_documento_sat
from sat_client.services.fiscal_tools import documentos_sat_estado, estado_sat


def _assert_sat_access(request: HttpRequest) -> None:
    if not can_view_submodule(request.user, "conciliacion", "fiscal"):
        raise PermissionDenied("No tienes acceso al panel SAT fiscal.")


@login_required
def panel_fiscal(request: HttpRequest) -> HttpResponse:
    _assert_sat_access(request)
    if request.method == "POST":
        tipo = request.POST.get("tipo")
        if tipo in dict(SolicitudDocumentoSat.TIPO_CHOICES):
            solicitar_documento_sat(tipo=tipo, usuario=request.user)
            messages.warning(request, "Solicitud registrada. Falta conectar la descarga real en SAT.")
        return redirect(reverse("sat_client:panel_fiscal"))

    return render(
        request,
        "sat_client/panel_fiscal.html",
        {
            "estado_sat": estado_sat(),
            "documentos_sat": documentos_sat_estado(),
        },
    )
