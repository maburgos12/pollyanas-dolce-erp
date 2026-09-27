import re

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

from core.access import is_admin_or_dg
from operacion.services_higiene_consolidacion import (
    aplicar_consolidacion_higiene,
    proponer_consolidacion_higiene,
)


PATRON_PAR = re.compile(r"([1-9]\d*):([1-9]\d*)\Z")
MAX_PARES_POR_SOLICITUD = 500


def _es_async(request):
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def _parsear_pares(valores):
    if not valores:
        raise ValueError("Selecciona al menos una coincidencia exacta para aplicar.")
    if len(valores) > MAX_PARES_POR_SOLICITUD:
        raise ValueError(
            "La selección es demasiado grande; actualiza la revisión y vuelve a intentar."
        )
    pares = []
    vistos = set()
    for valor in valores:
        coincidencia = PATRON_PAR.fullmatch(valor.strip())
        if not coincidencia:
            raise ValueError("La selección contiene un par de fallas no válido.")
        par = tuple(int(item) for item in coincidencia.groups())
        if par[0] == par[1]:
            raise ValueError("Una falla no puede consolidarse consigo misma.")
        if par not in vistos:
            vistos.add(par)
            pares.append(par)
    return tuple(pares)


def _contexto(preview=None):
    preview = preview or proponer_consolidacion_higiene()
    return {
        "exactas": preview.exactas,
        "ambiguas": preview.ambiguas,
        "total_exactas": len(preview.exactas),
        "total_ambiguas": len(preview.ambiguas),
    }


@login_required
@require_http_methods(["GET", "POST"])
def consolidacion_higiene(request):
    if not is_admin_or_dg(request.user):
        raise PermissionDenied
    if request.method == "GET":
        return render(
            request,
            "mantenimiento/consolidacion_higiene.html",
            _contexto(),
        )

    try:
        pares = _parsear_pares(request.POST.getlist("pares"))
    except ValueError as exc:
        if _es_async(request):
            return JsonResponse(
                {
                    "ok": False,
                    "toast": {
                        "type": "error",
                        "message": str(exc),
                        "persistent": True,
                    },
                },
                status=400,
            )
        messages.error(request, str(exc))
        return render(
            request,
            "mantenimiento/consolidacion_higiene.html",
            _contexto(),
            status=400,
        )

    resultado = aplicar_consolidacion_higiene(pares, actor=request.user)
    mensaje = (
        f"{resultado.aplicados} coincidencias aplicadas; "
        f"{resultado.omitidos} omitidas tras validar la revisión vigente."
    )
    tipo = "success" if resultado.aplicados else "warning"
    payload = {
        "ok": True,
        "toast": {"type": tipo, "message": mensaje},
        "redirect": request.path,
    }
    if _es_async(request):
        return JsonResponse(payload)
    if tipo == "success":
        messages.success(request, mensaje)
    else:
        messages.warning(request, mensaje)
    return redirect(request.path)
