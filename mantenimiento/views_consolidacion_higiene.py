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


PATRON_PAR = re.compile(r"([1-9]\d{0,18}):([1-9]\d{0,18})\Z")
MAX_ID_BD = 9_223_372_036_854_775_807
MAX_PARES_POR_SOLICITUD = 500
ANCLA_EXACTAS = "#exactas-title"


def _es_async(request):
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def _normalizar_par(valor):
    if not isinstance(valor, str):
        return None
    coincidencia = PATRON_PAR.fullmatch(valor.strip())
    if not coincidencia:
        return None
    try:
        par = tuple(int(item) for item in coincidencia.groups())
    except ValueError:
        return None
    if any(item > MAX_ID_BD for item in par):
        return None
    return par if par[0] != par[1] else None


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
        par = _normalizar_par(valor)
        if par is None:
            raise ValueError("La selección contiene un par de fallas no válido.")
        if par not in vistos:
            vistos.add(par)
            pares.append(par)
    return tuple(pares)


def _pares_validos_para_reintento(valores):
    pares = set()
    for valor in valores[:MAX_PARES_POR_SOLICITUD]:
        par = _normalizar_par(valor)
        if par is not None:
            pares.add(f"{par[0]}:{par[1]}")
    return frozenset(pares)


def _contexto(preview=None, *, pares_seleccionados=()):
    preview = preview or proponer_consolidacion_higiene()
    return {
        "exactas": preview.exactas,
        "ambiguas": preview.ambiguas,
        "total_exactas": len(preview.exactas),
        "total_ambiguas": len(preview.ambiguas),
        "pares_seleccionados": pares_seleccionados,
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

    valores = request.POST.getlist("pares")
    try:
        pares = _parsear_pares(valores)
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
            _contexto(
                pares_seleccionados=_pares_validos_para_reintento(valores)
            ),
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
        "redirect": f"{request.path}{ANCLA_EXACTAS}",
    }
    if _es_async(request):
        return JsonResponse(payload)
    if tipo == "success":
        messages.success(request, mensaje)
    else:
        messages.warning(request, mensaje)
    return redirect(f"{request.path}{ANCLA_EXACTAS}")
