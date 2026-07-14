from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.contrib.staticfiles import finders
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache

from core.access import can_view_module, can_view_submodule, is_mermas_only
from core.models import Sucursal
from recetas.models import Receta

from .models import BitacoraOperativa, BitacoraOperativaLinea
from .services import build_operacion_context


BITACORA_CONFIG = {
    BitacoraOperativa.TIPO_SALIDAS_CFP1: {
        "titulo": "Salidas CFP1",
        "ayuda": "Cantidades enviadas por producto a cada sucursal.",
        "campos": ["cantidad"],
        "usa_sucursales": True,
    },
    BitacoraOperativa.TIPO_INVENTARIO_CFP1: {
        "titulo": "Inventario CFP1",
        "ayuda": "Existencia CEDIS y devolución del día.",
        "campos": ["cedis", "devolucion"],
    },
    BitacoraOperativa.TIPO_PLAGAS: {
        "titulo": "Control de plagas",
        "ayuda": "Registro de detección o aplicación.",
        "campos": ["plaga", "area", "metodo", "fecha_deteccion"],
        "sin_producto": True,
    },
    BitacoraOperativa.TIPO_CFP11: {
        "titulo": "Inventario CFP 1.1",
        "ayuda": "Bloques de existencia, salida y entrada.",
        "campos": ["bloque", "tamano", "existencia", "salida", "entrada"],
    },
    BitacoraOperativa.TIPO_ROTACION: {
        "titulo": "Rotación producto",
        "ayuda": "Producto, cantidad y fecha del producto.",
        "campos": ["cantidad", "fecha_producto"],
    },
    BitacoraOperativa.TIPO_REBANADO: {
        "titulo": "Producto rebanado",
        "ayuda": "Enteros, rebanadas y merma.",
        "campos": ["pastel_entero", "total_rebanadas", "merma_rebanadas", "fecha_producto", "motivo_merma"],
    },
}


@login_required
def app_home(request):
    return render(request, "operacion/app_home.html", build_operacion_context(request.user))


@never_cache
def app_sw(request):
    path = finders.find("operacion/sw.js")
    if not path:
        raise Http404("Service worker de App Operativa no encontrado")
    with open(path, encoding="utf-8") as service_worker:
        return HttpResponse(service_worker.read(), content_type="application/javascript")


def _can_use_bitacoras(user) -> bool:
    if user.is_superuser:
        return True
    if is_mermas_only(user):
        return False
    return (
        can_view_module(user, "produccion")
        or can_view_module(user, "logistica")
        or can_view_submodule(user, "mermas", "captura")
        or can_view_submodule(user, "mermas", "recepcion")
    )


def _decimal(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return str(Decimal(value))
    except (InvalidOperation, ValueError):
        return None


def _lineas_from_post(request, config):
    lineas = []
    for index in range(8):
        receta = None
        datos = {}
        observaciones = (request.POST.get(f"observaciones_{index}") or "").strip()
        if not config.get("sin_producto"):
            receta_id = request.POST.get(f"receta_{index}")
            if not receta_id:
                continue
            receta = Receta.objects.filter(pk=receta_id).first()
            if not receta:
                continue
        for campo in config["campos"]:
            raw = (request.POST.get(f"{campo}_{index}") or "").strip()
            if campo in {
                "cantidad",
                "cedis",
                "devolucion",
                "existencia",
                "salida",
                "entrada",
                "pastel_entero",
                "total_rebanadas",
                "merma_rebanadas",
            }:
                raw = _decimal(raw) or ""
            if raw:
                datos[campo] = raw
        if config.get("usa_sucursales"):
            cantidades = {}
            prefix = f"sucursal_{index}_"
            for key, raw in request.POST.items():
                if key.startswith(prefix):
                    value = _decimal(raw)
                    if value:
                        cantidades[key.removeprefix(prefix)] = value
            if cantidades:
                datos["sucursales"] = cantidades
        if receta or datos or observaciones:
            lineas.append((receta, datos, observaciones))
    return lineas


@login_required
def bitacoras_home(request):
    if not _can_use_bitacoras(request.user):
        raise PermissionDenied
    recientes = BitacoraOperativa.objects.select_related("creado_por").prefetch_related("lineas")[:8]
    return render(
        request,
        "operacion/bitacoras_home.html",
        {"tipos": BitacoraOperativa.TIPO_CHOICES, "config": BITACORA_CONFIG, "recientes": recientes},
    )


@login_required
def bitacora_captura(request, tipo):
    if not _can_use_bitacoras(request.user) or tipo not in BITACORA_CONFIG:
        raise PermissionDenied
    config = BITACORA_CONFIG[tipo]
    sucursales = list(Sucursal.objects.filter(activa=True).order_by("codigo"))
    recetas = Receta.objects.filter(pasa_modulo_produccion=True).order_by("nombre")[:120]
    if request.method == "POST":
        bitacora = BitacoraOperativa.objects.create(
            tipo=tipo,
            fecha=request.POST.get("fecha") or timezone.localdate(),
            sucursal_id=request.POST.get("sucursal") or None,
            notas=(request.POST.get("notas") or "").strip(),
            creado_por=request.user,
        )
        for receta, datos, observaciones in _lineas_from_post(request, config):
            BitacoraOperativaLinea.objects.create(
                bitacora=bitacora,
                receta=receta,
                datos=datos,
                observaciones=observaciones,
            )
        if request.POST.get("cerrar") == "1":
            bitacora.cerrar()
            bitacora.save(update_fields=["estatus", "cerrado_en", "actualizado_en"])
        messages.success(request, "Bitácora guardada.")
        return redirect("operacion:bitacoras_home")
    return render(
        request,
        "operacion/bitacora_captura.html",
        {
            "tipo": tipo,
            "config": config,
            "recetas": recetas,
            "sucursales": sucursales,
            "row_range": range(8),
            "today": timezone.localdate(),
        },
    )
