from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.contrib.staticfiles import finders
from django.db.models.functions import Trim
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache

from core.access import can_view_module, can_view_submodule
from core.models import Sucursal
from recetas.models import Receta

from .bitacoras_config import BITACORA_CONFIG
from .models import BitacoraOperativa, BitacoraOperativaLinea
from .services import build_operacion_context


PRODUCTION_BITACORA_TYPES = {
    BitacoraOperativa.TIPO_HORNOS,
    BitacoraOperativa.TIPO_ARMADO,
}

DECIMAL_FIELDS = {
    "cantidad",
    "cedis",
    "devolucion",
    "existencia",
    "salida",
    "entrada",
    "pastel_entero",
    "total_rebanadas",
    "merma_rebanadas",
    "preparacion",
    "existencia_fisica",
    "salida_armado",
    "consumo_real",
    "producto_terminado",
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
    return (
        can_view_module(user, "produccion")
        or can_view_module(user, "logistica")
        or can_view_submodule(user, "mermas", "captura")
        or can_view_submodule(user, "mermas", "recepcion")
    )


def _can_use_bitacora_type(user, tipo: str) -> bool:
    if not _can_use_bitacoras(user):
        return False
    if tipo in PRODUCTION_BITACORA_TYPES:
        return user.is_superuser or can_view_module(user, "produccion")
    return True


def _decimal(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValidationError("Ingresa una cantidad numérica válida.")
    if not decimal_value.is_finite():
        raise ValidationError("Ingresa una cantidad numérica válida.")
    return str(decimal_value)


def _recetas_for_config(config):
    recetas = Receta.objects.filter(pasa_modulo_produccion=True)
    if config.get("receta_tipo"):
        recetas = recetas.filter(tipo=config["receta_tipo"])
    if config.get("requiere_codigo_point"):
        recetas = recetas.annotate(codigo_point_limpio=Trim("codigo_point")).exclude(codigo_point_limpio="")
    return recetas.order_by("nombre")


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
            receta = _recetas_for_config(config).filter(pk=receta_id).first()
            if not receta:
                if config.get("requiere_codigo_point"):
                    raise ValidationError("Selecciona un producto válido con identidad Point.")
                raise ValidationError("Selecciona un producto válido.")
        for campo in config["campos"]:
            raw = (request.POST.get(f"{campo}_{index}") or "").strip()
            if campo in DECIMAL_FIELDS:
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
        if receta and not datos and not observaciones:
            raise ValidationError("Captura al menos una cantidad u observación.")
        if receta or datos or observaciones:
            lineas.append((receta, datos, observaciones))
    if config.get("requiere_codigo_point") and not lineas:
        raise ValidationError("Selecciona un producto válido con identidad Point.")
    return lineas


@login_required
def bitacoras_home(request):
    if not _can_use_bitacoras(request.user):
        raise PermissionDenied
    tipos = [choice for choice in BitacoraOperativa.TIPO_CHOICES if _can_use_bitacora_type(request.user, choice[0])]
    recientes = BitacoraOperativa.objects.select_related("creado_por").prefetch_related("lineas")
    if not (request.user.is_superuser or can_view_module(request.user, "produccion")):
        recientes = recientes.exclude(tipo__in=PRODUCTION_BITACORA_TYPES)
    return render(
        request,
        "operacion/bitacoras_home.html",
        {"tipos": tipos, "config": BITACORA_CONFIG, "recientes": recientes[:8]},
    )


@login_required
def bitacora_captura(request, tipo):
    if tipo not in BITACORA_CONFIG or not _can_use_bitacora_type(request.user, tipo):
        raise PermissionDenied
    config = BITACORA_CONFIG[tipo]
    sucursales = list(Sucursal.objects.filter(activa=True).order_by("codigo"))
    recetas = _recetas_for_config(config)[:120]
    if request.method == "POST":
        try:
            lineas = _lineas_from_post(request, config)
        except ValidationError as exc:
            messages.error(request, exc.messages[0])
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
                    "submitted_values": request.POST,
                },
            )
        bitacora = BitacoraOperativa.objects.create(
            tipo=tipo,
            fecha=request.POST.get("fecha") or timezone.localdate(),
            sucursal_id=request.POST.get("sucursal") or None,
            notas=(request.POST.get("notas") or "").strip(),
            creado_por=request.user,
        )
        for receta, datos, observaciones in lineas:
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
