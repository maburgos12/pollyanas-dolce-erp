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

from core.access import can_view_module, can_view_submodule
from core.models import Sucursal
from recetas.models import Receta

from recetas.models import RecetaCodigoPointAlias
from recetas.utils.normalizacion import normalizar_nombre

from .bitacoras_config import BITACORA_CONFIG, BITACORA_GROUPS, SIZE_LABELS
from .models import BitacoraOperativa, BitacoraOperativaLinea
from .services import build_operacion_context


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


def _decimal(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return str(Decimal(value))
    except (InvalidOperation, ValueError):
        return None


NUMERIC_FIELDS = {
    "apertura",
    "cantidad",
    "cedis",
    "cierre",
    "devolucion",
    "existencia",
    "merma_rebanadas",
    "pastel_entero",
    "pedido",
    "preparacion",
    "proyeccion",
    "stock",
    "total",
    "total_rebanadas",
}


FIELD_LABELS = {
    "apertura": "T. apertura",
    "cantidad": "Cantidad",
    "cedis": "CEDIS",
    "cierre": "T. cierre",
    "devolucion": "Devolución",
    "dia": "Día",
    "existencia": "Existencia",
    "fecha_producto": "Fecha producto",
    "hora": "Hora",
    "item": "Producto / insumo",
    "merma_rebanadas": "Merma rebanadas",
    "motivo": "Motivo",
    "motivo_merma": "Motivo merma",
    "pastel_entero": "Pastel entero",
    "pedido": "Pedido",
    "preparacion": "Preparación",
    "proyeccion": "Proyección",
    "responsable": "Responsable",
    "stock": "Stock",
    "sucursal_texto": "Sucursal",
    "total": "Total",
    "total_rebanadas": "Total rebanadas",
    "unidad": "Unidad",
}


FAMILY_LABELS = {
    "checklist": "Checklist",
    "daily_product": "Diaria",
    "free_rows": "Registro",
    "temperature": "Temperatura",
    "weekly_matrix": "Semanal",
}


def _clean_field(campo: str, raw: str):
    raw = (raw or "").strip()
    if not raw:
        return ""
    if campo in NUMERIC_FIELDS:
        return _decimal(raw) or ""
    return raw


def _fields(config):
    return [{"name": campo, "label": FIELD_LABELS.get(campo, campo.replace("_", " ").title())} for campo in config.get("campos", [])]


def _resolve_receta_by_aliases(aliases):
    for alias in aliases:
        alias = (alias or "").strip()
        if not alias:
            continue
        receta = (
            Receta.objects.filter(nombre_normalizado=normalizar_nombre(alias), pasa_modulo_produccion=True)
            .order_by("id")
            .first()
        )
        if receta is not None:
            return receta
        point_alias = (
            RecetaCodigoPointAlias.objects.filter(activo=True, nombre_point__iexact=alias)
            .select_related("receta")
            .order_by("id")
            .first()
        )
        if point_alias and point_alias.receta_id:
            return point_alias.receta
    tokens = [token for alias in aliases for token in normalizar_nombre(alias).split() if len(token) > 2]
    if tokens:
        qs = Receta.objects.filter(pasa_modulo_produccion=True)
        for token in tokens[:4]:
            qs = qs.filter(nombre_normalizado__contains=token)
        return qs.order_by("nombre").first()
    return None


def _recipe_row(index, item):
    if isinstance(item, dict):
        producto = item["producto"]
        tamano = item["tamano"]
        aliases = item.get("aliases") or []
        fallback = f"{producto} {SIZE_LABELS.get(tamano, tamano)}".strip()
    elif isinstance(item, tuple):
        producto, tamano = item
        aliases = []
        fallback = f"{producto} {SIZE_LABELS.get(tamano, tamano)}".strip()
    else:
        producto = str(item)
        tamano = ""
        aliases = [producto]
        fallback = producto
    receta = _resolve_receta_by_aliases([*aliases, fallback, producto])
    return {
        "index": index,
        "item": receta.nombre if receta else fallback,
        "fallback_item": fallback,
        "producto": producto,
        "tamano": tamano,
        "tamano_label": SIZE_LABELS.get(tamano, tamano),
        "receta": receta,
        "receta_id": receta.id if receta else "",
        "codigo_point": receta.codigo_point if receta else "",
        "vinculado": bool(receta),
    }


def _prepared_rows(config):
    family = config["familia"]
    if family == "temperature":
        return [
            {"index": index, "item": equipo, "min": minimo, "max": maximo}
            for index, (equipo, minimo, maximo) in enumerate(config["equipos"])
        ]
    if family in {"daily_product", "weekly_matrix"}:
        return [_recipe_row(index, item) for index, item in enumerate(config["items"])]
    if family == "checklist":
        return [{"index": index, "section": section, "item": item} for index, (section, item) in enumerate(config["items"])]
    return [{"index": index} for index in range(12)]


def _lineas_from_family(request, config):
    family = config["familia"]
    rows = _prepared_rows(config)
    lineas = []
    for row in rows:
        index = row["index"]
        datos = {}
        has_input = False
        observaciones = (request.POST.get(f"observaciones_{index}") or "").strip()
        receta = None
        receta_id = request.POST.get(f"receta_{index}")
        if receta_id:
            receta = Receta.objects.filter(pk=receta_id).first()
            has_input = bool(receta)

        if family in {"daily_product", "weekly_matrix", "temperature"}:
            datos["item"] = receta.nombre if receta else row["item"]
        if row.get("tamano"):
            datos["tamano"] = row["tamano"]
        if receta and receta.codigo_point:
            datos["codigo_point"] = receta.codigo_point
        elif row.get("codigo_point"):
            datos["codigo_point"] = row["codigo_point"]
        if row.get("fallback_item") and row.get("fallback_item") != row["item"]:
            datos["alias_captura"] = row["fallback_item"]
        if family == "temperature":
            datos["rango_min"] = row["min"]
            datos["rango_max"] = row["max"]
        if family == "checklist":
            datos["seccion"] = row["section"]
            datos["item"] = row["item"]
            cumple = request.POST.get(f"cumple_{index}")
            if cumple in {"si", "no", "na"}:
                datos["cumple"] = cumple
                has_input = True

        for campo in config.get("campos", []):
            value = _clean_field(campo, request.POST.get(f"{campo}_{index}"))
            if value:
                datos[campo] = value
                has_input = True

        if family == "free_rows" and not has_input:
            continue
        if family == "checklist" and "cumple" not in datos and not observaciones:
            continue
        if has_input or observaciones:
            lineas.append((receta, datos, observaciones))
    return lineas


def _lineas_from_post(request, config):
    if config.get("familia"):
        return _lineas_from_family(request, config)

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


def _notas_from_post(request):
    partes = []
    dia = (request.POST.get("dia_captura") or "").strip()
    responsable = (request.POST.get("responsable_general") or "").strip()
    notas = (request.POST.get("notas") or "").strip()
    if dia:
        partes.append(f"Día: {dia}")
    if responsable:
        partes.append(f"Responsable: {responsable}")
    if notas:
        partes.append(notas)
    return "\n".join(partes)


@login_required
def bitacoras_home(request):
    if not _can_use_bitacoras(request.user):
        raise PermissionDenied
    recientes = BitacoraOperativa.objects.select_related("creado_por").prefetch_related("lineas")[:8]
    grupos = [
        {
            "title": title,
            "items": [
                {
                    "tipo": tipo,
                    "config": BITACORA_CONFIG[tipo],
                    "familia_label": FAMILY_LABELS.get(BITACORA_CONFIG[tipo]["familia"], "Captura"),
                }
                for tipo in tipos
                if tipo in BITACORA_CONFIG
            ],
        }
        for title, tipos in BITACORA_GROUPS
    ]
    return render(
        request,
        "operacion/bitacoras_home.html",
        {"grupos": grupos, "config": BITACORA_CONFIG, "recientes": recientes},
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
            notas=_notas_from_post(request),
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
            "fields": _fields(config),
            "family": config["familia"],
            "rows": _prepared_rows(config),
            "recetas": recetas,
            "sucursales": sucursales,
            "row_range": range(8),
            "today": timezone.localdate(),
        },
    )
