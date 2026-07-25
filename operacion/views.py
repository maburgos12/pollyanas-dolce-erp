from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.contrib.staticfiles import finders
from django.db.models import Q
from django.http import Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache

from core.access import can_view_module, can_view_submodule
from core.models import Notificacion, Sucursal, UserModuleAccess
from core.notificaciones import crear_notificaciones
from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
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
    "entrada",
    "existencia",
    "existencia_final",
    "merma_rebanadas",
    "pastel_entero",
    "pedido",
    "preparacion",
    "proyeccion",
    "salida",
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
    "entrada": "Entrada",
    "existencia": "Existencia",
    "existencia_final": "Existencia final",
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
    "salida": "Salida",
    "stock": "Stock",
    "sucursal_texto": "Sucursal",
    "total": "Total",
    "total_rebanadas": "Total rebanadas",
    "unidad": "Unidad",
}


REFERENCE_LABELS = {
    "proyeccion": "Proyección",
    "stock": "Stock fijo",
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


def _reference_fields(config):
    return [
        {"name": campo, "label": REFERENCE_LABELS.get(campo, FIELD_LABELS.get(campo, campo.replace("_", " ").title()))}
        for campo in config.get("referencias_jefatura", [])
    ]


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


def _suggest_recetas(alias, limit=6):
    tokens = [token for token in normalizar_nombre(alias).split() if len(token) > 2]
    qs = Receta.objects.filter(pasa_modulo_produccion=True)
    for token in tokens[:3]:
        qs = qs.filter(nombre_normalizado__contains=token)
    strict = list(qs.order_by("nombre")[:limit])
    if strict or not tokens:
        return strict
    any_token = Q()
    for token in tokens[:3]:
        any_token |= Q(nombre_normalizado__contains=token)
    return list(Receta.objects.filter(any_token, pasa_modulo_produccion=True).order_by("nombre")[:limit])


def _alias_codigo(alias):
    base = normalizar_codigo_point(alias)[:60] or "alias"
    return f"BITACORA-{base}"


def _previous_cfp11_final(receta, fallback, tamano, before_date=None):
    before_date = before_date or timezone.localdate()
    qs = (
        BitacoraOperativaLinea.objects.filter(bitacora__tipo=BitacoraOperativa.TIPO_CFP11, bitacora__fecha__lt=before_date)
        .exclude(datos__existencia_final="")
        .exclude(datos__existencia_final__isnull=True)
        .select_related("bitacora")
        .order_by("-bitacora__fecha", "-id")
    )
    if receta:
        qs = qs.filter(receta=receta)
    else:
        qs = qs.filter(datos__alias_captura=fallback)
    if tamano:
        qs = qs.filter(datos__tamano=tamano)
    linea = qs.first()
    return (linea.datos.get("existencia_final") if linea else "") or "--"


def _decimal_or_none(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _cfp11_reconciliation(bitacora):
    rows = []
    for linea in bitacora.lineas.select_related("receta").order_by("id"):
        datos = linea.datos or {}
        inicial_raw = _previous_cfp11_final(linea.receta, datos.get("alias_captura") or datos.get("item"), datos.get("tamano"), bitacora.fecha)
        inicial = _decimal_or_none(inicial_raw)
        entrada = _decimal_or_none(datos.get("entrada")) or Decimal("0")
        salida = _decimal_or_none(datos.get("salida")) or Decimal("0")
        final = _decimal_or_none(datos.get("existencia_final"))
        esperado = inicial + entrada - salida if inicial is not None else None
        diferencia = final - esperado if esperado is not None and final is not None else None
        rows.append(
            {
                "producto": linea.receta.nombre if linea.receta else datos.get("item", "Producto"),
                "inicial": inicial_raw,
                "entrada": datos.get("entrada") or "0",
                "salida": datos.get("salida") or "0",
                "esperado": esperado,
                "final": datos.get("existencia_final") or "--",
                "diferencia": diferencia,
                "estado": "Sin inicial" if esperado is None else ("OK" if diferencia == 0 else ("Sobra" if diferencia and diferencia > 0 else "Falta")),
            }
        )
    return rows


def _recipe_row(index, item, config=None):
    config = config or {}
    reference_fields = _reference_fields(config)
    if isinstance(item, dict):
        producto = item["producto"]
        tamano = item["tamano"]
        aliases = item.get("aliases") or []
        fallback = f"{producto} {SIZE_LABELS.get(tamano, tamano)}".strip()
        referencias = [
            {"label": field["label"], "value": item.get(field["name"]) or "--"}
            for field in reference_fields
        ]
    elif isinstance(item, tuple):
        producto, tamano = item
        aliases = []
        fallback = f"{producto} {SIZE_LABELS.get(tamano, tamano)}".strip()
        referencias = [{"label": field["label"], "value": "--"} for field in reference_fields]
    else:
        producto = str(item)
        tamano = ""
        aliases = [producto]
        fallback = producto
        referencias = [{"label": field["label"], "value": "--"} for field in reference_fields]
    receta = _resolve_receta_by_aliases([*aliases, fallback, producto])
    if config.get("mostrar_inicial_esperado"):
        referencias.insert(
            0,
            {
                "label": "Inicial esperado",
                "value": _previous_cfp11_final(receta, fallback, tamano),
            },
        )
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
        "referencias": referencias,
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
        return [_recipe_row(index, item, config) for index, item in enumerate(config["items"])]
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
        if row.get("fallback_item") and (not receta or row.get("fallback_item") != row["item"]):
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
    accion = (request.POST.get("accion") or "").strip()
    dia = (request.POST.get("dia_captura") or "").strip()
    responsable = (request.POST.get("responsable_general") or "").strip()
    notas = (request.POST.get("notas") or "").strip()
    if accion == "existencia":
        partes.append("Modo: corte de existencia a ciegas")
        partes.append(f"Existencia guardada: {timezone.localtime(timezone.now()):%d/%m/%Y %H:%M}")
    if dia:
        partes.append(f"Día: {dia}")
    if responsable:
        partes.append(f"Responsable: {responsable}")
    if notas:
        partes.append(notas)
    return "\n".join(partes)


def _production_leads():
    return (
        UserModuleAccess.objects.filter(module="produccion", access=UserModuleAccess.ACCESS_MANAGE, user__is_active=True)
        .select_related("user")
        .order_by("user__username")
    )


def _notify_existence_saved(bitacora, actor):
    usuarios = [row.user for row in _production_leads()]
    return crear_notificaciones(
        usuarios,
        titulo=f"Existencia guardada · {bitacora.get_tipo_display()}",
        mensaje=f"{actor.get_username()} guardó existencia el {timezone.localtime(timezone.now()):%d/%m/%Y %H:%M}.",
        url="/app/bitacoras/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="operacion.BitacoraOperativa",
        objeto_id=bitacora.id,
        excluir=actor,
    )


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
        {"grupos": grupos, "config": BITACORA_CONFIG, "recientes": recientes, "pendientes_count": _pending_aliases().count()},
    )


def _pending_aliases():
    product_types = [tipo for tipo, config in BITACORA_CONFIG.items() if config["familia"] in {"daily_product", "weekly_matrix"}]
    return (
        BitacoraOperativaLinea.objects.filter(bitacora__tipo__in=product_types, receta__isnull=True)
        .exclude(datos__alias_captura="")
        .exclude(datos__alias_captura__isnull=True)
        .order_by("datos__alias_captura")
        .values("datos__alias_captura", "datos__tamano")
        .distinct()
    )


@login_required
def bitacoras_pendientes(request):
    if not _can_use_bitacoras(request.user):
        raise PermissionDenied
    if request.method == "POST":
        alias = (request.POST.get("alias") or "").strip()
        if not alias:
            messages.error(request, "Alias inválido.")
            return redirect("operacion:bitacoras_pendientes")
        receta = get_object_or_404(Receta, pk=request.POST.get("receta"), pasa_modulo_produccion=True)
        RecetaCodigoPointAlias.objects.update_or_create(
            codigo_point_normalizado=normalizar_codigo_point(_alias_codigo(alias)),
            defaults={"receta": receta, "codigo_point": _alias_codigo(alias), "nombre_point": alias, "activo": True},
        )
        BitacoraOperativaLinea.objects.filter(receta__isnull=True, datos__alias_captura=alias).update(receta=receta)
        messages.success(request, f"{alias} vinculado a {receta.nombre}.")
        return redirect("operacion:bitacoras_pendientes")

    pendientes = []
    for row in _pending_aliases():
        alias = row["datos__alias_captura"]
        pendientes.append(
            {
                "alias": alias,
                "tamano": row["datos__tamano"],
                "lineas": BitacoraOperativaLinea.objects.filter(receta__isnull=True, datos__alias_captura=alias).count(),
                "sugerencias": _suggest_recetas(alias),
            }
        )
    return render(request, "operacion/bitacoras_pendientes.html", {"pendientes": pendientes})


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
        if request.POST.get("accion") == "existencia":
            notificadas = _notify_existence_saved(bitacora, request.user)
            messages.success(request, f"Existencia guardada. Jefatura de producción recibió {notificadas} notificación(es).")
        elif request.POST.get("cerrar") == "1":
            bitacora.cerrar()
            bitacora.save(update_fields=["estatus", "cerrado_en", "actualizado_en"])
            messages.success(request, "Bitácora cerrada.")
        else:
            messages.success(request, "Bitácora guardada.")
        if tipo == BitacoraOperativa.TIPO_CFP11:
            return redirect(f"{request.path}?revision={bitacora.id}")
        return redirect("operacion:bitacoras_home")
    revision = None
    if tipo == BitacoraOperativa.TIPO_CFP11 and request.GET.get("revision"):
        bitacora_revision = get_object_or_404(BitacoraOperativa, pk=request.GET["revision"], tipo=tipo)
        revision = {"bitacora": bitacora_revision, "rows": _cfp11_reconciliation(bitacora_revision)}
    return render(
        request,
        "operacion/bitacora_captura.html",
        {
            "tipo": tipo,
            "config": config,
            "fields": _fields(config),
            "reference_fields": _reference_fields(config),
            "family": config["familia"],
            "rows": _prepared_rows(config),
            "recetas": recetas,
            "sucursales": sucursales,
            "row_range": range(8),
            "revision": revision,
            "today": timezone.localdate(),
        },
    )
