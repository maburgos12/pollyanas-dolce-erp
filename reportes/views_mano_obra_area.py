from __future__ import annotations

from datetime import timedelta

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_manage_mano_obra_area
from recetas.models import Receta
from reportes.mano_obra_grupos_familia import grupo_de_familia
from reportes.models import RecetaAreaProduccion
from reportes.services_mano_obra_diaria_area import calcular_costo_diario_area

AREAS = [choice[0] for choice in RecetaAreaProduccion.AREA_CHOICES]


def _require_manage(user) -> None:
    if not can_manage_mano_obra_area(user):
        raise PermissionDenied("No tienes permisos para gestionar mano de obra por área")


@login_required
def clasificacion_area_produccion(request):
    _require_manage(request.user)

    if request.method == "POST":
        accion = request.POST.get("accion")
        if accion == "toggle_familia":
            # Se guarda el GRUPO canónico (ej. "Pastel"), no la familia
            # cruda del formulario — un grupo puede representar varias
            # familias reales de Point (ver mano_obra_grupos_familia.py).
            grupo = grupo_de_familia(request.POST.get("familia", "").strip())
            area = request.POST.get("area", "").strip()
            if grupo and area in AREAS:
                fila, created = RecetaAreaProduccion.objects.get_or_create(familia=grupo, area=area, receta=None)
                if not created:
                    fila.delete()
        elif accion == "agregar_excepcion":
            receta_id = request.POST.get("receta_id")
            area = request.POST.get("area", "").strip()
            if receta_id and area in AREAS:
                RecetaAreaProduccion.objects.get_or_create(receta_id=receta_id, area=area, familia="")
        elif accion == "quitar_excepcion":
            fila_id = request.POST.get("fila_id")
            RecetaAreaProduccion.objects.filter(id=fila_id, receta__isnull=False).delete()
        return redirect("reportes:mano_obra_area_clasificacion")

    # Point es la fuente de la familia real (Receta.familia); varias
    # familias reales pueden mapear al mismo grupo canónico de mano de obra
    # (decisión de negocio explícita, ver mano_obra_grupos_familia.py) —
    # se muestra UNA tarjeta por grupo, no por familia cruda.
    conteo_por_familia_real = dict(
        Receta.objects.exclude(familia="").values_list("familia").annotate(n=Count("id"))
    )
    grupos_ctx: dict[str, dict] = {}
    for familia_real, cantidad in conteo_por_familia_real.items():
        grupo = grupo_de_familia(familia_real)
        entrada = grupos_ctx.setdefault(grupo, {"nombre": grupo, "cantidad": 0, "familias_reales": set()})
        entrada["cantidad"] += cantidad
        entrada["familias_reales"].add(familia_real)

    areas_por_grupo = {}
    for fila in RecetaAreaProduccion.objects.filter(receta__isnull=True):
        areas_por_grupo.setdefault(fila.familia, set()).add(fila.area)

    for grupo, entrada in grupos_ctx.items():
        entrada["areas"] = areas_por_grupo.get(grupo, set())
        entrada["familias_reales"] = sorted(entrada["familias_reales"])

    familias_ctx = sorted(grupos_ctx.values(), key=lambda entrada: entrada["nombre"])

    excepciones = (
        RecetaAreaProduccion.objects.filter(receta__isnull=False)
        .select_related("receta")
        .order_by("receta__nombre")
    )
    excepciones_por_receta: dict[int, dict] = {}
    for fila in excepciones:
        entrada = excepciones_por_receta.setdefault(
            fila.receta_id,
            {"receta": fila.receta, "areas": set(), "filas": {}},
        )
        entrada["areas"].add(fila.area)
        entrada["filas"][fila.area] = fila.id

    return render(
        request,
        "reportes/mano_obra_area_clasificacion.html",
        {
            "familias": familias_ctx,
            "excepciones": list(excepciones_por_receta.values()),
            "areas": RecetaAreaProduccion.AREA_CHOICES,
        },
    )


@login_required
def reporte_costo_diario_area(request):
    _require_manage(request.user)

    try:
        dias = int(request.GET.get("dias", 7))
    except (TypeError, ValueError):
        dias = 7
    dias = max(1, min(dias, 30))

    hoy = timezone.localdate()
    fechas = [hoy - timedelta(days=offset) for offset in range(dias)]

    # ponytail: recalcula on-demand en cada request en vez de vía tarea
    # programada — aceptable para el volumen (dias<=30 x 3 areas). Si se
    # vuelve lento, mover a un PeriodicTask nocturno como
    # "reportes: snapshot operacion dg" y leer CostoManoObraDiarioArea aqui.
    bloques = []
    for area_valor, area_label in RecetaAreaProduccion.AREA_CHOICES:
        filas = [calcular_costo_diario_area(fecha, area_valor) for fecha in fechas]
        bloques.append({"valor": area_valor, "label": area_label, "hoy": filas[0], "filas": filas})

    return render(
        request,
        "reportes/mano_obra_area_reporte.html",
        {
            "bloques": bloques,
            "dias": dias,
        },
    )
