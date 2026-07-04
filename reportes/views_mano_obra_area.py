from __future__ import annotations

from datetime import timedelta
from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_manage_mano_obra_area
from recetas.models import Receta
from reportes.mano_obra_grupos_familia import grupo_de_familia
from reportes.models import FamiliaGrupoManoObra, RecetaAreaProduccion
from reportes.services_mano_obra_diaria_area import calcular_costo_diario_area

AREAS = [choice[0] for choice in RecetaAreaProduccion.AREA_CHOICES]


def _entero_o_none(valor: str | None) -> int | None:
    try:
        return int(valor) if valor else None
    except (TypeError, ValueError):
        return None


def _decimal_o_none(valor: str | None) -> Decimal | None:
    try:
        return Decimal(valor) if valor else None
    except (InvalidOperation, TypeError, ValueError):
        return None


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
        elif accion == "capturar_lote":
            grupo = request.POST.get("familia", "").strip()
            area = request.POST.get("area", "").strip()
            if grupo and area in AREAS:
                fila, _created = RecetaAreaProduccion.objects.get_or_create(
                    familia=grupo, area=area, receta=None
                )
                fila.lote_personas = _entero_o_none(request.POST.get("lote_personas"))
                fila.lote_minutos = _decimal_o_none(request.POST.get("lote_minutos"))
                fila.lote_piezas = _entero_o_none(request.POST.get("lote_piezas"))
                fila.save(update_fields=["lote_personas", "lote_minutos", "lote_piezas"])
        elif accion == "fusionar_grupo":
            # Fusión editable desde la pantalla, sin depender de un cambio
            # de código — Carolina resuelve casos nuevos de Point (ej.
            # "RELLENOS Y CREMAS") en el momento.
            familia_real = request.POST.get("familia_real", "").strip()
            grupo_destino = request.POST.get("grupo_destino", "").strip()
            if familia_real and grupo_destino:
                FamiliaGrupoManoObra.objects.update_or_create(
                    familia_real=familia_real, defaults={"grupo": grupo_destino}
                )
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

    filas_por_grupo: dict[str, dict[str, RecetaAreaProduccion]] = {}
    for fila in RecetaAreaProduccion.objects.filter(receta__isnull=True):
        filas_por_grupo.setdefault(fila.familia, {})[fila.area] = fila

    for grupo, entrada in grupos_ctx.items():
        filas_area = filas_por_grupo.get(grupo, {})
        entrada["areas"] = set(filas_area.keys())
        # Django templates no hacen lookup de diccionario con clave
        # dinámica — se arma aquí una lista ya emparejada por área para que
        # el template solo itere, sin necesitar filtros custom.
        entrada["areas_detalle"] = [
            {
                "value": area_valor,
                "label": area_label,
                "activa": area_valor in filas_area,
                "fila": filas_area.get(area_valor),
            }
            for area_valor, area_label in RecetaAreaProduccion.AREA_CHOICES
        ]
        entrada["familias_reales"] = sorted(entrada["familias_reales"])

    familias_ctx = sorted(grupos_ctx.values(), key=lambda entrada: entrada["nombre"])
    grupos_existentes = sorted(grupos_ctx.keys())

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
            "grupos_existentes": grupos_existentes,
        },
    )


def _con_aprovechamiento(snapshot):
    """Django templates no dividen — se calcula aquí el % de
    aprovechamiento y los minutos ociosos, y se cuelgan como atributos
    dinámicos del snapshot para que el template los lea directo."""
    disponibles = snapshot.minutos_disponibles
    demandados = snapshot.minutos_demandados
    if disponibles and disponibles > 0 and demandados is not None:
        snapshot.pct_aprovechamiento = (demandados / disponibles) * 100
        snapshot.minutos_ociosos = max(disponibles - demandados, Decimal("0"))
    else:
        snapshot.pct_aprovechamiento = None
        snapshot.minutos_ociosos = None
    return snapshot


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
        filas = [_con_aprovechamiento(calcular_costo_diario_area(fecha, area_valor)) for fecha in fechas]
        bloques.append({"valor": area_valor, "label": area_label, "hoy": filas[0], "filas": filas})

    return render(
        request,
        "reportes/mano_obra_area_reporte.html",
        {
            "bloques": bloques,
            "dias": dias,
        },
    )
