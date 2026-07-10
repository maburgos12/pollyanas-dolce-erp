from __future__ import annotations

from datetime import timedelta
from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_manage_mano_obra_area
from maestros.models import Insumo
from pos_bridge.models import PointProductionLine
from recetas.models import Receta
from reportes.models import RecetaAreaProduccion
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


def _grupos_ctx(modelo, es_grupo_insumo: bool, filtro_base, detectar_unidad: bool) -> tuple[list[dict], list[str]]:
    """Construye las tarjetas de clasificación (Productos o Catálogos):
    agrupa por `grupo_mano_obra` (o `nombre` si autocontenido), arma
    `areas_detalle` ya emparejado (Django templates no hacen lookup de
    diccionario con clave dinámica) y, si aplica, detecta la unidad real
    de producción para mostrarla en pantalla."""
    grupos: dict[str, dict] = {}
    for obj in filtro_base:
        grupo = obj.grupo_mano_obra or obj.nombre
        entrada = grupos.setdefault(grupo, {"nombre": grupo, "ids": set(), "miembros": set()})
        entrada["ids"].add(obj.id)
        entrada["miembros"].add(obj.nombre)

    filas_por_grupo: dict[str, dict[str, RecetaAreaProduccion]] = {}
    for fila in RecetaAreaProduccion.objects.filter(es_grupo_insumo=es_grupo_insumo):
        filas_por_grupo.setdefault(fila.familia, {})[fila.area] = fila

    for grupo, entrada in grupos.items():
        filas_area = filas_por_grupo.get(grupo, {})
        entrada["areas_detalle"] = [
            {
                "value": area_valor,
                "label": area_label,
                "activa": area_valor in filas_area,
                "fila": filas_area.get(area_valor),
            }
            for area_valor, area_label in RecetaAreaProduccion.AREA_CHOICES
        ]
        if detectar_unidad:
            unidad = (
                PointProductionLine.objects.filter(insumo_id__in=entrada["ids"])
                .exclude(unit="")
                .values_list("unit", flat=True)
                .first()
            )
            entrada["unidad_detectada"] = unidad or "unidades"
        entrada["miembros"] = sorted(entrada["miembros"])
        del entrada["ids"]

    ctx = sorted(grupos.values(), key=lambda entrada: entrada["nombre"])
    existentes = sorted(grupos.keys())
    return ctx, existentes


@login_required
def clasificacion_area_produccion(request):
    _require_manage(request.user)

    if request.method == "POST":
        accion = request.POST.get("accion")
        if accion == "toggle_familia":
            es_grupo_insumo = request.POST.get("es_grupo_insumo") == "1"
            grupo = request.POST.get("familia", "").strip()
            area = request.POST.get("area", "").strip()
            if grupo and area in AREAS:
                fila, created = RecetaAreaProduccion.objects.get_or_create(
                    familia=grupo, area=area, es_grupo_insumo=es_grupo_insumo
                )
                if not created:
                    fila.delete()
        elif accion == "capturar_lote":
            es_grupo_insumo = request.POST.get("es_grupo_insumo") == "1"
            grupo = request.POST.get("familia", "").strip()
            area = request.POST.get("area", "").strip()
            if grupo and area in AREAS:
                fila, _created = RecetaAreaProduccion.objects.get_or_create(
                    familia=grupo, area=area, es_grupo_insumo=es_grupo_insumo
                )
                fila.lote_personas = _entero_o_none(request.POST.get("lote_personas"))
                fila.lote_minutos = _decimal_o_none(request.POST.get("lote_minutos"))
                fila.lote_piezas = _entero_o_none(request.POST.get("lote_piezas"))
                fila.save(update_fields=["lote_personas", "lote_minutos", "lote_piezas"])
        elif accion == "fusionar_producto":
            # Fusión editable desde la pantalla: Carolina agrupa sabores
            # que comparten proceso, sin depender de un cambio de código.
            # Propaga a TODOS los productos que hoy resuelven a
            # grupo_actual (no solo el "fundador" del grupo).
            grupo_actual = request.POST.get("grupo_actual", "").strip()
            grupo_destino = request.POST.get("grupo_destino", "").strip()
            if grupo_actual and grupo_destino:
                Receta.objects.filter(
                    Q(grupo_mano_obra=grupo_actual) | Q(grupo_mano_obra="", nombre=grupo_actual)
                ).update(grupo_mano_obra=grupo_destino)
        elif accion == "fusionar_insumo":
            # Paralelo a fusionar_producto, pero para Catálogos.
            grupo_actual = request.POST.get("grupo_actual", "").strip()
            grupo_destino = request.POST.get("grupo_destino", "").strip()
            if grupo_actual and grupo_destino:
                Insumo.objects.filter(
                    Q(grupo_mano_obra=grupo_actual) | Q(grupo_mano_obra="", nombre=grupo_actual)
                ).update(grupo_mano_obra=grupo_destino)
        return redirect("reportes:mano_obra_area_clasificacion")

    # Productos (Receta) — sección "Productos" de Point.
    familias_ctx, grupos_existentes = _grupos_ctx(
        Receta,
        es_grupo_insumo=False,
        filtro_base=Receta.objects.exclude(familia=""),
        detectar_unidad=False,
    )

    # Catálogos (Insumo tipo interno) — sección "Catálogos" de Point.
    # Solo insumos con producción real: de ahí se puede inferir la unidad
    # (kg/lt/pza) que usa Point para calibrar.
    catalogos_ctx, grupos_insumo_existentes = _grupos_ctx(
        Insumo,
        es_grupo_insumo=True,
        filtro_base=Insumo.objects.filter(
            tipo_item=Insumo.TIPO_INTERNO,
            id__in=PointProductionLine.objects.exclude(insumo__isnull=True).values_list("insumo_id", flat=True),
        ),
        detectar_unidad=True,
    )

    return render(
        request,
        "reportes/mano_obra_area_clasificacion.html",
        {
            "familias": familias_ctx,
            "catalogos": catalogos_ctx,
            "areas": RecetaAreaProduccion.AREA_CHOICES,
            "grupos_existentes": grupos_existentes,
            "grupos_insumo_existentes": grupos_insumo_existentes,
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
