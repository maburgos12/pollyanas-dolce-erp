from datetime import timedelta

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Sum, Max
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils import timezone

from core.access import can_view_reportes
from inventario.models import ExistenciaInsumo, MovimientoInventario


@login_required
def costo_receta(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    return render(request, "reportes/costo_receta.html")


@login_required
def consumo(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    today = timezone.localdate()
    default_from = today - timedelta(days=30)

    date_from = request.GET.get("date_from") or default_from.isoformat()
    date_to = request.GET.get("date_to") or today.isoformat()
    tipo = (request.GET.get("tipo") or "all").upper()
    valid_tipos = {"ALL", "CONSUMO", "SALIDA", "ENTRADA"}
    if tipo not in valid_tipos:
        tipo = "ALL"

    movimientos = MovimientoInventario.objects.select_related("insumo").filter(
        fecha__date__gte=date_from,
        fecha__date__lte=date_to,
    )
    if tipo != "ALL":
        movimientos = movimientos.filter(tipo=tipo)

    resumen = list(
        movimientos.values("insumo_id", "insumo__nombre")
        .annotate(
            cantidad_total=Sum("cantidad"),
            ultima_fecha=Max("fecha"),
        )
        .order_by("-cantidad_total", "insumo__nombre")
    )

    context = {
        "rows": resumen,
        "total_movimientos": movimientos.count(),
        "total_insumos": len(resumen),
        "total_cantidad": sum((row["cantidad_total"] or 0) for row in resumen),
        "filters": {
            "date_from": date_from,
            "date_to": date_to,
            "tipo": tipo,
        },
    }
    return render(request, "reportes/consumo.html", context)


@login_required
def faltantes(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    nivel = (request.GET.get("nivel") or "alerta").lower()
    valid_levels = {"alerta", "critico", "bajo", "all"}
    if nivel not in valid_levels:
        nivel = "alerta"

    existencias = list(
        ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base").order_by("insumo__nombre")[:500]
    )

    criticos_count = 0
    bajo_count = 0
    rows = []
    for e in existencias:
        stock = e.stock_actual
        reorden = e.punto_reorden
        if stock <= 0:
            e.criticidad = "Alta"
            e.criticidad_badge = "bg-danger"
            e.nivel = "critico"
            criticos_count += 1
        elif stock < reorden:
            e.criticidad = "Media"
            e.criticidad_badge = "bg-warning"
            e.nivel = "bajo"
            bajo_count += 1
        else:
            e.criticidad = "Sin riesgo"
            e.criticidad_badge = "bg-success"
            e.nivel = "ok"

        e.sugerencia_compra = max(reorden - stock, 0)

        include = False
        if nivel == "all":
            include = True
        elif nivel == "alerta":
            include = e.nivel in {"critico", "bajo"}
        else:
            include = e.nivel == nivel

        if include:
            rows.append(e)

    context = {
        "rows": rows,
        "nivel": nivel,
        "criticos_count": criticos_count,
        "bajo_count": bajo_count,
        "alertas_count": criticos_count + bajo_count,
    }
    return render(request, "reportes/faltantes.html", context)
