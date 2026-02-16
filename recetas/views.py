from collections import defaultdict
from decimal import Decimal
from typing import Dict, Any, List

from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.core.paginator import Paginator
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from maestros.models import Insumo
from .models import Receta, LineaReceta

@login_required
def recetas_list(request: HttpRequest) -> HttpResponse:
    q = request.GET.get("q", "").strip()
    recetas = Receta.objects.all()
    if q:
        recetas = recetas.filter(nombre__icontains=q)
    recetas = recetas.order_by("nombre")
    paginator = Paginator(recetas, 20)
    page = paginator.get_page(request.GET.get("page"))
    return render(request, "recetas/recetas_list.html", {"page": page, "q": q})

@login_required
def receta_detail(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    lineas = receta.lineas.select_related("insumo").all()
    return render(request, "recetas/receta_detail.html", {"receta": receta, "lineas": lineas})

@permission_required("recetas.change_lineareceta", raise_exception=True)
def matching_pendientes(request: HttpRequest) -> HttpResponse:
    q = request.GET.get("q", "").strip()
    pendientes = LineaReceta.objects.filter(match_status=LineaReceta.STATUS_NEEDS_REVIEW).select_related("receta", "insumo").order_by("receta__nombre", "posicion")
    if q:
        pendientes = pendientes.filter(insumo_texto__icontains=q)
    paginator = Paginator(pendientes, 25)
    page = paginator.get_page(request.GET.get("page"))

    # Insumos para dropdown (limitado). En producción: usar búsqueda/autocomplete.
    insumos = Insumo.objects.filter(activo=True).order_by("nombre")[:500]
    return render(request, "recetas/matching_pendientes.html", {"page": page, "q": q, "insumos": insumos})

@permission_required("recetas.change_lineareceta", raise_exception=True)
def aprobar_matching(request: HttpRequest, linea_id: int) -> HttpResponse:
    linea = get_object_or_404(LineaReceta, pk=linea_id)
    insumo_id = request.POST.get("insumo_id")
    if not insumo_id:
        messages.error(request, "Selecciona un insumo para aprobar.")
        return redirect("recetas:matching_pendientes")

    insumo = get_object_or_404(Insumo, pk=insumo_id)
    linea.insumo = insumo
    linea.match_status = LineaReceta.STATUS_AUTO
    linea.match_method = "MANUAL"
    linea.match_score = 100.0
    linea.aprobado_por = request.user
    linea.aprobado_en = timezone.now()
    linea.save()
    messages.success(request, f"Matching aprobado: {linea.insumo_texto} → {insumo.nombre}")
    return redirect("recetas:matching_pendientes")

@login_required
def mrp_form(request: HttpRequest) -> HttpResponse:
    recetas = Receta.objects.order_by("nombre")
    resultado = None
    if request.method == "POST":
        receta_id = request.POST.get("receta_id")
        mult = request.POST.get("multiplicador", "1").strip()
        try:
            multiplicador = Decimal(mult)
        except Exception:
            multiplicador = Decimal("1")

        receta = get_object_or_404(Receta, pk=receta_id)
        agregados: Dict[str, Dict[str, Any]] = {}

        for l in receta.lineas.select_related("insumo").all():
            key = l.insumo.nombre if l.insumo else f"(NO MATCH) {l.insumo_texto}"
            if key not in agregados:
                agregados[key] = {"insumo": l.insumo, "nombre": key, "cantidad": Decimal("0"), "unidad": l.unidad_texto, "costo": 0.0}
            qty = Decimal(str(l.cantidad or 0)) * multiplicador
            agregados[key]["cantidad"] += qty
            agregados[key]["costo"] += float(l.costo_total_estimado) * float(multiplicador)

        resultado = {
            "receta": receta,
            "multiplicador": multiplicador,
            "items": sorted(agregados.values(), key=lambda x: x["nombre"]),
            "costo_total": sum(i["costo"] for i in agregados.values()),
        }

    return render(request, "recetas/mrp.html", {"recetas": recetas, "resultado": resultado})
