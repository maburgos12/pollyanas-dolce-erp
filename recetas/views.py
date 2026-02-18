from decimal import Decimal
from typing import Dict, Any, List

from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.db.models import Count, Q
from django.core.paginator import Paginator
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from maestros.models import Insumo, UnidadMedida
from .models import Receta, LineaReceta

@login_required
def recetas_list(request: HttpRequest) -> HttpResponse:
    q = request.GET.get("q", "").strip()
    estado = request.GET.get("estado", "").strip().lower()
    recetas = Receta.objects.all().annotate(
        pendientes_count=Count(
            "lineas",
            filter=Q(lineas__match_status=LineaReceta.STATUS_NEEDS_REVIEW),
        ),
        lineas_count=Count("lineas"),
    )
    if q:
        recetas = recetas.filter(nombre__icontains=q)
    if estado == "pendientes":
        recetas = recetas.filter(pendientes_count__gt=0)
    elif estado == "ok":
        recetas = recetas.filter(pendientes_count=0)
    recetas = recetas.order_by("nombre")

    total_recetas = recetas.count()
    total_pendientes = recetas.filter(pendientes_count__gt=0).count()
    total_lineas = sum(r.lineas_count for r in recetas)

    paginator = Paginator(recetas, 20)
    page = paginator.get_page(request.GET.get("page"))
    return render(
        request,
        "recetas/recetas_list.html",
        {
            "page": page,
            "q": q,
            "estado": estado,
            "total_recetas": total_recetas,
            "total_pendientes": total_pendientes,
            "total_lineas": total_lineas,
        },
    )

@login_required
def receta_detail(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    lineas = receta.lineas.select_related("insumo").order_by("posicion")
    total_lineas = lineas.count()
    total_match = lineas.filter(insumo__isnull=False).count()
    total_sin_match = total_lineas - total_match
    return render(
        request,
        "recetas/receta_detail.html",
        {
            "receta": receta,
            "lineas": lineas,
            "total_lineas": total_lineas,
            "total_match": total_match,
            "total_sin_match": total_sin_match,
        },
    )


@permission_required("recetas.change_receta", raise_exception=True)
@require_POST
def receta_update(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    nombre = (request.POST.get("nombre") or "").strip()
    sheet_name = (request.POST.get("sheet_name") or "").strip()
    if not nombre:
        messages.error(request, "El nombre de receta es obligatorio.")
        return redirect("recetas:receta_detail", pk=pk)
    receta.nombre = nombre[:250]
    receta.sheet_name = sheet_name[:120]
    receta.save()
    messages.success(request, "Receta actualizada.")
    return redirect("recetas:receta_detail", pk=pk)


def _to_decimal_or_none(value: str | None) -> Decimal | None:
    if value is None:
        return None
    raw = str(value).strip().replace(",", ".")
    if raw == "":
        return None
    try:
        return Decimal(raw)
    except Exception:
        return None


def _linea_form_context(receta: Receta, linea: LineaReceta | None = None) -> Dict[str, Any]:
    return {
        "receta": receta,
        "linea": linea,
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:800],
        "unidades": UnidadMedida.objects.order_by("codigo"),
    }


@permission_required("recetas.change_lineareceta", raise_exception=True)
def linea_edit(request: HttpRequest, pk: int, linea_id: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    linea = get_object_or_404(LineaReceta, pk=linea_id, receta=receta)

    if request.method == "POST":
        insumo_id = request.POST.get("insumo_id")
        unidad_id = request.POST.get("unidad_id")
        linea.insumo_texto = (request.POST.get("insumo_texto") or "").strip()[:250]
        linea.unidad_texto = (request.POST.get("unidad_texto") or "").strip()[:40]
        linea.cantidad = _to_decimal_or_none(request.POST.get("cantidad"))
        linea.costo_linea_excel = _to_decimal_or_none(request.POST.get("costo_linea_excel"))
        linea.posicion = int(request.POST.get("posicion") or linea.posicion)
        linea.insumo = Insumo.objects.filter(pk=insumo_id).first() if insumo_id else None
        linea.unidad = UnidadMedida.objects.filter(pk=unidad_id).first() if unidad_id else None

        if linea.insumo:
            linea.match_status = LineaReceta.STATUS_AUTO
            linea.match_method = "MANUAL"
            linea.match_score = 100.0
            linea.aprobado_por = request.user
            linea.aprobado_en = timezone.now()
        else:
            linea.match_status = LineaReceta.STATUS_REJECTED
            linea.match_method = LineaReceta.MATCH_NONE
            linea.match_score = 0.0

        linea.save()
        messages.success(request, "Línea actualizada.")
        return redirect("recetas:receta_detail", pk=pk)

    return render(request, "recetas/linea_form.html", _linea_form_context(receta, linea))


@permission_required("recetas.add_lineareceta", raise_exception=True)
def linea_create(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    if request.method == "POST":
        insumo_id = request.POST.get("insumo_id")
        unidad_id = request.POST.get("unidad_id")
        posicion_default = (receta.lineas.order_by("-posicion").first().posicion + 1) if receta.lineas.exists() else 1
        linea = LineaReceta(
            receta=receta,
            posicion=int(request.POST.get("posicion") or posicion_default),
            insumo_texto=(request.POST.get("insumo_texto") or "").strip()[:250],
            unidad_texto=(request.POST.get("unidad_texto") or "").strip()[:40],
            cantidad=_to_decimal_or_none(request.POST.get("cantidad")),
            costo_linea_excel=_to_decimal_or_none(request.POST.get("costo_linea_excel")),
            insumo=Insumo.objects.filter(pk=insumo_id).first() if insumo_id else None,
            unidad=UnidadMedida.objects.filter(pk=unidad_id).first() if unidad_id else None,
        )
        if linea.insumo:
            linea.match_status = LineaReceta.STATUS_AUTO
            linea.match_method = "MANUAL"
            linea.match_score = 100.0
            linea.aprobado_por = request.user
            linea.aprobado_en = timezone.now()
        else:
            linea.match_status = LineaReceta.STATUS_REJECTED
            linea.match_method = LineaReceta.MATCH_NONE
            linea.match_score = 0.0
        linea.save()
        messages.success(request, "Línea agregada.")
        return redirect("recetas:receta_detail", pk=pk)
    return render(request, "recetas/linea_form.html", _linea_form_context(receta))


@permission_required("recetas.delete_lineareceta", raise_exception=True)
@require_POST
def linea_delete(request: HttpRequest, pk: int, linea_id: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    linea = get_object_or_404(LineaReceta, pk=linea_id, receta=receta)
    linea.delete()
    messages.success(request, "Línea eliminada.")
    return redirect("recetas:receta_detail", pk=pk)

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
