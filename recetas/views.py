import csv
from io import BytesIO
from datetime import date
from decimal import Decimal
from typing import Dict, Any, List
from urllib.parse import urlencode

from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.db.models import Count, Q, OuterRef, Subquery, Case, When, Value, IntegerField
from django.core.paginator import Paginator
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.urls import reverse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST
from openpyxl import Workbook

from compras.models import OrdenCompra, SolicitudCompra
from core.access import can_manage_compras
from core.audit import log_event
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from .models import Receta, LineaReceta, RecetaPresentacion, PlanProduccion, PlanProduccionItem
from .utils.derived_insumos import sync_presentacion_insumo, sync_receta_derivados

@login_required
def recetas_list(request: HttpRequest) -> HttpResponse:
    q = request.GET.get("q", "").strip()
    estado = request.GET.get("estado", "").strip().lower()
    recetas = Receta.objects.select_related("rendimiento_unidad").all().annotate(
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
    lineas = list(receta.lineas.select_related("insumo").order_by("posicion"))
    presentaciones = receta.presentaciones.all().order_by("nombre")
    total_lineas = len(lineas)
    total_match = sum(1 for l in lineas if l.match_status == LineaReceta.STATUS_AUTO)
    total_revision = sum(1 for l in lineas if l.match_status == LineaReceta.STATUS_NEEDS_REVIEW)
    total_sin_match = sum(1 for l in lineas if l.match_status == LineaReceta.STATUS_REJECTED)
    total_costo_estimado = sum((l.costo_total_estimado or 0.0) for l in lineas)
    return render(
        request,
        "recetas/receta_detail.html",
        {
            "receta": receta,
            "lineas": lineas,
            "presentaciones": presentaciones,
            "total_lineas": total_lineas,
            "total_match": total_match,
            "total_revision": total_revision,
            "total_sin_match": total_sin_match,
            "total_costo_estimado": total_costo_estimado,
            "unidades": UnidadMedida.objects.order_by("codigo"),
            "tipo_choices": Receta.TIPO_CHOICES,
            "costo_por_kg_estimado": receta.costo_por_kg_estimado,
            "linea_tipo_choices": LineaReceta.TIPO_CHOICES,
        },
    )


@permission_required("recetas.change_receta", raise_exception=True)
@require_POST
def receta_update(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    nombre = (request.POST.get("nombre") or "").strip()
    codigo_point = (request.POST.get("codigo_point") or "").strip()
    sheet_name = (request.POST.get("sheet_name") or "").strip()
    tipo = (request.POST.get("tipo") or Receta.TIPO_PREPARACION).strip()
    usa_presentaciones = request.POST.get("usa_presentaciones") == "on"
    rendimiento_cantidad = _to_decimal_or_none(request.POST.get("rendimiento_cantidad"))
    rendimiento_unidad_id = request.POST.get("rendimiento_unidad_id")

    if not nombre:
        messages.error(request, "El nombre de receta es obligatorio.")
        return redirect("recetas:receta_detail", pk=pk)

    if tipo not in {Receta.TIPO_PREPARACION, Receta.TIPO_PRODUCTO_FINAL}:
        tipo = Receta.TIPO_PREPARACION

    receta.nombre = nombre[:250]
    receta.codigo_point = codigo_point[:80]
    receta.sheet_name = sheet_name[:120]
    receta.tipo = tipo
    receta.usa_presentaciones = usa_presentaciones
    receta.rendimiento_cantidad = rendimiento_cantidad
    receta.rendimiento_unidad = UnidadMedida.objects.filter(pk=rendimiento_unidad_id).first() if rendimiento_unidad_id else None
    receta.save()
    _sync_derived_insumos_safe(request, receta)
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


def _to_non_negative_decimal_or_none(value: str | None) -> Decimal | None:
    if value is None:
        return None
    raw = str(value).strip().replace(",", ".")
    if raw == "":
        return None
    try:
        num = Decimal(raw)
        return num if num >= 0 else None
    except Exception:
        return None


def _linea_form_context(receta: Receta, linea: LineaReceta | None = None) -> Dict[str, Any]:
    latest_cost_subquery = (
        CostoInsumo.objects.filter(insumo=OuterRef("pk"))
        .order_by("-fecha", "-id")
        .values("costo_unitario")[:1]
    )
    insumos_qs = (
        Insumo.objects.filter(activo=True)
        .select_related("unidad_base")
        .annotate(
            latest_costo_unitario=Subquery(latest_cost_subquery),
            origen_orden=Case(
                When(codigo__startswith="DERIVADO:RECETA:", then=Value(0)),
                default=Value(1),
                output_field=IntegerField(),
            ),
        )
        .order_by("origen_orden", "nombre")
    )
    insumos = list(insumos_qs[:1200])
    return {
        "receta": receta,
        "linea": linea,
        "insumos": insumos,
        "unidades": UnidadMedida.objects.order_by("codigo"),
        "linea_tipo_choices": LineaReceta.TIPO_CHOICES,
    }


def _latest_cost_for_insumo(insumo: Insumo | None) -> Decimal | None:
    if not insumo:
        return None
    cost = (
        CostoInsumo.objects.filter(insumo=insumo)
        .order_by("-fecha", "-id")
        .values_list("costo_unitario", flat=True)
        .first()
    )
    return Decimal(str(cost)) if cost is not None else None


def _switch_line_to_internal_cost(linea: LineaReceta) -> None:
    # Si ya hay cantidad + insumo, dejamos de usar costo fijo de Excel y
    # pasamos a costo dinámico interno (cantidad * costo_unitario_snapshot).
    if not linea.insumo:
        return
    if linea.cantidad is None or linea.cantidad <= 0:
        return

    if linea.costo_unitario_snapshot is None or linea.costo_unitario_snapshot <= 0:
        latest = _latest_cost_for_insumo(linea.insumo)
        if latest is not None and latest > 0:
            linea.costo_unitario_snapshot = latest
        elif linea.costo_linea_excel is not None and linea.costo_linea_excel > 0:
            try:
                linea.costo_unitario_snapshot = linea.costo_linea_excel / linea.cantidad
            except Exception:
                pass

    if linea.costo_unitario_snapshot is not None and linea.costo_unitario_snapshot > 0:
        linea.costo_linea_excel = None


def _autofill_unidad_from_insumo(linea: LineaReceta) -> None:
    if not linea.insumo:
        return
    if linea.unidad is None and linea.insumo.unidad_base is not None:
        linea.unidad = linea.insumo.unidad_base
    if linea.unidad and not (linea.unidad_texto or "").strip():
        linea.unidad_texto = linea.unidad.codigo


def _sync_derived_insumos_safe(request: HttpRequest, receta: Receta) -> None:
    try:
        sync_receta_derivados(receta)
    except Exception:
        messages.warning(
            request,
            "La receta se guardó, pero falló la sincronización automática de costos/insumos derivados.",
        )


@permission_required("recetas.change_lineareceta", raise_exception=True)
def linea_edit(request: HttpRequest, pk: int, linea_id: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    linea = get_object_or_404(LineaReceta, pk=linea_id, receta=receta)

    if request.method == "POST":
        insumo_id = request.POST.get("insumo_id")
        unidad_id = request.POST.get("unidad_id")
        tipo_linea = (request.POST.get("tipo_linea") or LineaReceta.TIPO_NORMAL).strip()
        if tipo_linea not in {LineaReceta.TIPO_NORMAL, LineaReceta.TIPO_SUBSECCION}:
            tipo_linea = LineaReceta.TIPO_NORMAL
        linea.insumo_texto = (request.POST.get("insumo_texto") or "").strip()[:250]
        linea.unidad_texto = (request.POST.get("unidad_texto") or "").strip()[:40]
        linea.etapa = (request.POST.get("etapa") or "").strip()[:120]
        linea.tipo_linea = tipo_linea
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
        elif tipo_linea == LineaReceta.TIPO_SUBSECCION:
            linea.match_status = LineaReceta.STATUS_AUTO
            linea.match_method = LineaReceta.MATCH_SUBSECTION
            linea.match_score = 100.0
            linea.aprobado_por = request.user
            linea.aprobado_en = timezone.now()
        else:
            linea.match_status = LineaReceta.STATUS_REJECTED
            linea.match_method = LineaReceta.MATCH_NONE
            linea.match_score = 0.0

        _autofill_unidad_from_insumo(linea)
        _switch_line_to_internal_cost(linea)
        linea.save()
        _sync_derived_insumos_safe(request, receta)
        messages.success(request, "Línea actualizada.")
        return redirect("recetas:receta_detail", pk=pk)

    return render(request, "recetas/linea_form.html", _linea_form_context(receta, linea))


@permission_required("recetas.add_lineareceta", raise_exception=True)
def linea_create(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    if request.method == "POST":
        insumo_id = request.POST.get("insumo_id")
        unidad_id = request.POST.get("unidad_id")
        tipo_linea = (request.POST.get("tipo_linea") or LineaReceta.TIPO_NORMAL).strip()
        if tipo_linea not in {LineaReceta.TIPO_NORMAL, LineaReceta.TIPO_SUBSECCION}:
            tipo_linea = LineaReceta.TIPO_NORMAL
        posicion_default = (receta.lineas.order_by("-posicion").first().posicion + 1) if receta.lineas.exists() else 1
        linea = LineaReceta(
            receta=receta,
            posicion=int(request.POST.get("posicion") or posicion_default),
            tipo_linea=tipo_linea,
            etapa=(request.POST.get("etapa") or "").strip()[:120],
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
        elif tipo_linea == LineaReceta.TIPO_SUBSECCION:
            linea.match_status = LineaReceta.STATUS_AUTO
            linea.match_method = LineaReceta.MATCH_SUBSECTION
            linea.match_score = 100.0
            linea.aprobado_por = request.user
            linea.aprobado_en = timezone.now()
        else:
            linea.match_status = LineaReceta.STATUS_REJECTED
            linea.match_method = LineaReceta.MATCH_NONE
            linea.match_score = 0.0

        _autofill_unidad_from_insumo(linea)
        _switch_line_to_internal_cost(linea)
        linea.save()
        _sync_derived_insumos_safe(request, receta)
        messages.success(request, "Línea agregada.")
        return redirect("recetas:receta_detail", pk=pk)
    return render(request, "recetas/linea_form.html", _linea_form_context(receta))


@permission_required("recetas.delete_lineareceta", raise_exception=True)
@require_POST
def linea_delete(request: HttpRequest, pk: int, linea_id: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    linea = get_object_or_404(LineaReceta, pk=linea_id, receta=receta)
    linea.delete()
    _sync_derived_insumos_safe(request, receta)
    messages.success(request, "Línea eliminada.")
    return redirect("recetas:receta_detail", pk=pk)


@permission_required("recetas.change_receta", raise_exception=True)
def presentacion_create(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    if request.method == "POST":
        nombre = (request.POST.get("nombre") or "").strip()
        peso_por_unidad_kg = _to_decimal_or_none(request.POST.get("peso_por_unidad_kg"))
        unidades_por_batch = request.POST.get("unidades_por_batch") or None
        unidades_por_pastel = request.POST.get("unidades_por_pastel") or None
        activo = request.POST.get("activo") == "on"
        if not nombre:
            messages.error(request, "El nombre de la presentación es obligatorio.")
            return redirect("recetas:presentacion_create", pk=pk)
        if not peso_por_unidad_kg or peso_por_unidad_kg <= 0:
            messages.error(request, "Peso por unidad (kg) debe ser mayor que cero.")
            return redirect("recetas:presentacion_create", pk=pk)

        presentacion, _ = RecetaPresentacion.objects.update_or_create(
            receta=receta,
            nombre=nombre[:80],
            defaults={
                "peso_por_unidad_kg": peso_por_unidad_kg,
                "unidades_por_batch": _to_non_negative_decimal_or_none(unidades_por_batch),
                "unidades_por_pastel": _to_non_negative_decimal_or_none(unidades_por_pastel),
                "activo": activo,
            },
        )
        if not receta.usa_presentaciones:
            receta.usa_presentaciones = True
            receta.save(update_fields=["usa_presentaciones"])
        _sync_derived_insumos_safe(request, receta)
        messages.success(request, "Presentación guardada.")
        return redirect("recetas:receta_detail", pk=pk)

    return render(
        request,
        "recetas/presentacion_form.html",
        {"receta": receta, "presentacion": None},
    )


@permission_required("recetas.change_receta", raise_exception=True)
def presentacion_edit(request: HttpRequest, pk: int, presentacion_id: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    presentacion = get_object_or_404(RecetaPresentacion, pk=presentacion_id, receta=receta)
    if request.method == "POST":
        nombre = (request.POST.get("nombre") or "").strip()
        peso_por_unidad_kg = _to_decimal_or_none(request.POST.get("peso_por_unidad_kg"))
        unidades_por_batch = request.POST.get("unidades_por_batch") or None
        unidades_por_pastel = request.POST.get("unidades_por_pastel") or None
        activo = request.POST.get("activo") == "on"
        if not nombre:
            messages.error(request, "El nombre de la presentación es obligatorio.")
            return redirect("recetas:presentacion_edit", pk=pk, presentacion_id=presentacion_id)
        if not peso_por_unidad_kg or peso_por_unidad_kg <= 0:
            messages.error(request, "Peso por unidad (kg) debe ser mayor que cero.")
            return redirect("recetas:presentacion_edit", pk=pk, presentacion_id=presentacion_id)

        presentacion.nombre = nombre[:80]
        presentacion.peso_por_unidad_kg = peso_por_unidad_kg
        presentacion.unidades_por_batch = _to_non_negative_decimal_or_none(unidades_por_batch)
        presentacion.unidades_por_pastel = _to_non_negative_decimal_or_none(unidades_por_pastel)
        presentacion.activo = activo
        presentacion.save()
        _sync_derived_insumos_safe(request, receta)
        messages.success(request, "Presentación actualizada.")
        return redirect("recetas:receta_detail", pk=pk)

    return render(
        request,
        "recetas/presentacion_form.html",
        {"receta": receta, "presentacion": presentacion},
    )


@permission_required("recetas.change_receta", raise_exception=True)
@require_POST
def presentacion_delete(request: HttpRequest, pk: int, presentacion_id: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    presentacion = get_object_or_404(RecetaPresentacion, pk=presentacion_id, receta=receta)
    try:
        sync_presentacion_insumo(presentacion, deactivate=True)
    except Exception:
        messages.warning(
            request,
            "La presentación se eliminó, pero falló la desactivación del insumo derivado.",
        )
    presentacion.delete()
    messages.success(request, "Presentación eliminada.")
    return redirect("recetas:receta_detail", pk=pk)

@permission_required("recetas.change_lineareceta", raise_exception=True)
def matching_pendientes(request: HttpRequest) -> HttpResponse:
    q = request.GET.get("q", "").strip()
    pendientes = LineaReceta.objects.filter(match_status=LineaReceta.STATUS_NEEDS_REVIEW).select_related("receta", "insumo").order_by("receta__nombre", "posicion")
    if q:
        pendientes = pendientes.filter(insumo_texto__icontains=q)
    paginator = Paginator(pendientes, 25)
    page = paginator.get_page(request.GET.get("page"))
    return render(request, "recetas/matching_pendientes.html", {"page": page, "q": q})


@permission_required("recetas.change_lineareceta", raise_exception=True)
def matching_insumos_search(request: HttpRequest) -> JsonResponse:
    q = (request.GET.get("q") or "").strip()
    limit_raw = (request.GET.get("limit") or "20").strip()
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 20
    limit = max(1, min(limit, 100))

    queryset = Insumo.objects.filter(activo=True)
    if q:
        queryset = queryset.filter(nombre__icontains=q)
    items = list(queryset.order_by("nombre").values("id", "nombre")[:limit])
    return JsonResponse({"results": items, "count": len(items)})

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


def _linea_unit_code(linea: LineaReceta) -> str:
    if linea.unidad_id and linea.unidad:
        return linea.unidad.codigo
    txt = (linea.unidad_texto or "").strip()
    if txt:
        return txt
    if linea.insumo_id and linea.insumo and linea.insumo.unidad_base_id and linea.insumo.unidad_base:
        return linea.insumo.unidad_base.codigo
    return "-"


def _linea_unit_cost(linea: LineaReceta, unit_cost_cache: Dict[int, Decimal | None]) -> Decimal | None:
    if not linea.insumo_id:
        return None
    if linea.costo_unitario_snapshot is not None and linea.costo_unitario_snapshot > 0:
        return Decimal(str(linea.costo_unitario_snapshot))

    if linea.insumo_id in unit_cost_cache:
        return unit_cost_cache[linea.insumo_id]

    latest = _latest_cost_for_insumo(linea.insumo)
    unit_cost_cache[linea.insumo_id] = latest
    return latest


def _plan_explosion(plan: PlanProduccion) -> Dict[str, Any]:
    items = (
        plan.items.select_related("receta")
        .prefetch_related(
            "receta__lineas__insumo__unidad_base",
            "receta__lineas__insumo__proveedor_principal",
            "receta__lineas__unidad",
        )
        .order_by("id")
    )

    unit_cost_cache: Dict[int, Decimal | None] = {}
    insumos_map: Dict[int, Dict[str, Any]] = {}
    items_detalle: List[Dict[str, Any]] = []
    lineas_sin_cantidad: set[str] = set()
    lineas_sin_costo_unitario: set[str] = set()
    lineas_sin_match = 0

    for item in items:
        multiplicador = Decimal(str(item.cantidad or 0))
        item_total = Decimal("0")
        if multiplicador <= 0:
            continue

        for linea in item.receta.lineas.all():
            if not linea.insumo_id:
                lineas_sin_match += 1
                continue

            qty_base = Decimal(str(linea.cantidad or 0))
            if qty_base <= 0:
                lineas_sin_cantidad.add(f"{item.receta.nombre}: {linea.insumo_texto}")
                continue

            qty = qty_base * multiplicador
            if qty <= 0:
                continue

            unit_code = _linea_unit_code(linea)
            unit_cost = _linea_unit_cost(linea, unit_cost_cache)
            costo_linea = Decimal("0")
            if unit_cost is not None and unit_cost > 0:
                costo_linea = qty * unit_cost
            else:
                lineas_sin_costo_unitario.add(f"{item.receta.nombre}: {linea.insumo_texto}")

            key = linea.insumo_id
            if key not in insumos_map:
                insumo_obj = linea.insumo
                origen = "Interno" if (insumo_obj.codigo or "").startswith("DERIVADO:RECETA:") else "Materia prima"
                proveedor_sugerido = "-"
                if insumo_obj.proveedor_principal_id and insumo_obj.proveedor_principal:
                    proveedor_sugerido = insumo_obj.proveedor_principal.nombre
                insumos_map[key] = {
                    "insumo_id": key,
                    "nombre": insumo_obj.nombre,
                    "origen": origen,
                    "proveedor_sugerido": proveedor_sugerido,
                    "unidad": unit_code,
                    "cantidad": Decimal("0"),
                    "costo_total": Decimal("0"),
                    "costo_unitario": unit_cost or Decimal("0"),
                }

            insumos_map[key]["cantidad"] += qty
            insumos_map[key]["costo_total"] += costo_linea
            item_total += costo_linea

        items_detalle.append(
            {
                "id": item.id,
                "receta": item.receta,
                "cantidad": multiplicador,
                "notas": item.notas,
                "costo_estimado": item_total,
            }
        )

    insumos = sorted(insumos_map.values(), key=lambda x: x["nombre"].lower())
    costo_total = sum((row["costo_total"] for row in insumos), Decimal("0"))

    return {
        "items_detalle": items_detalle,
        "insumos": insumos,
        "costo_total": costo_total,
        "lineas_sin_cantidad": sorted(lineas_sin_cantidad),
        "lineas_sin_costo_unitario": sorted(lineas_sin_costo_unitario),
        "lineas_sin_match": lineas_sin_match,
    }


def _export_plan_csv(plan: PlanProduccion, explosion: Dict[str, Any]) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    filename = f"plan_produccion_{plan.id}_{plan.fecha_produccion}.csv"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    writer = csv.writer(response)

    writer.writerow(["PLAN DE PRODUCCION", plan.nombre])
    writer.writerow(["Fecha", plan.fecha_produccion.isoformat()])
    writer.writerow(["Costo total estimado", str(explosion["costo_total"])])
    writer.writerow([])
    writer.writerow(["PRODUCTOS EN PLAN"])
    writer.writerow(["Receta", "Cantidad", "Notas", "Costo estimado"])
    for row in explosion["items_detalle"]:
        writer.writerow(
            [
                row["receta"].nombre,
                f"{Decimal(str(row['cantidad'])):.3f}",
                row["notas"] or "",
                f"{Decimal(str(row['costo_estimado'])):.2f}",
            ]
        )

    writer.writerow([])
    writer.writerow(["INSUMOS CONSOLIDADOS"])
    writer.writerow(["Insumo", "Origen", "Proveedor sugerido", "Cantidad requerida", "Unidad", "Costo unitario", "Costo total"])
    for row in explosion["insumos"]:
        writer.writerow(
            [
                row["nombre"],
                row["origen"],
                row.get("proveedor_sugerido") or "-",
                f"{Decimal(str(row['cantidad'])):.3f}",
                row["unidad"],
                f"{Decimal(str(row['costo_unitario'])):.2f}",
                f"{Decimal(str(row['costo_total'])):.2f}",
            ]
        )

    return response


def _export_plan_xlsx(plan: PlanProduccion, explosion: Dict[str, Any]) -> HttpResponse:
    wb = Workbook()
    ws_resumen = wb.active
    ws_resumen.title = "Resumen"
    ws_resumen.append(["Plan", plan.nombre])
    ws_resumen.append(["Fecha", plan.fecha_produccion.isoformat()])
    ws_resumen.append(["Costo total estimado", float(explosion["costo_total"] or 0)])
    ws_resumen.append([])
    ws_resumen.append(["Productos", len(explosion["items_detalle"])])
    ws_resumen.append(["Insumos", len(explosion["insumos"])])

    ws_productos = wb.create_sheet("Productos")
    ws_productos.append(["Receta", "Cantidad", "Notas", "Costo estimado"])
    for row in explosion["items_detalle"]:
        ws_productos.append(
            [
                row["receta"].nombre,
                float(row["cantidad"] or 0),
                row["notas"] or "",
                float(row["costo_estimado"] or 0),
            ]
        )

    ws_insumos = wb.create_sheet("Insumos")
    ws_insumos.append(["Insumo", "Origen", "Proveedor sugerido", "Cantidad requerida", "Unidad", "Costo unitario", "Costo total"])
    for row in explosion["insumos"]:
        ws_insumos.append(
            [
                row["nombre"],
                row["origen"],
                row.get("proveedor_sugerido") or "-",
                float(row["cantidad"] or 0),
                row["unidad"],
                float(row["costo_unitario"] or 0),
                float(row["costo_total"] or 0),
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    filename = f"plan_produccion_{plan.id}_{plan.fecha_produccion}.xlsx"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def _export_plan_point_xlsx(plan: PlanProduccion) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Importacion Produccion"
    ws.append(["Código", "Nombre", "Cantidad Solicitada", "Is Insumo"])

    items = plan.items.select_related("receta").order_by("id")
    for item in items:
        receta = item.receta
        ws.append(
            [
                receta.codigo_point or "",
                receta.nombre,
                float(item.cantidad or 0),
                1 if receta.tipo == Receta.TIPO_PREPARACION else 0,
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    filename = f"point_plan_produccion_{plan.id}_{plan.fecha_produccion}.xlsx"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@permission_required("recetas.view_planproduccion", raise_exception=True)
def plan_produccion(request: HttpRequest) -> HttpResponse:
    planes = PlanProduccion.objects.select_related("creado_por").prefetch_related("items").order_by("-fecha_produccion", "-id")
    plan_actual = None
    plan_id = request.GET.get("plan_id")
    if plan_id:
        plan_actual = get_object_or_404(PlanProduccion, pk=plan_id)
    elif planes.exists():
        plan_actual = planes.first()

    recetas_disponibles = Receta.objects.order_by("tipo", "nombre")
    explosion = _plan_explosion(plan_actual) if plan_actual else None
    return render(
        request,
        "recetas/plan_produccion.html",
        {
            "planes": planes[:30],
            "plan_actual": plan_actual,
            "recetas_disponibles": recetas_disponibles,
            "explosion": explosion,
        },
    )


@permission_required("recetas.view_planproduccion", raise_exception=True)
def plan_produccion_export(request: HttpRequest, plan_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    export_format = (request.GET.get("format") or "csv").lower()
    if export_format in {"point", "point-xlsx", "point_xlsx"}:
        return _export_plan_point_xlsx(plan)
    explosion = _plan_explosion(plan)
    if export_format == "xlsx":
        return _export_plan_xlsx(plan, explosion)
    return _export_plan_csv(plan, explosion)


@permission_required("recetas.view_planproduccion", raise_exception=True)
def plan_produccion_solicitud_print(request: HttpRequest, plan_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    explosion = _plan_explosion(plan)
    materias_primas = [row for row in explosion["insumos"] if row["origen"] != "Interno"]
    internos = [row for row in explosion["insumos"] if row["origen"] == "Interno"]
    folio = f"SOL-{plan.fecha_produccion.strftime('%Y%m%d')}-{plan.id:04d}"
    return render(
        request,
        "recetas/solicitud_insumos_print.html",
        {
            "plan": plan,
            "explosion": explosion,
            "materias_primas": materias_primas,
            "internos": internos,
            "folio": folio,
            "titulo_documento": "Orden de Solicitud de Insumos",
            "subtitulo_documento": "Documento interno de abastecimiento",
            "mostrar_internos": True,
        },
    )


@permission_required("recetas.view_planproduccion", raise_exception=True)
def plan_produccion_solicitud_compras_print(request: HttpRequest, plan_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    explosion = _plan_explosion(plan)
    materias_primas = [row for row in explosion["insumos"] if row["origen"] != "Interno"]
    folio = f"SOL-COMP-{plan.fecha_produccion.strftime('%Y%m%d')}-{plan.id:04d}"
    return render(
        request,
        "recetas/solicitud_insumos_print.html",
        {
            "plan": plan,
            "explosion": explosion,
            "materias_primas": materias_primas,
            "internos": [],
            "folio": folio,
            "titulo_documento": "Orden de Solicitud de Materia Prima",
            "subtitulo_documento": "Documento para área de Compras",
            "mostrar_internos": False,
        },
    )


@permission_required("recetas.add_planproduccion", raise_exception=True)
@require_POST
def plan_produccion_create(request: HttpRequest) -> HttpResponse:
    nombre = (request.POST.get("nombre") or "").strip()
    fecha_str = (request.POST.get("fecha_produccion") or "").strip()
    notas = (request.POST.get("notas") or "").strip()
    if not nombre:
        messages.error(request, "El nombre del plan es obligatorio.")
        return redirect("recetas:plan_produccion")

    fecha = timezone.localdate()
    if fecha_str:
        try:
            fecha = date.fromisoformat(fecha_str)
        except Exception:
            messages.warning(request, "Fecha inválida. Se usó la fecha de hoy.")

    plan = PlanProduccion.objects.create(
        nombre=nombre[:140],
        fecha_produccion=fecha,
        notas=notas,
        creado_por=request.user if request.user.is_authenticated else None,
    )
    messages.success(request, "Plan de producción creado.")
    return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")


@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def plan_produccion_item_create(request: HttpRequest, plan_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    receta_id = request.POST.get("receta_id")
    cantidad_raw = (request.POST.get("cantidad") or "1").strip()
    notas = (request.POST.get("notas") or "").strip()[:160]

    receta = Receta.objects.filter(pk=receta_id).first()
    if not receta:
        messages.error(request, "Selecciona una receta válida.")
        return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")

    try:
        cantidad = Decimal(cantidad_raw)
    except Exception:
        cantidad = Decimal("0")
    if cantidad <= 0:
        messages.error(request, "La cantidad debe ser mayor que cero.")
        return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")

    PlanProduccionItem.objects.create(
        plan=plan,
        receta=receta,
        cantidad=cantidad,
        notas=notas,
    )
    messages.success(request, "Producto agregado al plan.")
    return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")


@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def plan_produccion_item_delete(request: HttpRequest, plan_id: int, item_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    item = get_object_or_404(PlanProduccionItem, pk=item_id, plan=plan)
    item.delete()
    messages.success(request, "Renglón eliminado del plan.")
    return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")


@permission_required("recetas.delete_planproduccion", raise_exception=True)
@require_POST
def plan_produccion_delete(request: HttpRequest, plan_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    plan.delete()
    messages.success(request, "Plan eliminado.")
    return redirect("recetas:plan_produccion")


@login_required
@require_POST
def plan_produccion_generar_solicitudes(request: HttpRequest, plan_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para generar solicitudes de compra.")

    explosion = _plan_explosion(plan)
    materias_primas = [row for row in explosion["insumos"] if row["origen"] != "Interno" and Decimal(str(row["cantidad"] or 0)) > 0]
    area_tag = f"PLAN_PRODUCCION:{plan.id}"
    referencia_plan = f"PLAN_PRODUCCION:{plan.id}"
    auto_create_oc = bool(request.POST.get("auto_create_oc"))
    replace_prev_raw = (request.POST.get("replace_prev") or "1").strip().lower()
    replace_prev = replace_prev_raw not in {"0", "false", "off", "no"}

    deleted_prev = 0
    if replace_prev:
        # Idempotencia operativa: si vuelven a generar, reemplazamos únicamente borradores del plan.
        borradores_previos = SolicitudCompra.objects.filter(
            area=area_tag,
            estatus=SolicitudCompra.STATUS_BORRADOR,
        )
        deleted_prev = borradores_previos.count()
        if deleted_prev:
            borradores_previos.delete()

    creadas = 0
    actualizadas = 0
    sin_proveedor = 0
    oc_por_proveedor: dict[int, dict[str, Any]] = {}
    for row in materias_primas:
        insumo = Insumo.objects.filter(pk=row["insumo_id"]).first()
        if not insumo:
            continue
        proveedor = insumo.proveedor_principal
        qty = Decimal(str(row["cantidad"]))

        solicitud = None
        if not replace_prev:
            solicitud = (
                SolicitudCompra.objects.filter(
                    area=area_tag,
                    estatus=SolicitudCompra.STATUS_BORRADOR,
                    insumo=insumo,
                    fecha_requerida=plan.fecha_produccion,
                )
                .order_by("-creado_en")
                .first()
            )

        if solicitud:
            solicitud.cantidad = Decimal(str(solicitud.cantidad or 0)) + qty
            if not solicitud.proveedor_sugerido_id and proveedor:
                solicitud.proveedor_sugerido = proveedor
                solicitud.save(update_fields=["cantidad", "proveedor_sugerido"])
            else:
                solicitud.save(update_fields=["cantidad"])
            log_event(
                request.user,
                "UPDATE",
                "compras.SolicitudCompra",
                solicitud.id,
                {
                    "folio": solicitud.folio,
                    "source_plan_id": plan.id,
                    "source_plan_nombre": plan.nombre,
                    "mode": "accumulate",
                },
            )
            actualizadas += 1
        else:
            solicitud = SolicitudCompra.objects.create(
                area=area_tag,
                solicitante=request.user.username,
                insumo=insumo,
                proveedor_sugerido=proveedor,
                cantidad=qty,
                fecha_requerida=plan.fecha_produccion,
                estatus=SolicitudCompra.STATUS_BORRADOR,
            )
            log_event(
                request.user,
                "CREATE",
                "compras.SolicitudCompra",
                solicitud.id,
                {
                    "folio": solicitud.folio,
                    "source_plan_id": plan.id,
                    "source_plan_nombre": plan.nombre,
                },
            )
            creadas += 1

        if auto_create_oc:
            if not proveedor:
                sin_proveedor += 1
                continue
            bucket = oc_por_proveedor.setdefault(
                proveedor.id,
                {
                    "proveedor_id": proveedor.id,
                    "proveedor_nombre": proveedor.nombre,
                    "monto_estimado": Decimal("0"),
                },
            )
            bucket["monto_estimado"] += Decimal(str(row.get("costo_total") or 0))

    oc_prev_deleted = 0
    oc_creadas = 0
    oc_actualizadas = 0
    if auto_create_oc:
        if replace_prev:
            ocs_previas = OrdenCompra.objects.filter(
                referencia=referencia_plan,
                estatus=OrdenCompra.STATUS_BORRADOR,
                solicitud__isnull=True,
            )
            oc_prev_deleted = ocs_previas.count()
            if oc_prev_deleted:
                ocs_previas.delete()

        for data in oc_por_proveedor.values():
            orden = None
            if not replace_prev:
                orden = (
                    OrdenCompra.objects.filter(
                        referencia=referencia_plan,
                        estatus=OrdenCompra.STATUS_BORRADOR,
                        solicitud__isnull=True,
                        proveedor_id=data["proveedor_id"],
                    )
                    .order_by("-creado_en")
                    .first()
                )

            if orden:
                orden.monto_estimado = Decimal(str(orden.monto_estimado or 0)) + data["monto_estimado"]
                orden.fecha_entrega_estimada = plan.fecha_produccion
                orden.save(update_fields=["monto_estimado", "fecha_entrega_estimada"])
                log_event(
                    request.user,
                    "UPDATE",
                    "compras.OrdenCompra",
                    orden.id,
                    {
                        "folio": orden.folio,
                        "estatus": orden.estatus,
                        "source_plan_id": plan.id,
                        "source_plan_nombre": plan.nombre,
                        "proveedor": data["proveedor_nombre"],
                        "mode": "accumulate",
                    },
                )
                oc_actualizadas += 1
            else:
                orden = OrdenCompra.objects.create(
                    solicitud=None,
                    referencia=referencia_plan,
                    proveedor_id=data["proveedor_id"],
                    fecha_emision=timezone.localdate(),
                    fecha_entrega_estimada=plan.fecha_produccion,
                    monto_estimado=data["monto_estimado"],
                    estatus=OrdenCompra.STATUS_BORRADOR,
                )
                log_event(
                    request.user,
                    "CREATE",
                    "compras.OrdenCompra",
                    orden.id,
                    {
                        "folio": orden.folio,
                        "estatus": orden.estatus,
                        "source_plan_id": plan.id,
                        "source_plan_nombre": plan.nombre,
                        "proveedor": data["proveedor_nombre"],
                    },
                )
                oc_creadas += 1

    if creadas == 0:
        messages.warning(
            request,
            "No se generaron solicitudes: el plan no tiene materia prima con cantidad válida.",
        )
    else:
        mode_label = "reemplazo" if replace_prev else "acumulado"
        msg = (
            f"Solicitudes generadas: {creadas}. "
            f"Solicitudes actualizadas: {actualizadas}. "
            f"Modo: {mode_label}. "
            f"Borradores reemplazados del plan: {deleted_prev}."
        )
        if auto_create_oc:
            msg += (
                f" OC borrador creadas (agrupadas por proveedor): {oc_creadas}. "
                f"OC borrador actualizadas: {oc_actualizadas}. "
                f"OCs borrador previas reemplazadas: {oc_prev_deleted}."
            )
            if sin_proveedor:
                msg += f" Insumos sin proveedor principal: {sin_proveedor} (no entraron a OC automática)."
        messages.success(request, msg)
    next_view = (request.POST.get("next_view") or "plan").strip().lower()
    compras_query = urlencode(
        {
            "source": "plan",
            "plan_id": str(plan.id),
            "reabasto": "all",
        }
    )
    if next_view == "compras":
        return redirect(f"{reverse('compras:solicitudes')}?{compras_query}")
    if next_view == "compras_print":
        return redirect(f"{reverse('compras:solicitudes_print')}?{compras_query}")
    return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")


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
