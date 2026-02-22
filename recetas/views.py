import csv
from io import BytesIO
from math import sqrt
from datetime import date, timedelta
from calendar import monthrange
from collections import defaultdict
from decimal import Decimal
from typing import Dict, Any, List
from urllib.parse import urlencode

from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.db import OperationalError, ProgrammingError
from django.db.models import Count, Q, OuterRef, Subquery, Case, When, Value, IntegerField, Sum
from django.core.paginator import Paginator
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.urls import reverse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST
from openpyxl import Workbook, load_workbook

from compras.models import OrdenCompra, SolicitudCompra
from core.access import can_manage_compras
from core.audit import log_event
from core.models import Sucursal
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from .models import (
    Receta,
    LineaReceta,
    RecetaPresentacion,
    RecetaCostoVersion,
    CostoDriver,
    PlanProduccion,
    PlanProduccionItem,
    PronosticoVenta,
    SolicitudVenta,
    VentaHistorica,
)
from .utils.costeo_versionado import asegurar_version_costeo, calcular_costeo_receta, comparativo_versiones
from .utils.derived_insumos import sync_presentacion_insumo, sync_receta_derivados
from .utils.normalizacion import normalizar_nombre

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
    versiones_unavailable = False
    versiones_all = []
    versiones_recientes = []
    comparativo = None
    try:
        if not receta.versiones_costo.exists():
            _sync_cost_version_safe(request, receta, "AUTO_BOOTSTRAP")
        versiones_all = _load_versiones_costeo(receta, 80)
        versiones_recientes = versiones_all[:8]
        comparativo = comparativo_versiones(versiones_recientes)
    except (OperationalError, ProgrammingError):
        versiones_unavailable = True
        messages.warning(
            request,
            "Histórico de versiones de costo no disponible en este entorno. Ejecuta migraciones para habilitarlo.",
        )

    lineas = list(receta.lineas.select_related("insumo").order_by("posicion"))
    presentaciones = receta.presentaciones.all().order_by("nombre")
    costeo_unavailable = False
    try:
        costeo_actual = calcular_costeo_receta(receta)
    except (OperationalError, ProgrammingError):
        costeo_unavailable = True
        costeo_actual = _empty_costeo_actual()
        messages.warning(
            request,
            "Costeo avanzado no disponible en este entorno. Ejecuta migraciones para habilitar drivers.",
        )

    selected_base = None
    selected_target = None
    compare_data = None
    base_raw = (request.GET.get("base") or "").strip()
    target_raw = (request.GET.get("target") or "").strip()
    try:
        if base_raw and target_raw:
            base_num = int(base_raw)
            target_num = int(target_raw)
            by_num = {v.version_num: v for v in versiones_all}
            if base_num in by_num and target_num in by_num and base_num != target_num:
                selected_base = by_num[base_num]
                selected_target = by_num[target_num]
    except Exception:
        selected_base = None
        selected_target = None

    if not selected_base and not selected_target and len(versiones_all) >= 2:
        selected_target = versiones_all[0]
        selected_base = versiones_all[1]

    if selected_base and selected_target:
        compare_data = _compare_versions(selected_base, selected_target)

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
            "costeo_actual": costeo_actual,
            "costeo_unavailable": costeo_unavailable,
            "versiones_recientes": versiones_recientes,
            "versiones_all": versiones_all,
            "versiones_comparativo": comparativo,
            "versiones_unavailable": versiones_unavailable,
            "selected_base": selected_base,
            "selected_target": selected_target,
            "version_compare": compare_data,
        },
    )


@login_required
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
    _sync_cost_version_safe(request, receta, "RECETA_UPDATE")
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


def _sync_cost_version_safe(request: HttpRequest, receta: Receta, fuente: str) -> None:
    try:
        asegurar_version_costeo(receta, fuente=fuente)
    except Exception:
        messages.warning(
            request,
            "Se guardaron cambios, pero falló el versionado automático de costos.",
        )


def _load_versiones_costeo(receta: Receta, limit: int) -> list[RecetaCostoVersion]:
    return list(receta.versiones_costo.order_by("-version_num")[:limit])


def _empty_costeo_actual() -> dict[str, Any]:
    return {
        "driver": None,
        "costo_mp": Decimal("0"),
        "costo_mo": Decimal("0"),
        "costo_indirecto": Decimal("0"),
        "costo_total": Decimal("0"),
        "costo_por_unidad_rendimiento": Decimal("0"),
    }


def _compare_versions(base: RecetaCostoVersion, target: RecetaCostoVersion) -> dict[str, Decimal]:
    delta_mp = Decimal(str(target.costo_mp or 0)) - Decimal(str(base.costo_mp or 0))
    delta_mo = Decimal(str(target.costo_mo or 0)) - Decimal(str(base.costo_mo or 0))
    delta_ind = Decimal(str(target.costo_indirecto or 0)) - Decimal(str(base.costo_indirecto or 0))
    delta_total = Decimal(str(target.costo_total or 0)) - Decimal(str(base.costo_total or 0))

    base_total = Decimal(str(base.costo_total or 0))
    delta_pct_total = None
    if base_total > 0:
        delta_pct_total = (delta_total * Decimal("100")) / base_total

    base_unidad = Decimal(str(base.costo_por_unidad_rendimiento or 0))
    target_unidad = Decimal(str(target.costo_por_unidad_rendimiento or 0))
    delta_unidad = target_unidad - base_unidad
    delta_pct_unidad = None
    if base_unidad > 0:
        delta_pct_unidad = (delta_unidad * Decimal("100")) / base_unidad

    return {
        "delta_mp": delta_mp,
        "delta_mo": delta_mo,
        "delta_indirecto": delta_ind,
        "delta_total": delta_total,
        "delta_pct_total": delta_pct_total,
        "delta_unidad": delta_unidad,
        "delta_pct_unidad": delta_pct_unidad,
    }


def _map_driver_header(header: str) -> str:
    key = normalizar_nombre(header).replace("_", " ")
    if key in {"scope", "alcance", "tipo"}:
        return "scope"
    if key in {"nombre", "driver", "driver nombre"}:
        return "nombre"
    if key in {"receta", "producto", "nombre receta"}:
        return "receta"
    if key in {"codigo point", "codigo", "sku"}:
        return "codigo_point"
    if key in {"familia", "sheet", "categoria"}:
        return "familia"
    if key in {"lote desde", "lote min", "desde"}:
        return "lote_desde"
    if key in {"lote hasta", "lote max", "hasta"}:
        return "lote_hasta"
    if key in {"mo pct", "mo%", "mano obra pct", "mano de obra pct"}:
        return "mo_pct"
    if key in {"ind pct", "indirecto pct", "indirectos pct", "indirecto%"}:
        return "indirecto_pct"
    if key in {"mo fijo", "mano obra fijo", "mano de obra fijo"}:
        return "mo_fijo"
    if key in {"ind fijo", "indirecto fijo", "indirectos fijo"}:
        return "indirecto_fijo"
    if key in {"prioridad", "priority"}:
        return "prioridad"
    if key in {"activo", "enabled"}:
        return "activo"
    return key


def _normalize_driver_scope(raw: str | None) -> str:
    key = normalizar_nombre(raw or "")
    if key in {"producto", "product"}:
        return CostoDriver.SCOPE_PRODUCTO
    if key in {"familia", "family"}:
        return CostoDriver.SCOPE_FAMILIA
    if key in {"lote", "batch"}:
        return CostoDriver.SCOPE_LOTE
    return CostoDriver.SCOPE_GLOBAL


def _to_int_safe(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _to_bool_safe(value: object, default: bool = True) -> bool:
    if value is None:
        return default
    key = normalizar_nombre(str(value))
    if key in {"1", "true", "si", "yes", "on", "activo"}:
        return True
    if key in {"0", "false", "no", "off", "inactivo"}:
        return False
    return default


def _load_driver_rows(uploaded) -> list[dict]:
    filename = (uploaded.name or "").lower()
    rows: list[dict] = []
    if filename.endswith(".csv"):
        uploaded.seek(0)
        content = uploaded.read().decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(content.splitlines())
        for row in reader:
            parsed = {}
            for key, value in (row or {}).items():
                if not key:
                    continue
                parsed[_map_driver_header(str(key))] = value
            rows.append(parsed)
        return rows
    if filename.endswith(".xlsx") or filename.endswith(".xlsm"):
        uploaded.seek(0)
        wb = load_workbook(uploaded, read_only=True, data_only=True)
        ws = wb.active
        values = list(ws.values)
        if not values:
            return []
        headers = [_map_driver_header(str(h or "")) for h in values[0]]
        for raw in values[1:]:
            parsed = {}
            for idx, header in enumerate(headers):
                if not header:
                    continue
                parsed[header] = raw[idx] if idx < len(raw) else None
            rows.append(parsed)
        return rows
    raise ValueError("Formato no soportado. Usa CSV o XLSX.")


@login_required
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
        _sync_cost_version_safe(request, receta, "LINEA_EDIT")
        messages.success(request, "Línea actualizada.")
        return redirect("recetas:receta_detail", pk=pk)

    return render(request, "recetas/linea_form.html", _linea_form_context(receta, linea))


@login_required
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
        _sync_cost_version_safe(request, receta, "LINEA_CREATE")
        messages.success(request, "Línea agregada.")
        return redirect("recetas:receta_detail", pk=pk)
    return render(request, "recetas/linea_form.html", _linea_form_context(receta))


@login_required
@permission_required("recetas.delete_lineareceta", raise_exception=True)
@require_POST
def linea_delete(request: HttpRequest, pk: int, linea_id: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    linea = get_object_or_404(LineaReceta, pk=linea_id, receta=receta)
    linea.delete()
    _sync_derived_insumos_safe(request, receta)
    _sync_cost_version_safe(request, receta, "LINEA_DELETE")
    messages.success(request, "Línea eliminada.")
    return redirect("recetas:receta_detail", pk=pk)


@login_required
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
        _sync_cost_version_safe(request, receta, "PRESENTACION_CREATE")
        messages.success(request, "Presentación guardada.")
        return redirect("recetas:receta_detail", pk=pk)

    return render(
        request,
        "recetas/presentacion_form.html",
        {"receta": receta, "presentacion": None},
    )


@login_required
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
        _sync_cost_version_safe(request, receta, "PRESENTACION_EDIT")
        messages.success(request, "Presentación actualizada.")
        return redirect("recetas:receta_detail", pk=pk)

    return render(
        request,
        "recetas/presentacion_form.html",
        {"receta": receta, "presentacion": presentacion},
    )


@login_required
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
    _sync_cost_version_safe(request, receta, "PRESENTACION_DELETE")
    messages.success(request, "Presentación eliminada.")
    return redirect("recetas:receta_detail", pk=pk)


@login_required
@permission_required("recetas.view_receta", raise_exception=True)
def receta_versiones_export(request: HttpRequest, pk: int) -> HttpResponse:
    receta = get_object_or_404(Receta, pk=pk)
    export_format = (request.GET.get("format") or "csv").strip().lower()
    try:
        versiones = _load_versiones_costeo(receta, 300)
    except (OperationalError, ProgrammingError):
        versiones = []

    if export_format == "xlsx":
        wb = Workbook()
        ws = wb.active
        ws.title = "versiones_costeo"
        ws.append(
            [
                "version",
                "fecha",
                "fuente",
                "driver_scope",
                "driver_nombre",
                "costo_mp",
                "costo_mo",
                "costo_indirecto",
                "costo_total",
                "rendimiento",
                "unidad_rendimiento",
                "costo_por_unidad_rendimiento",
            ]
        )
        for v in versiones:
            ws.append(
                [
                    v.version_num,
                    timezone.localtime(v.creado_en).strftime("%Y-%m-%d %H:%M"),
                    v.fuente,
                    v.driver_scope,
                    v.driver_nombre,
                    float(v.costo_mp or 0),
                    float(v.costo_mo or 0),
                    float(v.costo_indirecto or 0),
                    float(v.costo_total or 0),
                    float(v.rendimiento_cantidad or 0),
                    v.rendimiento_unidad,
                    float(v.costo_por_unidad_rendimiento or 0),
                ]
            )
        out = BytesIO()
        wb.save(out)
        out.seek(0)
        resp = HttpResponse(
            out.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        resp["Content-Disposition"] = f'attachment; filename="receta_{receta.id}_versiones_costeo.xlsx"'
        return resp

    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="receta_{receta.id}_versiones_costeo.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "version",
            "fecha",
            "fuente",
            "driver_scope",
            "driver_nombre",
            "costo_mp",
            "costo_mo",
            "costo_indirecto",
            "costo_total",
            "rendimiento",
            "unidad_rendimiento",
            "costo_por_unidad_rendimiento",
        ]
    )
    for v in versiones:
        writer.writerow(
            [
                v.version_num,
                timezone.localtime(v.creado_en).strftime("%Y-%m-%d %H:%M"),
                v.fuente,
                v.driver_scope,
                v.driver_nombre,
                v.costo_mp,
                v.costo_mo,
                v.costo_indirecto,
                v.costo_total,
                v.rendimiento_cantidad,
                v.rendimiento_unidad,
                v.costo_por_unidad_rendimiento,
            ]
        )
    return response


@login_required
@permission_required("recetas.change_receta", raise_exception=True)
def drivers_costeo(request: HttpRequest) -> HttpResponse:
    q = (request.GET.get("q") or "").strip()
    scope = (request.GET.get("scope") or "").strip().upper()
    edit_raw = (request.GET.get("edit") or "").strip()
    edit_driver = None

    drivers_unavailable = False
    drivers = []
    try:
        qs = CostoDriver.objects.select_related("receta").order_by("scope", "prioridad", "id")
        if q:
            qs = qs.filter(
                Q(nombre__icontains=q)
                | Q(familia__icontains=q)
                | Q(receta__nombre__icontains=q)
            )
        if scope in {CostoDriver.SCOPE_PRODUCTO, CostoDriver.SCOPE_FAMILIA, CostoDriver.SCOPE_LOTE, CostoDriver.SCOPE_GLOBAL}:
            qs = qs.filter(scope=scope)
        if edit_raw:
            try:
                edit_driver = CostoDriver.objects.select_related("receta").filter(pk=int(edit_raw)).first()
            except Exception:
                edit_driver = None
        drivers = list(qs[:400])
    except (OperationalError, ProgrammingError):
        drivers_unavailable = True
        edit_driver = None

    if request.method == "POST":
        if drivers_unavailable:
            messages.error(request, "Drivers de costeo no disponibles en este entorno. Ejecuta migraciones.")
            return redirect("recetas:drivers_costeo")
        driver_id = (request.POST.get("driver_id") or "").strip()
        nombre = (request.POST.get("nombre") or "").strip()
        driver_scope = _normalize_driver_scope(request.POST.get("scope"))
        receta_id = (request.POST.get("receta_id") or "").strip()
        familia = (request.POST.get("familia") or "").strip()
        lote_desde = _to_decimal_or_none(request.POST.get("lote_desde"))
        lote_hasta = _to_decimal_or_none(request.POST.get("lote_hasta"))
        mo_pct = _to_decimal_safe(request.POST.get("mo_pct"))
        indirecto_pct = _to_decimal_safe(request.POST.get("indirecto_pct"))
        mo_fijo = _to_decimal_safe(request.POST.get("mo_fijo"))
        indirecto_fijo = _to_decimal_safe(request.POST.get("indirecto_fijo"))
        prioridad = _to_int_safe(request.POST.get("prioridad"), 100)
        activo = _to_bool_safe(request.POST.get("activo"), True)

        if not nombre:
            messages.error(request, "Nombre del driver es obligatorio.")
            return redirect("recetas:drivers_costeo")

        receta = Receta.objects.filter(pk=receta_id).first() if receta_id else None
        if driver_scope == CostoDriver.SCOPE_PRODUCTO and not receta:
            messages.error(request, "Para scope PRODUCTO debes seleccionar una receta.")
            return redirect("recetas:drivers_costeo")

        try:
            if driver_id:
                driver = CostoDriver.objects.filter(pk=driver_id).first()
                if not driver:
                    messages.error(request, "Driver no encontrado para editar.")
                    return redirect("recetas:drivers_costeo")
            else:
                driver = CostoDriver()

            driver.nombre = nombre[:120]
            driver.scope = driver_scope
            driver.receta = receta
            driver.familia = familia[:120]
            driver.lote_desde = lote_desde
            driver.lote_hasta = lote_hasta
            driver.mo_pct = mo_pct
            driver.indirecto_pct = indirecto_pct
            driver.mo_fijo = mo_fijo
            driver.indirecto_fijo = indirecto_fijo
            driver.prioridad = max(prioridad, 0)
            driver.activo = activo
            driver.save()
            messages.success(request, "Driver guardado.")
        except (OperationalError, ProgrammingError):
            messages.error(request, "No se pudo guardar el driver. Ejecuta migraciones pendientes.")
        return redirect("recetas:drivers_costeo")

    return render(
        request,
        "recetas/drivers_costeo.html",
        {
            "drivers": drivers,
            "recetas": Receta.objects.order_by("nombre"),
            "q": q,
            "scope": scope,
            "edit_driver": edit_driver,
            "drivers_unavailable": drivers_unavailable,
            "scope_choices": [
                CostoDriver.SCOPE_PRODUCTO,
                CostoDriver.SCOPE_FAMILIA,
                CostoDriver.SCOPE_LOTE,
                CostoDriver.SCOPE_GLOBAL,
            ],
        },
    )


@login_required
@permission_required("recetas.change_receta", raise_exception=True)
@require_POST
def drivers_costeo_delete(request: HttpRequest, driver_id: int) -> HttpResponse:
    try:
        driver = get_object_or_404(CostoDriver, pk=driver_id)
        driver.delete()
        messages.success(request, "Driver eliminado.")
    except (OperationalError, ProgrammingError):
        messages.error(request, "No se pudo eliminar el driver. Ejecuta migraciones pendientes.")
    return redirect("recetas:drivers_costeo")


@login_required
@permission_required("recetas.change_receta", raise_exception=True)
def drivers_costeo_plantilla(request: HttpRequest) -> HttpResponse:
    export_format = (request.GET.get("format") or "xlsx").strip().lower()
    headers = [
        "scope",
        "nombre",
        "receta",
        "codigo_point",
        "familia",
        "lote_desde",
        "lote_hasta",
        "mo_pct",
        "indirecto_pct",
        "mo_fijo",
        "indirecto_fijo",
        "prioridad",
        "activo",
    ]
    rows = [
        ["PRODUCTO", "Driver Pastel Fresas", "Pastel Fresas Con Crema - Chico", "PFC-CHICO", "", "", "", "8", "4", "0", "0", "10", "1"],
        ["FAMILIA", "Driver Insumos 1", "", "", "Insumos 1", "", "", "6", "3", "0", "0", "30", "1"],
        ["GLOBAL", "Driver Global Base", "", "", "", "", "", "5", "2", "0", "0", "100", "1"],
    ]

    if export_format == "csv":
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="plantilla_drivers_costeo.csv"'
        writer = csv.writer(response)
        writer.writerow(headers)
        writer.writerows(rows)
        return response

    wb = Workbook()
    ws = wb.active
    ws.title = "drivers_costeo"
    ws.append(headers)
    for row in rows:
        ws.append(row)
    for col in ("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"):
        ws.column_dimensions[col].width = 24
    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="plantilla_drivers_costeo.xlsx"'
    return response


@login_required
@permission_required("recetas.change_receta", raise_exception=True)
@require_POST
def drivers_costeo_importar(request: HttpRequest) -> HttpResponse:
    try:
        CostoDriver.objects.exists()
    except (OperationalError, ProgrammingError):
        messages.error(request, "Drivers de costeo no disponibles en este entorno. Ejecuta migraciones.")
        return redirect("recetas:drivers_costeo")

    uploaded = request.FILES.get("archivo")
    modo = (request.POST.get("modo") or "upsert").strip().lower()
    if modo not in {"upsert", "replace"}:
        modo = "upsert"

    if not uploaded:
        messages.error(request, "Selecciona un archivo para importar drivers.")
        return redirect("recetas:drivers_costeo")

    try:
        rows = _load_driver_rows(uploaded)
    except Exception as exc:
        messages.error(request, f"No se pudo leer archivo de drivers: {exc}")
        return redirect("recetas:drivers_costeo")

    if not rows:
        messages.warning(request, "Archivo de drivers sin filas.")
        return redirect("recetas:drivers_costeo")

    created = 0
    updated = 0
    skipped = 0

    if modo == "replace":
        CostoDriver.objects.all().delete()

    for row in rows:
        scope = _normalize_driver_scope(row.get("scope"))
        nombre = str(row.get("nombre") or "").strip()
        if not nombre:
            skipped += 1
            continue

        receta = None
        receta_name = str(row.get("receta") or "").strip()
        codigo_point = str(row.get("codigo_point") or "").strip()
        if codigo_point:
            receta = Receta.objects.filter(codigo_point__iexact=codigo_point).order_by("id").first()
        if receta is None and receta_name:
            receta = Receta.objects.filter(nombre_normalizado=normalizar_nombre(receta_name)).order_by("id").first()

        if scope == CostoDriver.SCOPE_PRODUCTO and not receta:
            skipped += 1
            continue

        familia = str(row.get("familia") or "").strip()
        lote_desde = _to_decimal_or_none(str(row.get("lote_desde") or "").strip())
        lote_hasta = _to_decimal_or_none(str(row.get("lote_hasta") or "").strip())

        filter_kwargs = {
            "scope": scope,
            "nombre": nombre[:120],
            "receta": receta,
            "familia_normalizada": normalizar_nombre(familia),
            "lote_desde": lote_desde,
            "lote_hasta": lote_hasta,
        }
        driver = CostoDriver.objects.filter(**filter_kwargs).first()
        if not driver:
            driver = CostoDriver(**filter_kwargs)
            created += 1
        else:
            updated += 1

        driver.familia = familia[:120]
        driver.mo_pct = _to_decimal_safe(row.get("mo_pct"))
        driver.indirecto_pct = _to_decimal_safe(row.get("indirecto_pct"))
        driver.mo_fijo = _to_decimal_safe(row.get("mo_fijo"))
        driver.indirecto_fijo = _to_decimal_safe(row.get("indirecto_fijo"))
        driver.prioridad = max(_to_int_safe(row.get("prioridad"), 100), 0)
        driver.activo = _to_bool_safe(row.get("activo"), True)
        driver.save()

    messages.success(
        request,
        f"Importación drivers completada. Creados: {created}. Actualizados: {updated}. Omitidos: {skipped}.",
    )
    return redirect("recetas:drivers_costeo")


@login_required
@permission_required("recetas.change_lineareceta", raise_exception=True)
def matching_pendientes(request: HttpRequest) -> HttpResponse:
    q = request.GET.get("q", "").strip()
    pendientes = LineaReceta.objects.filter(match_status=LineaReceta.STATUS_NEEDS_REVIEW).select_related("receta", "insumo").order_by("receta__nombre", "posicion")
    if q:
        pendientes = pendientes.filter(insumo_texto__icontains=q)

    total_pendientes = pendientes.count()
    recetas_afectadas = pendientes.values("receta_id").distinct().count()
    no_match_count = pendientes.filter(match_method=LineaReceta.MATCH_NONE).count()
    fuzzy_count = pendientes.filter(match_method=LineaReceta.MATCH_FUZZY).count()

    export_format = (request.GET.get("export") or "").strip().lower()
    if export_format == "csv":
        now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = f'attachment; filename="matching_pendientes_{now_str}.csv"'
        writer = csv.writer(response)
        writer.writerow(["receta", "posicion", "ingrediente", "metodo", "score", "insumo_ligado"])
        for linea in pendientes:
            writer.writerow(
                [
                    linea.receta.nombre,
                    linea.posicion,
                    linea.insumo_texto or "",
                    linea.match_method or "",
                    float(linea.match_score or 0),
                    linea.insumo.nombre if linea.insumo_id and linea.insumo else "",
                ]
            )
        return response
    if export_format == "xlsx":
        wb = Workbook()
        ws = wb.active
        ws.title = "matching_pendientes"
        ws.append(["receta", "posicion", "ingrediente", "metodo", "score", "insumo_ligado"])
        for linea in pendientes.iterator(chunk_size=500):
            ws.append(
                [
                    linea.receta.nombre,
                    linea.posicion,
                    linea.insumo_texto or "",
                    linea.match_method or "",
                    float(linea.match_score or 0),
                    linea.insumo.nombre if linea.insumo_id and linea.insumo else "",
                ]
            )
        ws.column_dimensions["A"].width = 38
        ws.column_dimensions["B"].width = 10
        ws.column_dimensions["C"].width = 34
        ws.column_dimensions["D"].width = 16
        ws.column_dimensions["E"].width = 12
        ws.column_dimensions["F"].width = 34

        output = BytesIO()
        wb.save(output)
        output.seek(0)
        now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
        response = HttpResponse(
            output.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = f'attachment; filename="matching_pendientes_{now_str}.xlsx"'
        return response

    paginator = Paginator(pendientes, 25)
    page = paginator.get_page(request.GET.get("page"))
    return render(
        request,
        "recetas/matching_pendientes.html",
        {
            "page": page,
            "q": q,
            "stats": {
                "total": total_pendientes,
                "recetas": recetas_afectadas,
                "no_match": no_match_count,
                "fuzzy": fuzzy_count,
            },
        },
    )


@login_required
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

@login_required
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
    _sync_cost_version_safe(request, linea.receta, "MATCHING_APPROVE")
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
                    "stock_actual": Decimal("0"),
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
    existencias_map = {
        e.insumo_id: Decimal(str(e.stock_actual or 0))
        for e in ExistenciaInsumo.objects.filter(insumo_id__in=list(insumos_map.keys()))
    }
    alertas_capacidad = 0
    for row in insumos:
        row["stock_actual"] = existencias_map.get(row["insumo_id"], Decimal("0"))
        faltante = Decimal(str(row["cantidad"] or 0)) - Decimal(str(row["stock_actual"] or 0))
        row["faltante"] = faltante if faltante > 0 else Decimal("0")
        row["alerta_capacidad"] = row["faltante"] > 0
        if row["alerta_capacidad"]:
            alertas_capacidad += 1
    costo_total = sum((row["costo_total"] for row in insumos), Decimal("0"))

    return {
        "items_detalle": items_detalle,
        "insumos": insumos,
        "costo_total": costo_total,
        "lineas_sin_cantidad": sorted(lineas_sin_cantidad),
        "lineas_sin_costo_unitario": sorted(lineas_sin_costo_unitario),
        "lineas_sin_match": lineas_sin_match,
        "alertas_capacidad": alertas_capacidad,
    }


def _plan_vs_pronostico(plan: PlanProduccion) -> Dict[str, Any]:
    periodo = plan.fecha_produccion.strftime("%Y-%m")
    plan_rows = (
        plan.items.values("receta_id", "receta__nombre")
        .annotate(cantidad_plan=Sum("cantidad"))
        .order_by("receta__nombre")
    )
    plan_map = {
        int(r["receta_id"]): {
            "receta_id": int(r["receta_id"]),
            "receta": r["receta__nombre"],
            "cantidad_plan": Decimal(str(r["cantidad_plan"] or 0)),
            "cantidad_pronostico": Decimal("0"),
        }
        for r in plan_rows
    }

    pronosticos_unavailable = False
    try:
        pronosticos = list(
            PronosticoVenta.objects.filter(periodo=periodo).select_related("receta")
        )
    except (OperationalError, ProgrammingError):
        pronosticos = []
        pronosticos_unavailable = True

    for p in pronosticos:
        row = plan_map.get(p.receta_id)
        if row:
            row["cantidad_pronostico"] = Decimal(str(p.cantidad or 0))
        else:
            plan_map[p.receta_id] = {
                "receta_id": p.receta_id,
                "receta": p.receta.nombre,
                "cantidad_plan": Decimal("0"),
                "cantidad_pronostico": Decimal(str(p.cantidad or 0)),
            }

    rows = sorted(plan_map.values(), key=lambda x: x["receta"].lower())
    con_desviacion = 0
    for row in rows:
        row["delta"] = row["cantidad_plan"] - row["cantidad_pronostico"]
        row["sin_pronostico"] = row["cantidad_plan"] > 0 and row["cantidad_pronostico"] <= 0
        if row["delta"] != 0:
            con_desviacion += 1

    return {
        "periodo": periodo,
        "rows": rows,
        "total_plan": sum((r["cantidad_plan"] for r in rows), Decimal("0")),
        "total_pronostico": sum((r["cantidad_pronostico"] for r in rows), Decimal("0")),
        "desviaciones": con_desviacion,
        "pronosticos_unavailable": pronosticos_unavailable,
    }


def _periodo_mrp_resumen(periodo_mes: str, periodo_tipo: str = "mes") -> Dict[str, Any]:
    periodo = _normalize_periodo_mes(periodo_mes)
    try:
        year, month = periodo.split("-")
        year_i = int(year)
        month_i = int(month)
    except Exception:
        today = timezone.localdate()
        year_i = today.year
        month_i = today.month
        periodo = f"{year_i:04d}-{month_i:02d}"

    plans_qs = PlanProduccion.objects.filter(
        fecha_produccion__year=year_i,
        fecha_produccion__month=month_i,
    ).order_by("fecha_produccion", "id")

    periodo_tipo_norm = (periodo_tipo or "mes").strip().lower()
    if periodo_tipo_norm not in {"mes", "q1", "q2"}:
        periodo_tipo_norm = "mes"
    if periodo_tipo_norm == "q1":
        plans_qs = plans_qs.filter(fecha_produccion__day__lte=15)
    elif periodo_tipo_norm == "q2":
        plans_qs = plans_qs.filter(fecha_produccion__day__gte=16)

    plans = list(plans_qs.only("id", "nombre", "fecha_produccion"))
    if not plans:
        return {
            "periodo": periodo,
            "periodo_tipo": periodo_tipo_norm,
            "planes_count": 0,
            "planes": [],
            "insumos_count": 0,
            "costo_total": Decimal("0"),
            "alertas_capacidad": 0,
            "lineas_sin_match": 0,
            "lineas_sin_cantidad": 0,
            "lineas_sin_costo_unitario": 0,
            "insumos": [],
        }

    plan_items_map = {
        row["plan_id"]: int(row["items_count"] or 0)
        for row in (
            PlanProduccionItem.objects.filter(plan_id__in=[p.id for p in plans])
            .values("plan_id")
            .annotate(items_count=Count("id"))
        )
    }

    items = (
        PlanProduccionItem.objects.filter(plan_id__in=[p.id for p in plans])
        .select_related("plan", "receta")
        .prefetch_related(
            "receta__lineas__insumo__unidad_base",
            "receta__lineas__insumo__proveedor_principal",
            "receta__lineas__unidad",
        )
        .order_by("plan__fecha_produccion", "plan_id", "id")
    )

    unit_cost_cache: Dict[int, Decimal | None] = {}
    insumos_map: Dict[int, Dict[str, Any]] = {}
    lineas_sin_cantidad = 0
    lineas_sin_costo_unitario = 0
    lineas_sin_match = 0

    for item in items:
        multiplicador = Decimal(str(item.cantidad or 0))
        if multiplicador <= 0:
            continue
        for linea in item.receta.lineas.all():
            if not linea.insumo_id:
                lineas_sin_match += 1
                continue

            qty_base = Decimal(str(linea.cantidad or 0))
            if qty_base <= 0:
                lineas_sin_cantidad += 1
                continue

            qty = qty_base * multiplicador
            if qty <= 0:
                continue

            unit_code = _linea_unit_code(linea)
            unit_cost = _linea_unit_cost(linea, unit_cost_cache)
            if unit_cost is None or unit_cost <= 0:
                lineas_sin_costo_unitario += 1
                unit_cost = Decimal("0")
            costo_linea = qty * unit_cost

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
                    "costo_unitario": unit_cost,
                    "stock_actual": Decimal("0"),
                }

            insumos_map[key]["cantidad"] += qty
            insumos_map[key]["costo_total"] += costo_linea
            if insumos_map[key]["costo_unitario"] <= 0 and unit_cost > 0:
                insumos_map[key]["costo_unitario"] = unit_cost

    insumos = sorted(insumos_map.values(), key=lambda x: x["nombre"].lower())
    existencias_map = {
        e.insumo_id: Decimal(str(e.stock_actual or 0))
        for e in ExistenciaInsumo.objects.filter(insumo_id__in=list(insumos_map.keys()))
    }

    alertas_capacidad = 0
    for row in insumos:
        row["stock_actual"] = existencias_map.get(row["insumo_id"], Decimal("0"))
        faltante = Decimal(str(row["cantidad"] or 0)) - Decimal(str(row["stock_actual"] or 0))
        row["faltante"] = faltante if faltante > 0 else Decimal("0")
        row["alerta_capacidad"] = row["faltante"] > 0
        if row["alerta_capacidad"]:
            alertas_capacidad += 1

    return {
        "periodo": periodo,
        "periodo_tipo": periodo_tipo_norm,
        "planes_count": len(plans),
        "planes": [
            {
                "id": p.id,
                "nombre": p.nombre,
                "fecha_produccion": p.fecha_produccion,
                "items_count": plan_items_map.get(p.id, 0),
            }
            for p in plans
        ],
        "insumos_count": len(insumos),
        "costo_total": sum((row["costo_total"] for row in insumos), Decimal("0")),
        "alertas_capacidad": alertas_capacidad,
        "lineas_sin_match": lineas_sin_match,
        "lineas_sin_cantidad": lineas_sin_cantidad,
        "lineas_sin_costo_unitario": lineas_sin_costo_unitario,
        "insumos": insumos,
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


def _export_periodo_mrp_csv(resumen: Dict[str, Any]) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    filename = f"mrp_periodo_{resumen['periodo']}_{resumen['periodo_tipo']}.csv"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    writer = csv.writer(response)

    writer.writerow(["MRP CONSOLIDADO POR PERIODO", resumen["periodo"]])
    writer.writerow(["Alcance", resumen["periodo_tipo"]])
    writer.writerow(["Planes", resumen["planes_count"]])
    writer.writerow(["Insumos", resumen["insumos_count"]])
    writer.writerow(["Costo total", str(resumen["costo_total"])])
    writer.writerow(["Alertas capacidad", resumen["alertas_capacidad"]])
    writer.writerow([])

    writer.writerow(["PLANES INCLUIDOS"])
    writer.writerow(["Plan", "Fecha producción", "Renglones"])
    for plan in resumen["planes"]:
        writer.writerow([plan["nombre"], str(plan["fecha_produccion"]), plan["items_count"]])
    writer.writerow([])

    writer.writerow(["INSUMOS CONSOLIDADOS"])
    writer.writerow(
        [
            "Insumo",
            "Origen",
            "Proveedor sugerido",
            "Cantidad requerida",
            "Stock actual",
            "Faltante",
            "Unidad",
            "Costo unitario",
            "Costo total",
            "Alerta capacidad",
        ]
    )
    for row in resumen["insumos"]:
        writer.writerow(
            [
                row["nombre"],
                row["origen"],
                row.get("proveedor_sugerido") or "-",
                f"{Decimal(str(row['cantidad'])):.3f}",
                f"{Decimal(str(row['stock_actual'])):.3f}",
                f"{Decimal(str(row['faltante'])):.3f}",
                row["unidad"],
                f"{Decimal(str(row['costo_unitario'])):.2f}",
                f"{Decimal(str(row['costo_total'])):.2f}",
                "SI" if row.get("alerta_capacidad") else "NO",
            ]
        )
    return response


def _export_periodo_mrp_xlsx(resumen: Dict[str, Any]) -> HttpResponse:
    wb = Workbook()
    ws_resumen = wb.active
    ws_resumen.title = "Resumen"
    ws_resumen.append(["Periodo", resumen["periodo"]])
    ws_resumen.append(["Alcance", resumen["periodo_tipo"]])
    ws_resumen.append(["Planes", resumen["planes_count"]])
    ws_resumen.append(["Insumos", resumen["insumos_count"]])
    ws_resumen.append(["Costo total", float(resumen["costo_total"] or 0)])
    ws_resumen.append(["Alertas capacidad", resumen["alertas_capacidad"]])

    ws_planes = wb.create_sheet("Planes")
    ws_planes.append(["Plan", "Fecha producción", "Renglones"])
    for plan in resumen["planes"]:
        ws_planes.append([plan["nombre"], str(plan["fecha_produccion"]), plan["items_count"]])

    ws_insumos = wb.create_sheet("Insumos")
    ws_insumos.append(
        [
            "Insumo",
            "Origen",
            "Proveedor sugerido",
            "Cantidad requerida",
            "Stock actual",
            "Faltante",
            "Unidad",
            "Costo unitario",
            "Costo total",
            "Alerta capacidad",
        ]
    )
    for row in resumen["insumos"]:
        ws_insumos.append(
            [
                row["nombre"],
                row["origen"],
                row.get("proveedor_sugerido") or "-",
                float(row["cantidad"] or 0),
                float(row["stock_actual"] or 0),
                float(row["faltante"] or 0),
                row["unidad"],
                float(row["costo_unitario"] or 0),
                float(row["costo_total"] or 0),
                "SI" if row.get("alerta_capacidad") else "NO",
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    filename = f"mrp_periodo_{resumen['periodo']}_{resumen['periodo_tipo']}.xlsx"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def _normalize_periodo_mes(raw: str | None) -> str:
    txt = (raw or "").strip()
    if not txt:
        today = timezone.localdate()
        return f"{today.year:04d}-{today.month:02d}"

    txt = txt.replace("/", "-")
    if len(txt) >= 7:
        txt = txt[:7]
    try:
        y, m = txt.split("-")
        yi = int(y)
        mi = int(m)
        if 1 <= mi <= 12:
            return f"{yi:04d}-{mi:02d}"
    except Exception:
        pass

    today = timezone.localdate()
    return f"{today.year:04d}-{today.month:02d}"


def _to_decimal_safe(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        txt = str(value).strip().replace(",", ".")
        if txt == "":
            return Decimal("0")
        return Decimal(txt)
    except Exception:
        return Decimal("0")


def _map_pronostico_header(header: str) -> str:
    key = normalizar_nombre(header).replace("_", " ")
    if key in {"receta", "producto", "nombre", "nombre receta"}:
        return "receta"
    if key in {"codigo point", "codigo", "sku"}:
        return "codigo_point"
    if key in {"periodo", "mes"}:
        return "periodo"
    if key in {"cantidad", "pronostico", "forecast"}:
        return "cantidad"
    return key


def _load_pronostico_rows(uploaded) -> list[dict]:
    filename = (uploaded.name or "").lower()
    rows: list[dict] = []
    if filename.endswith(".csv"):
        uploaded.seek(0)
        content = uploaded.read().decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(content.splitlines())
        for row in reader:
            parsed = {}
            for key, value in (row or {}).items():
                if not key:
                    continue
                parsed[_map_pronostico_header(str(key))] = value
            rows.append(parsed)
        return rows

    if filename.endswith(".xlsx") or filename.endswith(".xlsm"):
        uploaded.seek(0)
        wb = load_workbook(uploaded, read_only=True, data_only=True)
        ws = wb.active
        values = list(ws.values)
        if not values:
            return []
        headers = [_map_pronostico_header(str(h or "")) for h in values[0]]
        for raw in values[1:]:
            parsed = {}
            for idx, header in enumerate(headers):
                if not header:
                    continue
                parsed[header] = raw[idx] if idx < len(raw) else None
            rows.append(parsed)
        return rows

    raise ValueError("Formato no soportado. Usa CSV o XLSX.")


def _map_ventas_header(header: str) -> str:
    key = normalizar_nombre(header).replace("_", " ")
    if key in {"receta", "producto", "nombre receta", "nombre producto", "nombre"}:
        return "receta"
    if key in {"codigo point", "codigo", "sku", "codigo producto"}:
        return "codigo_point"
    if key in {"fecha", "dia", "date"}:
        return "fecha"
    if key in {"cantidad", "cantidad vendida", "unidades", "qty", "ventas"}:
        return "cantidad"
    if key in {"sucursal", "tienda", "store"}:
        return "sucursal"
    if key in {"sucursal codigo", "codigo sucursal", "store code"}:
        return "sucursal_codigo"
    if key in {"tickets", "ticket count", "num tickets"}:
        return "tickets"
    if key in {"monto", "total", "monto total", "importe"}:
        return "monto_total"
    return key


def _load_ventas_rows(uploaded) -> list[dict]:
    filename = (uploaded.name or "").lower()
    rows: list[dict] = []
    if filename.endswith(".csv"):
        uploaded.seek(0)
        content = uploaded.read().decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(content.splitlines())
        for row in reader:
            parsed = {}
            for key, value in (row or {}).items():
                if not key:
                    continue
                parsed[_map_ventas_header(str(key))] = value
            rows.append(parsed)
        return rows

    if filename.endswith(".xlsx") or filename.endswith(".xlsm"):
        uploaded.seek(0)
        wb = load_workbook(uploaded, read_only=True, data_only=True)
        ws = wb.active
        values = list(ws.values)
        if not values:
            return []
        headers = [_map_ventas_header(str(h or "")) for h in values[0]]
        for raw in values[1:]:
            parsed = {}
            for idx, header in enumerate(headers):
                if not header:
                    continue
                parsed[header] = raw[idx] if idx < len(raw) else None
            rows.append(parsed)
        return rows

    raise ValueError("Formato no soportado. Usa CSV o XLSX.")


def _map_solicitud_ventas_header(header: str) -> str:
    key = normalizar_nombre(header).replace("_", " ")
    if key in {"receta", "producto", "nombre receta", "nombre producto", "nombre"}:
        return "receta"
    if key in {"codigo point", "codigo", "sku", "codigo producto"}:
        return "codigo_point"
    if key in {"sucursal", "tienda", "store"}:
        return "sucursal"
    if key in {"sucursal codigo", "codigo sucursal", "store code"}:
        return "sucursal_codigo"
    if key in {"alcance", "tipo periodo", "scope"}:
        return "alcance"
    if key in {"periodo", "mes"}:
        return "periodo"
    if key in {"fecha", "fecha base", "base"}:
        return "fecha_base"
    if key in {"fecha inicio", "inicio", "start date"}:
        return "fecha_inicio"
    if key in {"fecha fin", "fin", "end date"}:
        return "fecha_fin"
    if key in {"cantidad", "cantidad solicitada", "qty", "solicitud"}:
        return "cantidad"
    return key


def _load_solicitud_ventas_rows(uploaded) -> list[dict]:
    filename = (uploaded.name or "").lower()
    rows: list[dict] = []
    if filename.endswith(".csv"):
        uploaded.seek(0)
        content = uploaded.read().decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(content.splitlines())
        for row in reader:
            parsed = {}
            for key, value in (row or {}).items():
                if not key:
                    continue
                parsed[_map_solicitud_ventas_header(str(key))] = value
            rows.append(parsed)
        return rows

    if filename.endswith(".xlsx") or filename.endswith(".xlsm"):
        uploaded.seek(0)
        wb = load_workbook(uploaded, read_only=True, data_only=True)
        ws = wb.active
        values = list(ws.values)
        if not values:
            return []
        headers = [_map_solicitud_ventas_header(str(h or "")) for h in values[0]]
        for raw in values[1:]:
            parsed = {}
            for idx, header in enumerate(headers):
                if not header:
                    continue
                parsed[header] = raw[idx] if idx < len(raw) else None
            rows.append(parsed)
        return rows

    raise ValueError("Formato no soportado. Usa CSV o XLSX.")


def _parse_date_safe(value: object) -> date | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.replace("/", "-")
    try:
        return date.fromisoformat(raw[:10])
    except Exception:
        pass
    parts = raw.split("-")
    if len(parts) == 3:
        try:
            if len(parts[0]) == 4:
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            else:
                d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
            return date(y, m, d)
        except Exception:
            return None
    return None


def _to_int_safe(value: object, default: int = 0) -> int:
    if value is None:
        return default
    try:
        txt = str(value).strip()
        if txt == "":
            return default
        return int(float(txt))
    except Exception:
        return default


def _resolve_receta_for_sales(receta_name: str, codigo_point: str) -> Receta | None:
    receta = None
    if codigo_point:
        receta = Receta.objects.filter(codigo_point__iexact=codigo_point).order_by("id").first()
    if receta is None and receta_name:
        receta = Receta.objects.filter(nombre_normalizado=normalizar_nombre(receta_name)).order_by("id").first()
    return receta


def _resolve_sucursal_for_sales(sucursal_name: str, sucursal_codigo: str, default_sucursal: Sucursal | None) -> Sucursal | None:
    sucursal = None
    if sucursal_codigo:
        sucursal = Sucursal.objects.filter(codigo__iexact=sucursal_codigo).order_by("id").first()
    if sucursal is None and sucursal_name:
        sucursal = Sucursal.objects.filter(nombre__iexact=sucursal_name).order_by("id").first()
    return sucursal or default_sucursal


def _normalize_alcance_solicitud(raw: str | None) -> str:
    key = normalizar_nombre(str(raw or "")).replace(" ", "_")
    if key in {"mes", "mensual"}:
        return SolicitudVenta.ALCANCE_MES
    if key in {"semana", "semanal", "week"}:
        return SolicitudVenta.ALCANCE_SEMANA
    if key in {"fin_semana", "fin_de_semana", "weekend"}:
        return SolicitudVenta.ALCANCE_FIN_SEMANA
    return SolicitudVenta.ALCANCE_MES


def _ui_to_model_alcance(raw: str | None) -> str:
    key = (raw or "").strip().lower()
    if key == "semana":
        return SolicitudVenta.ALCANCE_SEMANA
    if key == "fin_semana":
        return SolicitudVenta.ALCANCE_FIN_SEMANA
    return SolicitudVenta.ALCANCE_MES


def _resolve_solicitud_window(
    *,
    alcance: str,
    periodo_raw: str | None,
    fecha_base_raw: object,
    fecha_inicio_raw: object,
    fecha_fin_raw: object,
    periodo_default: str,
    fecha_base_default: date,
) -> tuple[str, date, date]:
    if alcance == SolicitudVenta.ALCANCE_MES:
        periodo = _normalize_periodo_mes(periodo_raw or periodo_default)
        fecha_inicio, fecha_fin = _month_start_end(periodo)
        return periodo, fecha_inicio, fecha_fin

    fecha_inicio = _parse_date_safe(fecha_inicio_raw)
    fecha_fin = _parse_date_safe(fecha_fin_raw)
    if fecha_inicio and fecha_fin and fecha_fin >= fecha_inicio:
        periodo = f"{fecha_inicio.year:04d}-{fecha_inicio.month:02d}"
        return periodo, fecha_inicio, fecha_fin

    base = _parse_date_safe(fecha_base_raw) or fecha_inicio or fecha_base_default
    if alcance == SolicitudVenta.ALCANCE_FIN_SEMANA:
        fecha_inicio, fecha_fin = _weekend_start_end(base)
    else:
        fecha_inicio, fecha_fin = _week_start_end(base)
    periodo = f"{fecha_inicio.year:04d}-{fecha_inicio.month:02d}"
    return periodo, fecha_inicio, fecha_fin


def _month_start_end(periodo: str) -> tuple[date, date]:
    y_txt, m_txt = periodo.split("-")
    y = int(y_txt)
    m = int(m_txt)
    start = date(y, m, 1)
    end = date(y, m, monthrange(y, m)[1])
    return start, end


def _week_start_end(base: date) -> tuple[date, date]:
    start = base - timedelta(days=base.weekday())
    end = start + timedelta(days=6)
    return start, end


def _weekend_start_end(base: date) -> tuple[date, date]:
    wd = base.weekday()
    if wd == 5:
        start = base
    elif wd == 6:
        start = base - timedelta(days=1)
    else:
        start = base + timedelta(days=(5 - wd))
    return start, start + timedelta(days=1)


def _weighted_avg_decimal(values: list[Decimal]) -> Decimal:
    clean = [Decimal(str(v)) for v in values if Decimal(str(v)) >= 0]
    if not clean:
        return Decimal("0")
    total_weight = Decimal("0")
    total = Decimal("0")
    for idx, value in enumerate(clean, start=1):
        weight = Decimal(str(idx))
        total += value * weight
        total_weight += weight
    if total_weight <= 0:
        return Decimal("0")
    return total / total_weight


def _stddev_decimal(values: list[Decimal]) -> Decimal:
    clean = [Decimal(str(v)) for v in values]
    if len(clean) <= 1:
        return Decimal("0")
    mean = sum(clean, Decimal("0")) / Decimal(str(len(clean)))
    variance = sum(((v - mean) ** 2 for v in clean), Decimal("0")) / Decimal(str(len(clean)))
    return Decimal(str(sqrt(float(variance))))


def _forecast_month_qty(
    day_rows: list[tuple[date, Decimal]], target_start: date
) -> tuple[Decimal, int, Decimal, Decimal, int]:
    if not day_rows:
        return Decimal("0"), 0, Decimal("0"), Decimal("0"), 0

    monthly_totals: dict[tuple[int, int], Decimal] = defaultdict(lambda: Decimal("0"))
    for d, qty in day_rows:
        monthly_totals[(d.year, d.month)] += qty
    ordered_keys = sorted(monthly_totals.keys())
    ordered_values = [monthly_totals[k] for k in ordered_keys]

    recent_values = ordered_values[-3:] if len(ordered_values) >= 3 else ordered_values
    recent_avg = sum(recent_values, Decimal("0")) / Decimal(str(len(recent_values) or 1))

    seasonal_values = [v for (y, m), v in monthly_totals.items() if m == target_start.month and (y, m) != (target_start.year, target_start.month)]
    seasonal_avg = (
        sum(seasonal_values, Decimal("0")) / Decimal(str(len(seasonal_values)))
        if seasonal_values
        else recent_avg
    )

    trend_values = ordered_values[-6:] if len(ordered_values) >= 6 else ordered_values
    trend_next = recent_avg
    if len(trend_values) >= 2:
        slope = (trend_values[-1] - trend_values[0]) / Decimal(str(len(trend_values) - 1))
        trend_next = max(trend_values[-1] + slope, Decimal("0"))

    weighted = (
        (recent_avg * Decimal("0.50"))
        + (seasonal_avg * Decimal("0.30"))
        + (trend_next * Decimal("0.20"))
    )
    variance_samples = ordered_values[-12:] if len(ordered_values) >= 12 else ordered_values
    dispersion = _stddev_decimal(variance_samples)
    confidence = min(Decimal("1"), Decimal(str(len(ordered_values))) / Decimal("12"))
    return weighted, len(day_rows), confidence, dispersion, len(variance_samples)


def _forecast_range_qty(
    day_rows: list[tuple[date, Decimal]], target_start: date, target_end: date
) -> tuple[Decimal, int, Decimal, Decimal, int]:
    if not day_rows:
        return Decimal("0"), 0, Decimal("0"), Decimal("0"), 0

    by_dow: dict[int, list[Decimal]] = defaultdict(list)
    lower_window = target_start - timedelta(days=84)
    for d, qty in day_rows:
        if d < target_start and d >= lower_window:
            by_dow[d.weekday()].append(qty)

    all_daily = [qty for _, qty in day_rows[-120:]]
    global_avg = sum(all_daily, Decimal("0")) / Decimal(str(len(all_daily) or 1))

    horizon_days: list[date] = []
    pointer = target_start
    while pointer <= target_end:
        horizon_days.append(pointer)
        pointer += timedelta(days=1)

    used_samples = 0
    pred_total = Decimal("0")
    variance_samples: list[Decimal] = []
    for d in horizon_days:
        samples = by_dow.get(d.weekday(), [])
        if samples:
            recent = samples[-8:]
            day_pred = _weighted_avg_decimal(recent)
            used_samples += len(recent)
            variance_samples.extend(recent)
        else:
            day_pred = global_avg
            variance_samples.append(global_avg)
        pred_total += day_pred

    confidence = Decimal("0")
    if horizon_days:
        confidence = min(
            Decimal("1"),
            Decimal(str(used_samples)) / Decimal(str(len(horizon_days) * 8)),
        )
    dispersion = _stddev_decimal(variance_samples)
    return pred_total, len(day_rows), confidence, dispersion, len(variance_samples)


def _build_forecast_from_history(
    *,
    alcance: str,
    periodo: str,
    fecha_base: date | None,
    sucursal: Sucursal | None,
    incluir_preparaciones: bool,
    safety_pct: Decimal,
) -> dict[str, Any]:
    if alcance == "mes":
        target_start, target_end = _month_start_end(periodo)
    else:
        base = fecha_base or timezone.localdate()
        if alcance == "fin_semana":
            target_start, target_end = _weekend_start_end(base)
        else:
            target_start, target_end = _week_start_end(base)

    qs = VentaHistorica.objects.filter(fecha__lt=target_start).select_related("receta", "sucursal")
    if sucursal:
        qs = qs.filter(sucursal=sucursal)
    if not incluir_preparaciones:
        qs = qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)

    grouped: dict[int, dict[str, Any]] = {}
    for row in qs.values("receta_id", "receta__nombre", "fecha").annotate(total=Sum("cantidad")).order_by("receta_id", "fecha"):
        rid = int(row["receta_id"])
        item = grouped.setdefault(
            rid,
            {
                "receta_id": rid,
                "receta": row["receta__nombre"],
                "days": [],
            },
        )
        item["days"].append((row["fecha"], Decimal(str(row["total"] or 0))))

    periodo_target = f"{target_start.year:04d}-{target_start.month:02d}"
    pron_map = {
        p.receta_id: Decimal(str(p.cantidad or 0))
        for p in PronosticoVenta.objects.filter(periodo=periodo_target)
    }

    rows = []
    safety_factor = Decimal("1") + (safety_pct / Decimal("100"))
    for rid, item in grouped.items():
        days = item["days"]
        if alcance == "mes":
            predicted, obs, confidence, dispersion, samples_count = _forecast_month_qty(days, target_start)
        else:
            predicted, obs, confidence, dispersion, samples_count = _forecast_range_qty(days, target_start, target_end)
        predicted = max(predicted * safety_factor, Decimal("0"))
        predicted = predicted.quantize(Decimal("0.001"))
        dispersion = max(dispersion * safety_factor, Decimal("0")).quantize(Decimal("0.001"))
        forecast_low = max(predicted - dispersion, Decimal("0")).quantize(Decimal("0.001"))
        forecast_high = max(predicted + dispersion, Decimal("0")).quantize(Decimal("0.001"))
        if predicted <= 0:
            continue

        current = pron_map.get(rid, Decimal("0"))
        delta = predicted - current
        threshold = max(Decimal("1.000"), current * Decimal("0.05"))
        if delta > threshold:
            recomendacion = "SUBIR"
        elif delta < (threshold * Decimal("-1")):
            recomendacion = "BAJAR"
        else:
            recomendacion = "MANTENER"

        rows.append(
            {
                "receta_id": rid,
                "receta": item["receta"],
                "forecast_qty": predicted,
                "forecast_low": forecast_low,
                "forecast_high": forecast_high,
                "desviacion": dispersion,
                "muestras": int(samples_count),
                "pronostico_actual": current.quantize(Decimal("0.001")),
                "delta": delta.quantize(Decimal("0.001")),
                "recomendacion": recomendacion,
                "observaciones": obs,
                "confianza": (confidence * Decimal("100")).quantize(Decimal("0.1")),
            }
        )

    rows.sort(key=lambda x: x["forecast_qty"], reverse=True)
    return {
        "alcance": alcance,
        "periodo": periodo_target,
        "target_start": target_start,
        "target_end": target_end,
        "sucursal_id": sucursal.id if sucursal else None,
        "sucursal_nombre": f"{sucursal.codigo} - {sucursal.nombre}" if sucursal else "Todas",
        "rows": rows,
        "totals": {
            "recetas_count": len(rows),
            "forecast_total": sum((r["forecast_qty"] for r in rows), Decimal("0")).quantize(Decimal("0.001")),
            "forecast_low_total": sum((r["forecast_low"] for r in rows), Decimal("0")).quantize(Decimal("0.001")),
            "forecast_high_total": sum((r["forecast_high"] for r in rows), Decimal("0")).quantize(Decimal("0.001")),
            "pronostico_total": sum((r["pronostico_actual"] for r in rows), Decimal("0")).quantize(Decimal("0.001")),
            "delta_total": sum((r["delta"] for r in rows), Decimal("0")).quantize(Decimal("0.001")),
        },
    }


def _forecast_session_payload(result: dict[str, Any], top_rows: int = 80) -> dict[str, Any]:
    rows = []
    for row in result["rows"][:top_rows]:
        rows.append(
            {
                "receta_id": int(row["receta_id"]),
                "receta": row["receta"],
                "forecast_qty": float(row["forecast_qty"]),
                "forecast_low": float(row.get("forecast_low", row["forecast_qty"])),
                "forecast_high": float(row.get("forecast_high", row["forecast_qty"])),
                "desviacion": float(row.get("desviacion", 0)),
                "muestras": int(row.get("muestras", 0)),
                "pronostico_actual": float(row["pronostico_actual"]),
                "delta": float(row["delta"]),
                "recomendacion": row["recomendacion"],
                "observaciones": int(row["observaciones"]),
                "confianza": float(row["confianza"]),
            }
        )
    return {
        "alcance": result["alcance"],
        "periodo": result["periodo"],
        "target_start": str(result["target_start"]),
        "target_end": str(result["target_end"]),
        "sucursal_id": int(result["sucursal_id"]) if result["sucursal_id"] else None,
        "sucursal_nombre": result["sucursal_nombre"],
        "rows": rows,
        "totals": {
            "recetas_count": int(result["totals"]["recetas_count"]),
            "forecast_total": float(result["totals"]["forecast_total"]),
            "forecast_low_total": float(result["totals"].get("forecast_low_total", result["totals"]["forecast_total"])),
            "forecast_high_total": float(result["totals"].get("forecast_high_total", result["totals"]["forecast_total"])),
            "pronostico_total": float(result["totals"]["pronostico_total"]),
            "delta_total": float(result["totals"]["delta_total"]),
        },
    }


def _forecast_vs_solicitud_preview(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not payload:
        return None
    rows_payload = payload.get("rows") or []
    if not rows_payload:
        return None

    try:
        target_start = date.fromisoformat(str(payload.get("target_start")))
        target_end = date.fromisoformat(str(payload.get("target_end")))
    except Exception:
        return None

    sucursal_id = payload.get("sucursal_id")
    forecast_map: dict[int, dict[str, Any]] = {}
    receta_ids: list[int] = []
    for row in rows_payload:
        try:
            rid = int(row.get("receta_id") or 0)
        except Exception:
            rid = 0
        if rid <= 0:
            continue
        receta_ids.append(rid)
        forecast_map[rid] = {
            "receta_id": rid,
            "receta": str(row.get("receta") or ""),
            "forecast_qty": Decimal(str(row.get("forecast_qty") or 0)),
        }

    if not forecast_map:
        return None

    solicitud_qs = SolicitudVenta.objects.filter(
        fecha_inicio__lte=target_end,
        fecha_fin__gte=target_start,
    )
    solicitud_qs = solicitud_qs.filter(alcance=_ui_to_model_alcance(str(payload.get("alcance") or "")))
    if sucursal_id:
        solicitud_qs = solicitud_qs.filter(sucursal_id=sucursal_id)
    solicitud_map = {
        int(r["receta_id"]): Decimal(str(r["total"] or 0))
        for r in solicitud_qs.values("receta_id").annotate(total=Sum("cantidad"))
    }

    missing_receta_ids = [rid for rid in solicitud_map.keys() if rid not in forecast_map]
    if missing_receta_ids:
        for receta in Receta.objects.filter(id__in=missing_receta_ids).only("id", "nombre"):
            forecast_map[receta.id] = {
                "receta_id": receta.id,
                "receta": receta.nombre,
                "forecast_qty": Decimal("0"),
            }

    rows: list[dict[str, Any]] = []
    for rid, base in forecast_map.items():
        forecast_qty = Decimal(str(base["forecast_qty"] or 0))
        solicitud_qty = solicitud_map.get(rid, Decimal("0"))
        delta = solicitud_qty - forecast_qty
        tolerance = max(Decimal("1"), forecast_qty * Decimal("0.05"))
        if forecast_qty <= 0 and solicitud_qty > 0:
            status = "SIN_BASE"
            variacion_pct = None
        elif abs(delta) <= tolerance:
            status = "OK"
            variacion_pct = (
                (delta / forecast_qty) * Decimal("100")
                if forecast_qty > 0
                else Decimal("0")
            )
        elif delta > 0:
            status = "SOBRE"
            variacion_pct = (
                (delta / forecast_qty) * Decimal("100")
                if forecast_qty > 0
                else None
            )
        else:
            status = "BAJO"
            variacion_pct = (
                (delta / forecast_qty) * Decimal("100")
                if forecast_qty > 0
                else None
            )

        rows.append(
            {
                "receta_id": rid,
                "receta": base["receta"],
                "forecast_qty": forecast_qty,
                "solicitud_qty": solicitud_qty,
                "delta_qty": delta,
                "variacion_pct": variacion_pct,
                "status": status,
            }
        )

    rows.sort(key=lambda x: abs(x["delta_qty"]), reverse=True)
    total_forecast = sum((r["forecast_qty"] for r in rows), Decimal("0"))
    total_solicitud = sum((r["solicitud_qty"] for r in rows), Decimal("0"))
    return {
        "target_start": target_start,
        "target_end": target_end,
        "sucursal_id": sucursal_id,
        "sucursal_nombre": payload.get("sucursal_nombre") or "Todas",
        "rows": rows,
        "totals": {
            "forecast_total": total_forecast,
            "solicitud_total": total_solicitud,
            "delta_total": total_solicitud - total_forecast,
            "ok_count": len([r for r in rows if r["status"] == "OK"]),
            "sobre_count": len([r for r in rows if r["status"] == "SOBRE"]),
            "bajo_count": len([r for r in rows if r["status"] == "BAJO"]),
            "sin_base_count": len([r for r in rows if r["status"] == "SIN_BASE"]),
        },
    }


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
def pronosticos_descargar_plantilla(request: HttpRequest) -> HttpResponse:
    export_format = (request.GET.get("format") or "xlsx").strip().lower()
    headers = ["receta", "codigo_point", "periodo", "cantidad"]
    sample_rows = [
        ["Pastel Fresas Con Crema - Chico", "PFC-CHICO", timezone.localdate().strftime("%Y-%m"), "120"],
        ["Pan Vainilla Dawn - Chico", "PVD-CHICO", timezone.localdate().strftime("%Y-%m"), "220"],
    ]

    if export_format == "csv":
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="plantilla_pronosticos.csv"'
        writer = csv.writer(response)
        writer.writerow(headers)
        writer.writerows(sample_rows)
        return response

    wb = Workbook()
    ws = wb.active
    ws.title = "pronosticos"
    ws.append(headers)
    for row in sample_rows:
        ws.append(row)
    for col in ("A", "B", "C", "D"):
        ws.column_dimensions[col].width = 28
    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="plantilla_pronosticos.xlsx"'
    return response


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
def ventas_historicas_descargar_plantilla(request: HttpRequest) -> HttpResponse:
    export_format = (request.GET.get("format") or "xlsx").strip().lower()
    headers = ["receta", "codigo_point", "sucursal_codigo", "sucursal", "fecha", "cantidad", "tickets", "monto_total"]
    sample_rows = [
        ["Pastel Fresas Con Crema - Chico", "PFC-CHICO", "MATRIZ", "Matriz", timezone.localdate().isoformat(), "24", "18", "3580"],
        ["Pastel Fresas Con Crema - Chico", "PFC-CHICO", "NORTE", "Sucursal Norte", timezone.localdate().isoformat(), "13", "9", "1970"],
    ]

    if export_format == "csv":
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="plantilla_ventas_historicas.csv"'
        writer = csv.writer(response)
        writer.writerow(headers)
        writer.writerows(sample_rows)
        return response

    wb = Workbook()
    ws = wb.active
    ws.title = "ventas_historicas"
    ws.append(headers)
    for row in sample_rows:
        ws.append(row)
    for col in ("A", "B", "C", "D", "E", "F", "G", "H"):
        ws.column_dimensions[col].width = 24
    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="plantilla_ventas_historicas.xlsx"'
    return response


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
def solicitud_ventas_descargar_plantilla(request: HttpRequest) -> HttpResponse:
    export_format = (request.GET.get("format") or "xlsx").strip().lower()
    headers = [
        "receta",
        "codigo_point",
        "sucursal_codigo",
        "sucursal",
        "alcance",
        "periodo",
        "fecha_inicio",
        "fecha_fin",
        "cantidad",
    ]
    sample_rows = [
        [
            "Pastel Fresas Con Crema - Chico",
            "PFC-CHICO",
            "MATRIZ",
            "Matriz",
            "MES",
            timezone.localdate().strftime("%Y-%m"),
            "",
            "",
            "120",
        ],
        [
            "Pastel Fresas Con Crema - Chico",
            "PFC-CHICO",
            "NORTE",
            "Sucursal Norte",
            "SEMANA",
            "",
            timezone.localdate().isoformat(),
            "",
            "34",
        ],
    ]

    if export_format == "csv":
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="plantilla_solicitud_ventas.csv"'
        writer = csv.writer(response)
        writer.writerow(headers)
        writer.writerows(sample_rows)
        return response

    wb = Workbook()
    ws = wb.active
    ws.title = "solicitud_ventas"
    ws.append(headers)
    for row in sample_rows:
        ws.append(row)
    for col in ("A", "B", "C", "D", "E", "F", "G", "H", "I"):
        ws.column_dimensions[col].width = 24
    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="plantilla_solicitud_ventas.xlsx"'
    return response


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def solicitud_ventas_guardar(request: HttpRequest) -> HttpResponse:
    plan_id = (request.POST.get("plan_id") or "").strip()
    next_params: dict[str, str] = {}
    if plan_id:
        next_params["plan_id"] = plan_id

    receta = Receta.objects.filter(pk=request.POST.get("receta_id")).first()
    if receta is None:
        messages.error(request, "Selecciona una receta válida para registrar solicitud de ventas.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    sucursal = Sucursal.objects.filter(pk=request.POST.get("sucursal_id")).first()
    alcance = _ui_to_model_alcance(request.POST.get("alcance"))
    periodo_default = _normalize_periodo_mes(request.POST.get("periodo"))
    fecha_base_default = _parse_date_safe(request.POST.get("fecha_base")) or timezone.localdate()
    cantidad = _to_decimal_safe(request.POST.get("cantidad"))
    if cantidad <= 0:
        messages.error(request, "La cantidad solicitada debe ser mayor a 0.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    periodo, fecha_inicio, fecha_fin = _resolve_solicitud_window(
        alcance=alcance,
        periodo_raw=request.POST.get("periodo"),
        fecha_base_raw=request.POST.get("fecha_base"),
        fecha_inicio_raw=request.POST.get("fecha_inicio"),
        fecha_fin_raw=request.POST.get("fecha_fin"),
        periodo_default=periodo_default,
        fecha_base_default=fecha_base_default,
    )
    fuente = (request.POST.get("fuente") or "UI_SOL_VENTAS").strip()[:40] or "UI_SOL_VENTAS"
    solicitud, created = SolicitudVenta.objects.get_or_create(
        receta=receta,
        sucursal=sucursal,
        alcance=alcance,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        defaults={
            "periodo": periodo,
            "cantidad": cantidad,
            "fuente": fuente,
        },
    )
    if created:
        messages.success(request, "Solicitud de ventas registrada.")
    else:
        solicitud.periodo = periodo
        solicitud.cantidad = cantidad
        solicitud.fuente = fuente
        solicitud.save(update_fields=["periodo", "cantidad", "fuente", "actualizado_en"])
        messages.success(request, "Solicitud de ventas actualizada.")

    next_params["periodo"] = periodo
    if next_params:
        return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
    return redirect(reverse("recetas:plan_produccion"))


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def solicitud_ventas_importar(request: HttpRequest) -> HttpResponse:
    uploaded = request.FILES.get("archivo")
    plan_id = (request.POST.get("plan_id") or "").strip()
    modo = (request.POST.get("modo") or "replace").strip().lower()
    if modo not in {"replace", "accumulate"}:
        modo = "replace"
    periodo_default = _normalize_periodo_mes(request.POST.get("periodo_default"))
    fecha_base_default = _parse_date_safe(request.POST.get("fecha_base_default")) or timezone.localdate()
    alcance_default = _ui_to_model_alcance(request.POST.get("alcance_default"))
    fuente = (request.POST.get("fuente") or "UI_SOL_VENTAS").strip()[:40] or "UI_SOL_VENTAS"
    sucursal_default = Sucursal.objects.filter(pk=request.POST.get("sucursal_default_id")).first()

    next_params: dict[str, str] = {}
    if plan_id:
        next_params["plan_id"] = plan_id
    next_params["periodo"] = periodo_default

    if not uploaded:
        messages.error(request, "Selecciona un archivo para importar solicitudes de ventas.")
        return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")

    try:
        rows = _load_solicitud_ventas_rows(uploaded)
    except Exception as exc:
        messages.error(request, f"No se pudo leer el archivo: {exc}")
        return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")

    if not rows:
        messages.warning(request, "Archivo sin filas válidas para solicitud de ventas.")
        return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")

    created = 0
    updated = 0
    skipped = 0
    unresolved_recetas: list[str] = []
    unresolved_sucursales: list[str] = []

    for row in rows:
        receta_name = str(row.get("receta") or "").strip()
        codigo_point = str(row.get("codigo_point") or "").strip()
        receta = _resolve_receta_for_sales(receta_name, codigo_point)
        if receta is None:
            unresolved_recetas.append(receta_name or codigo_point or "sin_identificador")
            skipped += 1
            continue

        cantidad = _to_decimal_safe(row.get("cantidad"))
        if cantidad < 0:
            skipped += 1
            continue

        sucursal_name = str(row.get("sucursal") or "").strip()
        sucursal_codigo = str(row.get("sucursal_codigo") or "").strip()
        sucursal = _resolve_sucursal_for_sales(sucursal_name, sucursal_codigo, sucursal_default)
        if (sucursal_name or sucursal_codigo) and sucursal is None:
            unresolved_sucursales.append(sucursal_codigo or sucursal_name)
            skipped += 1
            continue

        alcance = _normalize_alcance_solicitud(str(row.get("alcance") or ""))
        if not str(row.get("alcance") or "").strip():
            alcance = alcance_default
        periodo, fecha_inicio, fecha_fin = _resolve_solicitud_window(
            alcance=alcance,
            periodo_raw=str(row.get("periodo") or ""),
            fecha_base_raw=row.get("fecha_base"),
            fecha_inicio_raw=row.get("fecha_inicio"),
            fecha_fin_raw=row.get("fecha_fin"),
            periodo_default=periodo_default,
            fecha_base_default=fecha_base_default,
        )

        record = SolicitudVenta.objects.filter(
            receta=receta,
            sucursal=sucursal,
            alcance=alcance,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
        ).first()
        if record:
            if modo == "accumulate":
                record.cantidad = Decimal(str(record.cantidad or 0)) + cantidad
            else:
                record.cantidad = cantidad
            record.periodo = periodo
            record.fuente = fuente
            record.save(update_fields=["cantidad", "periodo", "fuente", "actualizado_en"])
            updated += 1
        else:
            SolicitudVenta.objects.create(
                receta=receta,
                sucursal=sucursal,
                alcance=alcance,
                periodo=periodo,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                cantidad=cantidad,
                fuente=fuente,
            )
            created += 1

    messages.success(
        request,
        f"Solicitudes de ventas importadas. Creadas: {created}. Actualizadas: {updated}. Omitidas: {skipped}.",
    )
    if unresolved_recetas:
        sample = ", ".join(unresolved_recetas[:5])
        extra = "" if len(unresolved_recetas) <= 5 else f" (+{len(unresolved_recetas) - 5} más)"
        messages.warning(request, f"Sin receta equivalente para: {sample}{extra}.")
    if unresolved_sucursales:
        sample = ", ".join(unresolved_sucursales[:5])
        extra = "" if len(unresolved_sucursales) <= 5 else f" (+{len(unresolved_sucursales) - 5} más)"
        messages.warning(request, f"Sucursales no encontradas: {sample}{extra}.")
    return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def solicitud_ventas_aplicar_desde_forecast(request: HttpRequest) -> HttpResponse:
    plan_id = (request.POST.get("plan_id") or "").strip()
    next_params: dict[str, str] = {}
    if plan_id:
        next_params["plan_id"] = plan_id

    payload = request.session.get("pronostico_estadistico_preview")
    if not payload:
        messages.error(request, "No hay preview de pronóstico activo. Ejecuta primero la previsualización.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    sucursal_id = payload.get("sucursal_id")
    if not sucursal_id:
        messages.error(request, "Selecciona una sucursal en la previsualización para aplicar ajuste automático.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    compare = _forecast_vs_solicitud_preview(payload)
    if not compare or not compare.get("rows"):
        messages.warning(request, "No hay filas disponibles para aplicar ajustes.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    modo = (request.POST.get("modo") or "desviadas").strip().lower()
    receta_id = _to_int_safe(request.POST.get("receta_id"), default=0)
    rows = list(compare["rows"])
    if modo == "sobre":
        rows = [r for r in rows if r["status"] == "SOBRE"]
    elif modo == "bajo":
        rows = [r for r in rows if r["status"] == "BAJO"]
    elif modo == "receta":
        rows = [r for r in rows if int(r["receta_id"]) == receta_id]
    else:
        rows = [r for r in rows if r["status"] in {"SOBRE", "BAJO"}]

    if not rows:
        messages.info(request, "No hay filas objetivo para el ajuste seleccionado.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    alcance = _ui_to_model_alcance(payload.get("alcance"))
    periodo = _normalize_periodo_mes(payload.get("periodo"))
    target_start = _parse_date_safe(payload.get("target_start")) or compare["target_start"]
    target_end = _parse_date_safe(payload.get("target_end")) or compare["target_end"]
    fuente = (request.POST.get("fuente") or "AUTO_FORECAST_ADJUST").strip()[:40] or "AUTO_FORECAST_ADJUST"

    sucursal = Sucursal.objects.filter(pk=sucursal_id).first()
    if sucursal is None:
        messages.error(request, "La sucursal del preview ya no existe; vuelve a generar el pronóstico.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        forecast_qty = Decimal(str(row.get("forecast_qty") or 0))
        if forecast_qty < 0:
            skipped += 1
            continue
        receta = Receta.objects.filter(pk=row["receta_id"]).first()
        if receta is None:
            skipped += 1
            continue
        record, was_created = SolicitudVenta.objects.get_or_create(
            receta=receta,
            sucursal=sucursal,
            alcance=alcance,
            fecha_inicio=target_start,
            fecha_fin=target_end,
            defaults={
                "periodo": periodo,
                "cantidad": forecast_qty,
                "fuente": fuente,
            },
        )
        if was_created:
            created += 1
            continue
        record.periodo = periodo
        record.cantidad = forecast_qty
        record.fuente = fuente
        record.save(update_fields=["periodo", "cantidad", "fuente", "actualizado_en"])
        updated += 1

    messages.success(
        request,
        f"Ajuste aplicado desde forecast. Creadas: {created}. Actualizadas: {updated}. Omitidas: {skipped}.",
    )
    next_params["periodo"] = periodo
    return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def ventas_historicas_importar(request: HttpRequest) -> HttpResponse:
    uploaded = request.FILES.get("archivo")
    plan_id = (request.POST.get("plan_id") or "").strip()
    modo = (request.POST.get("modo") or "replace").strip().lower()
    if modo not in {"replace", "accumulate"}:
        modo = "replace"
    fuente = (request.POST.get("fuente") or "UI_VENTAS").strip()[:40] or "UI_VENTAS"
    sucursal_default = Sucursal.objects.filter(pk=request.POST.get("sucursal_default_id")).first()

    next_url = reverse("recetas:plan_produccion")
    if plan_id:
        next_url = f"{next_url}?{urlencode({'plan_id': plan_id})}"

    if not uploaded:
        messages.error(request, "Selecciona un archivo para importar historial de ventas.")
        return redirect(next_url)

    try:
        rows = _load_ventas_rows(uploaded)
    except Exception as exc:
        messages.error(request, f"No se pudo leer el archivo: {exc}")
        return redirect(next_url)

    if not rows:
        messages.warning(request, "Archivo sin filas válidas para historial de ventas.")
        return redirect(next_url)

    created = 0
    updated = 0
    skipped = 0
    unresolved_recetas: list[str] = []
    unresolved_sucursales: list[str] = []

    for row in rows:
        receta_name = str(row.get("receta") or "").strip()
        codigo_point = str(row.get("codigo_point") or "").strip()
        receta = _resolve_receta_for_sales(receta_name, codigo_point)
        if receta is None:
            unresolved_recetas.append(receta_name or codigo_point or "sin_identificador")
            skipped += 1
            continue

        fecha = _parse_date_safe(row.get("fecha"))
        if not fecha:
            skipped += 1
            continue

        cantidad = _to_decimal_safe(row.get("cantidad"))
        if cantidad < 0:
            skipped += 1
            continue

        sucursal_name = str(row.get("sucursal") or "").strip()
        sucursal_codigo = str(row.get("sucursal_codigo") or "").strip()
        sucursal = _resolve_sucursal_for_sales(sucursal_name, sucursal_codigo, sucursal_default)
        if (sucursal_name or sucursal_codigo) and sucursal is None:
            unresolved_sucursales.append(sucursal_codigo or sucursal_name)
            skipped += 1
            continue

        tickets = max(0, _to_int_safe(row.get("tickets"), default=0))
        monto_total = _to_decimal_safe(row.get("monto_total"))

        existing_qs = VentaHistorica.objects.filter(receta=receta, fecha=fecha)
        if sucursal:
            existing_qs = existing_qs.filter(sucursal=sucursal)
        else:
            existing_qs = existing_qs.filter(sucursal__isnull=True)
        existing = existing_qs.order_by("id").first()

        if existing:
            if modo == "accumulate":
                existing.cantidad = Decimal(str(existing.cantidad or 0)) + cantidad
                existing.tickets = int(existing.tickets or 0) + tickets
                if monto_total > 0:
                    existing.monto_total = Decimal(str(existing.monto_total or 0)) + monto_total
            else:
                existing.cantidad = cantidad
                existing.tickets = tickets
                existing.monto_total = monto_total if monto_total > 0 else None
            existing.fuente = fuente
            existing.save(update_fields=["cantidad", "tickets", "monto_total", "fuente", "actualizado_en"])
            updated += 1
        else:
            VentaHistorica.objects.create(
                receta=receta,
                sucursal=sucursal,
                fecha=fecha,
                cantidad=cantidad,
                tickets=tickets,
                monto_total=monto_total if monto_total > 0 else None,
                fuente=fuente,
            )
            created += 1

    messages.success(
        request,
        f"Historial de ventas importado. Creados: {created}. Actualizados: {updated}. Omitidos: {skipped}.",
    )
    if unresolved_recetas:
        sample = ", ".join(unresolved_recetas[:5])
        extra = "" if len(unresolved_recetas) <= 5 else f" (+{len(unresolved_recetas) - 5} más)"
        messages.warning(request, f"Sin receta equivalente para: {sample}{extra}.")
    if unresolved_sucursales:
        sample = ", ".join(unresolved_sucursales[:5])
        extra = "" if len(unresolved_sucursales) <= 5 else f" (+{len(unresolved_sucursales) - 5} más)"
        messages.warning(request, f"Sucursales no encontradas: {sample}{extra}.")
    return redirect(next_url)


@login_required
@permission_required("recetas.add_planproduccion", raise_exception=True)
@require_POST
def pronostico_estadistico_desde_historial(request: HttpRequest) -> HttpResponse:
    plan_id = (request.POST.get("plan_id") or "").strip()
    next_params: dict[str, str] = {}
    if plan_id:
        next_params["plan_id"] = plan_id

    alcance = (request.POST.get("alcance") or "mes").strip().lower()
    if alcance not in {"mes", "semana", "fin_semana"}:
        alcance = "mes"
    periodo = _normalize_periodo_mes(request.POST.get("periodo"))
    fecha_base = _parse_date_safe(request.POST.get("fecha_base")) or timezone.localdate()
    incluir_preparaciones = request.POST.get("incluir_preparaciones") == "1"
    run_mode = (request.POST.get("run_mode") or "preview").strip().lower()
    if run_mode not in {"preview", "apply_pronostico", "crear_plan", "aplicar_y_plan"}:
        run_mode = "preview"
    safety_pct = _to_decimal_safe(request.POST.get("safety_pct"))
    if safety_pct < Decimal("-30"):
        safety_pct = Decimal("-30")
    if safety_pct > Decimal("100"):
        safety_pct = Decimal("100")

    sucursal = Sucursal.objects.filter(pk=request.POST.get("sucursal_id")).first()

    resultado = _build_forecast_from_history(
        alcance=alcance,
        periodo=periodo,
        fecha_base=fecha_base,
        sucursal=sucursal,
        incluir_preparaciones=incluir_preparaciones,
        safety_pct=safety_pct,
    )
    request.session["pronostico_estadistico_preview"] = _forecast_session_payload(resultado, top_rows=120)

    rows = resultado["rows"]
    if not rows:
        messages.warning(request, "No hay suficiente historial para generar pronóstico estadístico con esos filtros.")
        if next_params:
            return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
        return redirect(reverse("recetas:plan_produccion"))

    created_forecast = 0
    updated_forecast = 0
    if run_mode in {"apply_pronostico", "aplicar_y_plan"}:
        if alcance != "mes":
            messages.warning(request, "Aplicar pronóstico mensual solo está disponible cuando el alcance es 'Mes'.")
        else:
            for row in rows:
                receta = Receta.objects.filter(pk=row["receta_id"]).first()
                if receta is None:
                    continue
                forecast_qty = Decimal(str(row["forecast_qty"]))
                current = PronosticoVenta.objects.filter(receta=receta, periodo=resultado["periodo"]).first()
                if current:
                    current.cantidad = forecast_qty
                    current.fuente = "AUTO_HISTORIAL"
                    current.save(update_fields=["cantidad", "fuente", "actualizado_en"])
                    updated_forecast += 1
                else:
                    PronosticoVenta.objects.create(
                        receta=receta,
                        periodo=resultado["periodo"],
                        cantidad=forecast_qty,
                        fuente="AUTO_HISTORIAL",
                    )
                    created_forecast += 1

    created_plan = None
    if run_mode in {"crear_plan", "aplicar_y_plan"}:
        target_start = resultado["target_start"]
        default_name = f"Plan estadístico {resultado['target_start']} a {resultado['target_end']}"
        name = (request.POST.get("nombre_plan") or "").strip() or default_name
        plan = PlanProduccion.objects.create(
            nombre=name[:140],
            fecha_produccion=target_start,
            notas=(
                f"Generado por pronóstico estadístico ({alcance}) "
                f"{resultado['target_start']}..{resultado['target_end']} "
                f"- sucursal: {resultado['sucursal_nombre']}"
            )[:500],
            creado_por=request.user if request.user.is_authenticated else None,
        )
        lines = 0
        for row in rows:
            qty = Decimal(str(row["forecast_qty"]))
            if qty <= 0:
                continue
            PlanProduccionItem.objects.create(
                plan=plan,
                receta_id=row["receta_id"],
                cantidad=qty,
                notas="Pronóstico estadístico",
            )
            lines += 1
        if lines == 0:
            plan.delete()
            messages.warning(request, "No se creó plan: el pronóstico estadístico resultó en cantidades 0.")
        else:
            created_plan = plan
            next_params["plan_id"] = str(plan.id)
            messages.success(request, f"Plan estadístico creado con {lines} renglones.")

    if run_mode == "preview":
        messages.success(request, f"Vista previa generada ({len(rows)} recetas) para {resultado['sucursal_nombre']}.")
    if created_forecast or updated_forecast:
        messages.success(
            request,
            f"Pronóstico mensual aplicado desde historial. Creados: {created_forecast}. Actualizados: {updated_forecast}.",
        )
    if run_mode in {"crear_plan", "aplicar_y_plan"} and created_plan is None:
        messages.info(request, "Se generó la vista previa estadística, pero no se creó plan.")

    next_params["periodo"] = resultado["periodo"]
    if next_params:
        return redirect(f"{reverse('recetas:plan_produccion')}?{urlencode(next_params)}")
    return redirect(reverse("recetas:plan_produccion"))


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def pronosticos_importar(request: HttpRequest) -> HttpResponse:
    uploaded = request.FILES.get("archivo")
    plan_id = (request.POST.get("plan_id") or "").strip()
    modo = (request.POST.get("modo") or "replace").strip().lower()
    if modo not in {"replace", "accumulate"}:
        modo = "replace"
    fuente = (request.POST.get("fuente") or "UI_PRONOSTICO").strip()[:40] or "UI_PRONOSTICO"
    periodo_default = _normalize_periodo_mes(request.POST.get("periodo_default"))

    next_url = reverse("recetas:plan_produccion")
    if plan_id:
        next_url = f"{next_url}?{urlencode({'plan_id': plan_id})}"

    if not uploaded:
        messages.error(request, "Selecciona un archivo para importar pronósticos.")
        return redirect(next_url)

    try:
        rows = _load_pronostico_rows(uploaded)
    except Exception as exc:
        messages.error(request, f"No se pudo leer el archivo: {exc}")
        return redirect(next_url)

    if not rows:
        messages.warning(request, "Archivo sin filas válidas para pronósticos.")
        return redirect(next_url)

    created = 0
    updated = 0
    skipped = 0
    unresolved: list[str] = []

    for row in rows:
        receta_name = (row.get("receta") or "").strip()
        codigo_point = (row.get("codigo_point") or "").strip()
        cantidad = _to_decimal_safe(row.get("cantidad"))
        periodo = _normalize_periodo_mes(str(row.get("periodo") or row.get("mes") or periodo_default))

        if cantidad < 0:
            skipped += 1
            continue
        if not receta_name and not codigo_point:
            skipped += 1
            continue

        receta = None
        if codigo_point:
            receta = Receta.objects.filter(codigo_point__iexact=codigo_point).order_by("id").first()
        if receta is None and receta_name:
            receta = Receta.objects.filter(nombre_normalizado=normalizar_nombre(receta_name)).order_by("id").first()

        if receta is None:
            unresolved.append(receta_name or codigo_point)
            skipped += 1
            continue

        pronostico = PronosticoVenta.objects.filter(receta=receta, periodo=periodo).first()
        if pronostico:
            if modo == "accumulate":
                pronostico.cantidad = Decimal(str(pronostico.cantidad or 0)) + cantidad
            else:
                pronostico.cantidad = cantidad
            pronostico.fuente = fuente
            pronostico.save(update_fields=["cantidad", "fuente", "actualizado_en"])
            updated += 1
        else:
            PronosticoVenta.objects.create(
                receta=receta,
                periodo=periodo,
                cantidad=cantidad,
                fuente=fuente,
            )
            created += 1

    messages.success(
        request,
        f"Pronósticos importados. Creados: {created}. Actualizados: {updated}. Omitidos: {skipped}. Modo: {'acumular' if modo == 'accumulate' else 'reemplazar'}.",
    )
    if unresolved:
        sample = ", ".join(unresolved[:5])
        extra = "" if len(unresolved) <= 5 else f" (+{len(unresolved) - 5} más)"
        messages.warning(
            request,
            f"Sin receta equivalente para: {sample}{extra}. Homologa nombre/código point y vuelve a importar.",
        )
    return redirect(next_url)


@login_required
@permission_required("recetas.add_planproduccion", raise_exception=True)
@require_POST
def plan_produccion_generar_desde_pronostico(request: HttpRequest) -> HttpResponse:
    periodo = _normalize_periodo_mes(request.POST.get("periodo"))
    nombre = (request.POST.get("nombre") or "").strip()
    fecha_raw = (request.POST.get("fecha_produccion") or "").strip()
    incluir_preparaciones = request.POST.get("incluir_preparaciones") == "1"

    if not nombre:
        nombre = f"Plan desde pronóstico {periodo}"

    try:
        fecha_plan = date.fromisoformat(fecha_raw) if fecha_raw else date.fromisoformat(f"{periodo}-01")
    except Exception:
        messages.error(request, "Fecha de producción inválida.")
        return redirect(f"{reverse('recetas:plan_produccion')}?periodo={periodo}")

    pronosticos_qs = PronosticoVenta.objects.filter(periodo=periodo).select_related("receta").order_by("receta__nombre")
    if not incluir_preparaciones:
        pronosticos_qs = pronosticos_qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)

    pronosticos = list(pronosticos_qs)
    if not pronosticos:
        messages.warning(
            request,
            "No hay pronósticos para generar plan en ese período con los filtros actuales.",
        )
        return redirect(f"{reverse('recetas:plan_produccion')}?periodo={periodo}")

    plan = PlanProduccion.objects.create(
        nombre=nombre[:140],
        fecha_produccion=fecha_plan,
        notas=f"Generado desde pronóstico {periodo}",
        creado_por=request.user if request.user.is_authenticated else None,
    )

    created = 0
    skipped = 0
    for p in pronosticos:
        qty = Decimal(str(p.cantidad or 0))
        if qty <= 0:
            skipped += 1
            continue
        PlanProduccionItem.objects.create(
            plan=plan,
            receta=p.receta,
            cantidad=qty,
            notas=f"Pronóstico {periodo}",
        )
        created += 1

    if created == 0:
        plan.delete()
        messages.warning(
            request,
            "No se creó plan: todos los pronósticos tenían cantidad 0.",
        )
        return redirect(f"{reverse('recetas:plan_produccion')}?periodo={periodo}")

    messages.success(
        request,
        f"Plan generado desde pronóstico {periodo}. Renglones creados: {created}. Omitidos por cantidad 0: {skipped}.",
    )
    return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}&periodo={periodo}")


@login_required
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
    plan_vs_pronostico = _plan_vs_pronostico(plan_actual) if plan_actual else None
    periodo_pronostico_default = _normalize_periodo_mes(request.GET.get("periodo"))
    mrp_periodo = _normalize_periodo_mes(request.GET.get("mrp_periodo"))
    mrp_periodo_tipo = (request.GET.get("mrp_periodo_tipo") or "mes").strip().lower()
    alcance_estadistico = (request.GET.get("alcance_estadistico") or "mes").strip().lower()
    if alcance_estadistico not in {"mes", "semana", "fin_semana"}:
        alcance_estadistico = "mes"
    fecha_base_estadistica = request.GET.get("fecha_base_estadistica") or timezone.localdate().isoformat()
    if mrp_periodo_tipo not in {"mes", "q1", "q2"}:
        mrp_periodo_tipo = "mes"
    mrp_periodo_resumen = _periodo_mrp_resumen(mrp_periodo, mrp_periodo_tipo)
    pronosticos_unavailable = False
    ventas_historicas_unavailable = False
    solicitudes_venta_unavailable = False
    try:
        pronosticos_periodo = PronosticoVenta.objects.filter(periodo=periodo_pronostico_default)
        pronosticos_periodo_count = pronosticos_periodo.count()
        pronosticos_periodo_total = pronosticos_periodo.aggregate(total=Sum("cantidad")).get("total") or Decimal("0")
    except (OperationalError, ProgrammingError):
        pronosticos_periodo_count = 0
        pronosticos_periodo_total = Decimal("0")
        pronosticos_unavailable = True
    try:
        ventas_historicas_count = VentaHistorica.objects.count()
        ventas_historicas_total = VentaHistorica.objects.aggregate(total=Sum("cantidad")).get("total") or Decimal("0")
        ventas_hist_fecha_max = VentaHistorica.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    except (OperationalError, ProgrammingError):
        ventas_historicas_count = 0
        ventas_historicas_total = Decimal("0")
        ventas_hist_fecha_max = None
        ventas_historicas_unavailable = True
    try:
        solicitudes_venta_periodo = SolicitudVenta.objects.filter(periodo=periodo_pronostico_default)
        solicitudes_venta_count = solicitudes_venta_periodo.count()
        solicitudes_venta_total = solicitudes_venta_periodo.aggregate(total=Sum("cantidad")).get("total") or Decimal("0")
        solicitudes_venta_fecha_max = solicitudes_venta_periodo.order_by("-fecha_inicio").values_list("fecha_inicio", flat=True).first()
    except (OperationalError, ProgrammingError):
        solicitudes_venta_count = 0
        solicitudes_venta_total = Decimal("0")
        solicitudes_venta_fecha_max = None
        solicitudes_venta_unavailable = True
    forecast_preview = request.session.get("pronostico_estadistico_preview")
    try:
        forecast_vs_solicitud = _forecast_vs_solicitud_preview(forecast_preview)
    except (OperationalError, ProgrammingError):
        forecast_vs_solicitud = None
        solicitudes_venta_unavailable = True
    sucursales = Sucursal.objects.filter(activa=True).order_by("codigo", "nombre")
    return render(
        request,
        "recetas/plan_produccion.html",
        {
            "planes": planes[:30],
            "plan_actual": plan_actual,
            "recetas_disponibles": recetas_disponibles,
            "explosion": explosion,
            "plan_vs_pronostico": plan_vs_pronostico,
            "periodo_pronostico_default": periodo_pronostico_default,
            "mrp_periodo": mrp_periodo,
            "mrp_periodo_tipo": mrp_periodo_tipo,
            "mrp_periodo_resumen": mrp_periodo_resumen,
            "pronosticos_periodo_count": pronosticos_periodo_count,
            "pronosticos_periodo_total": pronosticos_periodo_total,
            "pronosticos_unavailable": pronosticos_unavailable,
            "ventas_historicas_count": ventas_historicas_count,
            "ventas_historicas_total": ventas_historicas_total,
            "ventas_hist_fecha_max": ventas_hist_fecha_max,
            "ventas_historicas_unavailable": ventas_historicas_unavailable,
            "solicitudes_venta_count": solicitudes_venta_count,
            "solicitudes_venta_total": solicitudes_venta_total,
            "solicitudes_venta_fecha_max": solicitudes_venta_fecha_max,
            "solicitudes_venta_unavailable": solicitudes_venta_unavailable,
            "forecast_preview": forecast_preview,
            "forecast_vs_solicitud": forecast_vs_solicitud,
            "sucursales": sucursales,
            "alcance_estadistico": alcance_estadistico,
            "fecha_base_estadistica": fecha_base_estadistica,
        },
    )


@login_required
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


@login_required
@permission_required("recetas.view_planproduccion", raise_exception=True)
def plan_produccion_periodo_export(request: HttpRequest) -> HttpResponse:
    periodo = _normalize_periodo_mes(request.GET.get("mrp_periodo"))
    periodo_tipo = (request.GET.get("mrp_periodo_tipo") or "mes").strip().lower()
    if periodo_tipo not in {"mes", "q1", "q2"}:
        periodo_tipo = "mes"
    export_format = (request.GET.get("format") or "csv").strip().lower()
    resumen = _periodo_mrp_resumen(periodo, periodo_tipo)
    if export_format == "xlsx":
        return _export_periodo_mrp_xlsx(resumen)
    return _export_periodo_mrp_csv(resumen)


@login_required
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


@login_required
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


@login_required
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


@login_required
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


@login_required
@permission_required("recetas.change_planproduccion", raise_exception=True)
@require_POST
def plan_produccion_item_delete(request: HttpRequest, plan_id: int, item_id: int) -> HttpResponse:
    plan = get_object_or_404(PlanProduccion, pk=plan_id)
    item = get_object_or_404(PlanProduccionItem, pk=item_id, plan=plan)
    item.delete()
    messages.success(request, "Renglón eliminado del plan.")
    return redirect(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")


@login_required
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

    if creadas == 0 and actualizadas == 0:
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
