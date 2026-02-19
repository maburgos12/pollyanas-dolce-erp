import csv
from io import BytesIO
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST
from openpyxl import Workbook

from core.access import can_manage_compras, can_view_compras
from core.audit import log_event
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, Proveedor
from recetas.models import PlanProduccion

from .models import OrdenCompra, RecepcionCompra, SolicitudCompra


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value or default)
    except (InvalidOperation, TypeError):
        return Decimal(default)


def _can_transition_solicitud(current: str, new: str) -> bool:
    transitions = {
        SolicitudCompra.STATUS_BORRADOR: {SolicitudCompra.STATUS_EN_REVISION, SolicitudCompra.STATUS_APROBADA, SolicitudCompra.STATUS_RECHAZADA},
        SolicitudCompra.STATUS_EN_REVISION: {SolicitudCompra.STATUS_APROBADA, SolicitudCompra.STATUS_RECHAZADA},
        SolicitudCompra.STATUS_APROBADA: set(),
        SolicitudCompra.STATUS_RECHAZADA: set(),
    }
    return new in transitions.get(current, set())


def _can_transition_orden(current: str, new: str) -> bool:
    transitions = {
        OrdenCompra.STATUS_BORRADOR: {OrdenCompra.STATUS_ENVIADA},
        OrdenCompra.STATUS_ENVIADA: {OrdenCompra.STATUS_CONFIRMADA, OrdenCompra.STATUS_PARCIAL},
        OrdenCompra.STATUS_CONFIRMADA: {OrdenCompra.STATUS_PARCIAL, OrdenCompra.STATUS_CERRADA},
        OrdenCompra.STATUS_PARCIAL: {OrdenCompra.STATUS_CERRADA},
        OrdenCompra.STATUS_CERRADA: set(),
    }
    return new in transitions.get(current, set())


def _can_transition_recepcion(current: str, new: str) -> bool:
    transitions = {
        RecepcionCompra.STATUS_PENDIENTE: {RecepcionCompra.STATUS_DIFERENCIAS, RecepcionCompra.STATUS_CERRADA},
        RecepcionCompra.STATUS_DIFERENCIAS: {RecepcionCompra.STATUS_CERRADA},
        RecepcionCompra.STATUS_CERRADA: set(),
    }
    return new in transitions.get(current, set())


def _build_insumo_options():
    insumos = list(Insumo.objects.filter(activo=True).order_by("nombre")[:200])
    existencias = {
        e.insumo_id: e
        for e in ExistenciaInsumo.objects.filter(insumo_id__in=[i.id for i in insumos])
    }
    options = []
    for insumo in insumos:
        ex = existencias.get(insumo.id)
        stock_actual = ex.stock_actual if ex else Decimal("0")
        punto_reorden = ex.punto_reorden if ex else Decimal("0")
        recomendado = max(punto_reorden - stock_actual, Decimal("0"))
        options.append(
            {
                "id": insumo.id,
                "nombre": insumo.nombre,
                "proveedor_sugerido": insumo.proveedor_principal.nombre if insumo.proveedor_principal_id else "",
                "stock_actual": stock_actual,
                "punto_reorden": punto_reorden,
                "recomendado": recomendado,
            }
        )
    return options


def _solicitudes_print_folio() -> str:
    now = timezone.localtime()
    return f"SC-{now.strftime('%Y%m%d-%H%M%S')}"


def _export_solicitudes_csv(solicitudes, source_filter: str, plan_filter: str, reabasto_filter: str) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="solicitudes_compras_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "Folio",
            "Area",
            "Solicitante",
            "Origen",
            "Plan",
            "Insumo",
            "Proveedor sugerido",
            "Cantidad",
            "Fecha requerida",
            "Estatus",
            "Reabasto",
            "Detalle reabasto",
            "Filtro origen",
            "Filtro plan",
            "Filtro reabasto",
        ]
    )
    for s in solicitudes:
        writer.writerow(
            [
                s.folio,
                s.area,
                s.solicitante,
                "PLAN" if s.source_tipo == "plan" else "MANUAL",
                s.source_plan_id or "",
                s.insumo.nombre,
                s.proveedor_sugerido.nombre if s.proveedor_sugerido_id else "",
                s.cantidad,
                s.fecha_requerida,
                s.get_estatus_display(),
                s.reabasto_texto,
                s.reabasto_detalle,
                source_filter,
                plan_filter or "",
                reabasto_filter,
            ]
        )
    return response


def _export_solicitudes_xlsx(solicitudes, source_filter: str, plan_filter: str, reabasto_filter: str) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Solicitudes"
    ws.append(
        [
            "Folio",
            "Area",
            "Solicitante",
            "Origen",
            "Plan",
            "Insumo",
            "Proveedor sugerido",
            "Cantidad",
            "Fecha requerida",
            "Estatus",
            "Reabasto",
            "Detalle reabasto",
            "Filtro origen",
            "Filtro plan",
            "Filtro reabasto",
        ]
    )
    for s in solicitudes:
        ws.append(
            [
                s.folio,
                s.area,
                s.solicitante,
                "PLAN" if s.source_tipo == "plan" else "MANUAL",
                s.source_plan_id or "",
                s.insumo.nombre,
                s.proveedor_sugerido.nombre if s.proveedor_sugerido_id else "",
                float(s.cantidad or 0),
                s.fecha_requerida.isoformat() if s.fecha_requerida else "",
                s.get_estatus_display(),
                s.reabasto_texto,
                s.reabasto_detalle,
                source_filter,
                plan_filter or "",
                reabasto_filter,
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="solicitudes_compras_{now_str}.xlsx"'
    return response


def _filtered_solicitudes(source_filter_raw: str, plan_filter_raw: str, reabasto_filter_raw: str):
    source_filter = (source_filter_raw or "all").lower()
    if source_filter not in {"all", "manual", "plan"}:
        source_filter = "all"
    plan_filter = (plan_filter_raw or "").strip()

    solicitudes_qs = SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido").all()
    if source_filter == "plan":
        solicitudes_qs = solicitudes_qs.filter(area__startswith="PLAN_PRODUCCION:")
    elif source_filter == "manual":
        solicitudes_qs = solicitudes_qs.exclude(area__startswith="PLAN_PRODUCCION:")

    if plan_filter:
        solicitudes_qs = solicitudes_qs.filter(area=f"PLAN_PRODUCCION:{plan_filter}")

    solicitudes = list(solicitudes_qs[:300])
    insumo_ids = [s.insumo_id for s in solicitudes]
    existencias = {
        e.insumo_id: e
        for e in ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids)
    }

    plan_ids = set()
    for s in solicitudes:
        if (s.area or "").startswith("PLAN_PRODUCCION:"):
            _, _, maybe_id = s.area.partition(":")
            if maybe_id.isdigit():
                plan_ids.add(int(maybe_id))
    planes_map = {
        p.id: p
        for p in PlanProduccion.objects.filter(id__in=plan_ids)
    }

    for s in solicitudes:
        ex = existencias.get(s.insumo_id)
        stock_actual = ex.stock_actual if ex else Decimal("0")
        punto_reorden = ex.punto_reorden if ex else Decimal("0")
        if stock_actual <= Decimal("0"):
            s.reabasto_nivel = "critico"
            s.reabasto_texto = "Sin stock"
        elif stock_actual < punto_reorden:
            s.reabasto_nivel = "bajo"
            s.reabasto_texto = "Bajo reorden"
        else:
            s.reabasto_nivel = "ok"
            s.reabasto_texto = "Stock suficiente"
        s.reabasto_detalle = f"Stock {stock_actual} / Reorden {punto_reorden}"
        s.source_tipo = "manual"
        s.source_plan_id = None
        s.source_plan_nombre = ""
        if (s.area or "").startswith("PLAN_PRODUCCION:"):
            _, _, maybe_id = s.area.partition(":")
            if maybe_id.isdigit():
                plan_id_int = int(maybe_id)
                s.source_tipo = "plan"
                s.source_plan_id = plan_id_int
                s.source_plan_nombre = planes_map.get(plan_id_int).nombre if plan_id_int in planes_map else f"Plan {plan_id_int}"

    open_orders_by_solicitud = {}
    solicitud_ids = [s.id for s in solicitudes]
    if solicitud_ids:
        for orden in (
            OrdenCompra.objects.filter(solicitud_id__in=solicitud_ids)
            .exclude(estatus=OrdenCompra.STATUS_CERRADA)
            .order_by("-creado_en")
        ):
            open_orders_by_solicitud.setdefault(orden.solicitud_id, orden)

    for s in solicitudes:
        open_order = open_orders_by_solicitud.get(s.id)
        s.has_open_order = bool(open_order)
        s.open_order_id = open_order.id if open_order else None
        s.open_order_folio = open_order.folio if open_order else ""

    reabasto_filter = (reabasto_filter_raw or "all").lower()
    if reabasto_filter in {"critico", "bajo", "ok"}:
        solicitudes = [s for s in solicitudes if s.reabasto_nivel == reabasto_filter]
    else:
        reabasto_filter = "all"

    plan_ids_all = set()
    for area_val in SolicitudCompra.objects.filter(area__startswith="PLAN_PRODUCCION:").values_list("area", flat=True).distinct():
        _, _, maybe_id = (area_val or "").partition(":")
        if maybe_id.isdigit():
            plan_ids_all.add(int(maybe_id))
    plan_options = list(PlanProduccion.objects.filter(id__in=plan_ids_all).order_by("-fecha_produccion", "-id")[:100])

    return solicitudes, source_filter, plan_filter, reabasto_filter, plan_options


@login_required
def solicitudes(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    if request.method == "POST":
        if not can_manage_compras(request.user):
            raise PermissionDenied("No tienes permisos para crear solicitudes.")
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            insumo = get_object_or_404(Insumo, pk=insumo_id)
            solicitud = SolicitudCompra.objects.create(
                area=request.POST.get("area", "General").strip() or "General",
                solicitante=request.POST.get("solicitante", request.user.username).strip() or request.user.username,
                insumo=insumo,
                proveedor_sugerido=insumo.proveedor_principal,
                cantidad=_to_decimal(request.POST.get("cantidad"), "1"),
                fecha_requerida=request.POST.get("fecha_requerida") or None,
                estatus=request.POST.get("estatus") or SolicitudCompra.STATUS_BORRADOR,
            )
            log_event(
                request.user,
                "CREATE",
                "compras.SolicitudCompra",
                solicitud.id,
                {"folio": solicitud.folio, "estatus": solicitud.estatus},
            )
        return redirect("compras:solicitudes")

    solicitudes, source_filter, plan_filter, reabasto_filter, plan_options = _filtered_solicitudes(
        request.GET.get("source"),
        request.GET.get("plan_id"),
        request.GET.get("reabasto"),
    )

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_solicitudes_csv(solicitudes, source_filter, plan_filter, reabasto_filter)
    if export_format == "xlsx":
        return _export_solicitudes_xlsx(solicitudes, source_filter, plan_filter, reabasto_filter)

    query_without_export = request.GET.copy()
    query_without_export.pop("export", None)

    context = {
        "solicitudes": solicitudes,
        "insumo_options": _build_insumo_options(),
        "status_choices": SolicitudCompra.STATUS_CHOICES,
        "can_manage_compras": can_manage_compras(request.user),
        "reabasto_filter": reabasto_filter,
        "source_filter": source_filter,
        "plan_filter": plan_filter,
        "plan_options": plan_options,
        "current_query": query_without_export.urlencode(),
    }
    return render(request, "compras/solicitudes.html", context)


@login_required
def solicitudes_print(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    solicitudes, source_filter, plan_filter, reabasto_filter, _ = _filtered_solicitudes(
        request.GET.get("source"),
        request.GET.get("plan_id"),
        request.GET.get("reabasto"),
    )

    total_cantidad = sum((s.cantidad for s in solicitudes), Decimal("0"))
    criticos_count = sum(1 for s in solicitudes if s.reabasto_nivel == "critico")
    bajos_count = sum(1 for s in solicitudes if s.reabasto_nivel == "bajo")
    ok_count = sum(1 for s in solicitudes if s.reabasto_nivel == "ok")

    context = {
        "solicitudes": solicitudes,
        "source_filter": source_filter,
        "plan_filter": plan_filter or "-",
        "reabasto_filter": reabasto_filter,
        "total_cantidad": total_cantidad,
        "criticos_count": criticos_count,
        "bajos_count": bajos_count,
        "ok_count": ok_count,
        "generated_at": timezone.localtime(),
        "generated_by": request.user.username,
        "document_folio": _solicitudes_print_folio(),
        "status_autorizacion": "Pendiente de firmas",
        "return_query": request.GET.urlencode(),
    }
    return render(request, "compras/solicitudes_print.html", context)


@login_required
def ordenes(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    if request.method == "POST":
        if not can_manage_compras(request.user):
            raise PermissionDenied("No tienes permisos para crear órdenes.")
        proveedor_id = request.POST.get("proveedor_id")
        if proveedor_id:
            solicitud_raw = request.POST.get("solicitud_id")
            if not solicitud_raw:
                messages.error(request, "Debes seleccionar una solicitud aprobada para crear una orden.")
                return redirect("compras:ordenes")

            solicitud = get_object_or_404(SolicitudCompra, pk=solicitud_raw)
            if solicitud.estatus != SolicitudCompra.STATUS_APROBADA:
                messages.error(request, f"La solicitud {solicitud.folio} no está aprobada.")
                return redirect("compras:ordenes")

            orden = OrdenCompra.objects.create(
                proveedor_id=proveedor_id,
                solicitud=solicitud,
                referencia=f"SOLICITUD:{solicitud.folio}",
                fecha_emision=request.POST.get("fecha_emision") or None,
                fecha_entrega_estimada=request.POST.get("fecha_entrega_estimada") or None,
                monto_estimado=_to_decimal(request.POST.get("monto_estimado"), "0"),
                estatus=request.POST.get("estatus") or OrdenCompra.STATUS_BORRADOR,
            )
            log_event(
                request.user,
                "CREATE",
                "compras.OrdenCompra",
                orden.id,
                {"folio": orden.folio, "estatus": orden.estatus},
            )
        return redirect("compras:ordenes")

    context = {
        "ordenes": OrdenCompra.objects.select_related("proveedor", "solicitud")[:50],
        "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre")[:200],
        "solicitudes": SolicitudCompra.objects.filter(estatus=SolicitudCompra.STATUS_APROBADA).order_by("-creado_en")[:200],
        "status_choices": OrdenCompra.STATUS_CHOICES,
        "can_manage_compras": can_manage_compras(request.user),
    }
    return render(request, "compras/ordenes.html", context)


@login_required
def recepciones(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    if request.method == "POST":
        if not can_manage_compras(request.user):
            raise PermissionDenied("No tienes permisos para registrar recepciones.")
        orden_id = request.POST.get("orden_id")
        if orden_id:
            orden = get_object_or_404(OrdenCompra, pk=orden_id)
            if orden.estatus in {OrdenCompra.STATUS_BORRADOR, OrdenCompra.STATUS_CERRADA}:
                messages.error(request, f"La orden {orden.folio} no admite recepciones en estatus {orden.get_estatus_display()}.")
                return redirect("compras:recepciones")

            recepcion = RecepcionCompra.objects.create(
                orden=orden,
                fecha_recepcion=request.POST.get("fecha_recepcion") or None,
                conformidad_pct=_to_decimal(request.POST.get("conformidad_pct"), "100"),
                estatus=request.POST.get("estatus") or RecepcionCompra.STATUS_PENDIENTE,
                observaciones=request.POST.get("observaciones", "").strip(),
            )
            log_event(
                request.user,
                "CREATE",
                "compras.RecepcionCompra",
                recepcion.id,
                {"folio": recepcion.folio, "estatus": recepcion.estatus},
            )
            if recepcion.estatus == RecepcionCompra.STATUS_CERRADA and orden.estatus != OrdenCompra.STATUS_CERRADA:
                orden_prev = orden.estatus
                orden.estatus = OrdenCompra.STATUS_CERRADA
                orden.save(update_fields=["estatus"])
                log_event(
                    request.user,
                    "APPROVE",
                    "compras.OrdenCompra",
                    orden.id,
                    {"from": orden_prev, "to": OrdenCompra.STATUS_CERRADA, "folio": orden.folio, "source": recepcion.folio},
                )
        return redirect("compras:recepciones")

    context = {
        "recepciones": RecepcionCompra.objects.select_related("orden", "orden__proveedor")[:50],
        "ordenes": OrdenCompra.objects.select_related("proveedor").exclude(estatus=OrdenCompra.STATUS_BORRADOR).exclude(estatus=OrdenCompra.STATUS_CERRADA).order_by("-creado_en")[:200],
        "status_choices": RecepcionCompra.STATUS_CHOICES,
        "can_manage_compras": can_manage_compras(request.user),
    }
    return render(request, "compras/recepciones.html", context)


@login_required
@require_POST
def actualizar_solicitud_estatus(request: HttpRequest, pk: int, estatus: str) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para aprobar/rechazar solicitudes.")

    solicitud = get_object_or_404(SolicitudCompra, pk=pk)
    prev = solicitud.estatus
    if _can_transition_solicitud(prev, estatus):
        solicitud.estatus = estatus
        solicitud.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.SolicitudCompra",
            solicitud.id,
            {"from": prev, "to": estatus, "folio": solicitud.folio},
        )
    return redirect("compras:solicitudes")


@login_required
@require_POST
def actualizar_orden_estatus(request: HttpRequest, pk: int, estatus: str) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para operar órdenes.")

    orden = get_object_or_404(OrdenCompra, pk=pk)
    prev = orden.estatus

    if estatus == OrdenCompra.STATUS_CERRADA:
        has_closed_recepcion = RecepcionCompra.objects.filter(
            orden=orden,
            estatus=RecepcionCompra.STATUS_CERRADA,
        ).exists()
        if not has_closed_recepcion:
            messages.error(request, f"No puedes cerrar {orden.folio} sin al menos una recepción cerrada.")
            return redirect("compras:ordenes")

    if _can_transition_orden(prev, estatus):
        orden.estatus = estatus
        orden.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.OrdenCompra",
            orden.id,
            {"from": prev, "to": estatus, "folio": orden.folio},
        )
    return redirect("compras:ordenes")


@login_required
@require_POST
def actualizar_recepcion_estatus(request: HttpRequest, pk: int, estatus: str) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para cerrar recepciones.")

    recepcion = get_object_or_404(RecepcionCompra, pk=pk)
    prev = recepcion.estatus
    if _can_transition_recepcion(prev, estatus):
        recepcion.estatus = estatus
        recepcion.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.RecepcionCompra",
            recepcion.id,
            {"from": prev, "to": estatus, "folio": recepcion.folio},
        )

        # Si la recepción quedó cerrada, marcamos la orden cerrada automáticamente.
        if estatus == RecepcionCompra.STATUS_CERRADA and recepcion.orden.estatus != OrdenCompra.STATUS_CERRADA:
            orden_prev = recepcion.orden.estatus
            recepcion.orden.estatus = OrdenCompra.STATUS_CERRADA
            recepcion.orden.save(update_fields=["estatus"])
            log_event(
                request.user,
                "APPROVE",
                "compras.OrdenCompra",
                recepcion.orden.id,
                {"from": orden_prev, "to": OrdenCompra.STATUS_CERRADA, "folio": recepcion.orden.folio, "source": recepcion.folio},
            )
    return redirect("compras:recepciones")


@login_required
@require_POST
def crear_orden_desde_solicitud(request: HttpRequest, pk: int) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para crear órdenes.")

    solicitud = get_object_or_404(SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido"), pk=pk)
    if solicitud.estatus != SolicitudCompra.STATUS_APROBADA:
        messages.error(request, f"La solicitud {solicitud.folio} no está aprobada.")
        return redirect("compras:solicitudes")

    has_open_order = OrdenCompra.objects.filter(solicitud=solicitud).exclude(estatus=OrdenCompra.STATUS_CERRADA).exists()
    if has_open_order:
        messages.info(request, f"La solicitud {solicitud.folio} ya tiene una orden activa.")
        return redirect("compras:ordenes")

    proveedor = solicitud.proveedor_sugerido or solicitud.insumo.proveedor_principal
    if not proveedor:
        messages.error(request, f"La solicitud {solicitud.folio} no tiene proveedor sugerido. Asigna uno y reintenta.")
        return redirect("compras:solicitudes")

    latest_cost = (
        CostoInsumo.objects.filter(insumo=solicitud.insumo)
        .order_by("-fecha", "-id")
        .first()
    )
    monto_estimado = (solicitud.cantidad or Decimal("0")) * (latest_cost.costo_unitario if latest_cost else Decimal("0"))

    orden = OrdenCompra.objects.create(
        solicitud=solicitud,
        proveedor=proveedor,
        referencia=f"SOLICITUD:{solicitud.folio}",
        fecha_emision=timezone.localdate(),
        fecha_entrega_estimada=solicitud.fecha_requerida,
        monto_estimado=monto_estimado,
        estatus=OrdenCompra.STATUS_BORRADOR,
    )
    log_event(
        request.user,
        "CREATE",
        "compras.OrdenCompra",
        orden.id,
        {"folio": orden.folio, "estatus": orden.estatus, "source": f"solicitud:{solicitud.folio}"},
    )
    messages.success(request, f"Orden {orden.folio} creada desde solicitud {solicitud.folio}.")
    return redirect("compras:ordenes")
