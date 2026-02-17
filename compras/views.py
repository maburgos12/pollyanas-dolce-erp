from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from maestros.models import Insumo, Proveedor

from .models import OrdenCompra, RecepcionCompra, SolicitudCompra


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value or default)
    except (InvalidOperation, TypeError):
        return Decimal(default)


@login_required
def solicitudes(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            SolicitudCompra.objects.create(
                area=request.POST.get("area", "General").strip() or "General",
                solicitante=request.POST.get("solicitante", request.user.username).strip() or request.user.username,
                insumo_id=insumo_id,
                cantidad=_to_decimal(request.POST.get("cantidad"), "1"),
                fecha_requerida=request.POST.get("fecha_requerida") or None,
                estatus=request.POST.get("estatus") or SolicitudCompra.STATUS_BORRADOR,
            )
        return redirect("compras:solicitudes")

    context = {
        "solicitudes": SolicitudCompra.objects.select_related("insumo")[:50],
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "status_choices": SolicitudCompra.STATUS_CHOICES,
    }
    return render(request, "compras/solicitudes.html", context)


@login_required
def ordenes(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        proveedor_id = request.POST.get("proveedor_id")
        if proveedor_id:
            solicitud_raw = request.POST.get("solicitud_id")
            OrdenCompra.objects.create(
                proveedor_id=proveedor_id,
                solicitud_id=solicitud_raw or None,
                fecha_emision=request.POST.get("fecha_emision") or None,
                fecha_entrega_estimada=request.POST.get("fecha_entrega_estimada") or None,
                monto_estimado=_to_decimal(request.POST.get("monto_estimado"), "0"),
                estatus=request.POST.get("estatus") or OrdenCompra.STATUS_BORRADOR,
            )
        return redirect("compras:ordenes")

    context = {
        "ordenes": OrdenCompra.objects.select_related("proveedor", "solicitud")[:50],
        "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre")[:200],
        "solicitudes": SolicitudCompra.objects.order_by("-creado_en")[:200],
        "status_choices": OrdenCompra.STATUS_CHOICES,
    }
    return render(request, "compras/ordenes.html", context)


@login_required
def recepciones(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        orden_id = request.POST.get("orden_id")
        if orden_id:
            RecepcionCompra.objects.create(
                orden_id=orden_id,
                fecha_recepcion=request.POST.get("fecha_recepcion") or None,
                conformidad_pct=_to_decimal(request.POST.get("conformidad_pct"), "100"),
                estatus=request.POST.get("estatus") or RecepcionCompra.STATUS_PENDIENTE,
                observaciones=request.POST.get("observaciones", "").strip(),
            )
        return redirect("compras:recepciones")

    context = {
        "recepciones": RecepcionCompra.objects.select_related("orden", "orden__proveedor")[:50],
        "ordenes": OrdenCompra.objects.select_related("proveedor").order_by("-creado_en")[:200],
        "status_choices": RecepcionCompra.STATUS_CHOICES,
    }
    return render(request, "compras/recepciones.html", context)
