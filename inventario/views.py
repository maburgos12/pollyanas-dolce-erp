from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_manage_inventario, can_view_inventario
from maestros.models import Insumo

from .models import AjusteInventario, ExistenciaInsumo, MovimientoInventario


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value or default)
    except (InvalidOperation, TypeError):
        return Decimal(default)


def _apply_movimiento(movimiento: MovimientoInventario) -> None:
    existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=movimiento.insumo)
    if movimiento.tipo == MovimientoInventario.TIPO_ENTRADA:
        existencia.stock_actual += movimiento.cantidad
    else:
        existencia.stock_actual -= movimiento.cantidad
    existencia.actualizado_en = timezone.now()
    existencia.save()


@login_required
def existencias(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para editar existencias.")
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo_id=insumo_id)
            existencia.stock_actual = _to_decimal(request.POST.get("stock_actual"), "0")
            existencia.punto_reorden = _to_decimal(request.POST.get("punto_reorden"), "0")
            existencia.actualizado_en = timezone.now()
            existencia.save()
        return redirect("inventario:existencias")

    context = {
        "existencias": ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base")[:200],
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "can_manage_inventario": can_manage_inventario(request.user),
    }
    return render(request, "inventario/existencias.html", context)


@login_required
def movimientos(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para registrar movimientos.")
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            movimiento = MovimientoInventario.objects.create(
                fecha=request.POST.get("fecha") or timezone.now(),
                tipo=request.POST.get("tipo") or MovimientoInventario.TIPO_ENTRADA,
                insumo_id=insumo_id,
                cantidad=_to_decimal(request.POST.get("cantidad"), "0"),
                referencia=request.POST.get("referencia", "").strip(),
            )
            _apply_movimiento(movimiento)
        return redirect("inventario:movimientos")

    context = {
        "movimientos": MovimientoInventario.objects.select_related("insumo")[:100],
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "tipo_choices": MovimientoInventario.TIPO_CHOICES,
        "can_manage_inventario": can_manage_inventario(request.user),
    }
    return render(request, "inventario/movimientos.html", context)


@login_required
def ajustes(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para registrar ajustes.")
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            ajuste = AjusteInventario.objects.create(
                insumo_id=insumo_id,
                cantidad_sistema=_to_decimal(request.POST.get("cantidad_sistema"), "0"),
                cantidad_fisica=_to_decimal(request.POST.get("cantidad_fisica"), "0"),
                motivo=request.POST.get("motivo", "").strip() or "Sin motivo",
                estatus=request.POST.get("estatus") or AjusteInventario.STATUS_PENDIENTE,
            )

            if ajuste.estatus == AjusteInventario.STATUS_APLICADO:
                existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo_id=ajuste.insumo_id)
                existencia.stock_actual = ajuste.cantidad_fisica
                existencia.actualizado_en = timezone.now()
                existencia.save()

                MovimientoInventario.objects.create(
                    tipo=MovimientoInventario.TIPO_AJUSTE,
                    insumo_id=ajuste.insumo_id,
                    cantidad=abs(ajuste.cantidad_fisica - ajuste.cantidad_sistema),
                    referencia=ajuste.folio,
                )
        return redirect("inventario:ajustes")

    context = {
        "ajustes": AjusteInventario.objects.select_related("insumo")[:100],
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "status_choices": AjusteInventario.STATUS_CHOICES,
        "can_manage_inventario": can_manage_inventario(request.user),
    }
    return render(request, "inventario/ajustes.html", context)
