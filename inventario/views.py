from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_manage_inventario, can_view_inventario
from core.audit import log_event
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
            prev_stock = existencia.stock_actual
            prev_reorden = existencia.punto_reorden
            prev_minimo = existencia.stock_minimo
            prev_maximo = existencia.stock_maximo
            prev_inv_prom = existencia.inventario_promedio
            prev_dias = existencia.dias_llegada_pedido
            prev_consumo = existencia.consumo_diario_promedio
            new_stock = _to_decimal(request.POST.get("stock_actual"), "0")
            new_reorden = _to_decimal(request.POST.get("punto_reorden"), "0")
            new_minimo = _to_decimal(request.POST.get("stock_minimo"), "0")
            new_maximo = _to_decimal(request.POST.get("stock_maximo"), "0")
            new_inv_prom = _to_decimal(request.POST.get("inventario_promedio"), "0")
            new_dias = int(_to_decimal(request.POST.get("dias_llegada_pedido"), "0"))
            new_consumo = _to_decimal(request.POST.get("consumo_diario_promedio"), "0")
            if (
                new_stock < 0
                or new_reorden < 0
                or new_minimo < 0
                or new_maximo < 0
                or new_inv_prom < 0
                or new_dias < 0
                or new_consumo < 0
            ):
                messages.error(request, "Los indicadores de inventario no pueden ser negativos.")
                return redirect("inventario:existencias")

            existencia.stock_actual = new_stock
            existencia.punto_reorden = new_reorden
            existencia.stock_minimo = new_minimo
            existencia.stock_maximo = new_maximo
            existencia.inventario_promedio = new_inv_prom
            existencia.dias_llegada_pedido = new_dias
            existencia.consumo_diario_promedio = new_consumo
            existencia.actualizado_en = timezone.now()
            existencia.save()
            log_event(
                request.user,
                "UPDATE",
                "inventario.ExistenciaInsumo",
                existencia.id,
                {
                    "insumo_id": existencia.insumo_id,
                    "from_stock": str(prev_stock),
                    "to_stock": str(existencia.stock_actual),
                    "from_reorden": str(prev_reorden),
                    "to_reorden": str(existencia.punto_reorden),
                    "from_stock_minimo": str(prev_minimo),
                    "to_stock_minimo": str(existencia.stock_minimo),
                    "from_stock_maximo": str(prev_maximo),
                    "to_stock_maximo": str(existencia.stock_maximo),
                    "from_inventario_promedio": str(prev_inv_prom),
                    "to_inventario_promedio": str(existencia.inventario_promedio),
                    "from_dias_llegada_pedido": prev_dias,
                    "to_dias_llegada_pedido": existencia.dias_llegada_pedido,
                    "from_consumo_diario_promedio": str(prev_consumo),
                    "to_consumo_diario_promedio": str(existencia.consumo_diario_promedio),
                },
            )
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
            tipo = request.POST.get("tipo") or MovimientoInventario.TIPO_ENTRADA
            if tipo == MovimientoInventario.TIPO_AJUSTE:
                messages.error(request, "El tipo AJUSTE se genera automáticamente desde la pantalla de ajustes.")
                return redirect("inventario:movimientos")

            cantidad = _to_decimal(request.POST.get("cantidad"), "0")
            if cantidad <= 0:
                messages.error(request, "La cantidad del movimiento debe ser mayor a cero.")
                return redirect("inventario:movimientos")

            existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo_id=insumo_id)
            if tipo in {MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO} and existencia.stock_actual < cantidad:
                messages.error(
                    request,
                    f"Stock insuficiente para {tipo.lower()}: disponible={existencia.stock_actual}, solicitado={cantidad}.",
                )
                return redirect("inventario:movimientos")

            movimiento = MovimientoInventario.objects.create(
                fecha=request.POST.get("fecha") or timezone.now(),
                tipo=tipo,
                insumo_id=insumo_id,
                cantidad=cantidad,
                referencia=request.POST.get("referencia", "").strip(),
            )
            _apply_movimiento(movimiento)
            log_event(
                request.user,
                "CREATE",
                "inventario.MovimientoInventario",
                movimiento.id,
                {
                    "tipo": movimiento.tipo,
                    "insumo_id": movimiento.insumo_id,
                    "cantidad": str(movimiento.cantidad),
                    "referencia": movimiento.referencia,
                },
            )
        return redirect("inventario:movimientos")

    existencias_by_insumo = {
        row["insumo_id"]: row["stock_actual"]
        for row in ExistenciaInsumo.objects.values("insumo_id", "stock_actual")
    }
    insumo_options = [
        {
            "id": i.id,
            "nombre": i.nombre,
            "stock": existencias_by_insumo.get(i.id, Decimal("0")),
        }
        for i in Insumo.objects.filter(activo=True).order_by("nombre")[:200]
    ]

    context = {
        "movimientos": MovimientoInventario.objects.select_related("insumo")[:100],
        "insumo_options": insumo_options,
        "tipo_choices": [
            (value, label)
            for value, label in MovimientoInventario.TIPO_CHOICES
            if value != MovimientoInventario.TIPO_AJUSTE
        ],
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
            cantidad_sistema = _to_decimal(request.POST.get("cantidad_sistema"), "0")
            cantidad_fisica = _to_decimal(request.POST.get("cantidad_fisica"), "0")
            if cantidad_sistema < 0 or cantidad_fisica < 0:
                messages.error(request, "Las cantidades del ajuste no pueden ser negativas.")
                return redirect("inventario:ajustes")

            ajuste = AjusteInventario.objects.create(
                insumo_id=insumo_id,
                cantidad_sistema=cantidad_sistema,
                cantidad_fisica=cantidad_fisica,
                motivo=request.POST.get("motivo", "").strip() or "Sin motivo",
                estatus=request.POST.get("estatus") or AjusteInventario.STATUS_PENDIENTE,
            )
            log_event(
                request.user,
                "CREATE",
                "inventario.AjusteInventario",
                ajuste.id,
                {
                    "folio": ajuste.folio,
                    "insumo_id": ajuste.insumo_id,
                    "cantidad_sistema": str(ajuste.cantidad_sistema),
                    "cantidad_fisica": str(ajuste.cantidad_fisica),
                    "estatus": ajuste.estatus,
                },
            )

            if ajuste.estatus == AjusteInventario.STATUS_APLICADO:
                existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo_id=ajuste.insumo_id)
                prev_stock = existencia.stock_actual
                existencia.stock_actual = ajuste.cantidad_fisica
                existencia.actualizado_en = timezone.now()
                existencia.save()
                log_event(
                    request.user,
                    "APPLY",
                    "inventario.ExistenciaInsumo",
                    existencia.id,
                    {
                        "source": ajuste.folio,
                        "insumo_id": ajuste.insumo_id,
                        "from_stock": str(prev_stock),
                        "to_stock": str(existencia.stock_actual),
                    },
                )

                delta = ajuste.cantidad_fisica - ajuste.cantidad_sistema
                if delta != 0:
                    movimiento_ajuste = MovimientoInventario.objects.create(
                        tipo=(
                            MovimientoInventario.TIPO_ENTRADA
                            if delta > 0
                            else MovimientoInventario.TIPO_SALIDA
                        ),
                        insumo_id=ajuste.insumo_id,
                        cantidad=abs(delta),
                        referencia=ajuste.folio,
                    )
                    log_event(
                        request.user,
                        "CREATE",
                        "inventario.MovimientoInventario",
                        movimiento_ajuste.id,
                        {
                            "tipo": movimiento_ajuste.tipo,
                            "insumo_id": movimiento_ajuste.insumo_id,
                            "cantidad": str(movimiento_ajuste.cantidad),
                            "referencia": movimiento_ajuste.referencia,
                        },
                    )
        return redirect("inventario:ajustes")

    context = {
        "ajustes": AjusteInventario.objects.select_related("insumo")[:100],
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "status_choices": AjusteInventario.STATUS_CHOICES,
        "can_manage_inventario": can_manage_inventario(request.user),
    }
    return render(request, "inventario/ajustes.html", context)


@login_required
def alertas(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    nivel = (request.GET.get("nivel") or "alerta").lower()
    valid_levels = {"alerta", "critico", "bajo", "ok", "all"}
    if nivel not in valid_levels:
        nivel = "alerta"

    existencias = list(
        ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base").order_by("insumo__nombre")[:500]
    )

    rows = []
    criticos_count = 0
    bajo_reorden_count = 0
    ok_count = 0

    for e in existencias:
        stock = e.stock_actual
        reorden = e.punto_reorden
        diferencia = stock - reorden
        if stock <= 0:
            nivel_row = "critico"
            etiqueta = "Sin stock"
            criticos_count += 1
        elif stock < reorden:
            nivel_row = "bajo"
            etiqueta = "Bajo reorden"
            bajo_reorden_count += 1
        else:
            nivel_row = "ok"
            etiqueta = "Stock suficiente"
            ok_count += 1

        include = False
        if nivel == "all":
            include = True
        elif nivel == "alerta":
            include = nivel_row in {"critico", "bajo"}
        elif nivel == nivel_row:
            include = True

        if include:
            e.alerta_nivel = nivel_row
            e.alerta_etiqueta = etiqueta
            e.alerta_diferencia = diferencia
            rows.append(e)

    context = {
        "rows": rows,
        "nivel": nivel,
        "criticos_count": criticos_count,
        "bajo_reorden_count": bajo_reorden_count,
        "ok_count": ok_count,
        "total_count": len(existencias),
    }
    return render(request, "inventario/alertas.html", context)
