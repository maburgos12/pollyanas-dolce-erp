from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Count, Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from core.access import can_manage_crm, can_view_crm
from core.audit import log_event

from .models import Cliente, PedidoCliente, SeguimientoPedido


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


@login_required
def clientes(request: HttpRequest) -> HttpResponse:
    if not can_view_crm(request.user):
        raise PermissionDenied("No tienes permisos para ver CRM")

    if request.method == "POST":
        if not can_manage_crm(request.user):
            raise PermissionDenied("No tienes permisos para gestionar CRM")

        nombre = (request.POST.get("nombre") or "").strip()
        if not nombre:
            messages.error(request, "El nombre del cliente es obligatorio.")
        else:
            cliente = Cliente.objects.create(
                nombre=nombre,
                telefono=(request.POST.get("telefono") or "").strip(),
                email=(request.POST.get("email") or "").strip(),
                tipo_cliente=(request.POST.get("tipo_cliente") or "").strip(),
                sucursal_referencia=(request.POST.get("sucursal_referencia") or "").strip(),
                notas=(request.POST.get("notas") or "").strip(),
            )
            log_event(
                request.user,
                "CREATE",
                "crm.Cliente",
                str(cliente.id),
                {
                    "codigo": cliente.codigo,
                    "nombre": cliente.nombre,
                },
            )
            messages.success(request, f"Cliente {cliente.nombre} creado.")
            return redirect("crm:clientes")

    q = (request.GET.get("q") or "").strip()
    estado = (request.GET.get("estado") or "activos").strip().lower()

    qs = Cliente.objects.all().annotate(total_pedidos=Count("pedidos"))
    if q:
        qs = qs.filter(
            Q(nombre__icontains=q)
            | Q(codigo__icontains=q)
            | Q(telefono__icontains=q)
            | Q(email__icontains=q)
        )
    if estado == "activos":
        qs = qs.filter(activo=True)
    elif estado == "inactivos":
        qs = qs.filter(activo=False)

    ctx = {
        "clientes": qs.order_by("nombre")[:500],
        "q": q,
        "estado": estado,
        "total_clientes": Cliente.objects.count(),
        "total_clientes_activos": Cliente.objects.filter(activo=True).count(),
        "total_pedidos_abiertos": PedidoCliente.objects.exclude(
            estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
        ).count(),
        "can_manage_crm": can_manage_crm(request.user),
    }
    return render(request, "crm/clientes.html", ctx)


@login_required
def pedidos(request: HttpRequest) -> HttpResponse:
    if not can_view_crm(request.user):
        raise PermissionDenied("No tienes permisos para ver CRM")

    if request.method == "POST":
        if not can_manage_crm(request.user):
            raise PermissionDenied("No tienes permisos para gestionar CRM")

        cliente_id = request.POST.get("cliente_id")
        descripcion = (request.POST.get("descripcion") or "").strip()
        if not cliente_id or not descripcion:
            messages.error(request, "Cliente y descripción son obligatorios.")
        else:
            cliente = get_object_or_404(Cliente, pk=cliente_id)
            pedido = PedidoCliente.objects.create(
                cliente=cliente,
                descripcion=descripcion,
                fecha_compromiso=request.POST.get("fecha_compromiso") or None,
                sucursal=(request.POST.get("sucursal") or "").strip(),
                canal=(request.POST.get("canal") or PedidoCliente.CANAL_MOSTRADOR).strip(),
                prioridad=(request.POST.get("prioridad") or PedidoCliente.PRIORIDAD_MEDIA).strip(),
                estatus=(request.POST.get("estatus") or PedidoCliente.ESTATUS_NUEVO).strip(),
                monto_estimado=_parse_decimal(request.POST.get("monto_estimado")),
                created_by=request.user,
            )
            SeguimientoPedido.objects.create(
                pedido=pedido,
                estatus_anterior="",
                estatus_nuevo=pedido.estatus,
                comentario="Alta de pedido",
                created_by=request.user,
            )
            log_event(
                request.user,
                "CREATE",
                "crm.PedidoCliente",
                str(pedido.id),
                {
                    "folio": pedido.folio,
                    "cliente": pedido.cliente.nombre,
                    "estatus": pedido.estatus,
                    "monto_estimado": str(pedido.monto_estimado),
                },
            )
            messages.success(request, f"Pedido {pedido.folio} creado.")
            return redirect("crm:pedidos")

    q = (request.GET.get("q") or "").strip()
    estatus = (request.GET.get("estatus") or "").strip()
    prioridad = (request.GET.get("prioridad") or "").strip()

    pedidos_qs = PedidoCliente.objects.select_related("cliente")
    if q:
        pedidos_qs = pedidos_qs.filter(
            Q(folio__icontains=q)
            | Q(cliente__nombre__icontains=q)
            | Q(descripcion__icontains=q)
            | Q(sucursal__icontains=q)
        )
    if estatus:
        pedidos_qs = pedidos_qs.filter(estatus=estatus)
    if prioridad:
        pedidos_qs = pedidos_qs.filter(prioridad=prioridad)

    ctx = {
        "clientes": Cliente.objects.filter(activo=True).order_by("nombre"),
        "pedidos": pedidos_qs.order_by("-created_at")[:500],
        "estatus_choices": PedidoCliente.ESTATUS_CHOICES,
        "prioridad_choices": PedidoCliente.PRIORIDAD_CHOICES,
        "canal_choices": PedidoCliente.CANAL_CHOICES,
        "q": q,
        "estatus": estatus,
        "prioridad": prioridad,
        "can_manage_crm": can_manage_crm(request.user),
        "pedidos_abiertos": PedidoCliente.objects.exclude(
            estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
        ).count(),
        "pedidos_hoy": PedidoCliente.objects.filter(created_at__date=timezone.localdate()).count(),
    }

    # Conteo por estatus para tarjeta rápida
    conteos = {
        key: PedidoCliente.objects.filter(estatus=key).count()
        for key, _ in PedidoCliente.ESTATUS_CHOICES
    }
    ctx["conteos_estatus"] = conteos
    return render(request, "crm/pedidos.html", ctx)


@login_required
def pedido_detail(request: HttpRequest, pedido_id: int) -> HttpResponse:
    if not can_view_crm(request.user):
        raise PermissionDenied("No tienes permisos para ver CRM")

    pedido = get_object_or_404(PedidoCliente.objects.select_related("cliente"), pk=pedido_id)

    if request.method == "POST":
        if not can_manage_crm(request.user):
            raise PermissionDenied("No tienes permisos para gestionar CRM")

        comentario = (request.POST.get("comentario") or "").strip()
        estatus_nuevo = (request.POST.get("estatus_nuevo") or "").strip()
        if not comentario and not estatus_nuevo:
            messages.error(request, "Captura comentario o cambia estatus para guardar seguimiento.")
        else:
            with transaction.atomic():
                estatus_anterior = pedido.estatus
                estatus_registro = estatus_nuevo if estatus_nuevo else ""
                if estatus_nuevo and estatus_nuevo != pedido.estatus:
                    pedido.estatus = estatus_nuevo
                    pedido.save(update_fields=["estatus", "updated_at"])
                SeguimientoPedido.objects.create(
                    pedido=pedido,
                    estatus_anterior=estatus_anterior if estatus_registro else "",
                    estatus_nuevo=estatus_registro,
                    comentario=comentario,
                    created_by=request.user,
                )
            log_event(
                request.user,
                "UPDATE",
                "crm.PedidoCliente",
                str(pedido.id),
                {
                    "folio": pedido.folio,
                    "estatus_anterior": estatus_anterior,
                    "estatus_nuevo": pedido.estatus,
                    "comentario": comentario,
                },
            )
            messages.success(request, "Seguimiento guardado.")
            return redirect("crm:pedido_detail", pedido_id=pedido.id)

    ctx = {
        "pedido": pedido,
        "seguimientos": pedido.seguimientos.select_related("created_by").all(),
        "estatus_choices": PedidoCliente.ESTATUS_CHOICES,
        "can_manage_crm": can_manage_crm(request.user),
    }
    return render(request, "crm/pedido_detail.html", ctx)
