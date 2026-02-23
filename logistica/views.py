from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from core.access import can_manage_logistica, can_view_logistica
from core.audit import log_event
from crm.models import PedidoCliente

from .models import EntregaRuta, RutaEntrega


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _parse_datetime_local(raw: str | None):
    value = (raw or "").strip()
    if not value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except ValueError:
        return None
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Rutas", "url_name": "logistica:rutas", "active": active == "rutas"},
    ]


@login_required
def rutas(request):
    if not can_view_logistica(request.user):
        raise PermissionDenied("No tienes permisos para ver Logística")

    if request.method == "POST":
        if not can_manage_logistica(request.user):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        nombre = (request.POST.get("nombre") or "").strip()
        if not nombre:
            messages.error(request, "El nombre de ruta es obligatorio.")
        else:
            ruta = RutaEntrega.objects.create(
                nombre=nombre,
                fecha_ruta=request.POST.get("fecha_ruta") or timezone.localdate(),
                chofer=(request.POST.get("chofer") or "").strip(),
                unidad=(request.POST.get("unidad") or "").strip(),
                estatus=(request.POST.get("estatus") or RutaEntrega.ESTATUS_PLANEADA).strip(),
                km_estimado=_parse_decimal(request.POST.get("km_estimado")),
                notas=(request.POST.get("notas") or "").strip(),
                created_by=request.user,
            )
            log_event(
                request.user,
                "CREATE",
                "logistica.RutaEntrega",
                str(ruta.id),
                {
                    "folio": ruta.folio,
                    "nombre": ruta.nombre,
                    "fecha_ruta": str(ruta.fecha_ruta),
                    "estatus": ruta.estatus,
                },
            )
            messages.success(request, f"Ruta {ruta.folio} creada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

    q = (request.GET.get("q") or "").strip()
    estatus = (request.GET.get("estatus") or "").strip().upper()

    rutas_qs = RutaEntrega.objects.all()
    if q:
        rutas_qs = rutas_qs.filter(
            Q(folio__icontains=q)
            | Q(nombre__icontains=q)
            | Q(chofer__icontains=q)
            | Q(unidad__icontains=q)
        )
    if estatus:
        rutas_qs = rutas_qs.filter(estatus=estatus)

    context = {
        "module_tabs": _module_tabs("rutas"),
        "can_manage_logistica": can_manage_logistica(request.user),
        "rutas": rutas_qs.order_by("-fecha_ruta", "-id")[:200],
        "q": q,
        "estatus": estatus,
        "estatus_choices": RutaEntrega.ESTATUS_CHOICES,
        "totales": {
            "rutas": RutaEntrega.objects.count(),
            "hoy": RutaEntrega.objects.filter(fecha_ruta=timezone.localdate()).count(),
            "en_ruta": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count(),
            "pendientes": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE).count(),
            "incidencias": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA).count(),
        },
    }
    return render(request, "logistica/rutas.html", context)


@login_required
def ruta_detail(request, pk: int):
    if not can_view_logistica(request.user):
        raise PermissionDenied("No tienes permisos para ver Logística")

    ruta = get_object_or_404(RutaEntrega, pk=pk)

    if request.method == "POST":
        if not can_manage_logistica(request.user):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        action = (request.POST.get("action") or "").strip().lower()

        if action == "add_entrega":
            pedido = None
            pedido_id = (request.POST.get("pedido_id") or "").strip()
            if pedido_id.isdigit():
                pedido = PedidoCliente.objects.filter(pk=int(pedido_id)).first()

            entrega = EntregaRuta.objects.create(
                ruta=ruta,
                secuencia=int(request.POST.get("secuencia") or 1),
                pedido=pedido,
                cliente_nombre=(request.POST.get("cliente_nombre") or "").strip(),
                direccion=(request.POST.get("direccion") or "").strip(),
                contacto=(request.POST.get("contacto") or "").strip(),
                telefono=(request.POST.get("telefono") or "").strip(),
                ventana_inicio=_parse_datetime_local(request.POST.get("ventana_inicio")),
                ventana_fin=_parse_datetime_local(request.POST.get("ventana_fin")),
                estatus=(request.POST.get("estatus") or EntregaRuta.ESTATUS_PENDIENTE).strip(),
                monto_estimado=_parse_decimal(request.POST.get("monto_estimado")),
                comentario=(request.POST.get("comentario") or "").strip(),
            )
            ruta.recompute_totals()
            ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])
            log_event(
                request.user,
                "CREATE",
                "logistica.EntregaRuta",
                str(entrega.id),
                {
                    "ruta": ruta.folio,
                    "secuencia": entrega.secuencia,
                    "cliente_nombre": entrega.cliente_nombre,
                    "estatus": entrega.estatus,
                },
            )
            messages.success(request, "Entrega agregada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "entrega_status":
            entrega_id = (request.POST.get("entrega_id") or "").strip()
            estatus_nuevo = (request.POST.get("estatus") or "").strip().upper()
            comentario = (request.POST.get("comentario") or "").strip()
            if entrega_id.isdigit() and estatus_nuevo in {c[0] for c in EntregaRuta.ESTATUS_CHOICES}:
                entrega = EntregaRuta.objects.filter(pk=int(entrega_id), ruta=ruta).first()
                if entrega:
                    entrega.estatus = estatus_nuevo
                    if comentario:
                        entrega.comentario = comentario
                    entrega.save(update_fields=["estatus", "comentario", "entregado_at", "updated_at"])
                    ruta.recompute_totals()
                    ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])
                    log_event(
                        request.user,
                        "UPDATE",
                        "logistica.EntregaRuta",
                        str(entrega.id),
                        {
                            "ruta": ruta.folio,
                            "estatus": entrega.estatus,
                        },
                    )
                    messages.success(request, "Estatus de entrega actualizado.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "delete_entrega":
            entrega_id = (request.POST.get("entrega_id") or "").strip()
            if entrega_id.isdigit():
                entrega = EntregaRuta.objects.filter(pk=int(entrega_id), ruta=ruta).first()
                if entrega:
                    entrega.delete()
                    ruta.recompute_totals()
                    ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])
                    messages.success(request, "Entrega eliminada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "ruta_status":
            estatus_nuevo = (request.POST.get("estatus") or "").strip().upper()
            if estatus_nuevo in {c[0] for c in RutaEntrega.ESTATUS_CHOICES}:
                from_status = ruta.estatus
                if from_status != estatus_nuevo:
                    ruta.estatus = estatus_nuevo
                    ruta.save(update_fields=["estatus", "updated_at"])
                    log_event(
                        request.user,
                        "UPDATE",
                        "logistica.RutaEntrega",
                        str(ruta.id),
                        {"from": from_status, "to": estatus_nuevo, "folio": ruta.folio},
                    )
                    messages.success(request, f"Ruta {ruta.folio} en {estatus_nuevo}.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

    pedidos_disponibles = (
        PedidoCliente.objects.select_related("cliente")
        .exclude(estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO])
        .order_by("fecha_compromiso", "-created_at")[:300]
    )

    context = {
        "module_tabs": _module_tabs("rutas"),
        "can_manage_logistica": can_manage_logistica(request.user),
        "ruta": ruta,
        "entregas": ruta.entregas.select_related("pedido", "pedido__cliente").all(),
        "pedidos": pedidos_disponibles,
        "estatus_ruta_choices": RutaEntrega.ESTATUS_CHOICES,
        "estatus_entrega_choices": EntregaRuta.ESTATUS_CHOICES,
    }
    return render(request, "logistica/ruta_detail.html", context)
