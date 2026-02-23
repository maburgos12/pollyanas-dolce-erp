from __future__ import annotations

from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_view_audit

from .models import PublicApiAccessLog, PublicApiClient


@login_required
def panel(request):
    if not can_view_audit(request.user):
        raise PermissionDenied("No tienes permisos para gestionar integraciones.")

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "create":
            nombre = (request.POST.get("nombre") or "").strip()
            descripcion = (request.POST.get("descripcion") or "").strip()
            if not nombre:
                messages.error(request, "El nombre del cliente es obligatorio.")
                return redirect("integraciones:panel")
            client, raw_key = PublicApiClient.create_with_generated_key(
                nombre=nombre,
                descripcion=descripcion,
                created_by=request.user,
            )
            request.session["integraciones_last_api_key"] = raw_key
            messages.success(request, f"Cliente creado: {client.nombre}")
            return redirect("integraciones:panel")

        if action == "rotate":
            client_id = (request.POST.get("client_id") or "").strip()
            client = PublicApiClient.objects.filter(id=client_id).first() if client_id.isdigit() else None
            if not client:
                messages.error(request, "Cliente no encontrado para rotación.")
                return redirect("integraciones:panel")
            raw_key = client.rotate_key()
            request.session["integraciones_last_api_key"] = raw_key
            messages.success(request, f"API key rotada para: {client.nombre}")
            return redirect("integraciones:panel")

        if action == "toggle":
            client_id = (request.POST.get("client_id") or "").strip()
            client = PublicApiClient.objects.filter(id=client_id).first() if client_id.isdigit() else None
            if not client:
                messages.error(request, "Cliente no encontrado.")
                return redirect("integraciones:panel")
            client.activo = not client.activo
            client.save(update_fields=["activo", "updated_at"])
            label = "activado" if client.activo else "desactivado"
            messages.success(request, f"Cliente {label}: {client.nombre}")
            return redirect("integraciones:panel")

    last_generated_api_key = request.session.pop("integraciones_last_api_key", "")
    clients = list(PublicApiClient.objects.order_by("nombre", "id"))

    logs = list(
        PublicApiAccessLog.objects.select_related("client")
        .order_by("-created_at", "-id")[:80]
    )

    since_24h = timezone.now() - timedelta(hours=24)
    top_clients_24h = list(
        PublicApiAccessLog.objects.filter(created_at__gte=since_24h)
        .values("client__nombre")
        .annotate(total=Count("id"))
        .order_by("-total", "client__nombre")[:10]
    )

    context = {
        "clients": clients,
        "logs": logs,
        "last_generated_api_key": last_generated_api_key,
        "top_clients_24h": top_clients_24h,
    }
    return render(request, "integraciones/panel.html", context)
