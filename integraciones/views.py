from __future__ import annotations

import csv
from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_view_audit
from core.audit import log_event

from .models import PublicApiAccessLog, PublicApiClient


def _export_logs_csv(logs_qs) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_api_logs.csv"'
    writer = csv.writer(response)
    writer.writerow(["fecha", "cliente", "metodo", "endpoint", "status_code"])
    for row in logs_qs:
        writer.writerow(
            [
                row.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                row.client.nombre if row.client_id else "",
                row.method,
                row.endpoint,
                row.status_code,
            ]
        )
    return response


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
            log_event(
                request.user,
                "CREATE",
                "integraciones.PublicApiClient",
                str(client.id),
                payload={
                    "nombre": client.nombre,
                    "clave_prefijo": client.clave_prefijo,
                    "activo": client.activo,
                },
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
            log_event(
                request.user,
                "ROTATE_KEY",
                "integraciones.PublicApiClient",
                str(client.id),
                payload={"nombre": client.nombre, "clave_prefijo": client.clave_prefijo},
            )
            request.session["integraciones_last_api_key"] = raw_key
            messages.success(request, f"API key rotada para: {client.nombre}")
            return redirect("integraciones:panel")

        if action == "toggle":
            client_id = (request.POST.get("client_id") or "").strip()
            client = PublicApiClient.objects.filter(id=client_id).first() if client_id.isdigit() else None
            if not client:
                messages.error(request, "Cliente no encontrado.")
                return redirect("integraciones:panel")
            old_status = client.activo
            client.activo = not client.activo
            client.save(update_fields=["activo", "updated_at"])
            log_event(
                request.user,
                "TOGGLE_ACTIVE",
                "integraciones.PublicApiClient",
                str(client.id),
                payload={"nombre": client.nombre, "old_activo": old_status, "new_activo": client.activo},
            )
            label = "activado" if client.activo else "desactivado"
            messages.success(request, f"Cliente {label}: {client.nombre}")
            return redirect("integraciones:panel")

    last_generated_api_key = request.session.pop("integraciones_last_api_key", "")
    clients = list(PublicApiClient.objects.order_by("nombre", "id"))

    filter_client_id = (request.GET.get("client") or "").strip()
    filter_status = (request.GET.get("status") or "all").strip().lower()
    filter_q = (request.GET.get("q") or "").strip()

    logs_qs = PublicApiAccessLog.objects.select_related("client")
    if filter_client_id.isdigit():
        logs_qs = logs_qs.filter(client_id=int(filter_client_id))
    if filter_status == "ok":
        logs_qs = logs_qs.filter(status_code__lt=400)
    elif filter_status == "error":
        logs_qs = logs_qs.filter(status_code__gte=400)
    if filter_q:
        logs_qs = logs_qs.filter(endpoint__icontains=filter_q)

    logs_qs = logs_qs.order_by("-created_at", "-id")
    if (request.GET.get("export") or "").strip().lower() == "csv":
        return _export_logs_csv(logs_qs[:5000])
    logs = list(logs_qs[:120])

    since_24h = timezone.now() - timedelta(hours=24)
    top_clients_24h = list(
        PublicApiAccessLog.objects.filter(created_at__gte=since_24h)
        .values("client__nombre")
        .annotate(
            total=Count("id"),
            errores=Count("id", filter=Q(status_code__gte=400)),
        )
        .order_by("-total", "client__nombre")[:10]
    )

    requests_24h = PublicApiAccessLog.objects.filter(created_at__gte=since_24h).count()
    errors_24h = PublicApiAccessLog.objects.filter(created_at__gte=since_24h, status_code__gte=400).count()

    context = {
        "clients": clients,
        "logs": logs,
        "last_generated_api_key": last_generated_api_key,
        "top_clients_24h": top_clients_24h,
        "requests_24h": requests_24h,
        "errors_24h": errors_24h,
        "filter_client_id": filter_client_id,
        "filter_status": filter_status,
        "filter_q": filter_q,
    }
    return render(request, "integraciones/panel.html", context)
