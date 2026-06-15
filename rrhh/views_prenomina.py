from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_date

from core.access import can_manage_rrhh, can_view_rrhh
from rrhh.exporters.contpaqi_prenomina import export_movimientos_contpaqi_xlsx, export_revision_xlsx
from rrhh.models import PrenominaCorte, PrenominaEmpleadoResumen, PrenominaMovimiento
from rrhh.services_prenomina import crear_corte_prenomina, recalcular_corte_prenomina
from rrhh.views import _module_tabs


def _parse_fecha(value, default=None):
    return parse_date((value or "").strip()) or default


def _blocked_export_reasons(corte: PrenominaCorte) -> list[str]:
    resumen = corte.resumen or {}
    checks = [
        ("bloqueados", "resúmenes bloqueados"),
        ("ajustes_pendientes", "ajustes pendientes"),
        ("movimientos_pendientes_configuracion", "movimientos sin clave CONTPAQi"),
        ("movimientos_bloqueados", "movimientos bloqueados"),
        ("movimientos_exportados", "movimientos ya exportados"),
    ]
    reasons = [label for key, label in checks if int(resumen.get(key) or 0) > 0]
    if corte.movimientos.exclude(estado=PrenominaMovimiento.ESTADO_LISTO).exists():
        reasons.append("movimientos fuera de estado listo")
    return reasons


@login_required
def prenomina(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenómina.")

    hoy = timezone.localdate()
    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para generar prenómina.")
        inicio = _parse_fecha(request.POST.get("fecha_inicio"))
        fin = _parse_fecha(request.POST.get("fecha_fin"))
        fecha_corte = _parse_fecha(request.POST.get("fecha_corte"), hoy)
        if not inicio or not fin or fin < inicio:
            messages.error(request, "Captura un rango de prenómina válido.")
            return redirect("rrhh:prenomina")
        corte = crear_corte_prenomina(
            fecha_inicio=inicio,
            fecha_fin=fin,
            fecha_corte=fecha_corte,
            creado_por=request.user,
            tipo_periodo=(request.POST.get("tipo_periodo") or PrenominaCorte.TIPO_QUINCENAL).strip(),
            sucursal=(request.POST.get("sucursal") or "").strip(),
            area=(request.POST.get("area") or "").strip(),
            notas=(request.POST.get("notas") or "").strip(),
        )
        messages.success(request, f"Corte {corte.folio} generado.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)

    return render(
        request,
        "rrhh/prenomina.html",
        {
            "module_tabs": _module_tabs("prenomina", request.user),
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "hoy": hoy.isoformat(),
            "cortes": PrenominaCorte.objects.select_related("creado_por").order_by("-fecha_fin", "-id")[:80],
            "tipo_choices": PrenominaCorte.TIPO_CHOICES,
        },
    )


@login_required
def prenomina_detail(request, pk):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenómina.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)

    if request.method == "POST" and request.POST.get("action") == "recalcular":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para recalcular prenómina.")
        corte = recalcular_corte_prenomina(corte)
        messages.success(request, f"Corte {corte.folio} recalculado.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)

    return render(
        request,
        "rrhh/prenomina_detail.html",
        {
            "module_tabs": _module_tabs("prenomina", request.user),
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "corte": corte,
            "resumenes": corte.resumenes.select_related("empleado").order_by("empleado__nombre", "empleado__codigo"),
            "movimientos": corte.movimientos.select_related("empleado").order_by("empleado__nombre", "fecha", "id")[:400],
            "export_blockers": _blocked_export_reasons(corte),
        },
    )


@login_required
def prenomina_persona(request, pk, empleado_id):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenómina.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    resumen = get_object_or_404(
        PrenominaEmpleadoResumen.objects.select_related("empleado"),
        corte=corte,
        empleado_id=empleado_id,
    )
    return render(
        request,
        "rrhh/prenomina_persona.html",
        {
            "module_tabs": _module_tabs("prenomina", request.user),
            "corte": corte,
            "resumen": resumen,
            "movimientos": corte.movimientos.filter(empleado_id=empleado_id).order_by("fecha", "tipo_movimiento_erp", "id"),
        },
    )


@login_required
def prenomina_export_revision(request, pk):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para exportar prenómina.")
    return export_revision_xlsx(get_object_or_404(PrenominaCorte, pk=pk))


@login_required
def prenomina_export_contpaqi(request, pk):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para exportar movimientos CONTPAQi.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    reasons = _blocked_export_reasons(corte)
    if reasons:
        messages.error(request, "No se puede exportar CONTPAQi: " + ", ".join(reasons) + ".")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)
    return export_movimientos_contpaqi_xlsx(corte)
