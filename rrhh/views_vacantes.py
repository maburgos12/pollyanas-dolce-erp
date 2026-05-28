from __future__ import annotations

from datetime import date

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.shortcuts import get_object_or_404, redirect, render

from core.models import Sucursal

from .models import Empleado, VacanteRRHH
from .services_vacantes import (
    aprobar_vacante_autorizacion,
    cancelar_vacante,
    can_autorizar_vacante,
    can_gestionar_vacantes,
    can_ver_vacante,
    cubrir_vacante,
    crear_solicitud_vacante,
    enviar_vacante_autorizacion,
    filtrar_vacantes_para_usuario,
    iniciar_reclutamiento_vacante,
    pausar_vacante,
    rechazar_vacante_autorizacion,
)
from .views import _module_tabs


def _parse_date(raw: str | None) -> date | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


@login_required
def vacantes_lista(request):
    if not _can_view_board(request.user):
        raise PermissionDenied("No tienes permisos para ver vacantes de Capital Humano")

    estado = (request.GET.get("estado") or "").strip()
    area = (request.GET.get("area") or "").strip()
    vacantes = (
        VacanteRRHH.objects.select_related(
            "sucursal",
            "solicitado_por",
            "autorizado_por",
            "autorizador_asignado",
            "empleado_cubrio",
        )
        .prefetch_related("coberturas")
        .order_by("-fecha_solicitada", "-id")
    )
    vacantes = filtrar_vacantes_para_usuario(request.user, vacantes)
    if estado:
        vacantes = vacantes.filter(estado=estado)
    if area:
        vacantes = vacantes.filter(area__icontains=area)

    base_qs = filtrar_vacantes_para_usuario(request.user, VacanteRRHH.objects.all())
    stats = {
        "total": base_qs.count(),
        "pendientes_autorizacion": base_qs.filter(estado=VacanteRRHH.ESTADO_PENDIENTE_DIRECCION).count(),
        "jefaturas_direccion": base_qs.filter(requiere_direccion=True).count(),
        "reclutamiento": base_qs.filter(estado=VacanteRRHH.ESTADO_RECLUTAMIENTO).count(),
        "cubiertas": base_qs.filter(estado=VacanteRRHH.ESTADO_CUBIERTA).count(),
    }
    return render(
        request,
        "rrhh/vacantes.html",
        {
            "module_tabs": _module_tabs("vacantes"),
            "can_manage_vacantes": can_gestionar_vacantes(request.user),
            "can_authorize_vacante": can_autorizar_vacante(request.user),
            "vacantes": vacantes[:300],
            "estado_choices": VacanteRRHH.ESTADO_CHOICES,
            "stats": stats,
            "estado_actual": estado,
            "area_actual": area,
        },
    )


@login_required
def vacante_nueva(request):
    if not can_gestionar_vacantes(request.user):
        raise PermissionDenied("No tienes permisos para crear solicitudes de vacantes")
    if request.method == "POST":
        sucursal = None
        sucursal_id = (request.POST.get("sucursal") or "").strip()
        if sucursal_id:
            sucursal = get_object_or_404(Sucursal, pk=sucursal_id)
        vacante = crear_solicitud_vacante(
            area=request.POST.get("area"),
            puesto=request.POST.get("puesto"),
            fecha_solicitada=_parse_date(request.POST.get("fecha_solicitada")),
            fecha_necesaria=_parse_date(request.POST.get("fecha_necesaria")),
            solicitado_por=request.user,
            creado_por=request.user,
            sucursal=sucursal,
            departamento=request.POST.get("departamento") or "",
            cantidad_solicitada=request.POST.get("cantidad_solicitada") or 1,
            tipo_solicitud=request.POST.get("tipo_solicitud") or VacanteRRHH.TIPO_REEMPLAZO,
            prioridad=request.POST.get("prioridad") or VacanteRRHH.PRIORIDAD_NORMAL,
            motivo_solicitud=request.POST.get("motivo_solicitud") or "",
            sugerencias=request.POST.get("sugerencias") or "",
        )
        messages.success(request, f"Solicitud {vacante.folio} creada.")
        return redirect("rrhh:rrhh_vacante_detalle", pk=vacante.pk)

    return render(
        request,
        "rrhh/vacante_form.html",
        {
            "module_tabs": _module_tabs("vacantes"),
            "sucursales": Sucursal.objects.filter(activa=True).order_by("nombre"),
            "departamento_choices": Empleado.DEP_CHOICES,
            "tipo_choices": VacanteRRHH.TIPO_CHOICES,
            "prioridad_choices": VacanteRRHH.PRIORIDAD_CHOICES,
        },
    )


@login_required
def vacante_detalle(request, pk: int):
    vacante = get_object_or_404(
        VacanteRRHH.objects.select_related(
            "sucursal",
            "solicitado_por",
            "creado_por",
            "validado_rrhh_por",
            "autorizado_por",
            "autorizador_asignado",
            "rechazado_por",
            "empleado_cubrio",
        ).prefetch_related("movimientos__actor", "coberturas__empleado"),
        pk=pk,
    )
    if not can_ver_vacante(request.user, vacante):
        raise PermissionDenied("No tienes acceso a esta solicitud de vacante.")
    return render(
        request,
        "rrhh/vacante_detalle.html",
        {
            "module_tabs": _module_tabs("vacantes"),
            "vacante": vacante,
            "empleados": Empleado.objects.filter(activo=True).order_by("nombre")[:1200],
            "can_manage_vacantes": can_gestionar_vacantes(request.user),
            "can_authorize_vacante": can_autorizar_vacante(request.user, vacante),
        },
    )


@login_required
def vacante_accion(request, pk: int):
    if request.method != "POST":
        return redirect("rrhh:rrhh_vacante_detalle", pk=pk)
    vacante = get_object_or_404(VacanteRRHH, pk=pk)
    action = (request.POST.get("action") or "").strip()
    comentario = (request.POST.get("comentario") or "").strip()
    try:
        if action in {"enviar_autorizacion", "enviar_direccion"}:
            enviar_vacante_autorizacion(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} enviada a autorización.")
        elif action == "aprobar":
            aprobar_vacante_autorizacion(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} aprobada.")
        elif action == "rechazar":
            rechazar_vacante_autorizacion(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} rechazada.")
        elif action == "reclutamiento":
            iniciar_reclutamiento_vacante(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} pasó a reclutamiento.")
        elif action == "pausar":
            pausar_vacante(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} pausada.")
        elif action == "cancelar":
            cancelar_vacante(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} cancelada.")
        elif action == "cubrir":
            empleado = get_object_or_404(Empleado, pk=request.POST.get("empleado"))
            cubrir_vacante(
                vacante,
                empleado,
                request.user,
                fecha_cobertura=_parse_date(request.POST.get("fecha_cobertura")),
                nota=comentario,
            )
            messages.success(request, f"Cobertura registrada para {vacante.folio}.")
        else:
            messages.error(request, "Acción inválida para la solicitud.")
    except ValidationError as exc:
        messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
    return redirect("rrhh:rrhh_vacante_detalle", pk=pk)


def _can_view_board(user) -> bool:
    if can_ver_vacante(user) or can_gestionar_vacantes(user) or can_autorizar_vacante(user):
        return True
    if not getattr(user, "is_authenticated", False):
        return False
    return (
        VacanteRRHH.objects.filter(solicitado_por=user).exists()
        or VacanteRRHH.objects.filter(creado_por=user).exists()
    )
