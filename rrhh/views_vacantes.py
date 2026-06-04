from __future__ import annotations

from datetime import date

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render

from core.models import Sucursal

from .models import Empleado, VacanteRRHH, VacanteSeguimiento
from .services_vacantes import (
    agregar_seguimiento_vacante,
    aprobar_vacante_autorizacion,
    cancelar_vacante,
    can_autorizar_vacante,
    can_gestionar_vacantes,
    can_solicitar_vacantes,
    can_ver_vacante,
    cubrir_vacante,
    crear_solicitud_vacante,
    devolver_vacante_correccion,
    enviar_vacante_autorizacion,
    filtrar_vacantes_para_usuario,
    iniciar_reclutamiento_vacante,
    pausar_vacante,
    reenviar_vacante_revision,
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
            "can_create_vacante_request": can_solicitar_vacantes(request.user),
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
    can_manage = can_gestionar_vacantes(request.user)
    if not can_solicitar_vacantes(request.user):
        raise PermissionDenied("No tienes permisos para crear solicitudes de vacantes")
    if request.method == "POST":
        sucursal = None
        sucursal_id = (request.POST.get("sucursal") or "").strip()
        if sucursal_id:
            sucursal = get_object_or_404(Sucursal, pk=sucursal_id)
        solicitado_por = request.user
        if can_manage:
            solicitado_por_id = (request.POST.get("solicitado_por") or "").strip()
            if solicitado_por_id:
                solicitado_por = get_object_or_404(get_user_model(), pk=solicitado_por_id, is_active=True)
        vacante = crear_solicitud_vacante(
            area=request.POST.get("area"),
            puesto=request.POST.get("puesto"),
            fecha_solicitada=_parse_date(request.POST.get("fecha_solicitada")),
            fecha_necesaria=_parse_date(request.POST.get("fecha_necesaria")),
            solicitado_por=solicitado_por,
            creado_por=request.user,
            sucursal=sucursal,
            departamento=request.POST.get("departamento") or "",
            cantidad_solicitada=request.POST.get("cantidad_solicitada") or 1,
            tipo_solicitud=request.POST.get("tipo_solicitud") or VacanteRRHH.TIPO_REEMPLAZO,
            prioridad=request.POST.get("prioridad") or VacanteRRHH.PRIORIDAD_NORMAL,
            motivo_solicitud=request.POST.get("motivo_solicitud") or "",
            sugerencias=request.POST.get("sugerencias") or "",
            estado_inicial=VacanteRRHH.ESTADO_SOLICITADA if can_manage else VacanteRRHH.ESTADO_REVISION_RRHH,
        )
        messages.success(request, f"Solicitud {vacante.folio} creada.")
        return redirect("rrhh:rrhh_vacante_detalle", pk=vacante.pk)

    return render(
        request,
        "rrhh/vacante_form.html",
        {
            "module_tabs": _module_tabs("vacantes"),
            "sucursales": Sucursal.objects.filter(activa=True).order_by("nombre"),
            "can_manage_vacantes": can_manage,
            "solicitantes": _usuarios_solicitantes_vacantes(),
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
        ).prefetch_related("movimientos__actor", "coberturas__empleado", "seguimientos__creado_por"),
        pk=pk,
    )
    if not can_ver_vacante(request.user, vacante):
        raise PermissionDenied("No tienes acceso a esta solicitud de vacante.")
    workflow = _vacante_workflow_context(vacante)
    show_rrhh_review_actions = can_gestionar_vacantes(request.user) and vacante.estado in {
        VacanteRRHH.ESTADO_SOLICITADA,
        VacanteRRHH.ESTADO_REVISION_RRHH,
    }
    show_recruitment_action = can_gestionar_vacantes(request.user) and vacante.estado in {
        VacanteRRHH.ESTADO_AUTORIZADA,
        VacanteRRHH.ESTADO_PAUSADA,
    }
    show_cover_action = False  # se muestra dentro del seguimiento, no en el banner
    show_pause_action = (
        can_gestionar_vacantes(request.user)
        and vacante.estado
        not in {VacanteRRHH.ESTADO_CUBIERTA, VacanteRRHH.ESTADO_CANCELADA, VacanteRRHH.ESTADO_RECHAZADA}
    )
    show_cancel_action = (
        can_gestionar_vacantes(request.user)
        and vacante.estado not in {VacanteRRHH.ESTADO_CUBIERTA, VacanteRRHH.ESTADO_CANCELADA}
    )
    show_authorize_actions = can_autorizar_vacante(request.user, vacante) and vacante.estado == VacanteRRHH.ESTADO_PENDIENTE_DIRECCION
    show_reenviar_revision = (
        vacante.estado == VacanteRRHH.ESTADO_DEVUELTA_CORRECCION
        and (
            can_gestionar_vacantes(request.user)
            or vacante.solicitado_por_id == request.user.id
            or vacante.creado_por_id == request.user.id
        )
    )
    show_tracking_form = (
        can_gestionar_vacantes(request.user)
        and vacante.estado not in {VacanteRRHH.ESTADO_CUBIERTA, VacanteRRHH.ESTADO_RECHAZADA, VacanteRRHH.ESTADO_CANCELADA}
    )
    return render(
        request,
        "rrhh/vacante_detalle.html",
        {
            "module_tabs": _module_tabs("vacantes"),
            "vacante": vacante,
            "empleados": Empleado.objects.filter(activo=True).order_by("nombre")[:1200],
            "can_manage_vacantes": can_gestionar_vacantes(request.user),
            "can_create_vacante_request": can_solicitar_vacantes(request.user),
            "can_authorize_vacante": can_autorizar_vacante(request.user, vacante),
            "workflow": workflow,
            "show_rrhh_review_actions": show_rrhh_review_actions,
            "show_recruitment_action": show_recruitment_action,
            "show_cover_action": show_cover_action,
            "show_pause_action": show_pause_action,
            "show_cancel_action": show_cancel_action,
            "show_authorize_actions": show_authorize_actions,
            "show_reenviar_revision": show_reenviar_revision,
            "show_tracking_form": show_tracking_form,
            "seguimiento_etapas": VacanteSeguimiento.ETAPA_CHOICES,
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
        elif action == "devolver_correccion":
            devolver_vacante_correccion(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} devuelta a corrección.")
        elif action == "reenviar_revision":
            reenviar_vacante_revision(vacante, request.user, comentario)
            messages.success(request, f"Solicitud {vacante.folio} reenviada a revisión.")
        elif action == "seguimiento":
            agregar_seguimiento_vacante(
                vacante,
                request.user,
                etapa=request.POST.get("etapa") or VacanteSeguimiento.ETAPA_COMENTARIO,
                candidato=request.POST.get("candidato") or "",
                comentario=comentario,
                fecha=_parse_date(request.POST.get("fecha")),
            )
            messages.success(request, f"Seguimiento registrado para {vacante.folio}.")
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
    if can_ver_vacante(user) or can_gestionar_vacantes(user) or can_autorizar_vacante(user) or can_solicitar_vacantes(user):
        return True
    if not getattr(user, "is_authenticated", False):
        return False
    return (
        VacanteRRHH.objects.filter(solicitado_por=user).exists()
        or VacanteRRHH.objects.filter(creado_por=user).exists()
    )


def _usuarios_solicitantes_vacantes():
    User = get_user_model()
    ids = set(
        Empleado.objects.filter(activo=True, usuario_erp__isnull=False)
        .filter(
            Q(puesto_operativo="JEFATURA")
            | Q(puesto__icontains="jefe")
            | Q(puesto__icontains="encarg")
            | Q(colaboradores_directos__isnull=False)
        )
        .distinct()
        .values_list("usuario_erp_id", flat=True)
    )
    ids.update(
        User.objects.filter(
            is_active=True,
        )
        .filter(Q(is_superuser=True) | Q(groups__name__iexact="DG"))
        .values_list("id", flat=True)
    )
    return User.objects.filter(id__in=ids, is_active=True).order_by("first_name", "last_name", "username")


def _display_user(user) -> str:
    if not user:
        return "-"
    return user.get_full_name() or user.username


def _vacante_workflow_context(vacante: VacanteRRHH) -> dict:
    current_step = _workflow_current_step(vacante)
    steps = [
        {"key": "solicitud", "label": "Solicitud", "state": _workflow_step_state("solicitud", current_step, vacante)},
        {"key": "revision", "label": "Revisión RRHH", "state": _workflow_step_state("revision", current_step, vacante)},
        {"key": "autorizacion", "label": "Autorización", "state": _workflow_step_state("autorizacion", current_step, vacante)},
        {"key": "reclutamiento", "label": "Reclutamiento", "state": _workflow_step_state("reclutamiento", current_step, vacante)},
        {"key": "cobertura", "label": "Cobertura", "state": _workflow_step_state("cobertura", current_step, vacante)},
    ]
    return {
        "steps": steps,
        "responsable": _workflow_responsable(vacante),
        "siguiente": _workflow_siguiente(vacante),
        "autorizacion": _workflow_autorizacion(vacante),
    }


def _workflow_current_step(vacante: VacanteRRHH) -> str:
    if vacante.estado == VacanteRRHH.ESTADO_SOLICITADA:
        return "solicitud"
    if vacante.estado in {VacanteRRHH.ESTADO_REVISION_RRHH, VacanteRRHH.ESTADO_DEVUELTA_CORRECCION}:
        return "revision"
    if vacante.estado in {
        VacanteRRHH.ESTADO_PENDIENTE_DIRECCION,
        VacanteRRHH.ESTADO_AUTORIZADA,
        VacanteRRHH.ESTADO_RECHAZADA,
    }:
        return "autorizacion"
    if vacante.estado in {VacanteRRHH.ESTADO_RECLUTAMIENTO, VacanteRRHH.ESTADO_PAUSADA, VacanteRRHH.ESTADO_CANCELADA}:
        return "reclutamiento"
    if vacante.estado == VacanteRRHH.ESTADO_CUBIERTA:
        return "cobertura"
    return "solicitud"


def _workflow_step_state(step: str, current_step: str, vacante: VacanteRRHH) -> str:
    order = ["solicitud", "revision", "autorizacion", "reclutamiento", "cobertura"]
    if vacante.estado in {VacanteRRHH.ESTADO_CANCELADA, VacanteRRHH.ESTADO_RECHAZADA} and step == current_step:
        return "blocked"
    if vacante.estado == VacanteRRHH.ESTADO_DEVUELTA_CORRECCION and step == "revision":
        return "attention"
    if step == current_step:
        return "current"
    return "done" if order.index(step) < order.index(current_step) else "pending"


def _workflow_responsable(vacante: VacanteRRHH) -> str:
    if vacante.estado in {VacanteRRHH.ESTADO_SOLICITADA, VacanteRRHH.ESTADO_REVISION_RRHH}:
        return "Capital Humano"
    if vacante.estado == VacanteRRHH.ESTADO_DEVUELTA_CORRECCION:
        if vacante.solicitado_por_id != vacante.creado_por_id and vacante.creado_por_id:
            return "Solicitante / capturista"
        return _display_user(vacante.solicitado_por)
    if vacante.estado == VacanteRRHH.ESTADO_PENDIENTE_DIRECCION:
        if vacante.requiere_direccion:
            return "Dirección General"
        return _display_user(vacante.autorizador_asignado) if vacante.autorizador_asignado else "Jefatura directa pendiente"
    if vacante.estado in {VacanteRRHH.ESTADO_AUTORIZADA, VacanteRRHH.ESTADO_RECLUTAMIENTO, VacanteRRHH.ESTADO_PAUSADA}:
        return "Capital Humano"
    return "Cerrada"


def _workflow_siguiente(vacante: VacanteRRHH) -> str:
    if vacante.estado == VacanteRRHH.ESTADO_SOLICITADA:
        return "Validar datos de la solicitud."
    if vacante.estado == VacanteRRHH.ESTADO_REVISION_RRHH:
        return "Validar o devolver a corrección."
    if vacante.estado == VacanteRRHH.ESTADO_DEVUELTA_CORRECCION:
        return "Corregir y reenviar a revisión."
    if vacante.estado == VacanteRRHH.ESTADO_PENDIENTE_DIRECCION:
        return "Aprobar o rechazar la necesidad."
    if vacante.estado == VacanteRRHH.ESTADO_AUTORIZADA:
        return "Iniciar reclutamiento."
    if vacante.estado == VacanteRRHH.ESTADO_RECLUTAMIENTO:
        return "Registrar seguimiento y cobertura."
    if vacante.estado == VacanteRRHH.ESTADO_PAUSADA:
        return "Retomar reclutamiento o cerrar."
    if vacante.estado == VacanteRRHH.ESTADO_CUBIERTA:
        return "Vacante cubierta."
    if vacante.estado == VacanteRRHH.ESTADO_RECHAZADA:
        return "Solicitud rechazada."
    if vacante.estado == VacanteRRHH.ESTADO_CANCELADA:
        return "Solicitud cancelada."
    return "-"


def _workflow_autorizacion(vacante: VacanteRRHH) -> str:
    if vacante.estado in {VacanteRRHH.ESTADO_SOLICITADA, VacanteRRHH.ESTADO_REVISION_RRHH}:
        return "Pendiente de validación RRHH"
    if vacante.estado == VacanteRRHH.ESTADO_DEVUELTA_CORRECCION:
        return "Pendiente de corrección"
    if vacante.requiere_direccion:
        return "Dirección General"
    if vacante.autorizado_por:
        return _display_user(vacante.autorizado_por)
    if vacante.autorizador_asignado:
        return _display_user(vacante.autorizador_asignado)
    return "Jefatura directa pendiente"
