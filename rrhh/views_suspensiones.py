from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.dateparse import parse_date
from django.views.decorators.http import require_POST

from core.access import can_manage_rrhh, can_view_rrhh

from .models import Empleado, SuspensionEmpleado
from .services import usuario_jefe_directo_de_empleado
from .services_asistencia_reglas import evaluar_rango_asistencia
from .views import _module_tabs


def _can_manage_suspension_for_employee(user, empleado: Empleado) -> bool:
    if can_manage_rrhh(user):
        return True
    jefe = usuario_jefe_directo_de_empleado(empleado)
    return bool(jefe and user and jefe.id == user.id)


def _require_can_manage_suspension(user, empleado: Empleado):
    if not _can_manage_suspension_for_employee(user, empleado):
        raise PermissionDenied("No tienes permisos para capturar o cancelar suspensiones de este empleado.")


@login_required
def rrhh_suspensiones(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver suspensiones.")

    empleado_id = (request.GET.get("empleado") or "").strip()
    estado = (request.GET.get("estado") or "").strip()
    suspensiones = SuspensionEmpleado.objects.select_related("empleado", "aplicada_por").order_by("-fecha_inicio")
    if empleado_id:
        suspensiones = suspensiones.filter(empleado_id=empleado_id)
    if estado in {SuspensionEmpleado.ESTADO_ACTIVA, SuspensionEmpleado.ESTADO_CANCELADA}:
        suspensiones = suspensiones.filter(estado=estado)

    empleados = Empleado.objects.filter(activo=True).order_by("nombre", "id")
    return render(
        request,
        "rrhh/suspensiones.html",
        {
            "module_tabs": _module_tabs("suspensiones", request.user),
            "suspensiones": suspensiones[:500],
            "empleados": empleados,
            "estado_choices": SuspensionEmpleado.ESTADO_CHOICES,
            "empleado_id": empleado_id,
            "estado": estado,
            "can_manage_rrhh": can_manage_rrhh(request.user),
        },
    )


@login_required
@require_POST
def crear_suspension(request):
    empleado = get_object_or_404(Empleado, pk=request.POST.get("empleado"))
    _require_can_manage_suspension(request.user, empleado)

    fecha_inicio = parse_date((request.POST.get("fecha_inicio") or "").strip())
    fecha_fin = parse_date((request.POST.get("fecha_fin") or "").strip())
    motivo = (request.POST.get("motivo") or "").strip()
    con_goce = request.POST.get("con_goce") == "on"
    if not fecha_inicio or not fecha_fin or not motivo:
        messages.error(request, "Empleado, fechas y motivo son obligatorios.")
        return redirect("rrhh:rrhh_suspensiones")

    suspension = SuspensionEmpleado(
        empleado=empleado,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        motivo=motivo,
        con_goce=con_goce,
        aplicada_por=request.user,
    )
    try:
        suspension.full_clean()
    except ValidationError as exc:
        messages.error(request, "; ".join(exc.messages))
        return redirect("rrhh:rrhh_suspensiones")

    suspension.save()
    evaluar_rango_asistencia(fecha_inicio, fecha_fin, empleados=[empleado])
    messages.success(request, "Suspensión creada y asistencia reevaluada.")
    return redirect("rrhh:rrhh_suspensiones")


@login_required
@require_POST
def cancelar_suspension(request, suspension_id):
    suspension = get_object_or_404(
        SuspensionEmpleado.objects.select_related("empleado"),
        pk=suspension_id,
    )
    _require_can_manage_suspension(request.user, suspension.empleado)
    comentario = (request.POST.get("comentario_cancelacion") or "").strip()
    if not comentario:
        messages.error(request, "El comentario de cancelación es obligatorio.")
        return redirect("rrhh:rrhh_suspensiones")

    suspension.estado = SuspensionEmpleado.ESTADO_CANCELADA
    suspension.comentario_cancelacion = comentario
    suspension.save(update_fields=["estado", "comentario_cancelacion", "actualizado_en"])
    evaluar_rango_asistencia(suspension.fecha_inicio, suspension.fecha_fin, empleados=[suspension.empleado])
    messages.success(request, "Suspensión cancelada y asistencia reevaluada.")
    return redirect("rrhh:rrhh_suspensiones")
