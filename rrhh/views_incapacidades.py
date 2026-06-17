from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_date
from django.views.decorators.http import require_POST

from core.access import can_manage_submodule, can_view_submodule

from .models import Empleado, IncapacidadEmpleado, SolicitudVacaciones
from .services_asistencia_reglas import evaluar_rango_asistencia
from .views import _module_tabs


def _require_manage_rrhh(user):
    if not can_manage_submodule(user, "rrhh", "nomina"):
        raise PermissionDenied("Solo Capital Humano puede capturar incapacidades.")


@login_required
def rrhh_incapacidades(request):
    if not can_view_submodule(request.user, "rrhh", "nomina"):
        raise PermissionDenied("No tienes permisos para ver incapacidades.")

    empleado_id = (request.GET.get("empleado") or "").strip()
    estado = (request.GET.get("estado") or "").strip()
    incapacidades_qs = IncapacidadEmpleado.objects.select_related("empleado", "registrada_por").order_by("-fecha_inicio", "-id")
    if empleado_id.isdigit():
        incapacidades_qs = incapacidades_qs.filter(empleado_id=int(empleado_id))
    if estado in {choice[0] for choice in IncapacidadEmpleado.ESTADO_CHOICES}:
        incapacidades_qs = incapacidades_qs.filter(estado=estado)

    rows = []
    for incapacidad in incapacidades_qs[:300]:
        vacaciones = SolicitudVacaciones.objects.filter(
            empleado=incapacidad.empleado,
            estado__in=[
                SolicitudVacaciones.ESTADO_SOLICITADA,
                SolicitudVacaciones.ESTADO_PREAUTORIZADA,
                SolicitudVacaciones.ESTADO_APROBADA,
            ],
            fecha_inicio__lte=incapacidad.fecha_fin,
            fecha_fin__gte=incapacidad.fecha_inicio,
        ).order_by("-creado_en", "-id")[:5]
        rows.append({"incapacidad": incapacidad, "vacaciones": vacaciones})

    empleados = Empleado.objects.filter(activo=True).order_by("nombre", "id")
    return render(
        request,
        "rrhh/incapacidades.html",
        {
            "module_tabs": _module_tabs("incapacidades", request.user),
            "rows": rows,
            "empleados": empleados,
            "tipo_choices": IncapacidadEmpleado.TIPO_CHOICES,
            "estado_choices": IncapacidadEmpleado.ESTADO_CHOICES,
            "empleado_id": empleado_id,
            "estado": estado,
            "can_manage_rrhh": can_manage_submodule(request.user, "rrhh", "nomina"),
        },
    )


@login_required
@require_POST
def crear_incapacidad(request):
    _require_manage_rrhh(request.user)
    empleado_id = (request.POST.get("empleado") or "").strip()
    if not empleado_id.isdigit():
        messages.error(request, "Selecciona un empleado activo.")
        return redirect("rrhh:rrhh_incapacidades")
    empleado = get_object_or_404(Empleado, pk=int(empleado_id), activo=True)
    fecha_inicio = parse_date((request.POST.get("fecha_inicio") or "").strip())
    fecha_fin = parse_date((request.POST.get("fecha_fin") or "").strip())
    tipo = (request.POST.get("tipo") or "").strip()
    estado = (request.POST.get("estado") or IncapacidadEmpleado.ESTADO_ACTIVA).strip()
    if not fecha_inicio or not fecha_fin or not tipo:
        messages.error(request, "Empleado, fechas y tipo son obligatorios.")
        return redirect("rrhh:rrhh_incapacidades")
    if tipo not in {choice[0] for choice in IncapacidadEmpleado.TIPO_CHOICES}:
        messages.error(request, "Tipo de incapacidad inválido.")
        return redirect("rrhh:rrhh_incapacidades")
    if estado not in {IncapacidadEmpleado.ESTADO_ACTIVA, IncapacidadEmpleado.ESTADO_CERRADA}:
        messages.error(request, "Solo se puede crear una incapacidad activa o cerrada.")
        return redirect("rrhh:rrhh_incapacidades")
    if IncapacidadEmpleado.objects.filter(
        empleado=empleado,
        estado__in=[IncapacidadEmpleado.ESTADO_ACTIVA, IncapacidadEmpleado.ESTADO_CERRADA],
        fecha_inicio__lte=fecha_fin,
        fecha_fin__gte=fecha_inicio,
    ).exists():
        messages.error(request, "Ya existe una incapacidad no cancelada que cruza esas fechas.")
        return redirect("rrhh:rrhh_incapacidades")

    incapacidad = IncapacidadEmpleado(
        empleado=empleado,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        tipo=tipo,
        folio=(request.POST.get("folio") or "").strip(),
        estado=estado,
        notas=(request.POST.get("notas") or "").strip(),
        registrada_por=request.user,
    )
    try:
        incapacidad.full_clean()
        incapacidad.save()
    except (IntegrityError, ValidationError) as exc:
        messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
        return redirect("rrhh:rrhh_incapacidades")

    evaluar_rango_asistencia(fecha_inicio, fecha_fin, empleados=[empleado])
    messages.success(request, "Incapacidad registrada.")
    return redirect("rrhh:rrhh_incapacidades")


@login_required
@require_POST
def cancelar_incapacidad(request, incapacidad_id):
    _require_manage_rrhh(request.user)
    incapacidad = get_object_or_404(IncapacidadEmpleado, pk=incapacidad_id)
    comentario = (request.POST.get("comentario_cancelacion") or "").strip()
    if not comentario:
        messages.error(request, "El comentario de cancelación es obligatorio.")
        return redirect("rrhh:rrhh_incapacidades")

    incapacidad.estado = IncapacidadEmpleado.ESTADO_CANCELADA
    incapacidad.comentario_cancelacion = comentario
    incapacidad.save(update_fields=["estado", "comentario_cancelacion", "actualizado_en"])
    hoy = timezone.localdate()
    if incapacidad.fecha_inicio <= hoy:
        evaluar_rango_asistencia(incapacidad.fecha_inicio, min(incapacidad.fecha_fin, hoy), empleados=[incapacidad.empleado])
    messages.success(request, "Incapacidad cancelada.")
    return redirect("rrhh:rrhh_incapacidades")
