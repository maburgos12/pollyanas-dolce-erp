from __future__ import annotations

from datetime import timedelta

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render
from django.utils import timezone

from core.access import can_view_rrhh

from .models import AsistenciaEmpleado, Empleado
from .views import _module_tabs


@login_required
def monitor_sincronizacion(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver el monitor de asistencia")

    hoy = timezone.localdate()
    empleados_activos = Empleado.objects.filter(activo=True).count()
    asistencias_hoy = AsistenciaEmpleado.objects.filter(fecha=hoy).select_related("empleado", "turno", "sucursal")
    emp_con_asistencia = set(asistencias_hoy.values_list("empleado_id", flat=True))
    emp_sin_asistencia = Empleado.objects.filter(activo=True).exclude(id__in=emp_con_asistencia).order_by("nombre")
    ultimas_por_api = (
        AsistenciaEmpleado.objects.filter(
            fuente__in=[AsistenciaEmpleado.FUENTE_HIKCONNECT_API, AsistenciaEmpleado.FUENTE_POINT]
        )
        .select_related("empleado", "turno", "sucursal")
        .order_by("-creado_en")[:20]
    )

    return render(
        request,
        "rrhh/monitor_sincronizacion.html",
        {
            "module_tabs": _module_tabs("monitor_sync"),
            "hoy": hoy,
            "ayer": hoy - timedelta(days=1),
            "empleados_activos": empleados_activos,
            "asistencias_hoy": asistencias_hoy.order_by("empleado__nombre"),
            "emp_sin_asistencia": emp_sin_asistencia,
            "ultimas_por_api": ultimas_por_api,
            "resumen": {
                "total_activos": empleados_activos,
                "con_asistencia_hoy": len(emp_con_asistencia),
                "sin_asistencia_hoy": emp_sin_asistencia.count(),
            },
        },
    )
