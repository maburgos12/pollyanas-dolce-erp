from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render

from core.access import ROLE_ADMIN, ROLE_DG, ROLE_VENTAS, has_any_role

from .models import SolicitudHorarioEspecial


def _can_access_special_hours(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_VENTAS)


@login_required
def index(request):
    if not _can_access_special_hours(request.user):
        raise PermissionDenied("No tienes permisos para gestionar horarios especiales.")

    rows = list(
        SolicitudHorarioEspecial.objects.select_related("requested_by", "approved_by", "executed_by")
        .prefetch_related("details__sucursal")
        .order_by("-created_at")[:50]
    )
    return render(
        request,
        "horarios_especiales/index.html",
        {
            "rows": rows,
            "stats": {
                "borrador": SolicitudHorarioEspecial.objects.filter(status=SolicitudHorarioEspecial.STATUS_BORRADOR).count(),
                "aprobado": SolicitudHorarioEspecial.objects.filter(status=SolicitudHorarioEspecial.STATUS_APROBADO).count(),
                "ejecutado": SolicitudHorarioEspecial.objects.filter(status=SolicitudHorarioEspecial.STATUS_EJECUTADO).count(),
                "fallido": SolicitudHorarioEspecial.objects.filter(status=SolicitudHorarioEspecial.STATUS_FALLIDO).count(),
            },
        },
    )

