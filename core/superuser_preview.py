from __future__ import annotations

from django.contrib.auth import get_user_model
from django.db.models import Q
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_GET, require_POST

from core.audit import log_event
from rrhh.models import Empleado


SESSION_KEY = "superuser_preview_user_id"
MANAGEMENT_PREFIX = "/superusuario/ver-como/"


def _actor(request):
    return getattr(request, "preview_actor", None) or getattr(request, "user", None)


def _require_superuser(request):
    actor = _actor(request)
    if not actor or not actor.is_authenticated or not actor.is_active or not actor.is_superuser:
        return None
    return actor


def _safe_next(request, fallback="/dashboard/"):
    candidate = (request.POST.get("next") or request.GET.get("next") or "").strip()
    if candidate and url_has_allowed_host_and_scheme(
        candidate,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return candidate
    return fallback


def eligible_preview_employees():
    return (
        Empleado.objects.select_related("usuario_erp", "sucursal_ref")
        .filter(
            activo=True,
            usuario_erp__isnull=False,
            usuario_erp__is_active=True,
            usuario_erp__is_superuser=False,
        )
        .order_by("nombre", "id")
    )


@require_GET
def preview_options(request):
    if not _require_superuser(request):
        return HttpResponseForbidden("Esta opción es exclusiva para superusuarios.")

    employees = eligible_preview_employees()
    query = (request.GET.get("q") or "").strip()
    for token in query.split():
        employees = employees.filter(
            Q(nombre__icontains=token)
            | Q(area__icontains=token)
            | Q(puesto__icontains=token)
            | Q(sucursal__icontains=token)
            | Q(usuario_erp__username__icontains=token)
        )

    results = [
        {
            "user_id": employee.usuario_erp_id,
            "name": employee.nombre,
            "area": employee.area or "",
            "position": employee.puesto or employee.puesto_operativo or "",
            "branch": employee.sucursal_ref.nombre if employee.sucursal_ref_id else employee.sucursal or "",
        }
        for employee in employees[:50]
    ]
    return JsonResponse({"results": results})


@require_POST
def preview_start(request):
    actor = _require_superuser(request)
    if not actor:
        return HttpResponseForbidden("Esta opción es exclusiva para superusuarios.")

    employee = get_object_or_404(
        eligible_preview_employees(),
        usuario_erp_id=request.POST.get("user_id"),
    )
    request.session[SESSION_KEY] = employee.usuario_erp_id
    log_event(
        actor,
        "SUPERUSER_PREVIEW_START",
        "auth.User",
        str(employee.usuario_erp_id),
        {
            "employee_id": employee.pk,
            "employee_name": employee.nombre,
            "read_only": True,
        },
    )
    return redirect(_safe_next(request))


@require_POST
def preview_stop(request):
    actor = _require_superuser(request)
    if not actor:
        return HttpResponseForbidden("Esta opción es exclusiva para superusuarios.")

    target_id = request.session.pop(SESSION_KEY, None)
    if target_id:
        target = get_user_model().objects.filter(pk=target_id).first()
        log_event(
            actor,
            "SUPERUSER_PREVIEW_STOP",
            "auth.User",
            str(target_id),
            {
                "target_username": getattr(target, "username", ""),
                "read_only": True,
            },
        )
    return redirect(_safe_next(request))

