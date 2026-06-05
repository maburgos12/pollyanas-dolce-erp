import json

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_POST

from core.access import can_manage_rrhh
from core.models import Sucursal, UserProfile


@login_required
def asignacion_sucursal_view(request):
    return render(request, "rrhh/asignacion_sucursal.html")


@login_required
def asignacion_sucursales_api(request):
    rows = list(
        Sucursal.objects.filter(activa=True)
        .exclude(nombre__in=["Matriz", "CEDIS", "Devoluciones", "Almacén"])
        .order_by("nombre")
        .values("id", "nombre", "activa")
    )
    return JsonResponse({"count": len(rows), "results": rows})


@login_required
def usuarios_sucursal_view(request):
    if not can_manage_rrhh(request.user):
        from django.core.exceptions import PermissionDenied
        raise PermissionDenied
    User = get_user_model()
    usuarios = (
        User.objects.filter(is_active=True)
        .select_related("userprofile__sucursal")
        .order_by("first_name", "last_name", "username")
    )
    sucursales = Sucursal.objects.filter(activa=True).order_by("nombre")
    return render(request, "rrhh/usuarios_sucursal.html", {
        "usuarios": usuarios,
        "sucursales": sucursales,
    })


@login_required
@require_POST
def usuarios_sucursal_update(request):
    if not can_manage_rrhh(request.user):
        return JsonResponse({"error": "Sin permiso"}, status=403)
    try:
        body = json.loads(request.body)
        user_id = int(body["user_id"])
        sucursal_id = body.get("sucursal_id") or None
        if sucursal_id:
            sucursal_id = int(sucursal_id)
    except (KeyError, ValueError, TypeError):
        return JsonResponse({"error": "Datos inválidos"}, status=400)

    User = get_user_model()
    user = User.objects.filter(pk=user_id, is_active=True).first()
    if not user:
        return JsonResponse({"error": "Usuario no encontrado"}, status=404)

    profile, _ = UserProfile.objects.get_or_create(user=user)
    if sucursal_id:
        sucursal = Sucursal.objects.filter(pk=sucursal_id, activa=True).first()
        if not sucursal:
            return JsonResponse({"error": "Sucursal no válida"}, status=400)
        profile.sucursal = sucursal
    else:
        profile.sucursal = None
    profile.save(update_fields=["sucursal"])

    nombre_sucursal = profile.sucursal.nombre if profile.sucursal else "Sin sucursal"
    return JsonResponse({
        "ok": True,
        "user_id": user_id,
        "sucursal_id": profile.sucursal_id,
        "sucursal_nombre": nombre_sucursal,
    })
