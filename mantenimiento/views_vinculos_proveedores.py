from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import JsonResponse
from django.shortcuts import render
from django.template.loader import render_to_string
from django.views.decorators.http import require_http_methods

from maestros.models import Proveedor
from .models import ProveedorServicio, VinculoProveedorDocumental
from .services_vinculos_proveedores import (
    actor_actual, confirmar_vinculo, puede_crear_vinculos, puede_leer_vinculos,
)


@login_required
@require_http_methods(["GET", "POST"])
def vinculos_proveedores(request):
    actor = actor_actual(request.user)
    if not puede_leer_vinculos(actor):
        raise PermissionDenied("Necesitas leer Mantenimiento y Maestros · Proveedores.")
    mensaje, ok, status = "", True, 200
    datos = request.POST if request.method == "POST" else request.GET
    if request.method == "POST":
        try:
            _, creado = confirmar_vinculo(
                user=request.user, perfil_id=datos.get("perfil_id"), proveedor_id=datos.get("proveedor_id"),
                motivo=datos.get("motivo"), evidencia=datos.get("evidencia"),
                confirmado=datos.get("confirmado") == "on",
            )
            mensaje = "Vínculo documental confirmado." if creado else "Este vínculo ya estaba confirmado. Se conservó su registro original."
        except ValidationError as exc:
            mensaje, ok = " ".join(exc.messages), False
            status = 409 if exc.code == "conflict" else 400
    contexto = {
        "puede_crear": puede_crear_vinculos(actor), "mensaje": mensaje, "ok": ok,
        "perfiles": ProveedorServicio.objects.filter(activo=True).order_by("nombre", "pk"),
        "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre", "pk"),
        "vinculos": VinculoProveedorDocumental.objects.select_related("perfil", "proveedor", "autor"),
        "perfil_elegido": str(datos.get("perfil_id", "")), "proveedor_elegido": str(datos.get("proveedor_id", "")),
        "motivo": datos.get("motivo", ""), "evidencia": datos.get("evidencia", ""),
        "confirmado": datos.get("confirmado") == "on",
    }
    if request.method == "POST" and ("application/json" in request.headers.get("Accept", "")
                                    or request.headers.get("X-Requested-With") == "XMLHttpRequest"):
        return JsonResponse({"ok": ok, "toast": {"type": "success" if ok else "error", "message": mensaje},
                             "target": "#vinculos-documentales", "html": render_to_string(
                                 "mantenimiento/_vinculos_proveedores.html", contexto, request=request)}, status=status)
    return render(request, "mantenimiento/vinculos_proveedores.html", contexto, status=status)
